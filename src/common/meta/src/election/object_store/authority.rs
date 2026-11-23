// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Object-store Authority with one writer per node and monotonic local leases.

#[cfg(test)]
mod tests;
mod writer;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};

use crate::election::object_store::authority::writer::{Request, Resignation, Writer};
use crate::election::object_store::{AuthorityHead, AuthorityState, HeadStore, NodeIdentity};
use crate::error::{self, Error, Result};

const LEASE: Duration = Duration::from_secs(10);
const RENEW_INTERVAL: Duration = Duration::from_secs(5);
const CLOCK_GUARD: Duration = Duration::from_secs(2);
const GET_TIMEOUT: Duration = Duration::from_millis(500);
const QUEUE_CAPACITY: usize = 32;

/// A token authorizes mutations only in its original local term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FencingToken {
    epoch: u64,
}

impl FencingToken {
    /// The logical epoch won by the takeover.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// Result of one takeover attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakeoverOutcome {
    /// The Head PUT applied and the node can lead in this term.
    Acquired(FencingToken),
    /// The Head PUT applied after local eligibility was lost.
    AppliedButInactive,
    /// The observed Head still has a live owner.
    NotEligible,
}

/// The local lifecycle of the Authority writer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityStatus {
    #[default]
    Follower,
    Leader {
        epoch: u64,
        deadline: Instant,
    },
    Resigning {
        epoch: u64,
        deadline: Instant,
    },
    RecoveryRequired,
    Closed,
}

impl AuthorityStatus {
    fn term(self) -> Option<(u64, Instant)> {
        match self {
            Self::Leader { epoch, deadline } | Self::Resigning { epoch, deadline } => {
                Some((epoch, deadline))
            }
            _ => None,
        }
    }
}

/// Additional condition on the Head for a fenced mutation.
#[derive(Debug, Default, Clone, Copy)]
pub enum AuthorityPrecondition {
    #[default]
    Any,
    WriteSeq(u64),
}

impl AuthorityPrecondition {
    fn check(self, head: &AuthorityHead) -> Result<()> {
        let actual = head.write_seq();
        if let Self::WriteSeq(expected) = self
            && actual != expected
        {
            return error::AuthorityWriteSeqMismatchSnafu { expected, actual }.fail();
        }
        Ok(())
    }
}

/// Fenced Head updates supported by Head v1.
#[derive(Debug, Clone, Copy)]
pub enum AuthorityMutation {
    Renew,
    Resign,
}

/// Node-local Authority. Share one instance and its writer across callers.
pub struct Authority {
    shared: Arc<Shared>,
    queue: mpsc::Sender<Request>,
    writer: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl Authority {
    /// Starts the writer with the process identity generated once at startup.
    pub fn new(store: Arc<HeadStore>, identity: NodeIdentity) -> Self {
        Self::start(
            store,
            identity,
            || {
                let now = Utc::now();
                i128::from(now.timestamp()) * 1_000_000_000
                    + i128::from(now.timestamp_subsec_nanos())
            },
            QUEUE_CAPACITY,
            common_runtime::spawn_global,
        )
    }

    fn start(
        store: Arc<HeadStore>,
        identity: NodeIdentity,
        wall_now_ns: impl Fn() -> i128 + Send + Sync + 'static,
        capacity: usize,
        spawn: impl FnOnce(Pin<Box<dyn Future<Output = ()> + Send>>) -> JoinHandle<()>,
    ) -> Self {
        let (status, _) = watch::channel(AuthorityStatus::Follower);
        let shared = Arc::new(Shared {
            store,
            identity,
            wall_now_ns: Box::new(wall_now_ns),
            control: Mutex::default(),
            status,
        });
        let (queue, requests) = mpsc::channel(capacity);
        let writer = spawn(Box::pin(Writer::new(shared.clone(), requests).run()));
        Self {
            shared,
            queue,
            writer: tokio::sync::Mutex::new(Some(writer)),
        }
    }

    /// Reads a coherent Head and its complete metadata in a single storage operation.
    pub async fn head(&self) -> Result<AuthorityState> {
        self.shared.read(Access::Read, None).await
    }

    /// Returns the local state, accounting for any elapsed monotonic deadline.
    pub fn status(&self) -> AuthorityStatus {
        self.shared.update(|control, _| control.status)
    }

    /// Attempts a single conditional takeover.
    pub async fn takeover(&self) -> Result<TakeoverOutcome> {
        let (reply, result) = oneshot::channel();
        self.submit(Access::Takeover, Request::Takeover(reply))
            .await?;
        result.await.unwrap_or_else(|_| Err(closed()))
    }

    /// Requests a fenced Head update. Success confirms its PUT.
    pub async fn mutate(
        &self,
        token: &FencingToken,
        precondition: AuthorityPrecondition,
        mutation: AuthorityMutation,
    ) -> Result<()> {
        let epoch = token.epoch;
        let (reply, result) = oneshot::channel();
        match mutation {
            AuthorityMutation::Renew => {
                self.submit(
                    Access::Renew(epoch),
                    Request::Renew {
                        epoch,
                        precondition,
                        reply,
                    },
                )
                .await?;
            }
            AuthorityMutation::Resign => self.shared.update(|control, _| {
                Access::Renew(epoch).check(control.status)?;
                let AuthorityStatus::Leader { deadline, .. } = control.status else {
                    return Err(rejected("term is inactive"));
                };
                control.status = AuthorityStatus::Resigning { epoch, deadline };
                control.resignation = Some(Resignation {
                    epoch,
                    precondition,
                    reply,
                });
                Ok(())
            })?,
        }
        result.await.unwrap_or_else(|_| Err(closed()))
    }

    /// Stops term admission and attempts to clear the owner before the frozen deadline.
    pub async fn resign(&self, token: &FencingToken) -> Result<()> {
        self.mutate(token, AuthorityPrecondition::Any, AuthorityMutation::Resign)
            .await
    }

    /// Stops the local term, cancels I/O waits and joins the writer.
    pub async fn shutdown(&self) {
        self.shared.close();
        let mut writer = self.writer.lock().await;
        if let Some(task) = writer.as_mut()
            && let Err(error) = task.await
        {
            common_telemetry::warn!("Authority writer exited with error: {error}");
        }
        writer.take();
    }

    async fn submit(&self, access: Access, request: Request) -> Result<()> {
        access.check(self.status())?;
        tokio::select! {
            permit = self.queue.reserve() => {
                let permit = permit.map_err(|_| closed())?;
                self.shared.update(|control, _| {
                    access.check(control.status)?;
                    permit.send(request);
                    Ok(())
                })
            }
            _ = self.shared.wait_until(|status| access.check(status).is_err()) => {
                access.check(self.status()).and_then(|()| Err(rejected("admission changed")))
            }
        }
    }
}

impl Drop for Authority {
    fn drop(&mut self) {
        self.shared.close();
    }
}

#[derive(Clone, Copy)]
enum Access {
    Read,
    Takeover,
    Renew(u64),
    Resign(u64),
}

impl Access {
    fn check(self, status: AuthorityStatus) -> Result<()> {
        match (self, status) {
            (_, AuthorityStatus::Closed) => Err(closed()),
            (_, AuthorityStatus::RecoveryRequired) => error::AuthorityRecoveryRequiredSnafu.fail(),
            (Self::Read, _) | (Self::Takeover, AuthorityStatus::Follower) => Ok(()),
            (Self::Renew(expected), AuthorityStatus::Leader { epoch, .. })
            | (Self::Resign(expected), AuthorityStatus::Resigning { epoch, .. })
                if expected == epoch =>
            {
                Ok(())
            }
            _ => Err(rejected(
                "local state or fencing token does not authorize the operation",
            )),
        }
    }
}

#[derive(Default)]
struct Control {
    status: AuthorityStatus,
    resignation: Option<Resignation>,
    snapshot: Option<SnapshotFloor>,
}

impl Control {
    fn expire(&mut self, now: Instant) {
        if self
            .status
            .term()
            .is_some_and(|(_, deadline)| now >= deadline)
        {
            self.status = AuthorityStatus::Follower;
        }
    }

    fn retire(&mut self, epoch: u64) {
        if self
            .status
            .term()
            .is_some_and(|(current, _)| current == epoch)
        {
            self.status = AuthorityStatus::Follower;
        }
    }
}

/// A confirmed PUT no longer has usable Metadata. Its base is retained solely
/// to rule out a stale conditional version when the next snapshot arrives.
enum SnapshotFloor {
    Observed(AuthorityState),
    Applied {
        head: AuthorityHead,
        base: AuthorityState,
    },
}

impl SnapshotFloor {
    fn head(&self) -> &AuthorityHead {
        match self {
            Self::Observed(state) => state.head(),
            Self::Applied { head, .. } => head,
        }
    }

    fn validate(&self, store: &HeadStore, state: &AuthorityState) -> Result<()> {
        let head = state.head();
        let floor = self.head();
        if head.epoch() < floor.epoch()
            || head.write_seq() < floor.write_seq()
            || (head.write_seq() == floor.write_seq() && head != floor)
        {
            return Err(invalid_snapshot(
                "Head moved behind an observed or applied write",
            ));
        }
        let (metadata, expect_same_version) = match self {
            Self::Observed(previous) => {
                (previous.metadata(), head.write_seq() == floor.write_seq())
            }
            Self::Applied { base, .. } => (base.metadata(), false),
        };
        if store.same_version(metadata, state.metadata())? != expect_same_version {
            return Err(invalid_snapshot(
                "conditional version disagrees with Head write sequence",
            ));
        }
        Ok(())
    }
}

struct Shared {
    store: Arc<HeadStore>,
    identity: NodeIdentity,
    wall_now_ns: Box<dyn Fn() -> i128 + Send + Sync>,
    control: Mutex<Control>,
    status: watch::Sender<AuthorityStatus>,
}

impl Shared {
    /// Runs the short state transitions and PUT authorization under the same lock.
    fn update<T>(&self, f: impl FnOnce(&mut Control, Instant) -> T) -> T {
        let mut control = self.control.lock().unwrap_or_else(PoisonError::into_inner);
        let before = control.status;
        let now = Instant::now();
        control.expire(now);
        let result = f(&mut control, now);
        if control.status != before {
            self.status.send_replace(control.status);
        }
        result
    }

    async fn wait_until(&self, predicate: impl Fn(AuthorityStatus) -> bool) {
        let mut status = self.status.subscribe();
        let _ = status.wait_for(|status| predicate(*status)).await;
    }

    fn close(&self) {
        self.update(|control, _| control.status = AuthorityStatus::Closed);
    }

    async fn read(&self, access: Access, deadline: Option<Instant>) -> Result<AuthorityState> {
        self.update(|control, _| access.check(control.status))?;
        let now = Instant::now();
        let stop = deadline.map_or(now + GET_TIMEOUT, |end| end.min(now + GET_TIMEOUT));
        if now >= stop {
            return Err(timeout("read"));
        }
        let result = tokio::select! {
            biased;
            _ = self.wait_until(|status| access.check(status).is_err()) => {
                return self.update(|control, _| {
                    access.check(control.status)?;
                    Err(rejected("read admission changed"))
                });
            }
            result = self.store.read_head() => result,
            _ = sleep_until(stop) => return Err(timeout("read")),
        };
        if matches!(result, Err(Error::AuthorityMissing { .. })) {
            self.update(|control, _| {
                if control.status != AuthorityStatus::Closed {
                    control.status = AuthorityStatus::RecoveryRequired;
                }
            });
        }
        let state = result?;
        if Instant::now() >= stop {
            return Err(timeout("read"));
        }
        self.observe(access, &state)?;
        Ok(state)
    }

    fn observe(&self, access: Access, state: &AuthorityState) -> Result<()> {
        self.update(|control, _| {
            let head = state.head();
            if let Some(floor) = &control.snapshot {
                // A concurrent reader can observe a newer Head while this response
                // is in flight. Historical evidence is usable for readback, but
                // it cannot replace the floor or supply a new CAS condition.
                if matches!(access, Access::Read)
                    && head.write_seq() < floor.head().write_seq()
                    && head.epoch() <= floor.head().epoch()
                {
                    return Ok(());
                }
                floor.validate(&self.store, state)?;
            }
            if let Some((epoch, _)) = control.status.term()
                && head.epoch() >= epoch
                && (head.epoch() != epoch || head.owner.as_ref() != Some(&self.identity))
            {
                control.retire(epoch);
            }
            control.snapshot = Some(SnapshotFloor::Observed(state.clone()));
            Ok(())
        })
    }

    fn diagnostic_deadline(&self) -> u64 {
        let ns = (self.wall_now_ns)().saturating_add(LEASE.as_nanos() as i128);
        u64::try_from(ns / 1_000_000).unwrap_or(if ns < 0 { 0 } else { u64::MAX })
    }
}

fn closed() -> Error {
    error::AuthorityClosedSnafu.build()
}

fn rejected(reason: &'static str) -> Error {
    error::AuthorityRejectedSnafu { reason }.build()
}

fn invalid_snapshot(reason: &'static str) -> Error {
    error::AuthorityMetadataSnafu { reason }.build()
}

fn timeout(operation: &'static str) -> Error {
    error::AuthorityTimeoutSnafu { operation }.build()
}
