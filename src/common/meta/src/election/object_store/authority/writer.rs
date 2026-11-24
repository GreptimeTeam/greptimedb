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

use std::sync::Arc;
use std::time::Duration;

use object_store::ErrorKind;
use snafu::{IntoError, OptionExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep_until};

use crate::election::object_store::authority::{
    Access, AuthorityMutation, AuthorityPrecondition, AuthorityStatus, CLOCK_GUARD, FencingToken,
    LEASE, RENEW_INTERVAL, Shared, SnapshotFloor, TakeoverOutcome, closed, rejected, timeout,
};
use crate::election::object_store::{AuthorityHead, AuthorityState};
use crate::error::{self, Error, Result};

const PUT_TIMEOUT: Duration = Duration::from_secs(2);
const READBACK_RESERVE: Duration = Duration::from_secs(1);
const INITIAL_BACKOFF: Duration = Duration::from_millis(50);
const MAX_BACKOFF: Duration = Duration::from_millis(400);

pub(crate) enum Request {
    Takeover(oneshot::Sender<Result<TakeoverOutcome>>),
    Mutate {
        epoch: u64,
        mutation: AuthorityMutation,
        precondition: AuthorityPrecondition,
        reply: oneshot::Sender<Result<()>>,
    },
}

impl Request {
    fn reject(self, error: Error) {
        match self {
            Self::Takeover(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Mutate { reply, .. } => {
                let _ = reply.send(Err(error));
            }
        }
    }
}

pub(crate) struct Resignation {
    pub(crate) epoch: u64,
    pub(crate) reply: oneshot::Sender<Result<()>>,
}

pub(crate) struct Writer {
    shared: Arc<Shared>,
    queue: mpsc::Receiver<Request>,
    ambiguous_takeover: Option<AuthorityState>,
}

impl Writer {
    pub(crate) fn new(shared: Arc<Shared>, queue: mpsc::Receiver<Request>) -> Self {
        Self {
            shared,
            queue,
            ambiguous_takeover: None,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut changes = self.shared.status.subscribe();
        loop {
            let (status, resignation) = self
                .shared
                .update(|control, _| (control.status, control.resignation.take()));
            if status == AuthorityStatus::Closed {
                if let Some(request) = resignation {
                    let _ = request.reply.send(Err(closed()));
                }
                break;
            }
            if let Some(request) = resignation {
                self.drain_resigning(request.epoch);
                let result = self.resign(request.epoch).await;
                let _ = request.reply.send(result);
                continue;
            }
            let term = match status {
                AuthorityStatus::Leader { epoch, deadline } => Some((epoch, deadline)),
                _ => None,
            };
            let deadline = term.map_or_else(Instant::now, |(_, deadline)| deadline);
            tokio::select! {
                biased;
                _ = changes.changed() => {}
                _ = sleep_until(deadline), if term.is_some() => {}
                _ = sleep_until(renew_at(deadline)), if term.is_some() => {
                    if let Some((epoch, _)) = term {
                        self.renew(epoch).await;
                    }
                }
                request = self.queue.recv() => match request {
                    Some(request) => self.process(request).await,
                    None => break,
                }
            }
        }
        self.queue.close();
        self.drain(closed);
    }

    fn drain_resigning(&mut self, epoch: u64) {
        // Once the term retires, the FIFO can also contain a new takeover.
        while let Some(request) = self.shared.update(|control, _| {
            Access::Resign(epoch).check(control.status).ok()?;
            self.queue.try_recv().ok()
        }) {
            request.reject(rejected("resignation stopped admission before PUT"));
        }
    }

    fn drain(&mut self, error: impl Fn() -> Error) {
        while let Ok(request) = self.queue.try_recv() {
            request.reject(error());
        }
    }

    async fn process(&mut self, request: Request) {
        match request {
            Request::Takeover(reply) => {
                let _ = reply.send(self.takeover().await);
            }
            Request::Mutate {
                epoch,
                mutation,
                precondition,
                reply,
            } => {
                let result = loop {
                    let put = match mutation {
                        AuthorityMutation::Renew => {
                            self.prepare_renewal(epoch, precondition, false).await
                        }
                        AuthorityMutation::Resign => {
                            self.prepare_term(Access::ConditionalResign(epoch), precondition, true)
                                .await
                        }
                    };
                    match put {
                        Ok(Some(put)) => {
                            if matches!(mutation, AuthorityMutation::Resign) {
                                self.drain_resigning(epoch);
                            }
                            break self.write(&put).await.map(|_| ());
                        }
                        Ok(None) => self.renew(epoch).await,
                        Err(error) => break Err(error),
                    }
                };
                let _ = reply.send(result);
            }
        }
    }

    async fn takeover(&mut self) -> Result<TakeoverOutcome> {
        let base = self.shared.read(Access::Takeover, None).await?;
        let ambiguous = if let Some(previous) = &self.ambiguous_takeover {
            self.shared
                .store
                .same_version(previous.metadata(), base.metadata())?
        } else {
            false
        };
        if !ambiguous {
            self.ambiguous_takeover = None;
        }

        if base.head().owner.is_some() {
            let modified = base
                .metadata()
                .last_modified()
                .context(error::AuthorityMetadataSnafu {
                    reason: "read returned no last_modified",
                })?
                .into_inner()
                .as_nanosecond();
            if (self.shared.wall_now_ns)() <= modified + (LEASE + CLOCK_GUARD).as_nanos() as i128 {
                return Ok(TakeoverOutcome::NotEligible);
            }
        }
        let mut next = base
            .head()
            .clone()
            .with_next_epoch()?
            .with_next_write_seq()?;
        next.owner = Some(self.shared.identity.clone());
        next.lease_deadline_ms = self.shared.diagnostic_deadline();
        let put = self.shared.update(|control, now| {
            Access::Takeover.check(control.status)?;
            if let Some(floor) = &control.snapshot {
                floor.validate(&self.shared.store, &base)?;
            }
            Ok(Put {
                access: Access::Takeover,
                base,
                next,
                sent_at: now,
                deadline: now + LEASE,
                ambiguous,
            })
        })?;

        match self.write(&put).await {
            Ok(true) => Ok(TakeoverOutcome::Acquired(FencingToken {
                epoch: put.next.epoch(),
            })),
            Ok(false) => Ok(TakeoverOutcome::AppliedButInactive),
            Err(error) => {
                if matches!(error, Error::AuthorityUnknown { .. }) {
                    let base_unresolved = self.shared.update(|control, _| {
                        control.snapshot.as_ref().is_none_or(|observed| {
                            observed.head().write_seq() <= put.base.head().write_seq()
                        })
                    });
                    self.ambiguous_takeover = base_unresolved.then_some(put.base);
                }
                Err(error)
            }
        }
    }

    async fn renew(&self, epoch: u64) {
        if let Ok(Some(put)) = self
            .prepare_renewal(epoch, AuthorityPrecondition::Any, true)
            .await
            && let Err(error) = self.write(&put).await
        {
            common_telemetry::warn!(error; "Authority renewal of epoch {epoch} failed");
        }
    }

    async fn prepare_renewal(
        &self,
        epoch: u64,
        precondition: AuthorityPrecondition,
        scheduled: bool,
    ) -> Result<Option<Put>> {
        let access = Access::Renew(epoch);
        let result = self.prepare_term(access, precondition, !scheduled).await;
        if let Err(error) = &result
            && !matches!(error, Error::AuthorityWriteSeqMismatch { .. })
        {
            // Resignation or shutdown can take over while the preparation GET is in flight.
            let retired = self.shared.update(|control, _| {
                if access.check(control.status).is_ok() {
                    control.retire(epoch);
                    true
                } else {
                    false
                }
            });
            if scheduled && retired {
                common_telemetry::warn!(error; "Authority renewal of epoch {epoch} failed");
            }
        }
        result
    }

    async fn resign(&self, epoch: u64) -> Result<()> {
        let result = match self
            .prepare_term(Access::Resign(epoch), AuthorityPrecondition::Any, false)
            .await
        {
            Ok(Some(put)) => self.write(&put).await.map(|_| ()),
            Ok(None) => Err(rejected("resignation could not be authorized")),
            Err(error) => Err(error),
        };
        self.shared.update(|control, _| control.retire(epoch));
        result
    }

    /// An empty result keeps the FIFO request in hand while we perform a due renewal.
    async fn prepare_term(
        &self,
        access: Access,
        precondition: AuthorityPrecondition,
        defer_for_renewal: bool,
    ) -> Result<Option<Put>> {
        let (epoch, deadline) = self.shared.update(|control, _| {
            access.check(control.status)?;
            control
                .status
                .term()
                .ok_or_else(|| rejected("term is inactive"))
        })?;
        let read = match access {
            Access::Renew(_) => {
                self.read_renewal_head(access, deadline, defer_for_renewal)
                    .await
            }
            _ => self.shared.read(access, Some(deadline)).await,
        };
        let base = match read {
            Ok(base) => base,
            Err(error) => {
                let renew_due = defer_for_renewal
                    && self.shared.update(|control, now| {
                        access.check(control.status).is_ok() && now >= renew_at(deadline)
                    });
                return if renew_due { Ok(None) } else { Err(error) };
            }
        };
        let next = base.head().clone().with_next_write_seq().map(|mut next| {
            if matches!(access, Access::Resign(_) | Access::ConditionalResign(_)) {
                next.owner = None;
                next.lease_deadline_ms = 0;
            } else {
                next.lease_deadline_ms = self.shared.diagnostic_deadline();
            }
            next
        });
        self.shared.update(|control, now| {
            access.check(control.status)?;
            if defer_for_renewal && now >= renew_at(deadline) {
                return Ok(None);
            }
            if now + READBACK_RESERVE >= deadline {
                control.retire(epoch);
                return Err(timeout("write budget"));
            }
            if base.head().epoch() != epoch
                || base.head().owner.as_ref() != Some(&self.shared.identity)
            {
                return Err(rejected("Head is not owned by this term"));
            }
            if let Some(floor) = &control.snapshot {
                floor.validate(&self.shared.store, &base)?;
            }
            precondition.check(base.head())?;
            let next = next?;
            let access = if matches!(access, Access::ConditionalResign(_)) {
                // Close admission and authorize the CAS against this read under one lock.
                control.status = AuthorityStatus::Resigning { epoch, deadline };
                Access::Resign(epoch)
            } else {
                access
            };
            Ok(Some(Put {
                access,
                base,
                next,
                sent_at: now,
                deadline,
                ambiguous: false,
            }))
        })
    }

    async fn read_renewal_head(
        &self,
        access: Access,
        deadline: Instant,
        defer_for_renewal: bool,
    ) -> Result<AuthorityState> {
        let prepare_deadline = deadline - READBACK_RESERVE;
        // A queued renewal yields to the scheduled renewal during read backoff.
        let retry_until = if defer_for_renewal {
            prepare_deadline.min(renew_at(deadline))
        } else {
            prepare_deadline
        };
        let mut backoff = INITIAL_BACKOFF;
        loop {
            let error = match self.shared.read(access, Some(prepare_deadline)).await {
                Ok(head) => return Ok(head),
                Err(error) => error,
            };
            let retryable = matches!(
                error,
                Error::AuthorityTimeout {
                    operation: "read",
                    ..
                }
            ) || error.authority_storage_error().is_some_and(|source| {
                source.is_temporary()
                    && !matches!(
                        source.kind(),
                        ErrorKind::PermissionDenied | ErrorKind::NotFound
                    )
            });
            let now = Instant::now();
            if !retryable || now >= retry_until {
                return Err(error);
            }
            tokio::select! {
                biased;
                _ = self.shared.wait_until(|status| access.check(status).is_err()) => {}
                _ = sleep_until((now + backoff).min(retry_until)) => {}
            }
            self.shared
                .update(|control, _| access.check(control.status))?;
            if Instant::now() >= retry_until {
                return Err(error);
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    async fn write(&self, put: &Put) -> Result<bool> {
        let response = self.send(put).await;
        match response {
            PutResponse::Applied => Ok(self.confirm(put, true)),
            PutResponse::Unsent(error) | PutResponse::Failed(error) => {
                self.retire(put);
                Err(error)
            }
            PutResponse::Conflict(error) => {
                self.retire(put);
                // Re-evaluate the observed version and term without replaying the CAS.
                let _ = self.shared.read(Access::Read, Some(put.deadline)).await;
                Err(error)
            }
            PutResponse::Unknown(error) => {
                self.retire(put);
                if self.resolve(put).await {
                    Ok(self.confirm(put, false))
                } else {
                    Err(error::AuthorityUnknownSnafu.into_error(Box::new(error)))
                }
            }
        }
    }

    async fn send(&self, put: &Put) -> PutResponse {
        let stop = (put.sent_at + PUT_TIMEOUT).min(put.deadline - READBACK_RESERVE);
        let request = async {
            if let Err(error) = Access::Read.check(self.shared.update(|control, _| control.status))
            {
                return PutResponse::Unsent(error);
            }
            if Instant::now() >= stop {
                return PutResponse::Unsent(timeout("write budget"));
            }
            match self.shared.store.replace_head(&put.base, &put.next).await {
                Ok(()) => PutResponse::Applied,
                Err(error) => match error.authority_storage_error() {
                    Some(storage)
                        if matches!(
                            storage.kind(),
                            ErrorKind::ConditionNotMatch
                                | ErrorKind::AlreadyExists
                                | ErrorKind::Conflict
                                | ErrorKind::NotFound
                        ) =>
                    {
                        PutResponse::Conflict(error)
                    }
                    Some(storage)
                        if storage.is_temporary() || storage.kind() == ErrorKind::Unexpected =>
                    {
                        PutResponse::Unknown(error)
                    }
                    _ => PutResponse::Failed(error),
                },
            }
        };
        // A response already available when the task wakes still determines the PUT
        // result. The state transition separately checks the clock under the lock.
        tokio::select! {
            biased;
            result = request => result,
            _ = self.shared.wait_until(|status| status == AuthorityStatus::Closed) => PutResponse::Unknown(closed()),
            _ = sleep_until(stop) => PutResponse::Unknown(timeout("write")),
        }
    }

    async fn resolve(&self, put: &Put) -> bool {
        let mut backoff = INITIAL_BACKOFF;
        while Instant::now() < put.deadline {
            match self.shared.read(Access::Read, Some(put.deadline)).await {
                Ok(state) => {
                    let advanced = self
                        .shared
                        .store
                        .same_version(put.base.metadata(), state.metadata());
                    if matches!(advanced, Ok(false))
                        && state.head().write_seq() >= put.next.write_seq()
                    {
                        // A later or competing write may have overwritten our evidence.
                        // With an older ambiguous takeover, even an exact match is inconclusive.
                        return !put.ambiguous && state.head() == &put.next;
                    }
                }
                Err(
                    Error::AuthorityClosed { .. }
                    | Error::AuthorityMissing { .. }
                    | Error::AuthorityRecoveryRequired { .. },
                ) => return false,
                Err(_) => {}
            }
            let retry_at = (Instant::now() + backoff).min(put.deadline);
            tokio::select! {
                _ = self.shared.wait_until(|status| Access::Read.check(status).is_err()) => return false,
                _ = sleep_until(retry_at) => {}
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
        false
    }

    fn confirm(&self, put: &Put, direct: bool) -> bool {
        self.shared.update(|control, now| {
            let current = control.snapshot.as_ref().map(SnapshotFloor::head);
            let still_current = current
                .is_none_or(|head| head.write_seq() < put.next.write_seq() || head == &put.next);
            if current.is_none_or(|head| head.write_seq() < put.next.write_seq()) {
                control.snapshot = Some(SnapshotFloor::Applied {
                    head: put.next.clone(),
                    base: put.base.clone(),
                });
            }
            let epoch = put.next.epoch();
            if matches!(put.access, Access::Resign(_)) {
                control.retire(epoch);
                return false;
            }
            if still_current && now < put.deadline {
                match put.access {
                    Access::Takeover if control.status == AuthorityStatus::Follower => {
                        control.status = AuthorityStatus::Leader {
                            epoch,
                            deadline: put.deadline,
                        };
                        return true;
                    }
                    Access::Renew(_)
                        if direct && Access::Renew(epoch).check(control.status).is_ok() =>
                    {
                        control.status = AuthorityStatus::Leader {
                            epoch,
                            deadline: put.sent_at + LEASE,
                        };
                    }
                    _ => {}
                }
            }
            false
        })
    }

    fn retire(&self, put: &Put) {
        if let Access::Renew(epoch) | Access::Resign(epoch) = put.access {
            self.shared.update(|control, _| control.retire(epoch));
        }
    }
}

fn renew_at(deadline: Instant) -> Instant {
    deadline - (LEASE - RENEW_INTERVAL)
}

/// The writer obtains this single-use authorization under the state lock and
/// owns it until the PUT outcome is classified. No other code sends a Head PUT.
struct Put {
    access: Access,
    base: AuthorityState,
    next: AuthorityHead,
    sent_at: Instant,
    deadline: Instant,
    ambiguous: bool,
}

enum PutResponse {
    Applied,
    Unsent(Error),
    Conflict(Error),
    Failed(Error),
    Unknown(Error),
}
