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

//! PoC of the memory ledger: accounts with runtime-adjustable limits.
//!
//! This module validates the resize contract of the memory ledger:
//!
//! - An [Account] is a bounded memory budget backed by a semaphore, shared by
//!   any number of handles (the limit lives in shared atomics, so every handle
//!   observes the same `target`/`effective` values).
//! - `set_limit_bytes` takes effect instantly for grow; shrink never revokes
//!   granted memory: idle capacity is harvested immediately, the remaining
//!   deficit is collected by [Account::collect_shrink] as guards release.
//! - The collector is driven by the caller (a controller tick in production,
//!   the test in tests) instead of a spawned task, so this crate stays free of
//!   runtime dependencies and the "single writer of primitive parameters"
//!   principle holds.
//! - [AccountGuard] exposes both faces the ledger needs: async acquisition
//!   with wait/fail policies (the scan-tracker face) and synchronous
//!   `try_grow`/`shrink` (the DataFusion memory-pool face). Both draw from the
//!   same account admission protocol.
//!
//! Resize linearizes when its control critical section ends. Successful
//! acquisition and growth linearize when their permits are added to the usage
//! counter in the same critical section after current-target revalidation.
//! Release linearizes when it subtracts from that counter, immediately before
//! returning the corresponding semaphore permits.

use std::future::{Future, poll_fn};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::task::Poll;

use snafu::ensure;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, watch};

use crate::error::{
    MemoryAcquireTimeoutSnafu, MemoryLimitExceededSnafu, MemorySemaphoreClosedSnafu, Result,
};
use crate::granularity::PermitGranularity;
use crate::policy::OnExhaustedPolicy;

/// Workload category of an account, matching the ledger tree's top level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    Ingest,
    Query,
    Background,
    Cache,
}

/// Max permits the shrink collector acquires per round.
///
/// Bounds head-of-line blocking in the semaphore's FIFO queue to one chunk.
const SHRINK_CHUNK_MAX_PERMITS: u32 = 64;

struct AccountInner {
    name: String,
    category: Category,
    granularity: PermitGranularity,
    semaphore: Arc<Semaphore>,
    /// Desired capacity in permits. Updated instantly by `set_limit_bytes`.
    target_permits: AtomicU32,
    /// Capacity the semaphore currently embodies (available + outstanding).
    /// Converges towards `target_permits` while shrinking.
    effective_permits: AtomicU32,
    /// Serializes limit changes, collector bookkeeping, and grant validation.
    control: Mutex<AccountControl>,
    /// Single-flight flag for the shrink collector.
    collecting: AtomicBool,
    /// Permits granted to guards, excluding collector reservations.
    used_permits: AtomicU32,
    /// Wakes a collector so it can cancel and replan an obsolete chunk.
    resize_tx: watch::Sender<()>,
}

#[derive(Default)]
struct AccountControl {
    unbacked_bytes: u64,
    unbacked_overflowed: bool,
}

impl AccountInner {
    fn bytes_to_permits(&self, bytes: u64) -> u32 {
        self.granularity.bytes_to_permits(bytes)
    }

    fn permits_to_bytes(&self, permits: u32) -> u64 {
        self.granularity.permits_to_bytes(permits)
    }

    fn lock_control(&self) -> MutexGuard<'_, AccountControl> {
        self.control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn target_bytes(&self) -> u64 {
        self.permits_to_bytes(self.target_permits.load(Ordering::Acquire))
    }

    fn validate_and_record_grant(&self, permits: u32, requested_bytes: u64) -> Result<()> {
        let control = self.lock_control();
        let target = self.target_permits.load(Ordering::Acquire);
        let used = self.used_permits.load(Ordering::Acquire);
        let delta = self.permits_to_bytes(permits);
        ensure!(
            permits == 0
                || self
                    .permits_to_bytes(used)
                    .saturating_add(control.unbacked_bytes)
                    .saturating_add(delta)
                    <= self.permits_to_bytes(target),
            MemoryLimitExceededSnafu {
                requested_bytes,
                limit_bytes: self.permits_to_bytes(target),
            }
        );
        self.used_permits.fetch_add(permits, Ordering::AcqRel);
        Ok(())
    }

    fn release_used(&self, permits: u32) {
        let _guard = self.lock_control();
        let previous = self.used_permits.fetch_sub(permits, Ordering::AcqRel);
        debug_assert!(previous >= permits, "account usage counter underflowed");
    }

    fn charge_unbacked(&self, local_bytes: &mut u64, local_overflowed: &mut bool, bytes: u64) {
        let mut control = self.lock_control();
        if *local_overflowed || control.unbacked_overflowed {
            *local_overflowed = true;
            *local_bytes = u64::MAX;
            control.unbacked_overflowed = true;
            control.unbacked_bytes = u64::MAX;
            return;
        }
        match (
            local_bytes.checked_add(bytes),
            control.unbacked_bytes.checked_add(bytes),
        ) {
            (Some(local), Some(global)) => {
                *local_bytes = local;
                control.unbacked_bytes = global;
            }
            _ => {
                *local_overflowed = true;
                *local_bytes = u64::MAX;
                control.unbacked_overflowed = true;
                control.unbacked_bytes = u64::MAX;
            }
        }
    }

    fn poison_unbacked(&self, local_bytes: &mut u64, local_overflowed: &mut bool) {
        let mut control = self.lock_control();
        *local_overflowed = true;
        *local_bytes = u64::MAX;
        control.unbacked_overflowed = true;
        control.unbacked_bytes = u64::MAX;
    }

    fn release_unbacked(&self, bytes: u64) {
        let mut control = self.lock_control();
        if control.unbacked_overflowed {
            return;
        }
        debug_assert!(control.unbacked_bytes >= bytes);
        control.unbacked_bytes -= bytes;
    }
}

/// RAII backstop that clears the shrink collector's single-flight flag.
///
/// [Account::collect_shrink] arms this right after winning the flag, so every
/// exit path releases it — including the future being dropped at an await
/// point (e.g. under `tokio::time::timeout`). The normal convergence path
/// calls [Self::clear] inside the `control` critical section instead, which
/// keeps "converged when the flag is released" atomic; the drop backstop only
/// fires on cancellation or on a closed semaphore.
struct CollectingFlagGuard<'a> {
    collecting: &'a AtomicBool,
    armed: bool,
}

impl CollectingFlagGuard<'_> {
    /// Clears the flag immediately and disarms the drop backstop.
    ///
    /// Call while holding the `control` lock so the clear is atomic with the
    /// convergence check.
    fn clear(&mut self) {
        self.collecting.store(false, Ordering::Release);
        self.armed = false;
    }
}

impl Drop for CollectingFlagGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.collecting.store(false, Ordering::Release);
        }
    }
}

enum CollectorWake {
    Resized,
    Acquired(std::result::Result<OwnedSemaphorePermit, tokio::sync::AcquireError>),
}

/// A bounded memory account with a runtime-adjustable limit.
///
/// Cloning shares the same underlying budget.
#[derive(Clone)]
pub struct Account {
    inner: Arc<AccountInner>,
}

impl Account {
    /// Creates a bounded account. Accounts are always bounded: "unlimited" is
    /// expressed by passing the parent budget as the limit.
    ///
    /// The limit saturates at `granularity.permits_to_bytes(u32::MAX)` (4 TiB
    /// at 1 KB granularity, further capped by `Semaphore::MAX_PERMITS` where
    /// smaller): larger values are stored as that maximum. Admission checks
    /// compare request bytes against the saturated target, so oversized
    /// requests fail instead of being silently clamped.
    pub fn new(
        name: impl Into<String>,
        category: Category,
        limit_bytes: u64,
        granularity: PermitGranularity,
    ) -> Self {
        // Saturates: the conversion clamps to the max permit count.
        let limit_permits = granularity.bytes_to_permits(limit_bytes);
        let (resize_tx, _) = watch::channel(());
        Self {
            inner: Arc::new(AccountInner {
                name: name.into(),
                category,
                granularity,
                semaphore: Arc::new(Semaphore::new(limit_permits as usize)),
                target_permits: AtomicU32::new(limit_permits),
                effective_permits: AtomicU32::new(limit_permits),
                control: Mutex::new(AccountControl::default()),
                collecting: AtomicBool::new(false),
                used_permits: AtomicU32::new(0),
                resize_tx,
            }),
        }
    }

    /// Account name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Workload category.
    pub fn category(&self) -> Category {
        self.inner.category
    }

    /// Permit granularity of this account, for adapter layers that need to
    /// reconcile byte-level remainders against whole-permit accounting.
    pub fn granularity(&self) -> PermitGranularity {
        self.inner.granularity
    }

    /// Desired limit in bytes (set instantly by `set_limit_bytes`).
    pub fn target_limit_bytes(&self) -> u64 {
        let _guard = self.inner.lock_control();
        self.inner.target_bytes()
    }

    /// Capacity the account currently embodies. Equals the target except while
    /// a shrink is converging.
    pub fn effective_limit_bytes(&self) -> u64 {
        let _guard = self.inner.lock_control();
        self.inner
            .permits_to_bytes(self.inner.effective_permits.load(Ordering::Acquire))
    }

    /// Bytes currently charged to guards.
    ///
    /// Resize bookkeeping and collector reservations are excluded. A permit
    /// acquired internally but not yet revalidated is not granted to a guard.
    /// Grant bookkeeping may briefly over-report before the guard is returned.
    /// A release updates this counter immediately before returning its permit;
    /// resize cannot make the value under-report an already successful grant.
    pub fn used_bytes(&self) -> u64 {
        let control = self.inner.lock_control();
        self.inner
            .permits_to_bytes(self.inner.used_permits.load(Ordering::Acquire))
            .saturating_add(control.unbacked_bytes)
    }

    /// Bytes currently backed by semaphore permits.
    pub fn backed_bytes(&self) -> u64 {
        self.inner
            .permits_to_bytes(self.inner.used_permits.load(Ordering::Acquire))
    }

    /// Bytes currently charged without semaphore permits.
    pub fn unbacked_bytes(&self) -> u64 {
        self.inner.lock_control().unbacked_bytes
    }

    /// Adjusts the limit. Returns the remaining shrink deficit in bytes.
    ///
    /// Grow takes effect instantly (waiters wake). Shrink harvests idle
    /// capacity instantly and leaves the remainder to [Self::collect_shrink];
    /// granted memory is never revoked.
    ///
    /// Like [Self::new], the limit saturates at
    /// `granularity.permits_to_bytes(u32::MAX)`: larger values are stored as
    /// that maximum.
    pub fn set_limit_bytes(&self, bytes: u64) -> u64 {
        // Saturates: the conversion clamps to the max permit count.
        let new_target = self.inner.bytes_to_permits(bytes);
        let _guard = self.inner.lock_control();
        let old_target = self.inner.target_permits.load(Ordering::Acquire);
        let effective = self.inner.effective_permits.load(Ordering::Acquire);
        self.inner
            .target_permits
            .store(new_target, Ordering::Release);

        let deficit = if new_target >= effective {
            let delta = new_target - effective;
            if delta > 0 {
                self.inner
                    .effective_permits
                    .store(new_target, Ordering::Release);
                self.inner.semaphore.add_permits(delta as usize);
            }
            0
        } else {
            let deficit = effective - new_target;
            let forgotten = self.inner.semaphore.forget_permits(deficit as usize) as u32;
            let now_effective = effective - forgotten;
            self.inner
                .effective_permits
                .store(now_effective, Ordering::Release);
            now_effective - new_target
        };
        let new_effective = self.inner.effective_permits.load(Ordering::Acquire);
        if new_target != old_target || new_effective != effective {
            self.inner.resize_tx.send_replace(());
        }
        self.inner.permits_to_bytes(deficit)
    }

    /// Drives an in-progress shrink until the account has converged
    /// (`effective <= target`). Never revokes granted memory: capacity is
    /// acquired in chunks through the semaphore's FIFO queue, competing
    /// fairly with normal waiters.
    ///
    /// Concurrency contract:
    /// - Idempotent: calling on a converged account is a cheap no-op.
    /// - Single-flight: while one call collects, concurrent calls return
    ///   immediately without waiting for convergence.
    /// - No stranded deficit: the single-flight flag is cleared in the same
    ///   `control` critical section that confirms convergence, and
    ///   `set_limit_bytes` mutates the target only under that lock — so a
    ///   new deficit is always seen either by the still-running collector or
    ///   by the next call, which then wins the flag.
    /// - Cancellation-safe: if the future is dropped at an await point (e.g.
    ///   under `tokio::time::timeout`), [CollectingFlagGuard] releases the
    ///   flag and the next call resumes collection.
    pub async fn collect_shrink(&self) {
        if self.inner.collecting.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut flag_guard = CollectingFlagGuard {
            collecting: &self.inner.collecting,
            armed: true,
        };
        let mut resize_rx = self.inner.resize_tx.subscribe();

        loop {
            // Harvest idle capacity and size the next chunk. Convergence and
            // the flag clear happen in one critical section: a concurrent
            // shrink of the target can never slip between them and strand
            // its deficit behind a still-set flag.
            let chunk = {
                let _guard = self.inner.lock_control();
                let target = self.inner.target_permits.load(Ordering::Acquire);
                let effective = self.inner.effective_permits.load(Ordering::Acquire);
                if effective <= target {
                    flag_guard.clear();
                    return;
                }
                let deficit = effective - target;
                let forgotten = self.inner.semaphore.forget_permits(deficit as usize) as u32;
                let now_effective = effective - forgotten;
                self.inner
                    .effective_permits
                    .store(now_effective, Ordering::Release);
                if now_effective <= target {
                    flag_guard.clear();
                    return;
                }
                let _ = resize_rx.borrow_and_update();
                (now_effective - target).min(SHRINK_CHUNK_MAX_PERMITS)
            };

            let wake =
                Self::wait_for_chunk_or_resize(self.inner.semaphore.clone(), chunk, &mut resize_rx)
                    .await;
            let mut permit = match wake {
                CollectorWake::Resized => continue,
                CollectorWake::Acquired(Ok(permit)) => permit,
                CollectorWake::Acquired(Err(_)) => return,
            };

            let _guard = self.inner.lock_control();
            let target = self.inner.target_permits.load(Ordering::Acquire);
            let effective = self.inner.effective_permits.load(Ordering::Acquire);
            let needed = effective.saturating_sub(target);
            if needed == 0 {
                drop(permit);
                continue;
            }
            let forget_n = chunk.min(needed);
            if forget_n < chunk {
                // Return the excess before forgetting the rest.
                let excess = permit.split((chunk - forget_n) as usize);
                drop(excess);
            }
            permit.forget();
            self.inner
                .effective_permits
                .store(effective - forget_n, Ordering::Release);
        }
    }

    async fn wait_for_chunk_or_resize(
        semaphore: Arc<Semaphore>,
        chunk: u32,
        resize_rx: &mut watch::Receiver<()>,
    ) -> CollectorWake {
        let mut resized = Box::pin(resize_rx.changed());
        let mut acquired = Box::pin(semaphore.acquire_many_owned(chunk));
        poll_fn(|cx| {
            if resized.as_mut().poll(cx).is_ready() {
                return Poll::Ready(CollectorWake::Resized);
            }
            if let Poll::Ready(result) = acquired.as_mut().poll(cx) {
                return Poll::Ready(CollectorWake::Acquired(result));
            }
            Poll::Pending
        })
        .await
    }

    /// Checks a request against the target limit, comparing in bytes before
    /// any permit-conversion clamping: a request larger than the (possibly
    /// saturated) target must fail loudly instead of being silently clamped
    /// to the maximum permit count.
    fn ensure_within_target(&self, bytes: u64) -> Result<()> {
        let target_bytes = self.target_limit_bytes();
        ensure!(
            bytes <= target_bytes,
            MemoryLimitExceededSnafu {
                requested_bytes: bytes,
                limit_bytes: target_bytes,
            }
        );
        Ok(())
    }

    /// Acquires memory, waiting until enough capacity is available.
    pub async fn acquire(&self, bytes: u64) -> Result<AccountGuard> {
        self.ensure_within_target(bytes)?;
        let permits = self.inner.bytes_to_permits(bytes);
        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| MemorySemaphoreClosedSnafu.build())?;
        self.inner.validate_and_record_grant(permits, bytes)?;
        Ok(AccountGuard {
            inner: self.inner.clone(),
            permit,
            unbacked_bytes: 0,
            unbacked_overflowed: false,
        })
    }

    /// Tries to acquire memory without waiting.
    pub fn try_acquire(&self, bytes: u64) -> Option<AccountGuard> {
        // Compare in bytes, like `ensure_within_target`.
        if bytes > self.target_limit_bytes() {
            return None;
        }
        let permits = self.inner.bytes_to_permits(bytes);
        let permit = match self.inner.semaphore.clone().try_acquire_many_owned(permits) {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => return None,
        };
        self.inner.validate_and_record_grant(permits, bytes).ok()?;
        Some(AccountGuard {
            inner: self.inner.clone(),
            permit,
            unbacked_bytes: 0,
            unbacked_overflowed: false,
        })
    }

    /// Acquires memory according to the given policy.
    pub async fn acquire_with_policy(
        &self,
        bytes: u64,
        policy: OnExhaustedPolicy,
    ) -> Result<AccountGuard> {
        match policy {
            OnExhaustedPolicy::Wait { timeout } => {
                match tokio::time::timeout(timeout, self.acquire(bytes)).await {
                    Ok(result) => result,
                    Err(_elapsed) => MemoryAcquireTimeoutSnafu {
                        requested_bytes: bytes,
                        waited: timeout,
                    }
                    .fail(),
                }
            }
            OnExhaustedPolicy::Fail => self.try_acquire(bytes).ok_or_else(|| {
                MemoryLimitExceededSnafu {
                    requested_bytes: bytes,
                    limit_bytes: self.target_limit_bytes(),
                }
                .build()
            }),
        }
    }
}

/// Guard over granted capacity, usable from both the async (wait/fail policy)
/// and the synchronous (DataFusion pool) face.
pub struct AccountGuard {
    inner: Arc<AccountInner>,
    permit: OwnedSemaphorePermit,
    unbacked_bytes: u64,
    unbacked_overflowed: bool,
}

impl AccountGuard {
    /// Bytes granted to this guard.
    pub fn granted_bytes(&self) -> u64 {
        self.inner
            .permits_to_bytes(self.permit.num_permits() as u32)
    }

    /// Bytes charged to this guard, including unbacked infallible charges.
    pub fn charged_bytes(&self) -> u64 {
        self.granted_bytes().saturating_add(self.unbacked_bytes)
    }

    /// Bytes charged to this guard without semaphore permits.
    pub fn unbacked_bytes(&self) -> u64 {
        self.unbacked_bytes
    }

    /// Whether this guard's infallible charge exceeded representable bytes.
    pub fn unbacked_overflowed(&self) -> bool {
        self.unbacked_overflowed
    }

    fn ensure_growth_within_target(&self, bytes: u64) -> Result<()> {
        let _guard = self.inner.lock_control();
        let target_bytes = self.inner.target_bytes();
        ensure!(
            bytes == 0 || self.charged_bytes().saturating_add(bytes) <= target_bytes,
            MemoryLimitExceededSnafu {
                requested_bytes: bytes,
                limit_bytes: target_bytes,
            }
        );
        Ok(())
    }

    /// Synchronously grows this guard. Returns false if capacity or the
    /// target limit does not allow it. The limit check compares bytes, so a
    /// request beyond the (possibly saturated) target fails instead of being
    /// clamped.
    pub fn try_grow(&mut self, bytes: u64) -> bool {
        if self.ensure_growth_within_target(bytes).is_err() {
            return false;
        }
        let permits = self.inner.bytes_to_permits(bytes);
        let extra = match self.inner.semaphore.clone().try_acquire_many_owned(permits) {
            Ok(extra) => extra,
            Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => return false,
        };
        if self
            .inner
            .validate_and_record_grant(permits, bytes)
            .is_err()
        {
            return false;
        }
        self.permit.merge(extra);
        true
    }

    /// Grows this guard, waiting until capacity is available. The limit
    /// check compares bytes, like [Self::try_grow].
    pub async fn grow(&mut self, bytes: u64) -> Result<()> {
        self.ensure_growth_within_target(bytes)?;
        let permits = self.inner.bytes_to_permits(bytes);
        let extra = self
            .inner
            .semaphore
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| MemorySemaphoreClosedSnafu.build())?;
        self.inner.validate_and_record_grant(permits, bytes)?;
        self.permit.merge(extra);
        Ok(())
    }

    /// Records an infallible charge that could not be backed by permits.
    pub fn charge_unbacked(&mut self, bytes: u64) {
        self.inner.charge_unbacked(
            &mut self.unbacked_bytes,
            &mut self.unbacked_overflowed,
            bytes,
        );
    }

    /// Conservatively marks an unrepresentable infallible charge.
    pub fn poison_unbacked(&mut self) {
        self.inner
            .poison_unbacked(&mut self.unbacked_bytes, &mut self.unbacked_overflowed);
    }

    /// Returns part of the granted capacity, releasing whole permits only:
    /// `bytes` is rounded DOWN to the permit granularity, so a request below
    /// one permit releases nothing and returns 0. Returns the bytes actually
    /// released (also clamped to the granted amount); any sub-permit
    /// remainder stays granted and is the caller's (e.g. a pool adapter's)
    /// responsibility to track.
    pub fn shrink(&mut self, bytes: u64) -> u64 {
        if self.unbacked_overflowed {
            return 0;
        }
        let aligned = bytes / self.inner.granularity.bytes() * self.inner.granularity.bytes();
        let released_unbacked = aligned.min(self.unbacked_bytes);
        if released_unbacked > 0 {
            self.inner.release_unbacked(released_unbacked);
            self.unbacked_bytes -= released_unbacked;
        }
        let remaining = aligned - released_unbacked;
        let whole_permits = remaining / self.inner.granularity.bytes();
        let permits = whole_permits.min(self.permit.num_permits() as u64) as u32;
        if permits == 0 {
            return released_unbacked;
        }
        match self.permit.split(permits as usize) {
            Some(returned) => {
                self.inner.release_used(permits);
                drop(returned);
                released_unbacked + self.inner.permits_to_bytes(permits)
            }
            None => {
                // Unreachable: `permits` is clamped to `num_permits` above,
                // and `split` only refuses requests beyond the held amount.
                debug_assert!(false, "split refused a request clamped to num_permits");
                0
            }
        }
    }
}

impl Drop for AccountGuard {
    fn drop(&mut self) {
        self.inner.release_unbacked(self.unbacked_bytes);
        self.inner.release_used(self.permit.num_permits() as u32);
    }
}

/// Point-in-time view of an account.
///
/// Fields are sampled independently, so concurrent grants or resize may yield
/// values that did not all coexist at one instant.
#[derive(Debug, Clone)]
pub struct AccountSnapshot {
    pub name: String,
    pub category: Category,
    pub target_limit_bytes: u64,
    pub effective_limit_bytes: u64,
    pub used_bytes: u64,
}

/// Registry of accounts. PoC scope: flat registry plus on-demand aggregation;
/// the category tree interior is observation-only by design.
pub struct MemoryLedger {
    root_budget_bytes: u64,
    accounts: RwLock<Vec<Account>>,
}

impl MemoryLedger {
    pub fn new(root_budget_bytes: u64) -> Self {
        Self {
            root_budget_bytes,
            accounts: RwLock::new(Vec::new()),
        }
    }

    pub fn root_budget_bytes(&self) -> u64 {
        self.root_budget_bytes
    }

    /// Registers a bounded account.
    pub fn register(
        &self,
        name: impl Into<String>,
        category: Category,
        limit_bytes: u64,
        granularity: PermitGranularity,
    ) -> Account {
        let account = Account::new(name, category, limit_bytes, granularity);
        self.accounts.write().unwrap().push(account.clone());
        account
    }

    /// Sum of bytes charged across all accounts.
    pub fn total_used_bytes(&self) -> u64 {
        self.accounts
            .read()
            .unwrap()
            .iter()
            .map(|a| a.used_bytes())
            .fold(0, u64::saturating_add)
    }

    /// The unaccounted gap between an externally observed RSS and the ledger.
    pub fn unaccounted_bytes(&self, rss_bytes: u64) -> u64 {
        rss_bytes.saturating_sub(self.total_used_bytes())
    }

    /// Snapshot of all accounts.
    pub fn snapshot(&self) -> Vec<AccountSnapshot> {
        self.accounts
            .read()
            .unwrap()
            .iter()
            .map(|a| AccountSnapshot {
                name: a.name().to_string(),
                category: a.category(),
                target_limit_bytes: a.target_limit_bytes(),
                effective_limit_bytes: a.effective_limit_bytes(),
                used_bytes: a.used_bytes(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Poll;
    use std::time::Duration;

    use super::*;

    const KB: u64 = 1024;

    fn account(limit_kb: u64) -> Account {
        Account::new(
            "test",
            Category::Query,
            limit_kb * KB,
            PermitGranularity::Kilobyte,
        )
    }

    async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
        poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    #[tokio::test]
    async fn grow_wakes_waiter_instantly() {
        let acc = account(4);
        let held = acc.acquire(4 * KB).await.unwrap();

        let mut waiter = Box::pin(acc.acquire(2 * KB));
        assert_pending(waiter.as_mut()).await;

        assert_eq!(acc.set_limit_bytes(8 * KB), 0);
        let got = waiter.await.unwrap();
        assert_eq!(got.granted_bytes(), 2 * KB);
        assert_eq!(acc.effective_limit_bytes(), 8 * KB);
        assert_eq!(acc.used_bytes(), 6 * KB);
        drop(held);
    }

    #[tokio::test]
    async fn shrink_idle_capacity_is_instant() {
        let acc = account(8);
        let deficit = acc.set_limit_bytes(4 * KB);
        assert_eq!(deficit, 0);
        assert_eq!(acc.effective_limit_bytes(), 4 * KB);
        // Oversized requests fail fast against the new target.
        assert!(acc.acquire(5 * KB).await.is_err());
        assert!(acc.try_acquire(5 * KB).is_none());
        assert!(acc.try_acquire(4 * KB).is_some());
    }

    #[tokio::test]
    async fn shrink_is_non_preemptive_and_converges() {
        let acc = account(10);
        let mut held = acc.acquire(8 * KB).await.unwrap();

        // Shrink to 4: 2 idle permits harvested instantly, 4 in deficit.
        let deficit = acc.set_limit_bytes(4 * KB);
        assert_eq!(deficit, 4 * KB);
        // Granted memory is untouched.
        assert_eq!(held.granted_bytes(), 8 * KB);
        assert_eq!(acc.effective_limit_bytes(), 8 * KB);

        let mut collector = Box::pin(acc.collect_shrink());
        assert_pending(collector.as_mut()).await;
        // Still not converged: nothing released yet.
        assert_eq!(acc.effective_limit_bytes(), 8 * KB);

        // Release 4 KB; the collector should absorb it.
        assert_eq!(held.shrink(4 * KB), 4 * KB);
        collector.await;
        assert_eq!(acc.effective_limit_bytes(), 4 * KB);
        assert_eq!(acc.target_limit_bytes(), 4 * KB);
        assert_eq!(acc.used_bytes(), 4 * KB);
        drop(held);
        assert_eq!(acc.used_bytes(), 0);
    }

    #[tokio::test]
    async fn shrink_collector_does_not_jump_the_queue() {
        let acc = account(4);
        let held = acc.acquire(4 * KB).await.unwrap();

        // A waiter queues before the shrink starts.
        let mut waiter = Box::pin(acc.acquire(2 * KB));
        assert_pending(waiter.as_mut()).await;

        // Shrink to 2 while 4 are held; collector queues after the waiter.
        assert_eq!(acc.set_limit_bytes(2 * KB), 2 * KB);
        let mut collector = Box::pin(acc.collect_shrink());
        assert_pending(collector.as_mut()).await;

        // Release everything: FIFO serves the earlier waiter first, then the
        // collector converges on what remains.
        drop(held);
        let (got, ()) = tokio::join!(waiter.as_mut(), collector.as_mut());
        let got = got.unwrap();
        assert_eq!(got.granted_bytes(), 2 * KB);
        assert_eq!(acc.effective_limit_bytes(), 2 * KB);
        // The account is exactly full: the waiter holds the entire capacity.
        assert_eq!(acc.used_bytes(), 2 * KB);
        assert!(acc.try_acquire(KB).is_none());
        drop(got);
    }

    #[tokio::test]
    async fn retarget_cancels_obsolete_collector_chunk() {
        let acc = account(8);
        let held = acc.acquire(8 * KB).await.unwrap();
        assert_eq!(acc.set_limit_bytes(4 * KB), 4 * KB);

        let mut collector = Box::pin(acc.collect_shrink());
        assert_pending(collector.as_mut()).await;
        let mut waiter = Box::pin(acc.acquire(2 * KB));
        assert_pending(waiter.as_mut()).await;

        // Only two permits are added. The obsolete four-permit collector
        // request must leave the queue so the smaller waiter can proceed.
        assert_eq!(acc.set_limit_bytes(10 * KB), 0);
        let ((), granted) = tokio::join!(collector.as_mut(), waiter.as_mut());
        let granted = granted.unwrap();
        assert_eq!(granted.granted_bytes(), 2 * KB);
        assert_eq!(acc.effective_limit_bytes(), 10 * KB);
        assert_eq!(acc.used_bytes(), 10 * KB);
        drop((held, granted));
    }

    #[tokio::test]
    async fn repeated_limit_keeps_collector_queue_position() {
        let acc = account(8);
        let mut held = acc.acquire(8 * KB).await.unwrap();
        assert_eq!(acc.set_limit_bytes(4 * KB), 4 * KB);

        let mut collector = Box::pin(acc.collect_shrink());
        assert_pending(collector.as_mut()).await;
        let mut waiter = Box::pin(acc.acquire(KB));
        assert_pending(waiter.as_mut()).await;

        // With no target or effective-capacity change, the queued collector
        // keeps its FIFO position ahead of the waiter.
        assert_eq!(acc.set_limit_bytes(4 * KB), 4 * KB);
        assert_eq!(held.shrink(4 * KB), 4 * KB);
        collector.await;
        assert_pending(waiter.as_mut()).await;
        assert_eq!(acc.effective_limit_bytes(), 4 * KB);

        drop(held);
        assert_eq!(waiter.await.unwrap().granted_bytes(), KB);
    }

    #[tokio::test]
    async fn pending_acquire_is_revalidated_after_shrink() {
        let acc = account(8);
        let held = acc.acquire(8 * KB).await.unwrap();
        let mut waiter = Box::pin(acc.acquire(6 * KB));
        assert_pending(waiter.as_mut()).await;

        assert_eq!(acc.set_limit_bytes(4 * KB), 4 * KB);
        drop(held);
        assert!(waiter.await.is_err());
        assert_eq!(acc.used_bytes(), 0);
        acc.collect_shrink().await;
        assert_eq!(acc.effective_limit_bytes(), 4 * KB);
    }

    #[tokio::test]
    async fn pending_grow_is_revalidated_after_shrink() {
        let acc = account(8);
        let mut growing = acc.acquire(4 * KB).await.unwrap();
        let held = acc.acquire(4 * KB).await.unwrap();
        let mut grow = Box::pin(growing.grow(4 * KB));
        assert_pending(grow.as_mut()).await;

        assert_eq!(acc.set_limit_bytes(6 * KB), 2 * KB);
        drop(held);
        assert!(grow.await.is_err());
        assert_eq!(growing.granted_bytes(), 4 * KB);
        assert_eq!(acc.used_bytes(), 4 * KB);
    }

    #[tokio::test]
    async fn waiter_cannot_replace_usage_above_new_target() {
        let acc = account(8);
        let mut first = acc.acquire(6 * KB).await.unwrap();
        let second = acc.acquire(2 * KB).await.unwrap();
        let mut waiter = Box::pin(acc.acquire(2 * KB));
        assert_pending(waiter.as_mut()).await;

        assert_eq!(acc.set_limit_bytes(4 * KB), 4 * KB);
        drop(second);
        assert!(waiter.await.is_err());
        assert_eq!(acc.used_bytes(), 6 * KB);
        assert_eq!(first.shrink(2 * KB), 2 * KB);
        acc.collect_shrink().await;
        assert_eq!(acc.used_bytes(), 4 * KB);
    }

    #[tokio::test]
    async fn used_bytes_is_independent_of_resize_bookkeeping() {
        let acc = account(8);
        let held = acc.acquire(6 * KB).await.unwrap();

        for _ in 0..100 {
            acc.set_limit_bytes(4 * KB);
            assert_eq!(acc.used_bytes(), 6 * KB);
            acc.set_limit_bytes(12 * KB);
            assert_eq!(acc.used_bytes(), 6 * KB);
        }

        drop(held);
        assert_eq!(acc.used_bytes(), 0);
    }

    #[tokio::test]
    async fn two_faces_share_one_budget() {
        let acc = account(10);

        // Async face holds 6.
        let async_guard = acc
            .acquire_with_policy(6 * KB, OnExhaustedPolicy::Fail)
            .await
            .unwrap();

        // Sync face grows to exactly the remainder.
        let mut sync_guard = acc.try_acquire(0).unwrap();
        assert!(sync_guard.try_grow(4 * KB));
        assert_eq!(acc.used_bytes(), 10 * KB);
        // One budget: nothing left for either face.
        assert!(!sync_guard.try_grow(KB));
        assert!(acc.try_acquire(KB).is_none());

        // Async face releases; the sync face can claim it.
        drop(async_guard);
        assert!(sync_guard.try_grow(6 * KB));
        assert_eq!(acc.used_bytes(), 10 * KB);

        // Partial shrink returns capacity to the shared budget. Sub-permit
        // amounts round down: the remainder stays granted.
        assert_eq!(sync_guard.shrink(KB - 1), 0);
        assert_eq!(sync_guard.shrink(3 * KB + 512), 3 * KB);
        assert_eq!(acc.used_bytes(), 7 * KB);
        assert!(acc.try_acquire(3 * KB).is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_storm_keeps_invariants() {
        let acc = account(12);
        let mut tasks = Vec::new();
        for i in 0..8u64 {
            let acc = acc.clone();
            tasks.push(tokio::spawn(async move {
                for j in 0..200u64 {
                    let bytes = ((i + j) % 3 + 1) * KB;
                    let policy = OnExhaustedPolicy::Wait {
                        timeout: Duration::from_millis(500),
                    };
                    if let Ok(guard) = acc.acquire_with_policy(bytes, policy).await {
                        tokio::task::yield_now().await;
                        drop(guard);
                    }
                }
            }));
        }

        // Storm of limit changes while the workload runs.
        for round in 0..40u64 {
            let target = if round % 2 == 0 { 6 } else { 12 };
            acc.set_limit_bytes(target * KB);
            acc.collect_shrink().await;
            // This task is the only `collect_shrink` caller, so the call
            // above won the single-flight flag and must have converged.
            assert_eq!(acc.effective_limit_bytes(), target * KB);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        for task in tasks {
            task.await.unwrap();
        }
        acc.set_limit_bytes(6 * KB);
        acc.collect_shrink().await;
        assert_eq!(acc.effective_limit_bytes(), 6 * KB);
        assert_eq!(acc.used_bytes(), 0);
    }

    #[tokio::test]
    async fn concurrent_collect_shrink_single_flight_converges() {
        let acc = account(8);
        let mut held = acc.acquire(8 * KB).await.unwrap();
        assert_eq!(acc.set_limit_bytes(4 * KB), 4 * KB);

        // The collector blocks on its first chunk: all capacity is granted.
        let mut collector = Box::pin(acc.collect_shrink());
        assert_pending(collector.as_mut()).await;

        // A concurrent call early-returns (single-flight) without waiting
        // for convergence.
        acc.collect_shrink().await;
        assert_eq!(acc.effective_limit_bytes(), 8 * KB);

        // Retarget below the in-flight target: the running collector must
        // observe the new target and converge to it.
        assert_eq!(acc.set_limit_bytes(2 * KB), 6 * KB);
        assert_eq!(held.shrink(6 * KB), 6 * KB);
        collector.await;
        assert_eq!(acc.effective_limit_bytes(), 2 * KB);
        assert_eq!(acc.used_bytes(), 2 * KB);

        // The flag was released on convergence: a fresh call wins it and
        // collects a new deficit instead of early-returning.
        assert_eq!(acc.set_limit_bytes(KB), KB);
        assert_eq!(held.shrink(KB), KB);
        acc.collect_shrink().await;
        assert_eq!(acc.effective_limit_bytes(), KB);
        assert_eq!(acc.used_bytes(), KB);
        drop(held);
        assert_eq!(acc.used_bytes(), 0);
    }

    #[tokio::test]
    async fn cancelled_collect_shrink_releases_single_flight_flag() {
        let acc = account(8);
        let held = acc.acquire(8 * KB).await.unwrap();
        assert_eq!(acc.set_limit_bytes(4 * KB), 4 * KB);

        let mut collector = Box::pin(acc.collect_shrink());
        assert_pending(collector.as_mut()).await;
        drop(collector);
        assert_eq!(acc.effective_limit_bytes(), 8 * KB);

        // The drop backstop released the flag: a later call must win it and
        // converge once capacity frees up.
        drop(held);
        acc.collect_shrink().await;
        assert_eq!(acc.effective_limit_bytes(), 4 * KB);
        assert_eq!(acc.used_bytes(), 0);
    }

    #[tokio::test]
    async fn unbacked_charge_is_visible_and_released_with_its_guard() {
        let ledger = MemoryLedger::new(16 * KB);
        let acc = ledger.register(
            "query",
            Category::Query,
            4 * KB,
            PermitGranularity::Kilobyte,
        );
        let mut guard = acc.acquire(2 * KB).await.unwrap();
        guard.charge_unbacked(3 * KB);

        assert_eq!(guard.granted_bytes(), 2 * KB);
        assert_eq!(guard.unbacked_bytes(), 3 * KB);
        assert_eq!(guard.charged_bytes(), 5 * KB);
        assert_eq!(acc.backed_bytes(), 2 * KB);
        assert_eq!(acc.unbacked_bytes(), 3 * KB);
        assert_eq!(acc.used_bytes(), 5 * KB);
        assert_eq!(ledger.total_used_bytes(), 5 * KB);
        assert_eq!(ledger.snapshot()[0].used_bytes, 5 * KB);
        assert_eq!(ledger.unaccounted_bytes(8 * KB), 3 * KB);

        drop(guard);
        assert_eq!(acc.backed_bytes(), 0);
        assert_eq!(acc.unbacked_bytes(), 0);
        assert_eq!(ledger.total_used_bytes(), 0);
    }

    #[test]
    fn shrink_releases_only_its_own_debt_before_backed_bytes() {
        let acc = account(8);
        let mut first = acc.try_acquire(2 * KB).unwrap();
        let mut second = acc.try_acquire(KB).unwrap();
        first.charge_unbacked(2 * KB);
        second.charge_unbacked(3 * KB);

        assert_eq!(first.shrink(3 * KB), 3 * KB);
        assert_eq!(first.unbacked_bytes(), 0);
        assert_eq!(first.granted_bytes(), KB);
        assert_eq!(second.unbacked_bytes(), 3 * KB);
        assert_eq!(acc.backed_bytes(), 2 * KB);
        assert_eq!(acc.unbacked_bytes(), 3 * KB);
        assert_eq!(acc.used_bytes(), 5 * KB);
    }

    #[test]
    fn sub_permit_shrink_does_not_release_debt() {
        let acc = account(4);
        let mut guard = acc.try_acquire(KB).unwrap();
        guard.charge_unbacked(KB);

        assert_eq!(guard.shrink(512), 0);
        assert_eq!(guard.granted_bytes(), KB);
        assert_eq!(guard.unbacked_bytes(), KB);
        assert_eq!(acc.used_bytes(), 2 * KB);
    }

    #[test]
    fn unbacked_overflow_fails_closed() {
        let acc = account(4);
        let mut guard = acc.try_acquire(0).unwrap();
        guard.charge_unbacked(u64::MAX);
        guard.charge_unbacked(1);

        assert_eq!(guard.unbacked_bytes(), u64::MAX);
        assert_eq!(acc.unbacked_bytes(), u64::MAX);
        assert_eq!(acc.used_bytes(), u64::MAX);
        assert_eq!(guard.shrink(u64::MAX), 0);
        drop(guard);
        assert_eq!(acc.unbacked_bytes(), u64::MAX);
        assert!(acc.try_acquire(KB).is_none());
    }

    #[test]
    fn fallible_admission_is_all_or_nothing_with_existing_debt() {
        let acc = account(4);
        let mut debtor = acc.try_acquire(0).unwrap();
        debtor.charge_unbacked(2 * KB);

        assert!(acc.try_acquire(3 * KB).is_none());
        assert_eq!(acc.backed_bytes(), 0);
        assert_eq!(acc.unbacked_bytes(), 2 * KB);

        let mut backed = acc.try_acquire(2 * KB).unwrap();
        assert!(!backed.try_grow(KB));
        assert_eq!(backed.granted_bytes(), 2 * KB);
        assert_eq!(acc.used_bytes(), 4 * KB);
        assert!(acc.try_acquire(0).is_some());
    }

    #[tokio::test]
    async fn pending_grant_revalidates_against_unbacked_charge() {
        let acc = account(4);
        let held = acc.acquire(4 * KB).await.unwrap();
        let mut waiter = Box::pin(acc.acquire(KB));
        assert_pending(waiter.as_mut()).await;

        let mut debtor = acc.try_acquire(0).unwrap();
        debtor.charge_unbacked(4 * KB);
        drop(held);

        assert!(waiter.await.is_err());
        assert_eq!(acc.backed_bytes(), 0);
        assert_eq!(acc.unbacked_bytes(), 4 * KB);
        assert_eq!(acc.used_bytes(), 4 * KB);
    }

    #[tokio::test]
    async fn resize_below_accounted_usage_blocks_new_grants() {
        let acc = account(6);
        let mut held = acc.acquire(4 * KB).await.unwrap();
        held.charge_unbacked(2 * KB);

        assert_eq!(acc.set_limit_bytes(3 * KB), KB);
        assert_eq!(acc.used_bytes(), 6 * KB);
        assert_eq!(acc.effective_limit_bytes(), 4 * KB);
        assert!(acc.try_acquire(KB).is_none());

        let mut collector = Box::pin(acc.collect_shrink());
        assert_pending(collector.as_mut()).await;
        assert_eq!(held.shrink(3 * KB), 3 * KB);
        collector.await;
        assert_eq!(held.unbacked_bytes(), 0);
        assert_eq!(held.granted_bytes(), 3 * KB);
        assert_eq!(acc.effective_limit_bytes(), 3 * KB);
        assert_eq!(acc.used_bytes(), 3 * KB);
    }

    #[tokio::test]
    async fn oversized_request_fails_instead_of_clamping() {
        // The limit saturates at u32::MAX permits (4 TiB at KB granularity).
        let acc = Account::new(
            "sat",
            Category::Query,
            u64::MAX,
            PermitGranularity::Kilobyte,
        );
        assert_eq!(acc.granularity(), PermitGranularity::Kilobyte);
        let max_bytes = PermitGranularity::Kilobyte.permits_to_bytes(u32::MAX);
        assert_eq!(acc.target_limit_bytes(), max_bytes);

        // Requests beyond the saturated target fail loudly instead of being
        // clamped to the maximum permit count by the byte conversion.
        assert!(acc.acquire(max_bytes + KB).await.is_err());
        assert!(acc.try_acquire(max_bytes + KB).is_none());

        let mut guard = acc.acquire(KB).await.unwrap();
        assert!(!guard.try_grow(max_bytes));
        assert!(guard.grow(max_bytes).await.is_err());
    }

    #[tokio::test]
    async fn oversized_grow_fails_instead_of_clamping() {
        let acc = Account::new(
            "sat",
            Category::Query,
            u64::MAX,
            PermitGranularity::Kilobyte,
        );
        let mut guard = acc.try_acquire(0).unwrap();

        assert!(!guard.try_grow(u64::MAX));
        assert!(guard.grow(u64::MAX).await.is_err());
        assert_eq!(guard.granted_bytes(), 0);
        assert_eq!(acc.used_bytes(), 0);
    }

    #[tokio::test]
    async fn ledger_snapshot_aggregates() {
        let ledger = MemoryLedger::new(100 * KB);
        let a = ledger.register(
            "query/engine",
            Category::Query,
            50 * KB,
            PermitGranularity::Kilobyte,
        );
        let b = ledger.register(
            "ingest/request_bytes",
            Category::Ingest,
            20 * KB,
            PermitGranularity::Kilobyte,
        );
        let _g1 = a.acquire(10 * KB).await.unwrap();
        let _g2 = b.acquire(5 * KB).await.unwrap();

        assert_eq!(ledger.total_used_bytes(), 15 * KB);
        assert_eq!(ledger.unaccounted_bytes(40 * KB), 25 * KB);
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].used_bytes, 10 * KB);
        assert_eq!(snapshot[1].used_bytes, 5 * KB);
    }

    /// Micro-benchmark of the sync face vs a raw atomic counter (stand-in for
    /// `GreedyMemoryPool`'s accounting). Run with:
    /// `cargo nextest run -p common-memory-manager bench_sync_face --run-ignored all`
    #[test]
    #[ignore]
    #[allow(clippy::print_stdout)]
    fn bench_sync_face_throughput() {
        use std::sync::atomic::AtomicU64;
        use std::time::Instant;

        const THREADS: usize = 4;
        const ITERS: u64 = 200_000;

        let acc = Account::new(
            "bench",
            Category::Query,
            1 << 30,
            PermitGranularity::Kilobyte,
        );
        let start = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let acc = acc.clone();
                s.spawn(move || {
                    let mut guard = acc.try_acquire(0).unwrap();
                    for _ in 0..ITERS {
                        assert!(guard.try_grow(KB));
                        guard.shrink(KB);
                    }
                });
            }
        });
        let ledger_ops = (THREADS as u64 * ITERS * 2) as f64 / start.elapsed().as_secs_f64();

        let counter = AtomicU64::new(0);
        let limit = 1u64 << 30;
        let start = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let counter = &counter;
                s.spawn(move || {
                    for _ in 0..ITERS {
                        let mut ok = false;
                        while !ok {
                            let cur = counter.load(Ordering::Relaxed);
                            if cur + KB > limit {
                                break;
                            }
                            ok = counter
                                .compare_exchange(
                                    cur,
                                    cur + KB,
                                    Ordering::Relaxed,
                                    Ordering::Relaxed,
                                )
                                .is_ok();
                        }
                        counter.fetch_sub(KB, Ordering::Relaxed);
                    }
                });
            }
        });
        let atomic_ops = (THREADS as u64 * ITERS * 2) as f64 / start.elapsed().as_secs_f64();

        println!(
            "sync face: {:.2}M ops/s, raw atomic: {:.2}M ops/s, ratio: {:.2}x",
            ledger_ops / 1e6,
            atomic_ops / 1e6,
            atomic_ops / ledger_ops
        );
    }
}
