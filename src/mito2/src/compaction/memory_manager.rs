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

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use common_telemetry::debug;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, Semaphore, TryAcquireError};

use crate::metrics::{
    COMPACTION_MEMORY_IN_USE, COMPACTION_MEMORY_LIMIT, COMPACTION_MEMORY_REJECTED,
};

/// Minimum bytes controlled by one semaphore permit.
const PERMIT_GRANULARITY_BYTES: u64 = 1 << 20; // 1 MB

/// Defines how the scheduler reacts when compaction cannot acquire enough memory.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnExhaustedPolicy {
    /// Wait until enough memory is released.
    #[default]
    Wait,
    /// Skip the compaction silently.
    Skip,
    /// Fail the compaction request immediately.
    Fail,
}

/// Global memory manager for compaction tasks.
#[derive(Clone)]
pub struct CompactionMemoryManager {
    inner: Option<Arc<Inner>>,
}

struct Inner {
    semaphore: Semaphore,
    limit_permits: u32,
    notify: Notify,
}

impl CompactionMemoryManager {
    /// Creates a new memory manager with the given limit in bytes.
    /// `limit_bytes = 0` disables the limit.
    pub fn new(limit_bytes: u64) -> Self {
        if limit_bytes == 0 {
            COMPACTION_MEMORY_LIMIT.set(0);
            return Self { inner: None };
        }

        let limit_permits = bytes_to_permits(limit_bytes);
        let limit_aligned_bytes = permits_to_bytes(limit_permits);
        COMPACTION_MEMORY_LIMIT.set(limit_aligned_bytes as i64);

        Self {
            inner: Some(Arc::new(Inner {
                semaphore: Semaphore::new(limit_permits as usize),
                limit_permits,
                notify: Notify::new(),
            })),
        }
    }

    /// Returns the configured limit in bytes (0 if unlimited).
    pub fn limit_bytes(&self) -> u64 {
        self.inner
            .as_ref()
            .map(|inner| permits_to_bytes(inner.limit_permits))
            .unwrap_or(0)
    }

    /// Returns currently used bytes.
    pub fn used_bytes(&self) -> u64 {
        self.inner
            .as_ref()
            .map(|inner| permits_to_bytes(inner.used_permits()))
            .unwrap_or(0)
    }

    /// Returns available bytes.
    pub fn available_bytes(&self) -> u64 {
        self.inner
            .as_ref()
            .map(|inner| permits_to_bytes(inner.available_permits_clamped()))
            .unwrap_or(0)
    }

    /// Tries to acquire memory. Returns Some(guard) on success, None if insufficient.
    pub fn try_acquire(&self, bytes: u64) -> Option<CompactionMemoryGuard> {
        match &self.inner {
            None => Some(CompactionMemoryGuard::unlimited()),
            Some(inner) => {
                let permits = bytes_to_permits(bytes);
                if permits == 0 {
                    return Some(CompactionMemoryGuard::unlimited());
                }

                match inner.semaphore.try_acquire_many(permits) {
                    Ok(permit) => {
                        permit.forget();
                        inner.on_acquire();
                        Some(CompactionMemoryGuard::limited(inner.clone(), permits))
                    }
                    Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => {
                        COMPACTION_MEMORY_REJECTED
                            .with_label_values(&["try_acquire"])
                            .inc();
                        None
                    }
                }
            }
        }
    }

    /// Waits until the specified amount of memory becomes available.
    ///
    /// Note: This method does not reserve the memory. After this method returns,
    /// you should call `try_acquire` to attempt to reserve the memory. However,
    /// there is no guarantee the memory will still be available at that point,
    /// as another task may acquire it concurrently.
    pub async fn wait_for_available(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let inner = inner.clone();
        let needed_permits = bytes_to_permits(bytes);
        loop {
            // Take a notified handle first to avoid missing notifications between
            // the availability check and awaiting.
            let notified = inner.notify.notified();
            if inner.available_permits_clamped() >= needed_permits {
                return;
            }
            notified.await;
        }
    }
}

impl Inner {
    fn on_acquire(&self) {
        self.update_in_use_metric();
    }

    fn release(&self, permits: u32) {
        if permits == 0 {
            return;
        }
        self.semaphore.add_permits(permits as usize);
        self.update_in_use_metric();
        self.notify.notify_waiters();
    }

    fn used_permits(&self) -> u32 {
        self.limit_permits
            .saturating_sub(self.available_permits_clamped())
    }

    fn available_permits_clamped(&self) -> u32 {
        // Clamp to limit_permits to ensure we never report more available permits
        // than our configured limit, even if semaphore state becomes inconsistent.
        self.semaphore
            .available_permits()
            .min(self.limit_permits as usize) as u32
    }

    fn update_in_use_metric(&self) {
        let bytes = permits_to_bytes(self.used_permits());
        COMPACTION_MEMORY_IN_USE.set(bytes as i64);
    }
}

/// Guard representing a slice of reserved compaction memory.
///
/// NOTE: `AdditionalMemoryGuard` must not outlive this guard. Dropping this guard
/// will release both base and additional permits; any surviving additional guard
/// would then observe zero permits and run without actually holding memory.
pub struct CompactionMemoryGuard {
    state: GuardState,
}

enum GuardState {
    Unlimited,
    Limited {
        inner: Arc<Inner>,
        /// Base quota allocated at task startup.
        base_permits: u32,
        /// Additional memory allocated during task execution.
        additional_permits: Arc<AtomicI64>,
    },
}

impl CompactionMemoryGuard {
    fn unlimited() -> Self {
        Self {
            state: GuardState::Unlimited,
        }
    }

    fn limited(inner: Arc<Inner>, base_permits: u32) -> Self {
        Self {
            state: GuardState::Limited {
                inner,
                base_permits,
                additional_permits: Arc::new(AtomicI64::new(0)),
            },
        }
    }

    /// Returns granted base quota in bytes.
    pub fn granted_bytes(&self) -> u64 {
        match &self.state {
            GuardState::Unlimited => 0,
            GuardState::Limited { base_permits, .. } => permits_to_bytes(*base_permits),
        }
    }

    /// Returns total memory usage (base + additional) in bytes.
    pub fn total_bytes(&self) -> u64 {
        match &self.state {
            GuardState::Unlimited => 0,
            GuardState::Limited {
                base_permits,
                additional_permits,
                ..
            } => {
                let additional = additional_permits.load(Ordering::Relaxed).max(0) as u64;
                permits_to_bytes(*base_permits) + permits_to_bytes(additional as u32)
            }
        }
    }

    /// Returns additional memory usage in bytes.
    pub fn additional_bytes(&self) -> u64 {
        match &self.state {
            GuardState::Unlimited => 0,
            GuardState::Limited {
                additional_permits, ..
            } => {
                let additional = additional_permits.load(Ordering::Relaxed).max(0) as u32;
                permits_to_bytes(additional)
            }
        }
    }

    /// Tries to allocate additional memory during task execution.
    ///
    /// Returns `Some(guard)` on success, `None` if the allocation would exceed the global limit.
    /// The returned guard will automatically release the memory when dropped.
    ///
    /// # Behavior
    /// - Running tasks can request additional memory beyond their base quota
    /// - If total memory (all tasks) would exceed limit, returns `None` immediately
    /// - The task should gracefully fail when this returns `None`
    /// - Memory is automatically released when the returned guard is dropped
    pub fn request_additional(&self, bytes: u64) -> Option<AdditionalMemoryGuard> {
        match &self.state {
            GuardState::Unlimited => Some(AdditionalMemoryGuard {
                state: AdditionalGuardState::Unlimited,
            }),
            GuardState::Limited {
                inner,
                additional_permits,
                ..
            } => {
                let permits = bytes_to_permits(bytes);
                if permits == 0 {
                    return Some(AdditionalMemoryGuard {
                        state: AdditionalGuardState::Unlimited,
                    });
                }

                // Try to acquire from global semaphore
                match inner.semaphore.try_acquire_many(permits) {
                    Ok(permit) => {
                        permit.forget();

                        // Record in additional_permits
                        additional_permits.fetch_add(permits as i64, Ordering::Relaxed);

                        // Update metrics
                        inner.update_in_use_metric();

                        debug!(
                            "Allocated additional {} bytes ({} permits), total additional: {}",
                            bytes,
                            permits,
                            permits_to_bytes(
                                additional_permits.load(Ordering::Relaxed).max(0) as u32
                            )
                        );

                        Some(AdditionalMemoryGuard {
                            state: AdditionalGuardState::Limited {
                                inner: inner.clone(),
                                permits,
                                parent_additional: additional_permits.clone(),
                            },
                        })
                    }
                    Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => {
                        COMPACTION_MEMORY_REJECTED
                            .with_label_values(&["request_additional"])
                            .inc();
                        None
                    }
                }
            }
        }
    }
}

impl Drop for CompactionMemoryGuard {
    fn drop(&mut self) {
        if let GuardState::Limited {
            inner,
            base_permits,
            additional_permits,
        } = &self.state
        {
            // Swap additional_permits to 0 and release base + whatever additional remains.
            // If additional is negative (children dropped after swap), we only release base.
            let additional = additional_permits.swap(0, Ordering::Relaxed).max(0) as u32;
            let total_permits = base_permits + additional;

            if total_permits > 0 {
                inner.release(total_permits);
            }

            debug!(
                "Released compaction memory: total={} bytes",
                permits_to_bytes(total_permits)
            );
        }
    }
}

/// RAII guard for additional memory allocated during task execution.
///
/// NOTE: Lifetime must not exceed the parent `CompactionMemoryGuard`. The parent
/// releases base+additional permits on drop; if this guard outlives it, the
/// remaining work would proceed without actually holding memory.
/// Purpose: allows releasing portions of additional permits early while the
/// parent task continues. The parent guard's drop still releases any remaining
/// additional permits as a safety net.
///
/// When dropped, the memory is automatically released back to the global pool.
pub struct AdditionalMemoryGuard {
    state: AdditionalGuardState,
}

enum AdditionalGuardState {
    Unlimited,
    Limited {
        inner: Arc<Inner>,
        permits: u32,
        parent_additional: Arc<AtomicI64>,
    },
}

impl AdditionalMemoryGuard {
    /// Returns the amount of memory held by this guard in bytes.
    pub fn bytes(&self) -> u64 {
        match &self.state {
            AdditionalGuardState::Unlimited => 0,
            AdditionalGuardState::Limited { permits, .. } => permits_to_bytes(*permits),
        }
    }
}

impl Drop for AdditionalMemoryGuard {
    fn drop(&mut self) {
        if let AdditionalGuardState::Limited {
            inner,
            permits,
            parent_additional,
        } = &self.state
            && *permits > 0
        {
            // fetch_sub returns the previous value.
            // If prev < permits (including negative), parent already released via swap(0).
            let prev = parent_additional.fetch_sub(*permits as i64, Ordering::Relaxed);

            if prev < *permits as i64 {
                return;
            }
            inner.release(*permits);
            debug!(
                "Released additional {} bytes ({} permits)",
                permits_to_bytes(*permits),
                permits
            );
        }
    }
}

impl fmt::Debug for AdditionalMemoryGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdditionalMemoryGuard")
            .field("bytes", &self.bytes())
            .finish()
    }
}

impl fmt::Debug for CompactionMemoryGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactionMemoryGuard")
            .field("base_bytes", &self.granted_bytes())
            .field("additional_bytes", &self.additional_bytes())
            .field("total_bytes", &self.total_bytes())
            .finish()
    }
}

fn bytes_to_permits(bytes: u64) -> u32 {
    if bytes == 0 {
        return 0;
    }
    bytes
        .saturating_add(PERMIT_GRANULARITY_BYTES - 1)
        .saturating_div(PERMIT_GRANULARITY_BYTES)
        .min(Semaphore::MAX_PERMITS as u64)
        .min(u32::MAX as u64) as u32
}

fn permits_to_bytes(permits: u32) -> u64 {
    (permits as u64).saturating_mul(PERMIT_GRANULARITY_BYTES)
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, sleep};

    use super::*;

    #[test]
    fn test_try_acquire_unlimited() {
        let manager = CompactionMemoryManager::new(0);
        let guard = manager.try_acquire(10 * PERMIT_GRANULARITY_BYTES).unwrap();
        assert_eq!(manager.limit_bytes(), 0);
        assert_eq!(guard.granted_bytes(), 0);
    }

    #[test]
    fn test_try_acquire_limited_success_and_release() {
        let bytes = 2 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(bytes);
        {
            let guard = manager.try_acquire(PERMIT_GRANULARITY_BYTES).unwrap();
            assert_eq!(guard.granted_bytes(), PERMIT_GRANULARITY_BYTES);
            assert_eq!(manager.used_bytes(), PERMIT_GRANULARITY_BYTES);
            drop(guard);
        }
        assert_eq!(manager.used_bytes(), 0);
    }

    #[test]
    fn test_try_acquire_exceeds_limit() {
        let limit = PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);
        let result = manager.try_acquire(limit + PERMIT_GRANULARITY_BYTES);
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_wait_for_available_unblocks() {
        let bytes = 2 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(bytes);
        let guard = manager.try_acquire(bytes).unwrap();
        let waiter = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager.wait_for_available(bytes).await;
            })
        };
        sleep(Duration::from_millis(10)).await;
        drop(guard);
        waiter.await.unwrap();
    }

    #[test]
    fn test_request_additional_success() {
        let limit = 10 * PERMIT_GRANULARITY_BYTES; // 10MB limit
        let manager = CompactionMemoryManager::new(limit);

        // Acquire base quota (5MB)
        let base = 5 * PERMIT_GRANULARITY_BYTES;
        let guard = manager.try_acquire(base).unwrap();
        assert_eq!(guard.granted_bytes(), base);
        assert_eq!(guard.additional_bytes(), 0);
        assert_eq!(guard.total_bytes(), base);
        assert_eq!(manager.used_bytes(), base);

        // Request additional memory (3MB) - should succeed
        let additional1 = guard
            .request_additional(3 * PERMIT_GRANULARITY_BYTES)
            .unwrap();
        assert_eq!(additional1.bytes(), 3 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(guard.additional_bytes(), 3 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(guard.total_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

        // Release additional1
        drop(additional1);
        assert_eq!(guard.additional_bytes(), 0);
        assert_eq!(guard.total_bytes(), base);
        assert_eq!(manager.used_bytes(), base);
    }

    #[test]
    fn test_request_additional_exceeds_limit() {
        let limit = 10 * PERMIT_GRANULARITY_BYTES; // 10MB limit
        let manager = CompactionMemoryManager::new(limit);

        // Acquire base quota (5MB)
        let base = 5 * PERMIT_GRANULARITY_BYTES;
        let guard = manager.try_acquire(base).unwrap();

        // Request additional memory (3MB) - should succeed
        let _additional1 = guard
            .request_additional(3 * PERMIT_GRANULARITY_BYTES)
            .unwrap();
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

        // Request more (3MB) - should fail (would exceed 10MB limit)
        let result = guard.request_additional(3 * PERMIT_GRANULARITY_BYTES);
        assert!(result.is_none());

        // Still at 8MB
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
    }

    #[test]
    fn test_request_additional_auto_release_on_guard_drop() {
        let limit = 10 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        {
            let guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

            // Request additional but don't explicitly drop the AdditionalMemoryGuard
            let _additional = guard
                .request_additional(3 * PERMIT_GRANULARITY_BYTES)
                .unwrap();
            assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

            // When guard drops, both base and additional should be released
        }

        // All memory should be released
        assert_eq!(manager.used_bytes(), 0);
    }

    #[test]
    fn test_request_additional_unlimited() {
        let manager = CompactionMemoryManager::new(0); // Unlimited
        let guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

        // Should always succeed with unlimited manager
        let additional = guard
            .request_additional(100 * PERMIT_GRANULARITY_BYTES)
            .unwrap();
        assert_eq!(additional.bytes(), 0); // Unlimited returns 0
        assert_eq!(guard.additional_bytes(), 0);
    }

    #[test]
    fn test_multiple_additional_guards() {
        let limit = 20 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        let guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();
        assert_eq!(manager.used_bytes(), 5 * PERMIT_GRANULARITY_BYTES);

        // Request multiple additional allocations
        let add1 = guard
            .request_additional(3 * PERMIT_GRANULARITY_BYTES)
            .unwrap();
        let add2 = guard
            .request_additional(4 * PERMIT_GRANULARITY_BYTES)
            .unwrap();
        let add3 = guard
            .request_additional(2 * PERMIT_GRANULARITY_BYTES)
            .unwrap();

        assert_eq!(guard.additional_bytes(), 9 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 14 * PERMIT_GRANULARITY_BYTES);

        // Release in different order
        drop(add2);
        assert_eq!(guard.additional_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 10 * PERMIT_GRANULARITY_BYTES);

        drop(add1);
        assert_eq!(guard.additional_bytes(), 2 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 7 * PERMIT_GRANULARITY_BYTES);

        drop(add3);
        assert_eq!(guard.additional_bytes(), 0);
        assert_eq!(manager.used_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
    }

    #[test]
    fn test_request_additional_zero_bytes() {
        let limit = 10 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        let guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

        // Request 0 bytes should succeed without affecting anything
        let additional = guard.request_additional(0).unwrap();
        assert_eq!(additional.bytes(), 0);
        assert_eq!(guard.additional_bytes(), 0);
        assert_eq!(manager.used_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
    }

    #[test]
    fn test_guard_drop_before_additional_drop() {
        // Test that dropping CompactionMemoryGuard before AdditionalMemoryGuard
        // doesn't cause double-release
        let limit = 10 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        let additional = {
            let guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();
            let additional = guard
                .request_additional(3 * PERMIT_GRANULARITY_BYTES)
                .unwrap();
            assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

            // Move additional out, guard will be dropped here
            additional
        };

        // After guard drop, all memory should be released (including additional)
        assert_eq!(manager.used_bytes(), 0);

        // Now drop additional - should not cause any issues (no double-release)
        drop(additional);
        assert_eq!(manager.used_bytes(), 0);

        // Verify we can still allocate the full limit
        let new_guard = manager.try_acquire(10 * PERMIT_GRANULARITY_BYTES).unwrap();
        assert_eq!(new_guard.granted_bytes(), 10 * PERMIT_GRANULARITY_BYTES);
    }
}
