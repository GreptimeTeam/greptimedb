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
use std::{fmt, mem};

use common_telemetry::debug;
use humantime::{format_duration, parse_duration};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::error::{
    CompactionMemoryLimitExceededSnafu, CompactionMemorySemaphoreClosedSnafu, Result,
};
use crate::metrics::{
    COMPACTION_MEMORY_IN_USE, COMPACTION_MEMORY_LIMIT, COMPACTION_MEMORY_REJECTED,
};

/// Minimum bytes controlled by one semaphore permit.
const PERMIT_GRANULARITY_BYTES: u64 = 1 << 20; // 1 MB

/// Default wait timeout for compaction memory.
pub const DEFAULT_MEMORY_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Defines how to react when compaction cannot acquire enough memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnExhaustedPolicy {
    /// Wait until enough memory is released, bounded by timeout.
    Wait { timeout: Duration },
    /// Skip the compaction if memory is not immediately available.
    Skip,
}

impl Default for OnExhaustedPolicy {
    fn default() -> Self {
        OnExhaustedPolicy::Wait {
            timeout: DEFAULT_MEMORY_WAIT_TIMEOUT,
        }
    }
}

impl Serialize for OnExhaustedPolicy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let text = match self {
            OnExhaustedPolicy::Skip => "skip".to_string(),
            OnExhaustedPolicy::Wait { timeout } if *timeout == DEFAULT_MEMORY_WAIT_TIMEOUT => {
                "wait".to_string()
            }
            OnExhaustedPolicy::Wait { timeout } => {
                format!("wait({})", format_duration(*timeout))
            }
        };
        serializer.serialize_str(&text)
    }
}

impl<'de> Deserialize<'de> for OnExhaustedPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let lower = raw.to_ascii_lowercase();

        if lower == "skip" {
            return Ok(OnExhaustedPolicy::Skip);
        }
        if lower == "wait" {
            return Ok(OnExhaustedPolicy::default());
        }
        if lower.starts_with("wait(") && lower.ends_with(')') {
            let inner = &raw[5..raw.len() - 1];
            let timeout = parse_duration(inner).map_err(serde::de::Error::custom)?;
            return Ok(OnExhaustedPolicy::Wait { timeout });
        }

        Err(serde::de::Error::custom(format!(
            "invalid compaction memory policy: {}, expected wait, wait(<duration>) or skip",
            raw
        )))
    }
}

/// Global memory manager for compaction tasks.
#[derive(Clone)]
pub struct CompactionMemoryManager {
    quota: Option<MemoryQuota>,
}

/// Shared memory quota state across all compaction guards.
#[derive(Clone)]
struct MemoryQuota {
    semaphore: Arc<Semaphore>,
    // Maximum permits (aligned to PERMIT_GRANULARITY_BYTES).
    limit_permits: u32,
}

impl CompactionMemoryManager {
    /// Creates a new memory manager with the given limit in bytes.
    /// `limit_bytes = 0` disables the limit.
    pub fn new(limit_bytes: u64) -> Self {
        if limit_bytes == 0 {
            COMPACTION_MEMORY_LIMIT.set(0);
            return Self { quota: None };
        }

        let limit_permits = bytes_to_permits(limit_bytes);
        let limit_aligned_bytes = permits_to_bytes(limit_permits);
        COMPACTION_MEMORY_LIMIT.set(limit_aligned_bytes as i64);

        Self {
            quota: Some(MemoryQuota {
                semaphore: Arc::new(Semaphore::new(limit_permits as usize)),
                limit_permits,
            }),
        }
    }

    /// Returns the configured limit in bytes (0 if unlimited).
    pub fn limit_bytes(&self) -> u64 {
        self.quota
            .as_ref()
            .map(|quota| permits_to_bytes(quota.limit_permits))
            .unwrap_or(0)
    }

    /// Returns currently used bytes.
    pub fn used_bytes(&self) -> u64 {
        self.quota
            .as_ref()
            .map(|quota| permits_to_bytes(quota.used_permits()))
            .unwrap_or(0)
    }

    /// Returns available bytes.
    pub fn available_bytes(&self) -> u64 {
        self.quota
            .as_ref()
            .map(|quota| permits_to_bytes(quota.available_permits_clamped()))
            .unwrap_or(0)
    }

    /// Acquires memory, waiting if necessary until enough is available.
    ///
    /// # Errors
    /// - Returns error if requested bytes exceed the total limit
    /// - Returns error if the semaphore is unexpectedly closed
    pub async fn acquire(&self, bytes: u64) -> Result<CompactionMemoryGuard> {
        match &self.quota {
            None => Ok(CompactionMemoryGuard::unlimited()),
            Some(quota) => {
                let permits = bytes_to_permits(bytes);

                // Fail-fast: reject if request exceeds total limit.
                ensure!(
                    permits <= quota.limit_permits,
                    CompactionMemoryLimitExceededSnafu {
                        requested_bytes: bytes,
                        limit_bytes: permits_to_bytes(quota.limit_permits),
                    }
                );

                let permit = quota
                    .semaphore
                    .clone()
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| CompactionMemorySemaphoreClosedSnafu.build())?;
                quota.update_in_use_metric();
                Ok(CompactionMemoryGuard::limited(permit, quota.clone()))
            }
        }
    }

    /// Tries to acquire memory. Returns Some(guard) on success, None if insufficient.
    pub fn try_acquire(&self, bytes: u64) -> Option<CompactionMemoryGuard> {
        match &self.quota {
            None => Some(CompactionMemoryGuard::unlimited()),
            Some(quota) => {
                let permits = bytes_to_permits(bytes);

                match quota.semaphore.clone().try_acquire_many_owned(permits) {
                    Ok(permit) => {
                        quota.update_in_use_metric();
                        Some(CompactionMemoryGuard::limited(permit, quota.clone()))
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
}

impl MemoryQuota {
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
/// Memory is automatically released when this guard is dropped.
pub struct CompactionMemoryGuard {
    state: GuardState,
}

enum GuardState {
    Unlimited,
    Limited {
        // Holds all permits owned by this guard (base plus any additional).
        // Additional requests merge into this permit and are released together on drop.
        permit: OwnedSemaphorePermit,
        // Memory quota for requesting additional permits and updating metrics.
        quota: MemoryQuota,
    },
}

impl CompactionMemoryGuard {
    fn unlimited() -> Self {
        Self {
            state: GuardState::Unlimited,
        }
    }

    fn limited(permit: OwnedSemaphorePermit, quota: MemoryQuota) -> Self {
        Self {
            state: GuardState::Limited { permit, quota },
        }
    }

    /// Returns granted quota in bytes.
    pub fn granted_bytes(&self) -> u64 {
        match &self.state {
            GuardState::Unlimited => 0,
            GuardState::Limited { permit, .. } => permits_to_bytes(permit.num_permits() as u32),
        }
    }

    /// Tries to allocate additional memory during task execution.
    ///
    /// On success, merges the new memory into this guard and returns true.
    /// On failure, returns false and leaves this guard unchanged.
    ///
    /// # Behavior
    /// - Running tasks can request additional memory on top of their initial allocation
    /// - If total memory (all tasks) would exceed limit, returns false immediately
    /// - The task should gracefully handle false by failing or adjusting its strategy
    /// - The additional memory is merged into this guard and released together on drop
    pub fn request_additional(&mut self, bytes: u64) -> bool {
        match &mut self.state {
            GuardState::Unlimited => true,
            GuardState::Limited { permit, quota } => {
                // Early return for zero-byte requests (no-op)
                if bytes == 0 {
                    return true;
                }

                let additional_permits = bytes_to_permits(bytes);

                // Try to acquire additional permits from the quota
                match quota
                    .semaphore
                    .clone()
                    .try_acquire_many_owned(additional_permits)
                {
                    Ok(additional_permit) => {
                        // Merge into main permit
                        permit.merge(additional_permit);
                        quota.update_in_use_metric();

                        debug!("Allocated additional {} bytes", bytes);

                        true
                    }
                    Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => {
                        COMPACTION_MEMORY_REJECTED
                            .with_label_values(&["request_additional"])
                            .inc();
                        false
                    }
                }
            }
        }
    }

    /// Releases a portion of granted memory back to the pool early,
    /// before the guard is dropped.
    ///
    /// This is useful when a task's memory requirement decreases during execution
    /// (e.g., after completing a memory-intensive phase). The guard remains valid
    /// with reduced quota, and the task can continue running.
    pub fn early_release_partial(&mut self, bytes: u64) -> bool {
        match &mut self.state {
            GuardState::Unlimited => true,
            GuardState::Limited { permit, quota } => {
                // Early return for zero-byte requests (no-op)
                if bytes == 0 {
                    return true;
                }

                let release_permits = bytes_to_permits(bytes);

                // Split out the permits we want to release
                match permit.split(release_permits as usize) {
                    Some(released_permit) => {
                        let released_bytes = permits_to_bytes(released_permit.num_permits() as u32);

                        // Drop the split permit to return it to the quota
                        drop(released_permit);
                        quota.update_in_use_metric();

                        debug!(
                            "Early released {} bytes from compaction memory guard",
                            released_bytes
                        );

                        true
                    }
                    None => {
                        // Requested release exceeds granted amount
                        false
                    }
                }
            }
        }
    }
}

impl Drop for CompactionMemoryGuard {
    fn drop(&mut self) {
        if let GuardState::Limited { permit, quota } =
            mem::replace(&mut self.state, GuardState::Unlimited)
        {
            let bytes = permits_to_bytes(permit.num_permits() as u32);

            // Release permits before updating metrics to reflect latest usage.
            drop(permit);
            quota.update_in_use_metric();

            debug!("Released compaction memory: {} bytes", bytes);
        }
    }
}

impl fmt::Debug for CompactionMemoryGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactionMemoryGuard")
            .field("granted_bytes", &self.granted_bytes())
            .finish()
    }
}

fn bytes_to_permits(bytes: u64) -> u32 {
    // Round up to the nearest permit.
    // Returns 0 for bytes=0, which allows lazy allocation via request_additional().
    // Non-zero bytes always round up to at least 1 permit due to the math.
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
    async fn test_acquire_blocks_and_unblocks() {
        let bytes = 2 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(bytes);
        let guard = manager.try_acquire(bytes).unwrap();

        // Spawn a task that will block on acquire()
        let waiter = {
            let manager = manager.clone();
            tokio::spawn(async move {
                // This will block until memory is available
                let _guard = manager.acquire(bytes).await.unwrap();
            })
        };

        sleep(Duration::from_millis(10)).await;
        // Release memory - this should unblock the waiter
        drop(guard);

        // Waiter should complete now
        waiter.await.unwrap();
    }

    #[test]
    fn test_request_additional_success() {
        let limit = 10 * PERMIT_GRANULARITY_BYTES; // 10MB limit
        let manager = CompactionMemoryManager::new(limit);

        // Acquire base quota (5MB)
        let base = 5 * PERMIT_GRANULARITY_BYTES;
        let mut guard = manager.try_acquire(base).unwrap();
        assert_eq!(guard.granted_bytes(), base);
        assert_eq!(manager.used_bytes(), base);

        // Request additional memory (3MB) - should succeed and merge
        assert!(guard.request_additional(3 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(guard.granted_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
    }

    #[test]
    fn test_request_additional_exceeds_limit() {
        let limit = 10 * PERMIT_GRANULARITY_BYTES; // 10MB limit
        let manager = CompactionMemoryManager::new(limit);

        // Acquire base quota (5MB)
        let base = 5 * PERMIT_GRANULARITY_BYTES;
        let mut guard = manager.try_acquire(base).unwrap();

        // Request additional memory (3MB) - should succeed
        assert!(guard.request_additional(3 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

        // Request more (3MB) - should fail (would exceed 10MB limit)
        let result = guard.request_additional(3 * PERMIT_GRANULARITY_BYTES);
        assert!(!result);

        // Still at 8MB
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(guard.granted_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
    }

    #[test]
    fn test_request_additional_auto_release_on_guard_drop() {
        let limit = 10 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        {
            let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

            // Request additional - memory is merged into guard
            assert!(guard.request_additional(3 * PERMIT_GRANULARITY_BYTES));
            assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

            // When guard drops, all memory (base + additional) is released together
        }

        // After scope, all memory should be released
        assert_eq!(manager.used_bytes(), 0);
    }

    #[test]
    fn test_request_additional_unlimited() {
        let manager = CompactionMemoryManager::new(0); // Unlimited
        let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

        // Should always succeed with unlimited manager
        assert!(guard.request_additional(100 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(guard.granted_bytes(), 0);
        assert_eq!(manager.used_bytes(), 0);
    }

    #[test]
    fn test_request_additional_zero_bytes() {
        let limit = 10 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

        // Request 0 bytes should succeed without affecting anything
        assert!(guard.request_additional(0));
        assert_eq!(guard.granted_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
    }

    #[test]
    fn test_early_release_partial_success() {
        let limit = 10 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        let mut guard = manager.try_acquire(8 * PERMIT_GRANULARITY_BYTES).unwrap();
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

        // Release half
        assert!(guard.early_release_partial(4 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(guard.granted_bytes(), 4 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 4 * PERMIT_GRANULARITY_BYTES);

        // Released memory should be available to others
        let _guard2 = manager.try_acquire(4 * PERMIT_GRANULARITY_BYTES).unwrap();
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
    }

    #[test]
    fn test_early_release_partial_exceeds_granted() {
        let manager = CompactionMemoryManager::new(10 * PERMIT_GRANULARITY_BYTES);
        let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

        // Try to release more than granted - should fail
        assert!(!guard.early_release_partial(10 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(guard.granted_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
    }

    #[test]
    fn test_early_release_partial_unlimited() {
        let manager = CompactionMemoryManager::new(0);
        let mut guard = manager.try_acquire(100 * PERMIT_GRANULARITY_BYTES).unwrap();

        // Unlimited guard - release should succeed (no-op)
        assert!(guard.early_release_partial(50 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(guard.granted_bytes(), 0);
    }

    #[test]
    fn test_request_and_early_release_symmetry() {
        let limit = 20 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();

        // Request additional
        assert!(guard.request_additional(5 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(guard.granted_bytes(), 10 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 10 * PERMIT_GRANULARITY_BYTES);

        // Early release some
        assert!(guard.early_release_partial(3 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(guard.granted_bytes(), 7 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 7 * PERMIT_GRANULARITY_BYTES);

        // Request again
        assert!(guard.request_additional(2 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(guard.granted_bytes(), 9 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 9 * PERMIT_GRANULARITY_BYTES);

        // Early release again
        assert!(guard.early_release_partial(4 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(guard.granted_bytes(), 5 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 5 * PERMIT_GRANULARITY_BYTES);

        drop(guard);
        assert_eq!(manager.used_bytes(), 0);
    }

    #[test]
    fn test_small_allocation_rounds_up() {
        // Test that allocations smaller than PERMIT_GRANULARITY_BYTES
        // round up to 1 permit and can use request_additional()
        let limit = 10 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        let mut guard = manager.try_acquire(512 * 1024).unwrap(); // 512KB
        assert_eq!(guard.granted_bytes(), PERMIT_GRANULARITY_BYTES); // Rounds up to 1MB
        assert!(guard.request_additional(2 * PERMIT_GRANULARITY_BYTES)); // Can request more
        assert_eq!(guard.granted_bytes(), 3 * PERMIT_GRANULARITY_BYTES);
    }

    #[test]
    fn test_acquire_zero_bytes_lazy_allocation() {
        // Test that acquire(0) returns 0 permits but can request_additional() later
        let manager = CompactionMemoryManager::new(10 * PERMIT_GRANULARITY_BYTES);

        let mut guard = manager.try_acquire(0).unwrap();
        assert_eq!(guard.granted_bytes(), 0); // No permits consumed
        assert_eq!(manager.used_bytes(), 0);

        assert!(guard.request_additional(3 * PERMIT_GRANULARITY_BYTES)); // Lazy allocation
        assert_eq!(guard.granted_bytes(), 3 * PERMIT_GRANULARITY_BYTES);
    }
}
