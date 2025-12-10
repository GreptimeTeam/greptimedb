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
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

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
    inner: Option<Arc<Inner>>,
}

struct Inner {
    semaphore: Arc<Semaphore>,
    limit_permits: u32,
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
                semaphore: Arc::new(Semaphore::new(limit_permits as usize)),
                limit_permits,
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

    /// Acquires memory, waiting if necessary until enough is available.
    pub async fn acquire(&self, bytes: u64) -> CompactionMemoryGuard {
        match &self.inner {
            None => CompactionMemoryGuard::unlimited(),
            Some(inner) => {
                let permits = bytes_to_permits(bytes);
                if permits == 0 {
                    return CompactionMemoryGuard::unlimited();
                }

                let permit = inner
                    .semaphore
                    .clone()
                    .acquire_many_owned(permits)
                    .await
                    .unwrap();
                inner.on_acquire();
                CompactionMemoryGuard::limited(permit, inner.clone())
            }
        }
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

                match inner.semaphore.clone().try_acquire_many_owned(permits) {
                    Ok(permit) => {
                        inner.on_acquire();
                        Some(CompactionMemoryGuard::limited(permit, inner.clone()))
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

impl Inner {
    fn on_acquire(&self) {
        self.update_in_use_metric();
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
/// Memory is automatically released when this guard is dropped.
pub struct CompactionMemoryGuard {
    state: GuardState,
}

enum GuardState {
    Unlimited,
    Limited {
        /// Holds all permits owned by this guard (base plus any additional).
        /// Additional requests merge into this permit and are released together on drop.
        permit: OwnedSemaphorePermit,
        /// Reference to inner for metrics updates.
        inner: Arc<Inner>,
    },
}

impl CompactionMemoryGuard {
    fn unlimited() -> Self {
        Self {
            state: GuardState::Unlimited,
        }
    }

    fn limited(permit: OwnedSemaphorePermit, inner: Arc<Inner>) -> Self {
        Self {
            state: GuardState::Limited { permit, inner },
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
            GuardState::Limited { permit, inner } => {
                let additional_permits = bytes_to_permits(bytes);
                if additional_permits == 0 {
                    return true;
                }

                // Try to acquire additional permits from global semaphore
                match inner
                    .semaphore
                    .clone()
                    .try_acquire_many_owned(additional_permits)
                {
                    Ok(additional_permit) => {
                        // Merge into main permit
                        permit.merge(additional_permit);
                        inner.update_in_use_metric();

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
}

impl Drop for CompactionMemoryGuard {
    fn drop(&mut self) {
        if let GuardState::Limited { permit, inner } =
            mem::replace(&mut self.state, GuardState::Unlimited)
        {
            let bytes = permits_to_bytes(permit.num_permits() as u32);

            // Release permits before updating metrics to reflect latest usage.
            drop(permit);
            inner.update_in_use_metric();

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
    async fn test_acquire_blocks_and_unblocks() {
        let bytes = 2 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(bytes);
        let guard = manager.try_acquire(bytes).unwrap();

        // Spawn a task that will block on acquire()
        let waiter = {
            let manager = manager.clone();
            tokio::spawn(async move {
                // This will block until memory is available
                let _guard = manager.acquire(bytes).await;
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
    fn test_multiple_additional_requests() {
        let limit = 20 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();
        assert_eq!(manager.used_bytes(), 5 * PERMIT_GRANULARITY_BYTES);

        // Request multiple additional allocations - all merged into guard
        assert!(guard.request_additional(3 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

        assert!(guard.request_additional(4 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(manager.used_bytes(), 12 * PERMIT_GRANULARITY_BYTES);

        assert!(guard.request_additional(2 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(manager.used_bytes(), 14 * PERMIT_GRANULARITY_BYTES);

        // All memory released together when guard drops
        drop(guard);
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
    fn test_request_additional_incremental() {
        // Test that additional memory is properly merged and released as one unit
        let limit = 10 * PERMIT_GRANULARITY_BYTES;
        let manager = CompactionMemoryManager::new(limit);

        let mut guard = manager.try_acquire(5 * PERMIT_GRANULARITY_BYTES).unwrap();
        assert_eq!(guard.granted_bytes(), 5 * PERMIT_GRANULARITY_BYTES);

        // Request additional - memory is merged
        assert!(guard.request_additional(3 * PERMIT_GRANULARITY_BYTES));
        assert_eq!(guard.granted_bytes(), 8 * PERMIT_GRANULARITY_BYTES);
        assert_eq!(manager.used_bytes(), 8 * PERMIT_GRANULARITY_BYTES);

        // Drop guard - all memory released together
        drop(guard);
        assert_eq!(manager.used_bytes(), 0);

        // Verify we can still allocate the full limit
        let new_guard = manager.try_acquire(10 * PERMIT_GRANULARITY_BYTES).unwrap();
        assert_eq!(new_guard.granted_bytes(), 10 * PERMIT_GRANULARITY_BYTES);
    }
}
