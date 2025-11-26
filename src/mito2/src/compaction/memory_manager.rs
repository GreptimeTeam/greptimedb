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
use std::sync::atomic::{AtomicU64, Ordering};

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
    limit_bytes: u64,
    used_bytes: AtomicU64,
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

        let permits = bytes_to_permits(limit_bytes) as usize;
        COMPACTION_MEMORY_LIMIT.set(limit_bytes as i64);

        Self {
            inner: Some(Arc::new(Inner {
                semaphore: Semaphore::new(permits),
                limit_bytes,
                used_bytes: AtomicU64::new(0),
                notify: Notify::new(),
            })),
        }
    }

    /// Returns the configured limit in bytes (0 if unlimited).
    pub fn limit_bytes(&self) -> u64 {
        self.inner
            .as_ref()
            .map(|inner| inner.limit_bytes)
            .unwrap_or(0)
    }

    /// Returns currently used bytes.
    pub fn used_bytes(&self) -> u64 {
        self.inner
            .as_ref()
            .map(|inner| inner.used_bytes.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Returns available bytes.
    pub fn available_bytes(&self) -> u64 {
        self.limit_bytes().saturating_sub(self.used_bytes())
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
                        inner.on_acquire(permits);
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

    /// Waits until memory becomes available. This does not reserve memory.
    pub async fn wait_for_available(&self, bytes: u64) {
        if self.inner.is_none() || bytes == 0 {
            return;
        }

        let inner = self.inner.as_ref().unwrap().clone();
        loop {
            if inner.available_bytes() >= bytes {
                return;
            }
            inner.notify.notified().await;
        }
    }
}

impl Inner {
    fn on_acquire(&self, permits: u32) {
        let bytes = permits_to_bytes(permits);
        let used = self.used_bytes.fetch_add(bytes, Ordering::AcqRel) + bytes;
        COMPACTION_MEMORY_IN_USE.set(used as i64);
    }

    fn release(&self, permits: u32) {
        if permits == 0 {
            return;
        }
        let bytes = permits_to_bytes(permits);
        let used = self.used_bytes.fetch_sub(bytes, Ordering::AcqRel) - bytes;
        COMPACTION_MEMORY_IN_USE.set(used as i64);
        self.semaphore.add_permits(permits as usize);
        self.notify.notify_waiters();
    }

    fn available_bytes(&self) -> u64 {
        self.limit_bytes
            .saturating_sub(self.used_bytes.load(Ordering::Relaxed))
    }
}

/// Guard representing a slice of reserved compaction memory.
pub struct CompactionMemoryGuard {
    state: GuardState,
}

enum GuardState {
    Unlimited,
    Limited { inner: Arc<Inner>, permits: u32 },
}

impl CompactionMemoryGuard {
    fn unlimited() -> Self {
        Self {
            state: GuardState::Unlimited,
        }
    }

    fn limited(inner: Arc<Inner>, permits: u32) -> Self {
        Self {
            state: GuardState::Limited { inner, permits },
        }
    }

    /// Returns granted bytes.
    pub fn granted_bytes(&self) -> u64 {
        match &self.state {
            GuardState::Unlimited => 0,
            GuardState::Limited { permits, .. } => permits_to_bytes(*permits),
        }
    }

    /// Returns remaining bytes inside this guard.
    pub fn remaining_bytes(&self) -> u64 {
        self.granted_bytes()
    }

    /// Attempts to split the guard and carve out `bytes`.
    pub fn try_split(&mut self, bytes: u64) -> Option<CompactionMemoryGuard> {
        match &mut self.state {
            GuardState::Unlimited => Some(CompactionMemoryGuard::unlimited()),
            GuardState::Limited { inner, permits } => {
                let needed = bytes_to_permits(bytes);
                if needed == 0 || *permits < needed {
                    return None;
                }
                *permits -= needed;
                Some(CompactionMemoryGuard::limited(inner.clone(), needed))
            }
        }
    }
}

impl Drop for CompactionMemoryGuard {
    fn drop(&mut self) {
        if let GuardState::Limited { inner, permits } = &self.state {
            inner.release(*permits);
        }
    }
}

impl fmt::Debug for CompactionMemoryGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactionMemoryGuard")
            .field("bytes", &self.granted_bytes())
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
        .min(u32::MAX as u64) as u32
}

fn permits_to_bytes(permits: u32) -> u64 {
    permits as u64 * PERMIT_GRANULARITY_BYTES
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
}
