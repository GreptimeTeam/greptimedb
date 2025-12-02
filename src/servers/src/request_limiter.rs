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

//! Request memory limiter for controlling total memory usage of concurrent requests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{Result, TooManyConcurrentRequestsSnafu};

/// Limiter for total memory usage of concurrent request bodies.
///
/// Tracks the total memory used by all concurrent request bodies
/// and rejects new requests when the limit is reached.
#[derive(Clone, Default)]
pub struct RequestMemoryLimiter {
    inner: Option<Arc<LimiterInner>>,
}

struct LimiterInner {
    current_usage: AtomicUsize,
    max_memory: usize,
}

impl RequestMemoryLimiter {
    /// Create a new memory limiter.
    ///
    /// # Arguments
    /// * `max_memory` - Maximum total memory for all concurrent request bodies in bytes (0 = unlimited)
    pub fn new(max_memory: usize) -> Self {
        if max_memory == 0 {
            return Self { inner: None };
        }

        Self {
            inner: Some(Arc::new(LimiterInner {
                current_usage: AtomicUsize::new(0),
                max_memory,
            })),
        }
    }

    /// Try to acquire memory for a request of given size.
    ///
    /// Returns `Ok(RequestMemoryGuard)` if memory was acquired successfully.
    /// Returns `Err` if the memory limit would be exceeded.
    pub fn try_acquire(&self, request_size: usize) -> Result<Option<RequestMemoryGuard>> {
        let Some(inner) = self.inner.as_ref() else {
            return Ok(None);
        };

        let mut new_usage = 0;
        let result =
            inner
                .current_usage
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    new_usage = current.saturating_add(request_size);
                    if new_usage <= inner.max_memory {
                        Some(new_usage)
                    } else {
                        None
                    }
                });

        match result {
            Ok(_) => Ok(Some(RequestMemoryGuard {
                size: request_size,
                limiter: Arc::clone(inner),
                usage_snapshot: new_usage,
            })),
            Err(_current) => TooManyConcurrentRequestsSnafu {
                limit: inner.max_memory,
                request_size,
            }
            .fail(),
        }
    }

    /// Check if limiter is enabled
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Get current memory usage
    pub fn current_usage(&self) -> usize {
        self.inner
            .as_ref()
            .map(|inner| inner.current_usage.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get max memory limit
    pub fn max_memory(&self) -> usize {
        self.inner
            .as_ref()
            .map(|inner| inner.max_memory)
            .unwrap_or(0)
    }
}

/// RAII guard that releases memory when dropped
pub struct RequestMemoryGuard {
    size: usize,
    limiter: Arc<LimiterInner>,
    usage_snapshot: usize,
}

impl RequestMemoryGuard {
    /// Returns the total memory usage snapshot at the time this guard was acquired.
    pub fn current_usage(&self) -> usize {
        self.usage_snapshot
    }
}

impl Drop for RequestMemoryGuard {
    fn drop(&mut self) {
        self.limiter
            .current_usage
            .fetch_sub(self.size, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::Barrier;

    use super::*;

    #[test]
    fn test_limiter_disabled() {
        let limiter = RequestMemoryLimiter::new(0);
        assert!(!limiter.is_enabled());
        assert!(limiter.try_acquire(1000000).unwrap().is_none());
        assert_eq!(limiter.current_usage(), 0);
    }

    #[test]
    fn test_limiter_basic() {
        let limiter = RequestMemoryLimiter::new(1000);
        assert!(limiter.is_enabled());
        assert_eq!(limiter.max_memory(), 1000);
        assert_eq!(limiter.current_usage(), 0);

        // Acquire 400 bytes
        let _guard1 = limiter.try_acquire(400).unwrap();
        assert_eq!(limiter.current_usage(), 400);

        // Acquire another 500 bytes
        let _guard2 = limiter.try_acquire(500).unwrap();
        assert_eq!(limiter.current_usage(), 900);

        // Try to acquire 200 bytes - should fail (900 + 200 > 1000)
        let result = limiter.try_acquire(200);
        assert!(result.is_err());
        assert_eq!(limiter.current_usage(), 900);

        // Drop first guard
        drop(_guard1);
        assert_eq!(limiter.current_usage(), 500);

        // Now we can acquire 200 bytes
        let _guard3 = limiter.try_acquire(200).unwrap();
        assert_eq!(limiter.current_usage(), 700);
    }

    #[test]
    fn test_limiter_exact_limit() {
        let limiter = RequestMemoryLimiter::new(1000);

        // Acquire exactly the limit
        let _guard = limiter.try_acquire(1000).unwrap();
        assert_eq!(limiter.current_usage(), 1000);

        // Try to acquire 1 more byte - should fail
        let result = limiter.try_acquire(1);
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_limiter_concurrent() {
        let limiter = RequestMemoryLimiter::new(1000);
        let barrier = Arc::new(Barrier::new(11)); // 10 tasks + main
        let mut handles = vec![];

        // Spawn 10 tasks each trying to acquire 200 bytes
        for _ in 0..10 {
            let limiter_clone = limiter.clone();
            let barrier_clone = barrier.clone();
            let handle = tokio::spawn(async move {
                barrier_clone.wait().await;
                limiter_clone.try_acquire(200)
            });
            handles.push(handle);
        }

        // Let all tasks start together
        barrier.wait().await;

        let mut success_count = 0;
        let mut fail_count = 0;
        let mut guards = Vec::new();

        for handle in handles {
            match handle.await.unwrap() {
                Ok(Some(guard)) => {
                    success_count += 1;
                    guards.push(guard);
                }
                Err(_) => fail_count += 1,
                Ok(None) => unreachable!(),
            }
        }

        // Only 5 tasks should succeed (5 * 200 = 1000)
        assert_eq!(success_count, 5);
        assert_eq!(fail_count, 5);
        drop(guards);
    }
}
