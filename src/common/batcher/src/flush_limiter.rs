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

use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

/// Shared execution budget. One permit represents one complete flush, not one RPC.
#[derive(Clone)]
pub struct FlushLimiter {
    semaphore: Arc<Semaphore>,
}

impl FlushLimiter {
    /// Returns `None` if the limit is zero or exceeds Tokio's supported semaphore capacity.
    pub fn try_new(max_concurrent_flushes: usize) -> Option<Self> {
        let permits = max_concurrent_flushes;
        ((1..=Semaphore::MAX_PERMITS).contains(&permits)).then(|| Self {
            semaphore: Arc::new(Semaphore::new(permits)),
        })
    }

    /// The caller must retain the returned permit until its flush finishes.
    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, AcquireError> {
        self.semaphore.clone().acquire_owned().await
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use super::*;

    #[test]
    fn test_invalid_capacity() {
        for capacity in [0, Semaphore::MAX_PERMITS + 1, usize::MAX] {
            assert!(FlushLimiter::try_new(capacity).is_none());
        }
        for capacity in [1, Semaphore::MAX_PERMITS] {
            assert!(FlushLimiter::try_new(capacity).is_some());
        }
    }

    #[tokio::test]
    async fn test_clones_share_flush_budget() {
        let limiter = FlushLimiter::try_new(1).unwrap();
        let other = limiter.clone();
        let permit = limiter.acquire().await.unwrap();
        let waiting = other.acquire();
        tokio::pin!(waiting);
        // Poll admission without a sleep or an assumption about scheduler timing.
        assert!(
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(waiting.as_mut().poll(cx).is_pending())
            })
            .await
        );
        drop(permit);
        let _next = waiting.await.unwrap();
    }
}
