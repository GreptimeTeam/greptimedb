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

/// Limits unfinished original requests independently of their number of submissions.
///
/// Acquire once for each original request, then clone its permit into all of that
/// request's submissions. Capacity is released when the last owner drops it.
#[derive(Clone)]
pub struct RequestLimiter {
    semaphore: Arc<Semaphore>,
}

impl RequestLimiter {
    /// Returns `None` if the limit is zero or exceeds Tokio's supported semaphore capacity.
    pub fn try_new(max_inflight_requests: usize) -> Option<Self> {
        let permits = max_inflight_requests;
        ((1..=Semaphore::MAX_PERMITS).contains(&permits)).then(|| Self {
            semaphore: Arc::new(Semaphore::new(permits)),
        })
    }

    /// Waits for one request slot and returns a permit shared by its submissions.
    ///
    /// Cancelling a pending acquisition does not consume a slot. Once acquired,
    /// callers must retain the permit until every submission has finished, even
    /// if the original caller stops waiting for its response.
    pub async fn acquire(&self) -> Result<Arc<OwnedSemaphorePermit>, AcquireError> {
        self.semaphore.clone().acquire_owned().await.map(Arc::new)
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::pin::Pin;
    use std::task::Poll;

    use tokio::sync::Semaphore;

    use crate::request_limiter::RequestLimiter;

    async fn is_pending<F: Future>(mut future: Pin<&mut F>) -> bool {
        poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
    }

    #[test]
    fn test_capacity_boundaries() {
        for capacity in [1, Semaphore::MAX_PERMITS] {
            assert!(RequestLimiter::try_new(capacity).is_some());
        }
        for capacity in [0, Semaphore::MAX_PERMITS + 1, usize::MAX] {
            assert!(RequestLimiter::try_new(capacity).is_none());
        }
    }

    #[tokio::test]
    async fn test_last_submission_releases_request_slot() {
        let limiter = RequestLimiter::try_new(1).unwrap();
        let other = limiter.clone();
        let request = limiter.acquire().await.unwrap();
        let first_submission = request.clone();
        let second_submission = request.clone();
        let mut waiting = Box::pin(other.acquire());
        assert!(is_pending(waiting.as_mut()).await);
        // The caller and one submission finish, but another submission remains.
        drop(request);
        drop(first_submission);
        assert!(is_pending(waiting.as_mut()).await);
        drop(second_submission);
        let next = waiting.await.unwrap();
        assert_eq!(next.num_permits(), 1);
    }

    #[tokio::test]
    async fn test_cancelled_acquisition_does_not_leak_capacity() {
        let limiter = RequestLimiter::try_new(1).unwrap();
        let permit = limiter.acquire().await.unwrap();
        let mut cancelled = Box::pin(limiter.acquire());
        assert!(is_pending(cancelled.as_mut()).await);
        drop(cancelled);
        let mut next = Box::pin(limiter.acquire());
        assert!(is_pending(next.as_mut()).await);
        drop(permit);
        let next = next.await.unwrap();
        drop(next);
        let _available = limiter.acquire().await.unwrap();
    }

    #[tokio::test]
    async fn test_independent_requests_consume_separate_slots() {
        let limiter = RequestLimiter::try_new(2).unwrap();
        let first = limiter.acquire().await.unwrap();
        let second = limiter.acquire().await.unwrap();
        let mut third = Box::pin(limiter.acquire());
        assert!(is_pending(third.as_mut()).await);
        drop(first);
        let _third = third.await.unwrap();
        drop(second);
    }
}
