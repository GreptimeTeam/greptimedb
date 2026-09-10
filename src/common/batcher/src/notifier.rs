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

use std::future::Future;
use std::num::NonZeroUsize;

use futures::{StreamExt, stream};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};

/// Best-effort notification admission, independent of the write result.
///
/// The caller owns the notification payload, delivery handler and failure metrics.
pub struct Notifier<T> {
    sender: Sender<T>,
}

impl<T> Clone for Notifier<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T> Notifier<T> {
    /// Returns `None` if the queue exceeds Tokio's supported capacity.
    pub fn try_new(capacity: NonZeroUsize) -> Option<(Self, Receiver<T>)> {
        if capacity.get() > Semaphore::MAX_PERMITS {
            return None;
        }
        let (sender, receiver) = mpsc::channel(capacity.get());
        Some((Self { sender }, receiver))
    }

    /// Configured queue capacity, independent of its current occupancy.
    pub fn max_capacity(&self) -> usize {
        self.sender.max_capacity()
    }

    /// Never waits for delivery. Returns the payload on a full or closed queue
    /// so the caller can record domain-specific diagnostics.
    pub fn try_notify(&self, notification: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(notification)
    }
}

/// Delivers queued notifications with bounded concurrency.
///
/// The caller schedules this future on its runtime. Closing all senders drains
/// queued and in-flight notifications. Handler failures are handled by the caller;
/// this worker does not retry or propagate them into write completion.
pub async fn run_notifier<T, F, Fut>(
    receiver: Receiver<T>,
    max_concurrent_notifications: NonZeroUsize,
    handler: F,
) where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = ()>,
{
    stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    })
    .for_each_concurrent(max_concurrent_notifications.get(), handler)
    .await;
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    use tokio::sync::Semaphore;
    use tokio::sync::mpsc::error::TrySendError;

    use crate::notifier::{Notifier, run_notifier};

    #[test]
    fn test_admission() {
        assert!(Notifier::<usize>::try_new(NonZeroUsize::new(usize::MAX).unwrap()).is_none());
        let (notifier, receiver) = Notifier::try_new(NonZeroUsize::new(1).unwrap()).unwrap();
        let other = notifier.clone();
        assert_eq!(other.max_capacity(), 1);
        assert_eq!(notifier.try_notify(1), Ok(()));
        assert_eq!(other.try_notify(2), Err(TrySendError::Full(2)));
        drop(receiver);
        assert_eq!(other.try_notify(3), Err(TrySendError::Closed(3)));
    }

    #[tokio::test]
    async fn test_concurrency_and_drain() {
        // One-factor comparison: only delivery concurrency changes.
        for _ in 0..2 {
            for concurrency in [1, 2] {
                let (notifier, receiver) =
                    Notifier::try_new(NonZeroUsize::new(3).unwrap()).unwrap();
                for item in 0..3 {
                    notifier.try_notify(item).unwrap();
                }
                drop(notifier);
                let started = Arc::new(AtomicUsize::new(0));
                let completed = Arc::new(AtomicUsize::new(0));
                let gate = Arc::new(Semaphore::new(0));
                let worker =
                    run_notifier(receiver, NonZeroUsize::new(concurrency).unwrap(), |_| {
                        let started = started.clone();
                        let completed = completed.clone();
                        let gate = gate.clone();
                        async move {
                            started.fetch_add(1, Ordering::SeqCst);
                            gate.acquire().await.unwrap().forget();
                            completed.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                tokio::pin!(worker);
                poll_fn(|cx| {
                    assert!(worker.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                assert_eq!(started.load(Ordering::SeqCst), concurrency);
                assert_eq!(completed.load(Ordering::SeqCst), 0);
                gate.add_permits(3);
                worker.await;
                assert_eq!(started.load(Ordering::SeqCst), 3);
                assert_eq!(completed.load(Ordering::SeqCst), 3);
            }
        }
    }
}
