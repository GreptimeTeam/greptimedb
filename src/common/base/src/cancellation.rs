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

//! [CancellationHandle] is used to compose with manual implementation of [futures::future::Future]
//! or [futures::stream::Stream] to facilitate cancellation.
//! See example in [frontend::stream_wrapper::CancellableStreamWrapper] and [CancellableFuture].

use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::task::AtomicWaker;
use pin_project::pin_project;

#[derive(Default)]
pub struct CancellationHandle {
    waker: AtomicWaker,
    cancelled: AtomicBool,
}

impl Debug for CancellationHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellationHandle")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl CancellationHandle {
    pub fn waker(&self) -> &AtomicWaker {
        &self.waker
    }

    /// Cancels a future or stream.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
        self.waker.wake();
    }

    /// Is this handle cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

#[pin_project]
#[derive(Debug, Clone)]
pub struct CancellableFuture<T> {
    #[pin]
    fut: T,
    handle: Arc<CancellationHandle>,
}

impl<T> CancellableFuture<T> {
    pub fn new(fut: T, handle: Arc<CancellationHandle>) -> Self {
        Self { fut, handle }
    }
}

impl<T> Future for CancellableFuture<T>
where
    T: Future,
{
    type Output = Result<T::Output, Cancelled>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();
        // Check if the task has been aborted
        if this.handle.is_cancelled() {
            return Poll::Ready(Err(Cancelled));
        }

        if let Poll::Ready(x) = this.fut.poll(cx) {
            return Poll::Ready(Ok(x));
        }

        this.handle.waker().register(cx.waker());
        if this.handle.is_cancelled() {
            return Poll::Ready(Err(Cancelled));
        }
        Poll::Pending
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Cancelled;

impl Display for Cancelled {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Future has been cancelled")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::{sleep, timeout};

    use crate::cancellation::{CancellableFuture, CancellationHandle, Cancelled};

    #[tokio::test]
    async fn test_cancellable_future_completes_normally() {
        let handle = Arc::new(CancellationHandle::default());
        let future = async { 42 };
        let cancellable = CancellableFuture::new(future, handle);

        let result = cancellable.await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_cancellable_future_cancelled_before_start() {
        let handle = Arc::new(CancellationHandle::default());
        handle.cancel();

        let future = async { 42 };
        let cancellable = CancellableFuture::new(future, handle);

        let result = cancellable.await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Cancelled));
    }

    #[tokio::test]
    async fn test_cancellable_future_cancelled_during_execution() {
        let handle = Arc::new(CancellationHandle::default());
        let handle_clone = handle.clone();

        // Create a future that sleeps for a long time
        let future = async {
            sleep(Duration::from_secs(10)).await;
            42
        };
        let cancellable = CancellableFuture::new(future, handle);

        // Cancel the future after a short delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            handle_clone.cancel();
        });

        let result = cancellable.await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Cancelled));
    }

    #[tokio::test]
    async fn test_cancellable_future_completes_before_cancellation() {
        let handle = Arc::new(CancellationHandle::default());
        let handle_clone = handle.clone();

        // Create a future that completes quickly
        let future = async {
            sleep(Duration::from_millis(10)).await;
            42
        };
        let cancellable = CancellableFuture::new(future, handle);

        // Try to cancel after the future should have completed
        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            handle_clone.cancel();
        });

        let result = cancellable.await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_cancellation_handle_is_cancelled() {
        let handle = CancellationHandle::default();
        assert!(!handle.is_cancelled());

        handle.cancel();
        assert!(handle.is_cancelled());
    }

    #[tokio::test]
    async fn test_multiple_cancellable_futures_with_same_handle() {
        let handle = Arc::new(CancellationHandle::default());

        let future1 = CancellableFuture::new(async { 1 }, handle.clone());
        let future2 = CancellableFuture::new(async { 2 }, handle.clone());

        // Cancel before starting
        handle.cancel();

        let (result1, result2) = tokio::join!(future1, future2);

        assert!(result1.is_err());
        assert!(result2.is_err());
        assert!(matches!(result1.unwrap_err(), Cancelled));
        assert!(matches!(result2.unwrap_err(), Cancelled));
    }

    #[tokio::test]
    async fn test_cancellable_future_with_timeout() {
        let handle = Arc::new(CancellationHandle::default());
        let future = async {
            sleep(Duration::from_secs(1)).await;
            42
        };
        let cancellable = CancellableFuture::new(future, handle.clone());

        // Use timeout to ensure the test doesn't hang
        let result = timeout(Duration::from_millis(100), cancellable).await;

        // Should timeout because the future takes 1 second but we timeout after 100ms
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cancelled_display() {
        let cancelled = Cancelled;
        assert_eq!(format!("{}", cancelled), "Future has been cancelled");
    }
}
