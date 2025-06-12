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
//! See example in [frontend::stream_wrapper::CancellableStreamWrapper].

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

    pub fn await_cancel(self: &Arc<Self>) -> Cancellation {
        Cancellation {
            handle: self.clone(),
        }
    }
}

pub struct Cancellation {
    handle: Arc<CancellationHandle>,
}

impl Future for Cancellation {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.handle.is_cancelled() {
            return Poll::Ready(());
        }
        self.handle.waker().register(cx.waker());
        Poll::Pending
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

    use crate::cancellation_handle::CancellationHandle;

    #[tokio::test]
    async fn test_cancel() {
        let handle = Arc::new(CancellationHandle::default());
        let cancellation = handle.await_cancel();
        tokio::spawn(async move {
            cancellation.await;
            println!("task cancelled");
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        });
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        handle.cancel();
        println!("issue cancelled");
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}
