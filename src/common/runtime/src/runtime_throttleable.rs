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

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::FutureExt;
use ratelimit::Ratelimiter;
use snafu::ResultExt;
use tokio::runtime::Handle;
pub use tokio::task::JoinHandle;
use tokio::time::Sleep;

use crate::error::{BuildRuntimeRateLimiterSnafu, Result};
use crate::runtime::{Dropper, Priority, RuntimeTrait};
use crate::Builder;

struct RuntimeRateLimiter {
    pub ratelimiter: Option<Ratelimiter>,
}

impl Debug for RuntimeRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeThrottleShareWithFuture")
            .field(
                "ratelimiter_max_tokens",
                &self.ratelimiter.as_ref().map(|v| v.max_tokens()),
            )
            .field(
                "ratelimiter_refill_amount",
                &self.ratelimiter.as_ref().map(|v| v.refill_amount()),
            )
            .finish()
    }
}

/// A runtime to run future tasks
#[derive(Clone, Debug)]
pub struct ThrottleableRuntime {
    name: String,
    handle: Handle,
    shared_with_future: Arc<RuntimeRateLimiter>,
    // Used to receive a drop signal when dropper is dropped, inspired by databend
    _dropper: Arc<Dropper>,
}

impl ThrottleableRuntime {
    pub(crate) fn new(
        name: &str,
        priority: Priority,
        handle: Handle,
        dropper: Arc<Dropper>,
    ) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            handle,
            shared_with_future: Arc::new(RuntimeRateLimiter {
                ratelimiter: priority.ratelimiter_count()?,
            }),
            _dropper: dropper,
        })
    }
}

impl RuntimeTrait for ThrottleableRuntime {
    fn builder() -> Builder {
        Builder::default()
    }

    /// Spawn a future and execute it in this thread pool
    ///
    /// Similar to tokio::runtime::Runtime::spawn()
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle
            .spawn(ThrottleFuture::new(self.shared_with_future.clone(), future))
    }

    /// Run the provided function on an executor dedicated to blocking
    /// operations.
    fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.handle.spawn_blocking(func)
    }

    /// Run a future to complete, this is the runtime's entry point
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

enum State {
    Pollable,
    Throttled(Pin<Box<Sleep>>),
}

impl State {
    fn unwrap_backoff(&mut self) -> &mut Pin<Box<Sleep>> {
        match self {
            State::Throttled(sleep) => sleep,
            _ => panic!("unwrap_backoff failed"),
        }
    }
}

#[pin_project::pin_project]
pub struct ThrottleFuture<F: Future + Send + 'static> {
    #[pin]
    future: F,

    /// RateLimiter of this future
    handle: Arc<RuntimeRateLimiter>,

    state: State,
}

impl<F> ThrottleFuture<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn new(handle: Arc<RuntimeRateLimiter>, future: F) -> Self {
        Self {
            future,
            handle,
            state: State::Pollable,
        }
    }
}

impl<F> Future for ThrottleFuture<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    type Output = F::Output;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.state {
            State::Pollable => {}
            State::Throttled(ref mut sleep) => match sleep.poll_unpin(cx) {
                Poll::Ready(_) => {
                    *this.state = State::Pollable;
                }
                Poll::Pending => return Poll::Pending,
            },
        };

        if let Some(ratelimiter) = &this.handle.ratelimiter {
            if let Err(wait) = ratelimiter.try_wait() {
                *this.state = State::Throttled(Box::pin(tokio::time::sleep(wait)));
                match this.state.unwrap_backoff().poll_unpin(cx) {
                    Poll::Ready(_) => {
                        *this.state = State::Pollable;
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
            }
        }

        let poll_res = this.future.poll(cx);

        match poll_res {
            Poll::Ready(r) => Poll::Ready(r),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Priority {
    fn ratelimiter_count(&self) -> Result<Option<Ratelimiter>> {
        let max = 8000;
        let gen_per_10ms = match self {
            Priority::VeryLow => Some(2000),
            Priority::Low => Some(4000),
            Priority::Middle => Some(6000),
            Priority::High => Some(8000),
            Priority::VeryHigh => None,
        };
        if let Some(gen_per_10ms) = gen_per_10ms {
            Ratelimiter::builder(gen_per_10ms, Duration::from_millis(10)) // generate poll count per 10ms
                .max_tokens(max) // reserved token for batch request
                .build()
                .context(BuildRuntimeRateLimiterSnafu)
                .map(Some)
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {

    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;
    use tokio::time::Duration;

    use super::*;
    use crate::runtime::BuilderBuild;

    #[tokio::test]
    async fn test_throttleable_runtime_spawn_simple() {
        for p in [
            Priority::VeryLow,
            Priority::Low,
            Priority::Middle,
            Priority::High,
            Priority::VeryHigh,
        ] {
            let runtime: ThrottleableRuntime = Builder::default()
                .runtime_name("test")
                .thread_name("test")
                .worker_threads(8)
                .priority(p)
                .build()
                .expect("Fail to create runtime");

            // Spawn a simple future that returns 42
            let handle = runtime.spawn(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                42
            });
            let result = handle.await.expect("Task panicked");
            assert_eq!(result, 42);
        }
    }

    #[tokio::test]
    async fn test_throttleable_runtime_spawn_complex() {
        let tempdir = tempfile::tempdir().unwrap();
        for p in [
            Priority::VeryLow,
            Priority::Low,
            Priority::Middle,
            Priority::High,
            Priority::VeryHigh,
        ] {
            let runtime: ThrottleableRuntime = Builder::default()
                .runtime_name("test")
                .thread_name("test")
                .worker_threads(8)
                .priority(p)
                .build()
                .expect("Fail to create runtime");
            let tempdirpath = tempdir.path().to_path_buf();
            let handle = runtime.spawn(async move {
                let mut file = File::create(tempdirpath.join("test.txt")).await.unwrap();
                file.write_all(b"Hello, world!").await.unwrap();
                42
            });
            let result = handle.await.expect("Task panicked");
            assert_eq!(result, 42);
        }
    }
}
