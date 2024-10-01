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
use tokio::runtime::Handle;
pub use tokio::task::JoinHandle;
use tokio::time::Sleep;

use crate::runtime::{Dropper, RuntimeTrait};
use crate::Builder;

pub struct RuntimeThrottleSharedWithFuture {
    pub ratelimiter: Option<Ratelimiter>,
}

impl Debug for RuntimeThrottleSharedWithFuture {
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
    shared_with_future: Arc<RuntimeThrottleSharedWithFuture>,
    // Used to receive a drop signal when dropper is dropped, inspired by databend
    _dropper: Arc<Dropper>,
}

impl ThrottleableRuntime {
    #[allow(dead_code)]
    pub fn new(name: &str, priority: Priority, handle: Handle, dropper: Arc<Dropper>) -> Self {
        Self {
            name: name.to_string(),
            handle,
            shared_with_future: Arc::new(RuntimeThrottleSharedWithFuture {
                ratelimiter: priority.ratelimiter_count(),
            }),
            _dropper: dropper,
        }
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

    /// priority of this future
    handle: Arc<RuntimeThrottleSharedWithFuture>,

    /// count of pendings, track the pending count for test
    #[cfg(test)]
    pub pend_cnt: u32,

    state: State,
}

impl<F> ThrottleFuture<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    pub fn new(handle: Arc<RuntimeThrottleSharedWithFuture>, future: F) -> Self {
        Self {
            future,
            handle,
            #[cfg(test)]
            pend_cnt: 0,
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
                #[cfg(test)]
                {
                    *this.pend_cnt += 1;
                }
                *this.state = State::Throttled(Box::pin(tokio::time::sleep(wait)));
                match this.state.unwrap_backoff().poll_unpin(cx) {
                    Poll::Ready(_) => {
                        *this.state = State::Pollable;
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
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
            Poll::Pending => {
                #[cfg(test)]
                {
                    *this.pend_cnt += 1;
                }

                Poll::Pending
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Priority {
    VeryLow = 0,
    Low = 1,
    Middle = 2,
    High = 3,
    VeryHigh = 4,
}
impl Priority {
    pub fn ratelimiter_count(&self) -> Option<Ratelimiter> {
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
                .unwrap()
                .into()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use common_telemetry::debug;

    use super::Priority;
    use crate::runtime::{BuilderBuild, RuntimeTrait};
    use crate::{Builder, Runtime};

    // configurations
    const TEST_GENERATE_DIR: &str = "test_generate_dir";
    const WORKLOAD_TYPE_COUNT: u64 = 4;

    fn compute_pi_str(precision: usize) -> String {
        let mut pi = 0.0;
        let mut sign = 1.0;

        for i in 0..precision {
            pi += sign / (2 * i + 1) as f64;
            sign *= -1.0;
        }

        pi *= 4.0;
        format!("{:.prec$}", pi, prec = precision)
    }
    #[test]
    fn test_compute_pi_str_time() {
        let start = std::time::Instant::now();
        compute_pi_str(10);
        debug!("elapsed {}", start.elapsed().as_micros());
    }

    async fn workload_compute_heavily() {
        let prefix = 10;

        for _ in 0..3000 {
            let _ = compute_pi_str(prefix);
            tokio::task::yield_now().await;
            std::thread::yield_now();
        }
    }
    async fn workload_compute_heavily2() {
        let prefix = 30;
        for _ in 0..2000 {
            let _ = compute_pi_str(prefix);
            tokio::task::yield_now().await;
            std::thread::yield_now();
        }
    }
    async fn workload_write_file(_idx: u64) {
        use tokio::io::AsyncWriteExt;
        let prefix = 50;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(format!("{}/pi_{}", TEST_GENERATE_DIR, prefix))
            .await
            .unwrap();
        for i in 0..200 {
            let pi = compute_pi_str(prefix);

            if i % 2 == 0 {
                file.write_all(pi.as_bytes()).await.unwrap();
            }
        }
    }
    async fn workload_spawn_blocking_write_file() {
        use std::io::Write;
        let prefix = 100;
        let mut file = Some(
            std::fs::OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(format!("{}/pi_{}", TEST_GENERATE_DIR, prefix))
                .unwrap(),
        );
        for i in 0..100 {
            let pi = compute_pi_str(prefix);
            if i % 2 == 0 {
                let mut file1 = file.take().unwrap();
                file = Some(
                    tokio::task::spawn_blocking(move || {
                        file1.write_all(pi.as_bytes()).unwrap();
                        file1
                    })
                    .await
                    .unwrap(),
                );
            }
        }
    }

    #[test]
    fn test_diff_workload_priority() {
        common_telemetry::init_default_ut_logging();
        let _ = std::fs::create_dir_all(TEST_GENERATE_DIR);
        let priorities = [
            Priority::VeryLow,
            Priority::Low,
            Priority::Middle,
            Priority::High,
            Priority::VeryHigh,
        ];
        for i in 0..WORKLOAD_TYPE_COUNT {
            for p in priorities.iter() {
                let runtime: Runtime = Builder::default()
                    .runtime_name("test")
                    .thread_name("test")
                    .worker_threads(8)
                    .priority(*p)
                    .build()
                    .expect("Fail to create runtime");
                let runtime2 = runtime.clone();
                runtime.block_on(test_spec_priority_and_workload(*p, runtime2, i));
            }
        }
    }

    pub async fn test_spec_priority_and_workload(
        priority: Priority,
        runtime: Runtime,
        workload_id: u64,
    ) {
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        debug!(
            "testing cpu usage for priority {:?} workload_id {}",
            priority, workload_id,
        );
        // start monitor thread
        let mut tasks = vec![];
        let start = std::time::Instant::now();
        for i in 0..500 {
            // persist cpu usage in json: {priority}.{workload_id}
            match workload_id {
                0 => {
                    tasks.push(runtime.spawn(workload_compute_heavily()));
                }
                1 => {
                    tasks.push(runtime.spawn(workload_compute_heavily2()));
                }
                2 => {
                    tasks.push(runtime.spawn(workload_spawn_blocking_write_file()));
                }
                3 => {
                    tasks.push(runtime.spawn(workload_write_file(i)));
                }
                id => {
                    panic!("invalid workload_id {}", id);
                }
            }
        }
        for task in tasks {
            task.await.unwrap();
        }
        let elapsed = start.elapsed();
        debug!(
            "test cpu usage for priority {:?} workload_id {} elapsed {}ms",
            priority,
            workload_id,
            elapsed.as_millis()
        );
    }
}
