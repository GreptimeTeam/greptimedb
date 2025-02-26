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
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};

use common_telemetry::warn;
use snafu::{ensure, OptionExt, ResultExt};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{InvalidSchedulerStateSnafu, InvalidSenderSnafu, Result, StopSchedulerSnafu};

pub type Job = Pin<Box<dyn Future<Output = ()> + Send>>;

///The state of scheduler
const STATE_RUNNING: u8 = 0;
const STATE_STOP: u8 = 1;
const STATE_AWAIT_TERMINATION: u8 = 2;

/// [Scheduler] defines a set of API to schedule Jobs
#[async_trait::async_trait]
pub trait Scheduler: Send + Sync {
    /// Schedules a Job
    fn schedule(&self, job: Job) -> Result<()>;

    /// Stops scheduler. If `await_termination` is set to true, the scheduler will wait until all tasks are processed.
    async fn stop(&self, await_termination: bool) -> Result<()>;
}

pub type SchedulerRef = Arc<dyn Scheduler>;

/// Request scheduler based on local state.
pub struct LocalScheduler {
    /// Sends jobs to flume bounded channel
    sender: RwLock<Option<async_channel::Sender<Job>>>,
    /// Task handles
    handles: Mutex<Vec<JoinHandle<()>>>,
    /// Token used to halt the scheduler
    cancel_token: CancellationToken,
    /// State of scheduler
    state: Arc<AtomicU8>,
}

impl LocalScheduler {
    /// Starts a new scheduler.
    ///
    /// concurrency: the number of bounded receiver
    pub fn new(concurrency: usize) -> Self {
        let (tx, rx) = async_channel::unbounded();
        let token = CancellationToken::new();
        let state = Arc::new(AtomicU8::new(STATE_RUNNING));

        let mut handles = Vec::with_capacity(concurrency);

        for _ in 0..concurrency {
            let child = token.child_token();
            let receiver = rx.clone();
            let state_clone = state.clone();
            let handle = common_runtime::spawn_global(async move {
                while state_clone.load(Ordering::Relaxed) == STATE_RUNNING {
                    tokio::select! {
                        _ = child.cancelled() => {
                            break;
                        }
                        req_opt = receiver.recv() =>{
                            if let Ok(job) = req_opt {
                                job.await;
                            }
                        }
                    }
                }
                // When task scheduler is cancelled, we will wait all task finished
                if state_clone.load(Ordering::Relaxed) == STATE_AWAIT_TERMINATION {
                    // recv_async waits until all sender's been dropped.
                    while let Ok(job) = receiver.recv().await {
                        job.await;
                    }
                    state_clone.store(STATE_STOP, Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        Self {
            sender: RwLock::new(Some(tx)),
            cancel_token: token,
            handles: Mutex::new(handles),
            state,
        }
    }

    #[inline]
    fn is_running(&self) -> bool {
        self.state.load(Ordering::Relaxed) == STATE_RUNNING
    }
}

#[async_trait::async_trait]
impl Scheduler for LocalScheduler {
    fn schedule(&self, job: Job) -> Result<()> {
        ensure!(self.is_running(), InvalidSchedulerStateSnafu);

        self.sender
            .read()
            .unwrap()
            .as_ref()
            .context(InvalidSchedulerStateSnafu)?
            .try_send(job)
            .map_err(|_| InvalidSenderSnafu {}.build())
    }

    /// if await_termination is true, scheduler will wait all tasks finished before stopping
    async fn stop(&self, await_termination: bool) -> Result<()> {
        ensure!(self.is_running(), InvalidSchedulerStateSnafu);
        let state = if await_termination {
            STATE_AWAIT_TERMINATION
        } else {
            STATE_STOP
        };
        self.sender.write().unwrap().take();
        self.state.store(state, Ordering::Relaxed);
        self.cancel_token.cancel();

        futures::future::join_all(self.handles.lock().await.drain(..))
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .context(StopSchedulerSnafu)?;

        Ok(())
    }
}

impl Drop for LocalScheduler {
    fn drop(&mut self) {
        if self.state.load(Ordering::Relaxed) != STATE_STOP {
            warn!("scheduler should be stopped before dropping, which means the state of scheduler must be STATE_STOP");

            // We didn't call `stop()` so we cancel all background workers here.
            self.sender.write().unwrap().take();
            self.cancel_token.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI32;
    use std::sync::Arc;

    use tokio::sync::Barrier;
    use tokio::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_sum_cap() {
        let task_size = 1000;
        let sum = Arc::new(AtomicI32::new(0));
        let local = LocalScheduler::new(task_size);

        for _ in 0..task_size {
            let sum_clone = sum.clone();
            local
                .schedule(Box::pin(async move {
                    sum_clone.fetch_add(1, Ordering::Relaxed);
                }))
                .unwrap();
        }
        local.stop(true).await.unwrap();
        assert_eq!(sum.load(Ordering::Relaxed), 1000);
    }

    #[tokio::test]
    async fn test_sum_consumer_num() {
        let task_size = 1000;
        let sum = Arc::new(AtomicI32::new(0));
        let local = LocalScheduler::new(3);
        let mut target = 0;
        for _ in 0..task_size {
            let sum_clone = sum.clone();
            let ok = local
                .schedule(Box::pin(async move {
                    sum_clone.fetch_add(1, Ordering::Relaxed);
                }))
                .is_ok();
            if ok {
                target += 1;
            }
        }
        local.stop(true).await.unwrap();
        assert_eq!(sum.load(Ordering::Relaxed), target);
    }

    #[tokio::test]
    async fn test_scheduler_many() {
        let task_size = 1000;

        let barrier = Arc::new(Barrier::new(task_size + 1));
        let local: LocalScheduler = LocalScheduler::new(task_size);

        for _ in 0..task_size {
            let barrier_clone = barrier.clone();
            local
                .schedule(Box::pin(async move {
                    barrier_clone.wait().await;
                }))
                .unwrap();
        }
        barrier.wait().await;
        local.stop(true).await.unwrap();
    }

    #[tokio::test]
    async fn test_scheduler_continuous_stop() {
        let sum = Arc::new(AtomicI32::new(0));
        let local = Arc::new(LocalScheduler::new(1000));

        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone = barrier.clone();
        let local_stop = local.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            local_stop.stop(true).await.unwrap();
            barrier_clone.wait().await;
        });

        let target = Arc::new(AtomicI32::new(0));
        let local_task = local.clone();
        let target_clone = target.clone();
        let sum_clone = sum.clone();
        tokio::spawn(async move {
            loop {
                let sum_c = sum_clone.clone();
                let ok = local_task
                    .schedule(Box::pin(async move {
                        sum_c.fetch_add(1, Ordering::Relaxed);
                    }))
                    .is_ok();
                if ok {
                    target_clone.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
                tokio::task::yield_now().await;
            }
        });
        barrier.wait().await;
        assert_eq!(sum.load(Ordering::Relaxed), target.load(Ordering::Relaxed));
    }
}
