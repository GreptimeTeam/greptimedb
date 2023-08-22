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
use std::sync::Arc;

use common_telemetry::info;
use snafu::{ensure, ResultExt};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{
    InvalidFlumeSenderSnafu, InvalidSchedulerStateSnafu, Result, StopSchedulerSnafu,
};

pub type Job = Pin<Box<dyn Future<Output = ()> + Send>>;

///The state of scheduler
const STATE_RUNNING: u8 = 0;
const STATE_STOP: u8 = 1;
const STATE_AWAIT_TERMINATION: u8 = 2;

/// The consumer count
const CONSUMER_NUM: u8 = 2;

/// [Scheduler] defines a set of API to schedule Jobs
#[async_trait::async_trait]
pub trait Scheduler {
    /// Schedules a Job
    async fn schedule(&self, req: Job) -> Result<()>;

    /// Stops scheduler. If `await_termination` is set to true, the scheduler will wait until all tasks are processed.
    async fn stop(&self, await_termination: bool) -> Result<()>;
}

/// Request scheduler based on local state.
pub struct LocalScheduler {
    /// Sends jobs to flume bounded channel
    sender: flume::Sender<Job>,
    /// Task handles
    handles: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
    /// Token used to halt the scheduler
    cancel_token: CancellationToken,
    /// State of scheduler
    state: Arc<AtomicU8>,
}

impl LocalScheduler {
    /// cap: flume bounded cap
    /// rev_num: the number of bounded receiver
    pub fn new(cap: usize, rev_num: usize) -> Self {
        let (tx, rx) = flume::bounded(cap);
        let token = CancellationToken::new();
        let state = Arc::new(AtomicU8::new(STATE_RUNNING));

        let mut handles = Vec::with_capacity(rev_num);

        for _ in 0..rev_num {
            let child = token.child_token().clone();
            let receiver = rx.clone();
            let state = Arc::clone(&state);
            let handle = common_runtime::spawn_bg(async move {
                while state.load(Ordering::Relaxed) == STATE_RUNNING {
                    info!("Task scheduler loop.");
                    tokio::select! {
                        _ = child.cancelled() => {
                            info!("Task scheduler cancelled.");
                            // Wait all task finished
                            if state.load(Ordering::Relaxed) == STATE_AWAIT_TERMINATION {
                                while let Ok(req) = receiver.try_recv() {
                                    req.await;
                                }
                                state.store(STATE_STOP, Ordering::Relaxed);
                            }
                            return;
                        }
                        req_opt = receiver.recv_async() =>{
                            if let Ok(req) = req_opt {
                                req.await;
                            }
                        }
                    }
                }
            });
            handles.push(handle);
        }

        Self {
            sender: tx,
            cancel_token: token,
            handles: Mutex::new(handles),
            state,
        }
    }

    #[inline]
    fn running(&self) -> bool {
        self.state.load(Ordering::Relaxed) == STATE_RUNNING
    }
}

#[async_trait::async_trait]
impl Scheduler for LocalScheduler {
    async fn schedule(&self, req: Job) -> Result<()> {
        ensure!(
            self.state.load(Ordering::Relaxed) == STATE_RUNNING,
            InvalidSchedulerStateSnafu
        );
        self.sender
            .send_async(req)
            .await
            .map_err(|_| InvalidFlumeSenderSnafu {}.build())
    }

    async fn stop(&self, await_termination: bool) -> Result<()> {
        ensure!(
            self.state.load(Ordering::Relaxed) == STATE_RUNNING,
            InvalidSchedulerStateSnafu
        );
        let state = if await_termination {
            STATE_AWAIT_TERMINATION
        } else {
            STATE_STOP
        };
        self.state.store(state, Ordering::Relaxed);
        self.cancel_token.cancel();

        for handle in self.handles.lock().await.drain(..) {
            handle.await.context(StopSchedulerSnafu)?;
        }

        Ok(())
    }
}

impl Drop for LocalScheduler {
    fn drop(&mut self) {
        self.state.store(STATE_STOP, Ordering::Relaxed);
        self.cancel_token.cancel();
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
        let local = LocalScheduler::new(3, task_size);

        for _ in 0..task_size {
            let sum = Arc::clone(&sum);
            local
                .schedule(Box::pin(async move {
                    tokio::time::sleep(Duration::from_micros(1)).await;
                    sum.fetch_add(1, Ordering::Relaxed);
                }))
                .await
                .unwrap();
        }
        local.stop(true).await.unwrap();
        assert_eq!(sum.load(Ordering::Relaxed), 1000);
    }

    #[tokio::test]
    async fn test_sum_consumer_num() {
        let task_size = 1000;
        let sum = Arc::new(AtomicI32::new(0));
        let local = LocalScheduler::new(task_size, 3);

        for _ in 0..task_size {
            let sum = Arc::clone(&sum);
            local
                .schedule(Box::pin(async move {
                    sum.fetch_add(1, Ordering::Relaxed);
                }))
                .await
                .unwrap();
        }

        local.stop(true).await.unwrap();

        assert_eq!(sum.load(Ordering::Relaxed), 1000);
    }

    #[tokio::test]
    async fn test_scheduler_many() {
        let task_size = 1000;

        let barrier = Arc::new(Barrier::new(task_size + 1));
        let local: LocalScheduler = LocalScheduler::new(10, task_size + 1);

        for _ in 0..task_size {
            let barrier_clone = barrier.clone();
            local
                .schedule(Box::pin(async move {
                    barrier_clone.wait().await;
                }))
                .await
                .unwrap();
        }
        barrier.wait().await;
        local.stop(true).await.unwrap();
    }

    #[tokio::test]
    async fn test_scheduler_continuous_stop() {
        let sum = Arc::new(AtomicI32::new(0));
        let local = Arc::new(LocalScheduler::new(1000, 1000));

        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone = barrier.clone();
        let local_stop = local.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            while local_stop.stop(true).await.is_ok() {}
            barrier_clone.wait().await;
        });

        let target = Arc::new(AtomicI32::new(0));
        let local_task = local.clone();
        let target_clone = Arc::clone(&target);
        let sum_clone = Arc::clone(&sum);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let sum_c = Arc::clone(&sum_clone);
                let ok = local_task
                    .schedule(Box::pin(async move {
                        sum_c.fetch_add(1, Ordering::Relaxed);
                    }))
                    .await
                    .is_ok();
                if ok {
                    target_clone.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }
        });
        barrier.wait().await;
        assert_eq!(sum.load(Ordering::Relaxed), target.load(Ordering::Relaxed));
    }
}
