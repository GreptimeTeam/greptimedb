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
use snafu::{OptionExt, ResultExt, ensure};
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

    /// Runs completion only after the job and its owned resources have been dropped.
    fn schedule_with_completion(&self, job: Job, completion: Job) -> Result<()> {
        self.schedule(Box::pin(async move {
            job.await;
            completion.await;
        }))
    }

    /// Stops scheduler. If `await_termination` is set to true, the scheduler will wait until all tasks are processed.
    async fn stop(&self, await_termination: bool) -> Result<()>;
}

pub type SchedulerRef = Arc<dyn Scheduler>;

/// A queued job with an optional completion future dispatched after the job exits.
struct ScheduledJob {
    job: Job,
    completion: Option<Job>,
}

impl ScheduledJob {
    /// Finishes the job before dispatching completion outside its runner.
    async fn run(self) {
        self.job.await;
        if let Some(completion) = self.completion {
            // The original job has exited. Notification must not occupy its runner.
            common_runtime::spawn_global(completion);
        }
    }
}

/// Request scheduler based on local state.
pub struct LocalScheduler {
    /// Sends jobs to flume bounded channel
    sender: RwLock<Option<async_channel::Sender<ScheduledJob>>>,
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
        let (tx, rx) = async_channel::unbounded::<ScheduledJob>();
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
                                job.run().await;
                            }
                        }
                    }
                }
                // When task scheduler is cancelled, we will wait all task finished
                if state_clone.load(Ordering::Relaxed) == STATE_AWAIT_TERMINATION {
                    // recv_async waits until all sender's been dropped.
                    while let Ok(job) = receiver.recv().await {
                        job.run().await;
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

    /// Enqueues work only while the scheduler and its sender remain available.
    fn submit(&self, job: ScheduledJob) -> Result<()> {
        ensure!(self.is_running(), InvalidSchedulerStateSnafu);
        self.sender
            .read()
            .unwrap()
            .as_ref()
            .context(InvalidSchedulerStateSnafu)?
            .try_send(job)
            .map_err(|_| InvalidSenderSnafu {}.build())
    }
}

#[async_trait::async_trait]
impl Scheduler for LocalScheduler {
    fn schedule(&self, job: Job) -> Result<()> {
        self.submit(ScheduledJob {
            job,
            completion: None,
        })
    }

    /// Queues the job and its completion together so completion cannot precede task exit.
    fn schedule_with_completion(&self, job: Job, completion: Job) -> Result<()> {
        self.submit(ScheduledJob {
            job,
            completion: Some(completion),
        })
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
            warn!(
                "scheduler should be stopped before dropping, which means the state of scheduler must be STATE_STOP"
            );

            // We didn't call `stop()` so we cancel all background workers here.
            self.sender.write().unwrap().take();
            self.cancel_token.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicI32;

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

#[cfg(test)]
mod wal_barrier_test {
    use std::time::Duration;

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_wal::config::object_store::{AckMode, ObjectStoreWalConfig, STANDALONE_GENERATION};
    use log_store::ObjectStoreLogStore;
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use store_api::logstore::LogStore;
    use store_api::logstore::provider::Provider;
    use store_api::storage::RegionId;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    use super::{LocalScheduler, Scheduler};
    use crate::error::Result;
    use crate::flush::wait_for_durable_wal;
    use crate::schedule::CancellableTaskState;
    use crate::wal::Wal;

    const WAIT: Duration = Duration::from_secs(30);

    /// A graceful stop of the flush scheduler waits for its running jobs and
    /// runs its queued ones. Once the engine is stopping, a job that waits
    /// for the WAL to become durable gives up, whether it was running or
    /// queued, so the stop does not wait for a create that never completes,
    /// while a job whose wait is satisfied still proceeds.
    #[tokio::test]
    async fn test_stopping_releases_scheduled_waits_for_the_wal() {
        let config = ObjectStoreWalConfig {
            ack_mode: AckMode::Enqueued,
            flush_interval: Duration::from_secs(3600),
            ..Default::default()
        };
        let store = ObjectStoreLogStore::try_new(
            ObjectStore::new(Memory::default()).unwrap(),
            &config,
            0,
            STANDALONE_GENERATION,
        )
        .await
        .unwrap();
        let region = RegionId::new(1, 1);
        let provider =
            Provider::object_store_provider(region, config.node_prefix(0, STANDALONE_GENERATION));
        let barrier = Wal::new(store.clone()).durability_barrier(region, &provider);
        store.hold_creates();
        let entry = store.entry(vec![1], 0, region, &provider).unwrap();
        let entry_id = store
            .append_batch(vec![entry])
            .await
            .unwrap()
            .last_entry_ids[&region];
        let seal = {
            let store = store.clone();
            tokio::spawn(async move { store.seal_open_batch().await })
        };

        // One job runs and waits for the entry, the second is queued behind
        // it.
        let stopping = CancellationToken::new();
        let scheduler = LocalScheduler::new(1);
        let schedule_wait = |entry_id| {
            let (done_tx, done_rx) = oneshot::channel::<Result<()>>();
            let barrier = barrier.clone();
            let stopping = stopping.clone();
            scheduler
                .schedule(Box::pin(async move {
                    let state = CancellableTaskState::new();
                    let result =
                        wait_for_durable_wal(region, &barrier, entry_id, &state, &stopping).await;
                    let _ = done_tx.send(result);
                }))
                .unwrap();
            done_rx
        };
        let running = schedule_wait(entry_id);
        let queued = schedule_wait(entry_id);
        tokio::time::timeout(WAIT, store.wait_for_parked_creates(1))
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(WAIT, store.wait_for_durability_waits(1))
            .await
            .unwrap()
            .unwrap();

        stopping.cancel();
        tokio::time::timeout(WAIT, scheduler.stop(true))
            .await
            .expect("the scheduler must stop")
            .unwrap();
        for done in [running, queued] {
            let error = done.await.unwrap().unwrap_err();
            assert_eq!(StatusCode::Cancelled, error.status_code(), "{error:?}");
        }
        // A wait for nothing that is not durable is satisfied at once.
        let state = CancellableTaskState::new();
        wait_for_durable_wal(region, &barrier, 0, &state, &stopping)
            .await
            .unwrap();

        store.release_creates();
        tokio::time::timeout(WAIT, store.stop())
            .await
            .unwrap()
            .unwrap();
        seal.await.unwrap().unwrap();
    }
}
