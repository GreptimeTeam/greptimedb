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

use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use common_telemetry::{debug, error, info};
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::compaction::dedup_deque::DedupDeque;
use crate::compaction::picker::{Picker, PickerContext};
use crate::compaction::rate_limit::{
    BoxedRateLimitToken, CascadeRateLimiter, MaxInflightTaskLimiter, RateLimitToken, RateLimiter,
};
use crate::compaction::task::CompactionTask;
use crate::error::{Result, StopCompactionSchedulerSnafu};
use crate::manifest::region::RegionManifest;
use crate::region::{RegionWriterRef, SharedDataRef};
use crate::schema::RegionSchemaRef;
use crate::sst::AccessLayerRef;
use crate::version::LevelMetasRef;
use crate::wal::Wal;

/// Region compaction request.
pub struct CompactionRequestImpl<S: LogStore> {
    pub region_id: RegionId,
    pub sst_layer: AccessLayerRef,
    pub writer: RegionWriterRef,
    pub shared: SharedDataRef,
    pub manifest: RegionManifest,
    pub wal: Wal<S>,
}

impl<S: LogStore> CompactionRequestImpl<S> {
    #[inline]
    pub(crate) fn schema(&self) -> RegionSchemaRef {
        self.shared.version_control.current().schema().clone()
    }

    #[inline]
    pub(crate) fn levels(&self) -> LevelMetasRef {
        self.shared.version_control.current().ssts().clone()
    }
}

impl<S: LogStore> Request<RegionId> for CompactionRequestImpl<S> {
    #[inline]
    fn key(&self) -> RegionId {
        self.region_id
    }
}

pub trait Request<K>: Send + Sync + 'static {
    fn key(&self) -> K;
}

#[derive(Debug)]
pub struct SchedulerConfig {
    pub max_inflight_task: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_inflight_task: 4,
        }
    }
}

#[async_trait::async_trait]
pub trait Handler<R> {
    async fn handle_request(
        &self,
        req: R,
        token: BoxedRateLimitToken,
        finish_notifier: Arc<Notify>,
    ) -> Result<()>;
}

/// CompactionScheduler defines a set of API to schedule compaction tasks.
#[async_trait]
pub trait Scheduler<R>: Debug {
    /// Schedules a compaction request.
    /// Returns true if request is scheduled. Returns false if task queue already
    /// contains the request with same region id.
    async fn schedule(&self, request: R) -> Result<bool>;

    /// Stops compaction scheduler.
    async fn stop(&self) -> Result<()>;
}

/// Compaction task scheduler based on local state.
pub struct LocalScheduler<R: Request<T>, T> {
    request_queue: Arc<RwLock<DedupDeque<T, R>>>,
    cancel_token: CancellationToken,
    task_notifier: Arc<Notify>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

impl<R, K> Debug for LocalScheduler<R, K>
where
    R: Request<K> + Send + Sync,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalCompactionScheduler<...>").finish()
    }
}

#[async_trait]
impl<R, K> Scheduler<R> for LocalScheduler<R, K>
where
    R: Request<K> + Send,
    K: Debug + Eq + Hash + Clone + Send + Sync + 'static,
{
    async fn schedule(&self, request: R) -> Result<bool> {
        debug!(
            "Schedule request: {:?}, queue size: {}",
            request.key(),
            self.remaining_requests()
        );
        let mut queue = self.request_queue.write().unwrap();
        let res = queue.push_back(request.key(), request);
        self.task_notifier.notify_one();
        Ok(res)
    }

    async fn stop(&self) -> Result<()> {
        self.cancel_token.cancel();
        let handle = { self.join_handle.lock().unwrap().take() };
        if let Some(handle) = handle {
            handle.await.context(StopCompactionSchedulerSnafu)?;
        }
        Ok(())
    }
}

impl<R, K> LocalScheduler<R, K>
where
    R: Request<K>,
    K: Debug + Eq + Hash + Clone + Send + Sync + 'static,
{
    pub fn new<H>(config: SchedulerConfig, handler: H) -> Self
    where
        H: Handler<R> + Send + Sync + 'static,
    {
        let request_queue: Arc<RwLock<DedupDeque<K, R>>> =
            Arc::new(RwLock::new(DedupDeque::default()));
        let cancel_token = CancellationToken::new();
        let task_notifier = Arc::new(Notify::new());

        let handle_loop = HandlerLoop {
            task_notifier: task_notifier.clone(),
            req_queue: request_queue.clone(),
            cancel_token: cancel_token.child_token(),
            limiter: Arc::new(CascadeRateLimiter::new(vec![Box::new(
                MaxInflightTaskLimiter::new(config.max_inflight_task),
            )])),
            request_handler: handler,
        };
        let join_handle = common_runtime::spawn_bg(async move {
            debug!("Compaction handler loop spawned");
            handle_loop.run().await;
        });
        Self {
            join_handle: Mutex::new(Some(join_handle)),
            request_queue,
            cancel_token,
            task_notifier,
        }
    }

    fn remaining_requests(&self) -> usize {
        self.request_queue.read().unwrap().len()
    }
}

struct HandlerLoop<R, K, H> {
    req_queue: Arc<RwLock<DedupDeque<K, R>>>,
    cancel_token: CancellationToken,
    task_notifier: Arc<Notify>,
    request_handler: H,
    limiter: Arc<CascadeRateLimiter<R>>,
}

impl<R: Request<K>, K: Debug + Clone + Eq + Hash + Send + 'static, H: Handler<R>>
    HandlerLoop<R, K, H>
{
    /// Runs region compaction requests dispatch loop.
    pub async fn run(&self) {
        let task_notifier = self.task_notifier.clone();
        let limiter = self.limiter.clone();
        loop {
            tokio::select! {
                _ = task_notifier.notified() => {
                    // poll requests as many as possible until rate limited, and then wait for
                    // notification (some task's finished).
                    debug!("Notified, queue size: {:?}", self.req_queue.read().unwrap().len());
                    while let Some((task_key,  req)) = self.poll_task().await{
                        if let Ok(token) = limiter.acquire_token(&req) {
                            debug!("Executing request: {:?}", task_key);
                            if let Err(e) = self.handle_request(req, token, self.task_notifier.clone()).await {
                                error!(e; "Failed to submit request: {:?}", task_key);
                            } else {
                                info!("Submitted task: {:?}", task_key);
                            }
                        } else {
                            // rate limited, put back to req queue to wait for next schedule
                            debug!("Put back request {:?}, queue size: {}", task_key, self.req_queue.read().unwrap().len());
                            self.put_back_req(task_key, req).await;
                            break;
                        }
                    }
                }
                _ = self.cancel_token.cancelled() => {
                    info!("Task scheduler stopped.");
                    return;
                }
            }
        }
    }

    #[inline]
    async fn poll_task(&self) -> Option<(K, R)> {
        let mut queue = self.req_queue.write().unwrap();
        queue.pop_front()
    }

    /// Puts request back to the front of request queue.
    #[inline]
    async fn put_back_req(&self, key: K, req: R) {
        let mut queue = self.req_queue.write().unwrap();
        queue.push_front(key, req);
    }

    // Handles request, submit task to bg runtime.
    async fn handle_request(
        &self,
        req: R,
        token: BoxedRateLimitToken,
        finish_notifier: Arc<Notify>,
    ) -> Result<()> {
        self.request_handler
            .handle_request(req, token, finish_notifier)
            .await
    }
}

pub struct CompactionHandler<R: Request<K>, P: Picker<R, T>, T: CompactionTask, K> {
    pub picker: P,
    pub _phantom: PhantomData<(R, K, T)>,
}

impl<R: Request<K>, P: Picker<R, T>, T: CompactionTask, K> CompactionHandler<R, P, T, K> {
    pub fn new(picker: P) -> Self {
        Self {
            picker,
            _phantom: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<R, P, T, K> Handler<R> for CompactionHandler<R, P, T, K>
where
    R: Request<K>,
    P: Picker<R, T> + Send + Sync,
    T: CompactionTask,
    K: Debug + Clone + Eq + Hash + Send + Sync + 'static,
{
    async fn handle_request(
        &self,
        req: R,
        token: BoxedRateLimitToken,
        finish_notifier: Arc<Notify>,
    ) -> Result<()> {
        let region_id = req.key();
        let Some(task) = self.picker.pick(&PickerContext {}, &req)? else {
            info!("No file needs compaction in region: {:?}", region_id);
            return Ok(());
        };

        debug!("Compaction task, region: {:?}, task: {:?}", region_id, task);
        // TODO(hl): we need to keep a track of task handle here to allow task cancellation.
        common_runtime::spawn_bg(async move {
            if let Err(e) = task.run().await {
                // TODO(hl): maybe resubmit compaction task on failure?
                error!(e; "Failed to compact region: {:?}", region_id);
            } else {
                info!("Successfully compacted region: {:?}", region_id);
            }
            // releases rate limit token
            token.try_release();
            // notify scheduler to schedule next task when current task finishes.
            finish_notifier.notify_one();
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::compaction::rate_limit::MaxInflightTaskLimiter;

    struct CountdownLatch {
        counter: std::sync::Mutex<usize>,
        notifies: std::sync::RwLock<Vec<Arc<Notify>>>,
    }

    impl CountdownLatch {
        fn new(size: usize) -> Self {
            Self {
                counter: std::sync::Mutex::new(size),
                notifies: std::sync::RwLock::new(vec![]),
            }
        }

        fn countdown(&self) {
            let mut counter = self.counter.lock().unwrap();
            if *counter >= 1 {
                *counter -= 1;
                if *counter == 0 {
                    let notifies = self.notifies.read().unwrap();
                    for waiter in notifies.iter() {
                        waiter.notify_one();
                    }
                }
            }
        }

        async fn wait(&self) {
            let notify = Arc::new(Notify::new());
            {
                let notify = notify.clone();
                let mut notifies = self.notifies.write().unwrap();
                notifies.push(notify);
            }
            notify.notified().await
        }
    }

    #[tokio::test]
    async fn test_schedule_handler() {
        common_telemetry::init_default_ut_logging();
        let queue = Arc::new(RwLock::new(DedupDeque::default()));
        let latch = Arc::new(CountdownLatch::new(2));
        let latch_cloned = latch.clone();
        let handler = Arc::new(HandlerLoop {
            req_queue: queue.clone(),
            cancel_token: Default::default(),
            task_notifier: Arc::new(Default::default()),
            request_handler: MockHandler {
                cb: move || {
                    latch_cloned.countdown();
                },
            },
            limiter: Arc::new(CascadeRateLimiter::new(vec![Box::new(
                MaxInflightTaskLimiter::new(3),
            )])),
        });

        let handler_cloned = handler.clone();
        common_runtime::spawn_bg(async move { handler_cloned.run().await });

        queue.write().unwrap().push_back(1, MockRequest::default());
        handler.task_notifier.notify_one();
        queue.write().unwrap().push_back(2, MockRequest::default());
        handler.task_notifier.notify_one();

        tokio::time::timeout(Duration::from_secs(1), latch.wait())
            .await
            .unwrap();
    }

    #[derive(Default, Debug)]
    struct MockRequest {
        region_id: RegionId,
    }

    struct MockHandler<F> {
        cb: F,
    }

    #[async_trait::async_trait]
    impl<F> Handler<MockRequest> for MockHandler<F>
    where
        F: Fn() + Send + Sync,
    {
        async fn handle_request(
            &self,
            _req: MockRequest,
            token: BoxedRateLimitToken,
            finish_notifier: Arc<Notify>,
        ) -> Result<()> {
            (self.cb)();
            token.try_release();
            finish_notifier.notify_one();
            Ok(())
        }
    }

    impl Request<RegionId> for MockRequest {
        fn key(&self) -> RegionId {
            self.region_id
        }
    }

    #[tokio::test]
    async fn test_scheduler() {
        let latch = Arc::new(CountdownLatch::new(2));
        let latch_cloned = latch.clone();

        let handler = MockHandler {
            cb: move || {
                latch_cloned.countdown();
            },
        };
        let scheduler: LocalScheduler<MockRequest, RegionId> = LocalScheduler::new(
            SchedulerConfig {
                max_inflight_task: 3,
            },
            handler,
        );

        scheduler
            .schedule(MockRequest { region_id: 1 })
            .await
            .unwrap();

        scheduler
            .schedule(MockRequest { region_id: 2 })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), latch.wait())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_scheduler_many() {
        common_telemetry::init_default_ut_logging();
        let task_size = 100;

        let latch = Arc::new(CountdownLatch::new(task_size));
        let latch_clone = latch.clone();

        let handler = MockHandler {
            cb: move || {
                latch_clone.countdown();
            },
        };

        let config = SchedulerConfig {
            max_inflight_task: 3,
        };
        let scheduler = LocalScheduler::new(config, handler);

        for i in 0..task_size {
            scheduler
                .schedule(MockRequest {
                    region_id: i as RegionId,
                })
                .await
                .unwrap();
        }

        tokio::time::timeout(Duration::from_secs(3), latch.wait())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_scheduler_interval() {
        common_telemetry::init_default_ut_logging();
        let task_size = 100;
        let latch = Arc::new(CountdownLatch::new(task_size));
        let latch_clone = latch.clone();

        let handler = MockHandler {
            cb: move || {
                latch_clone.countdown();
            },
        };

        let config = SchedulerConfig {
            max_inflight_task: 3,
        };
        let scheduler = LocalScheduler::new(config, handler);

        for i in 0..task_size / 2 {
            scheduler
                .schedule(MockRequest {
                    region_id: i as RegionId,
                })
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        for i in task_size / 2..task_size {
            scheduler
                .schedule(MockRequest {
                    region_id: i as RegionId,
                })
                .await
                .unwrap();
        }

        tokio::time::timeout(Duration::from_secs(6), latch.wait())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_schedule_duplicate_tasks() {
        common_telemetry::init_default_ut_logging();
        let handler = MockHandler { cb: || {} };
        let config = SchedulerConfig {
            max_inflight_task: 3,
        };
        let scheduler = LocalScheduler::new(config, handler);

        let mut scheduled_task = 0;
        for _ in 0..10 {
            if scheduler
                .schedule(MockRequest { region_id: 1 })
                .await
                .unwrap()
            {
                scheduled_task += 1;
            }
        }
        scheduler.stop().await.unwrap();
        debug!("Schedule tasks: {}", scheduled_task);
        assert!(scheduled_task < 10);
    }
}
