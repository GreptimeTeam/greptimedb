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

use std::marker::PhantomData;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use common_telemetry::{debug, info};
use snafu::ResultExt;
use store_api::logstore::LogStore;
use table::metadata::TableId;
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

/// Table compaction request.
pub struct CompactionRequestImpl<S: LogStore> {
    table_id: TableId,
    pub levels: LevelMetasRef,
    pub schema: RegionSchemaRef,
    pub sst_layer: AccessLayerRef,
    pub writer: RegionWriterRef,
    pub shared: SharedDataRef,
    pub manifest: RegionManifest,
    pub wal: Wal<S>,
}

impl<S: LogStore> CompactionRequest for CompactionRequestImpl<S> {
    #[inline]
    fn table_id(&self) -> TableId {
        self.table_id
    }
}

pub trait CompactionRequest: Send + Sync + 'static {
    fn table_id(&self) -> TableId;
}

#[derive(Debug)]
pub struct CompactionSchedulerConfig {
    max_inflight_task: usize,
}

impl Default for CompactionSchedulerConfig {
    fn default() -> Self {
        Self {
            max_inflight_task: 16,
        }
    }
}

/// CompactionScheduler defines a set of API to schedule compaction tasks.
#[async_trait]
pub trait CompactionScheduler<R> {
    /// Schedules a compaction request.
    /// Returns true if request is scheduled. Returns false if task queue already
    /// contains the request with same table id.
    async fn schedule(&self, request: R) -> Result<bool>;

    /// Stops compaction scheduler.
    async fn stop(&self) -> Result<()>;
}

/// Compaction task scheduler based on local state.
#[allow(unused)]
pub struct LocalCompactionScheduler<R: CompactionRequest> {
    request_queue: Arc<RwLock<DedupDeque<TableId, R>>>,
    cancel_token: CancellationToken,
    task_notifier: Arc<Notify>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

#[async_trait]
impl<R> CompactionScheduler<R> for LocalCompactionScheduler<R>
where
    R: CompactionRequest + Send + Sync,
{
    async fn schedule(&self, request: R) -> Result<bool> {
        debug!(
            "Schedule request: {}, queue size: {}",
            request.table_id(),
            self.remaining_requests().await
        );
        let mut queue = self.request_queue.write().unwrap();
        let res = queue.push_back(request.table_id(), request);
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

#[allow(unused)]
impl<R> LocalCompactionScheduler<R>
where
    R: CompactionRequest,
{
    pub fn new<P, T>(config: CompactionSchedulerConfig, picker: P) -> Self
    where
        T: CompactionTask,
        P: Picker<R, T> + Send + Sync,
    {
        let request_queue: Arc<RwLock<DedupDeque<TableId, R>>> =
            Arc::new(RwLock::new(DedupDeque::default()));
        let cancel_token = CancellationToken::new();
        let task_notifier = Arc::new(Notify::new());

        let handler = CompactionHandler {
            task_notifier: task_notifier.clone(),
            req_queue: request_queue.clone(),
            cancel_token: cancel_token.child_token(),
            limiter: Arc::new(CascadeRateLimiter::new(vec![Box::new(
                MaxInflightTaskLimiter::new(config.max_inflight_task),
            )])),
            picker,
            _phantom_data: PhantomData::<T>::default(),
        };
        let join_handle = common_runtime::spawn_bg(async move {
            debug!("Compaction handler loop spawned");
            handler.run().await;
        });
        Self {
            join_handle: Mutex::new(Some(join_handle)),
            request_queue,
            cancel_token,
            task_notifier,
        }
    }

    async fn remaining_requests(&self) -> usize {
        self.request_queue.read().unwrap().len()
    }
}

#[allow(unused)]
struct CompactionHandler<R, T: CompactionTask, P: Picker<R, T>> {
    req_queue: Arc<RwLock<DedupDeque<TableId, R>>>,
    cancel_token: CancellationToken,
    task_notifier: Arc<Notify>,
    limiter: Arc<CascadeRateLimiter<R>>,
    picker: P,
    _phantom_data: PhantomData<T>,
}

#[allow(unused)]
impl<R: CompactionRequest, T: CompactionTask, P: Picker<R, T>> CompactionHandler<R, T, P> {
    /// Runs table compaction requests dispatch loop.
    pub async fn run(&self) {
        let task_notifier = self.task_notifier.clone();
        let limiter = self.limiter.clone();
        loop {
            tokio::select! {
                _ = task_notifier.notified() => {
                    // poll requests as many as possible until rate limited, and then wait for
                    // notification (some task's finished).
                    debug!("Notified, queue size: {:?}", self.req_queue.read().unwrap().len());
                    while let Some((table_id,  req)) = self.poll_task().await {
                        if let Ok(token) = limiter.acquire_token(&req) {
                            debug!("Executing compaction request: {}", table_id);
                            self.handle_compaction_request(req, token).await;
                        } else {
                            // compaction rate limited, put back to req queue to wait for next
                            // schedule
                            debug!("Put back request {}, queue size: {}", table_id, self.req_queue.read().unwrap().len());
                            self.put_back_req(table_id, req).await;
                            break;
                        }
                    }
                }
                _ = self.cancel_token.cancelled() => {
                    info!("Compaction tasks scheduler stopped.");
                    return;
                }
            }
        }
    }

    #[inline]
    async fn poll_task(&self) -> Option<(TableId, R)> {
        let mut queue = self.req_queue.write().unwrap();
        queue.pop_front()
    }

    /// Puts request back to the front of request queue.
    #[inline]
    async fn put_back_req(&self, table_id: TableId, req: R) {
        let mut queue = self.req_queue.write().unwrap();
        queue.push_front(table_id, req);
    }

    // Handles compaction request, submit task to bg runtime.
    async fn handle_compaction_request(
        &self,
        mut req: R,
        token: BoxedRateLimitToken,
    ) -> Result<()> {
        let cloned_notify = self.task_notifier.clone();
        let table_id = req.table_id();
        let Some(task) = self.build_compaction_task(req).await? else {
            info!("No file needs compaction in table: {}", table_id);
            return Ok(());
        };

        // TODO(hl): we need to keep a track of task handle here to allow task cancellation.
        common_runtime::spawn_bg(async move {
            task.run().await; // TODO(hl): handle errors

            // releases rate limit token
            token.try_release();
            // notify scheduler to schedule next task when current task finishes.
            cloned_notify.notify_one();
        });

        Ok(())
    }

    // TODO(hl): generate compaction task(find SSTs to compact along with the output of compaction)
    async fn build_compaction_task(&self, req: R) -> crate::error::Result<Option<T>> {
        let ctx = PickerContext {};
        self.picker.pick(&ctx, &req)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::compaction::picker::tests::MockPicker;
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
        let picker = MockPicker::new(vec![Arc::new(move || {
            latch_cloned.countdown();
        })]);
        let handler = Arc::new(CompactionHandler {
            req_queue: queue.clone(),
            cancel_token: Default::default(),
            task_notifier: Arc::new(Default::default()),
            limiter: Arc::new(CascadeRateLimiter::new(vec![Box::new(
                MaxInflightTaskLimiter::new(3),
            )])),
            picker,
            _phantom_data: Default::default(),
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
        table_id: TableId,
    }

    impl CompactionRequest for MockRequest {
        fn table_id(&self) -> TableId {
            self.table_id
        }
    }

    #[tokio::test]
    async fn test_scheduler() {
        let latch = Arc::new(CountdownLatch::new(2));
        let latch_cloned = latch.clone();

        let picker = MockPicker::new(vec![Arc::new(move || latch_cloned.countdown())]);
        let scheduler = LocalCompactionScheduler::new(
            CompactionSchedulerConfig {
                max_inflight_task: 3,
            },
            picker,
        );

        scheduler
            .schedule(MockRequest { table_id: 1 })
            .await
            .unwrap();

        scheduler
            .schedule(MockRequest { table_id: 2 })
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

        let picker = MockPicker::new(vec![Arc::new(move || {
            latch_clone.countdown();
        })]);

        let config = CompactionSchedulerConfig {
            max_inflight_task: 3,
        };
        let scheduler = LocalCompactionScheduler::new(config, picker);

        for i in 0..task_size {
            scheduler
                .schedule(MockRequest {
                    table_id: i as TableId,
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

        let picker = MockPicker::new(vec![Arc::new(move || {
            latch_clone.countdown();
        })]);

        let config = CompactionSchedulerConfig {
            max_inflight_task: 3,
        };
        let scheduler = LocalCompactionScheduler::new(config, picker);

        for i in 0..task_size / 2 {
            scheduler
                .schedule(MockRequest {
                    table_id: i as TableId,
                })
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        for i in task_size / 2..task_size {
            scheduler
                .schedule(MockRequest {
                    table_id: i as TableId,
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
        let picker = MockPicker::new(vec![]);
        let config = CompactionSchedulerConfig {
            max_inflight_task: 3,
        };
        let scheduler = LocalCompactionScheduler::new(config, picker);

        let mut scheduled_task = 0;
        for _ in 0..10 {
            if scheduler
                .schedule(MockRequest { table_id: 1 })
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
