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

use std::sync::Arc;

use common_telemetry::{debug, error, info};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::Notify;

use crate::compaction::picker::{Picker, PickerContext};
use crate::compaction::task::CompactionTask;
use crate::error::Result;
use crate::manifest::region::RegionManifest;
use crate::region::{RegionWriterRef, SharedDataRef};
use crate::scheduler::rate_limit::BoxedRateLimitToken;
use crate::scheduler::{Handler, Request};
use crate::schema::RegionSchemaRef;
use crate::sst::AccessLayerRef;
use crate::version::LevelMetasRef;
use crate::wal::Wal;

impl<S: LogStore> Request for CompactionRequestImpl<S> {
    type Key = RegionId;

    #[inline]
    fn key(&self) -> RegionId {
        self.region_id
    }
}

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

pub struct CompactionHandler<P> {
    pub picker: P,
}

impl<P> CompactionHandler<P> {
    pub fn new(picker: P) -> Self {
        Self { picker }
    }
}

#[async_trait::async_trait]
impl<P> Handler for CompactionHandler<P>
where
    P: Picker + Send + Sync,
{
    type Request = P::Request;

    async fn handle_request(
        &self,
        req: Self::Request,
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
    use crate::scheduler::dedup_deque::DedupDeque;
    use crate::scheduler::rate_limit::{
        BoxedRateLimitToken, CascadeRateLimiter, MaxInflightTaskLimiter,
    };
    use crate::scheduler::{HandlerLoop, LocalScheduler, Scheduler, SchedulerConfig};

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
        let queue = Arc::new(std::sync::RwLock::new(DedupDeque::default()));
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
    impl<F> Handler for MockHandler<F>
    where
        F: Fn() + Send + Sync,
    {
        type Request = MockRequest;

        async fn handle_request(
            &self,
            _req: Self::Request,
            token: BoxedRateLimitToken,
            finish_notifier: Arc<Notify>,
        ) -> Result<()> {
            (self.cb)();
            token.try_release();
            finish_notifier.notify_one();
            Ok(())
        }
    }

    impl Request for MockRequest {
        type Key = RegionId;

        fn key(&self) -> Self::Key {
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
