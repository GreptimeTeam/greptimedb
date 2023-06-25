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
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use common_telemetry::{debug, error, info};
use snafu::{ensure, ResultExt};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{IllegalSchedulerStateSnafu, Result, StopSchedulerSnafu};
use crate::scheduler::dedup_deque::DedupDeque;
use crate::scheduler::rate_limit::{
    BoxedRateLimitToken, CascadeRateLimiter, MaxInflightTaskLimiter, RateLimiter,
};

pub mod dedup_deque;
pub mod rate_limit;

/// Request that can be scheduled.
/// It must contain a key for deduplication.
pub trait Request: Send + Sync + 'static {
    /// Type of request key.
    type Key: Eq + Hash + Clone + Debug + Send + Sync;

    /// Returns the request key.
    fn key(&self) -> Self::Key;

    /// Notify the request result.
    fn complete(self, result: Result<()>);
}

#[async_trait::async_trait]
pub trait Handler {
    type Request;

    async fn handle_request(
        &self,
        req: Self::Request,
        token: BoxedRateLimitToken,
        finish_notifier: Arc<Notify>,
    ) -> Result<()>;
}

/// [Scheduler] defines a set of API to schedule requests.
#[async_trait]
pub trait Scheduler: Debug {
    type Request;

    /// Schedules a request.
    /// Returns true if request is scheduled. Returns false if task queue already
    /// contains the request with same key.
    fn schedule(&self, request: Self::Request) -> Result<bool>;

    /// Stops scheduler. If `await_termination` is set to true, the scheduler will
    /// wait until all queued requests are processed.
    async fn stop(&self, await_termination: bool) -> Result<()>;
}

/// Scheduler config.
#[derive(Debug)]
pub struct SchedulerConfig {
    pub max_inflight_tasks: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_inflight_tasks: 4,
        }
    }
}

const STATE_RUNNING: u8 = 0;
const STATE_STOP: u8 = 1;
const STATE_AWAIT_TERMINATION: u8 = 2;

/// Request scheduler based on local state.
pub struct LocalScheduler<R: Request> {
    /// Request FIFO with key deduplication.
    request_queue: Arc<RwLock<DedupDeque<R::Key, R>>>,
    /// Token used to halt the scheduler.
    cancel_token: CancellationToken,
    /// Tasks use a cooperative manner to notify scheduler that another request can be scheduled.
    task_notifier: Arc<Notify>,
    /// Join handle of spawned request handling loop.
    join_handle: Mutex<Option<JoinHandle<()>>>,
    /// State of scheduler.
    state: Arc<AtomicU8>,
}

impl<R> Debug for LocalScheduler<R>
where
    R: Request + Send + Sync,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalScheduler")
            .field("state", &self.state)
            .finish()
    }
}

impl<R> Drop for LocalScheduler<R>
where
    R: Request,
{
    fn drop(&mut self) {
        self.state.store(STATE_STOP, Ordering::Relaxed);

        self.cancel_token.cancel();

        // Clear all requests
        self.request_queue.write().unwrap().clear();
    }
}

#[async_trait]
impl<R> Scheduler for LocalScheduler<R>
where
    R: Request + Send,
{
    type Request = R;

    fn schedule(&self, request: Self::Request) -> Result<bool> {
        ensure!(self.running(), IllegalSchedulerStateSnafu);
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

    async fn stop(&self, await_termination: bool) -> Result<()> {
        let state = if await_termination {
            STATE_AWAIT_TERMINATION
        } else {
            STATE_STOP
        };
        self.state.store(state, Ordering::Relaxed);

        self.cancel_token.cancel();
        let handle = { self.join_handle.lock().unwrap().take() };
        if let Some(handle) = handle {
            handle.await.context(StopSchedulerSnafu)?;
        }
        Ok(())
    }
}

impl<R> LocalScheduler<R>
where
    R: Request,
{
    /// Creates a new scheduler instance with given config and request handler.
    pub fn new<H>(config: SchedulerConfig, handler: H) -> Self
    where
        H: Handler<Request = R> + Send + Sync + 'static,
    {
        let request_queue = Arc::new(RwLock::new(DedupDeque::default()));
        let cancel_token = CancellationToken::new();
        let task_notifier = Arc::new(Notify::new());
        let state = Arc::new(AtomicU8::new(STATE_RUNNING));
        let handle_loop = HandlerLoop {
            task_notifier: task_notifier.clone(),
            req_queue: request_queue.clone(),
            cancel_token: cancel_token.child_token(),
            limiter: Arc::new(CascadeRateLimiter::new(vec![Box::new(
                MaxInflightTaskLimiter::new(config.max_inflight_tasks),
            )])),
            request_handler: handler,
            state: state.clone(),
        };
        let join_handle = common_runtime::spawn_bg(async move {
            debug!("Task handler loop spawned");
            handle_loop.run().await;
        });
        Self {
            join_handle: Mutex::new(Some(join_handle)),
            request_queue,
            cancel_token,
            task_notifier,
            state,
        }
    }

    /// Returns remaining requests number.
    #[inline]
    fn remaining_requests(&self) -> usize {
        self.request_queue.read().unwrap().len()
    }

    #[inline]
    fn running(&self) -> bool {
        self.state.load(Ordering::Relaxed) == STATE_RUNNING
    }
}

pub struct HandlerLoop<R: Request, H: Handler> {
    pub req_queue: Arc<RwLock<DedupDeque<R::Key, R>>>,
    pub cancel_token: CancellationToken,
    pub task_notifier: Arc<Notify>,
    pub request_handler: H,
    pub limiter: Arc<CascadeRateLimiter<R>>,
    pub state: Arc<AtomicU8>,
}

impl<R, H> HandlerLoop<R, H>
where
    R: Request,
    H: Handler<Request = R>,
{
    /// Runs scheduled requests dispatch loop.
    pub async fn run(&self) {
        let limiter = self.limiter.clone();
        while self.running() {
            tokio::select! {
                _ = self.task_notifier.notified() => {
                    debug!("Notified, queue size: {:?}",self.req_queue.read().unwrap().len());
                    self.poll_and_execute(&limiter).await;
                }
                _ = self.cancel_token.cancelled() => {
                    info!("Task scheduler cancelled.");
                    break;
                }
            }
        }
        // For correctness, we need to poll requests from fifo again.
        if self.state.load(Ordering::Relaxed) == STATE_AWAIT_TERMINATION {
            info!("Waiting for all pending tasks to finish.");
            self.poll_and_execute(&limiter).await;
            self.state.store(STATE_STOP, Ordering::Relaxed);
        }
        info!("Task scheduler stopped");
    }

    /// Polls and executes requests as many as possible until rate limited.
    async fn poll_and_execute(&self, limiter: &Arc<CascadeRateLimiter<R>>) {
        while let Some((task_key, req)) = self.poll_task().await {
            if let Ok(token) = limiter.acquire_token(&req) {
                debug!("Executing request: {:?}", task_key);
                if let Err(e) = self
                    .handle_request(req, token, self.task_notifier.clone())
                    .await
                {
                    error!(e; "Failed to submit request: {:?}", task_key);
                } else {
                    info!("Submitted task: {:?}", task_key);
                }
            } else {
                // rate limited, put back to req queue to wait for next schedule
                debug!(
                    "Put back request {:?}, queue size: {}",
                    task_key,
                    self.req_queue.read().unwrap().len()
                );
                self.put_back_req(task_key, req).await;
                break;
            }
        }
    }

    #[inline]
    async fn poll_task(&self) -> Option<(R::Key, R)> {
        let mut queue = self.req_queue.write().unwrap();
        queue.pop_front()
    }

    /// Puts request back to the front of request queue.
    #[inline]
    async fn put_back_req(&self, key: R::Key, req: R) {
        let mut queue = self.req_queue.write().unwrap();
        let _ = queue.push_front(key, req);
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

    #[inline]
    fn running(&self) -> bool {
        self.state.load(Ordering::Relaxed) == STATE_RUNNING
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicI32};
    use std::time::Duration;

    use store_api::storage::RegionId;

    use super::*;
    use crate::scheduler::dedup_deque::DedupDeque;
    use crate::scheduler::rate_limit::{
        BoxedRateLimitToken, CascadeRateLimiter, MaxInflightTaskLimiter,
    };
    use crate::scheduler::{HandlerLoop, LocalScheduler, Scheduler, SchedulerConfig};

    struct CountdownLatch {
        counter: std::sync::Mutex<usize>,
        notify: Notify,
    }

    impl CountdownLatch {
        fn new(size: usize) -> Self {
            Self {
                counter: std::sync::Mutex::new(size),
                notify: Notify::new(),
            }
        }

        fn countdown(&self) {
            let mut counter = self.counter.lock().unwrap();
            if *counter >= 1 {
                *counter -= 1;
                if *counter == 0 {
                    self.notify.notify_one();
                }
            }
        }

        /// Users should only call this once.
        async fn wait(&self) {
            self.notify.notified().await
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
            state: Arc::new(AtomicU8::default()),
        });

        let handler_cloned = handler.clone();
        let _handle = common_runtime::spawn_bg(async move { handler_cloned.run().await });

        let _ = queue.write().unwrap().push_back(1, MockRequest::default());
        handler.task_notifier.notify_one();
        let _ = queue.write().unwrap().push_back(2, MockRequest::default());
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

        fn complete(self, _result: Result<()>) {}
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
        let scheduler: LocalScheduler<MockRequest> = LocalScheduler::new(
            SchedulerConfig {
                max_inflight_tasks: 3,
            },
            handler,
        );

        assert!(scheduler.schedule(MockRequest { region_id: 1 }).is_ok());
        assert!(scheduler.schedule(MockRequest { region_id: 2 }).is_ok());

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
            max_inflight_tasks: 3,
        };
        let scheduler = LocalScheduler::new(config, handler);

        for i in 0..task_size {
            assert!(scheduler
                .schedule(MockRequest {
                    region_id: i as RegionId,
                })
                .is_ok());
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
            max_inflight_tasks: 3,
        };
        let scheduler = LocalScheduler::new(config, handler);

        for i in 0..task_size / 2 {
            assert!(scheduler
                .schedule(MockRequest {
                    region_id: i as RegionId,
                })
                .is_ok());
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        for i in task_size / 2..task_size {
            assert!(scheduler
                .schedule(MockRequest {
                    region_id: i as RegionId,
                })
                .is_ok());
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
            max_inflight_tasks: 30,
        };
        let scheduler = LocalScheduler::new(config, handler);

        let mut scheduled_task = 0;
        for _ in 0..10 {
            if scheduler.schedule(MockRequest { region_id: 1 }).unwrap() {
                scheduled_task += 1;
            }
        }
        scheduler.stop(true).await.unwrap();
        debug!("Schedule tasks: {}", scheduled_task);
        assert!(scheduled_task < 10);
    }

    #[tokio::test]
    async fn test_await_termination() {
        common_telemetry::init_default_ut_logging();

        let finished = Arc::new(AtomicI32::new(0));
        let finished_clone = finished.clone();
        let handler = MockHandler {
            cb: move || {
                let _ = finished_clone.fetch_add(1, Ordering::Relaxed);
            },
        };

        let config = SchedulerConfig {
            max_inflight_tasks: 3,
        };
        let scheduler = Arc::new(LocalScheduler::new(config, handler));
        let scheduler_cloned = scheduler.clone();
        let task_scheduled = Arc::new(AtomicI32::new(0));
        let task_scheduled_cloned = task_scheduled.clone();

        let scheduling = Arc::new(AtomicBool::new(true));
        let scheduling_clone = scheduling.clone();
        let handle = common_runtime::spawn_write(async move {
            for i in 0..10000 {
                if let Ok(res) = scheduler_cloned.schedule(MockRequest {
                    region_id: i as RegionId,
                }) {
                    if res {
                        let _ = task_scheduled_cloned.fetch_add(1, Ordering::Relaxed);
                    }
                }

                if !scheduling_clone.load(Ordering::Relaxed) {
                    break;
                }
            }
        });

        scheduler.stop(true).await.unwrap();
        scheduling.store(false, Ordering::Relaxed);

        let finished = finished.load(Ordering::Relaxed);
        handle.await.unwrap();

        assert_eq!(finished, task_scheduled.load(Ordering::Relaxed));
    }
}
