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
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use common_telemetry::{debug, error, info};
use snafu::ResultExt;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error;
use crate::error::StopSchedulerSnafu;
use crate::scheduler::dedup_deque::DedupDeque;
use crate::scheduler::rate_limit::{
    BoxedRateLimitToken, CascadeRateLimiter, MaxInflightTaskLimiter, RateLimiter,
};

pub mod dedup_deque;
pub mod rate_limit;

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
    ) -> error::Result<()>;
}

/// [Scheduler] defines a set of API to schedule requests.
#[async_trait]
pub trait Scheduler<R>: Debug {
    /// Schedules a request.
    /// Returns true if request is scheduled. Returns false if task queue already
    /// contains the request with same key.
    async fn schedule(&self, request: R) -> error::Result<bool>;

    /// Stops scheduler.
    async fn stop(&self) -> error::Result<()>;
}

/// Request scheduler based on local state.
pub struct LocalScheduler<R: Request<T>, T> {
    /// Request FIFO with key deduplication.
    request_queue: Arc<RwLock<DedupDeque<T, R>>>,
    /// Token used to halt the scheduler.
    cancel_token: CancellationToken,
    /// Tasks use a cooperative manner to notify scheduler that another request can be scheduled.
    task_notifier: Arc<Notify>,
    /// Join handle of spawned request handling loop.
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

impl<R, K> Debug for LocalScheduler<R, K>
where
    R: Request<K> + Send + Sync,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalScheduler<...>").finish()
    }
}

#[async_trait]
impl<R, K> Scheduler<R> for LocalScheduler<R, K>
where
    R: Request<K> + Send,
    K: Debug + Eq + Hash + Clone + Send + Sync + 'static,
{
    async fn schedule(&self, request: R) -> error::Result<bool> {
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

    async fn stop(&self) -> error::Result<()> {
        self.cancel_token.cancel();
        let handle = { self.join_handle.lock().unwrap().take() };
        if let Some(handle) = handle {
            handle.await.context(StopSchedulerSnafu)?;
        }
        Ok(())
    }
}

impl<R, K> LocalScheduler<R, K>
where
    R: Request<K>,
    K: Debug + Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Creates a new scheduler instance with given config and request handler.
    pub fn new<H>(config: SchedulerConfig, handler: H) -> Self
    where
        H: Handler<R> + Send + Sync + 'static,
    {
        let request_queue = Arc::new(RwLock::new(DedupDeque::default()));
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
            debug!("Task handler loop spawned");
            handle_loop.run().await;
        });
        Self {
            join_handle: Mutex::new(Some(join_handle)),
            request_queue,
            cancel_token,
            task_notifier,
        }
    }

    /// Returns remaining requests number.
    #[inline]
    fn remaining_requests(&self) -> usize {
        self.request_queue.read().unwrap().len()
    }
}

pub struct HandlerLoop<R, K, H> {
    pub req_queue: Arc<RwLock<DedupDeque<K, R>>>,
    pub cancel_token: CancellationToken,
    pub task_notifier: Arc<Notify>,
    pub request_handler: H,
    pub limiter: Arc<CascadeRateLimiter<R>>,
}

impl<R: Request<K>, K: Debug + Clone + Eq + Hash + Send + 'static, H: Handler<R>>
    HandlerLoop<R, K, H>
{
    /// Runs scheduled requests dispatch loop.
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
    ) -> error::Result<()> {
        self.request_handler
            .handle_request(req, token, finish_notifier)
            .await
    }
}
