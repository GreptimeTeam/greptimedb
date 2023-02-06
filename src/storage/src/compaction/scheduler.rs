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

use async_trait::async_trait;
use common_telemetry::info;
use table::metadata::TableId;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::compaction::dedup_deque::DedupDeque;
use crate::compaction::rate_limit::{
    CascadeRateLimiter, RateLimitToken, RateLimitTokenPtr, RateLimiter,
};
use crate::compaction::task::CompactionTask;
use crate::error::Result;

/// Table compaction request.
#[derive(Default)]
pub struct CompactionRequest {
    table_id: TableId,
}

impl CompactionRequest {
    #[inline]
    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

/// CompactionScheduler defines a set of API to schedule compaction tasks.
#[async_trait]
pub trait CompactionScheduler {
    /// Schedules a compaction request.
    async fn schedule(&self, request: CompactionRequest) -> Result<()>;

    /// Stops compaction scheduler.
    async fn stop(&self) -> Result<()>;
}

/// Compaction task scheduler based on local state.
#[allow(unused)]
pub struct LocalCompactionScheduler {
    request_queue: Arc<RwLock<DedupDeque<TableId, CompactionRequest>>>,
    cancel_token: CancellationToken,
    task_notifier: Arc<Notify>,
    join_handle: JoinHandle<()>,
}

#[async_trait]
impl CompactionScheduler for LocalCompactionScheduler {
    async fn schedule(&self, request: CompactionRequest) -> Result<()> {
        let mut queue = self.request_queue.write().await;
        queue.push_back(request.table_id(), request);
        self.task_notifier.notify_one();
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.cancel_token.cancel();
        Ok(())
    }
}

#[allow(unused)]
impl LocalCompactionScheduler {
    pub fn new() -> Self {
        let request_queue: Arc<RwLock<DedupDeque<TableId, CompactionRequest>>> =
            Arc::new(RwLock::new(DedupDeque::default()));
        let cancel_token = CancellationToken::new();
        let task_notifier = Arc::new(Notify::new());
        let handler = CompactionHandler {
            task_notifier: task_notifier.clone(),
            req_queue: request_queue.clone(),
            cancel_token: cancel_token.child_token(),
            limiter: Arc::new(CascadeRateLimiter::new(vec![])),
        };
        let join_handle: JoinHandle<()> =
            common_runtime::spawn_bg(async move { handler.run().await });
        Self {
            join_handle,
            request_queue,
            cancel_token,
            task_notifier,
        }
    }
}

#[allow(unused)]
struct CompactionHandler {
    req_queue: Arc<RwLock<DedupDeque<TableId, CompactionRequest>>>,
    cancel_token: CancellationToken,
    task_notifier: Arc<Notify>,
    limiter: Arc<CascadeRateLimiter<CompactionRequest>>,
}

#[allow(unused)]
impl CompactionHandler {
    /// Runs table compaction requests dispatch loop.
    pub async fn run(self) {
        let task_notifier = self.task_notifier.clone();
        let limiter = self.limiter.clone();
        loop {
            tokio::select! {
                _ = task_notifier.notified() => {
                    // poll requests as many as possible until rate limited, and then wait for
                    // notification (some task's finished).
                    while let Some((table_id,  req)) = self.poll_task().await {
                        if let Ok(token) = limiter.acquire_token(&req) {
                            self.handle_compaction_request(req, token).await;
                        } else {
                            // compaction rate limited, put back to req queue to wait for next
                            // schedule
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
    async fn poll_task(&self) -> Option<(TableId, CompactionRequest)> {
        let mut queue = self.req_queue.write().await;
        queue.pop_front()
    }

    /// Puts request back to the front of request queue.
    #[inline]
    async fn put_back_req(&self, table_id: TableId, req: CompactionRequest) {
        let mut queue = self.req_queue.write().await;
        queue.push_front(table_id, req);
    }

    // Handles compaction request, submit task to bg runtime.
    async fn handle_compaction_request(
        &self,
        mut req: CompactionRequest,
        token: RateLimitTokenPtr,
    ) {
        let cloned_notify = self.task_notifier.clone();
        let task = self.build_compaction_task(req).await;

        common_runtime::spawn_bg(async move {
            task.run();
            // releases rate limit token
            token.try_release();
            // notify scheduler to schedule next task when current task finishes.
            cloned_notify.notify_one();
        });
    }

    // TODO(hl): generate compaction task(find SSTs to compact along with the output of compaction)
    async fn build_compaction_task(&self, req: CompactionRequest) -> CompactionTask {
        todo!()
    }
}
