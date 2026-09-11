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
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use common_batcher::flush_limiter::FlushLimiter;
use common_batcher::flush_policy::FlushTrigger;
use common_batcher::flush_policy::timing::TimingFlushPolicy;
use common_batcher::pending_worker::PendingWorker as PendingCore;
use common_runtime::spawn_global;
use common_telemetry::warn;
use operator::insert::Inserter;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};

use crate::batcher::table::batch::{Batch, flush_batch};
use crate::batcher::table::flow_notifier::FlowNotifier;
use crate::batcher::table::metrics::{PENDING_BATCHES, PENDING_ROWS, PENDING_WORKERS};
use crate::batcher::table::pending_batch::PendingBatch;
use crate::batcher::table::{BatchKey, PendingWorkers};

pub(in crate::batcher::table) enum WorkerCommand {
    Submit(PendingBatch),
}

/// The sender handle does not own the worker's processing state.
pub(in crate::batcher::table) struct PendingWorker {
    pub tx: mpsc::Sender<WorkerCommand>,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::batcher::table) fn start_worker(
    key: BatchKey,
    worker_tx: mpsc::Sender<WorkerCommand>,
    workers: Arc<PendingWorkers>,
    mut rx: mpsc::Receiver<WorkerCommand>,
    shutdown: broadcast::Sender<()>,
    flush_policy: TimingFlushPolicy,
    flush_limiter: FlushLimiter,
    inserter: Arc<Inserter>,
    notifier: FlowNotifier,
    idle_timeout: Duration,
) {
    spawn_global(async move {
        let mut pending = PendingCore::new(flush_policy);
        let mut schema: Option<SchemaRef> = None;
        // Handles are retained only for the schema transition barrier. Dropping
        // them on shutdown detaches writes, matching the Prom worker contract.
        let mut flushes: Vec<JoinHandle<()>> = Vec::new();
        let mut shutdown_rx = shutdown.subscribe();
        let mut closing = false;
        let idle_timer = sleep_until(Instant::now() + idle_timeout);
        tokio::pin!(idle_timer);
        loop {
            let batch = tokio::select! {
                command = rx.recv() => {
                    match command {
                        Some(WorkerCommand::Submit(batch)) => {
                            idle_timer.as_mut().reset(Instant::now() + idle_timeout);
                            if schema.as_ref().is_some_and(|schema| *schema != batch.batch.schema()) {
                                if let Some(old) = drain_batch(&mut pending, None)
                                    && let Some(task) = spawn_flush(old, &flush_limiter, inserter.clone(), notifier.clone()).await {
                                    flushes.push(task);
                                }
                                // Never send a new schema ahead of an old-schema write.
                                for task in flushes.drain(..) {
                                    if let Err(error) = task.await { warn!(error; "Failed to join old-schema batch flush"); }
                                }
                            }
                            schema = Some(batch.batch.schema());
                            let rows = batch.batch.num_rows();
                            if pending.is_empty() { PENDING_BATCHES.inc(); }
                            pending.submit(batch, rows);
                            PENDING_ROWS.add(rows as i64);
                            drain_batch(&mut pending, Some(FlushTrigger::Submission))
                        }
                        None => {
                            if let Some(batch) = drain_batch(&mut pending, None) {
                                flush_batch(batch, inserter.clone(), notifier.clone()).await;
                            }
                            break;
                        }
                    }
                }
                _ = pending.wait_flush() => drain_batch(&mut pending, Some(FlushTrigger::Deadline)),
                _ = &mut idle_timer, if !closing => {
                    if pending.is_empty() && rx.is_empty() && flushes.iter().all(JoinHandle::is_finished) {
                        // Keep schema history while writes are in flight. Closing
                        // still allows previously reserved sends to be received.
                        rx.close();
                        closing = true;
                    }
                    idle_timer.as_mut().reset(Instant::now() + idle_timeout);
                    None
                }
                _ = shutdown_rx.recv() => {
                    if let Some(batch) = drain_batch(&mut pending, None) {
                        flush_batch(batch, inserter.clone(), notifier.clone()).await;
                    }
                    break;
                }
            };
            if let Some(batch) = batch {
                flushes.retain(|task| !task.is_finished());
                if let Some(task) =
                    spawn_flush(batch, &flush_limiter, inserter.clone(), notifier.clone()).await
                {
                    flushes.push(task);
                }
            }
        }
        if workers.remove_if_same(&key, &worker_tx).await {
            PENDING_WORKERS.set(workers.len().await as i64);
        }
    });
}

fn drain_batch(
    pending: &mut PendingCore<PendingBatch, TimingFlushPolicy>,
    trigger: Option<FlushTrigger>,
) -> Option<Batch> {
    let total_rows = pending.total_rows();
    let submissions = match trigger {
        Some(trigger) => pending.take_ready(trigger)?,
        None => pending.take_pending()?,
    };
    PENDING_ROWS.sub(total_rows as i64);
    PENDING_BATCHES.dec();
    Some(Batch {
        submissions,
        total_rows,
    })
}

async fn spawn_flush(
    batch: Batch,
    limiter: &FlushLimiter,
    inserter: Arc<Inserter>,
    notifier: FlowNotifier,
) -> Option<JoinHandle<()>> {
    match limiter.acquire().await {
        Ok(permit) => Some(spawn_global(async move {
            let _permit = permit;
            flush_batch(batch, inserter, notifier).await;
        })),
        Err(error) => {
            warn!(error; "Flush limiter closed, flushing inline");
            flush_batch(batch, inserter, notifier).await;
            None
        }
    }
}
