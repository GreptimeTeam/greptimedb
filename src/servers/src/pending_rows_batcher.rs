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
use std::time::{Duration, Instant};

use api::v1::{RowInsertRequest, RowInsertRequests};
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::warn;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use session::context::QueryContextRef;
use tokio::sync::{Semaphore, broadcast, mpsc};

use crate::error::{Error, Result};
use crate::metrics::{FLUSH_ROWS, FLUSH_TOTAL, PENDING_BATCHES, PENDING_ROWS};
use crate::query_handler::PromStoreProtocolHandlerRef;

const PHYSICAL_TABLE_KEY: &str = "physical_table";

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct BatchKey {
    catalog: String,
    schema: String,
    physical_table: String,
}

// Batch key is derived from QueryContext; it assumes catalog/schema/physical_table fully
// define the write target and must remain consistent across the batch.
fn batch_key_from_ctx(ctx: &QueryContextRef) -> BatchKey {
    let physical_table = ctx
        .extension(PHYSICAL_TABLE_KEY)
        .unwrap_or(GREPTIME_PHYSICAL_TABLE)
        .to_string();
    BatchKey {
        catalog: ctx.current_catalog().to_string(),
        schema: ctx.current_schema(),
        physical_table,
    }
}

/// Prometheus remote write pending rows batcher.
pub struct PendingRowsBatcher {
    workers: DashMap<BatchKey, PendingWorker>,
    flush_interval: Duration,
    max_batch_rows: usize,
    prom_store_handler: PromStoreProtocolHandlerRef,
    with_metric_engine: bool,
    flush_semaphore: Arc<Semaphore>,
    worker_channel_capacity: usize,
    shutdown: broadcast::Sender<()>,
}

impl PendingRowsBatcher {
    pub fn try_new(
        prom_store_handler: PromStoreProtocolHandlerRef,
        with_metric_engine: bool,
        flush_interval: Duration,
        max_batch_rows: usize,
        max_concurrent_flushes: usize,
        worker_channel_capacity: usize,
    ) -> Option<Arc<Self>> {
        if flush_interval.is_zero() {
            return None;
        }

        let (shutdown, _) = broadcast::channel(1);
        Some(Arc::new(Self {
            workers: DashMap::new(),
            flush_interval,
            max_batch_rows,
            prom_store_handler,
            with_metric_engine,
            flush_semaphore: Arc::new(Semaphore::new(max_concurrent_flushes)),
            worker_channel_capacity,
            shutdown,
        }))
    }

    pub async fn submit(&self, requests: RowInsertRequests, ctx: QueryContextRef) -> Result<u64> {
        let row_count = count_rows(&requests);

        let worker = self.get_or_spawn_worker(batch_key_from_ctx(&ctx));
        let cmd = WorkerCommand::Submit {
            requests,
            row_count,
            ctx,
        };

        match worker.tx.try_send(cmd) {
            Ok(()) => Ok(row_count as u64),
            Err(mpsc::error::TrySendError::Full(_)) => Err(Error::BatcherQueueFull {
                capacity: self.worker_channel_capacity,
            }),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::BatcherChannelClosed),
        }
    }

    fn get_or_spawn_worker(&self, key: BatchKey) -> PendingWorker {
        if let Some(worker) = self.workers.get(&key) {
            return worker.clone();
        }

        let entry = self.workers.entry(key);
        match entry {
            Entry::Occupied(worker) => worker.get().clone(),
            Entry::Vacant(vacant) => {
                let (tx, rx) = mpsc::channel(self.worker_channel_capacity);
                let worker = PendingWorker { tx };

                start_worker(
                    rx,
                    self.shutdown.clone(),
                    self.prom_store_handler.clone(),
                    self.with_metric_engine,
                    self.flush_interval,
                    self.max_batch_rows,
                    self.flush_semaphore.clone(),
                );

                vacant.insert(worker.clone());
                worker
            }
        }
    }
}

impl Drop for PendingRowsBatcher {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
    }
}

#[derive(Clone)]
struct PendingWorker {
    tx: mpsc::Sender<WorkerCommand>,
}

enum WorkerCommand {
    Submit {
        requests: RowInsertRequests,
        row_count: usize,
        ctx: QueryContextRef,
    },
}

struct PendingBatch {
    inserts: Vec<RowInsertRequest>,
    created_at: Option<Instant>,
    row_count: usize,
    ctx: Option<QueryContextRef>,
}

struct FlushBatch {
    requests: RowInsertRequests,
    row_count: usize,
    ctx: QueryContextRef,
}

impl PendingBatch {
    fn new() -> Self {
        Self {
            inserts: Vec::new(),
            created_at: None,
            row_count: 0,
            ctx: None,
        }
    }
}

fn start_worker(
    mut rx: mpsc::Receiver<WorkerCommand>,
    shutdown: broadcast::Sender<()>,
    prom_store_handler: PromStoreProtocolHandlerRef,
    with_metric_engine: bool,
    flush_interval: Duration,
    max_batch_rows: usize,
    flush_semaphore: Arc<Semaphore>,
) {
    tokio::spawn(async move {
        let mut batch = PendingBatch::new();
        let mut interval = tokio::time::interval(flush_interval);
        let mut shutdown_rx = shutdown.subscribe();

        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    match cmd {
                        Some(WorkerCommand::Submit { requests, row_count, ctx }) => {
                            if batch.row_count == 0 {
                                batch.created_at = Some(Instant::now());
                                batch.ctx = Some(ctx);
                                PENDING_BATCHES.inc();
                            }

                            batch.inserts.extend(requests.inserts);
                            batch.row_count += row_count;
                            PENDING_ROWS.add(row_count as i64);

                            if batch.row_count >= max_batch_rows
                                && let Some(flush) = drain_batch(&mut batch) {
                                    spawn_flush(
                                        flush,
                                        prom_store_handler.clone(),
                                        with_metric_engine,
                                        flush_semaphore.clone(),
                                    ).await;
                            }
                        }
                        None => {
                            if let Some(flush) = drain_batch(&mut batch) {
                                flush_batch(
                                    flush,
                                    prom_store_handler.clone(),
                                    with_metric_engine,
                                )
                                .await;
                            }
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if let Some(created_at) = batch.created_at
                        && batch.row_count > 0 && created_at.elapsed() >= flush_interval
                        && let Some(flush) = drain_batch(&mut batch) {
                                spawn_flush(
                                    flush,
                                    prom_store_handler.clone(),
                                    with_metric_engine,
                                    flush_semaphore.clone(),
                                ).await;
                    }
                }
                _ = shutdown_rx.recv() => {
                    if let Some(flush) = drain_batch(&mut batch) {
                        flush_batch(
                            flush,
                            prom_store_handler.clone(),
                            with_metric_engine,
                        )
                        .await;
                    }
                    break;
                }
            }
        }
    });
}

fn drain_batch(batch: &mut PendingBatch) -> Option<FlushBatch> {
    if batch.row_count == 0 {
        return None;
    }

    let ctx = match batch.ctx.take() {
        Some(ctx) => ctx,
        None => {
            flush_with_error(batch, "Pending batch missing context");
            return None;
        }
    };

    let row_count = batch.row_count;
    let requests = RowInsertRequests {
        inserts: std::mem::take(&mut batch.inserts),
    };
    batch.row_count = 0;
    batch.created_at = None;

    PENDING_ROWS.sub(row_count as i64);
    PENDING_BATCHES.dec();

    Some(FlushBatch {
        requests,
        row_count,
        ctx,
    })
}

async fn spawn_flush(
    flush: FlushBatch,
    prom_store_handler: PromStoreProtocolHandlerRef,
    with_metric_engine: bool,
    semaphore: Arc<Semaphore>,
) {
    match semaphore.acquire_owned().await {
        Ok(permit) => {
            tokio::spawn(async move {
                let _permit = permit;
                flush_batch(flush, prom_store_handler, with_metric_engine).await;
            });
        }
        Err(err) => {
            warn!(err; "Flush semaphore closed, flushing inline");
            flush_batch(flush, prom_store_handler, with_metric_engine).await;
        }
    }
}

async fn flush_batch(
    flush: FlushBatch,
    prom_store_handler: PromStoreProtocolHandlerRef,
    with_metric_engine: bool,
) {
    let FlushBatch {
        requests,
        row_count,
        ctx,
    } = flush;

    let result = prom_store_handler
        .write(requests, ctx, with_metric_engine)
        .await;

    FLUSH_TOTAL.inc();
    FLUSH_ROWS.observe(row_count as f64);

    if let Err(err) = result {
        common_telemetry::error!(err; "Pending rows batch flush failed");
    }
}

fn flush_with_error(batch: &mut PendingBatch, message: &str) {
    if batch.row_count == 0 {
        return;
    }

    let row_count = batch.row_count;
    batch.inserts.clear();
    batch.row_count = 0;
    batch.created_at = None;
    batch.ctx = None;

    PENDING_ROWS.sub(row_count as i64);
    PENDING_BATCHES.dec();

    common_telemetry::error!(%message, "Pending rows batch dropped");
}

fn count_rows(requests: &RowInsertRequests) -> usize {
    requests
        .inserts
        .iter()
        .filter_map(|r| r.rows.as_ref())
        .map(|r| r.rows.len())
        .sum()
}
