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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::meta::Peer;
use api::v1::region::{
    BulkInsertRequest, RegionRequest, RegionRequestHeader, bulk_insert_request, region_request,
};
use api::v1::{ArrowIpc, ColumnSchema, RowInsertRequests, Rows};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use catalog::CatalogManagerRef;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_meta::node_manager::NodeManagerRef;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{debug, error, info, warn};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use metric_engine::batch_modifier::{TagColumnInfo, modify_batch_sparse};
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::OptionExt;
use store_api::storage::RegionId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot};

use crate::error;
use crate::error::{Error, Result};
use crate::metrics::{
    FLUSH_DROPPED_ROWS, FLUSH_ELAPSED, FLUSH_FAILURES, FLUSH_ROWS, FLUSH_TOTAL, PENDING_BATCHES,
    PENDING_ROWS, PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED, PENDING_WORKERS
};
use crate::prom_row_builder::{
    build_prom_create_table_schema_from_proto, identify_missing_columns_from_proto,
    rows_to_aligned_record_batch,
};

const PHYSICAL_TABLE_KEY: &str = "physical_table";
/// Whether wait for ingestion result before reply to client.
const PENDING_ROWS_BATCH_SYNC_ENV: &str = "PENDING_ROWS_BATCH_SYNC";
const WORKER_IDLE_TIMEOUT_MULTIPLIER: u32 = 3;

#[async_trait]
pub trait PendingRowsSchemaAlterer: Send + Sync {
    async fn create_table_if_missing(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        request_schema: &[ColumnSchema],
        with_metric_engine: bool,
        ctx: QueryContextRef,
    ) -> Result<()>;

    async fn add_missing_prom_tag_columns(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        columns: &[String],
        ctx: QueryContextRef,
    ) -> Result<()>;

    /// Batch-create multiple logical tables that are missing.
    /// Each entry is `(table_name, request_schema)`.
    async fn create_tables_if_missing_batch(
        &self,
        catalog: &str,
        schema: &str,
        tables: &[(&str, &[ColumnSchema])],
        with_metric_engine: bool,
        ctx: QueryContextRef,
    ) -> Result<()>;

    /// Batch-alter multiple logical tables to add missing tag columns.
    /// Each entry is `(table_name, missing_column_names)`.
    async fn add_missing_prom_tag_columns_batch(
        &self,
        catalog: &str,
        schema: &str,
        tables: &[(&str, &[String])],
        ctx: QueryContextRef,
    ) -> Result<()>;
}

pub type PendingRowsSchemaAltererRef = Arc<dyn PendingRowsSchemaAlterer>;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct BatchKey {
    catalog: String,
    schema: String,
    physical_table: String,
}

#[derive(Debug)]
struct TableBatch {
    table_name: String,
    table_id: Option<u32>,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

struct PendingBatch {
    tables: HashMap<String, TableBatch>,
    created_at: Option<Instant>,
    total_row_count: usize,
    ctx: Option<QueryContextRef>,
    waiters: Vec<FlushWaiter>,
}

struct FlushWaiter {
    response_tx: oneshot::Sender<Result<()>>,
    _permit: OwnedSemaphorePermit,
}

struct FlushBatch {
    table_batches: Vec<TableBatch>,
    total_row_count: usize,
    ctx: QueryContextRef,
    waiters: Vec<FlushWaiter>,
}

#[derive(Clone)]
struct PendingWorker {
    tx: mpsc::Sender<WorkerCommand>,
}

enum WorkerCommand {
    Submit {
        table_batches: Vec<(String, u32, RecordBatch)>,
        total_rows: usize,
        ctx: QueryContextRef,
        response_tx: oneshot::Sender<Result<()>>,
        _permit: OwnedSemaphorePermit,
    },
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
    workers: Arc<DashMap<BatchKey, PendingWorker>>,
    flush_interval: Duration,
    max_batch_rows: usize,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
    flush_semaphore: Arc<Semaphore>,
    inflight_semaphore: Arc<Semaphore>,
    worker_channel_capacity: usize,
    prom_store_with_metric_engine: bool,
    schema_alterer: PendingRowsSchemaAltererRef,
    pending_rows_batch_sync: bool,
    shutdown: broadcast::Sender<()>,
}

impl PendingRowsBatcher {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
        catalog_manager: CatalogManagerRef,
        prom_store_with_metric_engine: bool,
        schema_alterer: PendingRowsSchemaAltererRef,
        flush_interval: Duration,
        max_batch_rows: usize,
        max_concurrent_flushes: usize,
        worker_channel_capacity: usize,
        max_inflight_requests: usize,
    ) -> Option<Arc<Self>> {
        // Disable the batcher if flush is disabled or configuration is invalid.
        // Zero values for these knobs either cause panics (e.g., zero-capacity channels)
        // or deadlocks (e.g., semaphores with no permits).
        if flush_interval.is_zero()
            || max_batch_rows == 0
            || max_concurrent_flushes == 0
            || worker_channel_capacity == 0
            || max_inflight_requests == 0
        {
            return None;
        }

        let (shutdown, _) = broadcast::channel(1);
        let pending_rows_batch_sync = std::env::var(PENDING_ROWS_BATCH_SYNC_ENV)
            .ok()
            .as_deref()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);
        let workers = Arc::new(DashMap::new());
        PENDING_WORKERS.set(workers.len() as i64);

        Some(Arc::new(Self {
            workers,
            flush_interval,
            max_batch_rows,
            partition_manager,
            node_manager,
            catalog_manager,
            prom_store_with_metric_engine,
            schema_alterer,
            flush_semaphore: Arc::new(Semaphore::new(max_concurrent_flushes)),
            inflight_semaphore: Arc::new(Semaphore::new(max_inflight_requests)),
            worker_channel_capacity,
            pending_rows_batch_sync,
            shutdown,
        }))
    }

    pub async fn submit(&self, requests: RowInsertRequests, ctx: QueryContextRef) -> Result<u64> {
        let (table_batches, total_rows) = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["submit_build_and_align"])
                .start_timer();
            self.build_and_align_table_batches(requests, &ctx).await?
        };
        if total_rows == 0 {
            return Ok(0);
        }

        let permit = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["submit_acquire_inflight_permit"])
                .start_timer();
            self.inflight_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::BatcherChannelClosed)?
        };

        let (response_tx, response_rx) = oneshot::channel();

        let batch_key = batch_key_from_ctx(&ctx);
        let mut cmd = Some(WorkerCommand::Submit {
            table_batches,
            total_rows,
            ctx,
            response_tx,
            _permit: permit,
        });

        {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["submit_send_to_worker"])
                .start_timer();

            for _ in 0..2 {
                let worker = self.get_or_spawn_worker(batch_key.clone());
                let Some(worker_cmd) = cmd.take() else {
                    break;
                };

                match worker.tx.send(worker_cmd).await {
                    Ok(()) => break,
                    Err(err) => {
                        cmd = Some(err.0);
                        remove_worker_if_same_channel(
                            self.workers.as_ref(),
                            &batch_key,
                            &worker.tx,
                        );
                    }
                }
            }

            if cmd.is_some() {
                return Err(Error::BatcherChannelClosed);
            }
        }

        if self.pending_rows_batch_sync {
            let result = {
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                    .with_label_values(&["submit_wait_flush_result"])
                    .start_timer();
                response_rx.await.map_err(|_| Error::BatcherChannelClosed)?
            };
            result.map(|()| total_rows as u64)
        } else {
            Ok(total_rows as u64)
        }
    }

    /// Converts proto `RowInsertRequests` directly into aligned `RecordBatch`es
    /// in a single pass, handling table creation, schema alteration, column
    /// renaming, reordering, and null-filling without building intermediate
    /// RecordBatches.
    async fn build_and_align_table_batches(
        &self,
        requests: RowInsertRequests,
        ctx: &QueryContextRef,
    ) -> Result<(Vec<(String, u32, RecordBatch)>, usize)> {
        let catalog = ctx.current_catalog().to_string();
        let schema = ctx.current_schema();

        // Extract (table_name, Rows) pairs from proto requests.
        let mut table_rows: Vec<(String, Rows)> = Vec::with_capacity(requests.inserts.len());
        let mut total_rows = 0;
        for request in requests.inserts {
            let Some(rows) = request.rows else {
                continue;
            };
            if rows.rows.is_empty() {
                continue;
            }
            total_rows += rows.rows.len();
            table_rows.push((request.table_name, rows));
        }
        if total_rows == 0 {
            return Ok((Vec::new(), 0));
        }

        let mut region_schemas: HashMap<String, (Arc<ArrowSchema>, u32)> =
            HashMap::with_capacity(table_rows.len());

        // --- Pass 1: Identify missing tables and missing columns ---
        let mut tables_to_create: Vec<(String, Vec<ColumnSchema>)> = Vec::new();
        let mut tables_to_alter: Vec<(String, Vec<String>)> = Vec::new();

        // Collect unique table names (preserving first-seen proto schema).
        let mut unique_tables: Vec<(&str, &[ColumnSchema])> = Vec::new();
        {
            let mut seen = HashSet::new();
            for (table_name, rows) in &table_rows {
                if seen.insert(table_name.as_str()) {
                    unique_tables.push((table_name.as_str(), &rows.schema));
                }
            }
        }

        // Fetch all table metadata concurrently.
        let resolved_tables = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_resolve_table"])
                .start_timer();
            futures::future::join_all(unique_tables.iter().map(|(table_name, _)| {
                self.catalog_manager
                    .table(&catalog, &schema, table_name, Some(ctx.as_ref()))
            }))
            .await
        };

        // Classify into existing / to-create / to-alter using proto schemas directly.
        for ((table_name, rows_schema), table_result) in
            unique_tables.iter().zip(resolved_tables.into_iter())
        {
            let table = table_result?;

            if let Some(table) = table {
                let table_info = table.table_info();
                let table_id = table_info.ident.table_id;
                let region_schema = table_info.meta.schema.arrow_schema().clone();

                // Check for missing columns directly from proto schema.
                let missing_columns = {
                    let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                        .with_label_values(&["align_identify_missing_columns"])
                        .start_timer();
                    identify_missing_columns_from_proto(rows_schema, region_schema.as_ref())?
                };
                if !missing_columns.is_empty() {
                    tables_to_alter.push((table_name.to_string(), missing_columns));
                }
                region_schemas.insert(table_name.to_string(), (region_schema, table_id));
            } else {
                // Table doesn't exist — build create schema directly from proto.
                let request_schema = {
                    let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                        .with_label_values(&["align_build_create_table_schema"])
                        .start_timer();
                    build_prom_create_table_schema_from_proto(rows_schema)?
                };
                tables_to_create.push((table_name.to_string(), request_schema));
            }
        }

        // --- Batch DDL: create missing tables ---
        if !tables_to_create.is_empty() {
            let create_refs: Vec<(&str, &[ColumnSchema])> = tables_to_create
                .iter()
                .map(|(name, schema)| (name.as_str(), schema.as_slice()))
                .collect();
            {
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                    .with_label_values(&["align_batch_create_tables"])
                    .start_timer();
                self.schema_alterer
                    .create_tables_if_missing_batch(
                        &catalog,
                        &schema,
                        &create_refs,
                        self.prom_store_with_metric_engine,
                        ctx.clone(),
                    )
                    .await?;
            }
            // Resolve newly created tables concurrently.
            let created_table_results = {
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                    .with_label_values(&["align_resolve_table_after_create"])
                    .start_timer();
                futures::future::join_all(tables_to_create.iter().map(|(table_name, _)| {
                    self.catalog_manager
                        .table(&catalog, &schema, table_name, Some(ctx.as_ref()))
                }))
                .await
            };
            for ((table_name, _), table_result) in tables_to_create
                .iter()
                .zip(created_table_results.into_iter())
            {
                let table = table_result?.with_context(|| error::UnexpectedResultSnafu {
                    reason: format!(
                        "Table not found after pending batch create attempt: {}",
                        table_name
                    ),
                })?;
                let table_info = table.table_info();
                let table_id = table_info.ident.table_id;
                let region_schema = table_info.meta.schema.arrow_schema().clone();
                region_schemas.insert(table_name.clone(), (region_schema, table_id));
            }

            // For newly created tables, re-check all rows for additional missing columns
            // (subsequent rows for the same table may have extra tag columns).
            for (table_name, rows) in &table_rows {
                if let Some((region_schema, _table_id)) = region_schemas.get(table_name) {
                    if tables_to_create.iter().any(|(n, _)| n == table_name) {
                        let missing_columns = identify_missing_columns_from_proto(
                            &rows.schema,
                            region_schema.as_ref(),
                        )?;
                        if !missing_columns.is_empty()
                            && !tables_to_alter.iter().any(|(n, _)| n == table_name)
                        {
                            tables_to_alter.push((table_name.clone(), missing_columns));
                        }
                    }
                }
            }
        }

        // --- Batch DDL: alter tables with missing columns ---
        if !tables_to_alter.is_empty() {
            let alter_refs: Vec<(&str, &[String])> = tables_to_alter
                .iter()
                .map(|(name, cols)| (name.as_str(), cols.as_slice()))
                .collect();
            {
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                    .with_label_values(&["align_batch_add_missing_columns"])
                    .start_timer();
                self.schema_alterer
                    .add_missing_prom_tag_columns_batch(&catalog, &schema, &alter_refs, ctx.clone())
                    .await?;
            }
            // Refresh schemas for altered tables concurrently.
            let altered_table_results = {
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                    .with_label_values(&["align_resolve_table_after_schema_alter"])
                    .start_timer();
                futures::future::join_all(tables_to_alter.iter().map(|(table_name, _)| {
                    self.catalog_manager
                        .table(&catalog, &schema, table_name, Some(ctx.as_ref()))
                }))
                .await
            };
            for ((table_name, _), table_result) in tables_to_alter
                .iter()
                .zip(altered_table_results.into_iter())
            {
                let table = table_result?.with_context(|| error::UnexpectedResultSnafu {
                    reason: format!(
                        "Table not found after pending batch schema alter: {}",
                        table_name
                    ),
                })?;
                let table_info = table.table_info();
                let table_id = table_info.ident.table_id;
                let refreshed_region_schema = table_info.meta.schema.arrow_schema().clone();
                region_schemas.insert(table_name.clone(), (refreshed_region_schema, table_id));
            }
        }

        // --- Build all record batches directly aligned to their region schemas ---
        let mut aligned_batches = Vec::with_capacity(table_rows.len());
        for (table_name, rows) in &table_rows {
            let (region_schema, table_id) =
                region_schemas.get(table_name).cloned().with_context(|| {
                    error::UnexpectedResultSnafu {
                        reason: format!("Region schema not resolved for table: {}", table_name),
                    }
                })?;

            let (record_batch, _missing_columns) = {
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                    .with_label_values(&["align_rows_to_record_batch"])
                    .start_timer();
                rows_to_aligned_record_batch(rows, region_schema.as_ref())?
            };
            aligned_batches.push((table_name.clone(), table_id, record_batch));
        }

        Ok((aligned_batches, total_rows))
    }

    fn get_or_spawn_worker(&self, key: BatchKey) -> PendingWorker {
        if let Some(worker) = self.workers.get(&key)
            && !worker.tx.is_closed()
        {
            return worker.clone();
        }

        let entry = self.workers.entry(key.clone());
        match entry {
            Entry::Occupied(mut worker) => {
                if worker.get().tx.is_closed() {
                    let new_worker = self.spawn_worker(key);
                    worker.insert(new_worker.clone());
                    PENDING_WORKERS.set(self.workers.len() as i64);
                    new_worker
                } else {
                    worker.get().clone()
                }
            }
            Entry::Vacant(vacant) => {
                let worker = self.spawn_worker(key);

                vacant.insert(worker.clone());
                PENDING_WORKERS.set(self.workers.len() as i64);
                worker
            }
        }
    }

    fn spawn_worker(&self, key: BatchKey) -> PendingWorker {
        let (tx, rx) = mpsc::channel(self.worker_channel_capacity);
        let worker = PendingWorker { tx: tx.clone() };
        let worker_idle_timeout = self
            .flush_interval
            .checked_mul(WORKER_IDLE_TIMEOUT_MULTIPLIER)
            .unwrap_or(self.flush_interval);

        start_worker(
            key,
            worker.tx.clone(),
            self.workers.clone(),
            rx,
            self.shutdown.clone(),
            self.partition_manager.clone(),
            self.node_manager.clone(),
            self.catalog_manager.clone(),
            self.flush_interval,
            worker_idle_timeout,
            self.max_batch_rows,
            self.flush_semaphore.clone(),
        );

        worker
    }
}

impl Drop for PendingRowsBatcher {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
    }
}

impl PendingBatch {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            created_at: None,
            total_row_count: 0,
            ctx: None,
            waiters: Vec::new(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn start_worker(
    key: BatchKey,
    worker_tx: mpsc::Sender<WorkerCommand>,
    workers: Arc<DashMap<BatchKey, PendingWorker>>,
    mut rx: mpsc::Receiver<WorkerCommand>,
    shutdown: broadcast::Sender<()>,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
    flush_interval: Duration,
    worker_idle_timeout: Duration,
    max_batch_rows: usize,
    flush_semaphore: Arc<Semaphore>,
) {
    tokio::spawn(async move {
        let mut batch = PendingBatch::new();
        let mut interval = tokio::time::interval(flush_interval);
        let mut shutdown_rx = shutdown.subscribe();
        let idle_deadline = tokio::time::Instant::now() + worker_idle_timeout;
        let idle_timer = tokio::time::sleep_until(idle_deadline);
        tokio::pin!(idle_timer);

        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    match cmd {
                        Some(WorkerCommand::Submit { table_batches, total_rows, ctx, response_tx, _permit }) => {
                            idle_timer.as_mut().reset(tokio::time::Instant::now() + worker_idle_timeout);

                            if batch.total_row_count == 0 {
                                batch.created_at = Some(Instant::now());
                                batch.ctx = Some(ctx);
                                PENDING_BATCHES.inc();
                            }

                            batch.waiters.push(FlushWaiter { response_tx, _permit });

                            for (table_name, table_id, record_batch) in table_batches {
                                let entry = batch.tables.entry(table_name.clone()).or_insert_with(|| TableBatch {
                                    table_name,
                                    table_id: Some(table_id),
                                    batches: Vec::new(),
                                    row_count: 0,
                                });
                                entry.row_count += record_batch.num_rows();
                                entry.batches.push(record_batch);
                            }

                            batch.total_row_count += total_rows;
                            PENDING_ROWS.add(total_rows as i64);

                            if batch.total_row_count >= max_batch_rows
                                && let Some(flush) = drain_batch(&mut batch) {
                                    spawn_flush(
                                        flush,
                                        partition_manager.clone(),
                                        node_manager.clone(),
                                        catalog_manager.clone(),
                                        flush_semaphore.clone(),
                                    ).await;
                            }
                        }
                        None => {
                            if let Some(flush) = drain_batch(&mut batch) {
                                flush_batch(
                                    flush,
                                    partition_manager.clone(),
                                    node_manager.clone(),
                                    catalog_manager.clone(),
                                ).await;
                            }
                            break;
                        }
                    }
                }
                _ = &mut idle_timer => {
                    if !should_close_worker_on_idle_timeout(batch.total_row_count, rx.len()) {
                        idle_timer
                            .as_mut()
                            .reset(tokio::time::Instant::now() + worker_idle_timeout);
                        continue;
                    }

                    debug!(
                        "Closing idle pending rows worker due to timeout: catalog={}, schema={}, physical_table={}",
                        key.catalog,
                        key.schema,
                        key.physical_table
                    );
                    break;
                }
                _ = interval.tick() => {
                    if let Some(created_at) = batch.created_at
                        && batch.total_row_count > 0
                        && created_at.elapsed() >= flush_interval
                        && let Some(flush) = drain_batch(&mut batch) {
                            spawn_flush(
                                flush,
                                partition_manager.clone(),
                                node_manager.clone(),
                                catalog_manager.clone(),
                                flush_semaphore.clone(),
                            ).await;
                    }
                }
                _ = shutdown_rx.recv() => {
                    if let Some(flush) = drain_batch(&mut batch) {
                        flush_batch(
                            flush,
                            partition_manager.clone(),
                            node_manager.clone(),
                            catalog_manager.clone(),
                        ).await;
                    }
                    break;
                }
            }
        }

        remove_worker_if_same_channel(workers.as_ref(), &key, &worker_tx);
    });
}

fn remove_worker_if_same_channel(
    workers: &DashMap<BatchKey, PendingWorker>,
    key: &BatchKey,
    worker_tx: &mpsc::Sender<WorkerCommand>,
) -> bool {
    if let Some(worker) = workers.get(key)
        && worker.tx.same_channel(worker_tx)
    {
        drop(worker);
        workers.remove(key);
        PENDING_WORKERS.set(workers.len() as i64);
        return true;
    }

    false
}

fn should_close_worker_on_idle_timeout(total_row_count: usize, queued_requests: usize) -> bool {
    total_row_count == 0 && queued_requests == 0
}

fn drain_batch(batch: &mut PendingBatch) -> Option<FlushBatch> {
    if batch.total_row_count == 0 {
        return None;
    }

    let ctx = match batch.ctx.take() {
        Some(ctx) => ctx,
        None => {
            flush_with_error(batch, "Pending batch missing context");
            return None;
        }
    };

    let total_row_count = batch.total_row_count;
    let table_batches = std::mem::take(&mut batch.tables).into_values().collect();
    let waiters = std::mem::take(&mut batch.waiters);
    batch.total_row_count = 0;
    batch.created_at = None;

    PENDING_ROWS.sub(total_row_count as i64);
    PENDING_BATCHES.dec();

    Some(FlushBatch {
        table_batches,
        total_row_count,
        ctx,
        waiters,
    })
}

async fn spawn_flush(
    flush: FlushBatch,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
    semaphore: Arc<Semaphore>,
) {
    match semaphore.acquire_owned().await {
        Ok(permit) => {
            tokio::spawn(async move {
                let _permit = permit;
                flush_batch(flush, partition_manager, node_manager, catalog_manager).await;
            });
        }
        Err(err) => {
            warn!(err; "Flush semaphore closed, flushing inline");
            flush_batch(flush, partition_manager, node_manager, catalog_manager).await;
        }
    }
}

struct FlushRegionWrite {
    region_id: RegionId,
    row_count: usize,
    datanode: Peer,
    request: RegionRequest,
}

enum FlushWriteResult {
    Success { row_count: usize },
    Failed { row_count: usize, message: String },
}

fn should_dispatch_concurrently(region_write_count: usize) -> bool {
    region_write_count > 1
}

async fn flush_region_writes_concurrently(
    node_manager: NodeManagerRef,
    writes: Vec<FlushRegionWrite>,
) -> Vec<FlushWriteResult> {
    if !should_dispatch_concurrently(writes.len()) {
        let mut results = Vec::with_capacity(writes.len());
        for write in writes {
            let datanode = node_manager.datanode(&write.datanode).await;
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_write_region"])
                .start_timer();
            match datanode.handle(write.request).await {
                Ok(_) => results.push(FlushWriteResult::Success {
                    row_count: write.row_count,
                }),
                Err(err) => results.push(FlushWriteResult::Failed {
                    row_count: write.row_count,
                    message: format!(
                        "Bulk insert flush failed for region {}: {:?}",
                        write.region_id, err
                    ),
                }),
            }
        }
        return results;
    }

    let write_futures = writes.into_iter().map(|write| {
        let node_manager = node_manager.clone();
        async move {
            let datanode = node_manager.datanode(&write.datanode).await;
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_write_region"])
                .start_timer();

            match datanode.handle(write.request).await {
                Ok(_) => FlushWriteResult::Success {
                    row_count: write.row_count,
                },
                Err(err) => FlushWriteResult::Failed {
                    row_count: write.row_count,
                    message: format!(
                        "Bulk insert flush failed for region {}: {:?}",
                        write.region_id, err
                    ),
                },
            }
        }
    });

    futures::future::join_all(write_futures).await
}

async fn flush_batch(
    flush: FlushBatch,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
) {
    let FlushBatch {
        table_batches,
        total_row_count,
        ctx,
        waiters,
    } = flush;
    let start = Instant::now();
    let mut first_error: Option<String> = None;

    let catalog = ctx.current_catalog().to_string();
    let schema = ctx.current_schema();

    // Attempt physical-table-level flush: transform all logical table batches
    // into physical format and write them together, reducing catalog lookups
    // and partition rule fetches from N (per logical table) to 1.
    let physical_table_name = ctx
        .extension(PHYSICAL_TABLE_KEY)
        .unwrap_or(GREPTIME_PHYSICAL_TABLE)
        .to_string();
    let physical_flush_attempted = flush_batch_physical(
        &table_batches,
        total_row_count,
        &catalog,
        &schema,
        &physical_table_name,
        &ctx,
        &partition_manager,
        &node_manager,
        &catalog_manager,
        &mut first_error,
    )
    .await;

    if !physical_flush_attempted {
        // Fallback: flush per logical table (original path).
        // This handles non-metric-engine tables, dense encoding, or any
        // case where the physical flush path cannot be used.
        flush_batch_per_logical_table(
            &table_batches,
            &catalog,
            &schema,
            &ctx,
            &partition_manager,
            &node_manager,
            &catalog_manager,
            &mut first_error,
        )
        .await;
    }

    let elapsed = start.elapsed().as_secs_f64();
    FLUSH_ELAPSED.observe(elapsed);
    debug!(
        "Pending rows batch flushed, total rows: {}, elapsed time: {}s",
        total_row_count, elapsed
    );

    notify_waiters(waiters, &first_error);
}

/// Attempts to flush all table batches by transforming them into the physical
/// table format (sparse primary key encoding) and writing directly to the
/// physical data regions.
///
/// Returns `true` if the physical flush was attempted (even if some individual
/// table batches failed). Returns `false` if the physical table could not be
/// resolved or does not support this path, meaning the caller should fall back
/// to per-logical-table flushing.
#[allow(clippy::too_many_arguments)]
async fn flush_batch_physical(
    table_batches: &[TableBatch],
    total_row_count: usize,
    catalog: &str,
    schema: &str,
    physical_table_name: &str,
    ctx: &QueryContextRef,
    partition_manager: &PartitionRuleManagerRef,
    node_manager: &NodeManagerRef,
    catalog_manager: &CatalogManagerRef,
    first_error: &mut Option<String>,
) -> bool {
    macro_rules! record_failure {
        ($row_count:expr, $msg:expr) => {{
            let msg = $msg;
            if first_error.is_none() {
                *first_error = Some(msg.clone());
            }
            mark_flush_failure($row_count, &msg);
        }};
    }

    // 1. Resolve the physical table and get column ID mapping
    let physical_table = {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_resolve_table"])
            .start_timer();
        match catalog_manager
            .table(catalog, schema, physical_table_name, Some(ctx.as_ref()))
            .await
        {
            Ok(Some(table)) => table,
            Ok(None) => {
                debug!(
                    "Physical table '{}' not found, falling back to per-logical-table flush",
                    physical_table_name
                );
                return false;
            }
            Err(err) => {
                debug!(
                    "Failed to resolve physical table '{}': {:?}, falling back",
                    physical_table_name, err
                );
                return false;
            }
        }
    };

    let physical_table_info = physical_table.table_info();
    let name_to_ids = match physical_table_info.name_to_ids() {
        Some(ids) => ids,
        None => {
            debug!(
                "Physical table '{}' has no column IDs, falling back to per-logical-table flush",
                physical_table_name
            );
            return false;
        }
    };

    // 2. Get the physical table's partition rule (one lookup instead of N)
    let partition_rule = {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_fetch_partition_rule"])
            .start_timer();
        match partition_manager
            .find_table_partition_rule(&physical_table_info)
            .await
        {
            Ok(rule) => rule,
            Err(err) => {
                debug!(
                    "Failed to fetch partition rule for physical table '{}': {:?}, falling back",
                    physical_table_name, err
                );
                return false;
            }
        }
    };

    // 3. Transform each logical table batch into physical format
    let mut modified_batches: Vec<RecordBatch> = Vec::with_capacity(table_batches.len());
    let mut modified_row_count: usize = 0;

    'next_table: for table_batch in table_batches {
        let table_id = match table_batch.table_id {
            Some(id) => id,
            None => {
                record_failure!(
                    table_batch.row_count,
                    format!(
                        "Missing table_id for logical table '{}' during physical flush",
                        table_batch.table_name
                    )
                );
                continue 'next_table;
            }
        };

        let Some(first_batch) = table_batch.batches.first() else {
            continue 'next_table;
        };

        // Identify tag columns and non-tag columns from the logical batch schema.
        // All chunks within a table_batch share the same schema, so we resolve
        // column metadata once from the first batch.
        // In prom batches, Float64 = value, Timestamp = timestamp, Utf8 = tags.
        let mut tag_columns = Vec::new();
        let mut non_tag_indices = Vec::new();
        let batch_schema = first_batch.schema();
        let mut column_resolution_failed = false;
        for (index, field) in batch_schema.fields().iter().enumerate() {
            match field.data_type() {
                ArrowDataType::Utf8 => {
                    let column_id = match name_to_ids.get(field.name()) {
                        Some(&id) => id,
                        None => {
                            // Column not found in physical table — this table batch
                            // cannot be transformed to physical format. Record error
                            // and skip the entire table batch.
                            warn!(
                                "Column '{}' from logical table '{}' not found in physical table column IDs",
                                field.name(),
                                table_batch.table_name
                            );
                            record_failure!(
                                table_batch.row_count,
                                format!(
                                    "Column '{}' not found in physical table for logical table '{}'",
                                    field.name(),
                                    table_batch.table_name
                                )
                            );
                            column_resolution_failed = true;
                            break;
                        }
                    };
                    tag_columns.push(TagColumnInfo {
                        name: field.name().clone(),
                        index,
                        column_id,
                    });
                }
                _ => {
                    non_tag_indices.push(index);
                }
            }
        }
        if column_resolution_failed {
            continue 'next_table;
        }
        tag_columns.sort_by(|a, b| a.name.cmp(&b.name));

        // Transform each chunk to physical format directly, avoiding an
        // intermediate concat_batches per logical table.
        for batch in &table_batch.batches {
            let modified = {
                let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                    .with_label_values(&["flush_physical_modify_batch"])
                    .start_timer();
                match modify_batch_sparse(batch.clone(), table_id, &tag_columns, &non_tag_indices) {
                    Ok(batch) => batch,
                    Err(err) => {
                        record_failure!(
                            table_batch.row_count,
                            format!(
                                "Failed to modify batch for logical table '{}': {:?}",
                                table_batch.table_name, err
                            )
                        );
                        continue 'next_table;
                    }
                }
            };

            modified_row_count += modified.num_rows();
            modified_batches.push(modified);
        }
    }

    if modified_batches.is_empty() {
        return true;
    }

    // 4. Concatenate all modified batches (all share the same physical schema)
    let combined_batch = {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_concat_all"])
            .start_timer();
        let combined_schema = modified_batches[0].schema();
        match concat_batches(&combined_schema, &modified_batches) {
            Ok(batch) => batch,
            Err(err) => {
                record_failure!(
                    modified_row_count,
                    format!("Failed to concat modified batches: {:?}", err)
                );
                return true;
            }
        }
    };

    // 5. Split by physical partition rule and send to regions
    let physical_table_id = physical_table_info.table_id();
    let region_masks = {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_split_record_batch"])
            .start_timer();
        match partition_rule.0.split_record_batch(&combined_batch) {
            Ok(masks) => masks,
            Err(err) => {
                record_failure!(
                    total_row_count,
                    format!(
                        "Failed to split combined batch for physical table '{}': {:?}",
                        physical_table_name, err
                    )
                );
                return true;
            }
        }
    };

    let mut region_writes = Vec::new();
    for (region_number, mask) in region_masks {
        if mask.select_none() {
            continue;
        }

        let region_batch = if mask.select_all() {
            combined_batch.clone()
        } else {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_physical_filter_record_batch"])
                .start_timer();
            match filter_record_batch(&combined_batch, mask.array()) {
                Ok(batch) => batch,
                Err(err) => {
                    record_failure!(
                        total_row_count,
                        format!(
                            "Failed to filter combined batch for physical table '{}': {:?}",
                            physical_table_name, err
                        )
                    );
                    continue;
                }
            }
        };

        let row_count = region_batch.num_rows();
        if row_count == 0 {
            continue;
        }

        let region_id = RegionId::new(physical_table_id, region_number);
        let datanode = {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_physical_resolve_region_leader"])
                .start_timer();
            match partition_manager.find_region_leader(region_id).await {
                Ok(peer) => peer,
                Err(err) => {
                    record_failure!(
                        row_count,
                        format!(
                            "Failed to resolve region leader for physical region {}: {:?}",
                            region_id, err
                        )
                    );
                    continue;
                }
            }
        };

        let (schema_bytes, data_header, payload) = {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_physical_encode_ipc"])
                .start_timer();
            match record_batch_to_ipc(region_batch) {
                Ok(encoded) => encoded,
                Err(err) => {
                    record_failure!(
                        row_count,
                        format!(
                            "Failed to encode Arrow IPC for physical region {}: {:?}",
                            region_id, err
                        )
                    );
                    continue;
                }
            }
        };

        let request = RegionRequest {
            header: Some(RegionRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                ..Default::default()
            }),
            body: Some(region_request::Body::BulkInsert(BulkInsertRequest {
                region_id: region_id.as_u64(),
                partition_expr_version: None,
                body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                    schema: schema_bytes,
                    data_header,
                    payload,
                })),
            })),
        };

        region_writes.push(FlushRegionWrite {
            region_id,
            row_count,
            datanode,
            request,
        });
    }

    for result in flush_region_writes_concurrently(node_manager.clone(), region_writes).await {
        match result {
            FlushWriteResult::Success { row_count } => {
                FLUSH_TOTAL.inc();
                FLUSH_ROWS.observe(row_count as f64);
            }
            FlushWriteResult::Failed { row_count, message } => {
                record_failure!(row_count, message);
            }
        }
    }

    true
}

/// Original per-logical-table flush path. Used as fallback when the physical
/// table flush path cannot be applied.
#[allow(clippy::too_many_arguments)]
async fn flush_batch_per_logical_table(
    table_batches: &[TableBatch],
    catalog: &str,
    schema: &str,
    ctx: &QueryContextRef,
    partition_manager: &PartitionRuleManagerRef,
    node_manager: &NodeManagerRef,
    catalog_manager: &CatalogManagerRef,
    first_error: &mut Option<String>,
) {
    macro_rules! record_failure {
        ($row_count:expr, $msg:expr) => {{
            let msg = $msg;
            if first_error.is_none() {
                *first_error = Some(msg.clone());
            }
            mark_flush_failure($row_count, &msg);
        }};
    }

    for table_batch in table_batches {
        let Some(first_batch) = table_batch.batches.first() else {
            continue;
        };

        let schema_ref = first_batch.schema();
        let record_batch = {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_concat_table_batches"])
                .start_timer();
            match concat_batches(&schema_ref, &table_batch.batches) {
                Ok(batch) => batch,
                Err(err) => {
                    record_failure!(
                        table_batch.row_count,
                        format!(
                            "Failed to concat table batch {}: {:?}",
                            table_batch.table_name, err
                        )
                    );
                    continue;
                }
            }
        };

        let table = {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_resolve_table"])
                .start_timer();
            match catalog_manager
                .table(catalog, schema, &table_batch.table_name, Some(ctx.as_ref()))
                .await
            {
                Ok(Some(table)) => table,
                Ok(None) => {
                    record_failure!(
                        table_batch.row_count,
                        format!(
                            "Table not found during pending flush: {}",
                            table_batch.table_name
                        )
                    );
                    continue;
                }
                Err(err) => {
                    record_failure!(
                        table_batch.row_count,
                        format!(
                            "Failed to resolve table {} for pending flush: {:?}",
                            table_batch.table_name, err
                        )
                    );
                    continue;
                }
            }
        };
        let table_info = table.table_info();

        let partition_rule = {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_fetch_partition_rule"])
                .start_timer();
            match partition_manager
                .find_table_partition_rule(&table_info)
                .await
            {
                Ok(rule) => rule,
                Err(err) => {
                    record_failure!(
                        table_batch.row_count,
                        format!(
                            "Failed to fetch partition rule for table {}: {:?}",
                            table_batch.table_name, err
                        )
                    );
                    continue;
                }
            }
        };

        let region_masks = {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_split_record_batch"])
                .start_timer();
            match partition_rule.0.split_record_batch(&record_batch) {
                Ok(masks) => masks,
                Err(err) => {
                    record_failure!(
                        table_batch.row_count,
                        format!(
                            "Failed to split record batch for table {}: {:?}",
                            table_batch.table_name, err
                        )
                    );
                    continue;
                }
            }
        };

        let mut region_writes = Vec::new();
        for (region_number, mask) in region_masks {
            if mask.select_none() {
                continue;
            }

            let region_batch = if mask.select_all() {
                record_batch.clone()
            } else {
                let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                    .with_label_values(&["flush_filter_record_batch"])
                    .start_timer();
                match filter_record_batch(&record_batch, mask.array()) {
                    Ok(batch) => batch,
                    Err(err) => {
                        record_failure!(
                            table_batch.row_count,
                            format!(
                                "Failed to filter record batch for table {}: {:?}",
                                table_batch.table_name, err
                            )
                        );
                        continue;
                    }
                }
            };

            let row_count = region_batch.num_rows();
            if row_count == 0 {
                continue;
            }

            let region_id = RegionId::new(table_info.table_id(), region_number);
            let datanode = {
                let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                    .with_label_values(&["flush_resolve_region_leader"])
                    .start_timer();
                match partition_manager.find_region_leader(region_id).await {
                    Ok(peer) => peer,
                    Err(err) => {
                        record_failure!(
                            row_count,
                            format!("Failed to resolve region leader {}: {:?}", region_id, err)
                        );
                        continue;
                    }
                }
            };

            let (schema_bytes, data_header, payload) = {
                let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                    .with_label_values(&["flush_encode_ipc"])
                    .start_timer();
                match record_batch_to_ipc(region_batch) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        record_failure!(
                            row_count,
                            format!(
                                "Failed to encode Arrow IPC for region {}: {:?}",
                                region_id, err
                            )
                        );
                        continue;
                    }
                }
            };

            let request = RegionRequest {
                header: Some(RegionRequestHeader {
                    tracing_context: TracingContext::from_current_span().to_w3c(),
                    ..Default::default()
                }),
                body: Some(region_request::Body::BulkInsert(BulkInsertRequest {
                    region_id: region_id.as_u64(),
                    partition_expr_version: None,
                    body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                        schema: schema_bytes,
                        data_header,
                        payload,
                    })),
                })),
            };

            region_writes.push(FlushRegionWrite {
                region_id,
                row_count,
                datanode,
                request,
            });
        }

        for result in flush_region_writes_concurrently(node_manager.clone(), region_writes).await {
            match result {
                FlushWriteResult::Success { row_count } => {
                    FLUSH_TOTAL.inc();
                    FLUSH_ROWS.observe(row_count as f64);
                }
                FlushWriteResult::Failed { row_count, message } => {
                    record_failure!(row_count, message);
                }
            }
        }
    }
}

fn notify_waiters(waiters: Vec<FlushWaiter>, first_error: &Option<String>) {
    for waiter in waiters {
        let result = match first_error {
            Some(err_msg) => Err(Error::Internal {
                err_msg: err_msg.clone(),
            }),
            None => Ok(()),
        };
        let _ = waiter.response_tx.send(result);
        // waiter._permit is dropped here, releasing the inflight semaphore slot
    }
}

fn mark_flush_failure(row_count: usize, message: &str) {
    error!("Pending rows batch flush failed, message: {}", message);
    FLUSH_FAILURES.inc();
    FLUSH_DROPPED_ROWS.inc_by(row_count as u64);
}

fn flush_with_error(batch: &mut PendingBatch, message: &str) {
    if batch.total_row_count == 0 {
        return;
    }

    let row_count = batch.total_row_count;
    let waiters = std::mem::take(&mut batch.waiters);
    batch.tables.clear();
    batch.total_row_count = 0;
    batch.created_at = None;
    batch.ctx = None;

    PENDING_ROWS.sub(row_count as i64);
    PENDING_BATCHES.dec();

    let err_msg = Some(message.to_string());
    notify_waiters(waiters, &err_msg);
    mark_flush_failure(row_count, message);
}

fn record_batch_to_ipc(record_batch: RecordBatch) -> Result<(Bytes, Bytes, Bytes)> {
    let mut encoder = FlightEncoder::default();
    let schema = encoder.encode_schema(record_batch.schema().as_ref());
    let mut iter = encoder
        .encode(FlightMessage::RecordBatch(record_batch))
        .into_iter();
    let Some(flight_data) = iter.next() else {
        return Err(Error::Internal {
            err_msg: "Failed to encode empty flight data".to_string(),
        });
    };
    if iter.next().is_some() {
        return Err(Error::NotSupported {
            feat: "bulk insert RecordBatch with dictionary arrays".to_string(),
        });
    }

    Ok((
        schema.data_header,
        flight_data.data_header,
        flight_data.data_body,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use api::region::RegionResponse;
    use api::v1::flow::{DirtyWindowRequests, FlowRequest, FlowResponse};
    use api::v1::meta::Peer;
    use api::v1::region::{InsertRequests, RegionRequest};
    use async_trait::async_trait;
    use common_meta::error::Result as MetaResult;
    use common_meta::node_manager::{
        Datanode, DatanodeManager, DatanodeRef, Flownode, FlownodeManager, FlownodeRef,
    };
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use store_api::storage::RegionId;
    use tokio::time::sleep;
    use dashmap::DashMap;
    use tokio::sync::mpsc;

    use super::{
        FlushRegionWrite, FlushWriteResult, flush_region_writes_concurrently,
        should_dispatch_concurrently,
    };
    use super::{
        BatchKey, PendingWorker, WorkerCommand, align_record_batch_to_schema,
        remove_worker_if_same_channel, rows_to_record_batch, should_close_worker_on_idle_timeout,
    };

    #[derive(Clone)]
    struct ConcurrentMockDatanode {
        delay: Duration,
        inflight: Arc<AtomicUsize>,
        max_inflight: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Datanode for ConcurrentMockDatanode {
        async fn handle(&self, _request: RegionRequest) -> MetaResult<RegionResponse> {
            let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            loop {
                let max = self.max_inflight.load(Ordering::SeqCst);
                if now <= max {
                    break;
                }
                if self
                    .max_inflight
                    .compare_exchange(max, now, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }

            sleep(self.delay).await;
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            Ok(RegionResponse::new(0))
        }

        async fn handle_query(
            &self,
            _request: QueryRequest,
        ) -> MetaResult<SendableRecordBatchStream> {
            unimplemented!()
        }
    }

    #[derive(Clone)]
    struct ConcurrentMockNodeManager {
        datanodes: Arc<HashMap<u64, DatanodeRef>>,
    }

    #[async_trait]
    impl DatanodeManager for ConcurrentMockNodeManager {
        async fn datanode(&self, node: &Peer) -> DatanodeRef {
            self.datanodes
                .get(&node.id)
                .expect("datanode not found")
                .clone()
        }
    }

    struct NoopFlownode;

    #[async_trait]
    impl Flownode for NoopFlownode {
        async fn handle(&self, _request: FlowRequest) -> MetaResult<FlowResponse> {
            unimplemented!()
        }

        async fn handle_inserts(&self, _request: InsertRequests) -> MetaResult<FlowResponse> {
            unimplemented!()
        }

        async fn handle_mark_window_dirty(
            &self,
            _req: DirtyWindowRequests,
        ) -> MetaResult<FlowResponse> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl FlownodeManager for ConcurrentMockNodeManager {
        async fn flownode(&self, _node: &Peer) -> FlownodeRef {
            Arc::new(NoopFlownode)
        }
    }

    #[test]
    fn test_rows_to_record_batch() {
        let rows = Rows {
            schema: vec![
                ColumnSchema {
                    column_name: "ts".to_string(),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    semantic_type: SemanticType::Timestamp as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "value".to_string(),
                    datatype: ColumnDataType::Float64 as i32,
                    semantic_type: SemanticType::Field as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "host".to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
            ],
            rows: vec![
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(1000)),
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(42.0)),
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("h1".to_string())),
                        },
                    ],
                },
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(2000)),
                        },
                        Value { value_data: None },
                        Value {
                            value_data: Some(ValueData::StringValue("h2".to_string())),
                        },
                    ],
                },
            ],
        };

        let rb = rows_to_record_batch(&rows).unwrap();
        assert_eq!(2, rb.num_rows());
        assert_eq!(3, rb.num_columns());
    }

    #[test]
    fn test_align_record_batch_to_schema_reorder_and_fill_missing() {
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(StringArray::from(vec!["h1"])),
                Arc::new(Float64Array::from(vec![42.0])),
            ],
        )
        .unwrap();

        let target = ArrowSchema::new(vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]);

        let aligned = align_record_batch_to_schema(source, &target).unwrap();
        assert_eq!(aligned.schema().as_ref(), &target);
        assert_eq!(1, aligned.num_rows());
        assert_eq!(3, aligned.num_columns());
        let ts = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(ts.is_null(0));
    }

    #[test]
    fn test_align_record_batch_to_schema_cast_column_type() {
        let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int32Array::from(vec![Some(7), None]))],
        )
        .unwrap();

        let target = ArrowSchema::new(vec![Field::new("value", DataType::Int64, true)]);
        let aligned = align_record_batch_to_schema(source, &target).unwrap();
        let value = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(Some(7), value.iter().next().flatten());
        assert!(value.is_null(1));
    }

    #[test]
    fn test_remove_worker_if_same_channel_removes_matching_entry() {
        let workers = DashMap::new();
        let key = BatchKey {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            physical_table: "phy".to_string(),
        };

        let (tx, _rx) = mpsc::channel::<WorkerCommand>(1);
        workers.insert(key.clone(), PendingWorker { tx: tx.clone() });

        assert!(remove_worker_if_same_channel(&workers, &key, &tx));
        assert!(!workers.contains_key(&key));
    }

    #[test]
    fn test_remove_worker_if_same_channel_keeps_newer_entry() {
        let workers = DashMap::new();
        let key = BatchKey {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            physical_table: "phy".to_string(),
        };

        let (stale_tx, _stale_rx) = mpsc::channel::<WorkerCommand>(1);
        let (fresh_tx, _fresh_rx) = mpsc::channel::<WorkerCommand>(1);
        workers.insert(
            key.clone(),
            PendingWorker {
                tx: fresh_tx.clone(),
            },
        );

        assert!(!remove_worker_if_same_channel(&workers, &key, &stale_tx));
        assert!(workers.contains_key(&key));
        assert!(workers.get(&key).unwrap().tx.same_channel(&fresh_tx));
    }

    #[test]
    fn test_worker_idle_timeout_close_decision() {
        assert!(should_close_worker_on_idle_timeout(0, 0));
        assert!(!should_close_worker_on_idle_timeout(1, 0));
        assert!(!should_close_worker_on_idle_timeout(0, 1));
    }


    #[test]
    fn test_prepare_record_batch_for_target_schema_collects_missing_tag_columns() {
        let source = ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("instance", DataType::Utf8, true),
            Field::new("greptime_value", DataType::Float64, true),
        ]);
        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let record_batch = RecordBatch::try_new(
            Arc::new(source),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![Some(1000)])),
                Arc::new(StringArray::from(vec!["h1"])),
                Arc::new(StringArray::from(vec!["i1"])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();

        let (_, missing) =
            accommodate_record_batch_for_target_schema(record_batch, &target).unwrap();
        assert_eq!(missing, vec!["instance".to_string()]);
    }

    #[test]
    fn test_prepare_record_batch_for_target_schema_reject_non_utf8_missing_column() {
        let source = ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("code", DataType::Utf8, true),
            Field::new("greptime_value", DataType::Float64, true),
        ]);
        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let record_batch = RecordBatch::try_new(
            Arc::new(source),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![Some(1000)])),
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let (rb, mut missing) =
            accommodate_record_batch_for_target_schema(record_batch, &target).unwrap();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing.swap_remove(0).as_str(), "code");
        assert_eq!(
            rb.schema()
                .fields
                .iter()
                .find(|f| matches!(f.data_type(), DataType::Timestamp(_, _)))
                .unwrap()
                .name(),
            "my_ts"
        );
        assert_eq!(
            rb.schema()
                .fields
                .iter()
                .find(|f| matches!(f.data_type(), DataType::Float64))
                .unwrap()
                .name(),
            "my_value"
        );
    }

    #[test]
    fn test_build_prom_create_table_schema_from_request_schema() {
        let source = ArrowSchema::new(vec![
            Field::new(
                common_query::prelude::greptime_timestamp(),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("job", DataType::Utf8, true),
            Field::new(
                common_query::prelude::greptime_value(),
                DataType::Float64,
                true,
            ),
        ]);

        let schema = build_prom_create_table_schema(&source).unwrap();
        assert_eq!(3, schema.len());

        assert_eq!(
            common_query::prelude::greptime_timestamp(),
            schema[0].column_name
        );
        assert_eq!(
            api::v1::SemanticType::Timestamp as i32,
            schema[0].semantic_type
        );
        assert_eq!(
            api::v1::ColumnDataType::TimestampMillisecond as i32,
            schema[0].datatype
        );

        assert_eq!("job", schema[1].column_name);
        assert_eq!(api::v1::SemanticType::Tag as i32, schema[1].semantic_type);
        assert_eq!(api::v1::ColumnDataType::String as i32, schema[1].datatype);

        assert_eq!(
            common_query::prelude::greptime_value(),
            schema[2].column_name
        );
        assert_eq!(api::v1::SemanticType::Field as i32, schema[2].semantic_type);
        assert_eq!(api::v1::ColumnDataType::Float64 as i32, schema[2].datatype);
    }

    #[test]
    fn test_prepare_record_batch_for_target_schema_renames_prom_special_columns() {
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("greptime_value", DataType::Float64, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1000),
                    Some(2000),
                ])),
                Arc::new(StringArray::from(vec!["h1", "h2"])),
                Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])),
            ],
        )
        .unwrap();

        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let (prepared, missing) =
            accommodate_record_batch_for_target_schema(source, &target).unwrap();
        assert!(missing.is_empty());
        let aligned = align_record_batch_to_schema(prepared, &target).unwrap();

        assert_eq!(aligned.schema().as_ref(), &target);
        assert_eq!(2, aligned.num_rows());
        assert_eq!(3, aligned.num_columns());
    }

    #[test]
    fn test_prepare_record_batch_for_target_schema_requires_timestamp_column() {
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("greptime_value", DataType::Float64, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![Some(1000)])),
                Arc::new(StringArray::from(vec!["h1"])),
                Arc::new(Float64Array::from(vec![Some(1.0)])),
            ],
        )
        .unwrap();

        let target = ArrowSchema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let err = accommodate_record_batch_for_target_schema(source, &target).unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to locate timestamp column in target schema")
        );
    }

    #[test]
    fn test_prepare_record_batch_for_target_schema_requires_field_column() {
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("greptime_value", DataType::Float64, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![Some(1000)])),
                Arc::new(StringArray::from(vec!["h1"])),
                Arc::new(Float64Array::from(vec![Some(1.0)])),
            ],
        )
        .unwrap();

        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
        ]);

        let err = accommodate_record_batch_for_target_schema(source, &target).unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to locate field column in target schema")
        );
    }

    #[tokio::test]
    async fn test_flush_region_writes_concurrently_dispatches_multiple_datanodes() {
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_inflight = Arc::new(AtomicUsize::new(0));
        let datanode1: DatanodeRef = Arc::new(ConcurrentMockDatanode {
            delay: Duration::from_millis(100),
            inflight: inflight.clone(),
            max_inflight: max_inflight.clone(),
        });
        let datanode2: DatanodeRef = Arc::new(ConcurrentMockDatanode {
            delay: Duration::from_millis(100),
            inflight,
            max_inflight: max_inflight.clone(),
        });

        let mut datanodes = HashMap::new();
        datanodes.insert(1, datanode1);
        datanodes.insert(2, datanode2);
        let node_manager = Arc::new(ConcurrentMockNodeManager {
            datanodes: Arc::new(datanodes),
        });

        let writes = vec![
            FlushRegionWrite {
                region_id: RegionId::new(1024, 1),
                row_count: 10,
                datanode: Peer {
                    id: 1,
                    addr: "node1".to_string(),
                },
                request: RegionRequest::default(),
            },
            FlushRegionWrite {
                region_id: RegionId::new(1024, 2),
                row_count: 12,
                datanode: Peer {
                    id: 2,
                    addr: "node2".to_string(),
                },
                request: RegionRequest::default(),
            },
        ];

        let results = flush_region_writes_concurrently(node_manager, writes).await;
        assert_eq!(2, results.len());
        assert!(
            results
                .iter()
                .all(|result| matches!(result, FlushWriteResult::Success { .. }))
        );
        assert!(max_inflight.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn test_should_dispatch_concurrently_by_region_count() {
        assert!(!should_dispatch_concurrently(0));
        assert!(!should_dispatch_concurrently(1));
        assert!(should_dispatch_concurrently(2));
    }
}
