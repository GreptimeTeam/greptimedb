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

mod batch;
mod batch_convert;
mod pending_worker;
mod region_write;
#[cfg(test)]
mod test_util;

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use api::v1::flow::{DirtyWindowRequest, DirtyWindowRequests};
use api::v1::{ColumnSchema, RowInsertRequests, Rows};
use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_batcher::flush_limiter::FlushLimiter;
use common_batcher::flush_policy::timing::TimingFlushPolicy;
use common_batcher::notifier::{Notifier, run_notifier};
use common_batcher::request_limiter::RequestLimiter;
use common_batcher::worker_registry::WorkerRegistry;
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::node_manager::NodeManagerRef;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_runtime::spawn_global;
use common_telemetry::{error, warn};
use datatypes::timestamp::append_timestamps;
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use table::metadata::TableId;
use tokio::sync::{Semaphore, broadcast, mpsc, oneshot};

pub use crate::batcher::logical_table::batch::flush_batch_physical;
pub use crate::batcher::logical_table::batch_convert::{RecordBatchWithTsIdx, TableBatch};
use crate::batcher::logical_table::pending_worker::{
    PendingWorker, WorkerCommand, remove_worker_if_same_channel, start_worker,
};
pub use crate::batcher::logical_table::region_write::{
    PhysicalFlushCatalogProvider, PhysicalFlushNodeRequester, PhysicalFlushPartitionProvider,
    PhysicalTableMetadata,
};
use crate::error;
use crate::error::{Error, Result};
use crate::metrics::{
    FLOW_NOTIFICATION_DROPPED, PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED, PENDING_WORKERS,
};
use crate::prom_row_builder::{
    build_prom_create_table_schema_from_proto, identify_missing_columns_from_proto,
    rows_to_aligned_record_batch,
};

const PHYSICAL_TABLE_KEY: &str = "physical_table";

/// Whether wait for ingestion result before reply to client.
const PENDING_ROWS_BATCH_SYNC_ENV: &str = "PENDING_ROWS_BATCH_SYNC";

/// Returns whether pending-row batch submissions wait for the flush result
/// before replying to the client (synchronous mode), controlled by the
/// `PENDING_ROWS_BATCH_SYNC` environment variable and defaulting to `true`.
///
/// Callers that reason about how long a remote write request may block (e.g.
/// the frontend HTTP timeout fallback) must consult this instead of
/// duplicating the env lookup.
pub fn pending_rows_batch_sync_enabled() -> bool {
    std::env::var(PENDING_ROWS_BATCH_SYNC_ENV)
        .ok()
        .as_deref()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(true)
}

const WORKER_IDLE_TIMEOUT_MULTIPLIER: u32 = 3;

const MAX_CONCURRENT_FLOW_NOTIFICATIONS: NonZeroUsize = NonZeroUsize::new(8).unwrap();

#[async_trait]
pub trait PendingRowsSchemaAlterer: Send + Sync {
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

/// Intermediate planning state for resolving and preparing logical tables
/// before row-to-batch alignment.
struct TableResolutionPlan {
    /// Resolved table schema and table id by logical table name.
    region_schemas: HashMap<String, (Arc<ArrowSchema>, u32)>,
    /// Missing tables that need to be created before alignment.
    tables_to_create: Vec<(String, Vec<ColumnSchema>)>,
    /// Existing tables that need tag-column schema evolution.
    tables_to_alter: Vec<(String, Vec<String>)>,
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
pub struct LogicalTablePendingRowsBatcher {
    workers: Arc<WorkerRegistry<BatchKey, WorkerCommand>>,
    flush_interval: Duration,
    flush_policy: TimingFlushPolicy,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
    flow_notification_tx: Notifier<FlowNotification>,
    flush_limiter: FlushLimiter,
    request_limiter: RequestLimiter,
    worker_channel_capacity: usize,
    prom_store_with_metric_engine: bool,
    schema_alterer: PendingRowsSchemaAltererRef,
    pending_rows_batch_sync: bool,
    shutdown: broadcast::Sender<()>,
}

impl LogicalTablePendingRowsBatcher {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
        catalog_manager: CatalogManagerRef,
        table_flownode_set_cache: TableFlownodeSetCacheRef,
        prom_store_with_metric_engine: bool,
        schema_alterer: PendingRowsSchemaAltererRef,
        flush_interval: Duration,
        max_batch_rows: usize,
        max_concurrent_flushes: usize,
        worker_channel_capacity: usize,
        max_inflight_requests: usize,
        flow_notification_queue_capacity: NonZeroUsize,
    ) -> Option<Arc<Self>> {
        if worker_channel_capacity == 0 || worker_channel_capacity > Semaphore::MAX_PERMITS {
            return None;
        }

        let flush_policy = TimingFlushPolicy::try_new(flush_interval, max_batch_rows)?;
        let flush_limiter = FlushLimiter::try_new(max_concurrent_flushes)?;

        let request_limiter = RequestLimiter::try_new(max_inflight_requests)?;
        let (flow_notification_tx, flow_notification_rx) =
            Notifier::try_new(flow_notification_queue_capacity.get())?;

        let (shutdown, _) = broadcast::channel(1);
        let pending_rows_batch_sync = pending_rows_batch_sync_enabled();
        let workers = Arc::new(WorkerRegistry::new());
        PENDING_WORKERS.set(0);
        start_flow_notification_worker(
            flow_notification_rx,
            table_flownode_set_cache,
            node_manager.clone(),
        );

        Some(Arc::new(Self {
            workers,
            flush_interval,
            flush_policy,
            partition_manager,
            node_manager,
            catalog_manager,
            flow_notification_tx,
            prom_store_with_metric_engine,
            schema_alterer,
            flush_limiter,
            request_limiter,
            worker_channel_capacity,
            pending_rows_batch_sync,
            shutdown,
        }))
    }
}

impl LogicalTablePendingRowsBatcher {
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
            self.request_limiter
                .acquire()
                .await
                .map_err(|_| error::BatcherChannelClosedSnafu.build())?
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
                let worker = self.get_or_spawn_worker(batch_key.clone()).await;
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
                        )
                        .await;
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
                response_rx
                    .await
                    .map_err(|_| error::BatcherChannelClosedSnafu.build())?
            };
            result
                .context(error::SubmitBatchSnafu)
                .map(|()| total_rows as u64)
        } else {
            Ok(total_rows as u64)
        }
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Converts proto `RowInsertRequests` directly into aligned `RecordBatch`es
    /// in a single pass, handling table creation, schema alteration, column
    /// renaming, reordering, and null-filling without building intermediate
    /// RecordBatches.
    async fn build_and_align_table_batches(
        &self,
        requests: RowInsertRequests,
        ctx: &QueryContextRef,
    ) -> Result<(Vec<(String, u32, RecordBatchWithTsIdx)>, usize)> {
        let catalog = ctx.current_catalog().to_string();
        let schema = ctx.current_schema();

        let (table_rows, total_rows) = Self::collect_non_empty_table_rows(requests);
        if total_rows == 0 {
            return Ok((Vec::new(), 0));
        }

        let unique_tables = Self::collect_unique_table_schemas(&table_rows)?;
        let mut plan = self
            .plan_table_resolution(&catalog, &schema, ctx, &unique_tables)
            .await?;

        self.create_missing_tables_and_refresh_schemas(
            &catalog,
            &schema,
            ctx,
            &table_rows,
            &mut plan,
        )
        .await?;

        self.alter_tables_and_refresh_schemas(&catalog, &schema, ctx, &mut plan)
            .await?;

        let aligned_batches = Self::build_aligned_batches(&table_rows, &plan.region_schemas)?;

        Ok((aligned_batches, total_rows))
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Extracts non-empty `(table_name, rows)` pairs and computes total row
    /// count across the retained entries.
    fn collect_non_empty_table_rows(requests: RowInsertRequests) -> (Vec<(String, Rows)>, usize) {
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

        (table_rows, total_rows)
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Returns unique `(table_name, proto_schema)` pairs while keeping the
    /// first-seen schema for duplicate table names.
    fn collect_unique_table_schemas(
        table_rows: &[(String, Rows)],
    ) -> Result<Vec<(&str, &[ColumnSchema])>> {
        let mut unique_tables: Vec<(&str, &[ColumnSchema])> = Vec::with_capacity(table_rows.len());
        let mut seen = HashSet::new();

        for (table_name, rows) in table_rows {
            if seen.insert(table_name.as_str()) {
                unique_tables.push((table_name.as_str(), &rows.schema));
            } else {
                // table_rows should group rows by table name.
                return error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "Found duplicated table name in RowInsertRequest: {}",
                        table_name
                    ),
                }
                .fail();
            }
        }

        Ok(unique_tables)
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Resolves table metadata and classifies each table into existing,
    /// to-create, and to-alter groups used by subsequent DDL steps.
    async fn plan_table_resolution(
        &self,
        catalog: &str,
        schema: &str,
        ctx: &QueryContextRef,
        unique_tables: &[(&str, &[ColumnSchema])],
    ) -> Result<TableResolutionPlan> {
        let mut plan = TableResolutionPlan {
            region_schemas: HashMap::with_capacity(unique_tables.len()),
            tables_to_create: Vec::new(),
            tables_to_alter: Vec::new(),
        };

        let resolved_tables = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_resolve_table"])
                .start_timer();
            futures::future::join_all(unique_tables.iter().map(|(table_name, _)| {
                self.catalog_manager
                    .table(catalog, schema, table_name, Some(ctx.as_ref()))
            }))
            .await
        };

        for ((table_name, rows_schema), table_result) in unique_tables.iter().zip(resolved_tables) {
            let table = table_result?;

            if let Some(table) = table {
                let table_info = table.table_info();
                let table_id = table_info.ident.table_id;
                let region_schema = table_info.meta.schema.arrow_schema().clone();

                let missing_columns = {
                    let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                        .with_label_values(&["align_identify_missing_columns"])
                        .start_timer();
                    identify_missing_columns_from_proto(rows_schema, region_schema.as_ref())?
                };
                if !missing_columns.is_empty() {
                    plan.tables_to_alter
                        .push(((*table_name).to_string(), missing_columns));
                }
                plan.region_schemas
                    .insert((*table_name).to_string(), (region_schema, table_id));
            } else {
                let request_schema = {
                    let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                        .with_label_values(&["align_build_create_table_schema"])
                        .start_timer();
                    build_prom_create_table_schema_from_proto(rows_schema)?
                };
                plan.tables_to_create
                    .push(((*table_name).to_string(), request_schema));
            }
        }

        Ok(plan)
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Batch-creates missing tables, refreshes their schema metadata, and
    /// enqueues follow-up alters for extra tag columns discovered in later rows.
    async fn create_missing_tables_and_refresh_schemas(
        &self,
        catalog: &str,
        schema: &str,
        ctx: &QueryContextRef,
        table_rows: &[(String, Rows)],
        plan: &mut TableResolutionPlan,
    ) -> Result<()> {
        if plan.tables_to_create.is_empty() {
            return Ok(());
        }

        let create_refs: Vec<(&str, &[ColumnSchema])> = plan
            .tables_to_create
            .iter()
            .map(|(name, schema)| (name.as_str(), schema.as_slice()))
            .collect();

        {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_batch_create_tables"])
                .start_timer();
            self.schema_alterer
                .create_tables_if_missing_batch(
                    catalog,
                    schema,
                    &create_refs,
                    self.prom_store_with_metric_engine,
                    ctx.clone(),
                )
                .await?;
        }

        let created_table_results = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_resolve_table_after_create"])
                .start_timer();
            futures::future::join_all(plan.tables_to_create.iter().map(|(table_name, _)| {
                self.catalog_manager
                    .table(catalog, schema, table_name, Some(ctx.as_ref()))
            }))
            .await
        };

        for ((table_name, _), table_result) in
            plan.tables_to_create.iter().zip(created_table_results)
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
            plan.region_schemas
                .insert(table_name.clone(), (region_schema, table_id));
        }

        Self::enqueue_alter_for_new_tables(table_rows, plan)?;

        Ok(())
    }
}

impl LogicalTablePendingRowsBatcher {
    /// For newly created tables, re-checks all row schemas and appends alter
    /// operations when additional tag columns are still missing.
    fn enqueue_alter_for_new_tables(
        table_rows: &[(String, Rows)],
        plan: &mut TableResolutionPlan,
    ) -> Result<()> {
        let created_tables: HashSet<&str> = plan
            .tables_to_create
            .iter()
            .map(|(table_name, _)| table_name.as_str())
            .collect();

        for (table_name, rows) in table_rows {
            if !created_tables.contains(table_name.as_str()) {
                continue;
            }

            let Some((region_schema, _)) = plan.region_schemas.get(table_name) else {
                continue;
            };

            let missing_columns = identify_missing_columns_from_proto(&rows.schema, region_schema)?;
            if missing_columns.is_empty()
                || plan
                    .tables_to_alter
                    .iter()
                    .any(|(existing_name, _)| existing_name == table_name)
            {
                continue;
            }

            plan.tables_to_alter
                .push((table_name.clone(), missing_columns));
        }

        Ok(())
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Batch-alters tables that have missing tag columns and refreshes the
    /// in-memory schema map used for row alignment.
    async fn alter_tables_and_refresh_schemas(
        &self,
        catalog: &str,
        schema: &str,
        ctx: &QueryContextRef,
        plan: &mut TableResolutionPlan,
    ) -> Result<()> {
        if plan.tables_to_alter.is_empty() {
            return Ok(());
        }

        let alter_refs: Vec<(&str, &[String])> = plan
            .tables_to_alter
            .iter()
            .map(|(name, cols)| (name.as_str(), cols.as_slice()))
            .collect();
        {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_batch_add_missing_columns"])
                .start_timer();
            self.schema_alterer
                .add_missing_prom_tag_columns_batch(catalog, schema, &alter_refs, ctx.clone())
                .await?;
        }

        let altered_table_results = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_resolve_table_after_schema_alter"])
                .start_timer();
            futures::future::join_all(plan.tables_to_alter.iter().map(|(table_name, _)| {
                self.catalog_manager
                    .table(catalog, schema, table_name, Some(ctx.as_ref()))
            }))
            .await
        };

        for ((table_name, _), table_result) in
            plan.tables_to_alter.iter().zip(altered_table_results)
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
            plan.region_schemas
                .insert(table_name.clone(), (refreshed_region_schema, table_id));
        }

        Ok(())
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Converts proto rows to `RecordBatch` values aligned to resolved region
    /// schemas and returns `(table_name, table_id, batch)` tuples.
    fn build_aligned_batches(
        table_rows: &[(String, Rows)],
        region_schemas: &HashMap<String, (Arc<ArrowSchema>, u32)>,
    ) -> Result<Vec<(String, u32, RecordBatchWithTsIdx)>> {
        let mut aligned_batches = Vec::with_capacity(table_rows.len());
        for (table_name, rows) in table_rows {
            let (region_schema, table_id) =
                region_schemas.get(table_name).cloned().with_context(|| {
                    error::UnexpectedResultSnafu {
                        reason: format!("Region schema not resolved for table: {}", table_name),
                    }
                })?;

            let record_batch = {
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                    .with_label_values(&["align_rows_to_record_batch"])
                    .start_timer();
                rows_to_aligned_record_batch(rows, region_schema.as_ref())?
            };
            aligned_batches.push((table_name.clone(), table_id, record_batch));
        }

        Ok(aligned_batches)
    }
}

impl LogicalTablePendingRowsBatcher {
    async fn get_or_spawn_worker(&self, key: BatchKey) -> PendingWorker {
        let (tx, receiver) = self
            .workers
            .get_or_create(key.clone(), self.worker_channel_capacity)
            .await;
        if let Some(rx) = receiver {
            self.spawn_worker(key, tx.clone(), rx);
            PENDING_WORKERS.set(self.workers.len().await as i64);
        }
        PendingWorker { tx }
    }
}

impl LogicalTablePendingRowsBatcher {
    fn spawn_worker(
        &self,
        key: BatchKey,
        tx: mpsc::Sender<WorkerCommand>,
        rx: mpsc::Receiver<WorkerCommand>,
    ) {
        let worker_idle_timeout = self
            .flush_interval
            .checked_mul(WORKER_IDLE_TIMEOUT_MULTIPLIER)
            .unwrap_or(self.flush_interval);

        start_worker(
            key,
            tx,
            self.workers.clone(),
            rx,
            self.shutdown.clone(),
            self.partition_manager.clone(),
            self.node_manager.clone(),
            self.catalog_manager.clone(),
            self.flow_notification_tx.clone(),
            worker_idle_timeout,
            self.flush_policy,
            self.flush_limiter.clone(),
        );
    }
}

impl Drop for LogicalTablePendingRowsBatcher {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
    }
}

fn extract_timestamps(table_batch: &TableBatch) -> Vec<i64> {
    let mut timestamps = Vec::with_capacity(table_batch.row_count);
    for batch in &table_batch.batches {
        let timestamp_column = batch.batch.column(batch.timestamp_index);
        let Some(()) = append_timestamps(timestamp_column, &mut timestamps) else {
            error!(
                "Failed to extract timestamps from record batch, table_id: {}, timestamp_index: {}",
                table_batch.table_id, batch.timestamp_index
            );
            continue;
        };
    }
    timestamps
}

struct FlowNotification {
    table_id: TableId,
    timestamps: Vec<i64>,
}

fn try_enqueue_flow_notification(
    tx: &Notifier<FlowNotification>,
    notification: FlowNotification,
) -> bool {
    match tx.try_notify(notification) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(notification)) => {
            FLOW_NOTIFICATION_DROPPED.with_label_values(&["full"]).inc();
            warn!(
                "Dropping flow notification because queue is full, table_id: {}, queue_capacity: {}",
                notification.table_id,
                tx.max_capacity()
            );
            false
        }
        Err(mpsc::error::TrySendError::Closed(notification)) => {
            FLOW_NOTIFICATION_DROPPED
                .with_label_values(&["closed"])
                .inc();
            error!(
                "Dropping flow notification because queue is closed, table_id: {}, queue_capacity: {}",
                notification.table_id,
                tx.max_capacity()
            );
            false
        }
    }
}

fn enqueue_flow_notifications(table_batches: Vec<TableBatch>, tx: &Notifier<FlowNotification>) {
    for table_batch in table_batches {
        let timestamps = extract_timestamps(&table_batch);
        if timestamps.is_empty() {
            continue;
        }
        try_enqueue_flow_notification(
            tx,
            FlowNotification {
                table_id: table_batch.table_id,
                timestamps,
            },
        );
    }
}

async fn handle_flow_notification(
    notification: FlowNotification,
    table_flownode_set_cache: TableFlownodeSetCacheRef,
    node_manager: NodeManagerRef,
) {
    let table_id = notification.table_id;
    let flownodes = match table_flownode_set_cache.get(table_id).await {
        Ok(Some(flownodes)) => flownodes,
        Ok(None) => return,
        Err(e) => {
            error!(e; "Failed to get flownodes for table id: {}", table_id);
            return;
        }
    };
    let peers = flownodes.values().cloned().collect::<HashSet<_>>();

    for peer in peers {
        if let Err(e) = node_manager
            .flownode(&peer)
            .await
            .handle_mark_window_dirty(DirtyWindowRequests {
                requests: vec![DirtyWindowRequest {
                    table_id,
                    timestamps: notification.timestamps.clone(),
                    time_ranges: Vec::new(),
                }],
            })
            .await
        {
            error!(
                e;
                "Failed to mark timestamps as dirty, table_id: {}, peer_id: {}, peer_addr: {}",
                table_id,
                peer.id,
                peer.addr
            );
        }
    }
}

fn start_flow_notification_worker(
    notification_rx: mpsc::Receiver<FlowNotification>,
    table_flownode_set_cache: TableFlownodeSetCacheRef,
    node_manager: NodeManagerRef,
) {
    spawn_global(async move {
        run_notifier(
            notification_rx,
            MAX_CONCURRENT_FLOW_NOTIFICATIONS,
            |notification| {
                let table_flownode_set_cache = table_flownode_set_cache.clone();
                let node_manager = node_manager.clone();
                handle_flow_notification(notification, table_flownode_set_cache, node_manager)
            },
        )
        .await;
    });
}

#[cfg(test)]
fn notify_flow_dirty_windows_after_flush(
    table_batches: Vec<TableBatch>,
    table_flownode_set_cache: TableFlownodeSetCacheRef,
    node_manager: NodeManagerRef,
) {
    let (tx, rx) = Notifier::try_new(table_batches.len().max(1)).unwrap();
    start_flow_notification_worker(rx, table_flownode_set_cache, node_manager);
    enqueue_flow_notifications(table_batches, &tx);
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use api::v1::meta::Peer;
    use api::v1::value::ValueData;
    use api::v1::{
        ColumnDataType, ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType,
        Value,
    };
    use arrow::array::{StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use common_batcher::notifier::Notifier;
    use common_meta::cache::new_table_flownode_set_cache;
    use common_meta::error::Result as MetaResult;
    use common_meta::instruction::{CacheIdent, CreateFlow};
    use common_meta::kv_backend::{KvBackend, TxnService};
    use common_meta::node_manager::NodeManagerRef;
    use common_meta::rpc::store::{
        BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse,
        BatchPutRequest, BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
        PutResponse, RangeRequest, RangeResponse,
    };
    use common_query::prelude::greptime_timestamp;
    use moka::future::CacheBuilder;
    use tokio::sync::{Notify, mpsc, oneshot};

    use crate::batcher::logical_table::batch_convert::{RecordBatchWithTsIdx, TableBatch};
    use crate::batcher::logical_table::test_util::{
        FlowNotificationMockNodeManager, RecordingFlownode, mock_aligned_tag_batch,
        mock_table_flownode_cache, mock_tag_batch,
    };
    use crate::batcher::logical_table::{
        LogicalTablePendingRowsBatcher, extract_timestamps, notify_flow_dirty_windows_after_flush,
        try_enqueue_flow_notification,
    };
    use crate::metrics::FLOW_NOTIFICATION_DROPPED;
    use crate::prom_row_builder::rows_to_aligned_record_batch;

    fn mock_rows(row_count: usize, schema_name: &str) -> Rows {
        Rows {
            schema: vec![ColumnSchema {
                column_name: schema_name.to_string(),
                ..Default::default()
            }],
            rows: (0..row_count).map(|_| Row { values: vec![] }).collect(),
        }
    }

    fn mock_timestamp_batch(timestamps: Vec<Option<i64>>) -> RecordBatchWithTsIdx {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                greptime_timestamp(),
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            )])),
            vec![Arc::new(TimestampMillisecondArray::from(timestamps))],
        )
        .unwrap();
        RecordBatchWithTsIdx::try_new(batch, 0).unwrap()
    }

    #[test]
    fn test_extract_timestamps_appends_non_null_batches_in_order() {
        let table_batch = TableBatch {
            table_name: "cpu".to_string(),
            table_id: 42,
            batches: vec![
                mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0),
                mock_aligned_tag_batch("tag1", "host-1", 2000, 2.0),
            ],
            row_count: 2,
        };

        assert_eq!(vec![1000, 2000], extract_timestamps(&table_batch));
    }

    #[test]
    fn test_extract_timestamps_omits_nulls_and_retains_order() {
        let table_batch = TableBatch {
            table_name: "cpu".to_string(),
            table_id: 42,
            batches: vec![
                mock_timestamp_batch(vec![Some(1000), None, Some(3000)]),
                mock_timestamp_batch(vec![None, Some(5000)]),
            ],
            row_count: 5,
        };

        assert_eq!(vec![1000, 3000, 5000], extract_timestamps(&table_batch));
    }

    #[test]
    fn test_record_batch_with_ts_idx_rejects_out_of_bounds_index() {
        let batch = mock_tag_batch("tag1", "host-1", 1000, 1.0);

        assert!(RecordBatchWithTsIdx::try_new(batch, 3).is_err());
    }

    #[test]
    fn test_record_batch_with_ts_idx_rejects_non_timestamp_column() {
        let batch = mock_tag_batch("tag1", "host-1", 1000, 1.0);

        assert!(RecordBatchWithTsIdx::try_new(batch, 1).is_err());
    }

    #[test]
    fn test_extract_timestamps_supports_per_batch_timestamp_indices() {
        let timestamp_first = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new(
                    "ts",
                    ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("host", ArrowDataType::Utf8, true),
            ])),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1000, 2000])),
                Arc::new(StringArray::from(vec!["host-1", "host-2"])),
            ],
        )
        .unwrap();
        let timestamp_second = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("host", ArrowDataType::Utf8, true),
                Field::new(
                    "timestamp",
                    ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["host-3", "host-4"])),
                Arc::new(TimestampMillisecondArray::from(vec![3000, 4000])),
            ],
        )
        .unwrap();
        let table_batch = TableBatch {
            table_name: "cpu".to_string(),
            table_id: 42,
            batches: vec![
                RecordBatchWithTsIdx::try_new(timestamp_first, 0).unwrap(),
                RecordBatchWithTsIdx::try_new(timestamp_second, 1).unwrap(),
            ],
            row_count: 4,
        };

        assert_eq!(
            vec![1000, 2000, 3000, 4000],
            extract_timestamps(&table_batch)
        );
    }

    #[test]
    fn test_extract_timestamps_uses_aligned_custom_timestamp_index() {
        let rows = Rows {
            schema: vec![
                ColumnSchema {
                    column_name: greptime_timestamp().to_string(),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    semantic_type: SemanticType::Timestamp as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "host".to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "greptime_value".to_string(),
                    datatype: ColumnDataType::Float64 as i32,
                    semantic_type: SemanticType::Field as i32,
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
                            value_data: Some(ValueData::StringValue("host-1".to_string())),
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(1.0)),
                        },
                    ],
                },
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(2000)),
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("host-2".to_string())),
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(2.0)),
                        },
                    ],
                },
            ],
        };
        let target_schema = ArrowSchema::new(vec![
            Field::new("host", ArrowDataType::Utf8, true),
            Field::new(
                "timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
        ]);
        let batch = rows_to_aligned_record_batch(&rows, &target_schema).unwrap();
        assert_eq!(1, batch.timestamp_index);
        let table_batch = TableBatch {
            table_name: "cpu".to_string(),
            table_id: 42,
            row_count: batch.batch.num_rows(),
            batches: vec![batch],
        };

        assert_eq!(vec![1000, 2000], extract_timestamps(&table_batch));
    }

    #[test]
    fn test_flow_notification_queue_drops_when_full() {
        let (tx, mut rx) = Notifier::try_new(1).unwrap();
        let notification = |table_id| crate::batcher::logical_table::FlowNotification {
            table_id,
            timestamps: vec![table_id as i64],
        };
        let dropped = FLOW_NOTIFICATION_DROPPED.with_label_values(&["full"]);
        let dropped_before = dropped.get();

        assert!(try_enqueue_flow_notification(&tx, notification(1)));
        assert!(!try_enqueue_flow_notification(&tx, notification(2)));

        assert_eq!(1, rx.try_recv().unwrap().table_id);
        assert_eq!(dropped_before + 1, dropped.get());

        let closed = FLOW_NOTIFICATION_DROPPED.with_label_values(&["closed"]);
        let closed_before = closed.get();
        drop(rx);
        assert!(!try_enqueue_flow_notification(&tx, notification(3)));
        assert_eq!(closed_before + 1, closed.get());
    }

    #[test]
    fn test_collect_non_empty_table_rows_filters_empty_payloads() {
        let requests = RowInsertRequests {
            inserts: vec![
                RowInsertRequest {
                    table_name: "cpu".to_string(),
                    rows: Some(mock_rows(2, "host")),
                },
                RowInsertRequest {
                    table_name: "mem".to_string(),
                    rows: Some(mock_rows(0, "host")),
                },
                RowInsertRequest {
                    table_name: "disk".to_string(),
                    rows: None,
                },
            ],
        };

        let (table_rows, total_rows) =
            LogicalTablePendingRowsBatcher::collect_non_empty_table_rows(requests);

        assert_eq!(2, total_rows);
        assert_eq!(1, table_rows.len());
        assert_eq!("cpu", table_rows[0].0);
        assert_eq!(2, table_rows[0].1.rows.len());
    }

    struct BlockingRangeKvBackend {
        range_started: Mutex<Option<oneshot::Sender<()>>>,
        range_release: Arc<Notify>,
    }

    impl TxnService for BlockingRangeKvBackend {
        type Error = common_meta::error::Error;
    }

    #[async_trait]
    impl KvBackend for BlockingRangeKvBackend {
        fn name(&self) -> &str {
            "blocking_range"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn range(&self, _req: RangeRequest) -> MetaResult<RangeResponse> {
            let range_started = self.range_started.lock().unwrap().take();
            if let Some(range_started) = range_started {
                let _ = range_started.send(());
                self.range_release.notified().await;
            }
            Ok(RangeResponse {
                kvs: Vec::new(),
                more: false,
            })
        }

        async fn put(&self, _req: PutRequest) -> MetaResult<PutResponse> {
            unimplemented!()
        }

        async fn batch_put(&self, _req: BatchPutRequest) -> MetaResult<BatchPutResponse> {
            unimplemented!()
        }

        async fn batch_get(&self, _req: BatchGetRequest) -> MetaResult<BatchGetResponse> {
            unimplemented!()
        }

        async fn delete_range(&self, _req: DeleteRangeRequest) -> MetaResult<DeleteRangeResponse> {
            unimplemented!()
        }

        async fn batch_delete(&self, _req: BatchDeleteRequest) -> MetaResult<BatchDeleteResponse> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_flow_notifications_do_not_block_on_previous_table_cache_lookup() {
        let blocked_table_id = 41;
        let cached_table_id = 42;
        let peer = Peer {
            id: 7,
            addr: "flow-7".to_string(),
        };
        let (range_started_tx, range_started_rx) = oneshot::channel();
        let range_release = Arc::new(Notify::new());
        let cache = Arc::new(new_table_flownode_set_cache(
            "test".to_string(),
            CacheBuilder::new(2).build(),
            Arc::new(BlockingRangeKvBackend {
                range_started: Mutex::new(Some(range_started_tx)),
                range_release: range_release.clone(),
            }),
        ));
        cache
            .invalidate(&[CacheIdent::CreateFlow(CreateFlow {
                flow_id: 1,
                source_table_ids: vec![cached_table_id],
                partition_to_peer_mapping: vec![(0, peer)],
            })])
            .await
            .unwrap();
        let (requests_tx, mut requests_rx) = mpsc::unbounded_channel();
        let _requests_tx = requests_tx.clone();
        let node_manager: NodeManagerRef = Arc::new(FlowNotificationMockNodeManager {
            flownode: Arc::new(RecordingFlownode { requests_tx }),
        });
        let table_batches = vec![
            TableBatch {
                table_name: "blocked".to_string(),
                table_id: blocked_table_id,
                batches: vec![mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0)],
                row_count: 1,
            },
            TableBatch {
                table_name: "cached".to_string(),
                table_id: cached_table_id,
                batches: vec![mock_aligned_tag_batch("tag1", "host-2", 2000, 2.0)],
                row_count: 1,
            },
        ];

        notify_flow_dirty_windows_after_flush(table_batches, cache, node_manager);

        tokio::time::timeout(Duration::from_secs(1), range_started_rx)
            .await
            .unwrap()
            .unwrap();
        let requests = tokio::time::timeout(Duration::from_secs(1), requests_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cached_table_id, requests.requests[0].table_id);
        range_release.notify_one();
    }

    #[tokio::test]
    async fn test_successful_flush_notifies_flownode_with_logical_table_timestamps() {
        let table_id = 42;
        let peer = Peer {
            id: 7,
            addr: "flow-7".to_string(),
        };
        let cache = mock_table_flownode_cache(table_id, peer).await;
        let (requests_tx, mut requests_rx) = mpsc::unbounded_channel();
        let _requests_tx = requests_tx.clone();
        let node_manager: NodeManagerRef = Arc::new(FlowNotificationMockNodeManager {
            flownode: Arc::new(RecordingFlownode { requests_tx }),
        });
        let table_batches = vec![TableBatch {
            table_name: "cpu".to_string(),
            table_id,
            batches: vec![mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0)],
            row_count: 1,
        }];

        notify_flow_dirty_windows_after_flush(table_batches, cache, node_manager);

        let requests = tokio::time::timeout(Duration::from_secs(1), requests_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            vec![api::v1::flow::DirtyWindowRequest {
                table_id,
                timestamps: vec![1000],
                time_ranges: Vec::new(),
            }],
            requests.requests
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), requests_rx.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_successful_flush_coalesces_logical_batches_per_flownode() {
        let table_id = 42;
        let peer = Peer {
            id: 7,
            addr: "flow-7".to_string(),
        };
        let cache = mock_table_flownode_cache(table_id, peer).await;
        let (requests_tx, mut requests_rx) = mpsc::unbounded_channel();
        let _requests_tx = requests_tx.clone();
        let node_manager: NodeManagerRef = Arc::new(FlowNotificationMockNodeManager {
            flownode: Arc::new(RecordingFlownode { requests_tx }),
        });
        let table_batches = vec![TableBatch {
            table_name: "cpu".to_string(),
            table_id,
            batches: vec![
                mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0),
                mock_aligned_tag_batch("tag1", "host-1", 2000, 2.0),
            ],
            row_count: 2,
        }];

        notify_flow_dirty_windows_after_flush(table_batches, cache, node_manager);

        let requests = tokio::time::timeout(Duration::from_secs(1), requests_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            vec![api::v1::flow::DirtyWindowRequest {
                table_id,
                timestamps: vec![1000, 2000],
                time_ranges: Vec::new(),
            }],
            requests.requests
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), requests_rx.recv())
                .await
                .is_err()
        );
    }
}
