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
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use catalog::CatalogManagerRef;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_meta::node_manager::NodeManagerRef;
use common_query::prelude::{GREPTIME_PHYSICAL_TABLE, greptime_timestamp, greptime_value};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{debug, warn};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use metric_engine::batch_modifier::{TagColumnInfo, modify_batch_sparse};
use partition::manager::PartitionRuleManagerRef;
use partition::partition::PartitionRuleRef;
use session::context::QueryContextRef;
use smallvec::SmallVec;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::{RegionId, TableId};
use table::metadata::{TableInfo, TableInfoRef};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot};

use crate::error;
use crate::error::{Error, Result};
use crate::metrics::{
    FLUSH_DROPPED_ROWS, FLUSH_ELAPSED, FLUSH_FAILURES, FLUSH_ROWS, FLUSH_TOTAL, PENDING_BATCHES,
    PENDING_ROWS, PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED, PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED,
    PENDING_WORKERS,
};
use crate::prom_row_builder::{
    build_prom_create_table_schema_from_proto, identify_missing_columns_from_proto,
    rows_to_aligned_record_batch,
};

const PHYSICAL_TABLE_KEY: &str = "physical_table";
/// Whether wait for ingestion result before reply to client.
const PENDING_ROWS_BATCH_SYNC_ENV: &str = "PENDING_ROWS_BATCH_SYNC";
const WORKER_IDLE_TIMEOUT_MULTIPLIER: u32 = 3;
const PHYSICAL_REGION_ESSENTIAL_COLUMN_COUNT: usize = 3;

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

#[derive(Clone)]
pub struct PhysicalTableMetadata {
    pub table_info: TableInfoRef,
    pub name_to_ids: Option<HashMap<String, u32>>,
}

#[async_trait]
pub trait PhysicalFlushCatalogProvider: Send + Sync {
    async fn physical_table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        query_ctx: &session::context::QueryContext,
    ) -> catalog::error::Result<Option<PhysicalTableMetadata>>;
}

#[async_trait]
pub trait PhysicalFlushPartitionProvider: Send + Sync {
    async fn find_table_partition_rule(
        &self,
        table_info: &TableInfo,
    ) -> partition::error::Result<PartitionRuleRef>;

    async fn find_region_leader(&self, region_id: RegionId) -> Result<Peer>;
}

#[async_trait]
pub trait PhysicalFlushNodeRequester: Send + Sync {
    async fn handle(
        &self,
        peer: &Peer,
        request: RegionRequest,
    ) -> Result<api::region::RegionResponse>;
}

#[derive(Clone)]
struct CatalogManagerPhysicalFlushAdapter {
    catalog_manager: CatalogManagerRef,
}

#[async_trait]
impl PhysicalFlushCatalogProvider for CatalogManagerPhysicalFlushAdapter {
    async fn physical_table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        query_ctx: &session::context::QueryContext,
    ) -> catalog::error::Result<Option<PhysicalTableMetadata>> {
        self.catalog_manager
            .table(catalog, schema, table_name, Some(query_ctx))
            .await
            .map(|table| {
                table.map(|table| {
                    let table_info = table.table_info();
                    let name_to_ids = table_info.name_to_ids();
                    PhysicalTableMetadata {
                        table_info,
                        name_to_ids,
                    }
                })
            })
    }
}

#[derive(Clone)]
struct PartitionManagerPhysicalFlushAdapter {
    partition_manager: PartitionRuleManagerRef,
}

#[async_trait]
impl PhysicalFlushPartitionProvider for PartitionManagerPhysicalFlushAdapter {
    async fn find_table_partition_rule(
        &self,
        table_info: &TableInfo,
    ) -> partition::error::Result<PartitionRuleRef> {
        self.partition_manager
            .find_table_partition_rule(table_info)
            .await
            .map(|(rule, _)| rule)
    }

    async fn find_region_leader(&self, region_id: RegionId) -> Result<Peer> {
        let peer = self.partition_manager.find_region_leader(region_id).await?;
        Ok(peer)
    }
}

#[derive(Clone)]
struct NodeManagerPhysicalFlushAdapter {
    node_manager: NodeManagerRef,
}

#[async_trait]
impl PhysicalFlushNodeRequester for NodeManagerPhysicalFlushAdapter {
    async fn handle(
        &self,
        peer: &Peer,
        request: RegionRequest,
    ) -> error::Result<api::region::RegionResponse> {
        let datanode = self.node_manager.datanode(peer).await;
        datanode
            .handle(request)
            .await
            .context(error::CommonMetaSnafu)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct BatchKey {
    catalog: String,
    schema: String,
    physical_table: String,
}

#[derive(Debug)]
pub struct TableBatch {
    pub table_name: String,
    pub table_id: TableId,
    pub batches: Vec<RecordBatch>,
    pub row_count: usize,
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

struct PendingBatch {
    tables: HashMap<String, TableBatch>,
    created_at: Instant,
    total_row_count: usize,
    ctx: QueryContextRef,
    waiters: Vec<FlushWaiter>,
}

struct FlushWaiter {
    response_tx: oneshot::Sender<std::result::Result<(), Arc<Error>>>,
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
        response_tx: oneshot::Sender<std::result::Result<(), Arc<Error>>>,
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

    /// Converts proto rows to `RecordBatch` values aligned to resolved region
    /// schemas and returns `(table_name, table_id, batch)` tuples.
    fn build_aligned_batches(
        table_rows: &[(String, Rows)],
        region_schemas: &HashMap<String, (Arc<ArrowSchema>, u32)>,
    ) -> Result<Vec<(String, u32, RecordBatch)>> {
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
    fn new(ctx: QueryContextRef) -> Self {
        Self {
            tables: HashMap::new(),
            created_at: Instant::now(),
            total_row_count: 0,
            ctx,
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
        let mut batch = None;
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

                            let pending_batch =batch.get_or_insert_with(||{
                                PENDING_BATCHES.inc();
                                PendingBatch::new(ctx)
                            });

                            pending_batch.waiters.push(FlushWaiter { response_tx, _permit });

                            for (table_name, table_id, record_batch) in table_batches {
                                let entry = pending_batch.tables.entry(table_name.clone()).or_insert_with(|| TableBatch {
                                    table_name,
                                    table_id,
                                    batches: Vec::new(),
                                    row_count: 0,
                                });
                                entry.row_count += record_batch.num_rows();
                                entry.batches.push(record_batch);
                            }

                            pending_batch.total_row_count += total_rows;
                            PENDING_ROWS.add(total_rows as i64);

                            if pending_batch.total_row_count >= max_batch_rows
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
                    if !should_close_worker_on_idle_timeout(
                        batch.as_ref().map_or(0, |batch| batch.total_row_count),
                        rx.len(),
                    ) {
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
                    if batch
                        .as_ref()
                        .is_some_and(|batch| batch.created_at.elapsed() >= flush_interval)
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

fn drain_batch(batch: &mut Option<PendingBatch>) -> Option<FlushBatch> {
    let batch = batch.take()?;
    let total_row_count = batch.total_row_count;

    if total_row_count == 0 {
        return None;
    }

    let table_batches = batch.tables.into_values().collect();
    let waiters = batch.waiters;

    PENDING_ROWS.sub(total_row_count as i64);
    PENDING_BATCHES.dec();

    Some(FlushBatch {
        table_batches,
        total_row_count,
        ctx: batch.ctx,
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
    row_count: usize,
    datanode: Peer,
    request: RegionRequest,
}

struct PlannedRegionBatch {
    region_id: RegionId,
    row_count: usize,
    batch: RecordBatch,
}

struct ResolvedRegionBatch {
    planned: PlannedRegionBatch,
    datanode: Peer,
}

fn should_dispatch_concurrently(region_write_count: usize) -> bool {
    region_write_count > 1
}

/// Classifies columns in a logical-table batch for sparse primary-key conversion.
///
/// Returns:
/// - `Vec<TagColumnInfo>`: all Utf8 tag columns sorted by tag name, used for
///   TSID and sparse primary-key encoding.
/// - `SmallVec<[usize; 3]>`: indices of columns copied into the physical batch
///   after `__primary_key`, ordered as `[greptime_timestamp, greptime_value,
///   partition_tag_columns...]`.
fn columns_taxonomy(
    batch_schema: &Arc<ArrowSchema>,
    table_name: &str,
    name_to_ids: &HashMap<String, u32>,
    partition_columns: &HashSet<&str>,
) -> Result<(Vec<TagColumnInfo>, SmallVec<[usize; 3]>)> {
    let mut tag_columns = Vec::new();
    let mut essential_column_indices =
        SmallVec::<[usize; 3]>::with_capacity(2 + partition_columns.len());
    // Placeholder for greptime_timestamp and greptime_value
    essential_column_indices.push(0);
    essential_column_indices.push(0);

    let mut timestamp_index = None;
    let mut value_index = None;

    for (index, field) in batch_schema.fields().iter().enumerate() {
        match field.data_type() {
            ArrowDataType::Utf8 => {
                let column_id = name_to_ids.get(field.name()).copied().with_context(|| {
                    error::InvalidPromRemoteRequestSnafu {
                        msg: format!(
                            "Column '{}' from logical table '{}' not found in physical table column IDs",
                            field.name(),
                            table_name
                        ),
                    }
                })?;
                tag_columns.push(TagColumnInfo {
                    name: field.name().clone(),
                    index,
                    column_id,
                });

                if partition_columns.contains(field.name().as_str()) {
                    essential_column_indices.push(index);
                }
            }
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
                ensure!(
                    timestamp_index.replace(index).is_none(),
                    error::InvalidPromRemoteRequestSnafu {
                        msg: format!(
                            "Duplicated timestamp column in logical table '{}' batch schema",
                            table_name
                        ),
                    }
                );
            }
            ArrowDataType::Float64 => {
                ensure!(
                    value_index.replace(index).is_none(),
                    error::InvalidPromRemoteRequestSnafu {
                        msg: format!(
                            "Duplicated value column in logical table '{}' batch schema",
                            table_name
                        ),
                    }
                );
            }
            datatype => {
                return error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "Unexpected data type '{datatype:?}' in logical table '{}' batch schema",
                        table_name
                    ),
                }
                .fail();
            }
        }
    }

    let timestamp_index =
        timestamp_index.with_context(|| error::InvalidPromRemoteRequestSnafu {
            msg: format!(
                "Missing essential column '{}' in logical table '{}' batch schema",
                greptime_timestamp(),
                table_name
            ),
        })?;
    let value_index = value_index.with_context(|| error::InvalidPromRemoteRequestSnafu {
        msg: format!(
            "Missing essential column '{}' in logical table '{}' batch schema",
            greptime_value(),
            table_name
        ),
    })?;

    tag_columns.sort_by(|a, b| a.name.cmp(&b.name));

    essential_column_indices[0] = timestamp_index;
    essential_column_indices[1] = value_index;

    Ok((tag_columns, essential_column_indices))
}

fn strip_partition_columns_from_batch(batch: RecordBatch) -> Result<RecordBatch> {
    ensure!(
        batch.num_columns() >= PHYSICAL_REGION_ESSENTIAL_COLUMN_COUNT,
        error::InternalSnafu {
            err_msg: format!(
                "Expected at least {} columns in physical batch, got {}",
                PHYSICAL_REGION_ESSENTIAL_COLUMN_COUNT,
                batch.num_columns()
            ),
        }
    );
    let essential_indices: Vec<usize> = (0..PHYSICAL_REGION_ESSENTIAL_COLUMN_COUNT).collect();
    batch.project(&essential_indices).context(error::ArrowSnafu)
}

async fn flush_region_writes_concurrently(
    node_manager: &(impl PhysicalFlushNodeRequester + ?Sized),
    writes: Vec<FlushRegionWrite>,
) -> Result<()> {
    if !should_dispatch_concurrently(writes.len()) {
        for write in writes {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_write_region"])
                .start_timer();
            node_manager.handle(&write.datanode, write.request).await?;
            FLUSH_TOTAL.inc();
            FLUSH_ROWS.observe(write.row_count as f64);
        }
        return Ok(());
    }

    let write_futures = writes.into_iter().map(|write| async move {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_write_region"])
            .start_timer();

        node_manager.handle(&write.datanode, write.request).await?;
        FLUSH_TOTAL.inc();
        FLUSH_ROWS.observe(write.row_count as f64);
        Ok::<_, Error>(())
    });

    // todo(hl): should be bounded.
    futures::future::try_join_all(write_futures).await?;
    Ok(())
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

    // Physical-table-level flush: transform all logical table batches
    // into physical format and write them together.
    let physical_table_name = ctx
        .extension(PHYSICAL_TABLE_KEY)
        .unwrap_or(GREPTIME_PHYSICAL_TABLE)
        .to_string();
    let partition_provider = PartitionManagerPhysicalFlushAdapter { partition_manager };
    let node_requester = NodeManagerPhysicalFlushAdapter { node_manager };
    let catalog_provider = CatalogManagerPhysicalFlushAdapter { catalog_manager };
    let result = flush_batch_physical(
        &table_batches,
        &physical_table_name,
        &ctx,
        &partition_provider,
        &node_requester,
        &catalog_provider,
    )
    .await;

    let elapsed = start.elapsed().as_secs_f64();
    FLUSH_ELAPSED.observe(elapsed);

    if result.is_err() {
        FLUSH_FAILURES.inc();
        FLUSH_DROPPED_ROWS.inc_by(total_row_count as u64);
    }

    debug!(
        "Pending rows batch flushed, total rows: {}, elapsed time: {}s",
        total_row_count, elapsed
    );

    notify_waiters(waiters, result);
}

/// Flushes a batch of logical table rows by transforming them into the physical table format
/// and writing them to the appropriate datanode regions.
///
/// This function performs the end-to-end physical flush pipeline:
/// 1. Resolves the physical table metadata and column ID mapping.
/// 2. Fetches the physical table's partition rule.
/// 3. Transforms each logical table batch into the physical (sparse primary key) format.
/// 4. Concatenates all transformed batches into a single combined batch.
/// 5. Splits the combined batch by partition rule and sends region write requests
///    concurrently to the target datanodes.
pub async fn flush_batch_physical(
    table_batches: &[TableBatch],
    physical_table_name: &str,
    ctx: &QueryContextRef,
    partition_manager: &(impl PhysicalFlushPartitionProvider + ?Sized),
    node_manager: &(impl PhysicalFlushNodeRequester + ?Sized),
    catalog_manager: &(impl PhysicalFlushCatalogProvider + ?Sized),
) -> Result<()> {
    // 1. Resolve the physical table and get column ID mapping
    let physical_table = {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_resolve_table"])
            .start_timer();
        catalog_manager
            .physical_table(
                ctx.current_catalog(),
                &ctx.current_schema(),
                physical_table_name,
                ctx.as_ref(),
            )
            .await?
            .context(error::InternalSnafu {
                err_msg: format!(
                    "Physical table '{}' not found during pending flush",
                    physical_table_name
                ),
            })?
    };

    let physical_table_info = physical_table.table_info;
    let name_to_ids = physical_table.name_to_ids.context(error::InternalSnafu {
        err_msg: format!(
            "Physical table '{}' has no column IDs for pending flush",
            physical_table_name
        ),
    })?;

    // 2. Get the physical table's partition rule (one lookup instead of N)
    let partition_rule = {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_fetch_partition_rule"])
            .start_timer();
        partition_manager
            .find_table_partition_rule(physical_table_info.as_ref())
            .await?
    };
    let partition_columns = partition_rule.partition_columns();
    let partition_columns_set: HashSet<&str> =
        partition_columns.iter().map(String::as_str).collect();

    // 3. Transform each logical table batch into physical format
    let modified_batches =
        transform_logical_batches_to_physical(table_batches, &name_to_ids, &partition_columns_set)?;

    // 4. Concatenate all modified batches (all share the same physical schema)
    let combined_batch = concat_modified_batches(&modified_batches)?;

    // 5. Split by physical partition rule and send to regions
    let physical_table_id = physical_table_info.table_id();
    let planned_batches = plan_region_batches(
        combined_batch,
        physical_table_id,
        partition_rule.as_ref(),
        partition_columns,
    )?;

    let resolved_batches = resolve_region_targets(planned_batches, partition_manager).await?;
    let region_writes = encode_region_write_requests(resolved_batches)?;
    flush_region_writes_concurrently(node_manager, region_writes).await
}

/// Transforms logical table batches into physical format (sparse primary key encoding).
///
/// It identifies tag columns and essential columns (timestamp, value) for each logical batch
/// and applies sparse primary key modification.
fn transform_logical_batches_to_physical(
    table_batches: &[TableBatch],
    name_to_ids: &HashMap<String, u32>,
    partition_columns_set: &HashSet<&str>,
) -> Result<Vec<RecordBatch>> {
    let mut modified_batches: Vec<RecordBatch> =
        Vec::with_capacity(table_batches.iter().map(|b| b.batches.len()).sum());

    let mut modify_elapsed = Duration::ZERO;
    let mut columns_taxonomy_elapsed = Duration::ZERO;

    for table_batch in table_batches {
        let table_id = table_batch.table_id;

        for batch in &table_batch.batches {
            let batch_schema = batch.schema();
            let start = Instant::now();
            let (tag_columns, essential_col_indices) = columns_taxonomy(
                &batch_schema,
                &table_batch.table_name,
                name_to_ids,
                partition_columns_set,
            )?;

            columns_taxonomy_elapsed += start.elapsed();
            if tag_columns.is_empty() && essential_col_indices.is_empty() {
                continue;
            }

            let modified = {
                let start = Instant::now();
                let batch = modify_batch_sparse(
                    batch.clone(),
                    table_id,
                    &tag_columns,
                    &essential_col_indices,
                )?;
                modify_elapsed += start.elapsed();
                batch
            };

            modified_batches.push(modified);
        }
    }

    PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
        .with_label_values(&["flush_physical_modify_batch"])
        .observe(modify_elapsed.as_secs_f64());
    PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
        .with_label_values(&["flush_physical_columns_taxonomy"])
        .observe(columns_taxonomy_elapsed.as_secs_f64());

    ensure!(
        !modified_batches.is_empty(),
        error::InternalSnafu {
            err_msg: "No batches can be transformed during pending flush",
        }
    );
    Ok(modified_batches)
}

/// Concatenates all modified batches into a single large batch.
///
/// All modified batches share the same physical schema.
fn concat_modified_batches(modified_batches: &[RecordBatch]) -> Result<RecordBatch> {
    let combined_schema = modified_batches[0].schema();
    let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
        .with_label_values(&["flush_physical_concat_all"])
        .start_timer();
    concat_batches(&combined_schema, modified_batches).context(error::ArrowSnafu)
}

fn split_combined_batch_by_region(
    combined_batch: &RecordBatch,
    partition_rule: &dyn partition::partition::PartitionRule,
) -> Result<HashMap<u32, partition::partition::RegionMask>> {
    let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
        .with_label_values(&["flush_physical_split_record_batch"])
        .start_timer();
    let map = partition_rule.split_record_batch(combined_batch)?;
    Ok(map)
}

fn prepare_physical_region_routing_batch(
    combined_batch: RecordBatch,
    partition_columns: &[String],
) -> Result<RecordBatch> {
    if partition_columns.is_empty() {
        return Ok(combined_batch);
    }
    strip_partition_columns_from_batch(combined_batch)
}

fn plan_region_batch(
    stripped_batch: &RecordBatch,
    physical_table_id: TableId,
    region_number: u32,
    mask: &partition::partition::RegionMask,
) -> Result<Option<PlannedRegionBatch>> {
    if mask.select_none() {
        return Ok(None);
    }

    let region_batch = if mask.select_all() {
        stripped_batch.clone()
    } else {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_filter_record_batch"])
            .start_timer();
        filter_record_batch(stripped_batch, mask.array()).context(error::ArrowSnafu)?
    };

    let row_count = region_batch.num_rows();
    if row_count == 0 {
        return Ok(None);
    }

    Ok(Some(PlannedRegionBatch {
        region_id: RegionId::new(physical_table_id, region_number),
        row_count,
        batch: region_batch,
    }))
}

#[allow(clippy::too_many_arguments)]
fn plan_region_batches(
    combined_batch: RecordBatch,
    physical_table_id: TableId,
    partition_rule: &dyn partition::partition::PartitionRule,
    partition_columns: &[String],
) -> Result<Vec<PlannedRegionBatch>> {
    let region_masks = split_combined_batch_by_region(&combined_batch, partition_rule)?;
    let stripped_batch = prepare_physical_region_routing_batch(combined_batch, partition_columns)?;

    let mut planned_batches = Vec::new();
    for (region_number, mask) in region_masks {
        if let Some(planned_batch) =
            plan_region_batch(&stripped_batch, physical_table_id, region_number, &mask)?
        {
            planned_batches.push(planned_batch);
        }
    }

    Ok(planned_batches)
}

async fn resolve_region_targets(
    planned_batches: Vec<PlannedRegionBatch>,
    partition_manager: &(impl PhysicalFlushPartitionProvider + ?Sized),
) -> Result<Vec<ResolvedRegionBatch>> {
    let mut resolved_batches = Vec::with_capacity(planned_batches.len());
    for planned in planned_batches {
        let datanode = {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_physical_resolve_region_leader"])
                .start_timer();
            partition_manager
                .find_region_leader(planned.region_id)
                .await?
        };

        resolved_batches.push(ResolvedRegionBatch { planned, datanode });
    }

    Ok(resolved_batches)
}

fn encode_region_write_requests(
    resolved_batches: Vec<ResolvedRegionBatch>,
) -> Result<Vec<FlushRegionWrite>> {
    let mut region_writes = Vec::with_capacity(resolved_batches.len());
    for resolved in resolved_batches {
        let region_id = resolved.planned.region_id;
        let row_count = resolved.planned.row_count;
        let (schema_bytes, data_header, payload) = {
            let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
                .with_label_values(&["flush_physical_encode_ipc"])
                .start_timer();
            record_batch_to_ipc(resolved.planned.batch)?
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
            row_count,
            datanode: resolved.datanode,
            request,
        });
    }

    Ok(region_writes)
}

fn notify_waiters(waiters: Vec<FlushWaiter>, result: Result<()>) {
    let shared_result = result.map_err(Arc::new);
    for waiter in waiters {
        let _ = waiter.response_tx.send(match &shared_result {
            Ok(()) => Ok(()),
            Err(error) => Err(Arc::clone(error)),
        });
        // waiter._permit is dropped here, releasing the inflight semaphore slot
    }
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
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use api::region::RegionResponse;
    use api::v1::flow::{DirtyWindowRequests, FlowRequest, FlowResponse};
    use api::v1::meta::Peer;
    use api::v1::region::{InsertRequests, RegionRequest, region_request};
    use api::v1::{ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows};
    use arrow::array::{BinaryArray, BooleanArray, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use catalog::error::Result as CatalogResult;
    use common_meta::error::Result as MetaResult;
    use common_meta::node_manager::{
        Datanode, DatanodeManager, DatanodeRef, Flownode, FlownodeManager, FlownodeRef,
    };
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use dashmap::DashMap;
    use datatypes::schema::{ColumnSchema as DtColumnSchema, Schema as DtSchema};
    use partition::error::Result as PartitionResult;
    use partition::partition::{PartitionRule, PartitionRuleRef, RegionMask};
    use smallvec::SmallVec;
    use snafu::ResultExt;
    use store_api::storage::RegionId;
    use table::metadata::TableId;
    use table::test_util::table_info::test_table_info;
    use tokio::sync::{Semaphore, mpsc, oneshot};
    use tokio::time::sleep;

    use super::{
        BatchKey, Error, FlushRegionWrite, FlushWaiter, PendingBatch, PendingRowsBatcher,
        PendingWorker, PhysicalFlushCatalogProvider, PhysicalFlushNodeRequester,
        PhysicalFlushPartitionProvider, PhysicalTableMetadata, PlannedRegionBatch,
        ResolvedRegionBatch, TableBatch, WorkerCommand, columns_taxonomy, drain_batch,
        encode_region_write_requests, flush_batch_physical, flush_region_writes_concurrently,
        plan_region_batches, remove_worker_if_same_channel, should_close_worker_on_idle_timeout,
        should_dispatch_concurrently, strip_partition_columns_from_batch,
        transform_logical_batches_to_physical,
    };
    use crate::error;

    fn mock_rows(row_count: usize, schema_name: &str) -> Rows {
        Rows {
            schema: vec![ColumnSchema {
                column_name: schema_name.to_string(),
                ..Default::default()
            }],
            rows: (0..row_count).map(|_| Row { values: vec![] }).collect(),
        }
    }

    fn mock_tag_batch(tag_name: &str, tag_value: &str, ts: i64, val: f64) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new(tag_name, ArrowDataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![ts])),
                Arc::new(arrow::array::Float64Array::from(vec![val])),
                Arc::new(StringArray::from(vec![tag_value])),
            ],
        )
        .unwrap()
    }

    fn mock_physical_table_metadata(table_id: TableId) -> PhysicalTableMetadata {
        let schema = Arc::new(
            DtSchema::try_new(vec![
                DtColumnSchema::new(
                    "__primary_key",
                    datatypes::prelude::ConcreteDataType::binary_datatype(),
                    false,
                ),
                DtColumnSchema::new(
                    "greptime_timestamp",
                    datatypes::prelude::ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                DtColumnSchema::new(
                    "greptime_value",
                    datatypes::prelude::ConcreteDataType::float64_datatype(),
                    true,
                ),
                DtColumnSchema::new(
                    "tag1",
                    datatypes::prelude::ConcreteDataType::string_datatype(),
                    true,
                ),
            ])
            .unwrap(),
        );
        let mut table_info = test_table_info(table_id, "phy", "public", "greptime", schema);
        table_info.meta.column_ids = vec![0, 1, 2, 3];

        PhysicalTableMetadata {
            table_info: Arc::new(table_info),
            name_to_ids: Some(HashMap::from([("tag1".to_string(), 3)])),
        }
    }

    struct MockFlushCatalogProvider {
        table: Option<PhysicalTableMetadata>,
    }

    #[async_trait]
    impl PhysicalFlushCatalogProvider for MockFlushCatalogProvider {
        async fn physical_table(
            &self,
            _catalog: &str,
            _schema: &str,
            _table_name: &str,
            _query_ctx: &session::context::QueryContext,
        ) -> CatalogResult<Option<PhysicalTableMetadata>> {
            Ok(self.table.clone())
        }
    }

    struct SingleRegionPartitionRule;

    impl PartitionRule for SingleRegionPartitionRule {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn partition_columns(&self) -> &[String] {
            &[]
        }

        fn find_region(
            &self,
            _values: &[datatypes::prelude::Value],
        ) -> partition::error::Result<store_api::storage::RegionNumber> {
            unimplemented!()
        }

        fn split_record_batch(
            &self,
            record_batch: &RecordBatch,
        ) -> partition::error::Result<HashMap<store_api::storage::RegionNumber, RegionMask>>
        {
            Ok(HashMap::from([(
                1,
                RegionMask::new(
                    arrow::array::BooleanArray::from(vec![true; record_batch.num_rows()]),
                    record_batch.num_rows(),
                ),
            )]))
        }
    }

    struct TwoRegionPartitionRule {
        partition_columns: Vec<String>,
    }

    impl PartitionRule for TwoRegionPartitionRule {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn partition_columns(&self) -> &[String] {
            &self.partition_columns
        }

        fn find_region(
            &self,
            _values: &[datatypes::prelude::Value],
        ) -> partition::error::Result<store_api::storage::RegionNumber> {
            unimplemented!()
        }

        fn split_record_batch(
            &self,
            _record_batch: &RecordBatch,
        ) -> partition::error::Result<HashMap<store_api::storage::RegionNumber, RegionMask>>
        {
            Ok(HashMap::from([
                (1, RegionMask::new(BooleanArray::from(vec![true, false]), 1)),
                (2, RegionMask::new(BooleanArray::from(vec![false, true]), 1)),
                (
                    3,
                    RegionMask::new(BooleanArray::from(vec![false, false]), 0),
                ),
            ]))
        }
    }

    struct MockFlushPartitionProvider {
        partition_rule_calls: Arc<AtomicUsize>,
        region_leader_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PhysicalFlushPartitionProvider for MockFlushPartitionProvider {
        async fn find_table_partition_rule(
            &self,
            _table_info: &table::metadata::TableInfo,
        ) -> PartitionResult<PartitionRuleRef> {
            self.partition_rule_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(SingleRegionPartitionRule))
        }

        async fn find_region_leader(&self, _region_id: RegionId) -> error::Result<Peer> {
            self.region_leader_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Peer {
                id: 1,
                addr: "node-1".to_string(),
            })
        }
    }

    #[derive(Default)]
    struct MockFlushNodeRequester {
        writes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PhysicalFlushNodeRequester for MockFlushNodeRequester {
        async fn handle(
            &self,
            _peer: &Peer,
            _request: RegionRequest,
        ) -> error::Result<RegionResponse> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(RegionResponse::new(0))
        }
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

        let (table_rows, total_rows) = PendingRowsBatcher::collect_non_empty_table_rows(requests);

        assert_eq!(2, total_rows);
        assert_eq!(1, table_rows.len());
        assert_eq!("cpu", table_rows[0].0);
        assert_eq!(2, table_rows[0].1.rows.len());
    }

    #[test]
    fn test_drain_batch_takes_initialized_pending_batch_from_option() {
        let ctx = session::context::QueryContext::arc();
        let (response_tx, _response_rx) = oneshot::channel();
        let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        let mut batch = Some(PendingBatch {
            tables: HashMap::from([(
                "cpu".to_string(),
                TableBatch {
                    table_name: "cpu".to_string(),
                    table_id: 42,
                    batches: vec![mock_tag_batch("tag1", "host-1", 1000, 1.0)],
                    row_count: 1,
                },
            )]),
            created_at: Instant::now(),
            total_row_count: 1,
            ctx: ctx.clone(),
            waiters: vec![FlushWaiter {
                response_tx,
                _permit: permit,
            }],
        });

        let flush = drain_batch(&mut batch).unwrap();

        assert!(batch.is_none());
        assert_eq!(1, flush.total_row_count);
        assert_eq!(1, flush.table_batches.len());
        assert_eq!(ctx.current_catalog(), flush.ctx.current_catalog());
    }

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

    #[async_trait]
    impl PhysicalFlushNodeRequester for ConcurrentMockNodeManager {
        async fn handle(
            &self,
            peer: &Peer,
            request: RegionRequest,
        ) -> error::Result<RegionResponse> {
            let datanode = self.datanode(peer).await;
            datanode
                .handle(request)
                .await
                .context(error::CommonMetaSnafu)
        }
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
                row_count: 10,
                datanode: Peer {
                    id: 1,
                    addr: "node1".to_string(),
                },
                request: RegionRequest::default(),
            },
            FlushRegionWrite {
                row_count: 12,
                datanode: Peer {
                    id: 2,
                    addr: "node2".to_string(),
                },
                request: RegionRequest::default(),
            },
        ];

        flush_region_writes_concurrently(node_manager.as_ref(), writes)
            .await
            .unwrap();
        assert!(max_inflight.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn test_should_dispatch_concurrently_by_region_count() {
        assert!(!should_dispatch_concurrently(0));
        assert!(!should_dispatch_concurrently(1));
        assert!(should_dispatch_concurrently(2));
    }

    #[test]
    fn test_strip_partition_columns_from_batch_removes_partition_tags() {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("__primary_key", ArrowDataType::Binary, false),
                Field::new(
                    "greptime_timestamp",
                    ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("greptime_value", ArrowDataType::Float64, true),
                Field::new("host", ArrowDataType::Utf8, true),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![b"k1".as_slice()])),
                Arc::new(TimestampMillisecondArray::from(vec![1000_i64])),
                Arc::new(arrow::array::Float64Array::from(vec![42.0_f64])),
                Arc::new(StringArray::from(vec!["node-1"])),
            ],
        )
        .unwrap();

        let stripped = strip_partition_columns_from_batch(batch).unwrap();

        assert_eq!(3, stripped.num_columns());
        assert_eq!("__primary_key", stripped.schema().field(0).name());
        assert_eq!("greptime_timestamp", stripped.schema().field(1).name());
        assert_eq!("greptime_value", stripped.schema().field(2).name());
    }

    #[test]
    fn test_strip_partition_columns_from_batch_projects_essential_columns_without_lookup() {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("__primary_key", ArrowDataType::Binary, false),
                Field::new(
                    "greptime_timestamp",
                    ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("greptime_value", ArrowDataType::Float64, true),
                Field::new("host", ArrowDataType::Utf8, true),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![b"k1".as_slice()])),
                Arc::new(TimestampMillisecondArray::from(vec![1000_i64])),
                Arc::new(arrow::array::Float64Array::from(vec![42.0_f64])),
                Arc::new(StringArray::from(vec!["node-1"])),
            ],
        )
        .unwrap();

        let stripped = strip_partition_columns_from_batch(batch).unwrap();

        assert_eq!(3, stripped.num_columns());
        assert_eq!("__primary_key", stripped.schema().field(0).name());
        assert_eq!("greptime_timestamp", stripped.schema().field(1).name());
        assert_eq!("greptime_value", stripped.schema().field(2).name());
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_keeps_partition_tag_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("host", ArrowDataType::Utf8, true),
            Field::new("region", ArrowDataType::Utf8, true),
        ]));
        let name_to_ids =
            HashMap::from([("host".to_string(), 1_u32), ("region".to_string(), 2_u32)]);
        let partition_columns = HashSet::from(["host"]);

        let (tag_columns, non_tag_indices) =
            columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns).unwrap();

        assert_eq!(2, tag_columns.len());
        assert_eq!(&[0, 1, 2], non_tag_indices.as_slice());
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_prioritizes_essential_columns() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("host", ArrowDataType::Utf8, true),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("region", ArrowDataType::Utf8, true),
        ]));
        let name_to_ids =
            HashMap::from([("host".to_string(), 1_u32), ("region".to_string(), 2_u32)]);
        let partition_columns = HashSet::from(["host", "region"]);

        let (_tag_columns, non_tag_indices): (_, SmallVec<[usize; 3]>) =
            columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns).unwrap();

        assert_eq!(&[2, 1, 0, 3], non_tag_indices.as_slice());
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_rejects_unexpected_data_type() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("host", ArrowDataType::Utf8, true),
            Field::new("invalid", ArrowDataType::Boolean, true),
        ]));
        let name_to_ids = HashMap::from([("host".to_string(), 1_u32)]);
        let partition_columns = HashSet::from(["host"]);

        let result = columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns);

        assert!(matches!(
            result,
            Err(Error::InvalidPromRemoteRequest { .. })
        ));
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_rejects_int64_timestamp_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("greptime_timestamp", ArrowDataType::Int64, false),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("host", ArrowDataType::Utf8, true),
        ]));
        let name_to_ids = HashMap::from([("host".to_string(), 1_u32)]);
        let partition_columns = HashSet::from(["host"]);

        let result = columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns);

        assert!(matches!(
            result,
            Err(Error::InvalidPromRemoteRequest { .. })
        ));
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_rejects_duplicated_timestamp_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts1",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "ts2",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("host", ArrowDataType::Utf8, true),
        ]));
        let name_to_ids = HashMap::from([("host".to_string(), 1_u32)]);
        let partition_columns = HashSet::from(["host"]);

        let result = columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns);

        assert!(matches!(
            result,
            Err(Error::InvalidPromRemoteRequest { .. })
        ));
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_rejects_duplicated_value_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value1", ArrowDataType::Float64, true),
            Field::new("value2", ArrowDataType::Float64, true),
            Field::new("host", ArrowDataType::Utf8, true),
        ]));
        let name_to_ids = HashMap::from([("host".to_string(), 1_u32)]);
        let partition_columns = HashSet::from(["host"]);

        let result = columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns);

        assert!(matches!(
            result,
            Err(Error::InvalidPromRemoteRequest { .. })
        ));
    }

    #[test]
    fn test_modify_batch_sparse_with_taxonomy_per_batch() {
        use arrow::array::BinaryArray;
        use metric_engine::batch_modifier::modify_batch_sparse;

        let schema1 = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("tag1", ArrowDataType::Utf8, true),
        ]));

        let schema2 = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("tag1", ArrowDataType::Utf8, true),
            Field::new("tag2", ArrowDataType::Utf8, true),
        ]));
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![2000])),
                Arc::new(arrow::array::Float64Array::from(vec![2.0])),
                Arc::new(StringArray::from(vec!["v1"])),
                Arc::new(StringArray::from(vec!["v2"])),
            ],
        )
        .unwrap();

        let name_to_ids = HashMap::from([("tag1".to_string(), 1), ("tag2".to_string(), 2)]);
        let partition_columns = HashSet::new();

        // A batch that only has tag1, same values as batch2 for ts and val.
        let batch3 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![2000])),
                Arc::new(arrow::array::Float64Array::from(vec![2.0])),
                Arc::new(StringArray::from(vec!["v1"])),
            ],
        )
        .unwrap();

        // Simulate the new loop logic in flush_batch_physical:
        // Resolve taxonomy FOR EACH BATCH.
        let (tag_columns2, indices2) =
            columns_taxonomy(&batch2.schema(), "table", &name_to_ids, &partition_columns).unwrap();
        let modified2 = modify_batch_sparse(batch2, 123, &tag_columns2, &indices2).unwrap();

        let (tag_columns3, indices3) =
            columns_taxonomy(&batch3.schema(), "table", &name_to_ids, &partition_columns).unwrap();
        let modified3 = modify_batch_sparse(batch3, 123, &tag_columns3, &indices3).unwrap();

        let pk2 = modified2
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let pk3 = modified3
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        // Now they SHOULD be different because tag2 is included in pk2 but not in pk3.
        assert_ne!(
            pk2.value(0),
            pk3.value(0),
            "PK should be different because batch2 has tag2!"
        );
    }

    #[test]
    fn test_transform_logical_batches_to_physical_success() {
        let batch = mock_tag_batch("tag1", "v1", 1000, 1.0);

        let table_batches = vec![TableBatch {
            table_name: "t1".to_string(),
            table_id: 1,
            batches: vec![batch],
            row_count: 1,
        }];

        let name_to_ids = HashMap::from([("tag1".to_string(), 1)]);
        let partition_columns = HashSet::new();
        let modified =
            transform_logical_batches_to_physical(&table_batches, &name_to_ids, &partition_columns)
                .unwrap();

        assert_eq!(1, modified.len());
        assert_eq!(3, modified[0].num_columns());
        assert_eq!("__primary_key", modified[0].schema().field(0).name());
        assert_eq!("greptime_timestamp", modified[0].schema().field(1).name());
        assert_eq!("greptime_value", modified[0].schema().field(2).name());
    }

    #[test]
    fn test_transform_logical_batches_to_physical_taxonomy_failure() {
        let batch = mock_tag_batch("tag1", "v1", 1000, 1.0);

        let table_batches = vec![TableBatch {
            table_name: "t1".to_string(),
            table_id: 1,
            batches: vec![batch],
            row_count: 1,
        }];

        // tag1 is missing from name_to_ids, causing columns_taxonomy to fail.
        let name_to_ids = HashMap::new();
        let partition_columns = HashSet::new();
        let err =
            transform_logical_batches_to_physical(&table_batches, &name_to_ids, &partition_columns)
                .unwrap_err();

        assert!(
            err.to_string()
                .contains("not found in physical table column IDs")
        );
    }

    #[test]
    fn test_transform_logical_batches_to_physical_multiple_batches() {
        let batch1 = mock_tag_batch("tag1", "v1", 1000, 1.0);
        let batch2 = mock_tag_batch("tag2", "v2", 2000, 2.0);

        let table_batches = vec![
            TableBatch {
                table_name: "t1".to_string(),
                table_id: 1,
                batches: vec![batch1],
                row_count: 1,
            },
            TableBatch {
                table_name: "t2".to_string(),
                table_id: 2,
                batches: vec![batch2],
                row_count: 1,
            },
        ];

        let name_to_ids = HashMap::from([("tag1".to_string(), 1), ("tag2".to_string(), 2)]);
        let partition_columns = HashSet::new();
        let modified =
            transform_logical_batches_to_physical(&table_batches, &name_to_ids, &partition_columns)
                .unwrap();

        assert_eq!(2, modified.len());
    }

    #[test]
    fn test_transform_logical_batches_to_physical_mixed_success_failure() {
        let batch1 = mock_tag_batch("tag1", "v1", 1000, 1.0);
        let batch2 = mock_tag_batch("tag2", "v2", 2000, 2.0);

        let table_batches = vec![
            TableBatch {
                table_name: "t1".to_string(),
                table_id: 1,
                batches: vec![batch1],
                row_count: 1,
            },
            TableBatch {
                table_name: "t2".to_string(),
                table_id: 2,
                batches: vec![batch2],
                row_count: 1,
            },
        ];

        // tag1 is missing from name_to_ids, causing batch1 to fail.
        let name_to_ids = HashMap::from([("tag2".to_string(), 2)]);
        let partition_columns = HashSet::new();
        let err =
            transform_logical_batches_to_physical(&table_batches, &name_to_ids, &partition_columns)
                .unwrap_err();

        assert!(err.to_string().contains("tag1"));
    }

    #[tokio::test]
    async fn test_flush_batch_physical_uses_mockable_trait_dependencies() {
        let table_batches = vec![TableBatch {
            table_name: "t1".to_string(),
            table_id: 11,
            batches: vec![mock_tag_batch("tag1", "host-1", 1000, 1.0)],
            row_count: 1,
        }];
        let partition_calls = Arc::new(AtomicUsize::new(0));
        let leader_calls = Arc::new(AtomicUsize::new(0));
        let node = MockFlushNodeRequester::default();
        let ctx = session::context::QueryContext::arc();

        flush_batch_physical(
            &table_batches,
            "phy",
            &ctx,
            &MockFlushPartitionProvider {
                partition_rule_calls: partition_calls.clone(),
                region_leader_calls: leader_calls.clone(),
            },
            &node,
            &MockFlushCatalogProvider {
                table: Some(mock_physical_table_metadata(1024)),
            },
        )
        .await
        .unwrap();

        assert_eq!(1, partition_calls.load(Ordering::SeqCst));
        assert_eq!(1, leader_calls.load(Ordering::SeqCst));
        assert_eq!(1, node.writes.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_flush_batch_physical_stops_before_partition_and_node_when_table_missing() {
        let table_batches = vec![TableBatch {
            table_name: "t1".to_string(),
            table_id: 11,
            batches: vec![mock_tag_batch("tag1", "host-1", 1000, 1.0)],
            row_count: 1,
        }];
        let partition_calls = Arc::new(AtomicUsize::new(0));
        let leader_calls = Arc::new(AtomicUsize::new(0));
        let node = MockFlushNodeRequester::default();
        let ctx = session::context::QueryContext::arc();

        let err = flush_batch_physical(
            &table_batches,
            "missing_phy",
            &ctx,
            &MockFlushPartitionProvider {
                partition_rule_calls: partition_calls.clone(),
                region_leader_calls: leader_calls.clone(),
            },
            &node,
            &MockFlushCatalogProvider { table: None },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Physical table 'missing_phy' not found")
        );
        assert_eq!(0, partition_calls.load(Ordering::SeqCst));
        assert_eq!(0, leader_calls.load(Ordering::SeqCst));
        assert_eq!(0, node.writes.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_flush_batch_physical_aborts_immediately_on_transform_error() {
        let table_batches = vec![
            TableBatch {
                table_name: "broken".to_string(),
                table_id: 11,
                batches: vec![mock_tag_batch("unknown_tag", "host-1", 1000, 1.0)],
                row_count: 1,
            },
            TableBatch {
                table_name: "healthy".to_string(),
                table_id: 12,
                batches: vec![mock_tag_batch("tag1", "host-2", 2000, 2.0)],
                row_count: 1,
            },
        ];
        let partition_calls = Arc::new(AtomicUsize::new(0));
        let leader_calls = Arc::new(AtomicUsize::new(0));
        let node = MockFlushNodeRequester::default();
        let ctx = session::context::QueryContext::arc();

        let err = flush_batch_physical(
            &table_batches,
            "phy",
            &ctx,
            &MockFlushPartitionProvider {
                partition_rule_calls: partition_calls.clone(),
                region_leader_calls: leader_calls.clone(),
            },
            &node,
            &MockFlushCatalogProvider {
                table: Some(mock_physical_table_metadata(1024)),
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("unknown_tag"));
        assert_eq!(1, partition_calls.load(Ordering::SeqCst));
        assert_eq!(0, leader_calls.load(Ordering::SeqCst));
        assert_eq!(0, node.writes.load(Ordering::SeqCst));
    }

    #[test]
    fn test_plan_region_batches_splits_and_strips_partition_columns() {
        let combined_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("__primary_key", ArrowDataType::Binary, false),
                Field::new(
                    "greptime_timestamp",
                    ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("greptime_value", ArrowDataType::Float64, true),
                Field::new("host", ArrowDataType::Utf8, true),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![b"k1".as_slice(), b"k2".as_slice()])),
                Arc::new(TimestampMillisecondArray::from(vec![1000_i64, 2000_i64])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0_f64, 2.0_f64])),
                Arc::new(StringArray::from(vec!["node-1", "node-2"])),
            ],
        )
        .unwrap();
        let mut planned_batches = plan_region_batches(
            combined_batch,
            1024,
            &TwoRegionPartitionRule {
                partition_columns: vec!["host".to_string()],
            },
            &["host".to_string()],
        )
        .unwrap();
        planned_batches.sort_by_key(|planned| planned.region_id.region_number());

        assert_eq!(2, planned_batches.len());
        assert_eq!(RegionId::new(1024, 1), planned_batches[0].region_id);
        assert_eq!(1, planned_batches[0].row_count);
        assert_eq!(3, planned_batches[0].batch.num_columns());
        assert_eq!(RegionId::new(1024, 2), planned_batches[1].region_id);
        assert_eq!(1, planned_batches[1].row_count);
        assert_eq!(3, planned_batches[1].batch.num_columns());
    }

    #[test]
    fn test_encode_region_write_requests_builds_bulk_insert_requests() {
        let planned_batch = PlannedRegionBatch {
            region_id: RegionId::new(1024, 1),
            row_count: 1,
            batch: RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("__primary_key", ArrowDataType::Binary, false),
                    Field::new(
                        "greptime_timestamp",
                        ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new("greptime_value", ArrowDataType::Float64, true),
                ])),
                vec![
                    Arc::new(BinaryArray::from(vec![b"k1".as_slice()])),
                    Arc::new(TimestampMillisecondArray::from(vec![1000_i64])),
                    Arc::new(arrow::array::Float64Array::from(vec![1.0_f64])),
                ],
            )
            .unwrap(),
        };
        let resolved_batch = ResolvedRegionBatch {
            planned: planned_batch,
            datanode: Peer {
                id: 1,
                addr: "node-1".to_string(),
            },
        };
        let writes = encode_region_write_requests(vec![resolved_batch]).unwrap();

        assert_eq!(1, writes.len());
        assert_eq!(1, writes[0].row_count);
        assert_eq!(1, writes[0].datanode.id);
        let Some(region_request::Body::BulkInsert(request)) = &writes[0].request.body else {
            panic!("expected bulk insert request");
        };
        assert_eq!(RegionId::new(1024, 1).as_u64(), request.region_id);
    }
}
