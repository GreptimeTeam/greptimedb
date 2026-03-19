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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::helper::ColumnDataTypeWrapper;
use api::v1::region::{
    BulkInsertRequest, RegionRequest, RegionRequestHeader, bulk_insert_request, region_request,
};
use api::v1::value::ValueData;
use api::v1::{ArrowIpc, RowInsertRequests, Rows};
use arrow::array::{
    ArrayRef, Float64Builder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
    new_null_array,
};
use arrow::compute::{cast, concat_batches, filter_record_batch};
use arrow::datatypes::{Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_schema::TimeUnit;
use bytes::Bytes;
use catalog::CatalogManagerRef;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_meta::node_manager::NodeManagerRef;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{error, info, warn};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::{ResultExt, ensure};
use store_api::storage::RegionId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot};

use crate::error;
use crate::error::{Error, Result};
use crate::metrics::{
    FLUSH_DROPPED_ROWS, FLUSH_ELAPSED, FLUSH_FAILURES, FLUSH_ROWS, FLUSH_TOTAL, PENDING_BATCHES,
    PENDING_ROWS, PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED,
};

const PHYSICAL_TABLE_KEY: &str = "physical_table";
/// Whether wait for ingestion result before reply to client.
const PENDING_ROWS_BATCH_SYNC_ENV: &str = "PENDING_ROWS_BATCH_SYNC";

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct BatchKey {
    catalog: String,
    schema: String,
    physical_table: String,
}

#[derive(Debug)]
struct TableBatch {
    table_name: String,
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
        table_batches: Vec<(String, RecordBatch)>,
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
    workers: DashMap<BatchKey, PendingWorker>,
    flush_interval: Duration,
    max_batch_rows: usize,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
    flush_semaphore: Arc<Semaphore>,
    inflight_semaphore: Arc<Semaphore>,
    worker_channel_capacity: usize,
    pending_rows_batch_sync: bool,
    shutdown: broadcast::Sender<()>,
}

impl PendingRowsBatcher {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
        catalog_manager: CatalogManagerRef,
        flush_interval: Duration,
        max_batch_rows: usize,
        max_concurrent_flushes: usize,
        worker_channel_capacity: usize,
        max_inflight_requests: usize,
    ) -> Option<Arc<Self>> {
        if flush_interval.is_zero() {
            return None;
        }

        let (shutdown, _) = broadcast::channel(1);
        let pending_rows_batch_sync = std::env::var(PENDING_ROWS_BATCH_SYNC_ENV)
            .ok()
            .as_deref()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);
        Some(Arc::new(Self {
            workers: DashMap::new(),
            flush_interval,
            max_batch_rows,
            partition_manager,
            node_manager,
            catalog_manager,
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
                .with_label_values(&["submit_build_table_batches"])
                .start_timer();
            build_table_batches(requests)?
        };
        if total_rows == 0 {
            return Ok(0);
        }
        let table_batches = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["submit_align_region_schema"])
                .start_timer();
            self.align_table_batches_to_region_schema(table_batches, &ctx)
                .await?
        };

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

        let worker = self.get_or_spawn_worker(batch_key_from_ctx(&ctx));
        let cmd = WorkerCommand::Submit {
            table_batches,
            total_rows,
            ctx,
            response_tx,
            _permit: permit,
        };

        {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["submit_send_to_worker"])
                .start_timer();
            worker
                .tx
                .send(cmd)
                .await
                .map_err(|_| Error::BatcherChannelClosed)?;
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

    async fn align_table_batches_to_region_schema(
        &self,
        table_batches: Vec<(String, RecordBatch)>,
        ctx: &QueryContextRef,
    ) -> Result<Vec<(String, RecordBatch)>> {
        let catalog = ctx.current_catalog().to_string();
        let schema = ctx.current_schema();
        let mut region_schemas: HashMap<String, Arc<ArrowSchema>> = HashMap::new();
        let mut aligned_batches = Vec::with_capacity(table_batches.len());

        for (table_name, record_batch) in table_batches {
            let region_schema = if let Some(region_schema) = region_schemas.get(&table_name) {
                region_schema.clone()
            } else {
                let table = self
                    .catalog_manager
                    .table(&catalog, &schema, &table_name, Some(ctx.as_ref()))
                    .await
                    .map_err(|err| Error::Internal {
                        err_msg: format!(
                            "Failed to resolve table {} for pending batch alignment: {}",
                            table_name, err
                        ),
                    })?
                    .ok_or_else(|| Error::Internal {
                        err_msg: format!(
                            "Table not found during pending batch alignment: {}",
                            table_name
                        ),
                    })?;
                let region_schema = table.table_info().meta.schema.arrow_schema().clone();
                region_schemas.insert(table_name.clone(), region_schema.clone());
                region_schema
            };

            let record_batch = align_record_batch_to_schema(record_batch, region_schema.as_ref())?;
            aligned_batches.push((table_name, record_batch));
        }

        Ok(aligned_batches)
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
                    self.partition_manager.clone(),
                    self.node_manager.clone(),
                    self.catalog_manager.clone(),
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
    mut rx: mpsc::Receiver<WorkerCommand>,
    shutdown: broadcast::Sender<()>,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
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
                        Some(WorkerCommand::Submit { table_batches, total_rows, ctx, response_tx, _permit }) => {
                            if batch.total_row_count == 0 {
                                batch.created_at = Some(Instant::now());
                                batch.ctx = Some(ctx);
                                PENDING_BATCHES.inc();
                            }

                            batch.waiters.push(FlushWaiter { response_tx, _permit });

                            for (table_name, record_batch) in table_batches {
                                let entry = batch.tables.entry(table_name.clone()).or_insert_with(|| TableBatch {
                                    table_name,
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
    });
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

    macro_rules! record_failure {
        ($row_count:expr, $msg:expr) => {{
            let msg = $msg;
            if first_error.is_none() {
                first_error = Some(msg.clone());
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
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
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
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["flush_resolve_table"])
                .start_timer();
            match catalog_manager
                .table(
                    &catalog,
                    &schema,
                    &table_batch.table_name,
                    Some(ctx.as_ref()),
                )
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
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
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
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
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

        for (region_number, mask) in region_masks {
            if mask.select_none() {
                continue;
            }

            let region_batch = if mask.select_all() {
                record_batch.clone()
            } else {
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
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
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
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
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
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

            let datanode = node_manager.datanode(&datanode).await;
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["flush_write_region"])
                .start_timer();
            match datanode.handle(request).await {
                Ok(_) => {
                    FLUSH_TOTAL.inc();
                    FLUSH_ROWS.observe(row_count as f64);
                }
                Err(err) => {
                    record_failure!(
                        row_count,
                        format!(
                            "Bulk insert flush failed for region {}: {:?}",
                            region_id, err
                        )
                    );
                }
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    FLUSH_ELAPSED.observe(elapsed);
    info!(
        "Pending rows batch flushed, total rows: {}, elapsed time: {}s",
        total_row_count, elapsed
    );

    notify_waiters(waiters, &first_error);
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

fn build_table_batches(requests: RowInsertRequests) -> Result<(Vec<(String, RecordBatch)>, usize)> {
    let mut table_batches = Vec::with_capacity(requests.inserts.len());
    let mut total_rows = 0;

    for request in requests.inserts {
        let Some(rows) = request.rows else {
            continue;
        };
        if rows.rows.is_empty() {
            continue;
        }

        let record_batch = rows_to_record_batch(&rows)?;
        total_rows += record_batch.num_rows();
        table_batches.push((request.table_name, record_batch));
    }

    Ok((table_batches, total_rows))
}

fn align_record_batch_to_schema(
    record_batch: RecordBatch,
    target_schema: &ArrowSchema,
) -> Result<RecordBatch> {
    let source_schema = record_batch.schema();
    if source_schema.as_ref() == target_schema {
        return Ok(record_batch);
    }

    for source_field in source_schema.fields() {
        if target_schema
            .column_with_name(source_field.name())
            .is_none()
        {
            return Err(Error::Internal {
                err_msg: format!(
                    "Failed to align record batch schema, column '{}' not found in target schema",
                    source_field.name()
                ),
            });
        }
    }

    let row_count = record_batch.num_rows();
    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for target_field in target_schema.fields() {
        let column = if let Some((index, source_field)) =
            source_schema.column_with_name(target_field.name())
        {
            let source_column = record_batch.column(index).clone();
            if source_field.data_type() == target_field.data_type() {
                source_column
            } else {
                cast(source_column.as_ref(), target_field.data_type()).map_err(|err| {
                    Error::Internal {
                        err_msg: format!(
                            "Failed to cast column '{}' to target type {:?}: {}",
                            target_field.name(),
                            target_field.data_type(),
                            err
                        ),
                    }
                })?
            }
        } else {
            new_null_array(target_field.data_type(), row_count)
        };
        columns.push(column);
    }

    RecordBatch::try_new(Arc::new(target_schema.clone()), columns).map_err(|err| Error::Internal {
        err_msg: format!("Failed to build aligned record batch: {}", err),
    })
}

fn rows_to_record_batch(rows: &Rows) -> Result<RecordBatch> {
    let row_count = rows.rows.len();
    let column_count = rows.schema.len();

    for (idx, row) in rows.rows.iter().enumerate() {
        ensure!(
            row.values.len() == column_count,
            error::InternalSnafu {
                err_msg: format!(
                    "Column count mismatch in row {}, expected {}, got {}",
                    idx,
                    column_count,
                    row.values.len()
                )
            }
        );
    }

    let mut fields = Vec::with_capacity(column_count);
    let mut columns = Vec::with_capacity(column_count);

    for (idx, column_schema) in rows.schema.iter().enumerate() {
        let datatype_wrapper = ColumnDataTypeWrapper::try_new(
            column_schema.datatype,
            column_schema.datatype_extension.clone(),
        )?;
        let data_type = ConcreteDataType::from(datatype_wrapper);
        fields.push(Field::new(
            column_schema.column_name.clone(),
            data_type.as_arrow_type(),
            true,
        ));
        columns.push(build_arrow_array(
            rows,
            idx,
            &column_schema.column_name,
            data_type.as_arrow_type(),
            row_count,
        )?);
    }

    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).context(error::ArrowSnafu)
}

fn build_arrow_array(
    rows: &Rows,
    col_idx: usize,
    column_name: &String,
    column_data_type: arrow::datatypes::DataType,
    row_count: usize,
) -> Result<ArrayRef> {
    macro_rules! build_array {
        ($builder:expr, $( $pattern:pat => $value:expr ),+ $(,)?) => {{
            let mut builder = $builder;
            for row in &rows.rows {
                match row.values[col_idx].value_data.as_ref() {
                    $(Some($pattern) => builder.append_value($value),)+
                    Some(v) => {
                        return error::InvalidPromRemoteRequestSnafu {
                            msg: format!("Unexpected value: {:?}", v),
                        }
                        .fail();
                    }
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }};
    }

    let array: ArrayRef = match column_data_type {
        arrow::datatypes::DataType::Float64 => {
            build_array!(Float64Builder::with_capacity(row_count), ValueData::F64Value(v) => *v)
        }
        arrow::datatypes::DataType::Utf8 => build_array!(
            StringBuilder::with_capacity(row_count, 0),
            ValueData::StringValue(v) => v
        ),
        arrow::datatypes::DataType::Timestamp(u, _) => match u {
            TimeUnit::Second => build_array!(
                TimestampSecondBuilder::with_capacity(row_count),
                ValueData::TimestampSecondValue(v) => *v
            ),
            TimeUnit::Millisecond => build_array!(
                TimestampMillisecondBuilder::with_capacity(row_count),
                ValueData::TimestampMillisecondValue(v) => *v
            ),
            TimeUnit::Microsecond => build_array!(
                TimestampMicrosecondBuilder::with_capacity(row_count),
                ValueData::DatetimeValue(v) => *v,
                ValueData::TimestampMicrosecondValue(v) => *v
            ),
            TimeUnit::Nanosecond => build_array!(
                TimestampNanosecondBuilder::with_capacity(row_count),
                ValueData::TimestampNanosecondValue(v) => *v
            ),
        },
        ty => {
            return error::InvalidPromRemoteRequestSnafu {
                msg: format!(
                    "Unexpected column type {:?}, column name: {}",
                    ty, column_name
                ),
            }
            .fail();
        }
    };

    Ok(array)
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
    use std::sync::Arc;

    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use super::{align_record_batch_to_schema, rows_to_record_batch};

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
}
