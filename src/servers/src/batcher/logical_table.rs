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
mod flow_notifier;
mod pending_worker;
mod region_write;
mod tables;
#[cfg(test)]
mod test_util;

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use api::v1::RowInsertRequests;
use catalog::CatalogManagerRef;
use common_batcher::flush_limiter::FlushLimiter;
use common_batcher::flush_policy::timing::TimingFlushPolicy;
use common_batcher::request_limiter::RequestLimiter;
use common_batcher::worker_registry::WorkerRegistry;
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::node_manager::NodeManagerRef;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_time::timestamp::TimeUnit;
use meter_core::data::MeterRecord;
use meter_macros::write_meter;
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::ResultExt;
use tokio::sync::{Semaphore, broadcast, mpsc, oneshot};

use crate::batcher::flow_notifier::{FlowNotifier, start_flow_notification_worker};
pub use crate::batcher::logical_table::batch::flush_batch_physical;
pub use crate::batcher::logical_table::batch_convert::{RecordBatchWithTsIdx, TableBatch};
use crate::batcher::logical_table::pending_worker::{
    PendingWorker, WorkerCommand, remove_worker_if_same_channel, start_worker,
};
pub use crate::batcher::logical_table::region_write::{
    PhysicalFlushCatalogProvider, PhysicalFlushNodeRequester, PhysicalFlushPartitionProvider,
    PhysicalTableMetadata,
};
pub use crate::batcher::logical_table::tables::{
    PendingRowsSchemaAlterer, PendingRowsSchemaAltererRef,
};
use crate::batcher::pending_rows_batch_sync_enabled;
use crate::error;
use crate::error::{Error, Result};
use crate::metrics::{
    FLOW_NOTIFICATION_DROPPED, PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED, PENDING_WORKERS,
};

const PHYSICAL_TABLE_KEY: &str = "physical_table";

const WORKER_IDLE_TIMEOUT_MULTIPLIER: u32 = 3;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct BatchKey {
    catalog: String,
    schema: String,
    physical_table: String,
    skip_wal: bool,
}

// Requests can share a batch only when their write target and WAL policy match.
fn batch_key_from_ctx(ctx: &QueryContextRef) -> BatchKey {
    let physical_table = ctx
        .extension(PHYSICAL_TABLE_KEY)
        .unwrap_or(GREPTIME_PHYSICAL_TABLE)
        .to_string();
    BatchKey {
        catalog: ctx.current_catalog().to_string(),
        schema: ctx.current_schema(),
        physical_table,
        skip_wal: ctx.skip_wal(),
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
    flow_notification_tx: FlowNotifier,
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
        let (flow_notification_tx, flow_notification_rx) = FlowNotifier::try_new(
            flow_notification_queue_capacity.get(),
            FLOW_NOTIFICATION_DROPPED.clone(),
        )?;

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
        self.submit_with(requests, ctx, |_| ready(Ok(())))
            .await
            .map(|(rows, ())| rows)
    }

    /// Returns whether the physical metric table resolved from `ctx` can use
    /// the bulk path. The bulk encode produces millisecond timestamp batches
    /// only, so a physical table with another time index unit (e.g. created
    /// as `TIMESTAMP(6)`) must stay on the ordinary insert path, which
    /// converts the requests to the physical table's unit. A missing physical
    /// table is accepted: it is auto-created with the millisecond unit.
    pub async fn accepts_physical_table_time_index(&self, ctx: &QueryContextRef) -> bool {
        let key = batch_key_from_ctx(ctx);
        let Ok(Some(table)) = self
            .catalog_manager
            .table(&key.catalog, &key.schema, &key.physical_table, None)
            .await
        else {
            return true;
        };
        table
            .table_info()
            .meta
            .schema
            .timestamp_column()
            .and_then(|col| col.data_type.as_timestamp().map(|ts| ts.unit()))
            .map(|unit| unit == TimeUnit::Millisecond)
            .unwrap_or(true)
    }

    /// Submits with request-level accounting after schema preparation and before
    /// queue admission. Acknowledgement follows the global batching policy.
    pub async fn submit_with<T, F>(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
        after_prepare: impl FnOnce(RowInsertRequests) -> F,
    ) -> Result<(u64, T)>
    where
        F: Future<Output = Result<T>> + Send,
    {
        let (table_batches, total_rows) = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["submit_build_and_align"])
                .start_timer();
            self.build_and_align_table_batches(&requests, &ctx).await?
        };
        let prepared = after_prepare(requests).await?;
        if total_rows == 0 {
            return Ok((0, prepared));
        }

        // Flushes dispatch directly to datanodes, so admit once before enqueueing.
        write_meter!(MeterRecord::new(
            ctx.current_catalog().to_string(),
            ctx.current_schema(),
            0,
            ctx.write_rows_to_admit(
                ctx.current_catalog(),
                &ctx.current_schema(),
                total_rows as u64
            ),
            ctx.channel() as u8,
        ))
        .await
        .context(error::WriteRejectedSnafu)?;

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
                .map(|()| (total_rows as u64, prepared))
        } else {
            Ok((total_rows as u64, prepared))
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use common_query::prelude::greptime_timestamp;

    use crate::batcher::logical_table::batch_convert::{RecordBatchWithTsIdx, TableBatch};
    use crate::batcher::logical_table::flow_notifier::extract_timestamps;
    use crate::batcher::logical_table::test_util::{mock_aligned_tag_batch, mock_tag_batch};

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
    fn test_batch_key_groups_by_skip_wal() {
        use std::collections::HashMap;

        use crate::batcher::logical_table::batch_key_from_ctx;

        let wal_ctx = session::context::QueryContext::arc();
        let skip_wal_ctx = session::context::QueryContext::arc();
        skip_wal_ctx.set_skip_wal(true);
        let another_skip_wal_ctx = session::context::QueryContext::arc();
        another_skip_wal_ctx.set_skip_wal(true);

        let mut batches = HashMap::new();
        *batches.entry(batch_key_from_ctx(&wal_ctx)).or_insert(0) += 1;
        *batches
            .entry(batch_key_from_ctx(&skip_wal_ctx))
            .or_insert(0) += 1;
        *batches
            .entry(batch_key_from_ctx(&another_skip_wal_ctx))
            .or_insert(0) += 1;
        *batches
            .entry(batch_key_from_ctx(&session::context::QueryContext::arc()))
            .or_insert(0) += 1;

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[&batch_key_from_ctx(&wal_ctx)], 2);
        assert_eq!(batches[&batch_key_from_ctx(&skip_wal_ctx)], 2);
    }
}
