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

//! Table-scoped RecordBatch accumulation before bulk routing and encoding.

mod batch;
mod flow_notifier;
mod metrics;
mod pending_batch;
mod pending_worker;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_batcher::flush_limiter::FlushLimiter;
use common_batcher::flush_policy::timing::TimingFlushPolicy;
use common_batcher::request_limiter::RequestLimiter;
use common_batcher::worker_registry::WorkerRegistry;
use operator::batcher::PendingRowsBatcher;
use operator::error::{BatchFlushSnafu, Result, UnexpectedSnafu};
use operator::insert::Inserter;
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::metadata::TableInfoRef;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, oneshot};

use crate::batcher::table::flow_notifier::FlowNotifier;
use crate::batcher::table::metrics::PENDING_WORKERS;
use crate::batcher::table::pending_batch::PendingBatch;
use crate::batcher::table::pending_worker::{PendingWorker, WorkerCommand, start_worker};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct BatchKey {
    catalog: String,
    schema: String,
    table_id: u32,
    table_version: u64,
}

type PendingWorkers = WorkerRegistry<BatchKey, WorkerCommand>;

/// Admission and worker lookup for table-scoped bulk writes.
pub struct TablePendingRowsBatcher {
    workers: Arc<PendingWorkers>,
    flush_policy: TimingFlushPolicy,
    flush_limiter: FlushLimiter,
    request_limiter: RequestLimiter,
    worker_channel_capacity: usize,
    worker_idle_timeout: Duration,
    inserter: Arc<Inserter>,
    flow_notifier: FlowNotifier,
    shutdown: broadcast::Sender<()>,
}

impl TablePendingRowsBatcher {
    /// Creates a shared timing batcher, rejecting disabled or unsupported limits.
    pub fn try_new(
        flush_interval: Duration,
        max_batch_rows: usize,
        max_concurrent_flushes: usize,
        worker_channel_capacity: usize,
        max_inflight_requests: usize,
        flow_notification_queue_capacity: NonZeroUsize,
        inserter: Arc<Inserter>,
    ) -> Option<Arc<Self>> {
        if worker_channel_capacity == 0
            || worker_channel_capacity > Semaphore::MAX_PERMITS
            || max_inflight_requests == 0
            || max_inflight_requests > Semaphore::MAX_PERMITS
        {
            return None;
        }
        let flush_policy = TimingFlushPolicy::try_new(flush_interval, max_batch_rows)?;
        let flush_limiter = FlushLimiter::try_new(max_concurrent_flushes)?;
        let request_limiter = RequestLimiter::try_new(max_inflight_requests)?;
        let flow_notifier = FlowNotifier::new(
            inserter.table_flownode_set_cache().clone(),
            inserter.node_manager().clone(),
            flow_notification_queue_capacity,
        )?;
        let (shutdown, _) = broadcast::channel(1);
        PENDING_WORKERS.set(0);
        Some(Arc::new(Self {
            workers: Arc::new(WorkerRegistry::new()),
            flush_policy,
            flush_limiter,
            request_limiter,
            worker_channel_capacity,
            worker_idle_timeout: flush_interval.checked_mul(3).unwrap_or(flush_interval),
            inserter,
            flow_notifier,
            shutdown,
        }))
    }

    async fn worker(&self, key: &BatchKey) -> PendingWorker {
        let (tx, receiver) = self
            .workers
            .get_or_create(key.clone(), self.worker_channel_capacity)
            .await;
        if let Some(rx) = receiver {
            start_worker(
                key.clone(),
                tx.clone(),
                self.workers.clone(),
                rx,
                self.shutdown.clone(),
                self.flush_policy,
                self.flush_limiter.clone(),
                self.inserter.clone(),
                self.flow_notifier.clone(),
                self.worker_idle_timeout,
            );
            PENDING_WORKERS.set(self.workers.len().await as i64);
        }
        PendingWorker { tx }
    }
}

impl Drop for TablePendingRowsBatcher {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
    }
}

#[async_trait]
impl PendingRowsBatcher for TablePendingRowsBatcher {
    /// Acquires once per original request, before submitting any of its tables.
    /// Retain a clone in every submission until its flush has completed.
    async fn acquire(&self) -> Result<Arc<OwnedSemaphorePermit>> {
        self.request_limiter.acquire().await.map_err(|_| {
            UnexpectedSnafu {
                violated: "batcher admission closed".to_string(),
            }
            .build()
        })
    }

    /// Waits for completed bulk writes. Cancellation does not retract an admitted
    /// submission. Combined failures affect all waiters and may be partial writes.
    async fn submit(
        &self,
        table_info: TableInfoRef,
        batch: RecordBatch,
        ctx: QueryContextRef,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<usize> {
        let total_rows = batch.num_rows();
        if total_rows == 0 {
            return Ok(0);
        }
        if table_info.catalog_name != ctx.current_catalog()
            || table_info.schema_name != ctx.current_schema()
        {
            return UnexpectedSnafu {
                violated: "batch table and request database differ".to_string(),
            }
            .fail();
        }
        // TODO: Propagate skip_wal through the ordinary-table bulk path.
        let key = BatchKey {
            catalog: ctx.current_catalog().to_string(),
            schema: ctx.current_schema().clone(),
            table_id: table_info.table_id(),
            table_version: table_info.ident.version,
        };
        let (response_tx, response_rx) = oneshot::channel();
        let pending = PendingBatch {
            table_info,
            batch,
            ctx,
            response_tx,
            _permit: permit,
        };
        let mut command = Some(WorkerCommand::Submit(pending));
        for _ in 0..2 {
            let worker = self.worker(&key).await;
            let Some(pending) = command.take() else { break };
            match worker.tx.send(pending).await {
                Ok(()) => break,
                Err(error) => {
                    command = Some(error.0);
                    if self.workers.remove_if_same(&key, &worker.tx).await {
                        PENDING_WORKERS.set(self.workers.len().await as i64);
                    }
                }
            }
        }
        if command.is_some() {
            return UnexpectedSnafu {
                violated: "batch worker channel closed".to_string(),
            }
            .fail();
        }
        response_rx
            .await
            .map_err(|_| {
                UnexpectedSnafu {
                    violated: "batch worker stopped before reporting its write result".to_string(),
                }
                .build()
            })?
            .context(BatchFlushSnafu)?;
        Ok(total_rows)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use api::region::RegionResponse;
    use api::v1::helper::{tag_column_schema, time_index_column_schema};
    use api::v1::region::{RegionRequest, bulk_insert_request, region_request};
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row, Rows, Value};
    use arrow::array::{Int32Array, TimestampMillisecondArray};
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::record_batch::RecordBatch;
    use common_batcher::flush_limiter::FlushLimiter;
    use common_batcher::flush_policy::timing::TimingFlushPolicy;
    use common_batcher::request_limiter::RequestLimiter;
    use common_batcher::worker_registry::WorkerRegistry;
    use common_grpc::flight::FlightDecoder;
    use common_meta::error::Result as MetaResult;
    use common_meta::peer::Peer;
    use common_meta::test_util::{MockDatanodeHandler, MockDatanodeManager};
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use common_telemetry::info;
    use datatypes::schema::{ColumnDefaultConstraint, SchemaBuilder};
    use datatypes::value::Value as DtValue;
    use operator::batcher::PendingRowsBatcher;
    use operator::error::Error;
    use operator::insert::Inserter;
    use operator::req_convert::insert::rows_to_record_batch;
    use operator::test_util::{
        create_partition_rule_manager, new_test_table_info, prepare_mocked_backend,
    };
    use session::context::{Channel, QueryContext};
    use store_api::storage::RegionId;
    use table::metadata::TableInfoRef;
    use tokio::sync::{broadcast, mpsc, oneshot};
    use tokio::time::timeout;

    use crate::batcher::table::flow_notifier::FlowNotifier;
    use crate::batcher::table::pending_batch::PendingBatch;
    use crate::batcher::table::pending_worker::{WorkerCommand, start_worker};
    use crate::batcher::table::{BatchKey, PendingWorkers, TablePendingRowsBatcher};
    use crate::batcher::test_util::mock_table_flownode_cache;

    #[derive(Clone)]
    struct BulkHandler {
        requests: Arc<Mutex<Vec<RecordBatch>>>,
        report_missing_row: bool,
    }

    #[async_trait::async_trait]
    impl MockDatanodeHandler for BulkHandler {
        async fn handle(&self, peer: &Peer, request: RegionRequest) -> MetaResult<RegionResponse> {
            assert_eq!(3, peer.id);
            assert_eq!("public", request.header.unwrap().dbname);
            let Some(region_request::Body::BulkInsert(request)) = request.body else {
                panic!("expected bulk insert")
            };
            assert_eq!(RegionId::new(1, 3).as_u64(), request.region_id);
            assert!(request.partition_expr_version.is_some());
            let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = request.body else {
                panic!("expected Arrow IPC")
            };
            let batch = FlightDecoder::try_from_schema_bytes(&ipc.schema)
                .unwrap()
                .try_decode_record_batch(&ipc.data_header, &ipc.payload)
                .unwrap();
            let rows = batch.num_rows();
            self.requests.lock().unwrap().push(batch);
            Ok(RegionResponse::new(
                rows - usize::from(self.report_missing_row),
            ))
        }

        async fn handle_query(
            &self,
            _: &Peer,
            _: QueryRequest,
        ) -> MetaResult<SendableRecordBatchStream> {
            panic!("batching must not query the datanode")
        }
    }

    fn rows(value: i32) -> Rows {
        Rows {
            schema: vec![
                tag_column_schema("a", ColumnDataType::Int32),
                time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
            ],
            rows: vec![Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::I32Value(value)),
                    },
                    Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(
                            1000 + i64::from(value),
                        )),
                    },
                ],
            }],
        }
    }

    async fn run_bulk_case(
        max_batch_rows: usize,
        report_missing_row: bool,
        shared_request: bool,
    ) -> usize {
        let backend = prepare_mocked_backend().await;
        let partitions = create_partition_rule_manager(backend.clone()).await;
        let captured = Arc::new(Mutex::new(Vec::new()));
        let nodes = Arc::new(MockDatanodeManager::new(BulkHandler {
            requests: captured.clone(),
            report_missing_row,
        }));
        let inserter = Arc::new(Inserter::new(
            catalog::memory::MemoryCatalogManager::new(),
            partitions,
            nodes,
            mock_table_flownode_cache(1, vec![]).await,
            true,
        ));
        let mut table = new_test_table_info(1, "table_1", [1, 2, 3].into_iter());
        let mut columns = table.meta.schema.column_schemas().to_vec();
        columns[2] = columns[2]
            .clone()
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::Int32(7))))
            .unwrap();
        table.meta.schema = Arc::new(
            SchemaBuilder::try_from(columns)
                .unwrap()
                .version(123)
                .build()
                .unwrap(),
        );
        let table = Arc::new(table);
        let first = rows_to_record_batch(&rows(1), &table).unwrap();
        let second = rows_to_record_batch(&rows(2), &table).unwrap();
        // Keep timer/concurrency/workload fixed; only the supplied row threshold
        // determines whether these two submissions share a bulk request.
        let batcher: Arc<dyn PendingRowsBatcher> = TablePendingRowsBatcher::try_new(
            Duration::from_secs(3600),
            max_batch_rows,
            1,
            4,
            if shared_request { 1 } else { 4 },
            NonZeroUsize::new(16).unwrap(),
            inserter,
        )
        .unwrap();
        let influx_ctx = Arc::new(QueryContext::with_channel(
            "greptime",
            "public",
            Channel::Influx,
        ));
        let opentsdb_ctx = Arc::new(QueryContext::with_channel(
            "greptime",
            "public",
            Channel::Opentsdb,
        ));
        let first_permit = batcher.acquire().await.unwrap();
        let second_permit = if shared_request {
            first_permit.clone()
        } else {
            batcher.acquire().await.unwrap()
        };
        let (first_result, second_result) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                batcher.submit(table.clone(), first, influx_ctx, first_permit),
                batcher.submit(table.clone(), second, opentsdb_ctx, second_permit),
            )
        })
        .await
        .expect("the row threshold did not dispatch the combined bulk insert");
        if report_missing_row {
            assert!(first_result.is_err());
            assert!(second_result.is_err());
        } else {
            assert_eq!(1, first_result.unwrap());
            assert_eq!(1, second_result.unwrap());
        }
        let requests = captured.lock().unwrap();
        let mut actual_rows = Vec::new();
        for batch in requests.iter() {
            assert_eq!(
                vec!["a", "ts", "b"],
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>()
            );
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let ts = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let b = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            actual_rows.extend(
                (0..batch.num_rows())
                    .map(|index| (a.value(index), ts.value(index), b.value(index))),
            );
        }
        actual_rows.sort_unstable();
        assert_eq!(vec![(1, 1001, 7), (2, 1002, 7)], actual_rows);
        requests.len()
    }

    #[tokio::test]
    async fn test_submit_combines_then_routes_bulk_with_defaults() {
        for report_missing_row in [false, true] {
            assert_eq!(1, run_bulk_case(2, report_missing_row, false).await);
        }
    }

    #[tokio::test]
    async fn test_row_threshold_ablation_reduces_bulk_requests() {
        for repetition in 0..2 {
            let direct_count = run_bulk_case(1, false, false).await;
            let combined_count = run_bulk_case(2, false, false).await;
            assert_eq!(2, direct_count);
            assert_eq!(1, combined_count);
            info!(
                "repetition={repetition} rows=2 threshold=1 bulk_requests={direct_count}; threshold=2 bulk_requests={combined_count}"
            );
        }
    }

    #[tokio::test]
    async fn test_original_request_shares_one_admission_slot() {
        assert_eq!(1, run_bulk_case(2, false, true).await);
    }

    const WORKER_TIMEOUT: Duration = Duration::from_secs(5);

    #[derive(Clone)]
    struct GatedHandler {
        inner: BulkHandler,
        entered: mpsc::UnboundedSender<oneshot::Sender<()>>,
    }

    #[async_trait::async_trait]
    impl MockDatanodeHandler for GatedHandler {
        async fn handle(&self, peer: &Peer, request: RegionRequest) -> MetaResult<RegionResponse> {
            let response = self.inner.handle(peer, request).await?;
            let (release_tx, release_rx) = oneshot::channel();
            self.entered.send(release_tx).unwrap();
            release_rx.await.unwrap();
            Ok(response)
        }

        async fn handle_query(
            &self,
            _: &Peer,
            _: QueryRequest,
        ) -> MetaResult<SendableRecordBatchStream> {
            panic!("worker must not query the datanode")
        }
    }

    struct WorkerTest {
        tx: mpsc::Sender<WorkerCommand>,
        shutdown: broadcast::Sender<()>,
        workers: Arc<PendingWorkers>,
        table: TableInfoRef,
        entered: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
    }

    impl Drop for WorkerTest {
        fn drop(&mut self) {
            let _ = self.shutdown.send(());
        }
    }

    impl WorkerTest {
        async fn new(max_rows: usize, max_flushes: usize) -> Self {
            let backend = prepare_mocked_backend().await;
            let partitions = create_partition_rule_manager(backend.clone()).await;
            let (entered_tx, entered) = mpsc::unbounded_channel();
            let nodes = Arc::new(MockDatanodeManager::new(GatedHandler {
                inner: BulkHandler {
                    requests: Arc::new(Mutex::new(Vec::new())),
                    report_missing_row: false,
                },
                entered: entered_tx,
            }));
            let cache = mock_table_flownode_cache(1, vec![]).await;
            let notifier =
                FlowNotifier::new(cache.clone(), nodes.clone(), NonZeroUsize::new(16).unwrap())
                    .unwrap();
            let inserter = Arc::new(Inserter::new(
                catalog::memory::MemoryCatalogManager::new(),
                partitions,
                nodes,
                cache,
                true,
            ));
            let table = Arc::new(new_test_table_info(1, "table_1", [1, 2, 3].into_iter()));
            let key = BatchKey {
                catalog: table.catalog_name.clone(),
                schema: table.schema_name.clone(),
                table_id: 1,
                table_version: table.ident.version,
            };
            let workers = Arc::new(WorkerRegistry::new());
            let (tx, rx) = mpsc::channel(1);
            workers.get_or_insert_with(key.clone(), || tx.clone()).await;
            let (shutdown, _) = broadcast::channel(1);
            start_worker(
                key,
                tx.clone(),
                workers.clone(),
                rx,
                shutdown.clone(),
                TimingFlushPolicy::try_new(Duration::from_secs(3600), max_rows).unwrap(),
                FlushLimiter::try_new(max_flushes).unwrap(),
                inserter,
                notifier,
                Duration::from_secs(10800),
            );
            Self {
                tx,
                shutdown,
                workers,
                table,
                entered,
            }
        }

        async fn submit(
            &self,
            count: usize,
            changed_schema: bool,
        ) -> oneshot::Receiver<Result<(), Arc<Error>>> {
            let mut input = rows(1);
            input.rows = vec![input.rows[0].clone(); count];
            let mut batch = rows_to_record_batch(&input, &self.table).unwrap();
            if changed_schema {
                let schema = ArrowSchema::new_with_metadata(
                    batch.schema().fields().clone(),
                    [("test_version".to_string(), "2".to_string())]
                        .into_iter()
                        .collect(),
                );
                batch = RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec()).unwrap();
            }
            let limiter = RequestLimiter::try_new(1).unwrap();
            let (response_tx, response_rx) = oneshot::channel();
            self.tx
                .send(WorkerCommand::Submit(PendingBatch {
                    table_info: self.table.clone(),
                    batch,
                    ctx: QueryContext::arc(),
                    response_tx,
                    _permit: limiter.acquire().await.unwrap(),
                }))
                .await
                .unwrap();
            // With capacity one, this proves the submitted command was dequeued.
            drop(
                timeout(WORKER_TIMEOUT, self.tx.reserve())
                    .await
                    .unwrap()
                    .unwrap(),
            );
            response_rx
        }

        async fn entered(&mut self) -> oneshot::Sender<()> {
            timeout(WORKER_TIMEOUT, self.entered.recv())
                .await
                .expect("flush did not reach datanode")
                .unwrap()
        }

        async fn stopped(&self) {
            timeout(WORKER_TIMEOUT, async {
                while !self.workers.is_empty().await {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("worker did not remove its registry entry");
        }
    }

    #[tokio::test]
    async fn test_same_schema_flushes_overlap() {
        let mut worker = WorkerTest::new(1, 2).await;
        let first = worker.submit(1, false).await;
        let first_release = worker.entered().await;
        let second = worker.submit(1, false).await;
        let second_release = worker.entered().await;
        // Both RPCs reached the datanode before either was allowed to complete.
        first_release.send(()).unwrap();
        second_release.send(()).unwrap();
        timeout(WORKER_TIMEOUT, first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(WORKER_TIMEOUT, second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        worker.shutdown.send(()).unwrap();
        worker.stopped().await;
    }

    #[tokio::test]
    async fn test_schema_change_waits_for_prior_flush() {
        let mut worker = WorkerTest::new(1, 2).await;
        let first = worker.submit(1, false).await;
        let first_release = worker.entered().await;
        let second = worker.submit(1, true).await;
        // A second flush permit is free, but the schema barrier must withhold the RPC.
        assert!(
            timeout(Duration::from_millis(100), worker.entered.recv())
                .await
                .is_err()
        );
        first_release.send(()).unwrap();
        let second_release = worker.entered().await;
        second_release.send(()).unwrap();
        timeout(WORKER_TIMEOUT, first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(WORKER_TIMEOUT, second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        worker.shutdown.send(()).unwrap();
        worker.stopped().await;
    }

    #[tokio::test]
    async fn test_shutdown_flushes_inline_without_joining_inflight() {
        let mut worker = WorkerTest::new(2, 1).await;
        let mut first = worker.submit(2, false).await;
        let first_release = worker.entered().await;
        let pending = worker.submit(1, false).await;
        worker.shutdown.send(()).unwrap();
        // The normal flush owns the sole permit. Shutdown must still start pending inline.
        let pending_release = worker.entered().await;
        pending_release.send(()).unwrap();
        timeout(WORKER_TIMEOUT, pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        worker.stopped().await;
        assert!(matches!(
            first.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        first_release.send(()).unwrap();
        timeout(WORKER_TIMEOUT, first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
