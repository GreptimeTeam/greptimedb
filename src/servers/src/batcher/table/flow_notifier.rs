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

use std::num::NonZeroUsize;

use arrow::record_batch::RecordBatch;
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::node_manager::NodeManagerRef;
use common_telemetry::error;
use lazy_static::lazy_static;
use operator::req_convert::insert::extract_timestamps;
use prometheus::{IntCounterVec, register_int_counter_vec};
use table::metadata::TableInfoRef;

use crate::batcher::flow_notifier::{
    FlowNotifier as NotificationQueue, start_flow_notification_worker,
};
use crate::batcher::flow_sender::FlowNotification;

lazy_static! {
    static ref FLOW_NOTIFICATION_DROPPED: IntCounterVec = register_int_counter_vec!(
        "greptime_table_batcher_flow_notification_dropped_total",
        "Ordinary batch flow notifications dropped before delivery",
        &["reason"]
    )
    .unwrap();
}

/// Enqueues best-effort flow notifications independently of write completion.
#[derive(Clone)]
pub(in crate::batcher::table) struct FlowNotifier {
    notifier: NotificationQueue,
}

impl FlowNotifier {
    /// Starts bounded delivery on the shared runtime, or rejects unsupported capacity.
    pub fn new(
        cache: TableFlownodeSetCacheRef,
        node_manager: NodeManagerRef,
        capacity: NonZeroUsize,
    ) -> Option<Self> {
        let (notifier, receiver) =
            NotificationQueue::try_new(capacity.get(), FLOW_NOTIFICATION_DROPPED.clone())?;
        start_flow_notification_worker(receiver, cache, node_manager);
        Some(Self { notifier })
    }

    /// Enqueues one notification for batches from the same table.
    /// Never waits for queue capacity or delivery and never changes the write result.
    pub fn notify<'a>(
        &self,
        table_info: TableInfoRef,
        batches: impl IntoIterator<Item = &'a RecordBatch>,
    ) {
        let Some(timestamp_column) = table_info.meta.schema.timestamp_column() else {
            return;
        };
        let mut timestamps = Vec::new();
        for batch in batches {
            match extract_timestamps(batch, &timestamp_column.name) {
                Ok(batch_timestamps) => timestamps.extend(batch_timestamps),
                Err(error) => {
                    error!(error; "Failed to extract flow notification timestamps, table_id: {}", table_info.table_id());
                }
            }
        }
        if timestamps.is_empty() {
            return;
        }
        self.enqueue(FlowNotification {
            table_id: table_info.table_id(),
            timestamps,
        });
    }

    fn enqueue(&self, notification: FlowNotification) {
        self.notifier.try_notify(notification);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::helper::ColumnDataTypeWrapper;
    use api::region::RegionResponse;
    use api::v1::helper::{tag_column_schema, time_index_column_schema};
    use api::v1::region::{RegionRequest, bulk_insert_request, region_request};
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row, Rows, Value};
    use arrow::array::{
        ArrayRef, Int32Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use catalog::memory::MemoryCatalogManager;
    use common_grpc::flight::FlightDecoder;
    use common_meta::error::Result as MetaResult;
    use common_meta::peer::Peer;
    use common_meta::test_util::{MockDatanodeHandler, MockDatanodeManager};
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use operator::insert::Inserter;
    use operator::req_convert::insert::rows_to_record_batch;
    use operator::test_util::{
        create_partition_rule_manager, new_test_table_info, prepare_mocked_backend,
    };
    use session::context::QueryContext;
    use tokio::sync::{Semaphore, oneshot};

    use crate::batcher::flow_notifier::FlowNotifier as NotificationQueue;
    use crate::batcher::table::batch::{Batch, flush_batch};
    use crate::batcher::table::flow_notifier::{
        FLOW_NOTIFICATION_DROPPED, FlowNotification, FlowNotifier,
    };
    use crate::batcher::table::metrics;
    use crate::batcher::table::pending_batch::PendingBatch;
    use crate::batcher::test_util::mock_table_flownode_cache;

    #[tokio::test]
    async fn test_flush_coalesces_notifications_for_successful_submissions() {
        #[derive(Clone)]
        struct PartialHandler;

        #[async_trait::async_trait]
        impl MockDatanodeHandler for PartialHandler {
            async fn handle(&self, _: &Peer, request: RegionRequest) -> MetaResult<RegionResponse> {
                let Some(region_request::Body::BulkInsert(request)) = request.body else {
                    panic!("expected bulk insert")
                };
                let Some(bulk_insert_request::Body::ArrowIpc(ipc)) = request.body else {
                    panic!("expected Arrow IPC")
                };
                let batch = FlightDecoder::try_from_schema_bytes(&ipc.schema)
                    .unwrap()
                    .try_decode_record_batch(&ipc.data_header, &ipc.payload)
                    .unwrap();
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                if values.values().contains(&20) {
                    return common_meta::error::UnexpectedSnafu {
                        err_msg: "lost acknowledgement",
                    }
                    .fail();
                }
                Ok(RegionResponse::new(batch.num_rows()))
            }

            async fn handle_query(
                &self,
                _: &Peer,
                _: QueryRequest,
            ) -> MetaResult<SendableRecordBatchStream> {
                unreachable!()
            }
        }

        let partitions = create_partition_rule_manager(prepare_mocked_backend().await).await;
        let inserter = Arc::new(Inserter::new(
            MemoryCatalogManager::new(),
            partitions,
            Arc::new(MockDatanodeManager::new(PartialHandler)),
            mock_table_flownode_cache(1, vec![]).await,
            true,
        ));
        let table = Arc::new(new_test_table_info(1, "table_1", [1, 2, 3].into_iter()));
        let cases = [
            (
                vec![vec![1], vec![2]],
                vec![(1, false), (1, false)],
                vec![60_000, 120_000],
                true,
            ),
            (
                vec![vec![1, 20], vec![2], vec![3]],
                vec![(1, true), (1, false), (1, false)],
                vec![120_000, 180_000],
                false,
            ),
            (
                vec![vec![20], vec![20]],
                vec![(0, true), (0, true)],
                vec![],
                false,
            ),
            (
                vec![vec![1]; 100],
                vec![(1, false); 100],
                vec![60_000; 100],
                true,
            ),
        ];
        let ctx = Arc::new(QueryContext::arc().with_confirmed_writes());
        let ingested = operator::metrics::DIST_INGEST_ROW_COUNT
            .with_label_values(&[ctx.get_db_string().as_str()]);
        for (inputs, expected_progress, expected_timestamps, successful_flush) in cases {
            let permits = Arc::new(Semaphore::new(inputs.len()));
            let mut submissions = Vec::new();
            let mut completions = Vec::new();
            let mut total_rows = 0;
            for values in inputs {
                let rows = Rows {
                    schema: vec![
                        tag_column_schema("a", ColumnDataType::Int32),
                        time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
                    ],
                    rows: values
                        .into_iter()
                        .map(|value| Row {
                            values: vec![
                                Value {
                                    value_data: Some(ValueData::I32Value(value)),
                                },
                                Value {
                                    value_data: Some(ValueData::TimestampMillisecondValue(
                                        i64::from(value) * 60_000,
                                    )),
                                },
                            ],
                        })
                        .collect(),
                };
                total_rows += rows.rows.len();
                let (response_tx, response_rx) = oneshot::channel();
                submissions.push(PendingBatch {
                    table_info: table.clone(),
                    batch: rows_to_record_batch(&rows, &table).unwrap(),
                    ctx: ctx.clone(),
                    response_tx,
                    _permit: Arc::new(permits.clone().acquire_owned().await.unwrap()),
                });
                completions.push(response_rx);
            }
            // Keep a one-slot queue undrained so per-submission notifications lose timestamps.
            let (notifier, mut notifications) =
                NotificationQueue::try_new(1, FLOW_NOTIFICATION_DROPPED.clone()).unwrap();
            let successes = metrics::FLUSH_TOTAL.get();
            let failures = metrics::FLUSH_FAILURES.get();
            let samples = metrics::FLUSH_ROWS.get_sample_count();
            let sum = metrics::FLUSH_ROWS.get_sample_sum();
            let dropped = metrics::FLUSH_DROPPED_ROWS.get();
            let written = ingested.get();
            flush_batch(
                Batch {
                    submissions,
                    total_rows,
                },
                inserter.clone(),
                FlowNotifier { notifier },
            )
            .await;

            let expected_written: usize = expected_progress.iter().map(|(rows, _)| rows).sum();
            assert_eq!(
                metrics::FLUSH_TOTAL.get(),
                successes + u64::from(successful_flush)
            );
            assert_eq!(
                metrics::FLUSH_FAILURES.get(),
                failures + u64::from(!successful_flush)
            );
            assert_eq!(
                metrics::FLUSH_ROWS.get_sample_count(),
                samples + u64::from(successful_flush)
            );
            assert_eq!(
                metrics::FLUSH_ROWS.get_sample_sum(),
                sum + if successful_flush {
                    total_rows as f64
                } else {
                    0.0
                }
            );
            assert_eq!(
                metrics::FLUSH_DROPPED_ROWS.get(),
                dropped + (total_rows - expected_written) as u64
            );
            assert_eq!(ingested.get(), written + expected_written as u64);
            for (completion, (affected_rows, failed)) in
                completions.into_iter().zip(expected_progress)
            {
                let progress = completion.await.unwrap();
                assert_eq!(progress.affected_rows, affected_rows);
                assert_eq!(progress.error.is_some(), failed);
            }
            if !expected_timestamps.is_empty() {
                let notification = notifications.try_recv().unwrap();
                assert_eq!(notification.table_id, 1);
                assert_eq!(notification.timestamps, expected_timestamps);
            }
            assert!(notifications.try_recv().is_err());
        }
    }

    #[test]
    fn test_full_and_closed_queues_do_not_block() {
        let (notifier, receiver) =
            NotificationQueue::try_new(1, FLOW_NOTIFICATION_DROPPED.clone()).unwrap();
        let notifier = FlowNotifier { notifier };
        let full = FLOW_NOTIFICATION_DROPPED.with_label_values(&["full"]);
        let closed = FLOW_NOTIFICATION_DROPPED.with_label_values(&["closed"]);
        let before_full = full.get();
        let before_closed = closed.get();
        for _ in 0..2 {
            notifier.enqueue(FlowNotification {
                table_id: 1,
                timestamps: vec![42],
            });
        }
        assert!(full.get() > before_full);
        drop(receiver);
        notifier.enqueue(FlowNotification {
            table_id: 1,
            timestamps: vec![42],
        });
        assert!(closed.get() > before_closed);
    }
    #[test]
    fn test_notification_preserves_native_timestamp_units() {
        let cases: Vec<(ColumnDataType, ArrayRef)> = vec![
            (
                ColumnDataType::TimestampSecond,
                Arc::new(TimestampSecondArray::from(vec![Some(42), None])),
            ),
            (
                ColumnDataType::TimestampMillisecond,
                Arc::new(TimestampMillisecondArray::from(vec![Some(42), None])),
            ),
            (
                ColumnDataType::TimestampMicrosecond,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(42), None])),
            ),
            (
                ColumnDataType::TimestampNanosecond,
                Arc::new(TimestampNanosecondArray::from(vec![Some(42), None])),
            ),
        ];
        for (datatype, array) in cases {
            let mut table = new_test_table_info(1, "test", [0].into_iter());
            let column = ColumnSchema::new(
                "ts",
                ConcreteDataType::from(ColumnDataTypeWrapper::new(datatype, None)),
                true,
            )
            .with_time_index(true);
            table.meta.schema = Arc::new(Schema::new(vec![column]));
            let batch = RecordBatch::try_new(table.meta.schema.arrow_schema().clone(), vec![array])
                .unwrap();
            let (notifier, mut receiver) =
                NotificationQueue::try_new(1, FLOW_NOTIFICATION_DROPPED.clone()).unwrap();
            FlowNotifier { notifier }.notify(Arc::new(table), [&batch, &batch]);
            let notification = receiver.try_recv().unwrap();
            assert_eq!(notification.table_id, 1);
            assert_eq!(notification.timestamps, vec![42, 42]);
            assert!(receiver.try_recv().is_err());
        }
    }
}
