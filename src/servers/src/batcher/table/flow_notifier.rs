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

    /// Never waits for queue capacity or delivery and never changes the write result.
    pub fn notify(&self, table_info: TableInfoRef, batch: &RecordBatch) {
        let Some(timestamp_column) = table_info.meta.schema.timestamp_column() else {
            return;
        };
        let timestamps = match extract_timestamps(batch, &timestamp_column.name) {
            Ok(timestamps) => timestamps,
            Err(error) => {
                error!(error; "Failed to extract flow notification timestamps, table_id: {}", table_info.table_id());
                return;
            }
        };
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
    use api::v1::ColumnDataType;
    use arrow::array::{
        ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use operator::test_util::new_test_table_info;

    use crate::batcher::flow_notifier::FlowNotifier as NotificationQueue;
    use crate::batcher::table::flow_notifier::{
        FLOW_NOTIFICATION_DROPPED, FlowNotification, FlowNotifier,
    };

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
            FlowNotifier { notifier }.notify(Arc::new(table), &batch);
            let notification = receiver.try_recv().unwrap();
            assert_eq!(notification.table_id, 1);
            assert_eq!(notification.timestamps, vec![42]);
        }
    }
}
