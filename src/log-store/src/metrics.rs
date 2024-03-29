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

use lazy_static::lazy_static;
use prometheus::*;

/// Logstore label.
pub const LOGSTORE_LABEL: &str = "logstore";
/// Operation type label.
pub const OPTYPE_LABEL: &str = "optype";

lazy_static! {
    /// Counters of bytes of each operation on a logstore.
    pub static ref METRIC_LOGSTORE_OP_BYTES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_logstore_op_bytes_total",
        "logstore operation bytes total",
        &[LOGSTORE_LABEL, OPTYPE_LABEL],
    )
    .unwrap();
    /// Counter of bytes of the append_batch operation on the kafka logstore.
    pub static ref METRIC_KAFKA_APPEND_BATCH_BYTES_TOTAL: IntCounter = METRIC_LOGSTORE_OP_BYTES_TOTAL.with_label_values(
        &["kafka", "append_batch"],
    );
    /// Counter of bytes of the read operation on the kafka logstore.
    pub static ref METRIC_KAFKA_READ_BYTES_TOTAL: IntCounter = METRIC_LOGSTORE_OP_BYTES_TOTAL.with_label_values(
        &["kafka", "read"],
    );
    /// Counter of bytes of the append_batch operation on the raft-engine logstore.
    pub static ref METRIC_RAFT_ENGINE_APPEND_BATCH_BYTES_TOTAL: IntCounter = METRIC_LOGSTORE_OP_BYTES_TOTAL.with_label_values(
        &["raft-engine", "append_batch"],
    );
    /// Counter of bytes of the read operation on the raft-engine logstore.
    pub static ref METRIC_RAFT_ENGINE_READ_BYTES_TOTAL: IntCounter = METRIC_LOGSTORE_OP_BYTES_TOTAL.with_label_values(
        &["raft-engine", "read"],
    );

    /// Counter of bytes of the records read by the kafka logstore.
    pub static ref METRIC_KAFKA_READ_RECORD_BYTES_TOTAL: IntCounter = register_int_counter!(
        "greptime_kafka_read_record_bytes_total",
        "kafka read record bytes total"
    ).unwrap();

    /// Counter of the numbers of the records produced by the kafka logstore.
    pub static ref METRIC_KAFKA_PRODUCE_RECORD_COUNTS: IntCounter = register_int_counter!(
        "greptime_kafka_produce_record_counts",
        "kafka produce record counts",
    ).unwrap();

    /// Counter of bytes of the records produced by the kafka logstore.
    pub static ref METRIC_KAFKA_PRODUCE_RECORD_BYTES_TOTAL: IntCounter = register_int_counter!(
        "greptime_kafka_produce_record_bytes_total",
        "kafka produce record bytes total"
    ).unwrap();

    /// Counters of calls of each operation on a logstore.
    pub static ref METRIC_LOGSTORE_OP_CALLS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_logstore_op_calls_total",
        "logstore operation calls total",
        &[LOGSTORE_LABEL, OPTYPE_LABEL],
    )
    .unwrap();
    /// Counter of calls of the append_batch operation on the kafka logstore.
    pub static ref METRIC_KAFKA_APPEND_BATCH_CALLS_TOTAL: IntCounter = METRIC_LOGSTORE_OP_CALLS_TOTAL.with_label_values(
        &["kafka", "append_batch"],
    );
    /// Counter of calls of the read operation on the kafka logstore.
    pub static ref METRIC_KAFKA_READ_CALLS_TOTAL: IntCounter = METRIC_LOGSTORE_OP_CALLS_TOTAL.with_label_values(
        &["kafka", "read"],
    );
    /// Counter of calls of the append_batch operation on the raft-engine logstore.
    pub static ref METRIC_RAFT_ENGINE_APPEND_BATCH_CALLS_TOTAL: IntCounter = METRIC_LOGSTORE_OP_CALLS_TOTAL.with_label_values(
        &["raft-engine", "append_batch"],
    );
    /// Counter of calls of the read operation on the raft-engine logstore.
    pub static ref METRIC_RAFT_ENGINE_READ_CALLS_TOTAL: IntCounter = METRIC_LOGSTORE_OP_CALLS_TOTAL.with_label_values(
        &["raft-engine", "read"],
    );

    /// Timer of operations on a logstore.
    pub static ref METRIC_LOGSTORE_OP_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_logstore_op_elapsed",
        "logstore operation elapsed",
        &[LOGSTORE_LABEL, OPTYPE_LABEL],
    )
    .unwrap();
    /// Timer of the append_batch operation on the kafka logstore.
    pub static ref METRIC_KAFKA_APPEND_BATCH_ELAPSED: Histogram = METRIC_LOGSTORE_OP_ELAPSED.with_label_values(&["kafka", "append_batch"]);
    /// Timer of the append_batch operation on the kafka logstore.
    /// This timer only measures the duration of the read operation, not measures the total duration of replay.
    pub static ref METRIC_KAFKA_READ_ELAPSED: Histogram = METRIC_LOGSTORE_OP_ELAPSED.with_label_values(&["kafka", "read"]);
    /// Timer of the append_batch operation on the raft-engine logstore.
    pub static ref METRIC_RAFT_ENGINE_APPEND_BATCH_ELAPSED: Histogram = METRIC_LOGSTORE_OP_ELAPSED.with_label_values(&["raft-engine", "append_batch"]);
    /// Timer of the append_batch operation on the raft-engine logstore.
    /// This timer only measures the duration of the read operation, not measures the total duration of replay.
    pub static ref METRIC_RAFT_ENGINE_READ_ELAPSED: Histogram = METRIC_LOGSTORE_OP_ELAPSED.with_label_values(&["raft-engine", "read"]);
}
