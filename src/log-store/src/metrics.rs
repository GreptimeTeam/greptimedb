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
/// Kafka topic label.
pub const TOPIC_LABEL: &str = "topic";
/// Kafka partition label.
pub const PARTITION_LABEL: &str = "partition";

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

    /// Timer of operations on a logstore.
    pub static ref METRIC_LOGSTORE_OP_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_logstore_op_elapsed",
        "logstore operation elapsed",
        &[LOGSTORE_LABEL, OPTYPE_LABEL],
        vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 20.0, 30.0,
            45.0, 60.0,
        ],
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

    pub static ref METRIC_KAFKA_CLIENT_BYTES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_logstore_kafka_client_bytes_total",
        "kafka logstore's bytes traffic total",
        &[LOGSTORE_LABEL, PARTITION_LABEL],
    )
    .unwrap();
    pub static ref METRIC_KAFKA_CLIENT_TRAFFIC_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_logstore_kafka_client_traffic_total",
        "kafka logstore's request count traffic total",
        &[LOGSTORE_LABEL, PARTITION_LABEL],
    )
    .unwrap();
    pub static ref METRIC_KAFKA_CLIENT_PRODUCE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_logstore_kafka_client_produce_elapsed",
        "kafka logstore produce operation elapsed",
        &[LOGSTORE_LABEL, PARTITION_LABEL],
    )
    .unwrap();

    /// Counter of segments a read of the object store logstore skipped because
    /// they did not decode.
    pub static ref METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL: IntCounter = register_int_counter!(
        "greptime_logstore_object_store_wal_skipped_segments_total",
        "object store logstore skipped corrupted segments total",
    )
    .unwrap();
    /// Counter of objects the object store logstore deleted because no
    /// region needs them any more.
    pub static ref METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL: IntCounter = register_int_counter!(
        "greptime_logstore_object_store_wal_deleted_objects_total",
        "object store logstore deleted objects total",
    )
    .unwrap();
    /// Counter of object deletes of the object store logstore that failed and
    /// are attempted again at the next collection.
    pub static ref METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL: IntCounter = register_int_counter!(
        "greptime_logstore_object_store_wal_failed_deletes_total",
        "object store logstore failed object deletes total",
    )
    .unwrap();

    /// How long an object of the object store logstore takes to become durable
    /// once its batch is sealed, sampled for every conditional create that
    /// succeeds, an identical retry included. The time runs from the seal, so
    /// it includes any wait for a create slot and, in the `enqueued` mode,
    /// every repeated attempt and the delay before it.
    ///
    /// A create is a few milliseconds on a local object store and tens of
    /// milliseconds on a cloud one, so the buckets start at 1ms and reach 8s.
    pub static ref METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS: Histogram = register_histogram!(
        "greptime_logstore_object_store_wal_seal_to_durable_seconds",
        "object store logstore seconds from sealing a batch to the success of its conditional create, slot waits and retries included",
        exponential_buckets(0.001, 2.0, 14).unwrap(),
    )
    .unwrap();

    /// How long an append of the object store logstore waits for its
    /// acknowledgement in the `durable` mode, which is the write latency a
    /// caller sees.
    ///
    /// The wait is one seal plus every earlier object plus one create, so with
    /// the default 100ms flush interval it sits in the tens of milliseconds;
    /// the buckets straddle that and leave room for a stalled uploader.
    pub static ref METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS: Histogram = register_histogram!(
        "greptime_logstore_object_store_wal_append_ack_seconds",
        "object store logstore seconds an append waits for its acknowledgement in the durable mode",
        exponential_buckets(0.002, 2.0, 14).unwrap(),
    )
    .unwrap();

    /// How large the objects of the object store logstore are, which says
    /// whether batches seal on size or on the flush interval.
    ///
    /// The buckets are powers of two from 1KiB, the size of a timer-sealed
    /// batch of one small entry, to 16MiB, twice the 8MiB default batch size,
    /// which is a threshold rather than an upper bound.
    pub static ref METRIC_OBJECT_STORE_WAL_OBJECT_BYTES: Histogram = register_histogram!(
        "greptime_logstore_object_store_wal_object_bytes",
        "object store logstore bytes of each object sealed",
        exponential_buckets(1024.0, 2.0, 15).unwrap(),
    )
    .unwrap();

    /// How many entries the objects of the object store logstore hold, which
    /// says how many writes one request amortises.
    ///
    /// The largest finite bucket is 2^20, the position limit of one region in
    /// one object. An object holds the entries of many regions, so its total
    /// can pass that; such an object falls in the overflow bucket.
    pub static ref METRIC_OBJECT_STORE_WAL_OBJECT_ENTRIES: Histogram = register_histogram!(
        "greptime_logstore_object_store_wal_object_entries",
        "object store logstore entries of each object sealed",
        exponential_buckets(1.0, 4.0, 11).unwrap(),
    )
    .unwrap();

    /// Counter of conditional creates of the object store logstore that
    /// succeeded, an identical retry that found its own object included. A
    /// create that failed is counted in the create failure or conflict counter
    /// instead.
    pub static ref METRIC_OBJECT_STORE_WAL_CREATED_OBJECTS_TOTAL: IntCounter = register_int_counter!(
        "greptime_logstore_object_store_wal_created_objects_total",
        "object store logstore conditional creates that succeeded, identical retries included",
    )
    .unwrap();

    /// Number of objects the object store logstore has indexed, which is what
    /// a recovery of the prefix would have to read.
    pub static ref METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS: IntGauge = register_int_gauge!(
        "greptime_logstore_object_store_wal_indexed_objects",
        "object store logstore objects currently indexed",
    )
    .unwrap();

    /// Bytes of the objects the object store logstore has indexed, which is
    /// what the WAL occupies under its prefix.
    pub static ref METRIC_OBJECT_STORE_WAL_INDEXED_BYTES: IntGauge = register_int_gauge!(
        "greptime_logstore_object_store_wal_indexed_bytes",
        "object store logstore bytes currently indexed",
    )
    .unwrap();

    /// How long a recovery of the object store logstore that succeeded took to
    /// rebuild its catalog, which is the part of a restart the WAL is
    /// responsible for. A recovery that was abandoned records nothing.
    ///
    /// Recovery reads a few small ranges per object, so the buckets run from
    /// 10ms, an empty prefix, to minutes on a prefix with many objects.
    pub static ref METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS: Histogram = register_histogram!(
        "greptime_logstore_object_store_wal_recovery_seconds",
        "object store logstore seconds spent by a recovery that succeeded",
        exponential_buckets(0.01, 3.0, 10).unwrap(),
    )
    .unwrap();

    /// Counter of objects a recovery of the object store logstore that
    /// succeeded read footers from, which is what its time is spent on. It
    /// counts the objects of the same recoveries the timer above measures, so
    /// an abandoned one contributes to neither.
    pub static ref METRIC_OBJECT_STORE_WAL_RECOVERED_OBJECTS_TOTAL: IntCounter = register_int_counter!(
        "greptime_logstore_object_store_wal_recovered_objects_total",
        "object store logstore objects read by a recovery that succeeded total",
    )
    .unwrap();

    /// How long a read stream of the object store logstore took to serve the
    /// replay of one region, recorded once the stream has been read to its
    /// end; one that fails or is dropped part way records nothing.
    ///
    /// A read fetches one segment per object of the region, so the buckets run
    /// from 5ms, a region with nothing to replay, to minutes.
    pub static ref METRIC_OBJECT_STORE_WAL_READ_SECONDS: Histogram = register_histogram!(
        "greptime_logstore_object_store_wal_read_seconds",
        "object store logstore seconds spent serving a region read that ran to its end",
        exponential_buckets(0.005, 3.0, 10).unwrap(),
    )
    .unwrap();

    /// Counter of conditional creates of the object store logstore that found
    /// an object with different content. In the `durable` mode an object of an
    /// earlier epoch fails the batch like a transient failure; any other
    /// conflict poisons the store.
    pub static ref METRIC_OBJECT_STORE_WAL_CREATE_CONFLICTS_TOTAL: IntCounter = register_int_counter!(
        "greptime_logstore_object_store_wal_create_conflicts_total",
        "object store logstore conflicting conditional creates total",
    )
    .unwrap();

    /// Counter of conditional creates of the object store logstore the object
    /// store did not confirm, which fail the appends behind them in the
    /// `durable` mode and are repeated in the `enqueued` mode.
    pub static ref METRIC_OBJECT_STORE_WAL_CREATE_FAILURES_TOTAL: IntCounter = register_int_counter!(
        "greptime_logstore_object_store_wal_create_failures_total",
        "object store logstore transient conditional create failures total",
    )
    .unwrap();

    /// Counter of object store logstores that hit an error they cannot recover
    /// from, after which every operation of the store fails until it reopens.
    pub static ref METRIC_OBJECT_STORE_WAL_POISONED_TOTAL: IntCounter = register_int_counter!(
        "greptime_logstore_object_store_wal_poisoned_total",
        "object store logstores poisoned total",
    )
    .unwrap();

    /// Counter of appends the object store logstore held back because the
    /// unpersisted backlog reached a threshold in the `enqueued` mode, which
    /// says the write rate is above what the uploads sustain.
    pub static ref METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL: IntCounter = register_int_counter!(
        "greptime_logstore_object_store_wal_stalled_appends_total",
        "object store logstore appends stalled by the backlog thresholds total",
    )
    .unwrap();

    /// How long such an append stays held back, until it is admitted or until
    /// it is refused, a stop, a terminal error or the teardown of the store
    /// releasing it included, which is the latency the `enqueued` mode pays
    /// once the backlog is at a threshold.
    ///
    /// A stalled append waits for one upload to complete, so the buckets run
    /// from 5ms to half a minute of a struggling object store.
    pub static ref METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS: Histogram = register_histogram!(
        "greptime_logstore_object_store_wal_stalled_append_seconds",
        "object store logstore seconds a stalled append is held back before it is admitted or refused",
        exponential_buckets(0.005, 3.0, 9).unwrap(),
    )
    .unwrap();
}
