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

use std::any::Any;
use std::num::TryFromIntError;

use common_config::wal::kafka::KafkaTopic;
use common_error::ext::ErrorExt;
use common_macro::stack_trace_debug;
use common_runtime::error::Error as RuntimeError;
use rskafka::client::error::Error as RsKafkaError;
use snafu::{Location, Snafu};
use store_api::logstore::entry::Id as EntryId;
use store_api::logstore::namespace::Id as NamespaceId;
use store_api::logstore::RegionWalOptions;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to start log store gc task"))]
    StartGcTask {
        location: Location,
        source: RuntimeError,
    },

    #[snafu(display("Failed to stop log store gc task"))]
    StopGcTask {
        location: Location,
        source: RuntimeError,
    },

    #[snafu(display("Failed to add entry to LogBatch"))]
    AddEntryLogBatch {
        #[snafu(source)]
        error: raft_engine::Error,
        location: Location,
    },

    #[snafu(display("Failed to perform raft-engine operation"))]
    RaftEngine {
        #[snafu(source)]
        error: raft_engine::Error,
        location: Location,
    },

    #[snafu(display("Log store not started yet"))]
    IllegalState { location: Location },

    #[snafu(display("Namespace is illegal: {}", ns))]
    IllegalNamespace { ns: u64, location: Location },

    #[snafu(display(
        "Failed to fetch entries from namespace: {}, start: {}, end: {}, max size: {}",
        ns,
        start,
        end,
        max_size,
    ))]
    FetchEntry {
        ns: u64,
        start: u64,
        end: u64,
        max_size: usize,
        #[snafu(source)]
        error: raft_engine::Error,
        location: Location,
    },

    #[snafu(display(
        "Cannot override compacted entry, namespace: {}, first index: {}, attempt index: {}",
        namespace,
        first_index,
        attempt_index
    ))]
    OverrideCompactedEntry {
        namespace: u64,
        first_index: u64,
        attempt_index: u64,
        location: Location,
    },

    #[snafu(display(
        "Failed to build a rskafka client, broker endpoints: {:?}",
        broker_endpoints
    ))]
    BuildKafkaClient {
        broker_endpoints: Vec<String>,
        location: Location,
        #[snafu(source)]
        error: RsKafkaError,
    },

    #[snafu(display(
        "Failed to build a rskafka partition client, topic: {}, partition: {}",
        topic,
        partition
    ))]
    BuildKafkaPartitionClient {
        topic: String,
        partition: i32,
        location: Location,
        #[snafu(source)]
        error: RsKafkaError,
    },

    #[snafu(display("Failed to get a Kafka topic client, topic: {}", topic))]
    GetKafkaTopicClient {
        topic: KafkaTopic,
        location: Location,
    },

    #[snafu(display("Failed to write entries to Kafka, topic: {}", topic))]
    WriteEntriesToKafka {
        topic: KafkaTopic,
        location: Location,
        #[snafu(source)]
        error: rskafka::client::producer::Error,
    },

    #[snafu(display("Returned Kafka offsets are empty"))]
    EmptyKafkaOffsets { location: Location },

    #[snafu(display(
        "Failed to convert an rskafka offset to entry offset, rskafka_offset: {}",
        rskafka_offset
    ))]
    ConvertRsKafkaOffsetToEntryOffset {
        rskafka_offset: i64,
        location: Location,
        #[snafu(source)]
        error: TryFromIntError,
    },

    #[snafu(display(
        "Missing required entry offset, entry_id: {}, region_id: {}, topic: {}",
        entry_id,
        region_id,
        topic
    ))]
    MissingEntryOffset {
        entry_id: EntryId,
        region_id: u64,
        topic: KafkaTopic,
        location: Location,
    },

    #[snafu(display(
        "Missing required kafka topic, ns_id: {}, region_wal_options: {:?}",
        ns_id,
        region_wal_options,
    ))]
    MissingKafkaTopic {
        ns_id: NamespaceId,
        region_wal_options: RegionWalOptions,
        location: Location,
    },

    #[snafu(display("Failed to serialize an entry meta"))]
    SerEntryMeta {
        location: Location,
        #[snafu(source)]
        error: serde_json::Error,
    },

    #[snafu(display("Missing required value in a record, record: {:?}", record))]
    MissingRecordValue {
        record: rskafka::record::Record,
        location: Location,
    },

    #[snafu(display("Missing required entry meta in a record header, record: {:?}", record))]
    MissingEntryMeta {
        record: rskafka::record::Record,
        location: Location,
    },

    #[snafu(display(
        "Failed to deserialize an entry meta from the record header, record: {:?}",
        record
    ))]
    DeserEntryMeta {
        record: rskafka::record::Record,
        location: Location,
        #[snafu(source)]
        error: serde_json::Error,
    },

    #[snafu(display(
        "Failed to convert an entry id into a Kafka offset, entry id: {}",
        entry_id
    ))]
    ConvertEntryIdToOffset {
        entry_id: EntryId,
        location: Location,
    },

    #[snafu(display(
        "Failed to read a record from Kafka, start offset {}, topic: {}, region id: {}",
        start_offset,
        topic,
        region_id,
    ))]
    ReadRecordFromKafka {
        start_offset: i64,
        topic: String,
        region_id: u64,
        location: Location,
        #[snafu(source)]
        error: RsKafkaError,
    },
}

impl ErrorExt for Error {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
