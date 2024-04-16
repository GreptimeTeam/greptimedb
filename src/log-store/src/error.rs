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

use common_error::ext::ErrorExt;
use common_macro::stack_trace_debug;
use common_runtime::error::Error as RuntimeError;
use serde_json::error::Error as JsonError;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;

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

    #[snafu(display("Failed to perform IO on path: {}", path))]
    Io {
        path: String,
        #[snafu(source)]
        error: std::io::Error,
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
        "Attempt to append discontinuous log entry, region: {}, last index: {}, attempt index: {}",
        region_id,
        last_index,
        attempt_index
    ))]
    DiscontinuousLogIndex {
        region_id: RegionId,
        last_index: u64,
        attempt_index: u64,
    },

    #[snafu(display(
        "Failed to build a Kafka client, broker endpoints: {:?}",
        broker_endpoints
    ))]
    BuildClient {
        broker_endpoints: Vec<String>,
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to resolve Kafka broker endpoint."))]
    ResolveKafkaEndpoint { source: common_wal::error::Error },

    #[snafu(display(
        "Failed to build a Kafka partition client, topic: {}, partition: {}",
        topic,
        partition
    ))]
    BuildPartitionClient {
        topic: String,
        partition: i32,
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to produce records to Kafka, topic: {}", topic))]
    ProduceRecord {
        topic: String,
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to pull a record from Kafka, topic: {}", topic))]
    PullRecord {
        topic: String,
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to produce entries, source: {}", error,))]
    ProduceEntries { location: Location, error: String },

    #[snafu(display("Failed to get the latest offset, topic: {}", topic))]
    GetLatestOffset {
        topic: String,
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to encode object into json"))]
    EncodeJson {
        location: Location,
        #[snafu(source)]
        error: JsonError,
    },

    #[snafu(display("Failed to decode object from json"))]
    DecodeJson {
        location: Location,
        #[snafu(source)]
        error: JsonError,
    },

    #[snafu(display(
        "The entry is too large, entry size: {}, buffer capacity: {}",
        entry_size,
        capacity
    ))]
    EntryTooLarge {
        entry_size: usize,
        capacity: usize,
        location: Location,
    },

    #[snafu(display("Failed to join task"))]
    JoinTask {
        location: Location,
        #[snafu(source)]
        error: tokio::task::JoinError,
    },

    #[snafu(display("Failed to cast a numeric timestamp to date time"))]
    CastTimestamp { location: Location },

    #[snafu(display("Failed to cast an entry id to Kafka offset"))]
    CastEntryId {
        location: Location,
        #[snafu(source)]
        error: TryFromIntError,
    },

    #[snafu(display(
        "Found a commit record with nothing to be committed, timestamp = {}",
        timestamp
    ))]
    CommitNothing { timestamp: i64, location: Location },
}

impl ErrorExt for Error {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
