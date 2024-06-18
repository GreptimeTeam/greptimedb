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

use common_error::ext::ErrorExt;
use common_macro::stack_trace_debug;
use common_runtime::error::Error as RuntimeError;
use serde_json::error::Error as JsonError;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;

use crate::kafka::producer::ProduceRequest;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Invalid provider type, expected: {}, actual: {}", expected, actual))]
    InvalidProvider {
        #[snafu(implicit)]
        location: Location,
        expected: String,
        actual: String,
    },

    #[snafu(display("Failed to start log store gc task"))]
    StartGcTask {
        #[snafu(implicit)]
        location: Location,
        source: RuntimeError,
    },

    #[snafu(display("Failed to stop log store gc task"))]
    StopGcTask {
        #[snafu(implicit)]
        location: Location,
        source: RuntimeError,
    },

    #[snafu(display("Failed to add entry to LogBatch"))]
    AddEntryLogBatch {
        #[snafu(source)]
        error: raft_engine::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to perform raft-engine operation"))]
    RaftEngine {
        #[snafu(source)]
        error: raft_engine::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to perform IO on path: {}", path))]
    Io {
        path: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Log store not started yet"))]
    IllegalState {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Namespace is illegal: {}", ns))]
    IllegalNamespace {
        ns: u64,
        #[snafu(implicit)]
        location: Location,
    },

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
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to build a Kafka client, broker endpoints: {:?}",
        broker_endpoints
    ))]
    BuildClient {
        broker_endpoints: Vec<String>,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to found client"))]
    ClientNotFount {
        #[snafu(implicit)]
        location: Location,
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
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display(
        "Failed to get a Kafka topic client, topic: {}, source: {}",
        topic,
        error
    ))]
    GetClient {
        topic: String,
        #[snafu(implicit)]
        location: Location,
        error: String,
    },

    #[snafu(display("Missing required key in a record"))]
    MissingKey {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing required value in a record"))]
    MissingValue {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot build a record from empty entries"))]
    EmptyEntries {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to produce records to Kafka, topic: {}, size: {}", topic, size))]
    ProduceRecord {
        topic: String,
        size: usize,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::producer::Error,
    },

    #[snafu(display("Failed to produce batch records to Kafka"))]
    BatchProduce {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to read a record from Kafka, topic: {}", topic))]
    ConsumeRecord {
        topic: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to get the latest offset, topic: {}", topic))]
    GetOffset {
        topic: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to do a cast"))]
    Cast {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to encode object into json"))]
    EncodeJson {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: JsonError,
    },

    #[snafu(display("Failed to decode object from json"))]
    DecodeJson {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: JsonError,
    },

    #[snafu(display("The record sequence is not legal, error: {}", error))]
    IllegalSequence {
        #[snafu(implicit)]
        location: Location,
        error: String,
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

    #[snafu(display("Failed to send produce request"))]
    SendProduceRequest {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: tokio::sync::mpsc::error::SendError<ProduceRequest>,
    },

    #[snafu(display("Failed to send produce request"))]
    WaitProduceResultReceiver {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: tokio::sync::oneshot::error::RecvError,
    },

    #[snafu(display(
        "The length of meta if exceeded the limit: {}, actual: {}",
        limit,
        actual
    ))]
    MetaLengthExceededLimit {
        #[snafu(implicit)]
        location: Location,
        limit: usize,
        actual: usize,
    },

    #[snafu(display("No max value"))]
    NoMaxValue {
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
