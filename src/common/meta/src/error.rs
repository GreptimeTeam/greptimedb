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

use std::str::Utf8Error;
use std::sync::Arc;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_wal::options::WalOptions;
use serde_json::error::Error as JsonError;
use snafu::{Location, Snafu};
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

use crate::peer::Peer;
use crate::DatanodeId;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Empty key is not allowed"))]
    EmptyKey {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Another procedure is operating the region: {} on peer: {}",
        region_id,
        peer_id
    ))]
    RegionOperatingRace {
        #[snafu(implicit)]
        location: Location,
        peer_id: DatanodeId,
        region_id: RegionId,
    },

    #[snafu(display("Invalid result with a txn response: {}", err_msg))]
    InvalidTxnResult {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid engine type: {}", engine_type))]
    InvalidEngineType {
        engine_type: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to connect to Etcd"))]
    ConnectEtcd {
        #[snafu(source)]
        error: etcd_client::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to execute via Etcd"))]
    EtcdFailed {
        #[snafu(source)]
        error: etcd_client::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to execute {} txn operations via Etcd", max_operations))]
    EtcdTxnFailed {
        max_operations: usize,
        #[snafu(source)]
        error: etcd_client::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get sequence: {}", err_msg))]
    NextSequence {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Sequence out of range: {}, start={}, step={}", name, start, step))]
    SequenceOutOfRange {
        name: String,
        start: u64,
        step: u64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected sequence value: {}", err_msg))]
    UnexpectedSequenceValue {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table info not found: {}", table))]
    TableInfoNotFound {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to register procedure loader, type name: {}", type_name))]
    RegisterProcedureLoader {
        type_name: String,
        #[snafu(implicit)]
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to submit procedure"))]
    SubmitProcedure {
        #[snafu(implicit)]
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Failed to query procedure"))]
    QueryProcedure {
        #[snafu(implicit)]
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Procedure not found: {pid}"))]
    ProcedureNotFound {
        #[snafu(implicit)]
        location: Location,
        pid: String,
    },

    #[snafu(display("Failed to parse procedure id: {key}"))]
    ParseProcedureId {
        #[snafu(implicit)]
        location: Location,
        key: String,
        #[snafu(source)]
        error: common_procedure::ParseIdError,
    },

    #[snafu(display("Unsupported operation {}", operation))]
    Unsupported {
        operation: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to wait procedure done"))]
    WaitProcedure {
        #[snafu(implicit)]
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display(
        "Failed to get procedure output, procedure id: {procedure_id}, error: {err_msg}"
    ))]
    ProcedureOutput {
        procedure_id: String,
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert RawTableInfo into TableInfo"))]
    ConvertRawTableInfo {
        #[snafu(implicit)]
        location: Location,
        source: datatypes::Error,
    },

    #[snafu(display("Primary key '{key}' not found when creating region request"))]
    PrimaryKeyNotFound {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build table meta for table: {}", table_name))]
    BuildTableMeta {
        table_name: String,
        #[snafu(source)]
        error: table::metadata::TableMetaBuilderError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table occurs error"))]
    Table {
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to find table route for table id {}", table_id))]
    TableRouteNotFound {
        table_id: TableId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode protobuf"))]
    DecodeProto {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: prost::DecodeError,
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

    #[snafu(display("Payload not exist"))]
    PayloadNotExist {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to send message: {err_msg}"))]
    SendMessage {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serde json"))]
    SerdeJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse value {} into key {}", value, key))]
    ParseOption {
        key: String,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Corrupted table route data, err: {}", err_msg))]
    RouteInfoCorrupted {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Illegal state from server, code: {}, error: {}", code, err_msg))]
    IllegalServerState {
        code: i32,
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert alter table request"))]
    ConvertAlterTableRequest {
        source: common_grpc_expr::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid protobuf message: {err_msg}"))]
    InvalidProtoMsg {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected: {err_msg}"))]
    Unexpected {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table already exists, table: {}", table_name))]
    TableAlreadyExists {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("View already exists, view: {}", view_name))]
    ViewAlreadyExists {
        view_name: String,
        location: Location,
    },

    #[snafu(display("Flow already exists: {}", flow_name))]
    FlowAlreadyExists {
        flow_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Catalog already exists, catalog: {}", catalog))]
    CatalogAlreadyExists {
        catalog: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema already exists, catalog:{}, schema: {}", catalog, schema))]
    SchemaAlreadyExists {
        catalog: String,
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert raw key to str"))]
    ConvertRawKey {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: Utf8Error,
    },

    #[snafu(display("Table not found: '{}'", table_name))]
    TableNotFound {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("View not found: '{}'", view_name))]
    ViewNotFound {
        view_name: String,
        location: Location,
    },

    #[snafu(display("Flow not found: '{}'", flow_name))]
    FlowNotFound {
        flow_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema nod found, schema: {}", table_schema))]
    SchemaNotFound {
        table_schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to rename table, reason: {}", reason))]
    RenameTable {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid table metadata, err: {}", err_msg))]
    InvalidTableMetadata {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid view info, err: {}", err_msg))]
    InvalidViewInfo { err_msg: String, location: Location },

    #[snafu(display("Failed to get kv cache, err: {}", err_msg))]
    GetKvCache { err_msg: String },

    #[snafu(display("Get null from cache, key: {}", key))]
    CacheNotGet {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Etcd txn error: {err_msg}"))]
    EtcdTxnOpResponse {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to move region {} in table {}, err: {}",
        region,
        table_id,
        err_msg
    ))]
    MoveRegion {
        table_id: TableId,
        region: RegionNumber,
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid catalog value"))]
    InvalidCatalogValue {
        source: common_catalog::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("External error"))]
    External {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Invalid heartbeat response"))]
    InvalidHeartbeatResponse {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to operate on datanode: {}", peer))]
    OperateDatanode {
        #[snafu(implicit)]
        location: Location,
        peer: Peer,
        source: BoxedError,
    },

    #[snafu(display("Retry later"))]
    RetryLater { source: BoxedError },

    #[snafu(display(
        "Failed to encode a wal options to json string, wal_options: {:?}",
        wal_options
    ))]
    EncodeWalOptions {
        wal_options: WalOptions,
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid number of topics {}", num_topics))]
    InvalidNumTopics {
        num_topics: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to build a Kafka client, broker endpoints: {:?}",
        broker_endpoints
    ))]
    BuildKafkaClient {
        broker_endpoints: Vec<String>,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to resolve Kafka broker endpoint."))]
    ResolveKafkaEndpoint { source: common_wal::error::Error },

    #[snafu(display("Failed to build a Kafka controller client"))]
    BuildKafkaCtrlClient {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display(
        "Failed to build a Kafka partition client, topic: {}, partition: {}",
        topic,
        partition
    ))]
    BuildKafkaPartitionClient {
        topic: String,
        partition: i32,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to produce records to Kafka, topic: {}", topic))]
    ProduceRecord {
        topic: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to create a Kafka wal topic"))]
    CreateKafkaWalTopic {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("The topic pool is empty"))]
    EmptyTopicPool {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected table route type: {}", err_msg))]
    UnexpectedLogicalRouteTable {
        #[snafu(implicit)]
        location: Location,
        err_msg: String,
    },

    #[snafu(display("The tasks of {} cannot be empty", name))]
    EmptyDdlTasks {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Metadata corruption: {}", err_msg))]
    MetadataCorruption {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Alter logical tables invalid arguments: {}", err_msg))]
    AlterLogicalTablesInvalidArguments {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Create logical tables invalid arguments: {}", err_msg))]
    CreateLogicalTablesInvalidArguments {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid  node info key: {}", key))]
    InvalidNodeInfoKey {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse number: {}", err_msg))]
    ParseNum {
        err_msg: String,
        #[snafu(source)]
        error: std::num::ParseIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid role: {}", role))]
    InvalidRole {
        role: i32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Delimiter not found, key: {}", key))]
    DelimiterNotFound {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid prefix: {}, key: {}", prefix, key))]
    MismatchPrefix {
        prefix: String,
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to move values: {err_msg}"))]
    MoveValues {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse {} from utf8", name))]
    FromUtf8 {
        name: String,
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Value not exists"))]
    ValueNotExist {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get cache"))]
    GetCache { source: Arc<Error> },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            IllegalServerState { .. }
            | EtcdTxnOpResponse { .. }
            | EtcdFailed { .. }
            | EtcdTxnFailed { .. }
            | ConnectEtcd { .. }
            | MoveValues { .. }
            | ValueNotExist { .. }
            | GetCache { .. } => StatusCode::Internal,

            SerdeJson { .. }
            | ParseOption { .. }
            | RouteInfoCorrupted { .. }
            | InvalidProtoMsg { .. }
            | InvalidTableMetadata { .. }
            | InvalidViewInfo { .. }
            | MoveRegion { .. }
            | Unexpected { .. }
            | TableInfoNotFound { .. }
            | NextSequence { .. }
            | SequenceOutOfRange { .. }
            | UnexpectedSequenceValue { .. }
            | InvalidHeartbeatResponse { .. }
            | InvalidTxnResult { .. }
            | EncodeJson { .. }
            | DecodeJson { .. }
            | PayloadNotExist { .. }
            | ConvertRawKey { .. }
            | DecodeProto { .. }
            | BuildTableMeta { .. }
            | TableRouteNotFound { .. }
            | ConvertRawTableInfo { .. }
            | RegionOperatingRace { .. }
            | EncodeWalOptions { .. }
            | BuildKafkaClient { .. }
            | BuildKafkaCtrlClient { .. }
            | BuildKafkaPartitionClient { .. }
            | ResolveKafkaEndpoint { .. }
            | ProduceRecord { .. }
            | CreateKafkaWalTopic { .. }
            | EmptyTopicPool { .. }
            | UnexpectedLogicalRouteTable { .. }
            | ProcedureOutput { .. }
            | FromUtf8 { .. }
            | MetadataCorruption { .. } => StatusCode::Unexpected,

            SendMessage { .. }
            | GetKvCache { .. }
            | CacheNotGet { .. }
            | CatalogAlreadyExists { .. }
            | SchemaAlreadyExists { .. }
            | RenameTable { .. }
            | Unsupported { .. } => StatusCode::Internal,

            ProcedureNotFound { .. }
            | PrimaryKeyNotFound { .. }
            | EmptyKey { .. }
            | InvalidEngineType { .. }
            | AlterLogicalTablesInvalidArguments { .. }
            | CreateLogicalTablesInvalidArguments { .. }
            | MismatchPrefix { .. }
            | DelimiterNotFound { .. } => StatusCode::InvalidArguments,

            FlowNotFound { .. } => StatusCode::FlowNotFound,
            FlowAlreadyExists { .. } => StatusCode::FlowAlreadyExists,

            ViewNotFound { .. } | TableNotFound { .. } => StatusCode::TableNotFound,
            ViewAlreadyExists { .. } | TableAlreadyExists { .. } => StatusCode::TableAlreadyExists,

            SubmitProcedure { source, .. }
            | QueryProcedure { source, .. }
            | WaitProcedure { source, .. } => source.status_code(),
            RegisterProcedureLoader { source, .. } => source.status_code(),
            External { source, .. } => source.status_code(),
            OperateDatanode { source, .. } => source.status_code(),
            Table { source, .. } => source.status_code(),
            RetryLater { source, .. } => source.status_code(),
            InvalidCatalogValue { source, .. } => source.status_code(),
            ConvertAlterTableRequest { source, .. } => source.status_code(),

            ParseProcedureId { .. }
            | InvalidNumTopics { .. }
            | SchemaNotFound { .. }
            | InvalidNodeInfoKey { .. }
            | ParseNum { .. }
            | InvalidRole { .. }
            | EmptyDdlTasks { .. } => StatusCode::InvalidArguments,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Error {
    /// Creates a new [Error::RetryLater] error from source `err`.
    pub fn retry_later<E: ErrorExt + Send + Sync + 'static>(err: E) -> Error {
        Error::RetryLater {
            source: BoxedError::new(err),
        }
    }

    /// Determine whether it is a retry later type through [StatusCode]
    pub fn is_retry_later(&self) -> bool {
        matches!(self, Error::RetryLater { .. })
    }

    /// Returns true if the response exceeds the size limit.
    pub fn is_exceeded_size_limit(&self) -> bool {
        if let Error::EtcdFailed {
            error: etcd_client::Error::GRpcStatus(status),
            ..
        } = self
        {
            return status.code() == tonic::Code::OutOfRange;
        }
        false
    }
}
