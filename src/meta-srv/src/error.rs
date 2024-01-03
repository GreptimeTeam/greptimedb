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

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_meta::peer::Peer;
use common_meta::DatanodeId;
use common_runtime::JoinError;
use rand::distributions::WeightedError;
use servers::define_into_tonic_status;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;
use table::metadata::TableId;
use tokio::sync::mpsc::error::SendError;
use tonic::codegen::http;

use crate::pubsub::Message;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("The target peer is unavailable temporally: {}", peer_id))]
    PeerUnavailable { location: Location, peer_id: u64 },

    #[snafu(display("Another migration procedure is running for region: {}", region_id))]
    MigrationRunning {
        location: Location,
        region_id: RegionId,
    },

    #[snafu(display("The region migration procedure aborted, reason: {}", reason))]
    MigrationAbort { location: Location, reason: String },

    #[snafu(display(
        "Another procedure is opening the region: {} on peer: {}",
        region_id,
        peer_id
    ))]
    RegionOpeningRace {
        location: Location,
        peer_id: DatanodeId,
        region_id: RegionId,
    },

    #[snafu(display("Failed to init ddl manager"))]
    InitDdlManager {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to create default catalog and schema"))]
    InitMetadata {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to allocate next sequence number"))]
    NextSequence {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to start telemetry task"))]
    StartTelemetryTask {
        location: Location,
        source: common_runtime::error::Error,
    },

    #[snafu(display("Failed to submit ddl task"))]
    SubmitDdlTask {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to invalidate table cache"))]
    InvalidateTableCache {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to operate region on peer:{}", peer))]
    OperateRegion {
        location: Location,
        peer: Peer,
        source: BoxedError,
    },

    #[snafu(display("Failed to list catalogs"))]
    ListCatalogs {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to list {}'s schemas", catalog))]
    ListSchemas {
        location: Location,
        catalog: String,
        source: BoxedError,
    },

    #[snafu(display("Failed to join a future"))]
    Join {
        location: Location,
        #[snafu(source)]
        error: JoinError,
    },

    #[snafu(display("Failed to execute transaction: {}", msg))]
    Txn { location: Location, msg: String },

    #[snafu(display(
        "Unexpected table_id changed, expected: {}, found: {}",
        expected,
        found,
    ))]
    TableIdChanged {
        location: Location,
        expected: u64,
        found: u64,
    },

    #[snafu(display(
        "Failed to request Datanode, required: {}, but only {} available",
        required,
        available
    ))]
    NoEnoughAvailableDatanode {
        location: Location,
        required: usize,
        available: usize,
    },

    #[snafu(display("Failed to request Datanode {}", peer))]
    RequestDatanode {
        location: Location,
        peer: Peer,
        source: client::Error,
    },

    #[snafu(display("Failed to send shutdown signal"))]
    SendShutdownSignal {
        #[snafu(source)]
        error: SendError<()>,
    },

    #[snafu(display("Failed to shutdown {} server", server))]
    ShutdownServer {
        location: Location,
        source: servers::error::Error,
        server: String,
    },

    #[snafu(display("Empty key is not allowed"))]
    EmptyKey { location: Location },

    #[snafu(display("Failed to execute via Etcd"))]
    EtcdFailed {
        #[snafu(source)]
        error: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Failed to connect to Etcd"))]
    ConnectEtcd {
        #[snafu(source)]
        error: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Failed to bind address {}", addr))]
    TcpBind {
        addr: String,
        #[snafu(source)]
        error: std::io::Error,
        location: Location,
    },

    #[snafu(display("Failed to convert to TcpIncoming"))]
    TcpIncoming {
        #[snafu(source)]
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to start gRPC server"))]
    StartGrpc {
        #[snafu(source)]
        error: tonic::transport::Error,
        location: Location,
    },
    #[snafu(display("Failed to start http server"))]
    StartHttp {
        location: Location,
        source: servers::error::Error,
    },
    #[snafu(display("Failed to init export metrics task"))]
    InitExportMetricsTask {
        location: Location,
        source: servers::error::Error,
    },
    #[snafu(display("Failed to parse address {}", addr))]
    ParseAddr {
        addr: String,
        #[snafu(source)]
        error: std::net::AddrParseError,
    },
    #[snafu(display("Empty table name"))]
    EmptyTableName { location: Location },

    #[snafu(display("Invalid datanode lease key: {}", key))]
    InvalidLeaseKey { key: String, location: Location },

    #[snafu(display("Invalid datanode stat key: {}", key))]
    InvalidStatKey { key: String, location: Location },

    #[snafu(display("Invalid inactive region key: {}", key))]
    InvalidInactiveRegionKey { key: String, location: Location },

    #[snafu(display("Failed to parse datanode lease key from utf8"))]
    LeaseKeyFromUtf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Failed to parse datanode lease value from utf8"))]
    LeaseValueFromUtf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Failed to parse datanode stat key from utf8"))]
    StatKeyFromUtf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Failed to parse datanode stat value from utf8"))]
    StatValueFromUtf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Failed to parse invalid region key from utf8"))]
    InvalidRegionKeyFromUtf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Failed to serialize to json: {}", input))]
    SerializeToJson {
        input: String,
        #[snafu(source)]
        error: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to deserialize from json: {}", input))]
    DeserializeFromJson {
        input: String,
        #[snafu(source)]
        error: serde_json::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse number: {}", err_msg))]
    ParseNum {
        err_msg: String,
        #[snafu(source)]
        error: std::num::ParseIntError,
        location: Location,
    },

    #[snafu(display("Invalid arguments: {}", err_msg))]
    InvalidArguments { err_msg: String, location: Location },

    #[snafu(display("Cannot parse catalog value"))]
    InvalidCatalogValue {
        location: Location,
        source: common_catalog::error::Error,
    },

    #[snafu(display("Cannot parse full table name"))]
    InvalidFullTableName {
        location: Location,
        source: common_catalog::error::Error,
    },

    #[snafu(display("Failed to decode table route"))]
    DecodeTableRoute {
        #[snafu(source)]
        error: prost::DecodeError,
        location: Location,
    },

    #[snafu(display("Failed to find table route for {table_id}"))]
    TableRouteNotFound {
        table_id: TableId,
        location: Location,
    },

    #[snafu(display("Failed to find table route for {region_id}"))]
    RegionRouteNotFound {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Table info not found: {}", table_id))]
    TableInfoNotFound {
        table_id: TableId,
        location: Location,
    },

    #[snafu(display("Datanode table not found: {}, datanode: {}", table_id, datanode_id))]
    DatanodeTableNotFound {
        table_id: TableId,
        datanode_id: DatanodeId,
        location: Location,
    },

    #[snafu(display("Table route corrupted, key: {}, reason: {}", key, reason))]
    CorruptedTableRoute {
        key: String,
        reason: String,
        location: Location,
    },

    #[snafu(display("MetaSrv has no leader at this moment"))]
    NoLeader { location: Location },

    #[snafu(display("Table {} not found", name))]
    TableNotFound { name: String, location: Location },

    #[snafu(display(
        "Failed to move the value of {} because other clients caused a race condition",
        key
    ))]
    MoveValue { key: String, location: Location },

    #[snafu(display("Unsupported selector type, {}", selector_type))]
    UnsupportedSelectorType {
        selector_type: String,
        location: Location,
    },

    #[snafu(display("Unexpected, violated: {violated}"))]
    Unexpected {
        violated: String,
        location: Location,
    },

    #[snafu(display("Failed to create gRPC channel"))]
    CreateChannel {
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to batch get KVs from leader's in_memory kv store"))]
    BatchGet {
        #[snafu(source)]
        error: tonic::Status,
        location: Location,
    },

    #[snafu(display("Failed to batch range KVs from leader's in_memory kv store"))]
    Range {
        #[snafu(source)]
        error: tonic::Status,
        location: Location,
    },

    #[snafu(display("Response header not found"))]
    ResponseHeaderNotFound { location: Location },

    #[snafu(display("The requested meta node is not leader, node addr: {}", node_addr))]
    IsNotLeader {
        node_addr: String,
        location: Location,
    },

    #[snafu(display("Invalid http body"))]
    InvalidHttpBody {
        #[snafu(source)]
        error: http::Error,
        location: Location,
    },

    #[snafu(display(
        "The number of retries for the grpc call {} exceeded the limit, {}",
        func_name,
        retry_num
    ))]
    ExceededRetryLimit {
        func_name: String,
        retry_num: usize,
        location: Location,
    },

    #[snafu(display("Failed to lock based on etcd"))]
    Lock {
        #[snafu(source)]
        error: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Failed to unlock based on etcd"))]
    Unlock {
        #[snafu(source)]
        error: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Failed to grant lease"))]
    LeaseGrant {
        #[snafu(source)]
        error: etcd_client::Error,
        location: Location,
    },

    #[snafu(display("Distributed lock is not configured"))]
    LockNotConfig { location: Location },

    #[snafu(display("Invalid utf-8 value"))]
    InvalidUtf8Value {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        location: Location,
    },

    #[snafu(display("Missing required parameter, param: {:?}", param))]
    MissingRequiredParameter { param: String },

    #[snafu(display("Failed to start procedure manager"))]
    StartProcedureManager {
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Failed to stop procedure manager"))]
    StopProcedureManager {
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Failed to wait procedure done"))]
    WaitProcedure {
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Failed to submit procedure"))]
    SubmitProcedure {
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Schema already exists, name: {schema_name}"))]
    SchemaAlreadyExists {
        schema_name: String,
        location: Location,
    },

    #[snafu(display("Table already exists: {table_name}"))]
    TableAlreadyExists {
        table_name: String,
        location: Location,
    },

    #[snafu(display("Pusher not found: {pusher_id}"))]
    PusherNotFound {
        pusher_id: String,
        location: Location,
    },

    #[snafu(display("Failed to push message: {err_msg}"))]
    PushMessage { err_msg: String, location: Location },

    #[snafu(display("Mailbox already closed: {id}"))]
    MailboxClosed { id: u64, location: Location },

    #[snafu(display("Mailbox timeout: {id}"))]
    MailboxTimeout { id: u64, location: Location },

    #[snafu(display("Mailbox receiver got an error: {id}, {err_msg}"))]
    MailboxReceiver {
        id: u64,
        err_msg: String,
        location: Location,
    },

    #[snafu(display("Missing request header"))]
    MissingRequestHeader { location: Location },

    #[snafu(display("Failed to register procedure loader, type name: {}", type_name))]
    RegisterProcedureLoader {
        type_name: String,
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to find failover candidates for region: {}", failed_region))]
    RegionFailoverCandidatesNotFound {
        failed_region: String,
        location: Location,
    },

    #[snafu(display(
        "Received unexpected instruction reply, mailbox message: {}, reason: {}",
        mailbox_message,
        reason
    ))]
    UnexpectedInstructionReply {
        mailbox_message: String,
        reason: String,
        location: Location,
    },

    #[snafu(display("Expected to retry later, reason: {}", reason))]
    RetryLater { reason: String, location: Location },

    #[snafu(display("Failed to update table metadata, err_msg: {}", err_msg))]
    UpdateTableMetadata { err_msg: String, location: Location },

    #[snafu(display("Failed to convert table route"))]
    TableRouteConversion {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to convert proto data"))]
    ConvertProtoData {
        location: Location,
        source: common_meta::error::Error,
    },

    // this error is used for custom error mapping
    // please do not delete it
    #[snafu(display("Other error"))]
    Other {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Table metadata manager error"))]
    TableMetadataManager {
        source: common_meta::error::Error,
        location: Location,
    },

    #[snafu(display("Keyvalue backend error"))]
    KvBackend {
        source: common_meta::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to update table route"))]
    UpdateTableRoute {
        source: common_meta::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to get table info error"))]
    GetFullTableInfo {
        source: common_meta::error::Error,
        location: Location,
    },

    #[snafu(display("Invalid heartbeat request: {}", err_msg))]
    InvalidHeartbeatRequest { err_msg: String, location: Location },

    #[snafu(display("Failed to publish message"))]
    PublishMessage {
        #[snafu(source)]
        error: SendError<Message>,
        location: Location,
    },

    #[snafu(display("Too many partitions"))]
    TooManyPartitions { location: Location },

    #[snafu(display("Unsupported operation {}", operation))]
    Unsupported {
        operation: String,
        location: Location,
    },

    #[snafu(display("Failed to set weight array"))]
    WeightArray {
        #[snafu(source)]
        error: WeightedError,
        location: Location,
    },

    #[snafu(display("Weight array is not set"))]
    NotSetWeightArray { location: Location },

    #[snafu(display("Unexpected table route type: {}", err_msg))]
    UnexpectedLogicalRouteTable {
        location: Location,
        err_msg: String,
        source: common_meta::error::Error,
    },
}

impl Error {
    /// Returns `true` if the error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::RetryLater { .. })
    }
}

pub type Result<T> = std::result::Result<T, Error>;

define_into_tonic_status!(Error);

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::EtcdFailed { .. }
            | Error::ConnectEtcd { .. }
            | Error::TcpBind { .. }
            | Error::TcpIncoming { .. }
            | Error::SerializeToJson { .. }
            | Error::DeserializeFromJson { .. }
            | Error::DecodeTableRoute { .. }
            | Error::NoLeader { .. }
            | Error::CreateChannel { .. }
            | Error::BatchGet { .. }
            | Error::Range { .. }
            | Error::ResponseHeaderNotFound { .. }
            | Error::IsNotLeader { .. }
            | Error::InvalidHttpBody { .. }
            | Error::Lock { .. }
            | Error::Unlock { .. }
            | Error::LeaseGrant { .. }
            | Error::LockNotConfig { .. }
            | Error::ExceededRetryLimit { .. }
            | Error::SendShutdownSignal { .. }
            | Error::ParseAddr { .. }
            | Error::SchemaAlreadyExists { .. }
            | Error::PusherNotFound { .. }
            | Error::PushMessage { .. }
            | Error::MailboxClosed { .. }
            | Error::MailboxTimeout { .. }
            | Error::MailboxReceiver { .. }
            | Error::RetryLater { .. }
            | Error::StartGrpc { .. }
            | Error::UpdateTableMetadata { .. }
            | Error::NoEnoughAvailableDatanode { .. }
            | Error::PublishMessage { .. }
            | Error::Join { .. }
            | Error::WeightArray { .. }
            | Error::NotSetWeightArray { .. }
            | Error::Unsupported { .. }
            | Error::PeerUnavailable { .. } => StatusCode::Internal,
            Error::TableAlreadyExists { .. } => StatusCode::TableAlreadyExists,
            Error::EmptyKey { .. }
            | Error::MissingRequiredParameter { .. }
            | Error::MissingRequestHeader { .. }
            | Error::EmptyTableName { .. }
            | Error::InvalidLeaseKey { .. }
            | Error::InvalidStatKey { .. }
            | Error::InvalidInactiveRegionKey { .. }
            | Error::ParseNum { .. }
            | Error::UnsupportedSelectorType { .. }
            | Error::InvalidArguments { .. }
            | Error::InitExportMetricsTask { .. }
            | Error::InvalidHeartbeatRequest { .. }
            | Error::TooManyPartitions { .. } => StatusCode::InvalidArguments,
            Error::LeaseKeyFromUtf8 { .. }
            | Error::LeaseValueFromUtf8 { .. }
            | Error::StatKeyFromUtf8 { .. }
            | Error::StatValueFromUtf8 { .. }
            | Error::InvalidRegionKeyFromUtf8 { .. }
            | Error::TableRouteNotFound { .. }
            | Error::TableInfoNotFound { .. }
            | Error::DatanodeTableNotFound { .. }
            | Error::CorruptedTableRoute { .. }
            | Error::MoveValue { .. }
            | Error::InvalidUtf8Value { .. }
            | Error::UnexpectedInstructionReply { .. }
            | Error::Unexpected { .. }
            | Error::Txn { .. }
            | Error::TableIdChanged { .. }
            | Error::RegionOpeningRace { .. }
            | Error::RegionRouteNotFound { .. }
            | Error::MigrationAbort { .. }
            | Error::MigrationRunning { .. } => StatusCode::Unexpected,
            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::InvalidateTableCache { source, .. } => source.status_code(),
            Error::RequestDatanode { source, .. } => source.status_code(),
            Error::InvalidCatalogValue { source, .. }
            | Error::InvalidFullTableName { source, .. } => source.status_code(),
            Error::SubmitProcedure { source, .. } | Error::WaitProcedure { source, .. } => {
                source.status_code()
            }
            Error::ShutdownServer { source, .. } | Error::StartHttp { source, .. } => {
                source.status_code()
            }
            Error::StartProcedureManager { source, .. }
            | Error::StopProcedureManager { source, .. } => source.status_code(),

            Error::ListCatalogs { source, .. } | Error::ListSchemas { source, .. } => {
                source.status_code()
            }
            Error::StartTelemetryTask { source, .. } => source.status_code(),

            Error::RegionFailoverCandidatesNotFound { .. } => StatusCode::RuntimeResourcesExhausted,
            Error::NextSequence { source, .. } => source.status_code(),

            Error::RegisterProcedureLoader { source, .. } => source.status_code(),
            Error::OperateRegion { source, .. } => source.status_code(),
            Error::SubmitDdlTask { source, .. } => source.status_code(),
            Error::TableRouteConversion { source, .. }
            | Error::ConvertProtoData { source, .. }
            | Error::TableMetadataManager { source, .. }
            | Error::KvBackend { source, .. }
            | Error::UpdateTableRoute { source, .. }
            | Error::GetFullTableInfo { source, .. }
            | Error::UnexpectedLogicalRouteTable { source, .. } => source.status_code(),

            Error::InitMetadata { source, .. } | Error::InitDdlManager { source, .. } => {
                source.status_code()
            }

            Error::Other { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// for form tonic
pub(crate) fn match_for_io_error(err_status: &tonic::Status) -> Option<&std::io::Error> {
    let mut err: &(dyn std::error::Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}
