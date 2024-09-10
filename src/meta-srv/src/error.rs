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

use common_error::define_into_tonic_status;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_meta::DatanodeId;
use common_runtime::JoinError;
use rand::distributions::WeightedError;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;
use table::metadata::TableId;
use tokio::sync::mpsc::error::SendError;
use tonic::codegen::http;

use crate::metasrv::SelectTarget;
use crate::pubsub::Message;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("The target peer is unavailable temporally: {}", peer_id))]
    PeerUnavailable {
        #[snafu(implicit)]
        location: Location,
        peer_id: u64,
    },

    #[snafu(display("Failed to lookup peer: {}", peer_id))]
    LookupPeer {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
        peer_id: u64,
    },

    #[snafu(display("Another migration procedure is running for region: {}", region_id))]
    MigrationRunning {
        #[snafu(implicit)]
        location: Location,
        region_id: RegionId,
    },

    #[snafu(display("The region migration procedure aborted, reason: {}", reason))]
    MigrationAbort {
        #[snafu(implicit)]
        location: Location,
        reason: String,
    },

    #[snafu(display(
        "Another procedure is opening the region: {} on peer: {}",
        region_id,
        peer_id
    ))]
    RegionOpeningRace {
        #[snafu(implicit)]
        location: Location,
        peer_id: DatanodeId,
        region_id: RegionId,
    },

    #[snafu(display("Failed to init ddl manager"))]
    InitDdlManager {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to create default catalog and schema"))]
    InitMetadata {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to allocate next sequence number"))]
    NextSequence {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to start telemetry task"))]
    StartTelemetryTask {
        #[snafu(implicit)]
        location: Location,
        source: common_runtime::error::Error,
    },

    #[snafu(display("Failed to submit ddl task"))]
    SubmitDdlTask {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to invalidate table cache"))]
    InvalidateTableCache {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to list catalogs"))]
    ListCatalogs {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to list {}'s schemas", catalog))]
    ListSchemas {
        #[snafu(implicit)]
        location: Location,
        catalog: String,
        source: BoxedError,
    },

    #[snafu(display("Failed to list {}.{}'s tables", catalog, schema))]
    ListTables {
        #[snafu(implicit)]
        location: Location,
        catalog: String,
        schema: String,
        source: BoxedError,
    },

    #[snafu(display("Failed to join a future"))]
    Join {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: JoinError,
    },

    #[snafu(display(
        "Failed to request {}, required: {}, but only {} available",
        select_target,
        required,
        available
    ))]
    NoEnoughAvailableNode {
        #[snafu(implicit)]
        location: Location,
        required: usize,
        available: usize,
        select_target: SelectTarget,
    },

    #[snafu(display("Failed to send shutdown signal"))]
    SendShutdownSignal {
        #[snafu(source)]
        error: SendError<()>,
    },

    #[snafu(display("Failed to shutdown {} server", server))]
    ShutdownServer {
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
        server: String,
    },

    #[snafu(display("Empty key is not allowed"))]
    EmptyKey {
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

    #[snafu(display("Failed to connect to Etcd"))]
    ConnectEtcd {
        #[snafu(source)]
        error: etcd_client::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to bind address {}", addr))]
    TcpBind {
        addr: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to start http server"))]
    StartHttp {
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to init export metrics task"))]
    InitExportMetricsTask {
        #[snafu(implicit)]
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to parse address {}", addr))]
    ParseAddr {
        addr: String,
        #[snafu(source)]
        error: std::net::AddrParseError,
    },

    #[snafu(display("Invalid lease key: {}", key))]
    InvalidLeaseKey {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid datanode stat key: {}", key))]
    InvalidStatKey {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid inactive region key: {}", key))]
    InvalidInactiveRegionKey {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse lease key from utf8"))]
    LeaseKeyFromUtf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse lease value from utf8"))]
    LeaseValueFromUtf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse stat key from utf8"))]
    StatKeyFromUtf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse stat value from utf8"))]
    StatValueFromUtf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse invalid region key from utf8"))]
    InvalidRegionKeyFromUtf8 {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize to json: {}", input))]
    SerializeToJson {
        input: String,
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize from json: {}", input))]
    DeserializeFromJson {
        input: String,
        #[snafu(source)]
        error: serde_json::error::Error,
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

    #[snafu(display("Failed to parse bool: {}", err_msg))]
    ParseBool {
        err_msg: String,
        #[snafu(source)]
        error: std::str::ParseBoolError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid arguments: {}", err_msg))]
    InvalidArguments {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find table route for {table_id}"))]
    TableRouteNotFound {
        table_id: TableId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find table route for {region_id}"))]
    RegionRouteNotFound {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table info not found: {}", table_id))]
    TableInfoNotFound {
        table_id: TableId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Datanode table not found: {}, datanode: {}", table_id, datanode_id))]
    DatanodeTableNotFound {
        table_id: TableId,
        datanode_id: DatanodeId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Metasrv has no leader at this moment"))]
    NoLeader {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table {} not found", name))]
    TableNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported selector type, {}", selector_type))]
    UnsupportedSelectorType {
        selector_type: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected, violated: {violated}"))]
    Unexpected {
        violated: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create gRPC channel"))]
    CreateChannel {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to batch get KVs from leader's in_memory kv store"))]
    BatchGet {
        #[snafu(source)]
        error: tonic::Status,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to batch range KVs from leader's in_memory kv store"))]
    Range {
        #[snafu(source)]
        error: tonic::Status,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Response header not found"))]
    ResponseHeaderNotFound {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("The requested meta node is not leader, node addr: {}", node_addr))]
    IsNotLeader {
        node_addr: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid http body"))]
    InvalidHttpBody {
        #[snafu(source)]
        error: http::Error,
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to lock based on etcd"))]
    Lock {
        #[snafu(source)]
        error: etcd_client::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to unlock based on etcd"))]
    Unlock {
        #[snafu(source)]
        error: etcd_client::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to grant lease"))]
    LeaseGrant {
        #[snafu(source)]
        error: etcd_client::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid utf-8 value"))]
    InvalidUtf8Value {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing required parameter, param: {:?}", param))]
    MissingRequiredParameter { param: String },

    #[snafu(display("Failed to start procedure manager"))]
    StartProcedureManager {
        #[snafu(implicit)]
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Failed to stop procedure manager"))]
    StopProcedureManager {
        #[snafu(implicit)]
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Failed to wait procedure done"))]
    WaitProcedure {
        #[snafu(implicit)]
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Failed to query procedure state"))]
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

    #[snafu(display("Failed to submit procedure"))]
    SubmitProcedure {
        #[snafu(implicit)]
        location: Location,
        source: common_procedure::Error,
    },

    #[snafu(display("Schema already exists, name: {schema_name}"))]
    SchemaAlreadyExists {
        schema_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table already exists: {table_name}"))]
    TableAlreadyExists {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Pusher not found: {pusher_id}"))]
    PusherNotFound {
        pusher_id: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to push message: {err_msg}"))]
    PushMessage {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mailbox already closed: {id}"))]
    MailboxClosed {
        id: u64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mailbox timeout: {id}"))]
    MailboxTimeout {
        id: u64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Mailbox receiver got an error: {id}, {err_msg}"))]
    MailboxReceiver {
        id: u64,
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing request header"))]
    MissingRequestHeader {
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

    #[snafu(display(
        "Received unexpected instruction reply, mailbox message: {}, reason: {}",
        mailbox_message,
        reason
    ))]
    UnexpectedInstructionReply {
        mailbox_message: String,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected to retry later, reason: {}", reason))]
    RetryLater {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Expected to retry later, reason: {}", reason))]
    RetryLaterWithSource {
        reason: String,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to convert proto data"))]
    ConvertProtoData {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    // this error is used for custom error mapping
    // please do not delete it
    #[snafu(display("Other error"))]
    Other {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table metadata manager error"))]
    TableMetadataManager {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Keyvalue backend error"))]
    KvBackend {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to publish message"))]
    PublishMessage {
        #[snafu(source)]
        error: SendError<Message>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Too many partitions"))]
    TooManyPartitions {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported operation {}", operation))]
    Unsupported {
        operation: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to set weight array"))]
    WeightArray {
        #[snafu(source)]
        error: WeightedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Weight array is not set"))]
    NotSetWeightArray {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected table route type: {}", err_msg))]
    UnexpectedLogicalRouteTable {
        #[snafu(implicit)]
        location: Location,
        err_msg: String,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to save cluster info"))]
    SaveClusterInfo {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Invalid cluster info format"))]
    InvalidClusterInfoFormat {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to serialize options to TOML"))]
    TomlFormat {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source(from(common_config::error::Error, Box::new)))]
        source: Box<common_config::error::Error>,
    },

    #[cfg(feature = "pg_kvbackend")]
    #[snafu(display("Failed to execute via postgres"))]
    PostgresExecution {
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(feature = "pg_kvbackend")]
    #[snafu(display("Failed to connect to PostgresSQL"))]
    ConnectPostgres {
        #[snafu(source)]
        error: tokio_postgres::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl Error {
    /// Returns `true` if the error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::RetryLater { .. })
            || matches!(self, Error::RetryLaterWithSource { .. })
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
            | Error::ExceededRetryLimit { .. }
            | Error::SendShutdownSignal { .. }
            | Error::PusherNotFound { .. }
            | Error::PushMessage { .. }
            | Error::MailboxClosed { .. }
            | Error::MailboxTimeout { .. }
            | Error::MailboxReceiver { .. }
            | Error::RetryLater { .. }
            | Error::RetryLaterWithSource { .. }
            | Error::StartGrpc { .. }
            | Error::NoEnoughAvailableNode { .. }
            | Error::PublishMessage { .. }
            | Error::Join { .. }
            | Error::WeightArray { .. }
            | Error::NotSetWeightArray { .. }
            | Error::PeerUnavailable { .. } => StatusCode::Internal,

            Error::Unsupported { .. } => StatusCode::Unsupported,

            Error::SchemaAlreadyExists { .. } => StatusCode::DatabaseAlreadyExists,

            Error::TableAlreadyExists { .. } => StatusCode::TableAlreadyExists,
            Error::EmptyKey { .. }
            | Error::MissingRequiredParameter { .. }
            | Error::MissingRequestHeader { .. }
            | Error::InvalidLeaseKey { .. }
            | Error::InvalidStatKey { .. }
            | Error::InvalidInactiveRegionKey { .. }
            | Error::ParseNum { .. }
            | Error::ParseBool { .. }
            | Error::ParseAddr { .. }
            | Error::UnsupportedSelectorType { .. }
            | Error::InvalidArguments { .. }
            | Error::InitExportMetricsTask { .. }
            | Error::ProcedureNotFound { .. }
            | Error::TooManyPartitions { .. }
            | Error::TomlFormat { .. } => StatusCode::InvalidArguments,
            Error::LeaseKeyFromUtf8 { .. }
            | Error::LeaseValueFromUtf8 { .. }
            | Error::StatKeyFromUtf8 { .. }
            | Error::StatValueFromUtf8 { .. }
            | Error::InvalidRegionKeyFromUtf8 { .. }
            | Error::TableRouteNotFound { .. }
            | Error::TableInfoNotFound { .. }
            | Error::DatanodeTableNotFound { .. }
            | Error::InvalidUtf8Value { .. }
            | Error::UnexpectedInstructionReply { .. }
            | Error::Unexpected { .. }
            | Error::RegionOpeningRace { .. }
            | Error::RegionRouteNotFound { .. }
            | Error::MigrationAbort { .. }
            | Error::MigrationRunning { .. } => StatusCode::Unexpected,
            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::SaveClusterInfo { source, .. }
            | Error::InvalidClusterInfoFormat { source, .. } => source.status_code(),
            Error::InvalidateTableCache { source, .. } => source.status_code(),
            Error::SubmitProcedure { source, .. }
            | Error::WaitProcedure { source, .. }
            | Error::QueryProcedure { source, .. } => source.status_code(),
            Error::ShutdownServer { source, .. } | Error::StartHttp { source, .. } => {
                source.status_code()
            }
            Error::StartProcedureManager { source, .. }
            | Error::StopProcedureManager { source, .. } => source.status_code(),

            Error::ListCatalogs { source, .. }
            | Error::ListSchemas { source, .. }
            | Error::ListTables { source, .. } => source.status_code(),
            Error::StartTelemetryTask { source, .. } => source.status_code(),

            Error::NextSequence { source, .. } => source.status_code(),

            Error::RegisterProcedureLoader { source, .. } => source.status_code(),
            Error::SubmitDdlTask { source, .. } => source.status_code(),
            Error::ConvertProtoData { source, .. }
            | Error::TableMetadataManager { source, .. }
            | Error::KvBackend { source, .. }
            | Error::UnexpectedLogicalRouteTable { source, .. } => source.status_code(),

            Error::InitMetadata { source, .. } | Error::InitDdlManager { source, .. } => {
                source.status_code()
            }

            Error::Other { source, .. } => source.status_code(),
            Error::LookupPeer { source, .. } => source.status_code(),
            #[cfg(feature = "pg_kvbackend")]
            Error::ConnectPostgres { .. } => StatusCode::Internal,
            #[cfg(feature = "pg_kvbackend")]
            Error::PostgresExecution { .. } => StatusCode::Internal,
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
