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
use common_procedure::ProcedureId;
use common_runtime::JoinError;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;
use table::metadata::TableId;
use tokio::sync::mpsc::error::SendError;
use tonic::codegen::http;
use uuid::Uuid;

use crate::metasrv::SelectTarget;
use crate::pubsub::Message;
use crate::service::mailbox::Channel;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to choose items"))]
    ChooseItems {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rand::distr::weighted::Error,
    },

    #[snafu(display("Exceeded deadline, operation: {}", operation))]
    ExceededDeadline {
        #[snafu(implicit)]
        location: Location,
        operation: String,
    },

    #[snafu(display("The target peer is unavailable temporally: {}", peer_id))]
    PeerUnavailable {
        #[snafu(implicit)]
        location: Location,
        peer_id: u64,
    },

    #[snafu(display("Failed to list active frontends"))]
    ListActiveFrontends {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to list active datanodes"))]
    ListActiveDatanodes {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to list active flownodes"))]
    ListActiveFlownodes {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("No available frontend"))]
    NoAvailableFrontend {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Another migration procedure is running for region: {}", region_id))]
    MigrationRunning {
        #[snafu(implicit)]
        location: Location,
        region_id: RegionId,
    },

    #[snafu(display(
        "The region migration procedure is completed for region: {}, target_peer: {}",
        region_id,
        target_peer_id
    ))]
    RegionMigrated {
        #[snafu(implicit)]
        location: Location,
        region_id: RegionId,
        target_peer_id: u64,
    },

    #[snafu(display("The region migration procedure aborted, reason: {}", reason))]
    MigrationAbort {
        #[snafu(implicit)]
        location: Location,
        reason: String,
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

    #[snafu(display("Failed to init ddl manager"))]
    InitDdlManager {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to init reconciliation manager"))]
    InitReconciliationManager {
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

    #[snafu(display("Failed to set next sequence number"))]
    SetNextSequence {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to peek sequence number"))]
    PeekSequence {
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

    #[snafu(display("Failed to submit reconcile procedure"))]
    SubmitReconcileProcedure {
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

    #[snafu(display("Failed to read file: {}", path))]
    FileIo {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
        path: String,
    },

    #[snafu(display("Failed to bind address {}", addr))]
    TcpBind {
        addr: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
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

    #[snafu(display("Failed to downgrade region leader, region: {}", region_id))]
    DowngradeLeader {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        source: BoxedError,
    },

    #[snafu(display("Region's leader peer changed: {}", msg))]
    LeaderPeerChanged {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid arguments: {}", err_msg))]
    InvalidArguments {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(feature = "mysql_kvbackend")]
    #[snafu(display("Failed to parse mysql url: {}", mysql_url))]
    ParseMySqlUrl {
        #[snafu(source)]
        error: sqlx::error::Error,
        mysql_url: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(feature = "mysql_kvbackend")]
    #[snafu(display("Failed to decode sql value"))]
    DecodeSqlValue {
        #[snafu(source)]
        error: sqlx::error::Error,
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

    #[snafu(display("Leader lease expired"))]
    LeaderLeaseExpired {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Leader lease changed during election"))]
    LeaderLeaseChanged {
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

    #[snafu(display("A prune task for topic {} is already running", topic))]
    PruneTaskAlreadyRunning {
        topic: String,
        #[snafu(implicit)]
        location: Location,
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

    #[snafu(display("Mailbox channel closed: {channel}"))]
    MailboxChannelClosed {
        channel: Channel,
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

    #[snafu(display("Runtime switch manager error"))]
    RuntimeSwitchManager {
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

    #[snafu(display("Failed to create repartition subtasks"))]
    RepartitionCreateSubtasks {
        source: partition::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Source partition expression '{}' does not match any existing region",
        expr
    ))]
    RepartitionSourceExprMismatch {
        expr: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to get the state receiver for repartition subprocedure {}",
        procedure_id
    ))]
    RepartitionSubprocedureStateReceiver {
        procedure_id: ProcedureId,
        #[snafu(source)]
        source: common_procedure::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported operation {}", operation))]
    Unsupported {
        operation: String,
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

    #[snafu(display("Invalid datanode stat format"))]
    InvalidDatanodeStatFormat {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Invalid node info format"))]
    InvalidNodeInfoFormat {
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
    #[snafu(display("Failed to execute via postgres, sql: {}", sql))]
    PostgresExecution {
        #[snafu(source)]
        error: tokio_postgres::Error,
        sql: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(feature = "pg_kvbackend")]
    #[snafu(display("Failed to get Postgres client"))]
    GetPostgresClient {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: deadpool::managed::PoolError<tokio_postgres::Error>,
    },

    #[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
    #[snafu(display("Sql execution timeout, sql: {}, duration: {:?}", sql, duration))]
    SqlExecutionTimeout {
        #[snafu(implicit)]
        location: Location,
        sql: String,
        duration: std::time::Duration,
    },

    #[cfg(feature = "pg_kvbackend")]
    #[snafu(display("Failed to create connection pool for Postgres"))]
    CreatePostgresPool {
        #[snafu(source)]
        error: deadpool_postgres::CreatePoolError,
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(feature = "pg_kvbackend")]
    #[snafu(display("Failed to get connection from Postgres pool: {}", reason))]
    GetPostgresConnection {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(feature = "mysql_kvbackend")]
    #[snafu(display("Failed to execute via mysql, sql: {}", sql))]
    MySqlExecution {
        #[snafu(source)]
        error: sqlx::Error,
        #[snafu(implicit)]
        location: Location,
        sql: String,
    },

    #[cfg(feature = "mysql_kvbackend")]
    #[snafu(display("Failed to create mysql pool"))]
    CreateMySqlPool {
        #[snafu(source)]
        error: sqlx::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(feature = "mysql_kvbackend")]
    #[snafu(display("Failed to acquire mysql client from pool"))]
    AcquireMySqlClient {
        #[snafu(source)]
        error: sqlx::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Handler not found: {}", name))]
    HandlerNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Flow state handler error"))]
    FlowStateHandler {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to build wal provider"))]
    BuildWalProvider {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to parse wal options"))]
    ParseWalOptions {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to build kafka client."))]
    BuildKafkaClient {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: common_meta::error::Error,
    },

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
        "Failed to delete records from Kafka, topic: {}, partition: {}, offset: {}",
        topic,
        partition,
        offset
    ))]
    DeleteRecords {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
        topic: String,
        partition: i32,
        offset: u64,
    },

    #[snafu(display("Failed to get offset from Kafka, topic: {}", topic))]
    GetOffset {
        topic: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: rskafka::client::error::Error,
    },

    #[snafu(display("Failed to update the TopicNameValue in kvbackend, topic: {}", topic))]
    UpdateTopicNameValue {
        topic: String,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to build tls options"))]
    BuildTlsOptions {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        source: common_meta::error::Error,
    },

    #[snafu(display(
        "Repartition group {} source region missing, region id: {}",
        group_id,
        region_id
    ))]
    RepartitionSourceRegionMissing {
        group_id: Uuid,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Repartition group {} target region missing, region id: {}",
        group_id,
        region_id
    ))]
    RepartitionTargetRegionMissing {
        group_id: Uuid,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize partition expression: {}", source))]
    SerializePartitionExpr {
        #[snafu(source)]
        source: partition::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Partition expression mismatch, region id: {}, expected: {}, actual: {}",
        region_id,
        expected,
        actual
    ))]
    PartitionExprMismatch {
        region_id: RegionId,
        expected: String,
        actual: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deallocate regions for table: {}", table_id))]
    DeallocateRegions {
        #[snafu(implicit)]
        location: Location,
        table_id: TableId,
        #[snafu(source)]
        source: common_meta::error::Error,
    },
}

impl Error {
    /// Returns `true` if the error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::RetryLater { .. }
                | Error::RetryLaterWithSource { .. }
                | Error::MailboxTimeout { .. }
        )
    }
}

pub type Result<T> = std::result::Result<T, Error>;

define_into_tonic_status!(Error);

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::EtcdFailed { .. }
            | Error::ConnectEtcd { .. }
            | Error::FileIo { .. }
            | Error::TcpBind { .. }
            | Error::SerializeToJson { .. }
            | Error::DeserializeFromJson { .. }
            | Error::NoLeader { .. }
            | Error::LeaderLeaseExpired { .. }
            | Error::LeaderLeaseChanged { .. }
            | Error::CreateChannel { .. }
            | Error::BatchGet { .. }
            | Error::Range { .. }
            | Error::ResponseHeaderNotFound { .. }
            | Error::InvalidHttpBody { .. }
            | Error::ExceededRetryLimit { .. }
            | Error::SendShutdownSignal { .. }
            | Error::PushMessage { .. }
            | Error::MailboxClosed { .. }
            | Error::MailboxReceiver { .. }
            | Error::StartGrpc { .. }
            | Error::PublishMessage { .. }
            | Error::Join { .. }
            | Error::ChooseItems { .. }
            | Error::FlowStateHandler { .. }
            | Error::BuildWalProvider { .. }
            | Error::BuildPartitionClient { .. }
            | Error::BuildKafkaClient { .. } => StatusCode::Internal,

            Error::DeleteRecords { .. }
            | Error::GetOffset { .. }
            | Error::PeerUnavailable { .. }
            | Error::PusherNotFound { .. } => StatusCode::Unexpected,
            Error::MailboxTimeout { .. } | Error::ExceededDeadline { .. } => StatusCode::Cancelled,
            Error::PruneTaskAlreadyRunning { .. }
            | Error::RetryLater { .. }
            | Error::MailboxChannelClosed { .. }
            | Error::IsNotLeader { .. } => StatusCode::IllegalState,
            Error::RetryLaterWithSource { source, .. } => source.status_code(),
            Error::SerializePartitionExpr { source, .. } => source.status_code(),

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
            | Error::ProcedureNotFound { .. }
            | Error::TooManyPartitions { .. }
            | Error::TomlFormat { .. }
            | Error::HandlerNotFound { .. }
            | Error::LeaderPeerChanged { .. }
            | Error::RepartitionSourceRegionMissing { .. }
            | Error::RepartitionTargetRegionMissing { .. }
            | Error::PartitionExprMismatch { .. }
            | Error::RepartitionSourceExprMismatch { .. } => StatusCode::InvalidArguments,
            Error::LeaseKeyFromUtf8 { .. }
            | Error::LeaseValueFromUtf8 { .. }
            | Error::InvalidRegionKeyFromUtf8 { .. }
            | Error::TableRouteNotFound { .. }
            | Error::TableInfoNotFound { .. }
            | Error::DatanodeTableNotFound { .. }
            | Error::InvalidUtf8Value { .. }
            | Error::UnexpectedInstructionReply { .. }
            | Error::Unexpected { .. }
            | Error::RegionOperatingRace { .. }
            | Error::RegionRouteNotFound { .. }
            | Error::MigrationAbort { .. }
            | Error::MigrationRunning { .. }
            | Error::RegionMigrated { .. } => StatusCode::Unexpected,
            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::SaveClusterInfo { source, .. }
            | Error::InvalidClusterInfoFormat { source, .. }
            | Error::InvalidDatanodeStatFormat { source, .. }
            | Error::InvalidNodeInfoFormat { source, .. } => source.status_code(),
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

            Error::NextSequence { source, .. }
            | Error::SetNextSequence { source, .. }
            | Error::PeekSequence { source, .. } => source.status_code(),
            Error::DowngradeLeader { source, .. } => source.status_code(),
            Error::RegisterProcedureLoader { source, .. } => source.status_code(),
            Error::SubmitDdlTask { source, .. }
            | Error::SubmitReconcileProcedure { source, .. } => source.status_code(),
            Error::ConvertProtoData { source, .. }
            | Error::TableMetadataManager { source, .. }
            | Error::RuntimeSwitchManager { source, .. }
            | Error::KvBackend { source, .. }
            | Error::UnexpectedLogicalRouteTable { source, .. }
            | Error::UpdateTopicNameValue { source, .. }
            | Error::ParseWalOptions { source, .. } => source.status_code(),
            Error::ListActiveFrontends { source, .. }
            | Error::ListActiveDatanodes { source, .. }
            | Error::ListActiveFlownodes { source, .. } => source.status_code(),
            Error::NoAvailableFrontend { .. } => StatusCode::IllegalState,

            Error::InitMetadata { source, .. }
            | Error::InitDdlManager { source, .. }
            | Error::InitReconciliationManager { source, .. } => source.status_code(),

            Error::BuildTlsOptions { source, .. } => source.status_code(),
            Error::Other { source, .. } => source.status_code(),
            Error::RepartitionCreateSubtasks { source, .. } => source.status_code(),
            Error::RepartitionSubprocedureStateReceiver { source, .. } => source.status_code(),
            Error::DeallocateRegions { source, .. } => source.status_code(),
            Error::NoEnoughAvailableNode { .. } => StatusCode::RuntimeResourcesExhausted,

            #[cfg(feature = "pg_kvbackend")]
            Error::CreatePostgresPool { .. }
            | Error::GetPostgresClient { .. }
            | Error::GetPostgresConnection { .. }
            | Error::PostgresExecution { .. } => StatusCode::Internal,
            #[cfg(feature = "mysql_kvbackend")]
            Error::MySqlExecution { .. }
            | Error::CreateMySqlPool { .. }
            | Error::ParseMySqlUrl { .. }
            | Error::DecodeSqlValue { .. }
            | Error::AcquireMySqlClient { .. } => StatusCode::Internal,
            #[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
            Error::SqlExecutionTimeout { .. } => StatusCode::Internal,
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
        if let Some(h2_err) = err.downcast_ref::<h2::Error>()
            && let Some(io_err) = h2_err.get_io()
        {
            return Some(io_err);
        }

        err = err.source()?;
    }
}
