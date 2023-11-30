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

use common_config::wal::WalProvider;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_procedure::ProcedureId;
use serde_json::error::Error as JsonError;
use servers::define_into_tonic_status;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;
use table::error::Error as TableError;

/// Business error of datanode.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to handle heartbeat response"))]
    HandleHeartbeatResponse {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to get info from meta server"))]
    GetMetadata {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to execute sql"))]
    ExecuteSql {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to plan statement"))]
    PlanStatement {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to execute statement"))]
    ExecuteStatement {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to execute logical plan"))]
    ExecuteLogicalPlan {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to decode logical plan"))]
    DecodeLogicalPlan {
        location: Location,
        source: substrait::error::Error,
    },

    #[snafu(display("Incorrect internal state: {}", state))]
    IncorrectInternalState { state: String, location: Location },

    #[snafu(display("Catalog not found: {}", name))]
    CatalogNotFound { name: String, location: Location },

    #[snafu(display("Schema not found: {}", name))]
    SchemaNotFound { name: String, location: Location },

    #[snafu(display("Failed to create table: {}", table_name))]
    CreateTable {
        table_name: String,
        location: Location,
        source: TableError,
    },

    #[snafu(display("Failed to drop table {}", table_name))]
    DropTable {
        table_name: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Table engine not found: {}", engine_name))]
    TableEngineNotFound {
        engine_name: String,
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Table engine procedure not found: {}", engine_name))]
    EngineProcedureNotFound {
        engine_name: String,
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound {
        table_name: String,
        location: Location,
    },

    #[snafu(display("Column {} not found in table {}", column_name, table_name))]
    ColumnNotFound {
        column_name: String,
        table_name: String,
    },

    #[snafu(display("Missing timestamp column in request"))]
    MissingTimestampColumn { location: Location },

    #[snafu(display(
        "Columns and values number mismatch, columns: {}, values: {}",
        columns,
        values
    ))]
    ColumnValuesNumberMismatch { columns: usize, values: usize },

    #[snafu(display("Missing insert body"))]
    MissingInsertBody {
        source: sql::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to insert value to table: {}", table_name))]
    Insert {
        table_name: String,
        location: Location,
        source: TableError,
    },

    #[snafu(display("Failed to delete value from table: {}", table_name))]
    Delete {
        table_name: String,
        location: Location,
        source: TableError,
    },

    #[snafu(display("Failed to flush table: {}", table_name))]
    FlushTable {
        table_name: String,
        location: Location,
        source: TableError,
    },

    #[snafu(display("Failed to start server"))]
    StartServer {
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to wait for GRPC serving"))]
    WaitForGrpcServing {
        source: servers::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse address {}", addr))]
    ParseAddr {
        addr: String,
        #[snafu(source)]
        error: std::net::AddrParseError,
    },

    #[snafu(display("Failed to create directory {}", dir))]
    CreateDir {
        dir: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to remove directory {}", dir))]
    RemoveDir {
        dir: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to open log store"))]
    OpenLogStore {
        location: Location,
        source: Box<log_store::error::Error>,
    },

    #[snafu(display("Failed to init backend"))]
    InitBackend {
        #[snafu(source)]
        error: object_store::Error,
        location: Location,
    },

    #[snafu(display("Runtime resource error"))]
    RuntimeResource {
        location: Location,
        source: common_runtime::error::Error,
    },

    #[snafu(display("Expect KvBackend but not found"))]
    MissingKvBackend { location: Location },

    #[snafu(display("Invalid SQL, error: {}", msg))]
    InvalidSql { msg: String },

    #[snafu(display("Not support SQL, error: {}", msg))]
    NotSupportSql { msg: String },

    #[snafu(display("Specified timestamp key or primary key column not found: {}", name))]
    KeyColumnNotFound { name: String, location: Location },

    #[snafu(display("Illegal primary keys definition: {}", msg))]
    IllegalPrimaryKeysDef { msg: String, location: Location },

    #[snafu(display(
        "Constraint in CREATE TABLE statement is not supported yet: {}",
        constraint
    ))]
    ConstraintNotSupported {
        constraint: String,
        location: Location,
    },

    #[snafu(display("Failed to register a new schema"))]
    RegisterSchema {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Schema {} already exists", name))]
    SchemaExists { name: String, location: Location },

    #[snafu(display("Failed to convert delete expr to request"))]
    DeleteExprToRequest {
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to parse SQL"))]
    ParseSql {
        location: Location,
        source: sql::error::Error,
    },

    #[snafu(display("Failed to prepare immutable table"))]
    PrepareImmutableTable {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to access catalog"))]
    Catalog {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to initialize meta client"))]
    MetaClientInit {
        location: Location,
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to insert data"))]
    InsertData {
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display(
        "Table id provider not found, cannot execute SQL directly on datanode in distributed mode"
    ))]
    TableIdProviderNotFound { location: Location },

    #[snafu(display("Failed to bump table id"))]
    BumpTableId {
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Missing node id in Datanode config"))]
    MissingNodeId { location: Location },

    #[snafu(display("Missing required field: {}", name))]
    MissingRequiredField { name: String, location: Location },

    #[snafu(display("Cannot find requested database: {}-{}", catalog, schema))]
    DatabaseNotFound { catalog: String, schema: String },

    #[snafu(display("Failed to build default value, column: {}", column))]
    ColumnDefaultValue {
        column: String,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "No valid default value can be built automatically, column: {}",
        column,
    ))]
    ColumnNoneDefaultValue { column: String, location: Location },

    #[snafu(display("Unrecognized table option"))]
    UnrecognizedTableOption {
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to submit procedure {}", procedure_id))]
    SubmitProcedure {
        procedure_id: ProcedureId,
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to wait procedure {} done", procedure_id))]
    WaitProcedure {
        procedure_id: ProcedureId,
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to shutdown server"))]
    ShutdownServer {
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to shutdown instance"))]
    ShutdownInstance {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to encode object into json"))]
    EncodeJson {
        location: Location,
        #[snafu(source)]
        error: JsonError,
    },

    #[snafu(display("Payload not exist"))]
    PayloadNotExist { location: Location },

    #[snafu(display("Missing WAL dir config"))]
    MissingWalDirConfig { location: Location },

    #[snafu(display("Failed to join task"))]
    JoinTask {
        #[snafu(source)]
        error: common_runtime::JoinError,
        location: Location,
    },

    #[snafu(display("Column datatype error"))]
    ColumnDataType {
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Unexpected, violated: {}", violated))]
    Unexpected {
        violated: String,
        location: Location,
    },

    #[snafu(display("Failed to handle request for region {}", region_id))]
    HandleRegionRequest {
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("RegionId {} not found", region_id))]
    RegionNotFound {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Region engine {} is not registered", name))]
    RegionEngineNotFound { name: String, location: Location },

    #[snafu(display("Unsupported gRPC request, kind: {}", kind))]
    UnsupportedGrpcRequest { kind: String, location: Location },

    #[snafu(display("Unsupported output type, expected: {}", expected))]
    UnsupportedOutput {
        expected: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to get metadata from engine {} for region_id {}",
        engine,
        region_id,
    ))]
    GetRegionMetadata {
        engine: String,
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to build region requests"))]
    BuildRegionRequests {
        location: Location,
        source: store_api::metadata::MetadataError,
    },

    #[snafu(display("Failed to stop region engine {}", name))]
    StopRegionEngine {
        name: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display(
        "The wal provider and the mode is unmatched, wal provider: {:?}, mode: {:?}",
        wal_provider,
        mode
    ))]
    UnexpectedWalProvider {
        wal_provider: WalProvider,
        mode: servers::Mode,
        location: Location,
    },

    #[snafu(display("Missing required raft-engine options"))]
    MissingRaftEngineOpts { location: Location },

    #[snafu(display("Missing required Kafka options"))]
    MissingKafkaOpts { location: Location },

    #[snafu(display("Failed to build a Kafka log store"))]
    BuildKafkaLogStore {
        location: Location,
        #[snafu(source)]
        error: log_store::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            ExecuteSql { source, .. }
            | PlanStatement { source, .. }
            | ExecuteStatement { source, .. }
            | ExecuteLogicalPlan { source, .. } => source.status_code(),

            BuildRegionRequests { source, .. } => source.status_code(),
            HandleHeartbeatResponse { source, .. } | GetMetadata { source, .. } => {
                source.status_code()
            }

            DecodeLogicalPlan { source, .. } => source.status_code(),
            RegisterSchema { source, .. } => source.status_code(),
            CreateTable { source, .. } => source.status_code(),
            DropTable { source, .. } => source.status_code(),
            FlushTable { source, .. } => source.status_code(),

            Insert { source, .. } => source.status_code(),
            Delete { source, .. } => source.status_code(),
            TableEngineNotFound { source, .. } | EngineProcedureNotFound { source, .. } => {
                source.status_code()
            }
            TableNotFound { .. } => StatusCode::TableNotFound,
            ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            ParseSql { source, .. } => source.status_code(),
            DeleteExprToRequest { source, .. } | InsertData { source, .. } => source.status_code(),

            ColumnValuesNumberMismatch { .. }
            | InvalidSql { .. }
            | NotSupportSql { .. }
            | KeyColumnNotFound { .. }
            | IllegalPrimaryKeysDef { .. }
            | MissingTimestampColumn { .. }
            | CatalogNotFound { .. }
            | SchemaNotFound { .. }
            | ConstraintNotSupported { .. }
            | SchemaExists { .. }
            | DatabaseNotFound { .. }
            | MissingNodeId { .. }
            | ColumnNoneDefaultValue { .. }
            | MissingWalDirConfig { .. }
            | PrepareImmutableTable { .. }
            | ColumnDataType { .. }
            | MissingKvBackend { .. } => StatusCode::InvalidArguments,

            EncodeJson { .. } | PayloadNotExist { .. } | Unexpected { .. } => {
                StatusCode::Unexpected
            }

            // TODO(yingwen): Further categorize http error.
            ParseAddr { .. }
            | CreateDir { .. }
            | RemoveDir { .. }
            | Catalog { .. }
            | MissingRequiredField { .. }
            | IncorrectInternalState { .. }
            | MissingInsertBody { .. }
            | ShutdownInstance { .. }
            | JoinTask { .. }
            | RegionEngineNotFound { .. }
            | UnsupportedOutput { .. }
            | GetRegionMetadata { .. } => StatusCode::Internal,

            RegionNotFound { .. } => StatusCode::RegionNotFound,

            StartServer { source, .. }
            | ShutdownServer { source, .. }
            | WaitForGrpcServing { source, .. } => source.status_code(),

            InitBackend { .. } => StatusCode::StorageUnavailable,

            OpenLogStore { source, .. } => source.status_code(),
            RuntimeResource { .. } => StatusCode::RuntimeResourcesExhausted,
            MetaClientInit { source, .. } => source.status_code(),
            TableIdProviderNotFound { .. } | UnsupportedGrpcRequest { .. } => {
                StatusCode::Unsupported
            }
            BumpTableId { source, .. } => source.status_code(),
            ColumnDefaultValue { source, .. } => source.status_code(),
            UnrecognizedTableOption { .. } => StatusCode::InvalidArguments,
            SubmitProcedure { source, .. } => source.status_code(),
            WaitProcedure { source, .. } => source.status_code(),
            HandleRegionRequest { source, .. } => source.status_code(),
            StopRegionEngine { source, .. } => source.status_code(),

            UnexpectedWalProvider { .. }
            | MissingRaftEngineOpts { .. }
            | MissingKafkaOpts { .. }
            | BuildKafkaLogStore { .. } => StatusCode::InvalidArguments,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_into_tonic_status!(Error);
