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

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_procedure::ProcedureId;
use serde_json::error::Error as JsonError;
use servers::define_into_tonic_status;
use snafu::{Location, Snafu};
use store_api::storage::{RegionId, RegionNumber};
use table::error::Error as TableError;
use table::metadata::TableId;

/// Business error of datanode.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to access catalog, source: {}", source))]
    AccessCatalog {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to deregister table: {}, source: {}", table_name, source))]
    DeregisterTable {
        table_name: String,
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to register table: {}, source: {}", table_name, source))]
    RegisterTable {
        table_name: String,
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to open table: {}, source: {}", table_name, source))]
    OpenTable {
        table_name: String,
        location: Location,
        source: TableError,
    },

    #[snafu(display("Failed to get table {table_id}, source: {source}, at {location}"))]
    GetTable {
        table_id: TableId,
        location: Location,
        source: TableError,
    },

    #[snafu(display(
        "Failed to close regions {:?} in table {}, source: {}",
        region_numbers,
        table_name,
        source
    ))]
    CloseTable {
        table_name: String,
        region_numbers: Vec<RegionNumber>,
        location: Location,
        source: TableError,
    },

    #[snafu(display(
        "Failed to check region {} in table: {}, source: {}",
        region_number,
        table_name,
        source
    ))]
    CheckRegion {
        table_name: String,
        location: Location,
        source: TableError,
        region_number: RegionNumber,
    },

    #[snafu(display("Failed to handle heartbeat response, source: {}", source))]
    HandleHeartbeatResponse {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to get info from meta server, source: {}", source))]
    GetMetadata {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to execute sql, source: {}", source))]
    ExecuteSql {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to plan statement, source: {}", source))]
    PlanStatement {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to execute statement, source: {}", source))]
    ExecuteStatement {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to execute logical plan, source: {}", source))]
    ExecuteLogicalPlan {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to decode logical plan, source: {}", source))]
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

    #[snafu(display("Failed to create table: {}, source: {}", table_name, source))]
    CreateTable {
        table_name: String,
        location: Location,
        source: TableError,
    },

    #[snafu(display("Failed to drop table {}, source: {}", table_name, source))]
    DropTable {
        table_name: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Table engine not found: {}, source: {}", engine_name, source))]
    TableEngineNotFound {
        engine_name: String,
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display(
        "Table engine procedure not found: {}, source: {}",
        engine_name,
        source
    ))]
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

    #[snafu(display("Failed to parse sql value, source: {}", source))]
    ParseSqlValue {
        location: Location,
        source: sql::error::Error,
    },

    #[snafu(display("Missing insert body, source: {source}"))]
    MissingInsertBody {
        source: sql::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to insert value to table: {}, source: {}", table_name, source))]
    Insert {
        table_name: String,
        location: Location,
        source: TableError,
    },

    #[snafu(display(
        "Failed to delete value from table: {}, source: {}",
        table_name,
        source
    ))]
    Delete {
        table_name: String,
        location: Location,
        source: TableError,
    },

    #[snafu(display("Failed to flush table: {}, source: {}", table_name, source))]
    FlushTable {
        table_name: String,
        location: Location,
        source: TableError,
    },

    #[snafu(display("Failed to start server, source: {}", source))]
    StartServer {
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to wait for GRPC serving, source: {}", source))]
    WaitForGrpcServing {
        source: servers::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse address {}, source: {}", addr, source))]
    ParseAddr {
        addr: String,
        source: std::net::AddrParseError,
    },

    #[snafu(display("Failed to create directory {}, source: {}", dir, source))]
    CreateDir { dir: String, source: std::io::Error },

    #[snafu(display("Failed to remove directory {}, source: {}", dir, source))]
    RemoveDir { dir: String, source: std::io::Error },

    #[snafu(display("Failed to open log store, source: {}", source))]
    OpenLogStore {
        location: Location,
        source: Box<log_store::error::Error>,
    },

    #[snafu(display("Failed to init backend, source: {}", source))]
    InitBackend {
        source: object_store::Error,
        location: Location,
    },

    #[snafu(display("Runtime resource error, source: {}", source))]
    RuntimeResource {
        location: Location,
        source: common_runtime::error::Error,
    },

    #[snafu(display("Expect KvBackend but not found, location: {}", location))]
    MissingKvBackend { location: Location },

    #[snafu(display("Expect MetaClient but not found, location: {}", location))]
    MissingMetaClient { location: Location },

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

    #[snafu(display("Failed to register a new schema, source: {}", source))]
    RegisterSchema {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Schema {} already exists", name))]
    SchemaExists { name: String, location: Location },

    #[snafu(display("Failed to convert alter expr to request: {}", source))]
    AlterExprToRequest {
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to convert create expr to request: {}", source))]
    CreateExprToRequest {
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to convert delete expr to request: {}", source))]
    DeleteExprToRequest {
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to parse SQL, source: {}", source))]
    ParseSql {
        location: Location,
        source: sql::error::Error,
    },

    #[snafu(display(
        "Failed to parse string to timestamp, string: {}, source: {}",
        raw,
        source
    ))]
    ParseTimestamp {
        raw: String,
        location: Location,
        source: common_time::error::Error,
    },

    #[snafu(display("Failed to prepare immutable table: {}", source))]
    PrepareImmutableTable {
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to access catalog, source: {}", source))]
    Catalog {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to initialize meta client, source: {}", source))]
    MetaClientInit {
        location: Location,
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to insert data, source: {}", source))]
    InsertData {
        location: Location,
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display(
        "Table id provider not found, cannot execute SQL directly on datanode in distributed mode"
    ))]
    TableIdProviderNotFound { location: Location },

    #[snafu(display("Failed to bump table id, source: {}", source))]
    BumpTableId {
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Missing node id in Datanode config, location: {}", location))]
    MissingNodeId { location: Location },

    #[snafu(display("Missing node id option in distributed mode, location: {}", location))]
    MissingMetasrvOpts { location: Location },

    #[snafu(display("Missing required field: {}", name))]
    MissingRequiredField { name: String, location: Location },

    #[snafu(display("Cannot find requested database: {}-{}", catalog, schema))]
    DatabaseNotFound { catalog: String, schema: String },

    #[snafu(display(
        "Failed to build default value, column: {}, source: {}",
        column,
        source
    ))]
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

    #[snafu(display("Unrecognized table option: {}", source))]
    UnrecognizedTableOption {
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to recover procedure, source: {}", source))]
    RecoverProcedure {
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to submit procedure {}, source: {}", procedure_id, source))]
    SubmitProcedure {
        procedure_id: ProcedureId,
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to wait procedure {} done, source: {}", procedure_id, source))]
    WaitProcedure {
        procedure_id: ProcedureId,
        location: Location,
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to close table engine, source: {}", source))]
    CloseTableEngine {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to shutdown server, source: {}", source))]
    ShutdownServer {
        location: Location,
        source: servers::error::Error,
    },

    #[snafu(display("Failed to shutdown instance, source: {}", source))]
    ShutdownInstance {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to encode object into json, source: {}", source))]
    EncodeJson {
        location: Location,
        source: JsonError,
    },

    #[snafu(display("Failed to decode object from json, source: {}", source))]
    DecodeJson {
        location: Location,
        source: JsonError,
    },

    #[snafu(display("Payload not exist"))]
    PayloadNotExist { location: Location },

    #[snafu(display("Failed to start the procedure manager"))]
    StartProcedureManager {
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to stop the procedure manager"))]
    StopProcedureManager {
        source: common_procedure::error::Error,
    },

    #[snafu(display("Missing WAL dir config"))]
    MissingWalDirConfig { location: Location },

    #[snafu(display("Failed to join task, source: {}", source))]
    JoinTask {
        source: common_runtime::JoinError,
        location: Location,
    },

    #[snafu(display(
        "Invalid insert row len, table: {}, expected: {}, actual: {}",
        table_name,
        expected,
        actual
    ))]
    InvalidInsertRowLen {
        table_name: String,
        expected: usize,
        actual: usize,
        location: Location,
    },

    #[snafu(display("Column datatype error, source: {}", source))]
    ColumnDataType {
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Failed to create vector, source: {}", source))]
    CreateVector {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Unexpected, violated: {}", violated))]
    Unexpected {
        violated: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to handle request for region {}, source: {}, location: {}",
        region_id,
        source,
        location
    ))]
    HandleRegionRequest {
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("RegionId {} not found, location: {}", region_id, location))]
    RegionNotFound {
        region_id: RegionId,
        location: Location,
    },

    #[snafu(display("Region engine {} is not registered, location: {}", name, location))]
    RegionEngineNotFound { name: String, location: Location },

    #[snafu(display("Unsupported gRPC request, kind: {}, location: {}", kind, location))]
    UnsupportedGrpcRequest { kind: String, location: Location },

    #[snafu(display(
        "Unsupported output type, expected: {}, location: {}",
        expected,
        location
    ))]
    UnsupportedOutput {
        expected: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to get metadata from engine {} for region_id {}, location: {}, source: {}",
        engine,
        region_id,
        location,
        source
    ))]
    GetRegionMetadata {
        engine: String,
        region_id: RegionId,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display(
        "Failed to build region requests, location:{}, source: {}",
        location,
        source
    ))]
    BuildRegionRequests {
        location: Location,
        source: store_api::metadata::MetadataError,
    },

    #[snafu(display(
        "Failed to stop region engine {}, location:{}, source: {}",
        name,
        location,
        source
    ))]
    StopRegionEngine {
        name: String,
        location: Location,
        source: BoxedError,
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
            CreateVector { source, .. } => source.status_code(),
            TableNotFound { .. } => StatusCode::TableNotFound,
            ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            ParseSqlValue { source, .. } | ParseSql { source, .. } => source.status_code(),

            AlterExprToRequest { source, .. }
            | CreateExprToRequest { source, .. }
            | DeleteExprToRequest { source, .. }
            | InsertData { source, .. } => source.status_code(),

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
            | ParseTimestamp { .. }
            | DatabaseNotFound { .. }
            | MissingNodeId { .. }
            | MissingMetasrvOpts { .. }
            | ColumnNoneDefaultValue { .. }
            | MissingWalDirConfig { .. }
            | PrepareImmutableTable { .. }
            | InvalidInsertRowLen { .. }
            | ColumnDataType { .. }
            | MissingKvBackend { .. }
            | MissingMetaClient { .. } => StatusCode::InvalidArguments,

            EncodeJson { .. } | DecodeJson { .. } | PayloadNotExist { .. } | Unexpected { .. } => {
                StatusCode::Unexpected
            }

            AccessCatalog { source, .. }
            | DeregisterTable { source, .. }
            | RegisterTable { source, .. } => source.status_code(),

            CheckRegion { source, .. }
            | OpenTable { source, .. }
            | CloseTable { source, .. }
            | GetTable { source, .. } => source.status_code(),

            // TODO(yingwen): Further categorize http error.
            ParseAddr { .. }
            | CreateDir { .. }
            | RemoveDir { .. }
            | Catalog { .. }
            | MissingRequiredField { .. }
            | IncorrectInternalState { .. }
            | MissingInsertBody { .. }
            | ShutdownInstance { .. }
            | CloseTableEngine { .. }
            | JoinTask { .. }
            | RegionNotFound { .. }
            | RegionEngineNotFound { .. }
            | UnsupportedOutput { .. }
            | GetRegionMetadata { .. } => StatusCode::Internal,

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
            RecoverProcedure { source, .. } | SubmitProcedure { source, .. } => {
                source.status_code()
            }
            WaitProcedure { source, .. } => source.status_code(),
            StartProcedureManager { source } | StopProcedureManager { source } => {
                source.status_code()
            }
            HandleRegionRequest { source, .. } => source.status_code(),
            StopRegionEngine { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_into_tonic_status!(Error);

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use snafu::ResultExt;

    use super::*;

    #[test]
    fn test_parse_timestamp() {
        let err = common_time::timestamp::Timestamp::from_str("test")
            .context(ParseTimestampSnafu { raw: "test" })
            .unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }
}
