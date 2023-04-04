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

use common_datasource::error::Error as DataSourceError;
use common_error::prelude::*;
use common_procedure::ProcedureId;
use datafusion::parquet;
use snafu::Location;
use storage::error::Error as StorageError;
use table::error::Error as TableError;
use url::ParseError;

use crate::datanode::ObjectStoreConfig;

/// Business error of datanode.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to execute sql, source: {}", source))]
    ExecuteSql {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to plan statement, source: {}", source))]
    PlanStatement {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to execute statement, source: {}", source))]
    ExecuteStatement {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to execute logical plan, source: {}", source))]
    ExecuteLogicalPlan {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to decode logical plan, source: {}", source))]
    DecodeLogicalPlan {
        #[snafu(backtrace)]
        source: substrait::error::Error,
    },

    #[snafu(display("Incorrect internal state: {}", state))]
    IncorrectInternalState { state: String, location: Location },

    #[snafu(display("Failed to create catalog list, source: {}", source))]
    NewCatalog {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Catalog not found: {}", name))]
    CatalogNotFound { name: String, location: Location },

    #[snafu(display("Schema not found: {}", name))]
    SchemaNotFound { name: String, location: Location },

    #[snafu(display("Failed to create table: {}, source: {}", table_name, source))]
    CreateTable {
        table_name: String,
        #[snafu(backtrace)]
        source: TableError,
    },

    #[snafu(display("Failed to get table: {}, source: {}", table_name, source))]
    GetTable {
        table_name: String,
        #[snafu(backtrace)]
        source: TableError,
    },

    #[snafu(display("Failed to alter table {}, source: {}", table_name, source))]
    AlterTable {
        table_name: String,
        #[snafu(backtrace)]
        source: TableError,
    },

    #[snafu(display("Failed to drop table {}, source: {}", table_name, source))]
    DropTable {
        table_name: String,
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Table engine not found: {}, source: {}", engine_name, source))]
    TableEngineNotFound {
        engine_name: String,
        #[snafu(backtrace)]
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
        #[snafu(backtrace)]
        source: sql::error::Error,
    },

    #[snafu(display("Missing insert body"))]
    MissingInsertBody { location: Location },

    #[snafu(display("Failed to insert value to table: {}, source: {}", table_name, source))]
    Insert {
        table_name: String,
        #[snafu(backtrace)]
        source: TableError,
    },

    #[snafu(display(
        "Failed to delete value from table: {}, source: {}",
        table_name,
        source
    ))]
    Delete {
        table_name: String,
        #[snafu(backtrace)]
        source: TableError,
    },

    #[snafu(display("Failed to flush table: {}, source: {}", table_name, source))]
    FlushTable {
        table_name: String,
        #[snafu(backtrace)]
        source: TableError,
    },

    #[snafu(display("Failed to start server, source: {}", source))]
    StartServer {
        #[snafu(backtrace)]
        source: servers::error::Error,
    },

    #[snafu(display("Failed to parse address {}, source: {}", addr, source))]
    ParseAddr {
        addr: String,
        source: std::net::AddrParseError,
    },

    #[snafu(display("Failed to bind address {}, source: {}", addr, source))]
    TcpBind {
        addr: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to start gRPC server, source: {}", source))]
    StartGrpc { source: tonic::transport::Error },

    #[snafu(display("Failed to create directory {}, source: {}", dir, source))]
    CreateDir { dir: String, source: std::io::Error },

    #[snafu(display("Failed to remove directory {}, source: {}", dir, source))]
    RemoveDir { dir: String, source: std::io::Error },

    #[snafu(display("Failed to open log store, source: {}", source))]
    OpenLogStore {
        #[snafu(backtrace)]
        source: log_store::error::Error,
    },

    #[snafu(display("Failed to storage engine, source: {}", source))]
    OpenStorageEngine { source: StorageError },

    #[snafu(display("Failed to init backend, config: {:#?}, source: {}", config, source))]
    InitBackend {
        config: Box<ObjectStoreConfig>,
        source: object_store::Error,
        location: Location,
    },

    #[snafu(display("Failed to build backend, source: {}", source))]
    BuildBackend {
        #[snafu(backtrace)]
        source: DataSourceError,
    },

    #[snafu(display("Failed to parse url, source: {}", source))]
    ParseUrl {
        source: DataSourceError,
        location: Location,
    },

    #[snafu(display("Runtime resource error, source: {}", source))]
    RuntimeResource {
        #[snafu(backtrace)]
        source: common_runtime::error::Error,
    },

    #[snafu(display("Invalid SQL, error: {}", msg))]
    InvalidSql { msg: String },

    #[snafu(display("Invalid url: {}, error :{}", url, source))]
    InvalidUrl { url: String, source: ParseError },

    #[snafu(display("Invalid filepath: {}", path))]
    InvalidPath { path: String },

    #[snafu(display("Invalid connection: {}", msg))]
    InvalidConnection { msg: String },

    #[snafu(display("Unsupported backend protocol: {}", protocol))]
    UnsupportedBackendProtocol { protocol: String },

    #[snafu(display("Failed to regex, source: {}", source))]
    BuildRegex {
        location: Location,
        source: regex::Error,
    },

    #[snafu(display("Failed to list objects, source: {}", source))]
    ListObjects {
        #[snafu(backtrace)]
        source: DataSourceError,
    },

    #[snafu(display("Failed to parse the data, source: {}", source))]
    ParseDataTypes {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Not support SQL, error: {}", msg))]
    NotSupportSql { msg: String },

    #[snafu(display("Failed to convert datafusion schema, source: {}", source))]
    ConvertSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

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

    #[snafu(display("Failed to insert into system catalog table, source: {}", source))]
    InsertSystemCatalog {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to rename table, source: {}", source))]
    RenameTable {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to register a new schema, source: {}", source))]
    RegisterSchema {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Schema {} already exists", name))]
    SchemaExists { name: String, location: Location },

    #[snafu(display("Failed to convert alter expr to request: {}", source))]
    AlterExprToRequest {
        #[snafu(backtrace)]
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to convert create expr to request: {}", source))]
    CreateExprToRequest {
        #[snafu(backtrace)]
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display("Failed to parse SQL, source: {}", source))]
    ParseSql {
        #[snafu(backtrace)]
        source: sql::error::Error,
    },

    #[snafu(display(
        "Failed to parse string to timestamp, string: {}, source: {}",
        raw,
        source
    ))]
    ParseTimestamp {
        raw: String,
        #[snafu(backtrace)]
        source: common_time::error::Error,
    },

    #[snafu(display("Failed to access catalog, source: {}", source))]
    Catalog {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to find table {} from catalog, source: {}", table_name, source))]
    FindTable {
        table_name: String,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to initialize meta client, source: {}", source))]
    MetaClientInit {
        #[snafu(backtrace)]
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to insert data, source: {}", source))]
    InsertData {
        #[snafu(backtrace)]
        source: common_grpc_expr::error::Error,
    },

    #[snafu(display(
        "Table id provider not found, cannot execute SQL directly on datanode in distributed mode"
    ))]
    TableIdProviderNotFound { location: Location },

    #[snafu(display("Failed to bump table id, source: {}", source))]
    BumpTableId {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to do vector computation, source: {}", source))]
    VectorComputation {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Missing node id option in distributed mode"))]
    MissingNodeId { location: Location },

    #[snafu(display("Missing node id option in distributed mode"))]
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
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "No valid default value can be built automatically, column: {}",
        column,
    ))]
    ColumnNoneDefaultValue { column: String, location: Location },

    #[snafu(display("Failed to describe schema for given statement, source: {}", source))]
    DescribeStatement {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to copy data from table: {}, source: {}", table_name, source))]
    CopyTable {
        table_name: String,
        #[snafu(backtrace)]
        source: TableError,
    },

    #[snafu(display("Failed to execute table scan, source: {}", source))]
    TableScanExec {
        #[snafu(backtrace)]
        source: common_query::error::Error,
    },

    #[snafu(display(
        "File Schema mismatch, expected table schema: {} but found :{}",
        table_schema,
        file_schema
    ))]
    InvalidSchema {
        table_schema: String,
        file_schema: String,
    },

    #[snafu(display("Failed to read parquet file, source: {}", source))]
    ReadParquet {
        source: parquet::errors::ParquetError,
        location: Location,
    },

    #[snafu(display("Failed to poll stream, source: {}", source))]
    PollStream {
        source: datafusion_common::DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to build parquet record batch stream, source: {}", source))]
    BuildParquetRecordBatchStream {
        location: Location,
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to read object in path: {}, source: {}", path, source))]
    ReadObject {
        path: String,
        location: Location,
        source: object_store::Error,
    },

    #[snafu(display("Failed to write object into path: {}, source: {}", path, source))]
    WriteObject {
        path: String,
        location: Location,
        source: object_store::Error,
    },

    #[snafu(display("Unrecognized table option: {}", source))]
    UnrecognizedTableOption {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to recover procedure, source: {}", source))]
    RecoverProcedure {
        #[snafu(backtrace)]
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to submit procedure {}, source: {}", procedure_id, source))]
    SubmitProcedure {
        procedure_id: ProcedureId,
        #[snafu(backtrace)]
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to wait procedure {} done, source: {}", procedure_id, source))]
    WaitProcedure {
        procedure_id: ProcedureId,
        #[snafu(backtrace)]
        source: common_procedure::error::Error,
    },

    #[snafu(display("Failed to close table engine, source: {}", source))]
    CloseTableEngine {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to shutdown server, source: {}", source))]
    ShutdownServer {
        #[snafu(backtrace)]
        source: servers::error::Error,
    },

    #[snafu(display("Failed to shutdown instance, source: {}", source))]
    ShutdownInstance {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to copy table to parquet file, source: {}", source))]
    WriteParquet {
        #[snafu(backtrace)]
        source: storage::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            ExecuteSql { source }
            | PlanStatement { source }
            | ExecuteStatement { source }
            | ExecuteLogicalPlan { source }
            | DescribeStatement { source } => source.status_code(),

            DecodeLogicalPlan { source } => source.status_code(),
            NewCatalog { source } | RegisterSchema { source } => source.status_code(),
            FindTable { source, .. } => source.status_code(),
            CreateTable { source, .. } | GetTable { source, .. } | AlterTable { source, .. } => {
                source.status_code()
            }
            DropTable { source, .. } => source.status_code(),
            FlushTable { source, .. } => source.status_code(),

            Insert { source, .. } => source.status_code(),
            Delete { source, .. } => source.status_code(),
            TableEngineNotFound { source, .. } => source.status_code(),
            TableNotFound { .. } => StatusCode::TableNotFound,
            ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            ParseSqlValue { source, .. } | ParseSql { source, .. } => source.status_code(),

            AlterExprToRequest { source, .. }
            | CreateExprToRequest { source }
            | InsertData { source } => source.status_code(),

            ConvertSchema { source, .. } | VectorComputation { source } => source.status_code(),

            ColumnValuesNumberMismatch { .. }
            | InvalidSql { .. }
            | InvalidUrl { .. }
            | InvalidPath { .. }
            | InvalidConnection { .. }
            | UnsupportedBackendProtocol { .. }
            | BuildRegex { .. }
            | NotSupportSql { .. }
            | KeyColumnNotFound { .. }
            | IllegalPrimaryKeysDef { .. }
            | MissingTimestampColumn { .. }
            | CatalogNotFound { .. }
            | SchemaNotFound { .. }
            | ConstraintNotSupported { .. }
            | SchemaExists { .. }
            | ParseTimestamp { .. }
            | MissingInsertBody { .. }
            | DatabaseNotFound { .. }
            | MissingNodeId { .. }
            | MissingMetasrvOpts { .. }
            | ColumnNoneDefaultValue { .. }
            | ParseUrl { .. } => StatusCode::InvalidArguments,

            // TODO(yingwen): Further categorize http error.
            StartServer { .. }
            | ParseAddr { .. }
            | TcpBind { .. }
            | StartGrpc { .. }
            | CreateDir { .. }
            | RemoveDir { .. }
            | InsertSystemCatalog { .. }
            | RenameTable { .. }
            | Catalog { .. }
            | MissingRequiredField { .. }
            | BuildParquetRecordBatchStream { .. }
            | InvalidSchema { .. }
            | ParseDataTypes { .. }
            | IncorrectInternalState { .. }
            | ShutdownServer { .. }
            | ShutdownInstance { .. }
            | CloseTableEngine { .. } => StatusCode::Internal,

            BuildBackend { .. }
            | InitBackend { .. }
            | ReadParquet { .. }
            | WriteParquet { .. }
            | PollStream { .. }
            | ReadObject { .. }
            | WriteObject { .. }
            | ListObjects { .. } => StatusCode::StorageUnavailable,
            OpenLogStore { source } => source.status_code(),
            OpenStorageEngine { source } => source.status_code(),
            RuntimeResource { .. } => StatusCode::RuntimeResourcesExhausted,
            MetaClientInit { source, .. } => source.status_code(),
            TableIdProviderNotFound { .. } => StatusCode::Unsupported,
            BumpTableId { source, .. } => source.status_code(),
            ColumnDefaultValue { source, .. } => source.status_code(),
            CopyTable { source, .. } => source.status_code(),
            TableScanExec { source, .. } => source.status_code(),
            UnrecognizedTableOption { .. } => StatusCode::InvalidArguments,
            RecoverProcedure { source, .. } | SubmitProcedure { source, .. } => {
                source.status_code()
            }
            WaitProcedure { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        tonic::Status::from_error(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_timestamp() {
        let err = common_time::timestamp::Timestamp::from_str("test")
            .context(ParseTimestampSnafu { raw: "test" })
            .unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }
}
