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

use common_error::prelude::*;
use storage::error::Error as StorageError;
use table::error::Error as TableError;

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

    #[snafu(display("Failed to decode logical plan, source: {}", source))]
    DecodeLogicalPlan {
        #[snafu(backtrace)]
        source: substrait::error::Error,
    },

    #[snafu(display("Incorrect internal state: {}", state))]
    IncorrectInternalState { state: String, backtrace: Backtrace },

    #[snafu(display("Failed to create catalog list, source: {}", source))]
    NewCatalog {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Catalog not found: {}", name))]
    CatalogNotFound { name: String, backtrace: Backtrace },

    #[snafu(display("Schema not found: {}", name))]
    SchemaNotFound { name: String, backtrace: Backtrace },

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

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("Column {} not found in table {}", column_name, table_name))]
    ColumnNotFound {
        column_name: String,
        table_name: String,
    },

    #[snafu(display("Missing timestamp column in request"))]
    MissingTimestampColumn { backtrace: Backtrace },

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

    #[snafu(display("Failed to insert value to table: {}, source: {}", table_name, source))]
    Insert {
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
        backtrace: Backtrace,
    },

    #[snafu(display("Runtime resource error, source: {}", source))]
    RuntimeResource {
        #[snafu(backtrace)]
        source: common_runtime::error::Error,
    },

    #[snafu(display("Invalid SQL, error: {}", msg))]
    InvalidSql { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to create schema when creating table, source: {}", source))]
    CreateSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to convert datafusion schema, source: {}", source))]
    ConvertSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Specified timestamp key or primary key column not found: {}", name))]
    KeyColumnNotFound { name: String, backtrace: Backtrace },

    #[snafu(display("Invalid primary key: {}", msg))]
    InvalidPrimaryKey { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Constraint in CREATE TABLE statement is not supported yet: {}",
        constraint
    ))]
    ConstraintNotSupported {
        constraint: String,
        backtrace: Backtrace,
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

    #[snafu(display("Schema already exists, name: {}", name))]
    SchemaExists { name: String, backtrace: Backtrace },

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

    #[snafu(display("Failed to start script manager, source: {}", source))]
    StartScriptManager {
        #[snafu(backtrace)]
        source: script::error::Error,
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
    TableIdProviderNotFound { backtrace: Backtrace },

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
    MissingNodeId { backtrace: Backtrace },

    #[snafu(display("Missing node id option in distributed mode"))]
    MissingMetasrvOpts { backtrace: Backtrace },

    #[snafu(display("Missing required field: {}", name))]
    MissingRequiredField { name: String, backtrace: Backtrace },

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
    ColumnNoneDefaultValue {
        column: String,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ExecuteSql { source } => source.status_code(),
            Error::DecodeLogicalPlan { source } => source.status_code(),
            Error::NewCatalog { source } => source.status_code(),
            Error::FindTable { source, .. } => source.status_code(),
            Error::CreateTable { source, .. }
            | Error::GetTable { source, .. }
            | Error::AlterTable { source, .. } => source.status_code(),
            Error::DropTable { source, .. } => source.status_code(),

            Error::Insert { source, .. } => source.status_code(),

            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            Error::ParseSqlValue { source, .. } | Error::ParseSql { source, .. } => {
                source.status_code()
            }

            Error::AlterExprToRequest { source, .. }
            | Error::CreateExprToRequest { source }
            | Error::InsertData { source } => source.status_code(),

            Error::CreateSchema { source, .. }
            | Error::ConvertSchema { source, .. }
            | Error::VectorComputation { source } => source.status_code(),

            Error::ColumnValuesNumberMismatch { .. }
            | Error::InvalidSql { .. }
            | Error::KeyColumnNotFound { .. }
            | Error::InvalidPrimaryKey { .. }
            | Error::MissingTimestampColumn { .. }
            | Error::CatalogNotFound { .. }
            | Error::SchemaNotFound { .. }
            | Error::ConstraintNotSupported { .. }
            | Error::SchemaExists { .. }
            | Error::ParseTimestamp { .. }
            | Error::DatabaseNotFound { .. } => StatusCode::InvalidArguments,

            // TODO(yingwen): Further categorize http error.
            Error::StartServer { .. }
            | Error::ParseAddr { .. }
            | Error::TcpBind { .. }
            | Error::StartGrpc { .. }
            | Error::CreateDir { .. }
            | Error::InsertSystemCatalog { .. }
            | Error::RenameTable { .. }
            | Error::RegisterSchema { .. }
            | Error::Catalog { .. }
            | Error::MissingRequiredField { .. }
            | Error::IncorrectInternalState { .. } => StatusCode::Internal,

            Error::InitBackend { .. } => StatusCode::StorageUnavailable,
            Error::OpenLogStore { source } => source.status_code(),
            Error::StartScriptManager { source } => source.status_code(),
            Error::OpenStorageEngine { source } => source.status_code(),
            Error::RuntimeResource { .. } => StatusCode::RuntimeResourcesExhausted,
            Error::MetaClientInit { source, .. } => source.status_code(),
            Error::TableIdProviderNotFound { .. } => StatusCode::Unsupported,
            Error::BumpTableId { source, .. } => source.status_code(),
            Error::MissingNodeId { .. } => StatusCode::InvalidArguments,
            Error::MissingMetasrvOpts { .. } => StatusCode::InvalidArguments,
            Error::ColumnDefaultValue { source, .. } => source.status_code(),
            Error::ColumnNoneDefaultValue { .. } => StatusCode::InvalidArguments,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
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
    use std::error::Error as StdError;
    use std::str::FromStr;

    use common_error::ext::BoxedError;
    use common_error::mock::MockError;

    use super::*;

    fn throw_query_error() -> std::result::Result<(), query::error::Error> {
        query::error::CatalogNotFoundSnafu {
            catalog: String::new(),
        }
        .fail()
    }

    fn throw_catalog_error() -> catalog::error::Result<()> {
        Err(catalog::error::Error::SchemaProviderOperation {
            source: BoxedError::new(MockError::with_backtrace(StatusCode::Internal)),
        })
    }

    fn assert_internal_error(err: &Error) {
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::Internal, err.status_code());
    }

    fn assert_invalid_argument_error(err: &Error) {
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    fn assert_tonic_internal_error(err: Error) {
        let status_code = err.status_code();
        let err_string = err.to_string();

        let s: tonic::Status = err.into();
        assert_eq!(s.code(), tonic::Code::Unknown);

        let source = s.source().unwrap().downcast_ref::<Error>().unwrap();
        assert_eq!(source.status_code(), status_code);
        assert_eq!(source.to_string(), err_string);
    }

    #[test]
    fn test_error() {
        let err = throw_query_error().context(ExecuteSqlSnafu).err().unwrap();
        assert_invalid_argument_error(&err);
        assert_tonic_internal_error(err);
        let err = throw_catalog_error()
            .context(NewCatalogSnafu)
            .err()
            .unwrap();
        assert_internal_error(&err);
        assert_tonic_internal_error(err);
    }

    #[test]
    fn test_parse_timestamp() {
        let err = common_time::timestamp::Timestamp::from_str("test")
            .context(ParseTimestampSnafu { raw: "test" })
            .unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }
}
