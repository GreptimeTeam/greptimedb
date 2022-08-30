use std::any::Any;

use api::serde::DecodeError;
use common_error::ext::BoxedError;
use common_error::prelude::*;
use datatypes::prelude::ConcreteDataType;
use storage::error::Error as StorageError;
use table::error::Error as TableError;

/// Business error of datanode.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Fail to execute sql, source: {}", source))]
    ExecuteSql {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to execute physical plan, source: {}", source))]
    ExecutePhysicalPlan {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Fail to create catalog list, source: {}", source))]
    NewCatalog {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Fail to create table: {}, {}", table_name, source))]
    CreateTable {
        table_name: String,
        #[snafu(backtrace)]
        source: TableError,
    },

    #[snafu(display("Fail to get table: {}, {}", table_name, source))]
    GetTable {
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

    #[snafu(display(
        "Columns and values number mismatch, columns: {}, values: {}",
        columns,
        values
    ))]
    ColumnValuesNumberMismatch { columns: usize, values: usize },

    #[snafu(display("Fail to parse value: {}, {}", msg, backtrace))]
    ParseSqlValue { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Column {} expect type: {:?}, actual: {:?}",
        column_name,
        expect,
        actual
    ))]
    ColumnTypeMismatch {
        column_name: String,
        expect: ConcreteDataType,
        actual: ConcreteDataType,
    },

    #[snafu(display("Fail to insert value to table: {}, {}", table_name, source))]
    Insert {
        table_name: String,
        source: TableError,
    },

    #[snafu(display("Illegal insert data"))]
    IllegalInsertData,

    #[snafu(display("Fail to convert bytes to insert batch, {}", source))]
    DecodeInsert { source: DecodeError },

    #[snafu(display("Failed to start server, source: {}", source))]
    StartServer {
        #[snafu(backtrace)]
        source: servers::error::Error,
    },

    #[snafu(display("Fail to parse address {}, source: {}", addr, source))]
    ParseAddr {
        addr: String,
        source: std::net::AddrParseError,
    },

    #[snafu(display("Fail to bind address {}, source: {}", addr, source))]
    TcpBind {
        addr: String,
        source: std::io::Error,
    },

    #[snafu(display("Fail to start gRPC server, source: {}", source))]
    StartGrpc { source: tonic::transport::Error },

    #[snafu(display("Failed to create directory {}, source: {}", dir, source))]
    CreateDir { dir: String, source: std::io::Error },

    #[snafu(display("Failed to open log store, source: {}", source))]
    OpenLogStore { source: log_store::error::Error },

    #[snafu(display("Failed to storage engine, source: {}", source))]
    OpenStorageEngine { source: StorageError },

    #[snafu(display("Failed to init backend, dir: {}, source: {}", dir, source))]
    InitBackend {
        dir: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert datafusion type: {}", from))]
    Conversion { from: String },

    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr { name: String },

    #[snafu(display("Runtime resource error, source: {}", source))]
    RuntimeResource {
        #[snafu(backtrace)]
        source: common_runtime::error::Error,
    },

    #[snafu(display("Invalid CREATE TABLE sql statement, cause: {}", msg))]
    InvalidCreateTableSql { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to create schema when creating table: {}", source))]
    CreateSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to convert datafusion schema: {}", source))]
    ConvertSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("SQL data type not supported yet: {:?}", t))]
    SqlTypeNotSupported {
        t: sql::ast::DataType,
        backtrace: Backtrace,
    },

    #[snafu(display("Specified timestamp key or primary key column not found: {}", name))]
    KeyColumnNotFound { name: String, backtrace: Backtrace },

    #[snafu(display(
        "Constraint in CREATE TABLE statement is not supported yet: {}",
        constraint
    ))]
    ConstraintNotSupported {
        constraint: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to insert into system catalog table: {}", source))]
    InsertSystemCatalog {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Physical Plan occur error: {}", source))]
    PhysicalPlan {
        #[snafu(backtrace)]
        source: common_grpc::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ExecuteSql { source } => source.status_code(),
            Error::ExecutePhysicalPlan { source } => source.status_code(),
            Error::NewCatalog { source } => source.status_code(),
            Error::CreateTable { source, .. } => source.status_code(),
            Error::GetTable { source, .. } => source.status_code(),
            Error::Insert { source, .. } => source.status_code(),
            Error::ConvertSchema { source, .. } => source.status_code(),
            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::ColumnNotFound { .. } => StatusCode::TableColumnNotFound,
            Error::ColumnValuesNumberMismatch { .. }
            | Error::ParseSqlValue { .. }
            | Error::ColumnTypeMismatch { .. }
            | Error::IllegalInsertData { .. }
            | Error::DecodeInsert { .. }
            | Error::InvalidCreateTableSql { .. }
            | Error::SqlTypeNotSupported { .. }
            | Error::CreateSchema { .. }
            | Error::KeyColumnNotFound { .. }
            | Error::ConstraintNotSupported { .. } => StatusCode::InvalidArguments,
            // TODO(yingwen): Further categorize http error.
            Error::StartServer { .. }
            | Error::ParseAddr { .. }
            | Error::TcpBind { .. }
            | Error::StartGrpc { .. }
            | Error::CreateDir { .. }
            | Error::InsertSystemCatalog { .. }
            | Error::Conversion { .. }
            | Error::PhysicalPlan { .. }
            | Error::UnsupportedExpr { .. } => StatusCode::Internal,
            Error::InitBackend { .. } => StatusCode::StorageUnavailable,
            Error::OpenLogStore { source } => source.status_code(),
            Error::OpenStorageEngine { source } => source.status_code(),
            Error::RuntimeResource { .. } => StatusCode::RuntimeResourcesExhausted,
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
        tonic::Status::new(tonic::Code::Internal, err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use common_error::mock::MockError;

    use super::*;

    fn throw_query_error() -> std::result::Result<(), query::error::Error> {
        Err(query::error::Error::new(MockError::with_backtrace(
            StatusCode::Internal,
        )))
    }

    fn throw_catalog_error() -> std::result::Result<(), catalog::error::Error> {
        Err(catalog::error::Error::RegisterTable {
            source: BoxedError::new(MockError::with_backtrace(StatusCode::Internal)),
        })
    }

    fn assert_internal_error(err: &Error) {
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::Internal, err.status_code());
    }

    fn assert_tonic_internal_error(err: Error) {
        let s: tonic::Status = err.into();
        assert_eq!(s.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_error() {
        let err = throw_query_error().context(ExecuteSqlSnafu).err().unwrap();
        assert_internal_error(&err);
        assert_tonic_internal_error(err);
        let err = throw_catalog_error()
            .context(NewCatalogSnafu)
            .err()
            .unwrap();
        assert_internal_error(&err);
        assert_tonic_internal_error(err);
    }
}
