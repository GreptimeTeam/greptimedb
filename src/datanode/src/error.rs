use std::any::Any;

use common_error::prelude::*;
use datatypes::arrow::error::ArrowError;
use storage::error::Error as StorageError;
use table::error::Error as TableError;

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

    #[snafu(display("Failed to execute physical plan, source: {}", source))]
    ExecutePhysicalPlan {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

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

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("Column {} not found in table {}", column_name, table_name))]
    ColumnNotFound {
        column_name: String,
        table_name: String,
    },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField { field: String, backtrace: Backtrace },

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

    #[snafu(display("Failed to register a new schema, source: {}", source))]
    RegisterSchema {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to decode as physical plan, source: {}", source))]
    IntoPhysicalPlan {
        #[snafu(backtrace)]
        source: common_grpc::Error,
    },

    #[snafu(display("Column datatype error, source: {}", source))]
    ColumnDataType {
        #[snafu(backtrace)]
        source: api::error::Error,
    },

    #[snafu(display("Invalid column default constraint, source: {}", source))]
    ColumnDefaultConstraint {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
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

    #[snafu(display("Failed to collect RecordBatches, source: {}", source))]
    CollectRecordBatches {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
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

    #[snafu(display("Failed to create a new RecordBatch, source: {}", source))]
    NewRecordBatch {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to create a new RecordBatches, source: {}", source))]
    NewRecordBatches {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Arrow computation error, source: {}", source))]
    ArrowComputation {
        backtrace: Backtrace,
        source: ArrowError,
    },

    #[snafu(display("Failed to cast an arrow array into vector, source: {}", source))]
    CastVector {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
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
        source: common_insert::error::Error,
    },

    #[snafu(display("Insert batch is empty"))]
    EmptyInsertBatch,
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ExecuteSql { source } => source.status_code(),
            Error::DecodeLogicalPlan { source } => source.status_code(),
            Error::ExecutePhysicalPlan { source } => source.status_code(),
            Error::NewCatalog { source } => source.status_code(),
            Error::FindTable { source, .. } => source.status_code(),
            Error::CreateTable { source, .. }
            | Error::GetTable { source, .. }
            | Error::AlterTable { source, .. } => source.status_code(),

            Error::Insert { source, .. } => source.status_code(),

            Error::TableNotFound { .. } => StatusCode::TableNotFound,
            Error::ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            Error::ParseSqlValue { source, .. } | Error::ParseSql { source, .. } => {
                source.status_code()
            }

            Error::CastVector { source, .. }
            | Error::ColumnDefaultConstraint { source, .. }
            | Error::CreateSchema { source, .. }
            | Error::ConvertSchema { source, .. } => source.status_code(),

            Error::ColumnValuesNumberMismatch { .. }
            | Error::InvalidSql { .. }
            | Error::KeyColumnNotFound { .. }
            | Error::InvalidPrimaryKey { .. }
            | Error::MissingField { .. }
            | Error::CatalogNotFound { .. }
            | Error::SchemaNotFound { .. }
            | Error::ConstraintNotSupported { .. }
            | Error::ParseTimestamp { .. } => StatusCode::InvalidArguments,

            // TODO(yingwen): Further categorize http error.
            Error::StartServer { .. }
            | Error::ParseAddr { .. }
            | Error::TcpBind { .. }
            | Error::StartGrpc { .. }
            | Error::CreateDir { .. }
            | Error::InsertSystemCatalog { .. }
            | Error::RegisterSchema { .. }
            | Error::Conversion { .. }
            | Error::IntoPhysicalPlan { .. }
            | Error::UnsupportedExpr { .. }
            | Error::ColumnDataType { .. }
            | Error::Catalog { .. } => StatusCode::Internal,

            Error::InitBackend { .. } => StatusCode::StorageUnavailable,
            Error::OpenLogStore { source } => source.status_code(),
            Error::StartScriptManager { source } => source.status_code(),
            Error::OpenStorageEngine { source } => source.status_code(),
            Error::RuntimeResource { .. } => StatusCode::RuntimeResourcesExhausted,
            Error::NewRecordBatch { source }
            | Error::NewRecordBatches { source }
            | Error::CollectRecordBatches { source } => source.status_code(),

            Error::ArrowComputation { .. } => StatusCode::Unexpected,
            Error::MetaClientInit { source, .. } => source.status_code(),
            Error::InsertData { source, .. } => source.status_code(),
            Error::EmptyInsertBatch => StatusCode::InvalidArguments,
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
    use std::str::FromStr;

    use common_error::ext::BoxedError;
    use common_error::mock::MockError;

    use super::*;

    fn throw_query_error() -> std::result::Result<(), query::error::Error> {
        Err(query::error::Error::new(MockError::with_backtrace(
            StatusCode::Internal,
        )))
    }

    fn throw_catalog_error() -> catalog::error::Result<()> {
        Err(catalog::error::Error::RegisterTable {
            source: BoxedError::new(MockError::with_backtrace(StatusCode::Internal)),
        })
    }

    fn throw_arrow_error() -> std::result::Result<(), ArrowError> {
        Err(ArrowError::NotYetImplemented("test".to_string()))
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
    fn test_arrow_computation_error() {
        let err = throw_arrow_error()
            .context(ArrowComputationSnafu)
            .unwrap_err();

        assert!(matches!(err, Error::ArrowComputation { .. }));
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::Unexpected, err.status_code());
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

    #[test]
    fn test_parse_timestamp() {
        let err = common_time::timestamp::Timestamp::from_str("test")
            .context(ParseTimestampSnafu { raw: "test" })
            .unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }
}
