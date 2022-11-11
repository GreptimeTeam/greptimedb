use std::any::Any;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::prelude::{Snafu, StatusCode};
use datafusion::error::DataFusionError;
use datatypes::arrow;
use datatypes::schema::RawSchema;
use snafu::{Backtrace, ErrorCompat};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to open system catalog table, source: {}", source))]
    OpenSystemCatalog {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to create system catalog table, source: {}", source))]
    CreateSystemCatalog {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display(
        "Failed to create table, table info: {}, source: {}",
        table_info,
        source
    ))]
    CreateTable {
        table_info: String,
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("System catalog is not valid: {}", msg))]
    SystemCatalog { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "System catalog table type mismatch, expected: binary, found: {:?} source: {}",
        data_type,
        source
    ))]
    SystemCatalogTypeMismatch {
        data_type: arrow::datatypes::DataType,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid system catalog entry type: {:?}", entry_type))]
    InvalidEntryType {
        entry_type: Option<u8>,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid system catalog key: {:?}", key))]
    InvalidKey {
        key: Option<String>,
        backtrace: Backtrace,
    },

    #[snafu(display("Catalog value is not present"))]
    EmptyValue { backtrace: Backtrace },

    #[snafu(display("Failed to deserialize value, source: {}", source))]
    ValueDeserialize {
        source: serde_json::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Cannot find catalog by name: {}", catalog_name))]
    CatalogNotFound {
        catalog_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Cannot find schema, schema info: {}", schema_info))]
    SchemaNotFound {
        schema_info: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Table {} already exists", table))]
    TableExists { table: String, backtrace: Backtrace },

    #[snafu(display("Schema {} already exists", schema))]
    SchemaExists {
        schema: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to register table"))]
    RegisterTable {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to open table, table info: {}, source: {}", table_info, source))]
    OpenTable {
        table_info: String,
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Table not found while opening table, table info: {}", table_info))]
    TableNotFound {
        table_info: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to read system catalog table records"))]
    ReadSystemCatalog {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display(
        "Failed to insert table creation record to system catalog, source: {}",
        source
    ))]
    InsertCatalogRecord {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Illegal catalog manager state: {}", msg))]
    IllegalManagerState { backtrace: Backtrace, msg: String },

    #[snafu(display("Failed to scan system catalog table, source: {}", source))]
    SystemCatalogTableScan {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display(
        "Invalid table schema in catalog entry, table:{}, schema: {:?}, source: {}",
        table_info,
        schema,
        source
    ))]
    InvalidTableSchema {
        table_info: String,
        schema: RawSchema,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to execute system catalog table scan, source: {}", source))]
    SystemCatalogTableScanExec {
        #[snafu(backtrace)]
        source: common_query::error::Error,
    },
    #[snafu(display("Cannot parse catalog value, source: {}", source))]
    InvalidCatalogValue {
        #[snafu(backtrace)]
        source: common_catalog::error::Error,
    },

    #[snafu(display("IO error occurred while fetching catalog info, source: {}", source))]
    Io {
        backtrace: Backtrace,
        source: std::io::Error,
    },

    #[snafu(display("Local and remote catalog data are inconsistent, msg: {}", msg))]
    CatalogStateInconsistent { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to perform metasrv operation, source: {}", source))]
    MetaSrv {
        #[snafu(backtrace)]
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to bump table id"))]
    BumpTableId { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to parse table id from metasrv, data: {:?}", data))]
    ParseTableId { data: String, backtrace: Backtrace },

    #[snafu(display("Failed to deserialize partition rule from string: {:?}", data))]
    DeserializePartitionRule {
        data: String,
        source: serde_json::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid table schema in catalog, source: {:?}", source))]
    InvalidSchemaInCatalog {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Catalog internal error: {}", source))]
    Internal {
        #[snafu(backtrace)]
        source: BoxedError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidKey { .. }
            | Error::SchemaNotFound { .. }
            | Error::TableNotFound { .. }
            | Error::IllegalManagerState { .. }
            | Error::CatalogNotFound { .. }
            | Error::InvalidEntryType { .. }
            | Error::CatalogStateInconsistent { .. } => StatusCode::Unexpected,

            Error::SystemCatalog { .. }
            | Error::EmptyValue { .. }
            | Error::ValueDeserialize { .. }
            | Error::Io { .. } => StatusCode::StorageUnavailable,

            Error::ReadSystemCatalog { source, .. } => source.status_code(),
            Error::SystemCatalogTypeMismatch { source, .. } => source.status_code(),
            Error::InvalidCatalogValue { source, .. } => source.status_code(),

            Error::RegisterTable { .. } => StatusCode::Internal,
            Error::TableExists { .. } => StatusCode::TableAlreadyExists,
            Error::SchemaExists { .. } => StatusCode::InvalidArguments,

            Error::OpenSystemCatalog { source, .. }
            | Error::CreateSystemCatalog { source, .. }
            | Error::InsertCatalogRecord { source, .. }
            | Error::OpenTable { source, .. }
            | Error::CreateTable { source, .. } => source.status_code(),
            Error::MetaSrv { source, .. } => source.status_code(),
            Error::SystemCatalogTableScan { source } => source.status_code(),
            Error::SystemCatalogTableScanExec { source } => source.status_code(),
            Error::InvalidTableSchema { source, .. } => source.status_code(),
            Error::BumpTableId { .. } | Error::ParseTableId { .. } => {
                StatusCode::StorageUnavailable
            }
            Error::DeserializePartitionRule { .. } => StatusCode::Unexpected,
            Error::InvalidSchemaInCatalog { .. } => StatusCode::Unexpected,
            Error::Internal { source, .. } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        DataFusionError::Internal(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use common_error::mock::MockError;
    use datatypes::arrow::datatypes::DataType;
    use snafu::GenerateImplicitData;

    use super::*;

    #[test]
    pub fn test_error_status_code() {
        assert_eq!(
            StatusCode::TableAlreadyExists,
            Error::TableExists {
                table: "some_table".to_string(),
                backtrace: Backtrace::generate(),
            }
            .status_code()
        );

        assert_eq!(
            StatusCode::Unexpected,
            InvalidKeySnafu { key: None }.build().status_code()
        );

        assert_eq!(
            StatusCode::StorageUnavailable,
            Error::OpenSystemCatalog {
                source: table::error::Error::new(MockError::new(StatusCode::StorageUnavailable))
            }
            .status_code()
        );

        assert_eq!(
            StatusCode::StorageUnavailable,
            Error::CreateSystemCatalog {
                source: table::error::Error::new(MockError::new(StatusCode::StorageUnavailable))
            }
            .status_code()
        );

        assert_eq!(
            StatusCode::StorageUnavailable,
            Error::SystemCatalog {
                msg: "".to_string(),
                backtrace: Backtrace::generate(),
            }
            .status_code()
        );

        assert_eq!(
            StatusCode::Internal,
            Error::SystemCatalogTypeMismatch {
                data_type: DataType::Boolean,
                source: datatypes::error::Error::UnsupportedArrowType {
                    arrow_type: DataType::Boolean,
                    backtrace: Backtrace::generate()
                }
            }
            .status_code()
        );
        assert_eq!(
            StatusCode::StorageUnavailable,
            EmptyValueSnafu {}.build().status_code()
        );
    }

    #[test]
    pub fn test_errors_to_datafusion_error() {
        let e: DataFusionError = Error::TableExists {
            table: "test_table".to_string(),
            backtrace: Backtrace::generate(),
        }
        .into();
        match e {
            DataFusionError::Internal(_) => {}
            _ => {
                panic!("catalog error should be converted to DataFusionError::Internal")
            }
        }
    }
}
