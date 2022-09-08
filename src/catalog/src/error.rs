use std::any::Any;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::prelude::{Snafu, StatusCode};
use datafusion::error::DataFusionError;
use datatypes::arrow;
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
    InvalidEntryType { entry_type: Option<u8> },

    #[snafu(display("Invalid system catalog key: {:?}", key))]
    InvalidKey { key: Option<String> },

    #[snafu(display("Catalog value is not present"))]
    EmptyValue,

    #[snafu(display("Failed to deserialize value, source: {}", source))]
    ValueDeserialize {
        source: serde_json::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Cannot find catalog by name: {}", catalog_name))]
    CatalogNotFound { catalog_name: String },

    #[snafu(display("Cannot find schema, schema info: {}", schema_info))]
    SchemaNotFound { schema_info: String },

    #[snafu(display("Table {} already exists", table))]
    TableExists { table: String, backtrace: Backtrace },

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
    TableNotFound { table_info: String },

    #[snafu(display("Failed to read system catalog table records"))]
    ReadSystemCatalog {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display(
        "Failed to insert table creation record to system catalog, source: {}",
        source
    ))]
    InsertTableRecord {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Illegal catalog manager state: {}", msg))]
    IllegalManagerState { backtrace: Backtrace, msg: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidKey { .. }
            | Error::OpenSystemCatalog { .. }
            | Error::CreateSystemCatalog { .. }
            | Error::SchemaNotFound { .. }
            | Error::TableNotFound { .. }
            | Error::IllegalManagerState { .. }
            | Error::InvalidEntryType { .. } => StatusCode::Unexpected,
            Error::SystemCatalog { .. }
            | Error::SystemCatalogTypeMismatch { .. }
            | Error::EmptyValue
            | Error::ValueDeserialize { .. }
            | Error::CatalogNotFound { .. }
            | Error::OpenTable { .. }
            | Error::CreateTable { .. }
            | Error::ReadSystemCatalog { .. }
            | Error::InsertTableRecord { .. } => StatusCode::StorageUnavailable,
            Error::RegisterTable { .. } => StatusCode::Internal,
            Error::TableExists { .. } => StatusCode::TableAlreadyExists,
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
            Error::InvalidKey { key: None }.status_code()
        );

        assert_eq!(
            StatusCode::Unexpected,
            Error::OpenSystemCatalog {
                source: table::error::Error::new(MockError::new(StatusCode::StorageUnavailable))
            }
            .status_code()
        );

        assert_eq!(
            StatusCode::Unexpected,
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
            StatusCode::StorageUnavailable,
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
            Error::EmptyValue.status_code()
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
