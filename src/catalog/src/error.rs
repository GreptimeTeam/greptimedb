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

    #[snafu(display("Invalid system catalog key: {:?}", key))]
    InvalidKey { key: Option<u8> },

    #[snafu(display("Catalog value is not present"))]
    EmptyValue,

    #[snafu(display("Failed to deserialize value, source: {}", source))]
    ValueDeserialize {
        source: serde_json::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Cannot find catalog by name: {}", name))]
    CatalogNotFound { name: String },

    #[snafu(display("Table {} already exists", table))]
    TableExists { table: String, backtrace: Backtrace },

    #[snafu(display("Failed to register table"))]
    RegisterTable {
        #[snafu(backtrace)]
        source: BoxedError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::TableExists { .. } => StatusCode::TableAlreadyExists,
            Error::InvalidKey { .. } => StatusCode::StorageUnavailable,
            Error::OpenSystemCatalog { .. } => StatusCode::Unexpected,
            Error::CreateSystemCatalog { .. } => StatusCode::Unexpected,
            Error::SystemCatalog { .. } => StatusCode::StorageUnavailable,
            Error::SystemCatalogTypeMismatch { .. } => StatusCode::StorageUnavailable,
            Error::EmptyValue => StatusCode::StorageUnavailable,
            Error::ValueDeserialize { .. } => StatusCode::StorageUnavailable,
            Error::CatalogNotFound { .. } => StatusCode::StorageUnavailable,
            Error::RegisterTable { .. } => StatusCode::Internal,
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
