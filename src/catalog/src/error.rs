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

    #[snafu(display(
        "Cannot find schema, catalog name: {}, schema name: {}",
        catalog_name,
        schema_name
    ))]
    SchemaNotFound {
        catalog_name: String,
        schema_name: String,
    },

    #[snafu(display("Table {} already exists", table))]
    TableExists { table: String, backtrace: Backtrace },

    #[snafu(display("Failed to register table"))]
    RegisterTable {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Unexpected CatalogProvider implementation"))]
    CatalogTypeMismatch { backtrace: Backtrace },

    #[snafu(display("Failed to open table, catalog name: {}, schema name: {}, table name: {}, table id: {}, source: {}", catalog_name, schema_name, table_name, table_id, source))]
    OpenTable {
        catalog_name: String,
        schema_name: String,
        table_name: String,
        table_id: u64,
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Table not found while opening table, catalog name: {}, schema name: {}, table name: {}, table id: {}", catalog_name, schema_name, table_name, table_id))]
    TableNotFound {
        catalog_name: String,
        schema_name: String,
        table_name: String,
        table_id: u64,
    },

    #[snafu(display("Failed to read system catalog table records"))]
    ReadSystemCatalog {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to build table schema for system catalog table"))]
    SystemCatalogSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::TableExists { .. } => StatusCode::TableAlreadyExists,
            Error::InvalidKey { .. } => StatusCode::Unexpected,
            Error::OpenSystemCatalog { .. } => StatusCode::Unexpected,
            Error::CreateSystemCatalog { .. } => StatusCode::Unexpected,
            Error::SystemCatalog { .. } => StatusCode::StorageUnavailable,
            Error::SystemCatalogTypeMismatch { .. } => StatusCode::StorageUnavailable,
            Error::EmptyValue => StatusCode::StorageUnavailable,
            Error::ValueDeserialize { .. } => StatusCode::StorageUnavailable,
            Error::CatalogNotFound { .. } => StatusCode::StorageUnavailable,
            Error::RegisterTable { .. } => StatusCode::Internal,
            Error::CatalogTypeMismatch { .. } => StatusCode::Unexpected,
            Error::SchemaNotFound { .. } => StatusCode::Unexpected,
            Error::OpenTable { .. } => StatusCode::StorageUnavailable,
            Error::TableNotFound { .. } => StatusCode::Unexpected,
            Error::ReadSystemCatalog { .. } => StatusCode::StorageUnavailable,
            Error::SystemCatalogSchema { .. } => StatusCode::Internal,
            Error::InvalidEntryType { .. } => StatusCode::Unexpected,
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
