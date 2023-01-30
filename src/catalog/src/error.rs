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
use std::fmt::Debug;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::prelude::{Snafu, StatusCode};
use datafusion::error::DataFusionError;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::RawSchema;
use snafu::{Backtrace, ErrorCompat};

use crate::DeregisterTableRequest;

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
        "System catalog table type mismatch, expected: binary, found: {:?}",
        data_type,
    ))]
    SystemCatalogTypeMismatch {
        data_type: ConcreteDataType,
        backtrace: Backtrace,
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

    #[snafu(display("Cannot find schema {} in catalog {}", schema, catalog))]
    SchemaNotFound {
        catalog: String,
        schema: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Table `{}` already exists", table))]
    TableExists { table: String, backtrace: Backtrace },

    #[snafu(display("Table `{}` not exist", table))]
    TableNotExist { table: String, backtrace: Backtrace },

    #[snafu(display("Schema {} already exists", schema))]
    SchemaExists {
        schema: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Operation {} not implemented yet", operation))]
    Unimplemented {
        operation: String,
        backtrace: Backtrace,
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

    #[snafu(display(
        "Failed to deregister table, request: {:?}, source: {}",
        request,
        source
    ))]
    DeregisterTable {
        request: DeregisterTableRequest,
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

    #[snafu(display("Failure during SchemaProvider operation, source: {}", source))]
    SchemaProviderOperation {
        #[snafu(backtrace)]
        source: BoxedError,
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

    #[snafu(display("Failed to perform metasrv operation, source: {}", source))]
    MetaSrv {
        #[snafu(backtrace)]
        source: meta_client::error::Error,
    },

    #[snafu(display("Invalid table info in catalog, source: {}", source))]
    InvalidTableInfoInCatalog {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
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
            | Error::InvalidEntryType { .. } => StatusCode::Unexpected,

            Error::SystemCatalog { .. }
            | Error::EmptyValue { .. }
            | Error::ValueDeserialize { .. } => StatusCode::StorageUnavailable,

            Error::SystemCatalogTypeMismatch { .. } => StatusCode::Internal,

            Error::ReadSystemCatalog { source, .. } => source.status_code(),
            Error::InvalidCatalogValue { source, .. } => source.status_code(),

            Error::TableExists { .. } => StatusCode::TableAlreadyExists,
            Error::TableNotExist { .. } => StatusCode::TableNotFound,
            Error::SchemaExists { .. } => StatusCode::InvalidArguments,

            Error::OpenSystemCatalog { source, .. }
            | Error::CreateSystemCatalog { source, .. }
            | Error::InsertCatalogRecord { source, .. }
            | Error::OpenTable { source, .. }
            | Error::CreateTable { source, .. }
            | Error::DeregisterTable { source, .. } => source.status_code(),

            Error::MetaSrv { source, .. } => source.status_code(),
            Error::SystemCatalogTableScan { source } => source.status_code(),
            Error::SystemCatalogTableScanExec { source } => source.status_code(),
            Error::InvalidTableSchema { source, .. } => source.status_code(),
            Error::InvalidTableInfoInCatalog { .. } => StatusCode::Unexpected,
            Error::SchemaProviderOperation { source } => source.status_code(),

            Error::Unimplemented { .. } => StatusCode::Unsupported,
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
            Error::SystemCatalog {
                msg: "".to_string(),
                backtrace: Backtrace::generate(),
            }
            .status_code()
        );

        assert_eq!(
            StatusCode::Internal,
            Error::SystemCatalogTypeMismatch {
                data_type: ConcreteDataType::binary_datatype(),
                backtrace: Backtrace::generate(),
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
