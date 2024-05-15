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
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datafusion::error::DataFusionError;
use datatypes::prelude::ConcreteDataType;
use snafu::{Location, Snafu};
use table::metadata::TableId;
use tokio::task::JoinError;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to list catalogs"))]
    ListCatalogs {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to list {}'s schemas", catalog))]
    ListSchemas {
        #[snafu(implicit)]
        location: Location,
        catalog: String,
        source: BoxedError,
    },

    #[snafu(display("Failed to list {}.{}'s tables", catalog, schema))]
    ListTables {
        #[snafu(implicit)]
        location: Location,
        catalog: String,
        schema: String,
        source: BoxedError,
    },

    #[snafu(display("Failed to list nodes in cluster: {source}"))]
    ListNodes {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to re-compile script due to internal error"))]
    CompileScriptInternal {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },
    #[snafu(display("Failed to open system catalog table"))]
    OpenSystemCatalog {
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to create system catalog table"))]
    CreateSystemCatalog {
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to create table, table info: {}", table_info))]
    CreateTable {
        table_info: String,
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("System catalog is not valid: {}", msg))]
    SystemCatalog {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "System catalog table type mismatch, expected: binary, found: {:?}",
        data_type,
    ))]
    SystemCatalogTypeMismatch {
        data_type: ConcreteDataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid system catalog entry type: {:?}", entry_type))]
    InvalidEntryType {
        entry_type: Option<u8>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid system catalog key: {:?}", key))]
    InvalidKey {
        key: Option<String>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Catalog value is not present"))]
    EmptyValue {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize value"))]
    ValueDeserialize {
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table engine not found: {}", engine_name))]
    TableEngineNotFound {
        engine_name: String,
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Cannot find catalog by name: {}", catalog_name))]
    CatalogNotFound {
        catalog_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot find schema {} in catalog {}", schema, catalog))]
    SchemaNotFound {
        catalog: String,
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table `{}` already exists", table))]
    TableExists {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table not found: {}", table))]
    TableNotExist {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema {} already exists", schema))]
    SchemaExists {
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Operation {} not implemented yet", operation))]
    Unimplemented {
        operation: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Operation {} not supported", op))]
    NotSupported {
        op: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to open table {table_id}"))]
    OpenTable {
        table_id: TableId,
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to open table in parallel"))]
    ParallelOpenTable {
        #[snafu(source)]
        error: JoinError,
    },

    #[snafu(display("Table not found while opening table, table info: {}", table_info))]
    TableNotFound {
        table_info: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find table partitions"))]
    FindPartitions { source: partition::error::Error },

    #[snafu(display("Failed to find region routes"))]
    FindRegionRoutes { source: partition::error::Error },

    #[snafu(display("Failed to read system catalog table records"))]
    ReadSystemCatalog {
        #[snafu(implicit)]
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to create recordbatch"))]
    CreateRecordBatch {
        #[snafu(implicit)]
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to insert table creation record to system catalog"))]
    InsertCatalogRecord {
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to scan system catalog table"))]
    SystemCatalogTableScan {
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Internal error"))]
    Internal {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to upgrade weak catalog manager reference"))]
    UpgradeWeakCatalogManagerRef {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to execute system catalog table scan"))]
    SystemCatalogTableScanExec {
        #[snafu(implicit)]
        location: Location,
        source: common_query::error::Error,
    },

    #[snafu(display("Cannot parse catalog value"))]
    InvalidCatalogValue {
        #[snafu(implicit)]
        location: Location,
        source: common_catalog::error::Error,
    },

    #[snafu(display("Failed to perform metasrv operation"))]
    Metasrv {
        #[snafu(implicit)]
        location: Location,
        source: meta_client::error::Error,
    },

    #[snafu(display("Invalid table info in catalog"))]
    InvalidTableInfoInCatalog {
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Illegal access to catalog: {} and schema: {}", catalog, schema))]
    QueryAccessDenied { catalog: String, schema: String },

    #[snafu(display("DataFusion error"))]
    Datafusion {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table schema mismatch"))]
    TableSchemaMismatch {
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("A generic error has occurred, msg: {}", msg))]
    Generic {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table metadata manager error"))]
    TableMetadataManager {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get table cache"))]
    GetTableCache {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get view info from cache"))]
    GetViewCache {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cache not found: {name}"))]
    CacheNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidKey { .. }
            | Error::SchemaNotFound { .. }
            | Error::CatalogNotFound { .. }
            | Error::FindPartitions { .. }
            | Error::FindRegionRoutes { .. }
            | Error::InvalidEntryType { .. }
            | Error::CacheNotFound { .. }
            | Error::ParallelOpenTable { .. } => StatusCode::Unexpected,

            Error::TableNotFound { .. } => StatusCode::TableNotFound,

            Error::SystemCatalog { .. }
            | Error::EmptyValue { .. }
            | Error::ValueDeserialize { .. } => StatusCode::StorageUnavailable,

            Error::Generic { .. }
            | Error::SystemCatalogTypeMismatch { .. }
            | Error::UpgradeWeakCatalogManagerRef { .. } => StatusCode::Internal,

            Error::ReadSystemCatalog { source, .. } | Error::CreateRecordBatch { source, .. } => {
                source.status_code()
            }
            Error::InvalidCatalogValue { source, .. } => source.status_code(),

            Error::TableExists { .. } => StatusCode::TableAlreadyExists,
            Error::TableNotExist { .. } => StatusCode::TableNotFound,
            Error::SchemaExists { .. } | Error::TableEngineNotFound { .. } => {
                StatusCode::InvalidArguments
            }

            Error::ListCatalogs { source, .. }
            | Error::ListNodes { source, .. }
            | Error::ListSchemas { source, .. }
            | Error::ListTables { source, .. } => source.status_code(),

            Error::OpenSystemCatalog { source, .. }
            | Error::CreateSystemCatalog { source, .. }
            | Error::InsertCatalogRecord { source, .. }
            | Error::OpenTable { source, .. }
            | Error::CreateTable { source, .. }
            | Error::TableSchemaMismatch { source, .. } => source.status_code(),

            Error::Metasrv { source, .. } => source.status_code(),
            Error::SystemCatalogTableScan { source, .. } => source.status_code(),
            Error::SystemCatalogTableScanExec { source, .. } => source.status_code(),
            Error::InvalidTableInfoInCatalog { source, .. } => source.status_code(),

            Error::CompileScriptInternal { source, .. } | Error::Internal { source, .. } => {
                source.status_code()
            }

            Error::Unimplemented { .. } | Error::NotSupported { .. } => StatusCode::Unsupported,
            Error::QueryAccessDenied { .. } => StatusCode::AccessDenied,
            Error::Datafusion { .. } => StatusCode::EngineExecuteQuery,
            Error::TableMetadataManager { source, .. } => source.status_code(),
            Error::GetViewCache { source, .. } | Error::GetTableCache { source, .. } => {
                source.status_code()
            }
        }
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
                location: Location::generate(),
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
                msg: String::default(),
                location: Location::generate(),
            }
            .status_code()
        );

        assert_eq!(
            StatusCode::Internal,
            Error::SystemCatalogTypeMismatch {
                data_type: ConcreteDataType::binary_datatype(),
                location: Location::generate(),
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
            location: Location::generate(),
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
