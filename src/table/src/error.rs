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

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datafusion::error::DataFusionError;
use datatypes::arrow::error::ArrowError;
use snafu::{Location, Snafu};

use crate::metadata::TableId;

pub type Result<T> = std::result::Result<T, Error>;

/// Default error implementation of table.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("DataFusion error"))]
    Datafusion {
        #[snafu(source)]
        error: DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to convert Arrow schema"))]
    SchemaConversion {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Engine not found: {}", engine))]
    EngineNotFound { engine: String, location: Location },

    #[snafu(display("Engine exist: {}", engine))]
    EngineExist { engine: String, location: Location },

    #[snafu(display("Table projection error"))]
    TableProjection {
        #[snafu(source)]
        error: ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to create record batch for Tables"))]
    TablesRecordBatch {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Column {column_name} already exists in table {table_name}"))]
    ColumnExists {
        column_name: String,
        table_name: String,
        location: Location,
    },

    #[snafu(display("Failed to build schema, msg: {}", msg))]
    SchemaBuild {
        location: Location,
        source: datatypes::error::Error,
        msg: String,
    },

    #[snafu(display("Column {} not exists in table {}", column_name, table_name))]
    ColumnNotExists {
        column_name: String,
        table_name: String,
        location: Location,
    },

    #[snafu(display("Duplicated call to plan execute method. table: {}", table))]
    DuplicatedExecuteCall { location: Location, table: String },

    #[snafu(display(
        "Not allowed to remove index column {} from table {}",
        column_name,
        table_name
    ))]
    RemoveColumnInIndex {
        column_name: String,
        table_name: String,
        location: Location,
    },

    #[snafu(display(
        "Failed to build column descriptor for table: {}, column: {}",
        table_name,
        column_name,
    ))]
    BuildColumnDescriptor {
        #[snafu(source)]
        error: store_api::storage::ColumnDescriptorBuilderError,
        table_name: String,
        column_name: String,
        location: Location,
    },

    #[snafu(display("Regions schemas mismatch in table: {}", table))]
    RegionSchemaMismatch { table: String, location: Location },

    #[snafu(display("Failed to operate table"))]
    TableOperation { source: BoxedError },

    #[snafu(display("Unsupported operation: {}", operation))]
    Unsupported { operation: String },

    #[snafu(display("Failed to parse table option, key: {}, value: {}", key, value))]
    ParseTableOption {
        key: String,
        value: String,
        location: Location,
    },

    #[snafu(display("Invalid alter table({}) request: {}", table, err))]
    InvalidAlterRequest {
        table: String,
        location: Location,
        err: String,
    },

    #[snafu(display("Invalid table state: {}", table_id))]
    InvalidTable {
        table_id: TableId,
        location: Location,
    },

    #[snafu(display("Missing time index column in table: {}", table_name))]
    MissingTimeIndexColumn {
        table_name: String,
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::Datafusion { .. }
            | Error::SchemaConversion { .. }
            | Error::TableProjection { .. } => StatusCode::EngineExecuteQuery,
            Error::RemoveColumnInIndex { .. }
            | Error::BuildColumnDescriptor { .. }
            | Error::InvalidAlterRequest { .. } => StatusCode::InvalidArguments,
            Error::TablesRecordBatch { .. } | Error::DuplicatedExecuteCall { .. } => {
                StatusCode::Unexpected
            }
            Error::ColumnExists { .. } => StatusCode::TableColumnExists,
            Error::SchemaBuild { source, .. } => source.status_code(),
            Error::TableOperation { source } => source.status_code(),
            Error::ColumnNotExists { .. } => StatusCode::TableColumnNotFound,
            Error::RegionSchemaMismatch { .. } => StatusCode::StorageUnavailable,
            Error::Unsupported { .. } => StatusCode::Unsupported,
            Error::ParseTableOption { .. }
            | Error::EngineNotFound { .. }
            | Error::EngineExist { .. } => StatusCode::InvalidArguments,

            Error::InvalidTable { .. } | Error::MissingTimeIndexColumn { .. } => {
                StatusCode::Internal
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}
