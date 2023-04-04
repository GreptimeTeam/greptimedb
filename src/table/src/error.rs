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
use common_recordbatch::error::Error as RecordBatchError;
use datafusion::error::DataFusionError;
use datatypes::arrow::error::ArrowError;
use snafu::Location;

pub type Result<T> = std::result::Result<T, Error>;

/// Default error implementation of table.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Datafusion error: {}", source))]
    Datafusion {
        source: DataFusionError,
        location: Location,
    },

    #[snafu(display("Poll stream failed, source: {}", source))]
    PollStream {
        source: ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to convert Arrow schema, source: {}", source))]
    SchemaConversion {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Engine not found: {}", engine))]
    EngineNotFound { engine: String, location: Location },

    #[snafu(display("Engine exist: {}", engine))]
    EngineExist { engine: String, location: Location },

    #[snafu(display("Table projection error, source: {}", source))]
    TableProjection {
        source: ArrowError,
        location: Location,
    },

    #[snafu(display("Failed to create record batch for Tables, source: {}", source))]
    TablesRecordBatch {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Column {} already exists in table {}", column_name, table_name))]
    ColumnExists {
        column_name: String,
        table_name: String,
        location: Location,
    },

    #[snafu(display("Failed to build schema, msg: {}, source: {}", msg, source))]
    SchemaBuild {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
        msg: String,
    },

    #[snafu(display("Column {} not exists in table {}", column_name, table_name))]
    ColumnNotExists {
        column_name: String,
        table_name: String,
        location: Location,
    },

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
        "Failed to build column descriptor for table: {}, column: {}, source: {}",
        table_name,
        column_name,
        source,
    ))]
    BuildColumnDescriptor {
        source: store_api::storage::ColumnDescriptorBuilderError,
        table_name: String,
        column_name: String,
        location: Location,
    },
    #[snafu(display("Regions schemas mismatch in table: {}", table))]
    RegionSchemaMismatch { table: String, location: Location },

    #[snafu(display("Failed to operate table, source: {}", source))]
    TableOperation { source: BoxedError },

    #[snafu(display("Unsupported operation: {}", operation))]
    Unsupported { operation: String },

    #[snafu(display("Failed to parse table option, key: {}, value: {}", key, value))]
    ParseTableOption {
        key: String,
        value: String,
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::Datafusion { .. }
            | Error::PollStream { .. }
            | Error::SchemaConversion { .. }
            | Error::TableProjection { .. } => StatusCode::EngineExecuteQuery,
            Error::RemoveColumnInIndex { .. } | Error::BuildColumnDescriptor { .. } => {
                StatusCode::InvalidArguments
            }
            Error::TablesRecordBatch { .. } => StatusCode::Unexpected,
            Error::ColumnExists { .. } => StatusCode::TableColumnExists,
            Error::SchemaBuild { source, .. } => source.status_code(),
            Error::TableOperation { source } => source.status_code(),
            Error::ColumnNotExists { .. } => StatusCode::TableColumnNotFound,
            Error::RegionSchemaMismatch { .. } => StatusCode::StorageUnavailable,
            Error::Unsupported { .. } => StatusCode::Unsupported,
            Error::ParseTableOption { .. }
            | Error::EngineNotFound { .. }
            | Error::EngineExist { .. } => StatusCode::InvalidArguments,
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

impl From<Error> for RecordBatchError {
    fn from(e: Error) -> RecordBatchError {
        RecordBatchError::External {
            source: BoxedError::new(e),
        }
    }
}
