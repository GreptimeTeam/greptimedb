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
use common_query::error::datafusion_status_code;
use datafusion::error::DataFusionError;
use datatypes::arrow::error::ArrowError;
use snafu::{Location, Snafu};

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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert Arrow schema"))]
    SchemaConversion {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table projection error"))]
    TableProjection {
        #[snafu(source)]
        error: ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create record batch for Tables"))]
    TablesRecordBatch {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Column {column_name} already exists in table {table_name}"))]
    ColumnExists {
        column_name: String,
        table_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build schema, msg: {}", msg))]
    SchemaBuild {
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
        msg: String,
    },

    #[snafu(display("Column {} not exists in table {}", column_name, table_name))]
    ColumnNotExists {
        column_name: String,
        table_name: String,
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Not allowed to remove partition column {} from table {}",
        column_name,
        table_name
    ))]
    RemovePartitionColumn {
        column_name: String,
        table_name: String,
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to operate table"))]
    TableOperation { source: BoxedError },

    #[snafu(display("Unsupported operation: {}", operation))]
    Unsupported { operation: String },

    #[snafu(display("Failed to parse table option, key: {}, value: {}", key, value))]
    ParseTableOption {
        key: String,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid alter table({}) request: {}", table, err))]
    InvalidAlterRequest {
        table: String,
        #[snafu(implicit)]
        location: Location,
        err: String,
    },

    #[snafu(display("Missing time index column in table: {}", table_name))]
    MissingTimeIndexColumn {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid column option, column name: {}, error: {}", column_name, msg))]
    InvalidColumnOption {
        column_name: String,
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to set fulltext options for column {}", column_name))]
    SetFulltextOptions {
        column_name: String,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to set skipping index options for column {}", column_name))]
    SetSkippingOptions {
        column_name: String,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to unset skipping index options for column {}", column_name))]
    UnsetSkippingOptions {
        column_name: String,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid table name: '{s}'"))]
    InvalidTableName { s: String },

    #[snafu(display("Failed to cast default value, reason: {}", reason))]
    CastDefaultValue {
        reason: String,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Sql common error"))]
    SqlCommon {
        source: common_sql::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::Datafusion { error, .. } => datafusion_status_code::<Self>(error, None),
            Error::SchemaConversion { .. } | Error::TableProjection { .. } => {
                StatusCode::EngineExecuteQuery
            }
            Error::RemoveColumnInIndex { .. }
            | Error::RemovePartitionColumn { .. }
            | Error::BuildColumnDescriptor { .. }
            | Error::InvalidAlterRequest { .. } => StatusCode::InvalidArguments,
            Error::CastDefaultValue { source, .. } => source.status_code(),
            Error::TablesRecordBatch { .. } => StatusCode::Unexpected,
            Error::ColumnExists { .. } => StatusCode::TableColumnExists,
            Error::SqlCommon { source, .. } => source.status_code(),
            Error::SchemaBuild { source, .. } | Error::SetFulltextOptions { source, .. } => {
                source.status_code()
            }
            Error::TableOperation { source } => source.status_code(),
            Error::InvalidColumnOption { .. } => StatusCode::InvalidArguments,
            Error::ColumnNotExists { .. } => StatusCode::TableColumnNotFound,
            Error::Unsupported { .. } => StatusCode::Unsupported,
            Error::ParseTableOption { .. } => StatusCode::InvalidArguments,
            Error::MissingTimeIndexColumn { .. } => StatusCode::IllegalState,
            Error::SetSkippingOptions { .. }
            | Error::UnsetSkippingOptions { .. }
            | Error::InvalidTableName { .. } => StatusCode::InvalidArguments,
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
