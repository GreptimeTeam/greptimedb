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
use std::time::Duration;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_query::error::datafusion_status_code;
use datafusion::error::DataFusionError;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use snafu::{Location, Snafu};
use store_api::storage::RegionId;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported show variable: {}", name))]
    UnsupportedVariable {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Operation {} not implemented yet", operation))]
    Unimplemented {
        operation: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("General catalog error"))]
    Catalog {
        source: catalog::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table not found: {}", table))]
    TableNotFound {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create RecordBatch"))]
    CreateRecordBatch {
        source: common_recordbatch::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failure during query execution"))]
    QueryExecution {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failure during query planning"))]
    QueryPlan {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failure during query parsing, query: {}", query))]
    QueryParse {
        query: String,
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Illegal access to catalog: {} and schema: {}", catalog, schema))]
    QueryAccessDenied {
        catalog: String,
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("The SQL string has multiple statements, query: {}", query))]
    MultipleStatements {
        query: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse timestamp `{}`", raw))]
    ParseTimestamp {
        raw: String,
        #[snafu(source)]
        error: chrono::ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse float number `{}`", raw))]
    ParseFloat {
        raw: String,
        #[snafu(source)]
        error: std::num::ParseFloatError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion error"))]
    DataFusion {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("General SQL error"))]
    Sql {
        #[snafu(implicit)]
        location: Location,
        source: sql::error::Error,
    },

    #[snafu(display("Failed to plan SQL"))]
    PlanSql {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Timestamp column for table '{table_name}' is missing!"))]
    MissingTimestampColumn {
        table_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert value to sql value: {}", value))]
    ConvertSqlValue {
        value: Value,
        source: sql::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert concrete type to sql type: {:?}", datatype))]
    ConvertSqlType {
        datatype: ConcreteDataType,
        source: sql::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing required field: {}", name))]
    MissingRequiredField {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to regex"))]
    BuildRegex {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: regex::Error,
    },

    #[snafu(display("Failed to build data source backend"))]
    BuildBackend {
        source: common_datasource::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to list objects"))]
    ListObjects {
        source: common_datasource::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse file format"))]
    ParseFileFormat {
        source: common_datasource::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to infer schema"))]
    InferSchema {
        source: common_datasource::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert datafusion schema"))]
    ConvertSchema {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unknown table type, downcast failed"))]
    UnknownTable {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot find time index column in table {}", table))]
    TimeIndexNotFound {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to add duration '{:?}' to SystemTime, overflowed", duration))]
    AddSystemTimeOverflow {
        duration: Duration,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Column schema incompatible, column: {}, file_type: {}, table_type: {}",
        column,
        file_type,
        table_type
    ))]
    ColumnSchemaIncompatible {
        column: String,
        file_type: ConcreteDataType,
        table_type: ConcreteDataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Column schema has no default value, column: {}", column))]
    ColumnSchemaNoDefault {
        column: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region query error"))]
    RegionQuery {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table mutation error"))]
    TableMutation {
        source: common_query::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing table mutation handler"))]
    MissingTableMutationHandler {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Range Query: {}", msg))]
    RangeQuery {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to get metadata from engine {} for region_id {}",
        engine,
        region_id,
    ))]
    GetRegionMetadata {
        engine: String,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Cannot change read-only table: {}", table))]
    TableReadOnly {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get fulltext options"))]
    GetFulltextOptions {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            QueryParse { .. } | MultipleStatements { .. } | RangeQuery { .. } => {
                StatusCode::InvalidSyntax
            }
            UnsupportedExpr { .. }
            | Unimplemented { .. }
            | UnknownTable { .. }
            | TimeIndexNotFound { .. }
            | ParseTimestamp { .. }
            | ParseFloat { .. }
            | MissingRequiredField { .. }
            | BuildRegex { .. }
            | ConvertSchema { .. }
            | AddSystemTimeOverflow { .. }
            | ColumnSchemaIncompatible { .. }
            | UnsupportedVariable { .. }
            | ColumnSchemaNoDefault { .. } => StatusCode::InvalidArguments,

            BuildBackend { .. } | ListObjects { .. } => StatusCode::StorageUnavailable,

            TableNotFound { .. } => StatusCode::TableNotFound,

            ParseFileFormat { source, .. } | InferSchema { source, .. } => source.status_code(),

            QueryAccessDenied { .. } => StatusCode::AccessDenied,
            Catalog { source, .. } => source.status_code(),
            CreateRecordBatch { source, .. } => source.status_code(),
            QueryExecution { source, .. } | QueryPlan { source, .. } => source.status_code(),
            PlanSql { error, .. } => {
                datafusion_status_code::<Self>(error, Some(StatusCode::PlanQuery))
            }

            DataFusion { error, .. } => datafusion_status_code::<Self>(error, None),

            MissingTimestampColumn { .. } => StatusCode::EngineExecuteQuery,
            Sql { source, .. } => source.status_code(),

            ConvertSqlType { source, .. } | ConvertSqlValue { source, .. } => source.status_code(),

            RegionQuery { source, .. } => source.status_code(),
            TableMutation { source, .. } => source.status_code(),
            MissingTableMutationHandler { .. } => StatusCode::Unexpected,
            GetRegionMetadata { .. } => StatusCode::RegionNotReady,
            TableReadOnly { .. } => StatusCode::Unsupported,
            GetFulltextOptions { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(e: Error) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}
