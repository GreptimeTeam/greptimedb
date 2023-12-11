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
use datafusion::error::DataFusionError;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr { name: String, location: Location },

    #[snafu(display("Operation {} not implemented yet", operation))]
    Unimplemented {
        operation: String,
        location: Location,
    },

    #[snafu(display("General catalog error"))]
    Catalog {
        source: catalog::error::Error,
        location: Location,
    },

    #[snafu(display("Catalog not found: {}", catalog))]
    CatalogNotFound { catalog: String, location: Location },

    #[snafu(display("Schema not found: {}", schema))]
    SchemaNotFound { schema: String, location: Location },

    #[snafu(display("Table not found: {}", table))]
    TableNotFound { table: String, location: Location },

    #[snafu(display("Failed to do vector computation"))]
    VectorComputation {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to create RecordBatch"))]
    CreateRecordBatch {
        source: common_recordbatch::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to create Schema"))]
    CreateSchema {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Failure during query execution"))]
    QueryExecution {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Failure during query planning"))]
    QueryPlan {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Failure during query parsing, query: {}", query))]
    QueryParse {
        query: String,
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Illegal access to catalog: {} and schema: {}", catalog, schema))]
    QueryAccessDenied {
        catalog: String,
        schema: String,
        location: Location,
    },

    #[snafu(display("The SQL string has multiple statements, query: {}", query))]
    MultipleStatements { query: String, location: Location },

    #[snafu(display("Failed to convert Datafusion schema"))]
    ConvertDatafusionSchema {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse timestamp `{}`", raw))]
    ParseTimestamp {
        raw: String,
        #[snafu(source)]
        error: chrono::ParseError,
        location: Location,
    },

    #[snafu(display("Failed to parse float number `{}`", raw))]
    ParseFloat {
        raw: String,
        #[snafu(source)]
        error: std::num::ParseFloatError,
        location: Location,
    },

    #[snafu(display("DataFusion error"))]
    DataFusion {
        #[snafu(source)]
        error: DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to encode Substrait logical plan"))]
    EncodeSubstraitLogicalPlan {
        source: substrait::error::Error,
        location: Location,
    },

    #[snafu(display("General SQL error"))]
    Sql {
        location: Location,
        source: sql::error::Error,
    },

    #[snafu(display("Failed to plan SQL"))]
    PlanSql {
        #[snafu(source)]
        error: DataFusionError,
        location: Location,
    },

    #[snafu(display("Timestamp column for table '{table_name}' is missing!"))]
    MissingTimestampColumn {
        table_name: String,
        location: Location,
    },

    #[snafu(display("Failed to convert value to sql value: {}", value))]
    ConvertSqlValue {
        value: Value,
        source: sql::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to convert concrete type to sql type: {:?}", datatype))]
    ConvertSqlType {
        datatype: ConcreteDataType,
        source: sql::error::Error,
        location: Location,
    },

    #[snafu(display("Missing required field: {}", name))]
    MissingRequiredField { name: String, location: Location },

    #[snafu(display("Failed to regex"))]
    BuildRegex {
        location: Location,
        #[snafu(source)]
        error: regex::Error,
    },

    #[snafu(display("Failed to build data source backend"))]
    BuildBackend {
        source: common_datasource::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to list objects"))]
    ListObjects {
        source: common_datasource::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse file format"))]
    ParseFileFormat {
        source: common_datasource::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to infer schema"))]
    InferSchema {
        source: common_datasource::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to convert datafusion schema"))]
    ConvertSchema {
        source: datatypes::error::Error,
        location: Location,
    },
    #[snafu(display("Unknown table type, downcast failed"))]
    UnknownTable { location: Location },

    #[snafu(display("Cannot find time index column in table {}", table))]
    TimeIndexNotFound { table: String, location: Location },

    #[snafu(display("Failed to add duration '{:?}' to SystemTime, overflowed", duration))]
    AddSystemTimeOverflow {
        duration: Duration,
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
        location: Location,
    },

    #[snafu(display("Column schema has no default value, column: {}", column))]
    ColumnSchemaNoDefault { column: String, location: Location },

    #[snafu(display("Region query error"))]
    RegionQuery {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Table mutation error"))]
    TableMutation {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Missing table mutation handler"))]
    MissingTableMutationHandler { location: Location },

    #[snafu(display("Range Query: {}", msg))]
    RangeQuery { msg: String, location: Location },
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
            | CatalogNotFound { .. }
            | SchemaNotFound { .. }
            | TableNotFound { .. }
            | UnknownTable { .. }
            | TimeIndexNotFound { .. }
            | ParseTimestamp { .. }
            | ParseFloat { .. }
            | MissingRequiredField { .. }
            | BuildRegex { .. }
            | ConvertSchema { .. }
            | AddSystemTimeOverflow { .. }
            | ColumnSchemaIncompatible { .. }
            | ColumnSchemaNoDefault { .. } => StatusCode::InvalidArguments,

            BuildBackend { .. } | ListObjects { .. } => StatusCode::StorageUnavailable,
            EncodeSubstraitLogicalPlan { source, .. } => source.status_code(),

            ParseFileFormat { source, .. } | InferSchema { source, .. } => source.status_code(),

            QueryAccessDenied { .. } => StatusCode::AccessDenied,
            Catalog { source, .. } => source.status_code(),
            VectorComputation { source, .. } | ConvertDatafusionSchema { source, .. } => {
                source.status_code()
            }
            CreateRecordBatch { source, .. } => source.status_code(),
            QueryExecution { source, .. } | QueryPlan { source, .. } => source.status_code(),
            DataFusion { error, .. } => match error {
                DataFusionError::Internal(_) => StatusCode::Internal,
                DataFusionError::NotImplemented(_) => StatusCode::Unsupported,
                DataFusionError::Plan(_) => StatusCode::PlanQuery,
                _ => StatusCode::EngineExecuteQuery,
            },
            MissingTimestampColumn { .. } => StatusCode::EngineExecuteQuery,
            Sql { source, .. } => source.status_code(),
            PlanSql { .. } => StatusCode::PlanQuery,
            ConvertSqlType { source, .. } | ConvertSqlValue { source, .. } => source.status_code(),
            CreateSchema { source, .. } => source.status_code(),

            RegionQuery { source, .. } => source.status_code(),
            TableMutation { source, .. } => source.status_code(),
            MissingTableMutationHandler { .. } => StatusCode::Unexpected,
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
