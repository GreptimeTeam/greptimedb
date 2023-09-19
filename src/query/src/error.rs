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
use common_meta::table_name::TableName;
use datafusion::error::DataFusionError;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr { name: String, location: Location },

    #[snafu(display("Operation {} not implemented yet", operation))]
    Unimplemented {
        operation: String,
        location: Location,
    },

    #[snafu(display("General catalog error: {}", source))]
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

    #[snafu(display("Failed to do vector computation, source: {}", source))]
    VectorComputation {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to create RecordBatch, source: {}", source))]
    CreateRecordBatch {
        source: common_recordbatch::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to create Schema, source: {}", source))]
    CreateSchema {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Failure during query execution, source: {}", source))]
    QueryExecution {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Failure during query planning, source: {}", source))]
    QueryPlan {
        source: BoxedError,
        location: Location,
    },

    #[snafu(display("Failure during query parsing, query: {}, source: {}", query, source))]
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

    #[snafu(display("Failed to convert Datafusion schema: {}", source))]
    ConvertDatafusionSchema {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse timestamp `{}`: {}", raw, source))]
    ParseTimestamp {
        raw: String,
        source: chrono::ParseError,
        location: Location,
    },

    #[snafu(display("Failed to parse float number `{}`: {}", raw, source))]
    ParseFloat {
        raw: String,
        source: std::num::ParseFloatError,
        location: Location,
    },

    #[snafu(display("DataFusion error: {}", source))]
    DataFusion {
        source: DataFusionError,
        location: Location,
    },

    #[snafu(display("Failed to encode Substrait logical plan, source: {}", source))]
    EncodeSubstraitLogicalPlan {
        source: substrait::error::Error,
        location: Location,
    },

    #[snafu(display("General SQL error: {}", source))]
    Sql {
        location: Location,
        source: sql::error::Error,
    },

    #[snafu(display("Cannot plan SQL: {}, source: {}", sql, source))]
    PlanSql {
        sql: String,
        source: DataFusionError,
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

    #[snafu(display("Failed to route partition of table {}, source: {}", table, source))]
    RoutePartition {
        table: TableName,
        source: partition::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse SQL, source: {}", source))]
    ParseSql {
        source: sql::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to request remote peer, source: {}", source))]
    RemoteRequest {
        source: client::Error,
        location: Location,
    },

    #[snafu(display("Unexpected query output. Expected: {}, Got: {}", expected, got))]
    UnexpectedOutputKind {
        expected: String,
        got: String,
        location: Location,
    },

    #[snafu(display("Missing required field: {}", name))]
    MissingRequiredField { name: String, location: Location },

    #[snafu(display("Failed to regex, source: {}", source))]
    BuildRegex {
        location: Location,
        source: regex::Error,
    },

    #[snafu(display("Failed to build data source backend, source: {}", source))]
    BuildBackend {
        source: common_datasource::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to list objects, source: {}", source))]
    ListObjects {
        source: common_datasource::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to parse file format: {}", source))]
    ParseFileFormat {
        source: common_datasource::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to infer schema: {}", source))]
    InferSchema {
        source: common_datasource::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to convert datafusion schema, source: {}", source))]
    ConvertSchema {
        source: datatypes::error::Error,
        location: Location,
    },
    #[snafu(display("Unknown table type, downcast failed, location: {}", location))]
    UnknownTable { location: Location },

    #[snafu(display(
        "Cannot find time index column in table {}, location: {}",
        table,
        location
    ))]
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

    #[snafu(display("Region query error, source: {}", source))]
    RegionQuery {
        source: BoxedError,
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            QueryParse { .. } | MultipleStatements { .. } => StatusCode::InvalidSyntax,
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
            ParseSql { source, .. } => source.status_code(),
            CreateRecordBatch { source, .. } => source.status_code(),
            QueryExecution { source, .. } | QueryPlan { source, .. } => source.status_code(),
            DataFusion { .. } | MissingTimestampColumn { .. } | RoutePartition { .. } => {
                StatusCode::Internal
            }
            Sql { source, .. } => source.status_code(),
            PlanSql { .. } => StatusCode::PlanQuery,
            ConvertSqlType { source, .. } | ConvertSqlValue { source, .. } => source.status_code(),
            RemoteRequest { source, .. } => source.status_code(),
            UnexpectedOutputKind { .. } => StatusCode::Unexpected,
            CreateSchema { source, .. } => source.status_code(),
            RegionQuery { source, .. } => source.status_code(),
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
