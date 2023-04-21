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
use datafusion::error::DataFusionError;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unsupported expr type: {}", name))]
    UnsupportedExpr { name: String, location: Location },

    #[snafu(display("General catalog error: {}", source))]
    Catalog {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Catalog not found: {}", catalog))]
    CatalogNotFound { catalog: String, location: Location },

    #[snafu(display("Schema not found: {}", schema))]
    SchemaNotFound { schema: String, location: Location },

    #[snafu(display("Table not found: {}", table))]
    TableNotFound { table: String, location: Location },

    #[snafu(display("Failed to do vector computation, source: {}", source))]
    VectorComputation {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to create RecordBatch, source: {}", source))]
    CreateRecordBatch {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failure during query execution, source: {}", source))]
    QueryExecution { source: BoxedError },

    #[snafu(display("Failure during query planning, source: {}", source))]
    QueryPlan { source: BoxedError },

    #[snafu(display("Failure during query parsing, query: {}, source: {}", query, source))]
    QueryParse { query: String, source: BoxedError },

    #[snafu(display("Illegal access to catalog: {} and schema: {}", catalog, schema))]
    QueryAccessDenied { catalog: String, schema: String },

    #[snafu(display("The SQL string has multiple statements, query: {}", query))]
    MultipleStatements { query: String, location: Location },

    #[snafu(display("Failed to convert Datafusion schema: {}", source))]
    ConvertDatafusionSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
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

    #[snafu(display("General SQL error: {}", source))]
    Sql {
        #[snafu(backtrace)]
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
        #[snafu(backtrace)]
        source: sql::error::Error,
    },

    #[snafu(display("Failed to convert concrete type to sql type: {:?}", datatype))]
    ConvertSqlType {
        datatype: ConcreteDataType,
        #[snafu(backtrace)]
        source: sql::error::Error,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            QueryParse { .. } | MultipleStatements { .. } => StatusCode::InvalidSyntax,
            UnsupportedExpr { .. }
            | CatalogNotFound { .. }
            | SchemaNotFound { .. }
            | TableNotFound { .. }
            | ParseTimestamp { .. }
            | ParseFloat { .. } => StatusCode::InvalidArguments,
            QueryAccessDenied { .. } => StatusCode::AccessDenied,
            Catalog { source } => source.status_code(),
            VectorComputation { source } | ConvertDatafusionSchema { source } => {
                source.status_code()
            }
            CreateRecordBatch { source } => source.status_code(),
            QueryExecution { source } | QueryPlan { source } => source.status_code(),
            DataFusion { .. } | MissingTimestampColumn { .. } => StatusCode::Internal,
            Sql { source } => source.status_code(),
            PlanSql { .. } => StatusCode::PlanQuery,
            ConvertSqlType { source, .. } | ConvertSqlValue { source, .. } => source.status_code(),
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
