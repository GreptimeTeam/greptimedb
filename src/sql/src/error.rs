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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datatypes::prelude::{ConcreteDataType, Value};
use snafu::{Location, Snafu};
use sqlparser::parser::ParserError;

use crate::ast::{Expr, Value as SqlValue};

pub type Result<T> = std::result::Result<T, Error>;

/// SQL parser errors.
// Now the error in parser does not contains backtrace to avoid generating backtrace
// every time the parser parses an invalid SQL.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("SQL statement is not supported: {}, keyword: {}", sql, keyword))]
    Unsupported { sql: String, keyword: String },

    #[snafu(display(
        "Unexpected token while parsing SQL statement: {}, expected: '{}', found: {}",
        sql,
        expected,
        actual,
    ))]
    Unexpected {
        sql: String,
        expected: String,
        actual: String,
        #[snafu(source)]
        error: ParserError,
    },

    #[snafu(display(
        "Unsupported expr in default constraint: {:?} for column: {}",
        expr,
        column_name
    ))]
    UnsupportedDefaultValue { column_name: String, expr: Expr },

    // Syntax error from sql parser.
    #[snafu(display(""))]
    Syntax {
        #[snafu(source)]
        error: ParserError,
        location: Location,
    },

    #[snafu(display("Missing time index constraint"))]
    MissingTimeIndex {},

    #[snafu(display("Invalid time index: {}", msg))]
    InvalidTimeIndex { msg: String },

    #[snafu(display("Invalid SQL, error: {}", msg))]
    InvalidSql { msg: String },

    #[snafu(display("Invalid column option, column name: {}, error: {}", name, msg))]
    InvalidColumnOption { name: String, msg: String },

    #[snafu(display("SQL data type not supported yet: {:?}", t))]
    SqlTypeNotSupported { t: crate::ast::DataType },

    #[snafu(display("Failed to parse value: {}", msg))]
    ParseSqlValue { msg: String },

    #[snafu(display(
        "Column {} expect type: {:?}, actual: {:?}",
        column_name,
        expect,
        actual,
    ))]
    ColumnTypeMismatch {
        column_name: String,
        expect: ConcreteDataType,
        actual: ConcreteDataType,
    },

    #[snafu(display("Invalid database name: {}", name))]
    InvalidDatabaseName { name: String },

    #[snafu(display("Invalid table name: {}", name))]
    InvalidTableName { name: String },

    #[snafu(display("Invalid default constraint, column: {}", column))]
    InvalidDefault {
        column: String,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to cast SQL value {} to datatype {}", sql_value, datatype))]
    InvalidCast {
        sql_value: sqlparser::ast::Value,
        datatype: ConcreteDataType,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid table option key: {}", key))]
    InvalidTableOption { key: String, location: Location },

    #[snafu(display("Failed to serialize column default constraint"))]
    SerializeColumnDefaultConstraint {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to convert data type to gRPC data type defined in proto"))]
    ConvertToGrpcDataType {
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Invalid sql value: {}", value))]
    InvalidSqlValue { value: String },

    #[snafu(display(
        "Converting timestamp {:?} to unit {:?} overflow",
        timestamp,
        target_unit
    ))]
    TimestampOverflow {
        timestamp: Timestamp,
        target_unit: TimeUnit,
    },

    #[snafu(display("Unable to convert statement {} to DataFusion statement", statement))]
    ConvertToDfStatement {
        statement: String,
        location: Location,
    },

    #[snafu(display("Unable to convert sql value {} to datatype {:?}", value, datatype))]
    ConvertSqlValue {
        value: SqlValue,
        datatype: ConcreteDataType,
        location: Location,
    },

    #[snafu(display("Unable to convert value {} to sql value", value))]
    ConvertValue { value: Value, location: Location },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            UnsupportedDefaultValue { .. } | Unsupported { .. } => StatusCode::Unsupported,
            Unexpected { .. }
            | Syntax { .. }
            | MissingTimeIndex { .. }
            | InvalidTimeIndex { .. }
            | InvalidSql { .. }
            | ParseSqlValue { .. }
            | SqlTypeNotSupported { .. }
            | InvalidDefault { .. } => StatusCode::InvalidSyntax,

            InvalidColumnOption { .. }
            | InvalidDatabaseName { .. }
            | ColumnTypeMismatch { .. }
            | InvalidTableName { .. }
            | InvalidSqlValue { .. }
            | TimestampOverflow { .. }
            | InvalidTableOption { .. }
            | InvalidCast { .. } => StatusCode::InvalidArguments,

            SerializeColumnDefaultConstraint { source, .. } => source.status_code(),
            ConvertToGrpcDataType { source, .. } => source.status_code(),
            ConvertToDfStatement { .. } => StatusCode::Internal,
            ConvertSqlValue { .. } | ConvertValue { .. } => StatusCode::Unsupported,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
