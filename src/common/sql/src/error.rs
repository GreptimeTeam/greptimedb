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
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use datafusion_sql::sqlparser::ast::UnaryOperator;
use datatypes::prelude::{ConcreteDataType, Value};
use snafu::{Location, Snafu};
pub use sqlparser::ast::{Expr, Value as SqlValue};

pub type Result<T> = std::result::Result<T, Error>;

/// SQL parser errors.
// Now the error in parser does not contain backtrace to avoid generating backtrace
// every time the parser parses an invalid SQL.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
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
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse value: {}", msg))]
    ParseSqlValue {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Unsupported default constraint for column: '{column_name}', reason: {reason}"
    ))]
    UnsupportedDefaultValue {
        column_name: String,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unable to convert sql value {} to datatype {:?}", value, datatype))]
    ConvertSqlValue {
        value: SqlValue,
        datatype: ConcreteDataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid sql value: {}", value))]
    InvalidSqlValue {
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported unary operator {}", unary_op))]
    UnsupportedUnaryOp {
        unary_op: UnaryOperator,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid unary operator {} for value {}", unary_op, value))]
    InvalidUnaryOp {
        unary_op: UnaryOperator,
        value: Value,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to cast SQL value {} to datatype {}", sql_value, datatype))]
    InvalidCast {
        sql_value: sqlparser::ast::Value,
        datatype: ConcreteDataType,
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Unable to convert {} to datatype {:?}", value, datatype))]
    ConvertStr {
        value: String,
        datatype: ConcreteDataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Converting timestamp {:?} to unit {:?} overflow",
        timestamp,
        target_unit
    ))]
    TimestampOverflow {
        timestamp: Timestamp,
        target_unit: TimeUnit,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Datatype error: {}", source))]
    Datatype {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize data, json: {}", json))]
    Deserialize {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
        json: String,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            UnsupportedDefaultValue { .. } => StatusCode::Unsupported,
            ParseSqlValue { .. } => StatusCode::InvalidSyntax,
            ColumnTypeMismatch { .. }
            | InvalidSqlValue { .. }
            | UnsupportedUnaryOp { .. }
            | InvalidUnaryOp { .. }
            | InvalidCast { .. }
            | ConvertStr { .. }
            | TimestampOverflow { .. } => StatusCode::InvalidArguments,
            Deserialize { .. } => StatusCode::Unexpected,

            Datatype { source, .. } => source.status_code(),
            ConvertSqlValue { .. } => StatusCode::Unsupported,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
