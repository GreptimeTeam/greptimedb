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
use datafusion_common::DataFusionError;
use datafusion_sql::sqlparser::ast::UnaryOperator;
use datatypes::prelude::{ConcreteDataType, Value};
use snafu::{Location, Snafu};
use sqlparser::ast::Ident;
use sqlparser::parser::ParserError;

use crate::ast::{Expr, Value as SqlValue};
use crate::parsers::error::TQLError;

pub type Result<T> = std::result::Result<T, Error>;

/// SQL parser errors.
// Now the error in parser does not contain backtrace to avoid generating backtrace
// every time the parser parses an invalid SQL.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("SQL statement is not supported, keyword: {}", keyword))]
    Unsupported {
        keyword: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Unexpected token while parsing SQL statement, expected: '{}', found: {}",
        expected,
        actual,
    ))]
    Unexpected {
        expected: String,
        actual: String,
        #[snafu(source)]
        error: ParserError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Unsupported expr in default constraint: {:?} for column: {}",
        expr,
        column_name
    ))]
    UnsupportedDefaultValue {
        column_name: String,
        expr: Expr,
        #[snafu(implicit)]
        location: Location,
    },

    // Syntax error from sql parser.
    #[snafu(display("Invalid SQL syntax"))]
    Syntax {
        #[snafu(source)]
        error: ParserError,
        #[snafu(implicit)]
        location: Location,
    },

    // Syntax error from tql parser.
    #[snafu(display("Invalid TQL syntax"))]
    TQLSyntax {
        #[snafu(source)]
        error: TQLError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing time index constraint"))]
    MissingTimeIndex {},

    #[snafu(display("Invalid time index: {}", msg))]
    InvalidTimeIndex {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid SQL, error: {}", msg))]
    InvalidSql {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Unexpected token while parsing SQL statement, expected: '{}', found: {}",
        expected,
        actual,
    ))]
    UnexpectedToken {
        expected: String,
        actual: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid column option, column name: {}, error: {}", name, msg))]
    InvalidColumnOption {
        name: String,
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("SQL data type not supported yet: {:?}", t))]
    SqlTypeNotSupported {
        t: crate::ast::DataType,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("ConcreteDataType not supported yet: {:?}", t))]
    ConcreteTypeNotSupported {
        t: ConcreteDataType,
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

    #[snafu(display("Invalid database name: {}", name))]
    InvalidDatabaseName {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid interval provided: {}", reason))]
    InvalidInterval {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unrecognized database option key: {}", key))]
    InvalidDatabaseOption {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid table name: {}", name))]
    InvalidTableName {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid flow name: {}", name))]
    InvalidFlowName {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid default constraint, column: {}", column))]
    InvalidDefault {
        column: String,
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to cast SQL value {} to datatype {}", sql_value, datatype))]
    InvalidCast {
        sql_value: sqlparser::ast::Value,
        datatype: ConcreteDataType,
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid unary operator {} for value {}", unary_op, value))]
    InvalidUnaryOp {
        unary_op: UnaryOperator,
        value: Value,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported unary operator {}", unary_op))]
    UnsupportedUnaryOp {
        unary_op: UnaryOperator,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unrecognized table option key: {}", key))]
    InvalidTableOption {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unrecognized table option key: {}, value: {}", key, value))]
    InvalidTableOptionValue {
        key: Ident,
        value: Expr,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize column default constraint"))]
    SerializeColumnDefaultConstraint {
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to convert data type to gRPC data type defined in proto"))]
    ConvertToGrpcDataType {
        #[snafu(implicit)]
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Invalid sql value: {}", value))]
    InvalidSqlValue {
        value: String,
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

    #[snafu(display("Unable to convert statement {} to DataFusion statement", statement))]
    ConvertToDfStatement {
        statement: String,
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

    #[snafu(display("Unable to convert value {} to sql value", value))]
    ConvertValue {
        value: Value,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert to logical TQL expression"))]
    ConvertToLogicalExpression {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to simplify TQL expression"))]
    Simplification {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Permission denied while operating catalog {} from current catalog {}",
        target,
        current
    ))]
    PermissionDenied {
        target: String,
        current: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to set fulltext option"))]
    SetFulltextOption {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to set SKIPPING index option"))]
    SetSkippingIndexOption {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Datatype error: {}", source))]
    Datatype {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Invalid partition number: {}, should be in range [2, 65536]",
        partition_num
    ))]
    InvalidPartitionNumber {
        partition_num: u32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unable to convert {} to datatype {:?}", value, datatype))]
    ConvertStr {
        value: String,
        datatype: ConcreteDataType,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            UnsupportedDefaultValue { .. } | Unsupported { .. } => StatusCode::Unsupported,
            Unexpected { .. }
            | Syntax { .. }
            | TQLSyntax { .. }
            | MissingTimeIndex { .. }
            | InvalidTimeIndex { .. }
            | InvalidSql { .. }
            | ParseSqlValue { .. }
            | SqlTypeNotSupported { .. }
            | ConcreteTypeNotSupported { .. }
            | UnexpectedToken { .. }
            | InvalidDefault { .. } => StatusCode::InvalidSyntax,

            InvalidColumnOption { .. }
            | InvalidTableOptionValue { .. }
            | InvalidDatabaseName { .. }
            | InvalidDatabaseOption { .. }
            | ColumnTypeMismatch { .. }
            | InvalidTableName { .. }
            | InvalidFlowName { .. }
            | InvalidSqlValue { .. }
            | TimestampOverflow { .. }
            | InvalidTableOption { .. }
            | InvalidCast { .. }
            | ConvertToLogicalExpression { .. }
            | Simplification { .. }
            | InvalidInterval { .. }
            | InvalidUnaryOp { .. }
            | InvalidPartitionNumber { .. }
            | UnsupportedUnaryOp { .. }
            | ConvertStr { .. } => StatusCode::InvalidArguments,

            SerializeColumnDefaultConstraint { source, .. } => source.status_code(),
            ConvertToGrpcDataType { source, .. } => source.status_code(),
            Datatype { source, .. } => source.status_code(),
            ConvertToDfStatement { .. } => StatusCode::Internal,
            ConvertSqlValue { .. } | ConvertValue { .. } => StatusCode::Unsupported,

            PermissionDenied { .. } => StatusCode::PermissionDenied,
            SetFulltextOption { .. } | SetSkippingIndexOption { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
