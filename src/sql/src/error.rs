// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

use common_error::prelude::*;
use datatypes::prelude::ConcreteDataType;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::TokenizerError;

use crate::ast::Expr;

pub type Result<T> = std::result::Result<T, Error>;

/// SQL parser errors.
// Now the error in parser does not contains backtrace to avoid generating backtrace
// every time the parser parses an invalid SQL.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("SQL statement is not supported: {}, keyword: {}", sql, keyword))]
    Unsupported { sql: String, keyword: String },

    #[snafu(display(
        "Unexpected token while parsing SQL statement: {}, expected: '{}', found: {}, source: {}",
        sql,
        expected,
        actual,
        source
    ))]
    Unexpected {
        sql: String,
        expected: String,
        actual: String,
        source: ParserError,
    },

    #[snafu(display(
        "Unsupported expr in default constraint: {} for column: {}",
        expr,
        column_name
    ))]
    UnsupportedDefaultValue {
        column_name: String,
        expr: Expr,
        backtrace: Backtrace,
    },

    // Syntax error from sql parser.
    #[snafu(display("Syntax error, sql: {}, source: {}", sql, source))]
    Syntax { sql: String, source: ParserError },

    #[snafu(display("Tokenizer error, sql: {}, source: {}", sql, source))]
    Tokenizer { sql: String, source: TokenizerError },

    #[snafu(display(
        "Invalid time index, it should contains only one column, sql: {}.",
        sql
    ))]
    InvalidTimeIndex { sql: String, backtrace: Backtrace },

    #[snafu(display("Invalid SQL, error: {}", msg))]
    InvalidSql { msg: String, backtrace: Backtrace },

    #[snafu(display("SQL data type not supported yet: {:?}", t))]
    SqlTypeNotSupported {
        t: crate::ast::DataType,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to parse value: {}, {}", msg, backtrace))]
    ParseSqlValue { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Column {} expect type: {:?}, actual: {:?}",
        column_name,
        expect,
        actual
    ))]
    ColumnTypeMismatch {
        column_name: String,
        expect: ConcreteDataType,
        actual: ConcreteDataType,
    },

    #[snafu(display("Invalid database name: {}", name))]
    InvalidDatabaseName { name: String, backtrace: Backtrace },

    #[snafu(display("Invalid table name: {}", name))]
    InvalidTableName { name: String, backtrace: Backtrace },

    #[snafu(display("Invalid default constraint, column: {}, source: {}", column, source))]
    InvalidDefault {
        column: String,
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Unsupported ALTER TABLE statement: {}", msg))]
    UnsupportedAlterTableStatement { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to serialize column default constraint, source: {}", source))]
    SerializeColumnDefaultConstraint {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "Failed to convert data type to gRPC data type defined in proto, source: {}",
        source
    ))]
    ConvertToGrpcDataType {
        #[snafu(backtrace)]
        source: api::error::Error,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            UnsupportedDefaultValue { .. } | Unsupported { .. } => StatusCode::Unsupported,
            Unexpected { .. }
            | Syntax { .. }
            | InvalidTimeIndex { .. }
            | Tokenizer { .. }
            | InvalidSql { .. }
            | ParseSqlValue { .. }
            | SqlTypeNotSupported { .. }
            | InvalidDefault { .. } => StatusCode::InvalidSyntax,

            InvalidDatabaseName { .. } | ColumnTypeMismatch { .. } | InvalidTableName { .. } => {
                StatusCode::InvalidArguments
            }
            UnsupportedAlterTableStatement { .. } => StatusCode::InvalidSyntax,
            SerializeColumnDefaultConstraint { source, .. } => source.status_code(),
            ConvertToGrpcDataType { source, .. } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
