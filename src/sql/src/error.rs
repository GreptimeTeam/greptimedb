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
        "Unexpected token while parsing SQL statement: {}, expected: {}, found: {}, source: {}",
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
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    fn throw_sp_error() -> std::result::Result<(), ParserError> {
        Err(ParserError::ParserError("parser error".to_string()))
    }

    #[test]
    fn test_syntax_error() {
        let err = throw_sp_error()
            .context(SyntaxSnafu { sql: "" })
            .err()
            .unwrap();
        assert_matches!(
            err,
            Error::Syntax {
                sql: _,
                source: ParserError::ParserError { .. }
            }
        );
        assert_eq!(StatusCode::InvalidSyntax, err.status_code());

        let err = throw_sp_error()
            .context(UnexpectedSnafu {
                sql: "",
                expected: "",
                actual: "",
            })
            .err()
            .unwrap();
        assert_eq!(StatusCode::InvalidSyntax, err.status_code());
    }

    #[test]
    fn test_unsupported_error() {
        let err = Error::Unsupported {
            sql: "".to_string(),
            keyword: "".to_string(),
        };
        assert_eq!(StatusCode::Unsupported, err.status_code());
    }

    #[test]
    pub fn test_tokenizer_error() {
        let err = Error::Tokenizer {
            sql: "".to_string(),
            source: sqlparser::tokenizer::TokenizerError {
                message: "tokenizer error".to_string(),
                col: 1,
                line: 1,
            },
        };

        assert_eq!(StatusCode::InvalidSyntax, err.status_code());
    }
}
