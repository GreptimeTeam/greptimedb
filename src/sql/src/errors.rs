use common_error::prelude::*;
use sqlparser::parser::ParserError as SpParserError;

/// SQL parser errors.
// Now the error in parser does not contains backtrace to avoid generating backtrace
// every time the parser parses an invalid SQL.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ParserError {
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
        source: SpParserError,
    },

    // Syntax error from sql parser.
    #[snafu(display("Syntax error, sql: {}, source: {}", sql, source))]
    SpSyntax { sql: String, source: SpParserError },
}

impl ErrorExt for ParserError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Unsupported { .. } => StatusCode::Unsupported,
            Self::Unexpected { .. } | Self::SpSyntax { .. } => StatusCode::InvalidSyntax,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    pub fn test_error_conversion() {
        pub fn raise_error() -> Result<(), SpParserError> {
            Err(SpParserError::ParserError("parser error".to_string()))
        }

        assert_matches!(
            raise_error().context(SpSyntaxSnafu {
                sql: "".to_string(),
            }),
            Err(ParserError::SpSyntax {
                sql: _,
                source: SpParserError::ParserError { .. }
            })
        )
    }
}
