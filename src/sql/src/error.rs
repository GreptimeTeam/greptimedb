use std::any::Any;

use common_error::prelude::*;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::TokenizerError;

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

    // Syntax error from sql parser.
    #[snafu(display("Syntax error, sql: {}, source: {}", sql, source))]
    Syntax { sql: String, source: ParserError },

    #[snafu(display("Tokenizer error, sql: {}, source: {}", sql, source))]
    Tokenizer { sql: String, source: TokenizerError },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            Unsupported { .. } => StatusCode::Unsupported,
            Unexpected { .. } | Syntax { .. } | Tokenizer { .. } => StatusCode::InvalidSyntax,
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

    fn throw_sp_error() -> Result<(), ParserError> {
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
