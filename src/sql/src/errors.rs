use snafu::prelude::*;
use sqlparser::parser::ParserError as SpParserError;

/// SQL parser errors.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ParserError {
    #[snafu(display("SQL statement is not supported: {sql}, keyword: {keyword}"))]
    Unsupported { sql: String, keyword: String },

    #[snafu(display(
        "Unexpected token while parsing SQL statement: {sql}, expected: {expected}, found: {actual}, source: {source}"
    ))]
    Unexpected {
        sql: String,
        expected: String,
        actual: String,
        source: SpParserError,
    },

    #[snafu(display("SQL syntax error: {msg}"))]
    SyntaxError { msg: String },

    #[snafu(display("Unknown inner parser error, sql: {sql}, source: {source}"))]
    InnerError { sql: String, source: SpParserError },
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use snafu::ResultExt;

    #[test]
    pub fn test_error_conversion() {
        pub fn raise_error() -> Result<(), sqlparser::parser::ParserError> {
            Err(sqlparser::parser::ParserError::ParserError(
                "parser error".to_string(),
            ))
        }

        assert_matches!(
            raise_error().context(crate::errors::InnerSnafu {
                sql: "".to_string(),
            }),
            Err(super::ParserError::InnerError {
                sql: _,
                source: sqlparser::parser::ParserError::ParserError { .. }
            })
        )
    }
}
