use snafu::prelude::*;
use sqlparser::parser::ParserError as SpParserError;

/// SQL parser errors.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ParserError {
    #[snafu(display("SQL statement is not supported: {sql}"))]
    Unsupported { sql: String },

    #[snafu(display(
        "Unexpected token while parsing SQL statement: {sql}, expected: {expected}, found: {actual}"
    ))]
    Unexpected {
        sql: String,
        expected: String,
        actual: String,
        source: SpParserError,
    },
}
