use sqlparser::ast::Statement as SpStatement;
use sqlparser::parser::ParserError::ParserError;

use crate::statements::statement_query::Query;
use crate::statements::statement_show_database::SqlShowDatabase;

/// Tokens parsed by `DFParser` are converted into these values.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    // Databases.
    ShowDatabases(SqlShowDatabase),

    // Query
    Query(Box<Query>),
}

/// Converts Statement to sqlparser statement
impl TryFrom<Statement> for SpStatement {
    type Error = sqlparser::parser::ParserError;

    fn try_from(value: Statement) -> Result<Self, Self::Error> {
        match value {
            Statement::ShowDatabases(_) => Err(ParserError(
                "sqlparser does not support SHOW DATABASE query.".to_string(),
            )),
            Statement::Query(s) => Ok(SpStatement::Query(Box::new(s.inner))),
        }
    }
}

/// Comment hints from SQL.
/// It'll be enabled when using `--comment` in mysql client.
/// Eg: `SELECT * FROM system.number LIMIT 1; -- { ErrorCode 25 }`
#[derive(Debug, Clone, PartialEq)]
pub struct Hint {
    pub error_code: Option<u16>,
    pub comment: String,
    pub prefix: String,
}
