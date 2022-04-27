use sqlparser::ast::{Query, Statement as SpStatement};
use sqlparser::parser::ParserError::ParserError;

use crate::statements::statement_query::SqlQuery;
use crate::statements::statement_show_database::SqlShowDatabase;

/// Tokens parsed by `DFParser` are converted into these values.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    // Databases.
    ShowDatabases(SqlShowDatabase),

    // Query
    Query(Box<SqlQuery>),
}

/// Converts Statement to sqlparser statement
impl TryInto<SpStatement> for Statement {
    type Error = sqlparser::parser::ParserError;

    fn try_into(self) -> Result<SpStatement, Self::Error> {
        match self {
            Statement::ShowDatabases(_) => Err(ParserError(
                "sqlparser does not support SHOW DATABASE query.".to_string(),
            )),
            Statement::Query(q) => Ok(SpStatement::Query(Box::new(Query {
                with: q.with,
                body: q.set_expr,
                order_by: q.order_by,
                limit: q.limit,
                offset: q.offset,
                fetch: q.fetch,
                lock: None,
            }))),
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
