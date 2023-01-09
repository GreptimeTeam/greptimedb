use common_error::prelude::BoxedError;
use common_telemetry::timer;
use promql_parser::parser::EvalStmt;
use snafu::ResultExt;
use sql::dialect::GenericDialect;
use sql::parser::ParserContext;
use sql::statements::statement::Statement;

use crate::error::{MultipleStatementsSnafu, QueryParseSnafu, Result};
use crate::metric::METRIC_PARSE_SQL_ELAPSED;

pub enum QueryStatement {
    SQL(Statement),
    PromQL(EvalStmt),
}

pub struct QueryLanguageParser {}

impl QueryLanguageParser {
    pub fn parse_sql(sql: &str) -> Result<QueryStatement> {
        let _timer = timer!(METRIC_PARSE_SQL_ELAPSED);
        let mut statement = ParserContext::create_with_dialect(sql, &GenericDialect {})
            .map_err(BoxedError::new)
            .context(QueryParseSnafu {
                query: sql.to_string(),
            })?;
        if statement.len() != 1 {
            MultipleStatementsSnafu {
                query: sql.to_string(),
            }
            .fail()
        } else {
            Ok(QueryStatement::SQL(statement.pop().unwrap()))
        }
    }
}
