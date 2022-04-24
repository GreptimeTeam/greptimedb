use errors::ParserError;
use snafu::prelude::*;

use crate::errors;
use crate::parser::ParserContext;
use crate::statements::statement::Statement;
use crate::statements::statement_query::SqlQuery;

impl<'a> ParserContext<'a> {
    /// Parses select and it's variants.
    pub(crate) fn parse_query(&mut self) -> Result<Statement, ParserError> {
        let spquery = self.parser.parse_query().context(errors::UnexpectedSnafu {
            sql: self.sql,
            expected: "EXPECT",
            actual: "ACTUAL",
        })?;

        Ok(Statement::Query(Box::new(SqlQuery::try_from(spquery)?)))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::ast::SetExpr;
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::parser::ParserContext;

    #[test]
    pub fn test_parse_query() {
        let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

        let parser = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        println!("{:?}", parser);
    }

    #[test]
    pub fn test_parser_invalid_query() {
        // sql without selection
        let sql = "SELECT FROM table_1";

        let parser = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        match &parser[0] {
            Statement::ShowDatabases(_) => {
                panic!("Not expected to be a show database statement")
            }
            Statement::Query(q) => {
                println!("{:?}", q.projection);
                assert_eq!(1, q.projection.len());
                assert_matches!(q.set_expr, SetExpr::Select(_))
            }
        }
    }
}
