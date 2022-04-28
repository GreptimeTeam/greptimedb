use snafu::prelude::*;

use crate::errors;
use crate::parser::ParserContext;
use crate::parser::Result;
use crate::statements::query::Query;
use crate::statements::statement::Statement;

impl<'a> ParserContext<'a> {
    /// Parses select and it's variants.
    pub(crate) fn parse_query(&mut self) -> Result<Statement> {
        let spquery = self
            .parser
            .parse_query()
            .context(errors::SpSyntaxSnafu { sql: self.sql })?;

        Ok(Statement::Query(Box::new(Query::try_from(spquery)?)))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::parser::ParserContext;

    #[test]
    pub fn test_parse_query() {
        let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

        let _ = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
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
            Statement::Insert(_) => {
                panic!("Not expected to be a show database statement")
            }
            Statement::Query(_) => {}
        }
    }
}
