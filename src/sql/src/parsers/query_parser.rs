use snafu::prelude::*;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::query::Query;
use crate::statements::statement::Statement;

impl<'a> ParserContext<'a> {
    /// Parses select and it's variants.
    pub(crate) fn parse_query(&mut self) -> Result<Statement> {
        let spquery = self
            .parser
            .parse_query()
            .context(error::SyntaxSnafu { sql: self.sql })?;

        Ok(Statement::Query(Box::new(Query::try_from(spquery)?)))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

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
    pub fn test_parse_invalid_query() {
        let sql = "SELECT * FROM table_1 WHERE";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected an expression"));
    }
}
