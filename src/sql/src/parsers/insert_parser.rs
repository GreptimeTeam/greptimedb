use snafu::ResultExt;
use sqlparser::ast::Statement as SpStatement;

use crate::errors;
use crate::parser::ParserContext;
use crate::parser::Result;
use crate::statements::insert::Insert;
use crate::statements::statement::Statement;

/// INSERT statement parser implementation
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_insert(&mut self) -> Result<Statement> {
        self.parser.next_token();
        let spstatement = self
            .parser
            .parse_insert()
            .context(errors::InnerSnafu { sql: self.sql })?;

        match spstatement {
            SpStatement::Insert { .. } => {
                Ok(Statement::Insert(Box::new(Insert { inner: spstatement })))
            }
            unexp => errors::UnsupportedSnafu {
                sql: self.sql.to_string(),
                keyword: unexp.to_string(),
            }
            .fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::dialect::GenericDialect;

    use super::*;

    #[test]
    pub fn test_parse_insert() {
        let sql = r"INSERT INTO table_1 VALUES (
            'test1',1,'true',
            'test2',2,'false')
         ";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());
        assert_matches!(result[0], Statement::Insert { .. })
    }

    #[test]
    pub fn test_parse_invalid_insert() {
        let sql = r"INSERT INTO table_1 VALUES ("; // intentionally a bad sql
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result.is_err(), "result is: {:?}", result);
    }
}
