use snafu::ResultExt;
use sqlparser::ast::Statement as SpStatement;

use crate::parser::ParserContext;
use crate::error::{self, Result};
use crate::statements::delete::Delete;
use crate::statements::statement::Statement;

/// DELETE statement parser implementation
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_delete(&mut self) -> Result<Statement> {
        self.parser.next_token();
        let spstatement = self
            .parser
            .parse_delete()
            .context(error::SyntaxSnafu { sql: self.sql })?;

        match spstatement {
            SpStatement::Delete { .. } => {
                Ok(Statement::Delete(Box::new(Delete { inner: spstatement })))
            }
            unexp => error::UnsupportedSnafu {
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
        let sql = r"delete from my_table where k1 = xxx and k2 = xxx and timestamp = xxx;";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        println!("result is: {:#?}", result);
        assert_eq!(1, result.len());
        assert_matches!(result[0], Statement::Delete { .. })
    }

    #[test]
    pub fn test_parse_invalid_insert() {
        let sql = r"delete my_table where "; // intentionally a bad sql
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result.is_err(), "result is: {result:?}");
    }
}
