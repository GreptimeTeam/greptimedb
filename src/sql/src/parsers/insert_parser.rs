use snafu::ResultExt;
use sqlparser::ast::Statement as SpStatement;

use crate::errors;
use crate::errors::ParserError;
use crate::parser::ParserContext;
use crate::statements::insert::Insert;
use crate::statements::statement::Statement;

/// INSERT statement parser implementation
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_insert(&mut self) -> Result<Statement, ParserError> {
        self.parser.next_token();
        let spstatement = self
            .parser
            .parse_insert()
            .context(errors::InnerSnafu { sql: self.sql })?;

        match spstatement {
            SpStatement::Insert { .. } => {
                Ok(Statement::Insert(Box::new(Insert { inner: spstatement })))
            }
            unexp => Err(errors::ParserError::Unsupported {
                sql: self.sql.to_string(),
                keyword: unexp.to_string(),
            }),
        }
    }
}
