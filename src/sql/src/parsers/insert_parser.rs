use snafu::ResultExt;
use sqlparser::ast::Statement as SpStatement;

use crate::error;
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
            .context(error::SyntaxSnafu { sql: self.sql })?;

        match spstatement {
            SpStatement::Insert { .. } => {
                Ok(Statement::Insert(Box::new(Insert { inner: spstatement })))
            }
            unexp => error::UnsupportedSnafu {
                sql: self.sql.to_string(),
                keyword: unexp.to_string(),
            }
            .fail(),
        }
    }
}
