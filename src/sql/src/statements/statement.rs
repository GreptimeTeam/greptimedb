use sqlparser::ast::Statement as SpStatement;
use sqlparser::parser::ParserError;

use crate::statements::alter::AlterTable;
use crate::statements::create_table::CreateTable;
use crate::statements::insert::Insert;
use crate::statements::query::Query;
use crate::statements::show::{ShowDatabases, ShowTables};

/// Tokens parsed by `DFParser` are converted into these values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    // Query
    Query(Box<Query>),
    // Insert
    Insert(Box<Insert>),
    /// CREATE TABLE
    Create(CreateTable),
    /// ALTER TABLE
    Alter(AlterTable),
    // Databases.
    ShowDatabases(ShowDatabases),
    // SHOW TABLES
    ShowTables(ShowTables),
}

/// Converts Statement to sqlparser statement
impl TryFrom<Statement> for SpStatement {
    type Error = sqlparser::parser::ParserError;

    fn try_from(value: Statement) -> Result<Self, Self::Error> {
        match value {
            Statement::ShowDatabases(_) => Err(ParserError::ParserError(
                "sqlparser does not support SHOW DATABASE query.".to_string(),
            )),
            Statement::ShowTables(_) => Err(ParserError::ParserError(
                "sqlparser does not support SHOW TABLES query.".to_string(),
            )),
            Statement::Query(s) => Ok(SpStatement::Query(Box::new(s.inner))),
            Statement::Insert(i) => Ok(i.inner),
            Statement::Create(_) | Statement::Alter(_) => unimplemented!(),
        }
    }
}

/// Comment hints from SQL.
/// It'll be enabled when using `--comment` in mysql client.
/// Eg: `SELECT * FROM system.number LIMIT 1; -- { ErrorCode 25 }`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hint {
    pub error_code: Option<u16>,
    pub comment: String,
    pub prefix: String,
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::parser::ParserContext;

    #[test]
    pub fn test_statement_convert() {
        let sql = "SELECT * FROM table_0";
        let mut stmts = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        let x = stmts.remove(0);
        let statement = SpStatement::try_from(x).unwrap();

        assert_matches!(statement, SpStatement::Query { .. });
    }
}
