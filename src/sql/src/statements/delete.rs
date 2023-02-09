use sqlparser::ast::{Expr, ObjectName, Statement, TableFactor};
use sqlparser::parser::ParserError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    // Can only be sqlparser::ast::Statement::Delete variant
    pub inner: Statement,
}

impl Delete {
    pub fn table_name(&self) -> &ObjectName {
        match &self.inner {
            Statement::Delete { table_name, .. } => match table_name {
                TableFactor::Table { name, .. } => name,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    pub fn selection(&self) -> &Option<Expr> {
        match &self.inner {
            Statement::Delete { selection, .. } => selection,
            _ => unreachable!(),
        }
    }
}

impl TryFrom<Statement> for Delete {
    type Error = ParserError;

    fn try_from(stmt: Statement) -> Result<Self, Self::Error> {
        match stmt {
            Statement::Delete { .. } => Ok(Delete { inner: stmt }),
            unexp => Err(ParserError::ParserError(format!(
                "Not expected to be {unexp}"
            ))),
        }
    }
}
