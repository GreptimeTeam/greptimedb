use sqlparser::ast::Statement;
use sqlparser::parser::ParserError;

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    // Can only be sqlparser::ast::Statement::Insert variant
    pub inner: Statement,
}

impl TryFrom<Statement> for Insert {
    type Error = ParserError;

    fn try_from(value: Statement) -> Result<Self, Self::Error> {
        match value {
            Statement::Insert { .. } => Ok(Insert { inner: value }),
            unexp => Err(ParserError::ParserError(format!(
                "Not expected to be {}",
                unexp
            ))),
        }
    }
}
