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

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::parser::ParserContext;

    #[test]
    pub fn test_insert_convert() {
        let sql = r"INSERT INTO tables_0 VALUES ( 'field_0', 0) ";
        let mut stmts = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        let insert = stmts.pop().unwrap();
        let r: Result<Statement, ParserError> = insert.try_into();
        r.unwrap();
    }
}
