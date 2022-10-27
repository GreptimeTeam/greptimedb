use sqlparser::ast::{SetExpr, Statement, Values};
use sqlparser::parser::ParserError;

use crate::ast::{Expr, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insert {
    // Can only be sqlparser::ast::Statement::Insert variant
    pub inner: Statement,
}

impl Insert {
    pub fn table_name(&self) -> String {
        match &self.inner {
            Statement::Insert { table_name, .. } => {
                // FIXME(dennis): table_name may be in the form of "catalog.schema.table"
                table_name.to_string()
            }
            _ => unreachable!(),
        }
    }

    pub fn columns(&self) -> Vec<&String> {
        match &self.inner {
            Statement::Insert { columns, .. } => columns.iter().map(|ident| &ident.value).collect(),
            _ => unreachable!(),
        }
    }

    pub fn values(&self) -> Vec<Vec<Value>> {
        match &self.inner {
            Statement::Insert { source, .. } => match &source.body {
                SetExpr::Values(Values(values)) => values
                    .iter()
                    .map(|v| {
                        v.iter()
                            .map(|expr| match expr {
                                Expr::Value(v) => v.clone(),
                                Expr::Identifier(ident) => {
                                    Value::SingleQuotedString(ident.value.clone())
                                }
                                _ => unreachable!("unknown value {}", expr),
                            })
                            .collect::<Vec<Value>>()
                    })
                    .collect(),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
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
