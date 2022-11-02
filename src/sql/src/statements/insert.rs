use sqlparser::ast::{SetExpr, Statement, UnaryOperator, Values};
use sqlparser::parser::ParserError;

use crate::ast::{Expr, Value};
use crate::error::{self, Result};

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

    pub fn values(&self) -> Result<Vec<Vec<Value>>> {
        let values = match &self.inner {
            Statement::Insert { source, .. } => match &source.body {
                SetExpr::Values(Values(exprs)) => sql_exprs_to_values(exprs)?,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
        Ok(values)
    }
}

fn sql_exprs_to_values(exprs: &Vec<Vec<Expr>>) -> Result<Vec<Vec<Value>>> {
    let mut values = Vec::with_capacity(exprs.len());
    for es in exprs.iter() {
        let mut vs = Vec::with_capacity(es.len());
        for expr in es.iter() {
            vs.push(match expr {
                Expr::Value(v) => v.clone(),
                Expr::Identifier(ident) => Value::SingleQuotedString(ident.value.clone()),
                Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: x,
                } if matches!(**x, Expr::Value(_)) => match &**x {
                    Expr::Value(Value::Number(s, b)) => Value::Number(format!("-{}", s), *b),
                    _ => {
                        return error::ParseSqlValueSnafu {
                            msg: format!("{:?}", expr),
                        }
                        .fail()
                    }
                },
                _ => {
                    return error::ParseSqlValueSnafu {
                        msg: format!("{:?}", expr),
                    }
                    .fail()
                }
            });
        }
        values.push(vs);
    }
    Ok(values)
}

impl TryFrom<Statement> for Insert {
    type Error = ParserError;

    fn try_from(value: Statement) -> std::result::Result<Self, Self::Error> {
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
        let r: std::result::Result<Statement, ParserError> = insert.try_into();
        assert!(r.is_ok());
    }

    #[test]
    fn test_insert_negative_value() {
        use crate::statements::statement::Statement;

        let sql = "INSERT INTO my_table VALUES(-1)";
        let stmt = ParserContext::create_with_dialect(sql, &GenericDialect {})
            .unwrap()
            .remove(0);
        match stmt {
            Statement::Insert(insert) => {
                let values = insert.values().unwrap();
                assert_eq!(values, vec![vec![Value::Number("-1".to_string(), false)]]);
            }
            _ => unreachable!(),
        }
    }
}
