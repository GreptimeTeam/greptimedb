// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use sqlparser::ast::{ObjectName, Query, SetExpr, Statement, UnaryOperator, Values};
use sqlparser::parser::ParserError;

use crate::ast::{Expr, Value};
use crate::error::Result;
use crate::statements::query::Query as GtQuery;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insert {
    // Can only be sqlparser::ast::Statement::Insert variant
    pub inner: Statement,
}

impl Insert {
    pub fn new(inner: Statement) -> Self {
        Self { inner }
    }

    pub fn table_name(&self) -> &ObjectName {
        match &self.inner {
            Statement::Insert { table_name, .. } => table_name,
            _ => unreachable!(),
        }
    }

    pub fn columns(&self) -> Vec<&String> {
        match &self.inner {
            Statement::Insert { columns, .. } => columns.iter().map(|ident| &ident.value).collect(),
            _ => unreachable!(),
        }
    }

    // Try to extract values for insertion.If the statement contains other expressons
    // Extracts the literal insert statement body if any, and returns `None` if the body of insert statement contains non literal values.
    pub fn values_body(&self) -> Option<Vec<Vec<Value>>> {
        match &self.inner {
            Statement::Insert {
                source:
                    box Query {
                        body: box SetExpr::Values(Values { rows, .. }),
                        ..
                    },
                ..
            } => sql_exprs_to_values(rows),
            _ => None,
        }
    }

    // Returns true when the insert statement can extract literal values.
    // The rules is the same as function `values_body()`.
    pub fn can_extract_values(&self) -> bool {
        match &self.inner {
            Statement::Insert {
                source:
                    box Query {
                        body: box SetExpr::Values(Values { rows, .. }),
                        ..
                    },
                ..
            } => rows.iter().all(|es| {
                es.iter().all(|expr| match expr {
                    Expr::Value(_) => true,
                    Expr::Identifier(ident) => {
                        if ident.quote_style.is_none() {
                            ident.value.to_lowercase() == "default"
                        } else {
                            true
                        }
                    }
                    Expr::UnaryOp { op, expr } => {
                        matches!(op, UnaryOperator::Minus | UnaryOperator::Plus)
                            && matches!(&**expr, Expr::Value(Value::Number(_, _)))
                    }
                    _ => false,
                })
            }),
            _ => false,
        }
    }

    pub fn query_body(&self) -> Result<Option<GtQuery>> {
        Ok(match &self.inner {
            Statement::Insert {
                source: box query, ..
            } => Some(query.clone().try_into()?),
            _ => None,
        })
    }
}

fn sql_exprs_to_values(exprs: &Vec<Vec<Expr>>) -> Option<Vec<Vec<Value>>> {
    let mut values = Vec::with_capacity(exprs.len());
    for es in exprs.iter() {
        let mut vs = Vec::with_capacity(es.len());
        for expr in es.iter() {
            vs.push(match expr {
                Expr::Value(v) => v.clone(),
                Expr::Identifier(ident) => {
                    if ident.quote_style.is_none() {
                        // Special processing for `default` value
                        if ident.value.to_lowercase() == "default" {
                            Value::Placeholder(ident.value.clone())
                        } else {
                            return None;
                        }
                    } else {
                        // Identifier with double quotes, we treat it as string.
                        Value::SingleQuotedString(ident.value.clone())
                    }
                }
                Expr::UnaryOp { op, expr }
                    if matches!(op, UnaryOperator::Minus | UnaryOperator::Plus) =>
                {
                    if let Expr::Value(Value::Number(s, b)) = &**expr {
                        match op {
                            UnaryOperator::Minus => Value::Number(format!("-{s}"), *b),
                            UnaryOperator::Plus => Value::Number(s.to_string(), *b),
                            _ => unreachable!(),
                        }
                    } else {
                        return None;
                    }
                }
                _ => {
                    return None;
                }
            });
        }
        values.push(vs);
    }
    Some(values)
}

impl TryFrom<Statement> for Insert {
    type Error = ParserError;

    fn try_from(value: Statement) -> std::result::Result<Self, Self::Error> {
        match value {
            Statement::Insert { .. } => Ok(Insert::new(value)),
            unexp => Err(ParserError::ParserError(format!(
                "Not expected to be {unexp}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParserContext;
    use crate::statements::statement::Statement;

    #[test]
    fn test_insert_value_with_unary_op() {
        // insert "-1"
        let sql = "INSERT INTO my_table VALUES(-1)";
        let stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .remove(0);
        match stmt {
            Statement::Insert(insert) => {
                let values = insert.values_body().unwrap();
                assert_eq!(values, vec![vec![Value::Number("-1".to_string(), false)]]);
            }
            _ => unreachable!(),
        }

        // insert "+1"
        let sql = "INSERT INTO my_table VALUES(+1)";
        let stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .remove(0);
        match stmt {
            Statement::Insert(insert) => {
                let values = insert.values_body().unwrap();
                assert_eq!(values, vec![vec![Value::Number("1".to_string(), false)]]);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_insert_value_with_default() {
        // insert "default"
        let sql = "INSERT INTO my_table VALUES(default)";
        let stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .remove(0);
        match stmt {
            Statement::Insert(insert) => {
                let values = insert.values_body().unwrap();
                assert_eq!(values, vec![vec![Value::Placeholder("default".to_owned())]]);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_insert_value_with_default_uppercase() {
        // insert "DEFAULT"
        let sql = "INSERT INTO my_table VALUES(DEFAULT)";
        let stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .remove(0);
        match stmt {
            Statement::Insert(insert) => {
                let values = insert.values_body().unwrap();
                assert_eq!(values, vec![vec![Value::Placeholder("DEFAULT".to_owned())]]);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_insert_value_with_quoted_string() {
        // insert "'default'"
        let sql = "INSERT INTO my_table VALUES('default')";
        let stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .remove(0);
        match stmt {
            Statement::Insert(insert) => {
                let values = insert.values_body().unwrap();
                assert_eq!(
                    values,
                    vec![vec![Value::SingleQuotedString("default".to_owned())]]
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_insert_select() {
        let sql = "INSERT INTO my_table select * from other_table";
        let stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .remove(0);
        match stmt {
            Statement::Insert(insert) => {
                let q = insert.query_body().unwrap().unwrap();
                assert!(matches!(
                    q.inner,
                    Query {
                        body: box SetExpr::Select { .. },
                        ..
                    }
                ));
            }
            _ => unreachable!(),
        }
    }
}
