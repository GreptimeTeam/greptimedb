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

use serde::Serialize;
use sqlparser::ast::{
    Insert as SpInsert, ObjectName, Query, SetExpr, Statement, TableObject, UnaryOperator,
    ValueWithSpan, Values,
};
use sqlparser::parser::ParserError;
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{Expr, Value};
use crate::error::{Result, UnsupportedSnafu};
use crate::statements::query::Query as GtQuery;

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct Insert {
    // Can only be sqlparser::ast::Statement::Insert variant
    pub inner: Statement,
}

macro_rules! parse_fail {
    ($expr: expr) => {
        return crate::error::ParseSqlValueSnafu {
            msg: format!("{:?}", $expr),
        }
        .fail();
    };
}

impl Insert {
    pub fn table_name(&self) -> Result<&ObjectName> {
        match &self.inner {
            Statement::Insert(insert) => {
                let TableObject::TableName(name) = &insert.table else {
                    return UnsupportedSnafu {
                        keyword: "TABLE FUNCTION".to_string(),
                    }
                    .fail();
                };
                Ok(name)
            }
            _ => unreachable!(),
        }
    }

    pub fn columns(&self) -> Vec<&String> {
        match &self.inner {
            Statement::Insert(insert) => insert.columns.iter().map(|ident| &ident.value).collect(),
            _ => unreachable!(),
        }
    }

    /// Extracts the literal insert statement body if possible
    pub fn values_body(&self) -> Result<Vec<Vec<Value>>> {
        match &self.inner {
            Statement::Insert(SpInsert {
                source:
                    Some(box Query {
                        body: box SetExpr::Values(Values { rows, .. }),
                        ..
                    }),
                ..
            }) => sql_exprs_to_values(rows),
            _ => unreachable!(),
        }
    }

    /// Returns true when the insert statement can extract literal values.
    /// The rules is the same as function `values_body()`.
    pub fn can_extract_values(&self) -> bool {
        match &self.inner {
            Statement::Insert(SpInsert {
                source:
                    Some(box Query {
                        body: box SetExpr::Values(Values { rows, .. }),
                        ..
                    }),
                ..
            }) => rows.iter().all(|es| {
                es.iter().all(|expr| match expr {
                    Expr::Value(_) => true,
                    Expr::Identifier(ident) => {
                        if ident.quote_style.is_none() {
                            ident.value.to_lowercase() == "default"
                        } else {
                            ident.quote_style == Some('"')
                        }
                    }
                    Expr::UnaryOp { op, expr } => {
                        matches!(op, UnaryOperator::Minus | UnaryOperator::Plus)
                            && matches!(
                                &**expr,
                                Expr::Value(ValueWithSpan {
                                    value: Value::Number(_, _),
                                    ..
                                })
                            )
                    }
                    _ => false,
                })
            }),
            _ => false,
        }
    }

    pub fn query_body(&self) -> Result<Option<GtQuery>> {
        Ok(match &self.inner {
            Statement::Insert(SpInsert {
                source: Some(box query),
                ..
            }) => Some(query.clone().try_into()?),
            _ => None,
        })
    }
}

fn sql_exprs_to_values(exprs: &[Vec<Expr>]) -> Result<Vec<Vec<Value>>> {
    let mut values = Vec::with_capacity(exprs.len());
    for es in exprs.iter() {
        let mut vs = Vec::with_capacity(es.len());
        for expr in es.iter() {
            vs.push(match expr {
                Expr::Value(v) => v.value.clone(),
                Expr::Identifier(ident) => {
                    if ident.quote_style.is_none() {
                        // Special processing for `default` value
                        if ident.value.to_lowercase() == "default" {
                            Value::Placeholder(ident.value.clone())
                        } else {
                            parse_fail!(expr);
                        }
                    } else {
                        // Identifiers with double quotes, we treat them as strings.
                        if ident.quote_style == Some('"') {
                            Value::SingleQuotedString(ident.value.clone())
                        } else {
                            parse_fail!(expr);
                        }
                    }
                }
                Expr::UnaryOp { op, expr }
                    if matches!(op, UnaryOperator::Minus | UnaryOperator::Plus) =>
                {
                    if let Expr::Value(ValueWithSpan {
                        value: Value::Number(s, b),
                        ..
                    }) = &**expr
                    {
                        match op {
                            UnaryOperator::Minus => Value::Number(format!("-{s}"), *b),
                            UnaryOperator::Plus => Value::Number(s.to_string(), *b),
                            _ => unreachable!(),
                        }
                    } else {
                        parse_fail!(expr);
                    }
                }
                _ => {
                    parse_fail!(expr);
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
                "Not expected to be {unexp}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_insert_value_with_unary_op() {
        // insert "-1"
        let sql = "INSERT INTO my_table VALUES(-1)";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
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
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
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
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
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
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
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
        // insert 'default'
        let sql = "INSERT INTO my_table VALUES('default')";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
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

        // insert "default". Treating double-quoted identifiers as strings.
        let sql = "INSERT INTO my_table VALUES(\"default\")";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
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

        let sql = "INSERT INTO my_table VALUES(`default`)";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .remove(0);
        match stmt {
            Statement::Insert(insert) => {
                assert!(insert.values_body().is_err());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_insert_select() {
        let sql = "INSERT INTO my_table select * from other_table";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
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
