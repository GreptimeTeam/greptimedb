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

use arrow::array::StringArray;
use arrow::compute::kernels::comparison;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::Like;
use datafusion::logical_expr::{Expr, Operator};
use datatypes::value::Value;
use store_api::storage::ScanRequest;

type ColumnName = String;
/// Predicate to filter `information_schema` tables stream,
/// we only support these simple predicates currently.
/// TODO(dennis): supports more predicate types.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum Predicate {
    Eq(ColumnName, Value),
    Like(ColumnName, String, bool),
    NotEq(ColumnName, Value),
    InList(ColumnName, Vec<Value>),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    /// Evaluate the predicate with the row, returns:
    /// - `None` when the predicate can't evaluate with the row.
    /// - `Some(true)` when the predicate is satisfied,
    /// - `Some(false)` when the predicate is not satisfied,
    fn eval(&self, row: &[(&str, &Value)]) -> Option<bool> {
        match self {
            Predicate::Eq(c, v) => {
                for (column, value) in row {
                    if c != column {
                        continue;
                    }
                    return Some(v == *value);
                }
            }
            Predicate::Like(c, pattern, case_insensitive) => {
                for (column, value) in row {
                    if c != column {
                        continue;
                    }

                    let Value::String(bs) = value else {
                        continue;
                    };

                    return like_utf8(bs.as_utf8(), pattern, case_insensitive);
                }
            }
            Predicate::NotEq(c, v) => {
                for (column, value) in row {
                    if c != column {
                        continue;
                    }
                    return Some(v != *value);
                }
            }
            Predicate::InList(c, values) => {
                for (column, value) in row {
                    if c != column {
                        continue;
                    }
                    return Some(values.iter().any(|v| v == *value));
                }
            }
            Predicate::And(left, right) => {
                let left = left.eval(row);

                // short-circuit
                if matches!(left, Some(false)) {
                    return Some(false);
                }

                return match (left, right.eval(row)) {
                    (Some(left), Some(right)) => Some(left && right),
                    (None, Some(false)) => Some(false),
                    _ => None,
                };
            }
            Predicate::Or(left, right) => {
                let left = left.eval(row);

                // short-circuit
                if matches!(left, Some(true)) {
                    return Some(true);
                }

                return match (left, right.eval(row)) {
                    (Some(left), Some(right)) => Some(left || right),
                    (None, Some(true)) => Some(true),
                    _ => None,
                };
            }
            Predicate::Not(p) => {
                return Some(!p.eval(row)?);
            }
        }

        // Can't evaluate predicate with the row
        None
    }

    /// Try to create a predicate from datafusion [`Expr`], return None if fails.
    fn from_expr(expr: Expr) -> Option<Predicate> {
        match expr {
            // NOT expr
            Expr::Not(expr) => Some(Predicate::Not(Box::new(Self::from_expr(*expr)?))),
            // expr LIKE pattern
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                case_insensitive,
                ..
            }) if is_column(&expr) && is_string_literal(&pattern) => {
                // Safety: ensured by gurad
                let Expr::Column(c) = *expr else {
                    unreachable!();
                };
                let Expr::Literal(ScalarValue::Utf8(Some(pattern)), _) = *pattern else {
                    unreachable!();
                };

                let p = Predicate::Like(c.name, pattern, case_insensitive);

                if negated {
                    Some(Predicate::Not(Box::new(p)))
                } else {
                    Some(p)
                }
            }
            // left OP right
            Expr::BinaryExpr(bin) => match (*bin.left, bin.op, *bin.right) {
                // left == right
                (Expr::Literal(scalar, _), Operator::Eq, Expr::Column(c))
                | (Expr::Column(c), Operator::Eq, Expr::Literal(scalar, _)) => {
                    let Ok(v) = Value::try_from(scalar) else {
                        return None;
                    };

                    Some(Predicate::Eq(c.name, v))
                }
                // left != right
                (Expr::Literal(scalar, _), Operator::NotEq, Expr::Column(c))
                | (Expr::Column(c), Operator::NotEq, Expr::Literal(scalar, _)) => {
                    let Ok(v) = Value::try_from(scalar) else {
                        return None;
                    };

                    Some(Predicate::NotEq(c.name, v))
                }
                // left AND right
                (left, Operator::And, right) => {
                    let left = Self::from_expr(left)?;
                    let right = Self::from_expr(right)?;

                    Some(Predicate::And(Box::new(left), Box::new(right)))
                }
                // left OR right
                (left, Operator::Or, right) => {
                    let left = Self::from_expr(left)?;
                    let right = Self::from_expr(right)?;

                    Some(Predicate::Or(Box::new(left), Box::new(right)))
                }
                _ => None,
            },
            // [NOT] IN (LIST)
            Expr::InList(list) => {
                match (*list.expr, list.list, list.negated) {
                    // column [NOT] IN (v1, v2, v3, ...)
                    (Expr::Column(c), list, negated) if is_all_scalars(&list) => {
                        let mut values = Vec::with_capacity(list.len());
                        for scalar in list {
                            // Safety: checked by `is_all_scalars`
                            let Expr::Literal(scalar, _) = scalar else {
                                unreachable!();
                            };

                            let Ok(value) = Value::try_from(scalar) else {
                                return None;
                            };

                            values.push(value);
                        }

                        let predicate = Predicate::InList(c.name, values);

                        if negated {
                            Some(Predicate::Not(Box::new(predicate)))
                        } else {
                            Some(predicate)
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

/// Perform SQL left LIKE right, return `None` if fail to evaluate.
/// - `s` the target string
/// - `pattern` the pattern just like '%abc'
/// - `case_insensitive` whether to perform case-insensitive like or not.
fn like_utf8(s: &str, pattern: &str, case_insensitive: &bool) -> Option<bool> {
    let array = StringArray::from(vec![s]);
    let patterns = StringArray::new_scalar(pattern);

    let Ok(booleans) = (if *case_insensitive {
        comparison::ilike(&array, &patterns)
    } else {
        comparison::like(&array, &patterns)
    }) else {
        return None;
    };

    // Safety: at least one value in result
    Some(booleans.value(0))
}

fn is_string_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Utf8(Some(_)), _))
}

fn is_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_))
}

/// A list of predicate
pub struct Predicates {
    predicates: Vec<Predicate>,
}

impl Predicates {
    /// Try its best to create predicates from [`ScanRequest`].
    pub fn from_scan_request(request: &Option<ScanRequest>) -> Predicates {
        if let Some(request) = request {
            let mut predicates = Vec::with_capacity(request.filters.len());

            for filter in &request.filters {
                if let Some(predicate) = Predicate::from_expr(filter.clone()) {
                    predicates.push(predicate);
                }
            }

            Self { predicates }
        } else {
            Self {
                predicates: Vec::new(),
            }
        }
    }

    /// Evaluate the predicates with the row.
    /// returns true when all the predicates are satisfied or can't be evaluated.
    pub fn eval(&self, row: &[(&str, &Value)]) -> bool {
        // fast path
        if self.predicates.is_empty() {
            return true;
        }

        self.predicates
            .iter()
            .filter_map(|p| p.eval(row))
            .all(|b| b)
    }
}

/// Returns true when the values are all [`DfExpr::Literal`].
fn is_all_scalars(list: &[Expr]) -> bool {
    list.iter().all(|v| matches!(v, Expr::Literal(_, _)))
}

#[cfg(test)]
mod tests {
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{BinaryExpr, Literal};

    use super::*;

    #[test]
    fn test_predicate_eval() {
        let a_col = "a".to_string();
        let b_col = "b".to_string();
        let a_value = Value::from("a_value");
        let b_value = Value::from("b_value");
        let wrong_value = Value::from("wrong_value");

        let a_row = [(a_col.as_str(), &a_value)];
        let b_row = [("b", &wrong_value)];
        let wrong_row = [(a_col.as_str(), &wrong_value)];

        // Predicate::Eq
        let p = Predicate::Eq(a_col.clone(), a_value.clone());
        assert!(p.eval(&a_row).unwrap());
        assert!(p.eval(&b_row).is_none());
        assert!(!p.eval(&wrong_row).unwrap());

        // Predicate::NotEq
        let p = Predicate::NotEq(a_col.clone(), a_value.clone());
        assert!(!p.eval(&a_row).unwrap());
        assert!(p.eval(&b_row).is_none());
        assert!(p.eval(&wrong_row).unwrap());

        // Predicate::InList
        let p = Predicate::InList(a_col.clone(), vec![a_value.clone(), b_value.clone()]);
        assert!(p.eval(&a_row).unwrap());
        assert!(p.eval(&b_row).is_none());
        assert!(!p.eval(&wrong_row).unwrap());
        assert!(p.eval(&[(&a_col, &b_value)]).unwrap());

        let p1 = Predicate::Eq(a_col.clone(), a_value.clone());
        let p2 = Predicate::Eq(b_col.clone(), b_value.clone());
        let row = [(a_col.as_str(), &a_value), (b_col.as_str(), &b_value)];
        let wrong_row = [(a_col.as_str(), &a_value), (b_col.as_str(), &wrong_value)];

        //Predicate::And
        let p = Predicate::And(Box::new(p1.clone()), Box::new(p2.clone()));
        assert!(p.eval(&row).unwrap());
        assert!(!p.eval(&wrong_row).unwrap());
        assert!(p.eval(&[]).is_none());
        assert!(p.eval(&[("c", &a_value)]).is_none());
        assert!(!p
            .eval(&[(a_col.as_str(), &b_value), (b_col.as_str(), &a_value)])
            .unwrap());
        assert!(!p
            .eval(&[(a_col.as_str(), &b_value), (b_col.as_str(), &b_value)])
            .unwrap());
        assert!(p
            .eval(&[(a_col.as_ref(), &a_value), ("c", &a_value)])
            .is_none());
        assert!(!p
            .eval(&[(a_col.as_ref(), &b_value), ("c", &a_value)])
            .unwrap());

        //Predicate::Or
        let p = Predicate::Or(Box::new(p1), Box::new(p2));
        assert!(p.eval(&row).unwrap());
        assert!(p.eval(&wrong_row).unwrap());
        assert!(p.eval(&[]).is_none());
        assert!(p.eval(&[("c", &a_value)]).is_none());
        assert!(!p
            .eval(&[(a_col.as_str(), &b_value), (b_col.as_str(), &a_value)])
            .unwrap());
        assert!(p
            .eval(&[(a_col.as_str(), &b_value), (b_col.as_str(), &b_value)])
            .unwrap());
        assert!(p
            .eval(&[(a_col.as_ref(), &a_value), ("c", &a_value)])
            .unwrap());
        assert!(p
            .eval(&[(a_col.as_ref(), &b_value), ("c", &a_value)])
            .is_none());
    }

    #[test]
    fn test_predicate_like() {
        // case insensitive
        let expr = Expr::Like(Like {
            negated: false,
            expr: Box::new(column("a")),
            pattern: Box::new("%abc".lit()),
            case_insensitive: true,
            escape_char: None,
        });

        let p = Predicate::from_expr(expr).unwrap();
        assert!(
            matches!(&p, Predicate::Like(c, pattern, case_insensitive) if
                         c == "a"
                         && pattern == "%abc"
                         && *case_insensitive)
        );

        let match_row = [
            ("a", &Value::from("hello AbC")),
            ("b", &Value::from("b value")),
        ];
        let unmatch_row = [("a", &Value::from("bca")), ("b", &Value::from("b value"))];

        assert!(p.eval(&match_row).unwrap());
        assert!(!p.eval(&unmatch_row).unwrap());
        assert!(p.eval(&[]).is_none());

        // case sensitive
        let expr = Expr::Like(Like {
            negated: false,
            expr: Box::new(column("a")),
            pattern: Box::new("%abc".lit()),
            case_insensitive: false,
            escape_char: None,
        });

        let p = Predicate::from_expr(expr).unwrap();
        assert!(
            matches!(&p, Predicate::Like(c, pattern, case_insensitive) if
                         c == "a"
                         && pattern == "%abc"
                         && !*case_insensitive)
        );
        assert!(!p.eval(&match_row).unwrap());
        assert!(!p.eval(&unmatch_row).unwrap());
        assert!(p.eval(&[]).is_none());

        // not like
        let expr = Expr::Like(Like {
            negated: true,
            expr: Box::new(column("a")),
            pattern: Box::new("%abc".lit()),
            case_insensitive: true,
            escape_char: None,
        });

        let p = Predicate::from_expr(expr).unwrap();
        assert!(!p.eval(&match_row).unwrap());
        assert!(p.eval(&unmatch_row).unwrap());
        assert!(p.eval(&[]).is_none());
    }

    fn column(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    fn match_string_value(v: &Value, expected: &str) -> bool {
        matches!(v, Value::String(bs) if bs.as_utf8() == expected)
    }

    fn match_string_values(vs: &[Value], expected: &[&str]) -> bool {
        assert_eq!(vs.len(), expected.len());

        let mut result = true;
        for (i, v) in vs.iter().enumerate() {
            result = result && match_string_value(v, expected[i]);
        }

        result
    }

    fn mock_exprs() -> (Expr, Expr) {
        let expr1 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(column("a")),
            op: Operator::Eq,
            right: Box::new("a_value".lit()),
        });

        let expr2 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(column("b")),
            op: Operator::NotEq,
            right: Box::new("b_value".lit()),
        });

        (expr1, expr2)
    }

    #[test]
    fn test_predicate_from_expr() {
        let (expr1, expr2) = mock_exprs();

        let p1 = Predicate::from_expr(expr1.clone()).unwrap();
        assert!(matches!(&p1, Predicate::Eq(column, v) if column == "a"
                         && match_string_value(v, "a_value")));

        let p2 = Predicate::from_expr(expr2.clone()).unwrap();
        assert!(matches!(&p2, Predicate::NotEq(column, v) if column == "b"
                         && match_string_value(v, "b_value")));

        let and_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(expr1.clone()),
            op: Operator::And,
            right: Box::new(expr2.clone()),
        });
        let or_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(expr1.clone()),
            op: Operator::Or,
            right: Box::new(expr2.clone()),
        });
        let not_expr = Expr::Not(Box::new(expr1.clone()));

        let and_p = Predicate::from_expr(and_expr).unwrap();
        assert!(matches!(and_p, Predicate::And(left, right) if *left == p1 && *right == p2));
        let or_p = Predicate::from_expr(or_expr).unwrap();
        assert!(matches!(or_p, Predicate::Or(left, right) if *left == p1 && *right == p2));
        let not_p = Predicate::from_expr(not_expr).unwrap();
        assert!(matches!(not_p, Predicate::Not(p) if *p == p1));

        let inlist_expr = Expr::InList(InList {
            expr: Box::new(column("a")),
            list: vec!["a1".lit(), "a2".lit()],
            negated: false,
        });

        let inlist_p = Predicate::from_expr(inlist_expr).unwrap();
        assert!(matches!(&inlist_p, Predicate::InList(c, values) if c == "a"
                         && match_string_values(values, &["a1", "a2"])));

        let inlist_expr = Expr::InList(InList {
            expr: Box::new(column("a")),
            list: vec!["a1".lit(), "a2".lit()],
            negated: true,
        });
        let inlist_p = Predicate::from_expr(inlist_expr).unwrap();
        assert!(matches!(inlist_p, Predicate::Not(p) if
                         matches!(&*p,
                                  Predicate::InList(c, values) if c == "a"
                                  && match_string_values(values, &["a1", "a2"]))));
    }

    #[test]
    fn test_predicates_from_scan_request() {
        let predicates = Predicates::from_scan_request(&None);
        assert!(predicates.predicates.is_empty());

        let (expr1, expr2) = mock_exprs();

        let request = ScanRequest {
            filters: vec![expr1, expr2],
            ..Default::default()
        };
        let predicates = Predicates::from_scan_request(&Some(request));

        assert_eq!(2, predicates.predicates.len());
        assert!(
            matches!(&predicates.predicates[0], Predicate::Eq(column, v) if column == "a"
                     && match_string_value(v, "a_value"))
        );
        assert!(
            matches!(&predicates.predicates[1], Predicate::NotEq(column, v) if column == "b"
                     && match_string_value(v, "b_value"))
        );
    }

    #[test]
    fn test_predicates_eval_row() {
        let wrong_row = [
            ("a", &Value::from("a_value")),
            ("b", &Value::from("b_value")),
            ("c", &Value::from("c_value")),
        ];
        let row = [
            ("a", &Value::from("a_value")),
            ("b", &Value::from("not_b_value")),
            ("c", &Value::from("c_value")),
        ];
        let c_row = [("c", &Value::from("c_value"))];

        // test empty predicates, always returns true
        let predicates = Predicates::from_scan_request(&None);
        assert!(predicates.eval(&row));
        assert!(predicates.eval(&wrong_row));
        assert!(predicates.eval(&c_row));

        let (expr1, expr2) = mock_exprs();
        let request = ScanRequest {
            filters: vec![expr1, expr2],
            ..Default::default()
        };
        let predicates = Predicates::from_scan_request(&Some(request));
        assert!(predicates.eval(&row));
        assert!(!predicates.eval(&wrong_row));
        assert!(predicates.eval(&c_row));
    }
}
