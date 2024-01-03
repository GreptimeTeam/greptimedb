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

use common_query::logical_plan::DfExpr;
use datafusion::logical_expr::Operator;
use datatypes::value::Value;
use store_api::storage::ScanRequest;

type ColumnName = String;
/// Predicate to filter information_schema tables stream,
/// we only support these simple predicates currently.
/// TODO(dennis): supports more predicate types.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Predicate {
    Eq(ColumnName, Value),
    NotEq(ColumnName, Value),
    InList(ColumnName, Vec<Value>),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    /// Evaluate the predicate with the row, returns:
    /// - None when the predicate can't evaluate with the row.
    /// - Some(true) when the predicate is satisfied,
    /// - Some(false) when the predicate is satisfied,
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
                return match (left.eval(row), right.eval(row)) {
                    (Some(left), Some(right)) => Some(left && right),
                    (Some(false), None) => Some(false),
                    (None, Some(false)) => Some(false),
                    _ => None,
                };
            }
            Predicate::Or(left, right) => {
                return match (left.eval(row), right.eval(row)) {
                    (Some(left), Some(right)) => Some(left || right),
                    (Some(true), None) => Some(true),
                    (None, Some(true)) => Some(true),
                    _ => None,
                };
            }
            Predicate::Not(p) => {
                let Some(b) = p.eval(row) else {
                    return None;
                };

                return Some(!b);
            }
        }

        // Can't evaluate predicate with the row
        None
    }

    // Try to create a predicate from datafusion `Expr`, return None if fails.
    fn from_expr(expr: DfExpr) -> Option<Predicate> {
        match expr {
            // NOT expr
            DfExpr::Not(expr) => {
                let Some(p) = Self::from_expr(*expr) else {
                    return None;
                };

                Some(Predicate::Not(Box::new(p)))
            }
            // left OP right
            DfExpr::BinaryExpr(bin) => match (*bin.left, bin.op, *bin.right) {
                // left == right
                (DfExpr::Literal(scalar), Operator::Eq, DfExpr::Column(c))
                | (DfExpr::Column(c), Operator::Eq, DfExpr::Literal(scalar)) => {
                    let Ok(v) = Value::try_from(scalar) else {
                        return None;
                    };

                    Some(Predicate::Eq(c.name, v))
                }
                // left != right
                (DfExpr::Literal(scalar), Operator::NotEq, DfExpr::Column(c))
                | (DfExpr::Column(c), Operator::NotEq, DfExpr::Literal(scalar)) => {
                    let Ok(v) = Value::try_from(scalar) else {
                        return None;
                    };

                    Some(Predicate::NotEq(c.name, v))
                }
                // left AND right
                (left, Operator::And, right) => {
                    let Some(left) = Self::from_expr(left) else {
                        return None;
                    };

                    let Some(right) = Self::from_expr(right) else {
                        return None;
                    };

                    Some(Predicate::And(Box::new(left), Box::new(right)))
                }
                // left OR right
                (left, Operator::Or, right) => {
                    let Some(left) = Self::from_expr(left) else {
                        return None;
                    };

                    let Some(right) = Self::from_expr(right) else {
                        return None;
                    };

                    Some(Predicate::Or(Box::new(left), Box::new(right)))
                }
                _ => None,
            },
            // [NOT] IN (LIST)
            DfExpr::InList(list) => {
                match (*list.expr, list.list, list.negated) {
                    // column [NOT] IN (v1, v2, v3, ...)
                    (DfExpr::Column(c), list, negated) if is_all_scalars(&list) => {
                        let mut values = Vec::with_capacity(list.len());
                        for scalar in list {
                            // Safety: checked by `is_all_scalars`
                            let DfExpr::Literal(scalar) = scalar else {
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

/// A list of predicate
pub struct Predicates {
    predicates: Vec<Predicate>,
}

impl Predicates {
    /// Try its best to create predicates from `ScanRequest`.
    pub fn from_scan_request(request: &Option<ScanRequest>) -> Predicates {
        if let Some(request) = request {
            let mut predicates = Vec::with_capacity(request.filters.len());

            for filter in &request.filters {
                if let Some(predicate) = Predicate::from_expr(filter.df_expr().clone()) {
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

        let mut result = true;

        for predicate in &self.predicates {
            match predicate.eval(row) {
                Some(b) => {
                    result = result && b;
                }
                // The predicate can't evaluate with the row, continue
                None => continue,
            }

            if !result {
                break;
            }
        }

        result
    }
}

/// Returns true when the values are all `ScalarValue`.
fn is_all_scalars(list: &[DfExpr]) -> bool {
    list.iter().all(|v| matches!(v, DfExpr::Literal(_)))
}

#[cfg(test)]
mod tests {
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
    fn test_predicate_from_expr() {
        todo!()
    }

    #[test]
    fn test_predicates_from_scan_request() {
        todo!()
    }

    #[test]
    fn test_predicates_eval_row() {
        todo!()
    }
}
