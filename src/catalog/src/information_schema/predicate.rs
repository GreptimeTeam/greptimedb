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
    /// Evaluate the predicate with value, returns:
    /// - `None` when the predicate can't run on value,
    /// - `Some(true)`  when the predicate is satisfied,.
    /// - `Some(false)` when the predicate is not satisfied.
    fn eval(&self, column: &str, value: &Value) -> Option<bool> {
        match self {
            Predicate::Eq(c, v) => {
                if c != column {
                    return None;
                }
                Some(v == value)
            }
            Predicate::NotEq(c, v) => {
                if c != column {
                    return None;
                }
                Some(v != value)
            }
            Predicate::InList(c, values) => {
                if c != column {
                    return None;
                }
                Some(values.iter().any(|v| v == value))
            }
            Predicate::And(left, right) => {
                let Some(left) = left.eval(column, value) else {
                    return None;
                };
                let Some(right) = right.eval(column, value) else {
                    return None;
                };

                Some(left && right)
            }
            Predicate::Or(left, right) => {
                let Some(left) = left.eval(column, value) else {
                    return None;
                };
                let Some(right) = right.eval(column, value) else {
                    return None;
                };

                Some(left || right)
            }
            Predicate::Not(p) => {
                let Some(p) = p.eval(column, value) else {
                    return None;
                };

                Some(!p)
            }
        }
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

    /// Evaluate the predicates with the column value,
    /// returns true when all the predicates are satisfied or can't be evaluated.
    fn eval_column(&self, column: &str, value: &Value) -> bool {
        let mut result = true;
        for predicate in &self.predicates {
            match predicate.eval(column, value) {
                Some(b) => {
                    result = result && b;
                }
                None => {
                    // Can't eval this predicate, continue
                    continue;
                }
            }

            if !result {
                break;
            }
        }

        result
    }

    /// Evaluate the predicates with the columns and values,
    /// returns true when all the predicates are satisfied or can't be evaluated.
    pub fn eval(&self, row: &[(&str, &Value)]) -> bool {
        // fast path
        if self.predicates.is_empty() {
            return true;
        }

        let mut result = true;
        for (column, value) in row {
            result = result && self.eval_column(column, value);

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
