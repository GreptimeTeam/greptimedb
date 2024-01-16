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

//! Util record batch stream wrapper that can perform precise filter.

use datafusion::logical_expr::{Expr, Operator};
use datafusion_common::arrow::array::{ArrayRef, Datum, Scalar};
use datafusion_common::arrow::buffer::BooleanBuffer;
use datafusion_common::arrow::compute::kernels::cmp;
use datafusion_common::ScalarValue;
use datatypes::vectors::VectorRef;
use snafu::ResultExt;

use crate::error::{ArrowComputeSnafu, Result, UnsupportedOperationSnafu};

/// An inplace expr evaluator for simple filter. Only support
/// - `col` `op` `literal`
/// - `literal` `op` `col`
///
/// And the `op` is one of `=`, `!=`, `>`, `>=`, `<`, `<=`.
///
/// This struct contains normalized predicate expr. In the form of
/// `col` `op` `literal` where the `col` is provided from input.
#[derive(Debug)]
pub struct SimpleFilterEvaluator {
    /// Name of the referenced column.
    column_name: String,
    /// The literal value.
    literal: Scalar<ArrayRef>,
    /// The operator.
    op: Operator,
}

impl SimpleFilterEvaluator {
    pub fn try_new(predicate: &Expr) -> Option<Self> {
        match predicate {
            Expr::BinaryExpr(binary) => {
                // check if the expr is in the supported form
                match binary.op {
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq => {}
                    _ => return None,
                }

                // swap the expr if it is in the form of `literal` `op` `col`
                let mut op = binary.op;
                let (lhs, rhs) = match (&*binary.left, &*binary.right) {
                    (Expr::Column(ref col), Expr::Literal(ref lit)) => (col, lit),
                    (Expr::Literal(ref lit), Expr::Column(ref col)) => {
                        // safety: The previous check ensures the operator is able to swap.
                        op = op.swap().unwrap();
                        (col, lit)
                    }
                    _ => return None,
                };

                Some(Self {
                    column_name: lhs.name.clone(),
                    literal: rhs.clone().to_scalar(),
                    op,
                })
            }
            _ => None,
        }
    }

    /// Get the name of the referenced column.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn evaluate_scalar(&self, input: &ScalarValue) -> Result<bool> {
        let result = self.evaluate_datum(&input.to_scalar())?;
        Ok(result.value(0))
    }

    pub fn evaluate_array(&self, input: &ArrayRef) -> Result<BooleanBuffer> {
        self.evaluate_datum(input)
    }

    pub fn evaluate_vector(&self, input: VectorRef) -> Result<BooleanBuffer> {
        self.evaluate_datum(&input.to_arrow_array())
    }

    fn evaluate_datum(&self, input: &impl Datum) -> Result<BooleanBuffer> {
        let result = match self.op {
            Operator::Eq => cmp::eq(input, &self.literal),
            Operator::NotEq => cmp::neq(input, &self.literal),
            Operator::Lt => cmp::lt(input, &self.literal),
            Operator::LtEq => cmp::lt_eq(input, &self.literal),
            Operator::Gt => cmp::gt(input, &self.literal),
            Operator::GtEq => cmp::gt_eq(input, &self.literal),
            _ => {
                return UnsupportedOperationSnafu {
                    reason: format!("{:?}", self.op),
                }
                .fail()
            }
        };
        result
            .context(ArrowComputeSnafu)
            .map(|array| array.values().clone())
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use datafusion::logical_expr::BinaryExpr;
    use datafusion_common::Column;

    use super::*;

    #[test]
    fn unsupported_filter_op() {
        // `+` is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "foo".to_string(),
            })),
            op: Operator::Plus,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

        // two literal is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

        // two column is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "foo".to_string(),
            })),
            op: Operator::Eq,
            right: Box::new(Expr::Column(Column {
                relation: None,
                name: "bar".to_string(),
            })),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

        // compound expr is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "foo".to_string(),
                })),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
            })),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());
    }

    #[test]
    fn supported_filter_op() {
        // equal
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "foo".to_string(),
            })),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
        });
        let _ = SimpleFilterEvaluator::try_new(&expr).unwrap();

        // swap operands
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
            op: Operator::Lt,
            right: Box::new(Expr::Column(Column {
                relation: None,
                name: "foo".to_string(),
            })),
        });
        let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();
        assert_eq!(evaluator.op, Operator::Gt);
        assert_eq!(evaluator.column_name, "foo".to_string());
    }

    #[test]
    fn run_on_array() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "foo".to_string(),
            })),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
        });
        let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();

        let input_1 = Arc::new(datatypes::arrow::array::Int64Array::from(vec![1, 2, 3])) as _;
        let result = evaluator.evaluate_array(&input_1).unwrap();
        assert_eq!(result, BooleanBuffer::from(vec![true, false, false]));

        let input_2 = Arc::new(datatypes::arrow::array::Int64Array::from(vec![1, 1, 1])) as _;
        let result = evaluator.evaluate_array(&input_2).unwrap();
        assert_eq!(result, BooleanBuffer::from(vec![true, true, true]));

        let input_3 = Arc::new(datatypes::arrow::array::Int64Array::new_null(0)) as _;
        let result = evaluator.evaluate_array(&input_3).unwrap();
        assert_eq!(result, BooleanBuffer::from(vec![]));
    }

    #[test]
    fn run_on_scalar() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "foo".to_string(),
            })),
            op: Operator::Lt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
        });
        let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();

        let input_1 = ScalarValue::Int64(Some(1));
        let result = evaluator.evaluate_scalar(&input_1).unwrap();
        assert_eq!(result, false);

        let input_2 = ScalarValue::Int64(Some(0));
        let result = evaluator.evaluate_scalar(&input_2).unwrap();
        assert_eq!(result, true);

        let input_3 = ScalarValue::Int64(None);
        let result = evaluator.evaluate_scalar(&input_3).unwrap();
        assert_eq!(result, false);
    }
}
