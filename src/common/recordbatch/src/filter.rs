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

use std::sync::Arc;

use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{Expr, Literal, Operator};
use datafusion::physical_plan::PhysicalExpr;
use datafusion_common::arrow::array::{ArrayRef, Datum, Scalar};
use datafusion_common::arrow::buffer::BooleanBuffer;
use datafusion_common::arrow::compute::kernels::cmp;
use datafusion_common::cast::{as_boolean_array, as_null_array};
use datafusion_common::{internal_err, DataFusionError, ScalarValue};
use datatypes::arrow::array::{Array, BooleanArray, RecordBatch};
use datatypes::arrow::compute::filter_record_batch;
use datatypes::vectors::VectorRef;
use snafu::ResultExt;

use crate::error::{ArrowComputeSnafu, Result, ToArrowScalarSnafu, UnsupportedOperationSnafu};

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
    pub fn new<T: Literal>(column_name: String, lit: T, op: Operator) -> Option<Self> {
        match op {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {}
            _ => return None,
        }

        let Expr::Literal(val) = lit.lit() else {
            return None;
        };

        Some(Self {
            column_name,
            literal: val.to_scalar().ok()?,
            op,
        })
    }

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

                let literal = rhs.to_scalar().ok()?;
                Some(Self {
                    column_name: lhs.name.clone(),
                    literal,
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
        let input = input
            .to_scalar()
            .with_context(|_| ToArrowScalarSnafu { v: input.clone() })?;
        let result = self.evaluate_datum(&input)?;
        Ok(result.value(0))
    }

    pub fn evaluate_array(&self, input: &ArrayRef) -> Result<BooleanBuffer> {
        self.evaluate_datum(input)
    }

    pub fn evaluate_vector(&self, input: &VectorRef) -> Result<BooleanBuffer> {
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

/// Evaluate the predicate on the input [RecordBatch], and return a new [RecordBatch].
/// Copy from datafusion::physical_plan::src::filter.rs
pub fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> DfResult<RecordBatch> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            let filter_array = match as_boolean_array(&array) {
                Ok(boolean_array) => Ok(boolean_array.clone()),
                Err(_) => {
                    let Ok(null_array) = as_null_array(&array) else {
                        return internal_err!(
                            "Cannot create filter_array from non-boolean predicates"
                        );
                    };

                    // if the predicate is null, then the result is also null
                    Ok::<BooleanArray, DataFusionError>(BooleanArray::new_null(null_array.len()))
                }
            }?;
            Ok(filter_record_batch(batch, &filter_array)?)
        })
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use datafusion::execution::context::ExecutionProps;
    use datafusion::logical_expr::{col, lit, BinaryExpr};
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_common::{Column, DFSchema};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};

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
        assert!(!result);

        let input_2 = ScalarValue::Int64(Some(0));
        let result = evaluator.evaluate_scalar(&input_2).unwrap();
        assert!(result);

        let input_3 = ScalarValue::Int64(None);
        let result = evaluator.evaluate_scalar(&input_3).unwrap();
        assert!(!result);
    }

    #[test]
    fn batch_filter_test() {
        let expr = col("ts").gt(lit(123456u64));
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("ts", DataType::UInt64, false),
        ]);
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();
        let props = ExecutionProps::new();
        let physical_expr = create_physical_expr(&expr, &df_schema, &props).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(datatypes::arrow::array::Int32Array::from(vec![4, 5, 6])),
                Arc::new(datatypes::arrow::array::UInt64Array::from(vec![
                    123456, 123457, 123458,
                ])),
            ],
        )
        .unwrap();
        let new_batch = batch_filter(&batch, &physical_expr).unwrap();
        assert_eq!(new_batch.num_rows(), 2);
        let first_column_values = new_batch
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Int32Array>()
            .unwrap();
        let expected = datatypes::arrow::array::Int32Array::from(vec![5, 6]);
        assert_eq!(first_column_values, &expected);
    }
}
