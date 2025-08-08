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
use datafusion_common::cast::{as_boolean_array, as_null_array, as_string_array};
use datafusion_common::{internal_err, DataFusionError, ScalarValue};
use datatypes::arrow::array::{
    Array, ArrayAccessor, ArrayData, BooleanArray, BooleanBufferBuilder, RecordBatch,
    StringArrayType,
};
use datatypes::arrow::compute::filter_record_batch;
use datatypes::arrow::datatypes::DataType;
use datatypes::arrow::error::ArrowError;
use datatypes::compute::or_kleene;
use datatypes::vectors::VectorRef;
use regex::Regex;
use snafu::ResultExt;

use crate::error::{ArrowComputeSnafu, Result, ToArrowScalarSnafu, UnsupportedOperationSnafu};

/// An inplace expr evaluator for simple filter. Only support
/// - `col` `op` `literal`
/// - `literal` `op` `col`
///
/// And the `op` is one of `=`, `!=`, `>`, `>=`, `<`, `<=`,
/// or regex operators: `~`, `~*`, `!~`, `!~*`.
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
    /// Only used when the operator is `Or`-chain.
    literal_list: Vec<Scalar<ArrayRef>>,
    /// Pre-compiled regex.
    /// Only used when the operator is regex operators.
    /// If the regex is empty, it is also `None`.
    regex: Option<Regex>,
    /// Whether the regex is negative.
    regex_negative: bool,
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

        let Expr::Literal(val, _) = lit.lit() else {
            return None;
        };

        Some(Self {
            column_name,
            literal: val.to_scalar().ok()?,
            op,
            literal_list: vec![],
            regex: None,
            regex_negative: false,
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
                    | Operator::GtEq
                    | Operator::RegexMatch
                    | Operator::RegexIMatch
                    | Operator::RegexNotMatch
                    | Operator::RegexNotIMatch => {}
                    Operator::Or => {
                        let lhs = Self::try_new(&binary.left)?;
                        let rhs = Self::try_new(&binary.right)?;
                        if lhs.column_name != rhs.column_name
                            || !matches!(lhs.op, Operator::Eq | Operator::Or)
                            || !matches!(rhs.op, Operator::Eq | Operator::Or)
                        {
                            return None;
                        }
                        let mut list = vec![];
                        let placeholder_literal = lhs.literal.clone();
                        // above check guarantees the op is either `Eq` or `Or`
                        if matches!(lhs.op, Operator::Or) {
                            list.extend(lhs.literal_list);
                        } else {
                            list.push(lhs.literal);
                        }
                        if matches!(rhs.op, Operator::Or) {
                            list.extend(rhs.literal_list);
                        } else {
                            list.push(rhs.literal);
                        }
                        return Some(Self {
                            column_name: lhs.column_name,
                            literal: placeholder_literal,
                            op: Operator::Or,
                            literal_list: list,
                            regex: None,
                            regex_negative: false,
                        });
                    }
                    _ => return None,
                }

                // swap the expr if it is in the form of `literal` `op` `col`
                let mut op = binary.op;
                let (lhs, rhs) = match (&*binary.left, &*binary.right) {
                    (Expr::Column(col), Expr::Literal(lit, _)) => (col, lit),
                    (Expr::Literal(lit, _), Expr::Column(col)) => {
                        // safety: The previous check ensures the operator is able to swap.
                        op = op.swap().unwrap();
                        (col, lit)
                    }
                    _ => return None,
                };

                let (regex, regex_negative) = Self::maybe_build_regex(op, rhs).ok()?;
                let literal = rhs.to_scalar().ok()?;
                Some(Self {
                    column_name: lhs.name.clone(),
                    literal,
                    op,
                    literal_list: vec![],
                    regex,
                    regex_negative,
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
        let result = self.evaluate_datum(&input, 1)?;
        Ok(result.value(0))
    }

    pub fn evaluate_array(&self, input: &ArrayRef) -> Result<BooleanBuffer> {
        self.evaluate_datum(input, input.len())
    }

    pub fn evaluate_vector(&self, input: &VectorRef) -> Result<BooleanBuffer> {
        self.evaluate_datum(&input.to_arrow_array(), input.len())
    }

    fn evaluate_datum(&self, input: &impl Datum, input_len: usize) -> Result<BooleanBuffer> {
        let result = match self.op {
            Operator::Eq => cmp::eq(input, &self.literal),
            Operator::NotEq => cmp::neq(input, &self.literal),
            Operator::Lt => cmp::lt(input, &self.literal),
            Operator::LtEq => cmp::lt_eq(input, &self.literal),
            Operator::Gt => cmp::gt(input, &self.literal),
            Operator::GtEq => cmp::gt_eq(input, &self.literal),
            Operator::RegexMatch => self.regex_match(input),
            Operator::RegexIMatch => self.regex_match(input),
            Operator::RegexNotMatch => self.regex_match(input),
            Operator::RegexNotIMatch => self.regex_match(input),
            Operator::Or => {
                // OR operator stands for OR-chained EQs (or INLIST in other words)
                let mut result: BooleanArray = vec![false; input_len].into();
                for literal in &self.literal_list {
                    let rhs = cmp::eq(input, literal).context(ArrowComputeSnafu)?;
                    result = or_kleene(&result, &rhs).context(ArrowComputeSnafu)?;
                }
                Ok(result)
            }
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

    /// Builds a regex pattern from a scalar value and operator.
    /// Returns the `(regex, negative)` and if successful.
    ///
    /// Returns `Err` if
    /// - the value is not a string
    /// - the regex pattern is invalid
    ///
    /// The regex is `None` if
    /// - the operator is not a regex operator
    /// - the pattern is empty
    fn maybe_build_regex(
        operator: Operator,
        value: &ScalarValue,
    ) -> Result<(Option<Regex>, bool), ArrowError> {
        let (ignore_case, negative) = match operator {
            Operator::RegexMatch => (false, false),
            Operator::RegexIMatch => (true, false),
            Operator::RegexNotMatch => (false, true),
            Operator::RegexNotIMatch => (true, true),
            _ => return Ok((None, false)),
        };
        let flag = if ignore_case { Some("i") } else { None };
        let regex = value
            .try_as_str()
            .ok_or_else(|| ArrowError::CastError(format!("Cannot cast {:?} to str", value)))?
            .ok_or_else(|| ArrowError::CastError("Regex should not be null".to_string()))?;
        let pattern = match flag {
            Some(flag) => format!("(?{flag}){regex}"),
            None => regex.to_string(),
        };
        if pattern.is_empty() {
            Ok((None, negative))
        } else {
            Regex::new(pattern.as_str())
                .map_err(|e| {
                    ArrowError::ComputeError(format!("Regular expression did not compile: {e:?}"))
                })
                .map(|regex| (Some(regex), negative))
        }
    }

    fn regex_match(&self, input: &impl Datum) -> std::result::Result<BooleanArray, ArrowError> {
        let array = input.get().0;
        let string_array = as_string_array(array).map_err(|_| {
            ArrowError::CastError(format!("Cannot cast {:?} to StringArray", array))
        })?;
        let mut result = regexp_is_match_scalar(string_array, self.regex.as_ref())?;
        if self.regex_negative {
            result = datatypes::compute::not(&result)?;
        }
        Ok(result)
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

/// The same as arrow [regexp_is_match_scalar()](datatypes::compute::kernels::regexp::regexp_is_match_scalar())
/// with pre-compiled regex.
/// See <https://github.com/apache/arrow-rs/blob/54.2.0/arrow-string/src/regexp.rs#L204-L246> for the implementation details.
pub fn regexp_is_match_scalar<'a, S>(
    array: &'a S,
    regex: Option<&Regex>,
) -> Result<BooleanArray, ArrowError>
where
    &'a S: StringArrayType<'a>,
{
    let null_bit_buffer = array.nulls().map(|x| x.inner().sliced());
    let mut result = BooleanBufferBuilder::new(array.len());

    if let Some(re) = regex {
        for i in 0..array.len() {
            let value = array.value(i);
            result.append(re.is_match(value));
        }
    } else {
        result.append_n(array.len(), true);
    }

    let buffer = result.into();
    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            array.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer],
            vec![],
        )
    };

    Ok(BooleanArray::from(data))
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
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Plus,
            right: Box::new(1.lit()),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

        // two literal is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(1.lit()),
            op: Operator::Eq,
            right: Box::new(1.lit()),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

        // two column is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Eq,
            right: Box::new(Expr::Column(Column::from_name("bar"))),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

        // compound expr is not supported
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("foo"))),
                op: Operator::Eq,
                right: Box::new(1.lit()),
            })),
            op: Operator::Eq,
            right: Box::new(1.lit()),
        });
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());
    }

    #[test]
    fn supported_filter_op() {
        // equal
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Eq,
            right: Box::new(1.lit()),
        });
        let _ = SimpleFilterEvaluator::try_new(&expr).unwrap();

        // swap operands
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(1.lit()),
            op: Operator::Lt,
            right: Box::new(Expr::Column(Column::from_name("foo"))),
        });
        let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();
        assert_eq!(evaluator.op, Operator::Gt);
        assert_eq!(evaluator.column_name, "foo".to_string());
    }

    #[test]
    fn run_on_array() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Eq,
            right: Box::new(1i64.lit()),
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
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Lt,
            right: Box::new(1i64.lit()),
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

    #[test]
    fn test_complex_filter_expression() {
        // Create an expression tree for: col = 'B' OR col = 'C' OR col = 'D'
        let col_eq_b = col("col").eq(lit("B"));
        let col_eq_c = col("col").eq(lit("C"));
        let col_eq_d = col("col").eq(lit("D"));

        // Build the OR chain
        let col_or_expr = col_eq_b.or(col_eq_c).or(col_eq_d);

        // Check that SimpleFilterEvaluator can handle OR chain
        let or_evaluator = SimpleFilterEvaluator::try_new(&col_or_expr).unwrap();
        assert_eq!(or_evaluator.column_name, "col");
        assert_eq!(or_evaluator.op, Operator::Or);
        assert_eq!(or_evaluator.literal_list.len(), 3);
        assert_eq!(format!("{:?}", or_evaluator.literal_list), "[Scalar(StringArray\n[\n  \"B\",\n]), Scalar(StringArray\n[\n  \"C\",\n]), Scalar(StringArray\n[\n  \"D\",\n])]");

        // Create a schema and batch for testing
        let schema = Schema::new(vec![Field::new("col", DataType::Utf8, false)]);
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();
        let props = ExecutionProps::new();
        let physical_expr = create_physical_expr(&col_or_expr, &df_schema, &props).unwrap();

        // Create test data
        let col_data = Arc::new(datatypes::arrow::array::StringArray::from(vec![
            "B", "C", "E", "B", "C", "D", "F",
        ]));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![col_data]).unwrap();
        let expected = datatypes::arrow::array::StringArray::from(vec!["B", "C", "B", "C", "D"]);

        // Filter the batch
        let filtered_batch = batch_filter(&batch, &physical_expr).unwrap();

        // Expected: rows with col in ("B", "C", "D")
        // That would be rows 0, 1, 3, 4, 5
        assert_eq!(filtered_batch.num_rows(), 5);

        let col_filtered = filtered_batch
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col_filtered, &expected);
    }

    #[test]
    fn test_maybe_build_regex() {
        // Test case for RegexMatch (case sensitive, non-negative)
        let (regex, negative) = SimpleFilterEvaluator::maybe_build_regex(
            Operator::RegexMatch,
            &ScalarValue::Utf8(Some("a.*b".to_string())),
        )
        .unwrap();
        assert!(regex.is_some());
        assert!(!negative);
        assert!(regex.unwrap().is_match("axxb"));

        // Test case for RegexIMatch (case insensitive, non-negative)
        let (regex, negative) = SimpleFilterEvaluator::maybe_build_regex(
            Operator::RegexIMatch,
            &ScalarValue::Utf8(Some("a.*b".to_string())),
        )
        .unwrap();
        assert!(regex.is_some());
        assert!(!negative);
        assert!(regex.unwrap().is_match("AxxB"));

        // Test case for RegexNotMatch (case sensitive, negative)
        let (regex, negative) = SimpleFilterEvaluator::maybe_build_regex(
            Operator::RegexNotMatch,
            &ScalarValue::Utf8(Some("a.*b".to_string())),
        )
        .unwrap();
        assert!(regex.is_some());
        assert!(negative);

        // Test case for RegexNotIMatch (case insensitive, negative)
        let (regex, negative) = SimpleFilterEvaluator::maybe_build_regex(
            Operator::RegexNotIMatch,
            &ScalarValue::Utf8(Some("a.*b".to_string())),
        )
        .unwrap();
        assert!(regex.is_some());
        assert!(negative);

        // Test with empty regex pattern
        let (regex, negative) = SimpleFilterEvaluator::maybe_build_regex(
            Operator::RegexMatch,
            &ScalarValue::Utf8(Some("".to_string())),
        )
        .unwrap();
        assert!(regex.is_none());
        assert!(!negative);

        // Test with non-regex operator
        let (regex, negative) = SimpleFilterEvaluator::maybe_build_regex(
            Operator::Eq,
            &ScalarValue::Utf8(Some("a.*b".to_string())),
        )
        .unwrap();
        assert!(regex.is_none());
        assert!(!negative);

        // Test with invalid regex pattern
        let result = SimpleFilterEvaluator::maybe_build_regex(
            Operator::RegexMatch,
            &ScalarValue::Utf8(Some("a(b".to_string())),
        );
        assert!(result.is_err());

        // Test with non-string value
        let result = SimpleFilterEvaluator::maybe_build_regex(
            Operator::RegexMatch,
            &ScalarValue::Int64(Some(123)),
        );
        assert!(result.is_err());

        // Test with null value
        let result = SimpleFilterEvaluator::maybe_build_regex(
            Operator::RegexMatch,
            &ScalarValue::Utf8(None),
        );
        assert!(result.is_err());
    }
}
