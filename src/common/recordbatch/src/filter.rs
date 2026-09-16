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

use common_time::timestamp::div_mod_units;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{Expr, Literal, Operator};
use datafusion::physical_plan::PhysicalExpr;
use datafusion_common::arrow::array::{ArrayRef, Datum, Scalar};
use datafusion_common::arrow::buffer::BooleanBuffer;
use datafusion_common::arrow::compute::kernels::cmp;
use datafusion_common::cast::{as_boolean_array, as_null_array, as_string_array};
use datafusion_common::{DataFusionError, ScalarValue, internal_err};
use datatypes::arrow::array::{
    Array, ArrayAccessor, ArrayData, BooleanArray, BooleanBufferBuilder, DictionaryArray,
    RecordBatch, StringArrayType,
};
use datatypes::arrow::compute::filter_record_batch;
use datatypes::arrow::datatypes::{DataType, TimeUnit, UInt32Type};
use datatypes::arrow::error::ArrowError;
use datatypes::compute::or_kleene;
use datatypes::data_type::{ConcreteDataType, DataType as _};
use datatypes::value::Value;
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
#[derive(Debug, Clone)]
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

    pub fn is_eq(&self) -> bool {
        matches!(self.op, Operator::Eq)
    }

    pub fn is_not_eq(&self) -> bool {
        matches!(self.op, Operator::NotEq)
    }

    pub fn is_lt(&self) -> bool {
        matches!(self.op, Operator::Lt)
    }

    pub fn is_lt_eq(&self) -> bool {
        matches!(self.op, Operator::LtEq)
    }

    pub fn is_gt(&self) -> bool {
        matches!(self.op, Operator::Gt)
    }

    pub fn is_gt_eq(&self) -> bool {
        matches!(self.op, Operator::GtEq)
    }

    /// Returns true if this filter represents an `OR` chain of equality comparisons, e.g.
    /// `col = lit1 OR col = lit2 ...`.
    pub fn is_or_eq_chain(&self) -> bool {
        matches!(self.op, Operator::Or)
    }

    /// Returns the literal as a [`Value`]. It returns `None` if the literal can't be converted.
    pub fn literal_value(&self) -> Option<Value> {
        let array = self.literal.get().0;
        let scalar = ScalarValue::try_from_array(array, 0).ok()?;
        Value::try_from(scalar).ok()
    }

    /// Returns the literal list as a list of [`Value`]s. It returns `None` if any literal can't be
    /// converted.
    pub fn literal_list_values(&self) -> Option<Vec<Value>> {
        self.literal_list
            .iter()
            .map(|scalar| {
                let array = scalar.get().0;
                let scalar = ScalarValue::try_from_array(array, 0).ok()?;
                Value::try_from(scalar).ok()
            })
            .collect()
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
                .fail();
            }
        };
        result
            .context(ArrowComputeSnafu)
            .map(|array| boolean_array_to_scan_mask(&array).values().clone())
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

        // Try to cast to StringArray first
        if let Ok(string_array) = as_string_array(array) {
            let mut result = regexp_is_match_scalar(string_array, self.regex.as_ref())?;
            if self.regex_negative {
                result = datatypes::compute::not(&result)?;
            }
            return Ok(result);
        }

        // Try to cast to StringDictionaryArray
        if let Some(dict_array) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
            let mut result = regexp_is_match_dictionary(dict_array, self.regex.as_ref())?;
            if self.regex_negative {
                result = datatypes::compute::not(&result)?;
            }
            return Ok(result);
        }

        Err(ArrowError::CastError(format!(
            "Cannot cast {:?} to StringArray or StringDictionaryArray",
            array.data_type()
        )))
    }

    /// Casts the filter's timestamp literal into the unit of the `target`
    /// timestamp type, so it can be evaluated against a column stored in that
    /// unit (e.g. an old-unit SST after the time index unit was widened).
    ///
    /// When the literal is not representable in `target`'s unit (e.g.
    /// `ts = 7_000_500us` against a millisecond column), the outcome keeps the
    /// row set instead of rounding the literal: `=` prunes / `!=` matches, and
    /// inequalities strengthen the operator (e.g. `>= 2_500_500us` becomes
    /// `> 2500ms`) so the excluded boundary row stays excluded.
    ///
    /// Returns `None` when the filter doesn't compare against a tz-naive
    /// timestamp literal, or `target` is not a timestamp type.
    pub fn cast_timestamp_unit(&self, target: &ConcreteDataType) -> Option<TimestampUnitCast> {
        let target_unit = match target.as_arrow_type() {
            DataType::Timestamp(unit, _) => unit,
            _ => return None,
        };

        // An `OR` chain of equalities (an IN list): convert each literal
        // with `=` semantics; literals not representable in the target unit
        // cannot match any row and simply drop out of the chain.
        if self.op == Operator::Or {
            let mut literal_list = Vec::with_capacity(self.literal_list.len());
            for literal in &self.literal_list {
                let scalar = ScalarValue::try_from_array(literal.get().0, 0).ok()?;
                let Some((value, unit)) = timestamp_scalar_parts(&scalar) else {
                    continue;
                };
                let Some(value) = value else {
                    continue;
                };
                let cast = div_mod_units(value, unit.into(), target_unit.into())?;
                if cast.remainder == 0 {
                    literal_list.push(timestamp_scalar(cast.quotient, target_unit)?);
                }
            }
            if literal_list.is_empty() {
                return Some(TimestampUnitCast::Pruned);
            }
            let literal = literal_list[0].clone();
            return Some(TimestampUnitCast::Filter(Self {
                column_name: self.column_name.clone(),
                literal,
                op: Operator::Or,
                literal_list,
                regex: None,
                regex_negative: false,
            }));
        }

        let scalar = ScalarValue::try_from_array(self.literal.get().0, 0).ok()?;
        let (value, unit) = timestamp_scalar_parts(&scalar)?;
        let Some(value) = value else {
            // A NULL literal never compares true (arrow comparison
            // semantics: null results are filtered out).
            return Some(TimestampUnitCast::Pruned);
        };
        if unit == target_unit {
            return Some(TimestampUnitCast::Filter(self.clone()));
        }
        let cast = div_mod_units(value, unit.into(), target_unit.into())?;
        let divisible = cast.remainder == 0;
        let literal = timestamp_scalar(cast.quotient, target_unit)?;
        let filter = |op: Operator| Self {
            column_name: self.column_name.clone(),
            literal: literal.clone(),
            op,
            literal_list: vec![],
            regex: None,
            regex_negative: false,
        };

        Some(match self.op {
            Operator::Eq if divisible => TimestampUnitCast::Filter(filter(Operator::Eq)),
            Operator::Eq => TimestampUnitCast::Pruned,
            Operator::NotEq if divisible => TimestampUnitCast::Filter(filter(Operator::NotEq)),
            Operator::NotEq => TimestampUnitCast::Matched,
            // v > L holds exactly for v > quotient, whether or not L is
            // representable in the target unit.
            Operator::Gt => TimestampUnitCast::Filter(filter(Operator::Gt)),
            Operator::GtEq if divisible => TimestampUnitCast::Filter(filter(Operator::GtEq)),
            // L strictly between two target values: v >= L is v > quotient.
            Operator::GtEq => TimestampUnitCast::Filter(filter(Operator::Gt)),
            Operator::Lt if divisible => TimestampUnitCast::Filter(filter(Operator::Lt)),
            // L strictly between two target values: v < L is v <= quotient.
            Operator::Lt => TimestampUnitCast::Filter(filter(Operator::LtEq)),
            // v <= L holds exactly for v <= quotient.
            Operator::LtEq => TimestampUnitCast::Filter(filter(Operator::LtEq)),
            // Regex predicates don't apply to timestamps.
            _ => return None,
        })
    }
}

/// The result of casting a [`SimpleFilterEvaluator`] to a different timestamp
/// unit, preserving the predicate's semantics on rows stored in that unit.
/// See [`SimpleFilterEvaluator::cast_timestamp_unit`].
#[derive(Debug, Clone)]
pub enum TimestampUnitCast {
    /// The cast filter; evaluates column values stored in the target unit.
    Filter(SimpleFilterEvaluator),
    /// No value in the target unit satisfies the filter.
    Pruned,
    /// Every value in the target unit satisfies the filter.
    Matched,
}

/// Extracts the value and unit from a tz-naive timestamp scalar.
fn timestamp_scalar_parts(scalar: &ScalarValue) -> Option<(Option<i64>, TimeUnit)> {
    let (value, unit, timezone) = match scalar {
        ScalarValue::TimestampSecond(v, tz) => (*v, TimeUnit::Second, tz),
        ScalarValue::TimestampMillisecond(v, tz) => (*v, TimeUnit::Millisecond, tz),
        ScalarValue::TimestampMicrosecond(v, tz) => (*v, TimeUnit::Microsecond, tz),
        ScalarValue::TimestampNanosecond(v, tz) => (*v, TimeUnit::Nanosecond, tz),
        _ => return None,
    };
    // A timezone-aware literal doesn't compare against a tz-naive column;
    // leave it to the caller instead of guessing the intended semantics.
    (timezone.is_none()).then_some((value, unit))
}

/// Builds a tz-naive timestamp literal for `value` in `unit`.
pub fn timestamp_scalar_value(value: i64, unit: TimeUnit) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), None),
    }
}

/// Builds a one-element scalar array holding `value` in `unit`.
fn timestamp_scalar(value: i64, unit: TimeUnit) -> Option<Scalar<ArrayRef>> {
    timestamp_scalar_value(value, unit).to_scalar().ok()
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
            Ok(filter_record_batch(
                batch,
                &boolean_array_to_scan_mask(&filter_array),
            )?)
        })
}

/// Converts nullable SQL predicate values to a scan mask, where `NULL` is `false`.
fn boolean_array_to_scan_mask(array: &BooleanArray) -> BooleanArray {
    if array.null_count() == 0 {
        return array.clone();
    }

    let mut values = BooleanBufferBuilder::new(array.len());
    for index in 0..array.len() {
        values.append(array.is_valid(index) && array.value(index));
    }
    BooleanArray::new(values.into(), None)
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

/// Similar to [regexp_is_match_scalar] but for StringDictionaryArray.
/// Iterates through dictionary keys to get string values and applies regex matching.
pub fn regexp_is_match_dictionary(
    dict_array: &DictionaryArray<UInt32Type>,
    regex: Option<&Regex>,
) -> Result<BooleanArray, ArrowError> {
    // Get the string values from the dictionary
    let string_values = dict_array
        .values()
        .as_any()
        .downcast_ref::<datatypes::arrow::array::StringArray>()
        .ok_or_else(|| {
            ArrowError::CastError("Dictionary values must be StringArray".to_string())
        })?;

    // Dictionary logical nulls include both null keys and keys whose dictionary value is null.
    let logical_nulls = dict_array.logical_nulls();
    let null_bit_buffer = logical_nulls.as_ref().map(|x| x.inner().sliced());
    let mut result = BooleanBufferBuilder::new(dict_array.len());

    if let Some(re) = regex {
        let keys = dict_array.keys().values();
        for i in 0..dict_array.len() {
            if logical_nulls.as_ref().is_some_and(|nulls| nulls.is_null(i)) {
                result.append(false);
            } else {
                let key = keys[i] as usize;
                let string_value = string_values.value(key);
                result.append(re.is_match(string_value));
            }
        }
    } else {
        result.append_n(dict_array.len(), true);
    }

    let buffer = result.into();
    let data = unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            dict_array.len(),
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
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::logical_expr::{BinaryExpr, col, lit};
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_common::{Column, DFSchema};
    use datatypes::arrow::array::{TimestampMillisecondArray, TimestampNanosecondArray};
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
        let physical_expr = create_physical_expr(
            &expr,
            &df_schema,
            &props,
            &PhysicalPlanningContext::default(),
        )
        .unwrap();
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
        assert_eq!(
            format!("{:?}", or_evaluator.literal_list),
            "[Scalar(StringArray\n[\n  \"B\",\n]), Scalar(StringArray\n[\n  \"C\",\n]), Scalar(StringArray\n[\n  \"D\",\n])]"
        );

        // Create a schema and batch for testing
        let schema = Schema::new(vec![Field::new("col", DataType::Utf8, false)]);
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();
        let props = ExecutionProps::new();
        let physical_expr = create_physical_expr(
            &col_or_expr,
            &df_schema,
            &props,
            &PhysicalPlanningContext::default(),
        )
        .unwrap();

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

    #[test]
    fn test_regex_match_dictionary_array() {
        use datatypes::arrow::array::StringDictionaryBuilder;

        // Create a StringDictionaryArray
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        builder.append("apple").unwrap();
        builder.append("banana").unwrap();
        builder.append("apple").unwrap();
        builder.append("cherry").unwrap();
        let dict_array = builder.finish();

        // Test regex that matches "apple"
        let regex = regex::Regex::new(r"app.*").unwrap();
        let result = regexp_is_match_dictionary(&dict_array, Some(&regex)).unwrap();

        // Should match indices 0 and 2 (both "apple")
        assert_eq!(result.len(), 4);
        assert!(result.value(0)); // "apple"
        assert!(!result.value(1)); // "banana"
        assert!(result.value(2)); // "apple"
        assert!(!result.value(3)); // "cherry"

        // Test regex that matches "banana"
        let regex2 = regex::Regex::new(r"ban.*").unwrap();
        let result2 = regexp_is_match_dictionary(&dict_array, Some(&regex2)).unwrap();

        assert!(!result2.value(0)); // "apple"
        assert!(result2.value(1)); // "banana"
        assert!(!result2.value(2)); // "apple"
        assert!(!result2.value(3)); // "cherry"

        // Test with no regex (should match all)
        let result3 = regexp_is_match_dictionary(&dict_array, None).unwrap();
        assert!(result3.value(0));
        assert!(result3.value(1));
        assert!(result3.value(2));
        assert!(result3.value(3));
    }

    #[test]
    fn test_regex_scan_masks_preserve_sql_null_semantics() {
        let plain = Arc::new(datatypes::arrow::array::StringArray::from(vec![
            Some("api"),
            Some("API"),
            Some("db"),
            None,
        ])) as ArrayRef;
        assert_regex_scan_masks(
            &plain,
            [
                vec![true, false, false, false],
                vec![true, true, false, false],
                vec![false, true, true, false],
                vec![false, false, true, false],
            ],
        );

        let dictionary = DictionaryArray::new(
            datatypes::arrow::array::UInt32Array::from(vec![
                Some(0),
                Some(1),
                Some(2),
                None,
                Some(3),
            ]),
            Arc::new(datatypes::arrow::array::StringArray::from(vec![
                Some("api"),
                Some("API"),
                Some("db"),
                None,
            ])),
        );
        let raw =
            regexp_is_match_dictionary(&dictionary, Some(&Regex::new("^api$").unwrap())).unwrap();
        assert!(raw.is_null(3)); // null dictionary key
        assert!(raw.is_null(4)); // non-null key referencing a null dictionary value

        let dictionary = Arc::new(dictionary) as ArrayRef;
        assert_regex_scan_masks(
            &dictionary,
            [
                vec![true, false, false, false, false],
                vec![true, true, false, false, false],
                vec![false, true, true, false, false],
                vec![false, false, true, false, false],
            ],
        );

        let negative = regex_evaluator(Operator::RegexNotMatch);
        assert!(!negative.evaluate_scalar(&ScalarValue::Utf8(None)).unwrap());
    }

    #[test]
    fn test_nullable_boolean_predicate_becomes_scan_mask() {
        let predicate = BooleanArray::from(vec![Some(true), None, Some(false)]);
        assert_eq!(
            BooleanArray::from(vec![true, false, false]),
            boolean_array_to_scan_mask(&predicate)
        );
    }

    fn assert_regex_scan_masks(input: &ArrayRef, expected: [Vec<bool>; 4]) {
        for (op, expected) in [
            Operator::RegexMatch,
            Operator::RegexIMatch,
            Operator::RegexNotMatch,
            Operator::RegexNotIMatch,
        ]
        .into_iter()
        .zip(expected)
        {
            assert_eq!(
                BooleanBuffer::from(expected),
                regex_evaluator(op).evaluate_array(input).unwrap(),
                "{op:?}"
            );
        }
    }

    fn regex_evaluator(op: Operator) -> SimpleFilterEvaluator {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("host"))),
            op,
            right: Box::new("^api$".lit()),
        });
        SimpleFilterEvaluator::try_new(&expr).unwrap()
    }

    fn ts_us(v: i64) -> ScalarValue {
        ScalarValue::TimestampMicrosecond(Some(v), None)
    }

    fn cast_to_ms(expr: &Expr) -> Option<TimestampUnitCast> {
        SimpleFilterEvaluator::try_new(expr)
            .unwrap()
            .cast_timestamp_unit(&ConcreteDataType::timestamp_millisecond_datatype())
    }

    /// Evaluates a cast `Filter` outcome against millisecond values and
    /// returns the mask.
    fn eval_ms_mask(cast: Option<TimestampUnitCast>, values: &[i64]) -> Vec<bool> {
        let filter = match cast.expect("cast must apply") {
            TimestampUnitCast::Filter(filter) => filter,
            other => panic!("expected Filter outcome, got {other:?}"),
        };
        let array = Arc::new(TimestampMillisecondArray::from(values.to_vec())) as ArrayRef;
        filter.evaluate_array(&array).unwrap().iter().collect()
    }

    #[test]
    fn cast_timestamp_unit_converts_representable_literal() {
        // ts = 7_000_000us evaluated against a ms column becomes ts = 7000ms.
        assert_eq!(
            vec![false, true, false],
            eval_ms_mask(
                cast_to_ms(&col("ts").eq(lit(ts_us(7_000_000)))),
                &[6_999, 7_000, 7_001]
            )
        );
        // != complements =.
        assert_eq!(
            vec![true, false, true],
            eval_ms_mask(
                cast_to_ms(&col("ts").not_eq(lit(ts_us(7_000_000)))),
                &[6_999, 7_000, 7_001]
            )
        );
    }

    #[test]
    fn cast_timestamp_unit_prunes_non_representable_equality() {
        // 7_000_500us is not a whole millisecond: no ms row can equal it,
        // and every ms row satisfies `!=`.
        assert!(matches!(
            cast_to_ms(&col("ts").eq(lit(ts_us(7_000_500)))),
            Some(TimestampUnitCast::Pruned)
        ));
        assert!(matches!(
            cast_to_ms(&col("ts").not_eq(lit(ts_us(7_000_500)))),
            Some(TimestampUnitCast::Matched)
        ));
    }

    #[test]
    fn cast_timestamp_unit_strengthens_inequalities() {
        // 2_500_500us is strictly between 2500ms and 2501ms: the cast must
        // not round the literal to a boundary the original predicate
        // excludes (>= 2500ms would wrongly match 2500ms).
        let values = vec![2_499, 2_500, 2_501];
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(cast_to_ms(&col("ts").gt(lit(ts_us(2_500_500)))), &values)
        );
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(cast_to_ms(&col("ts").gt_eq(lit(ts_us(2_500_500)))), &values)
        );
        assert_eq!(
            vec![true, true, false],
            eval_ms_mask(cast_to_ms(&col("ts").lt(lit(ts_us(2_500_500)))), &values)
        );
        assert_eq!(
            vec![true, true, false],
            eval_ms_mask(cast_to_ms(&col("ts").lt_eq(lit(ts_us(2_500_500)))), &values)
        );

        // Representable boundary keeps the original operator.
        let values = vec![2_499, 2_500, 2_501];
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(cast_to_ms(&col("ts").gt(lit(ts_us(2_500_000)))), &values)
        );
        assert_eq!(
            vec![false, true, true],
            eval_ms_mask(cast_to_ms(&col("ts").gt_eq(lit(ts_us(2_500_000)))), &values)
        );
        assert_eq!(
            vec![true, false, false],
            eval_ms_mask(cast_to_ms(&col("ts").lt(lit(ts_us(2_500_000)))), &values)
        );
        assert_eq!(
            vec![true, true, false],
            eval_ms_mask(cast_to_ms(&col("ts").lt_eq(lit(ts_us(2_500_000)))), &values)
        );
    }

    #[test]
    fn cast_timestamp_unit_handles_negative_instants() {
        // -2_500_500us floors to -2501ms with remainder 500us: only rows
        // with instant >= -2.5005ms may match >= / >.
        let values = vec![-2_502, -2_501, -2_500];
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(
                cast_to_ms(&col("ts").gt_eq(lit(ts_us(-2_500_500)))),
                &values
            )
        );
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(cast_to_ms(&col("ts").gt(lit(ts_us(-2_500_500)))), &values)
        );
        assert_eq!(
            vec![true, true, false],
            eval_ms_mask(
                cast_to_ms(&col("ts").lt_eq(lit(ts_us(-2_500_500)))),
                &values
            )
        );
        // Exactly representable negative literal: -2_500_000us == -2500ms.
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(cast_to_ms(&col("ts").eq(lit(ts_us(-2_500_000)))), &values)
        );
    }

    #[test]
    fn cast_timestamp_unit_or_chain_drops_unrepresentable_literals() {
        // Only 7_000_000us is a whole millisecond; 7_000_500us cannot match.
        let expr = col("ts")
            .eq(lit(ts_us(7_000_500)))
            .or(col("ts").eq(lit(ts_us(7_000_000))));
        assert_eq!(
            vec![false, true],
            eval_ms_mask(cast_to_ms(&expr), &[6_999, 7_000])
        );

        // A chain where no literal is representable matches nothing.
        let expr = col("ts")
            .eq(lit(ts_us(7_000_500)))
            .or(col("ts").eq(lit(ts_us(6_000_500))));
        assert!(matches!(cast_to_ms(&expr), Some(TimestampUnitCast::Pruned)));
    }

    #[test]
    fn cast_timestamp_unit_null_literal_prunes() {
        // A NULL literal never compares true.
        assert!(matches!(
            cast_to_ms(&col("ts").eq(lit(ScalarValue::TimestampMicrosecond(None, None)))),
            Some(TimestampUnitCast::Pruned)
        ));
    }

    #[test]
    fn cast_timestamp_unit_same_unit_returns_filter_directly() {
        // A literal already in the target unit is returned unchanged.
        let expr = col("ts").eq(lit(ts_us(7_000_000)));
        let filter = SimpleFilterEvaluator::try_new(&expr).unwrap();
        let cast = filter
            .cast_timestamp_unit(&ConcreteDataType::timestamp_microsecond_datatype())
            .unwrap();
        let TimestampUnitCast::Filter(f) = cast else {
            panic!("expected Filter, got {cast:?}")
        };
        assert_eq!(filter.op, f.op);
        assert_eq!(filter.column_name(), f.column_name());
        assert_eq!(
            ts_us(7_000_000),
            ScalarValue::try_from_array(f.literal.get().0, 0).unwrap()
        );
    }

    #[test]
    fn cast_timestamp_unit_rejects_non_timestamps() {
        // Non-timestamp target type.
        assert!(
            SimpleFilterEvaluator::try_new(&col("ts").gt(lit(ts_us(1))))
                .unwrap()
                .cast_timestamp_unit(&ConcreteDataType::int64_datatype())
                .is_none()
        );
        // Non-timestamp literal against a timestamp target.
        assert!(cast_to_ms(&col("ts").gt(lit(42_i64))).is_none());
    }

    #[test]
    fn cast_timestamp_unit_to_finer_unit() {
        // 5 seconds against a nanosecond column becomes 5_000_000_000ns.
        let filter = SimpleFilterEvaluator::try_new(
            &col("ts").eq(lit(ScalarValue::TimestampSecond(Some(5), None))),
        )
        .unwrap()
        .cast_timestamp_unit(&ConcreteDataType::timestamp_nanosecond_datatype())
        .and_then(|cast| match cast {
            TimestampUnitCast::Filter(filter) => Some(filter),
            _ => None,
        })
        .unwrap();
        let array = Arc::new(TimestampNanosecondArray::from(vec![
            4_999_999_999,
            5_000_000_000,
        ])) as ArrayRef;
        assert_eq!(
            vec![false, true],
            filter
                .evaluate_array(&array)
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        );
    }
}
