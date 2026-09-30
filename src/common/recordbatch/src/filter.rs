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

/// Evaluates supported single-column predicates with precompiled regular expressions.
/// Compound predicates preserve SQL null semantics until conversion to a scan mask.
#[derive(Debug, Clone)]
pub struct SimpleFilterEvaluator {
    column_name: String,
    predicate: SimplePredicate,
}

#[derive(Debug, Clone)]
enum SimplePredicate {
    Comparison {
        literal: Scalar<ArrayRef>,
        op: Operator,
        regex: Option<Regex>,
        regex_negative: bool,
    },
    InList {
        literals: Vec<Scalar<ArrayRef>>,
        negated: bool,
    },
    IsNull {
        negated: bool,
    },
    Constant(Option<bool>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl SimpleFilterEvaluator {
    pub fn new<T: Literal>(column_name: String, lit: T, op: Operator) -> Option<Self> {
        if !matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        ) {
            return None;
        }
        let Expr::Literal(value, _) = lit.lit() else {
            return None;
        };
        Some(Self {
            column_name,
            predicate: SimplePredicate::Comparison {
                literal: value.to_scalar().ok()?,
                op,
                regex: None,
                regex_negative: false,
            },
        })
    }

    pub fn try_new(predicate: &Expr) -> Option<Self> {
        Self::try_new_with_column_type(predicate, &|_| None)
    }

    /// Recognizes single-column filters, including lossless string dictionary casts
    /// when the caller can resolve the column's type.
    pub fn try_new_with_column_type(
        predicate: &Expr,
        column_type: &impl Fn(&str) -> Option<DataType>,
    ) -> Option<Self> {
        let columns = predicate.column_refs();
        if columns.len() != 1 {
            return None;
        }
        Some(Self {
            column_name: columns.into_iter().next()?.name.clone(),
            predicate: SimplePredicate::try_new(predicate, column_type)?,
        })
    }

    /// Returns the referenced column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn is_eq(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::Eq,
                ..
            }
        )
    }
    pub fn is_not_eq(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::NotEq,
                ..
            }
        )
    }
    pub fn is_lt(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::Lt,
                ..
            }
        )
    }
    pub fn is_lt_eq(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::LtEq,
                ..
            }
        )
    }
    pub fn is_gt(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::Gt,
                ..
            }
        )
    }
    pub fn is_gt_eq(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::Comparison {
                op: Operator::GtEq,
                ..
            }
        )
    }

    /// Returns true for a positive IN list or an OR chain of equality comparisons.
    pub fn is_or_eq_chain(&self) -> bool {
        matches!(
            self.predicate,
            SimplePredicate::InList { negated: false, .. }
        )
    }

    /// Returns the literal of a bare comparison, without discarding compound conditions.
    pub fn literal_value(&self) -> Option<Value> {
        let SimplePredicate::Comparison { literal, .. } = &self.predicate else {
            return None;
        };
        Value::try_from(ScalarValue::try_from_array(literal.get().0, 0).ok()?).ok()
    }

    /// Returns positive IN-list literals, or None for other predicates or unsupported values.
    pub fn literal_list_values(&self) -> Option<Vec<Value>> {
        let SimplePredicate::InList {
            literals,
            negated: false,
        } = &self.predicate
        else {
            return None;
        };
        literals
            .iter()
            .map(|literal| {
                Value::try_from(ScalarValue::try_from_array(literal.get().0, 0).ok()?).ok()
            })
            .collect()
    }

    pub fn evaluate_scalar(&self, input: &ScalarValue) -> Result<bool> {
        let input = input
            .to_scalar()
            .with_context(|_| ToArrowScalarSnafu { v: input.clone() })?;
        Ok(self.evaluate_datum(&input, 1)?.value(0))
    }

    pub fn evaluate_array(&self, input: &ArrayRef) -> Result<BooleanBuffer> {
        self.evaluate_datum(input, input.len())
    }

    pub fn evaluate_vector(&self, input: &VectorRef) -> Result<BooleanBuffer> {
        self.evaluate_array(&input.to_arrow_array())
    }

    fn evaluate_datum(&self, input: &impl Datum, len: usize) -> Result<BooleanBuffer> {
        let result = self.predicate.evaluate(input, len)?;
        Ok(boolean_array_to_scan_mask(&result).values().clone())
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

    /// Adapts timestamp predicates to an older SST's timestamp unit without rounding
    /// away boundary rows. Nonrepresentable equality literals cannot match a row.
    pub fn cast_timestamp_unit(&self, target: &ConcreteDataType) -> Option<TimestampUnitCast> {
        let DataType::Timestamp(target_unit, _) = target.as_arrow_type() else {
            return None;
        };
        let wrap = |predicate| Self {
            column_name: self.column_name.clone(),
            predicate,
        };
        match &self.predicate {
            SimplePredicate::And(left, right) | SimplePredicate::Or(left, right) => {
                let left = wrap(*left.clone()).cast_timestamp_unit(target)?;
                let right = wrap(*right.clone()).cast_timestamp_unit(target)?;
                let and = matches!(self.predicate, SimplePredicate::And(..));
                match (left, right) {
                    (TimestampUnitCast::Pruned, _) | (_, TimestampUnitCast::Pruned) if and => {
                        Some(TimestampUnitCast::Pruned)
                    }
                    (TimestampUnitCast::Matched, _) | (_, TimestampUnitCast::Matched) if !and => {
                        Some(TimestampUnitCast::Matched)
                    }
                    (TimestampUnitCast::Matched, other) | (other, TimestampUnitCast::Matched)
                        if and =>
                    {
                        Some(other)
                    }
                    (TimestampUnitCast::Pruned, other) | (other, TimestampUnitCast::Pruned)
                        if !and =>
                    {
                        Some(other)
                    }
                    (TimestampUnitCast::Filter(left), TimestampUnitCast::Filter(right)) => {
                        let left = Box::new(left.predicate);
                        let right = Box::new(right.predicate);
                        Some(TimestampUnitCast::Filter(wrap(if and {
                            SimplePredicate::And(left, right)
                        } else {
                            SimplePredicate::Or(left, right)
                        })))
                    }
                    _ => None,
                }
            }
            SimplePredicate::IsNull { .. } => Some(TimestampUnitCast::Filter(self.clone())),
            SimplePredicate::Constant(Some(true)) => Some(TimestampUnitCast::Matched),
            SimplePredicate::Constant(_) => Some(TimestampUnitCast::Pruned),
            SimplePredicate::InList { literals, negated } => {
                let mut converted = Vec::with_capacity(literals.len());
                for literal in literals {
                    let scalar = ScalarValue::try_from_array(literal.get().0, 0).ok()?;
                    let (value, unit) = timestamp_scalar_parts(&scalar)?;
                    let Some(value) = value else {
                        if *negated {
                            return Some(TimestampUnitCast::Pruned);
                        }
                        continue;
                    };
                    let cast = div_mod_units(value, unit.into(), target_unit.into())?;
                    if cast.remainder == 0 {
                        converted.push(timestamp_scalar(cast.quotient, target_unit)?);
                    }
                }
                if converted.is_empty() {
                    return Some(if *negated {
                        TimestampUnitCast::Matched
                    } else {
                        TimestampUnitCast::Pruned
                    });
                }
                Some(TimestampUnitCast::Filter(wrap(SimplePredicate::InList {
                    literals: converted,
                    negated: *negated,
                })))
            }
            SimplePredicate::Comparison { literal, op, .. } => {
                let scalar = ScalarValue::try_from_array(literal.get().0, 0).ok()?;
                let (value, unit) = timestamp_scalar_parts(&scalar)?;
                let Some(value) = value else {
                    return Some(TimestampUnitCast::Pruned);
                };
                if unit == target_unit {
                    return Some(TimestampUnitCast::Filter(self.clone()));
                }
                let cast = div_mod_units(value, unit.into(), target_unit.into())?;
                let divisible = cast.remainder == 0;
                let literal = timestamp_scalar(cast.quotient, target_unit)?;
                let filter = |op| {
                    TimestampUnitCast::Filter(wrap(SimplePredicate::Comparison {
                        literal: literal.clone(),
                        op,
                        regex: None,
                        regex_negative: false,
                    }))
                };
                Some(match op {
                    Operator::Eq if divisible => filter(Operator::Eq),
                    Operator::Eq => TimestampUnitCast::Pruned,
                    Operator::NotEq if divisible => filter(Operator::NotEq),
                    Operator::NotEq => TimestampUnitCast::Matched,
                    // The quotient is floor(L), including negative timestamps.
                    // v > L is v > quotient whether or not L is representable.
                    Operator::Gt => filter(Operator::Gt),
                    Operator::GtEq if divisible => filter(Operator::GtEq),
                    // Between target values, v >= L is v > quotient.
                    Operator::GtEq => filter(Operator::Gt),
                    Operator::Lt if divisible => filter(Operator::Lt),
                    // Between target values, v < L is v <= quotient.
                    Operator::Lt => filter(Operator::LtEq),
                    Operator::LtEq => filter(Operator::LtEq),
                    _ => return None,
                })
            }
        }
    }
}

impl SimplePredicate {
    fn try_new(expr: &Expr, column_type: &impl Fn(&str) -> Option<DataType>) -> Option<Self> {
        match expr {
            Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
                let left = Self::try_new(&binary.left, column_type)?;
                let right = Self::try_new(&binary.right, column_type)?;
                if binary.op == Operator::And {
                    return Some(Self::And(Box::new(left), Box::new(right)));
                }
                // Preserve the encoded-key IN-list fast path for pure equality disjunctions.
                if let (Some(mut lhs), Some(rhs)) =
                    (left.equality_literals(), right.equality_literals())
                {
                    lhs.extend(rhs);
                    return Some(Self::InList {
                        literals: lhs,
                        negated: false,
                    });
                }
                Some(Self::Or(Box::new(left), Box::new(right)))
            }
            Expr::BinaryExpr(binary) => {
                let mut op = binary.op;
                if !matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::RegexMatch
                        | Operator::RegexIMatch
                        | Operator::RegexNotMatch
                        | Operator::RegexNotIMatch
                ) {
                    return None;
                }
                let literal = if let Expr::Literal(literal, _) = &*binary.right {
                    Self::check_column(&binary.left, column_type)?;
                    literal
                } else if let Expr::Literal(literal, _) = &*binary.left {
                    Self::check_column(&binary.right, column_type)?;
                    op = op.swap()?;
                    literal
                } else {
                    return None;
                };
                let (regex, regex_negative) =
                    SimpleFilterEvaluator::maybe_build_regex(op, literal).ok()?;
                Some(Self::Comparison {
                    literal: literal.to_scalar().ok()?,
                    op,
                    regex,
                    regex_negative,
                })
            }
            Expr::IsNull(column) | Expr::IsNotNull(column) => {
                Self::check_column(column, column_type)?;
                Some(Self::IsNull {
                    negated: matches!(expr, Expr::IsNotNull(_)),
                })
            }
            Expr::InList(list) => {
                Self::check_column(&list.expr, column_type)?;
                let literals = list
                    .list
                    .iter()
                    .map(|expr| {
                        let Expr::Literal(value, _) = expr else {
                            return None;
                        };
                        value.to_scalar().ok()
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(Self::InList {
                    literals,
                    negated: list.negated,
                })
            }
            Expr::Literal(ScalarValue::Boolean(value), _) => Some(Self::Constant(*value)),
            _ => None,
        }
    }

    fn check_column(expr: &Expr, column_type: &impl Fn(&str) -> Option<DataType>) -> Option<()> {
        match expr {
            Expr::Column(_) => Some(()),
            Expr::Cast(cast) if cast.field.data_type() == &DataType::Utf8 => {
                let Expr::Column(column) = &*cast.expr else {
                    return None;
                };
                match column_type(&column.name)? {
                    DataType::Utf8 => Some(()),
                    DataType::Dictionary(key, value)
                        if *key == DataType::UInt32 && *value == DataType::Utf8 =>
                    {
                        Some(())
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn equality_literals(&self) -> Option<Vec<Scalar<ArrayRef>>> {
        match self {
            Self::Comparison {
                literal,
                op: Operator::Eq,
                ..
            } => Some(vec![literal.clone()]),
            Self::InList {
                literals,
                negated: false,
            } => Some(literals.clone()),
            _ => None,
        }
    }

    fn evaluate(&self, input: &impl Datum, len: usize) -> Result<BooleanArray> {
        match self {
            Self::Comparison {
                literal,
                op,
                regex,
                regex_negative,
            } => match op {
                Operator::Eq => cmp::eq(input, literal),
                Operator::NotEq => cmp::neq(input, literal),
                Operator::Lt => cmp::lt(input, literal),
                Operator::LtEq => cmp::lt_eq(input, literal),
                Operator::Gt => cmp::gt(input, literal),
                Operator::GtEq => cmp::gt_eq(input, literal),
                Operator::RegexMatch
                | Operator::RegexIMatch
                | Operator::RegexNotMatch
                | Operator::RegexNotIMatch => {
                    Self::regex_match(input, regex.as_ref(), *regex_negative)
                }
                _ => {
                    return UnsupportedOperationSnafu {
                        reason: format!("{op:?}"),
                    }
                    .fail();
                }
            }
            .context(ArrowComputeSnafu),
            Self::InList { literals, negated } => {
                let mut result = BooleanArray::from(vec![false; len]);
                for literal in literals {
                    let rhs = cmp::eq(input, literal).context(ArrowComputeSnafu)?;
                    result = or_kleene(&result, &rhs).context(ArrowComputeSnafu)?;
                }
                if *negated {
                    datatypes::compute::not(&result).context(ArrowComputeSnafu)
                } else {
                    Ok(result)
                }
            }
            Self::IsNull { negated } => {
                let values = match input.get().0.logical_nulls() {
                    Some(nulls) if *negated => nulls.inner().clone(),
                    Some(nulls) => !nulls.inner(),
                    None if *negated => BooleanBuffer::new_set(len),
                    None => BooleanBuffer::new_unset(len),
                };
                Ok(BooleanArray::new(values, None))
            }
            Self::Constant(value) => Ok(BooleanArray::from(vec![*value; len])),
            Self::And(left, right) => datatypes::compute::and_kleene(
                &left.evaluate(input, len)?,
                &right.evaluate(input, len)?,
            )
            .context(ArrowComputeSnafu),
            Self::Or(left, right) => {
                or_kleene(&left.evaluate(input, len)?, &right.evaluate(input, len)?)
                    .context(ArrowComputeSnafu)
            }
        }
    }

    fn regex_match(
        input: &impl Datum,
        regex: Option<&Regex>,
        negative: bool,
    ) -> std::result::Result<BooleanArray, ArrowError> {
        let array = input.get().0;

        // Try to cast to StringArray first
        if let Ok(string_array) = as_string_array(array) {
            let mut result = regexp_is_match_scalar(string_array, regex)?;
            if negative {
                result = datatypes::compute::not(&result)?;
            }
            return Ok(result);
        }

        // Try to cast to StringDictionaryArray
        if let Some(dict_array) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
            let mut result = regexp_is_match_dictionary(dict_array, regex)?;
            if negative {
                result = datatypes::compute::not(&result)?;
            }
            return Ok(result);
        }

        Err(ArrowError::CastError(format!(
            "Cannot cast {:?} to StringArray or StringDictionaryArray",
            array.data_type()
        )))
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
    fn null_disjunct_matches_string_labels() {
        use datatypes::arrow::array::{StringArray, UInt32Array};

        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("lo"),
            Some("eth0"),
            None,
        ]));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<UInt32Type>::new(
            UInt32Array::from(vec![None, Some(0), Some(1), Some(2), Some(3)]),
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("lo"),
                Some("eth0"),
                None,
            ])),
        ));
        for input in [strings, dictionary] {
            for (op, literal, expected) in [
                (Operator::Eq, "", vec![true, true, false, false, true]),
                (Operator::NotEq, "lo", vec![true, true, false, true, true]),
                (
                    Operator::RegexNotMatch,
                    "^(?:lo)$",
                    vec![true, true, false, true, true],
                ),
                (
                    Operator::RegexMatch,
                    "^(?:|eth0)$",
                    vec![true, true, false, true, true],
                ),
            ] {
                let comparison = Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(col("label")),
                    op,
                    Box::new(lit(literal)),
                ));
                for expr in [
                    col("label").is_null().or(comparison.clone()),
                    comparison.or(col("label").is_null()),
                ] {
                    let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();
                    assert_eq!(
                        evaluator.evaluate_array(&input).unwrap(),
                        BooleanBuffer::from(expected.clone())
                    );
                    assert!(evaluator.evaluate_scalar(&ScalarValue::Utf8(None)).unwrap());
                    assert_eq!(
                        evaluator
                            .evaluate_scalar(&ScalarValue::Utf8(Some("lo".into())))
                            .unwrap(),
                        expected[2]
                    );
                }
            }
        }
        for expr in [
            col("other").is_null().or(col("label").not_eq(lit("lo"))),
            col("a.label")
                .is_null()
                .or(col("b.label").not_eq(lit("lo"))),
            col("label").is_null().or(col("label").eq(col("other"))),
            col("label")
                .is_null()
                .or(Expr::Cast(datafusion::logical_expr::Cast::new(
                    Box::new(col("label")),
                    DataType::Int64,
                ))
                .eq(lit("lo"))),
        ] {
            assert!(SimpleFilterEvaluator::try_new(&expr).is_none(), "{expr}");
        }
    }

    #[test]
    fn string_cast_requires_matching_column_type() {
        let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(col("label")),
            DataType::Utf8,
        ));
        let expr = col("label").is_null().or(cast.not_eq(lit("lo")));
        for data_type in [
            DataType::Int64,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Binary,
        ] {
            assert!(
                SimpleFilterEvaluator::try_new_with_column_type(&expr, &|_| Some(
                    data_type.clone()
                ))
                .is_none()
            );
        }
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none());
    }

    #[tokio::test]
    async fn optimized_nullable_label_filters() {
        use datafusion::prelude::SessionContext;
        use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
        use datatypes::arrow::array::{StringArray, UInt32Array};

        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("tmpfs"),
            Some("ext4"),
        ]));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<UInt32Type>::new(
            UInt32Array::from(vec![None, Some(0), Some(1), Some(2)]),
            Arc::new(StringArray::from(vec!["", "tmpfs", "ext4"])),
        ));
        for input in [strings, dictionary] {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![Field::new(
                "label",
                input.data_type().clone(),
                true,
            )]));
            ctx.register_batch(
                "labels",
                RecordBatch::try_new(schema, vec![input.clone()]).unwrap(),
            )
            .unwrap();
            for (op, pattern) in [
                (Operator::RegexNotMatch, "^(?:tmpfs|vfat)$"),
                (Operator::NotEq, "tmpfs"),
            ] {
                let comparison = Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(col("label")),
                    op,
                    Box::new(lit(pattern)),
                ));
                let expr = col("label").is_null().or(comparison);
                let df = ctx.table("labels").await.unwrap().filter(expr).unwrap();
                let plan = df.into_optimized_plan().unwrap();
                let mut filters = Vec::new();
                plan.apply(|plan| {
                    if let datafusion::logical_expr::LogicalPlan::Filter(filter) = plan {
                        filters.push(filter.predicate.clone());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
                assert!(!filters.is_empty(), "{plan}");
                for filter in filters {
                    let evaluator =
                        SimpleFilterEvaluator::try_new_with_column_type(&filter, &|name| {
                            (name == "label").then(|| input.data_type().clone())
                        })
                        .unwrap_or_else(|| panic!("unsupported optimized filter: {filter}"));
                    assert_eq!(
                        evaluator.evaluate_array(&input).unwrap(),
                        BooleanBuffer::from(vec![true, true, false, true])
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn single_column_predicates_match_datafusion() {
        use datafusion::prelude::SessionContext;
        use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
        use datatypes::arrow::array::{Int32Array, StringArray, UInt32Array};

        let values = vec![
            None,
            Some(""),
            Some("lo"),
            Some("eth0"),
            Some("a\nb"),
            Some("服务"),
            None,
        ];
        let strings: ArrayRef = Arc::new(StringArray::from(values));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<UInt32Type>::new(
            UInt32Array::from(vec![
                None,
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
            ]),
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("lo"),
                Some("eth0"),
                Some("a\nb"),
                Some("服务"),
                None,
            ])),
        ));
        let null = || lit(ScalarValue::Utf8(None));
        let predicates = [
            col("label").is_null(),
            col("label").is_not_null(),
            col("label")
                .is_null()
                .or(col("label").eq(lit("")))
                .and(col("label").is_not_null()),
            col("label")
                .not_eq(lit("lo"))
                .and(col("label").not_eq(lit("eth0"))),
            col("label")
                .eq(lit("lo"))
                .or(col("label").eq(lit("eth0")))
                .or(col("label").eq(lit(""))),
            col("label").in_list(vec![lit(""), lit("lo"), null()], false),
            col("label").in_list(vec![lit(""), lit("lo")], true),
            col("label").in_list(vec![lit("lo"), null()], true),
            col("label")
                .is_null()
                .or(col("label").in_list(vec![lit("lo"), null()], true)),
            col("label").eq(null()).or(col("label").eq(lit("eth0"))),
            col("label").gt(lit("a")).and(col("label").lt(lit("z"))),
            lit("lo").not_eq(col("label")),
        ];
        for input in [strings, dictionary] {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("label", input.data_type().clone(), true),
                Field::new("id", DataType::Int32, false),
            ]));
            ctx.register_batch(
                "labels",
                RecordBatch::try_new(
                    schema,
                    vec![
                        input.clone(),
                        Arc::new(Int32Array::from_iter_values(0..input.len() as i32)),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
            // Compare nested predicates with DataFusion's three-valued logic.
            // Check scalars as well: primary-key filtering evaluates one series at a time.
            for left in &predicates {
                for right in &predicates {
                    for expr in [
                        left.clone().and(right.clone()),
                        left.clone().or(right.clone()),
                    ] {
                        let schema =
                            Schema::new(vec![Field::new("label", input.data_type().clone(), true)]);
                        let batch =
                            RecordBatch::try_new(Arc::new(schema.clone()), vec![input.clone()])
                                .unwrap();
                        let physical = create_physical_expr(
                            &expr,
                            &DFSchema::try_from(schema).unwrap(),
                            &ExecutionProps::new(),
                            &PhysicalPlanningContext::default(),
                        )
                        .unwrap();
                        let result = physical
                            .evaluate(&batch)
                            .unwrap()
                            .into_array(input.len())
                            .unwrap();
                        let expected =
                            boolean_array_to_scan_mask(as_boolean_array(&result).unwrap());
                        let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();
                        assert_eq!(
                            evaluator.evaluate_array(&input).unwrap(),
                            *expected.values(),
                            "{expr}"
                        );
                        for i in 0..input.len() {
                            let scalar = ScalarValue::try_from_array(&input, i).unwrap();
                            assert_eq!(
                                evaluator.evaluate_scalar(&scalar).unwrap(),
                                expected.value(i),
                                "{expr}, row={i}"
                            );
                        }
                    }
                }
            }
            for expr in &predicates {
                let df = ctx
                    .table("labels")
                    .await
                    .unwrap()
                    .filter(expr.clone())
                    .unwrap();
                let plan = df.clone().into_optimized_plan().unwrap();
                let expected = df
                    .collect()
                    .await
                    .unwrap()
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column_by_name("id")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect::<Vec<_>>();
                let evaluate = |expr: &Expr| {
                    SimpleFilterEvaluator::try_new_with_column_type(expr, &|name| {
                        (name == "label").then(|| input.data_type().clone())
                    })
                    .unwrap_or_else(|| panic!("unsupported: {expr}"))
                    .evaluate_array(&input)
                    .unwrap()
                };
                let selected = |mask: BooleanBuffer| {
                    mask.iter()
                        .enumerate()
                        .filter_map(|(i, matched)| matched.then_some(i as i32))
                        .collect::<Vec<_>>()
                };
                assert_eq!(selected(evaluate(expr)), expected, "{expr}");
                let mut filters = Vec::new();
                plan.apply(|plan| {
                    if let datafusion::logical_expr::LogicalPlan::Filter(filter) = plan {
                        filters.push(filter.predicate.clone());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
                if !filters.is_empty() {
                    let mut mask = BooleanBuffer::new_set(input.len());
                    for filter in filters {
                        mask = &mask & &evaluate(&filter);
                    }
                    assert_eq!(selected(mask), expected, "optimized: {plan}");
                }
            }
        }
    }

    #[test]
    fn float_membership_matches_datafusion() {
        use datatypes::arrow::array::Float64Array;

        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            None,
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(-f64::NAN),
            Some(f64::NEG_INFINITY),
            Some(f64::INFINITY),
            Some(1.0),
        ]));
        let schema = Schema::new(vec![Field::new("value", DataType::Float64, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![input.clone()]).unwrap();
        for literals in [
            vec![lit(0.0), lit(f64::NAN)],
            vec![lit(-0.0)],
            vec![lit(f64::NAN), lit(ScalarValue::Float64(None))],
        ] {
            for negated in [false, true] {
                let membership = col("value").in_list(literals.clone(), negated);
                for expr in [membership.clone(), col("value").is_null().or(membership)] {
                    let physical = create_physical_expr(
                        &expr,
                        &DFSchema::try_from(schema.clone()).unwrap(),
                        &ExecutionProps::new(),
                        &PhysicalPlanningContext::default(),
                    )
                    .unwrap();
                    let result = physical
                        .evaluate(&batch)
                        .unwrap()
                        .into_array(input.len())
                        .unwrap();
                    let expected = boolean_array_to_scan_mask(as_boolean_array(&result).unwrap());
                    let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();
                    assert_eq!(
                        evaluator.evaluate_array(&input).unwrap(),
                        *expected.values(),
                        "{expr}"
                    );
                    for i in 0..input.len() {
                        let scalar = ScalarValue::try_from_array(&input, i).unwrap();
                        assert_eq!(
                            evaluator.evaluate_scalar(&scalar).unwrap(),
                            expected.value(i),
                            "{expr}, row={i}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn compound_timestamp_filters_preserve_unit_conversion() {
        use datatypes::arrow::array::TimestampMicrosecondArray;

        let null = || lit(ScalarValue::TimestampMicrosecond(None, None));
        let predicates = [
            col("ts")
                .gt(lit(ts_us(-1500)))
                .and(col("ts").lt(lit(ts_us(500)))),
            col("ts")
                .eq(lit(ts_us(500)))
                .or(col("ts").gt_eq(lit(ts_us(1500)))),
            col("ts").in_list(vec![lit(ts_us(500)), lit(ts_us(1000)), null()], false),
            col("ts").in_list(vec![lit(ts_us(500)), lit(ts_us(1000))], true),
            col("ts").in_list(vec![lit(ts_us(500)), null()], true),
            col("ts")
                .is_null()
                .or(col("ts").in_list(vec![lit(ts_us(500)), null()], true)),
            col("ts")
                .is_not_null()
                .and(col("ts").not_eq(lit(ts_us(500)))),
            col("ts").eq(null()).or(col("ts").eq(lit(ts_us(-1000)))),
        ];
        let native: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            -2000, -1000, 0, 1000, 2000,
        ]));
        let old: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![-2, -1, 0, 1, 2]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            native.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![native]).unwrap();
        let composed = predicates.iter().flat_map(|left| {
            predicates.iter().flat_map(move |right| {
                [
                    left.clone().and(right.clone()),
                    left.clone().or(right.clone()),
                ]
            })
        });
        for expr in predicates.iter().cloned().chain(composed) {
            let physical = create_physical_expr(
                &expr,
                &DFSchema::try_from(schema.as_ref().clone()).unwrap(),
                &ExecutionProps::new(),
                &PhysicalPlanningContext::default(),
            )
            .unwrap();
            let result = physical
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();
            let expected = boolean_array_to_scan_mask(as_boolean_array(&result).unwrap())
                .values()
                .clone();
            let actual = match cast_to_ms(&expr).unwrap() {
                TimestampUnitCast::Matched => BooleanBuffer::new_set(old.len()),
                TimestampUnitCast::Pruned => BooleanBuffer::new_unset(old.len()),
                TimestampUnitCast::Filter(filter) => filter.evaluate_array(&old).unwrap(),
            };
            assert_eq!(actual, expected, "{expr}");
        }
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
        assert!(evaluator.is_gt());
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
        assert!(or_evaluator.is_or_eq_chain());
        assert_eq!(
            or_evaluator.literal_list_values().unwrap(),
            vec![Value::from("B"), Value::from("C"), Value::from("D")]
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
        assert!(filter.is_eq() && f.is_eq());
        assert_eq!(filter.column_name(), f.column_name());
        assert_eq!(filter.literal_value(), f.literal_value());
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
