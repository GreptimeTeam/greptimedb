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

//! Predicate recognition, execution, and precompiled regular expressions.

#[cfg(test)]
mod tests;

use datafusion::logical_expr::{Expr, Operator};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::{ArrayRef, Datum, Scalar};
use datafusion_common::arrow::buffer::BooleanBuffer;
use datafusion_common::arrow::compute::kernels::cmp;
use datafusion_common::cast::as_string_array;
use datatypes::arrow::array::{
    Array, ArrayAccessor, ArrayData, BooleanArray, BooleanBufferBuilder, DictionaryArray,
    StringArrayType,
};
use datatypes::arrow::datatypes::{DataType, UInt32Type};
use datatypes::arrow::error::ArrowError;
use datatypes::compute::or_kleene;
use regex::Regex;
use snafu::ResultExt;

use crate::error::{ArrowComputeSnafu, Result, UnsupportedOperationSnafu};

#[derive(Debug, Clone)]
pub(crate) enum SimplePredicate {
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

impl SimplePredicate {
    pub(crate) fn try_new(
        expr: &Expr,
        column_type: &impl Fn(&str) -> Option<DataType>,
    ) -> Option<Self> {
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
                let (regex, regex_negative) = Self::maybe_build_regex(op, literal).ok()?;
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

    pub(crate) fn evaluate(&self, input: &impl Datum, len: usize) -> Result<BooleanArray> {
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
