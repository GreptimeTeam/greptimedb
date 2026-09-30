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

//! Timestamp-unit adaptation for predicates evaluated against historical SSTs.

#[cfg(test)]
mod tests;

use common_time::timestamp::div_mod_units;
use datafusion::logical_expr::Operator;
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::{ArrayRef, Datum, Scalar};
use datatypes::arrow::datatypes::TimeUnit;

use crate::filter::predicate::SimplePredicate;

/// Builds a tz-naive timestamp literal for `value` in `unit`.
pub fn timestamp_scalar_value(value: i64, unit: TimeUnit) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), None),
    }
}

impl SimplePredicate {
    pub(crate) fn cast_timestamp_unit(&self, target_unit: TimeUnit) -> Option<Self> {
        // Constants represent scan outcomes here. As in the public Matched/Pruned
        // result, UNKNOWN can be collapsed to false under these AND/OR-only trees.
        match self {
            Self::And(left, right) | Self::Or(left, right) => {
                let left = left.cast_timestamp_unit(target_unit)?;
                let right = right.cast_timestamp_unit(target_unit)?;
                let and = matches!(self, Self::And(..));
                Some(match (left, right) {
                    (Self::Constant(Some(false)), _) | (_, Self::Constant(Some(false))) if and => {
                        Self::Constant(Some(false))
                    }
                    (Self::Constant(Some(true)), _) | (_, Self::Constant(Some(true))) if !and => {
                        Self::Constant(Some(true))
                    }
                    (Self::Constant(Some(true)), other) | (other, Self::Constant(Some(true)))
                        if and =>
                    {
                        other
                    }
                    (Self::Constant(Some(false)), other) | (other, Self::Constant(Some(false)))
                        if !and =>
                    {
                        other
                    }
                    (left, right) => {
                        if and {
                            Self::And(Box::new(left), Box::new(right))
                        } else {
                            Self::Or(Box::new(left), Box::new(right))
                        }
                    }
                })
            }
            Self::IsNull { .. } => Some(self.clone()),
            Self::Constant(value) => Some(Self::Constant(Some(value.unwrap_or(false)))),
            Self::InList { literals, negated } => {
                let mut converted = Vec::with_capacity(literals.len());
                for literal in literals {
                    let scalar = ScalarValue::try_from_array(literal.get().0, 0).ok()?;
                    let (value, unit) = timestamp_scalar_parts(&scalar)?;
                    let Some(value) = value else {
                        if *negated {
                            return Some(Self::Constant(Some(false)));
                        }
                        continue;
                    };
                    let cast = div_mod_units(value, unit.into(), target_unit.into())?;
                    if cast.remainder == 0 {
                        converted.push(timestamp_scalar(cast.quotient, target_unit)?);
                    }
                }
                if converted.is_empty() {
                    return Some(Self::Constant(Some(*negated)));
                }
                Some(Self::InList {
                    literals: converted,
                    negated: *negated,
                })
            }
            Self::Comparison { literal, op, .. } => {
                let scalar = ScalarValue::try_from_array(literal.get().0, 0).ok()?;
                let (value, unit) = timestamp_scalar_parts(&scalar)?;
                let Some(value) = value else {
                    return Some(Self::Constant(Some(false)));
                };
                if unit == target_unit {
                    return Some(self.clone());
                }
                let cast = div_mod_units(value, unit.into(), target_unit.into())?;
                let divisible = cast.remainder == 0;
                let literal = timestamp_scalar(cast.quotient, target_unit)?;
                let filter = |op| Self::Comparison {
                    literal: literal.clone(),
                    op,
                    regex: None,
                    regex_negative: false,
                };
                Some(match op {
                    Operator::Eq if divisible => filter(Operator::Eq),
                    Operator::Eq => Self::Constant(Some(false)),
                    Operator::NotEq if divisible => filter(Operator::NotEq),
                    Operator::NotEq => Self::Constant(Some(true)),
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

/// Builds a one-element scalar array holding `value` in `unit`.
fn timestamp_scalar(value: i64, unit: TimeUnit) -> Option<Scalar<ArrayRef>> {
    timestamp_scalar_value(value, unit).to_scalar().ok()
}
