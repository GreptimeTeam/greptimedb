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

//! Accumulators for aggregate functions that's is accumulatable. i.e. sum/count
//!
//! Accumulator will only be restore from row and being updated every time dataflow need process a new batch of rows.
//! So the overhead is acceptable.
//!
//! Currently support sum, count, any, all and min/max(with one caveat that min/max can't support delete with aggregate).
//! TODO: think of better ways to not ser/de every time a accum needed to be updated, since it's in a tight loop

use std::any::type_name;
use std::fmt::Display;

use common_decimal::Decimal128;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::{OrderedF32, OrderedF64, OrderedFloat, Value};
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::expr::error::{InternalSnafu, OverflowSnafu, TryFromValueSnafu, TypeMismatchSnafu};
use crate::expr::signature::GenericFn;
use crate::expr::{AggregateFunc, EvalError};
use crate::repr::Diff;

/// Accumulates values for the various types of accumulable aggregations.
#[enum_dispatch]
pub trait Accumulator: Sized {
    fn into_state(self) -> Vec<Value>;

    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError>;

    fn update_batch<I>(&mut self, aggr_fn: &AggregateFunc, value_diffs: I) -> Result<(), EvalError>
    where
        I: IntoIterator<Item = (Value, Diff)>,
    {
        for (v, d) in value_diffs {
            self.update(aggr_fn, v, d)?;
        }
        Ok(())
    }

    fn eval(&self, aggr_fn: &AggregateFunc) -> Result<Value, EvalError>;
}

/// Bool accumulator, used for `Any` `All` `Max/MinBool`
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Bool {
    /// The number of `true` values observed.
    trues: Diff,
    /// The number of `false` values observed.
    falses: Diff,
}

impl Bool {
    /// Expect two `Diff` type values, one for `true` and one for `false`.
    pub fn try_from_iter<I>(iter: &mut I) -> Result<Self, EvalError>
    where
        I: Iterator<Item = Value>,
    {
        Ok(Self {
            trues: Diff::try_from(iter.next().ok_or_else(fail_accum::<Self>)?)
                .map_err(err_try_from_val)?,
            falses: Diff::try_from(iter.next().ok_or_else(fail_accum::<Self>)?)
                .map_err(err_try_from_val)?,
        })
    }
}

impl TryFrom<Vec<Value>> for Bool {
    type Error = EvalError;

    fn try_from(state: Vec<Value>) -> Result<Self, Self::Error> {
        ensure!(
            state.len() == 2,
            InternalSnafu {
                reason: "Bool Accumulator state should have 2 values",
            }
        );
        let mut iter = state.into_iter();

        Self::try_from_iter(&mut iter)
    }
}

impl Accumulator for Bool {
    fn into_state(self) -> Vec<Value> {
        vec![self.trues.into(), self.falses.into()]
    }

    /// Null values are ignored
    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError> {
        ensure!(
            matches!(
                aggr_fn,
                AggregateFunc::Any
                    | AggregateFunc::All
                    | AggregateFunc::MaxBool
                    | AggregateFunc::MinBool
            ),
            InternalSnafu {
                reason: format!(
                    "Bool Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
        );

        match value {
            Value::Boolean(true) => self.trues += diff,
            Value::Boolean(false) => self.falses += diff,
            Value::Null => (), // ignore nulls
            x => {
                return Err(TypeMismatchSnafu {
                    expected: ConcreteDataType::boolean_datatype(),
                    actual: x.data_type(),
                }
                .build());
            }
        };
        Ok(())
    }

    fn eval(&self, aggr_fn: &AggregateFunc) -> Result<Value, EvalError> {
        match aggr_fn {
            AggregateFunc::Any => Ok(Value::from(self.trues > 0)),
            AggregateFunc::All => Ok(Value::from(self.falses == 0)),
            AggregateFunc::MaxBool => Ok(Value::from(self.trues > 0)),
            AggregateFunc::MinBool => Ok(Value::from(self.falses == 0)),
            _ => Err(InternalSnafu {
                reason: format!(
                    "Bool Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
            .build()),
        }
    }
}

/// Accumulates simple numeric values for sum over integer.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SimpleNumber {
    /// The accumulation of all non-NULL values observed.
    accum: i128,
    /// The number of non-NULL values observed.
    non_nulls: Diff,
}

impl SimpleNumber {
    /// Expect one `Decimal128` and one `Diff` type values.
    /// The `Decimal128` type is used to store the sum of all non-NULL values.
    /// The `Diff` type is used to count the number of non-NULL values.
    pub fn try_from_iter<I>(iter: &mut I) -> Result<Self, EvalError>
    where
        I: Iterator<Item = Value>,
    {
        Ok(Self {
            accum: Decimal128::try_from(iter.next().ok_or_else(fail_accum::<Self>)?)
                .map_err(err_try_from_val)?
                .val(),
            non_nulls: Diff::try_from(iter.next().ok_or_else(fail_accum::<Self>)?)
                .map_err(err_try_from_val)?,
        })
    }
}

impl TryFrom<Vec<Value>> for SimpleNumber {
    type Error = EvalError;

    fn try_from(state: Vec<Value>) -> Result<Self, Self::Error> {
        ensure!(
            state.len() == 2,
            InternalSnafu {
                reason: "Number Accumulator state should have 2 values",
            }
        );
        let mut iter = state.into_iter();
        Self::try_from_iter(&mut iter)
    }
}

impl Accumulator for SimpleNumber {
    fn into_state(self) -> Vec<Value> {
        vec![
            Value::Decimal128(Decimal128::new(self.accum, 38, 0)),
            self.non_nulls.into(),
        ]
    }

    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError> {
        ensure!(
            matches!(
                aggr_fn,
                AggregateFunc::SumInt16
                    | AggregateFunc::SumInt32
                    | AggregateFunc::SumInt64
                    | AggregateFunc::SumUInt16
                    | AggregateFunc::SumUInt32
                    | AggregateFunc::SumUInt64
            ),
            InternalSnafu {
                reason: format!(
                    "SimpleNumber Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
        );

        let v = match (aggr_fn, value) {
            (AggregateFunc::SumInt16, Value::Int16(x)) => i128::from(x),
            (AggregateFunc::SumInt32, Value::Int32(x)) => i128::from(x),
            (AggregateFunc::SumInt64, Value::Int64(x)) => i128::from(x),
            (AggregateFunc::SumUInt16, Value::UInt16(x)) => i128::from(x),
            (AggregateFunc::SumUInt32, Value::UInt32(x)) => i128::from(x),
            (AggregateFunc::SumUInt64, Value::UInt64(x)) => i128::from(x),
            (_f, Value::Null) => return Ok(()), // ignore null
            (f, v) => {
                let expected_datatype = f.signature().input;
                return Err(TypeMismatchSnafu {
                    expected: expected_datatype[0].clone(),
                    actual: v.data_type(),
                }
                .build())?;
            }
        };

        self.accum += v * i128::from(diff);

        self.non_nulls += diff;
        Ok(())
    }

    fn eval(&self, aggr_fn: &AggregateFunc) -> Result<Value, EvalError> {
        match aggr_fn {
            AggregateFunc::SumInt16 | AggregateFunc::SumInt32 | AggregateFunc::SumInt64 => {
                i64::try_from(self.accum)
                    .map_err(|_e| OverflowSnafu {}.build())
                    .map(Value::from)
            }
            AggregateFunc::SumUInt16 | AggregateFunc::SumUInt32 | AggregateFunc::SumUInt64 => {
                u64::try_from(self.accum)
                    .map_err(|_e| OverflowSnafu {}.build())
                    .map(Value::from)
            }
            _ => Err(InternalSnafu {
                reason: format!(
                    "SimpleNumber Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
            .build()),
        }
    }
}
/// Accumulates float values for sum over floating numbers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Float {
    /// Accumulates non-special float values, i.e. not NaN, +inf, -inf.
    /// accum will be set to zero if `non_nulls` is zero.
    accum: OrderedF64,
    /// Counts +inf
    pos_infs: Diff,
    /// Counts -inf
    neg_infs: Diff,
    /// Counts NaNs
    nans: Diff,
    /// Counts non-NULL values
    non_nulls: Diff,
}

impl Float {
    /// Expect first value to be `OrderedF64` and the rest four values to be `Diff` type values.
    pub fn try_from_iter<I>(iter: &mut I) -> Result<Self, EvalError>
    where
        I: Iterator<Item = Value>,
    {
        let mut ret = Self {
            accum: OrderedF64::try_from(iter.next().ok_or_else(fail_accum::<Self>)?)
                .map_err(err_try_from_val)?,
            pos_infs: Diff::try_from(iter.next().ok_or_else(fail_accum::<Self>)?)
                .map_err(err_try_from_val)?,
            neg_infs: Diff::try_from(iter.next().ok_or_else(fail_accum::<Self>)?)
                .map_err(err_try_from_val)?,
            nans: Diff::try_from(iter.next().ok_or_else(fail_accum::<Self>)?)
                .map_err(err_try_from_val)?,
            non_nulls: Diff::try_from(iter.next().ok_or_else(fail_accum::<Self>)?)
                .map_err(err_try_from_val)?,
        };

        // This prevent counter-intuitive behavior of summing over no values having non-zero results
        if ret.non_nulls == 0 {
            ret.accum = OrderedFloat::from(0.0);
        }

        Ok(ret)
    }
}

impl TryFrom<Vec<Value>> for Float {
    type Error = EvalError;

    fn try_from(state: Vec<Value>) -> Result<Self, Self::Error> {
        ensure!(
            state.len() == 5,
            InternalSnafu {
                reason: "Float Accumulator state should have 5 values",
            }
        );

        let mut iter = state.into_iter();

        let mut ret = Self {
            accum: OrderedF64::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            pos_infs: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            neg_infs: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            nans: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            non_nulls: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        };

        // This prevent counter-intuitive behavior of summing over no values
        if ret.non_nulls == 0 {
            ret.accum = OrderedFloat::from(0.0);
        }

        Ok(ret)
    }
}

impl Accumulator for Float {
    fn into_state(self) -> Vec<Value> {
        vec![
            self.accum.into(),
            self.pos_infs.into(),
            self.neg_infs.into(),
            self.nans.into(),
            self.non_nulls.into(),
        ]
    }

    /// sum ignore null
    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError> {
        ensure!(
            matches!(
                aggr_fn,
                AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64
            ),
            InternalSnafu {
                reason: format!(
                    "Float Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
        );

        let x = match (aggr_fn, value) {
            (AggregateFunc::SumFloat32, Value::Float32(x)) => OrderedF64::from(*x as f64),
            (AggregateFunc::SumFloat64, Value::Float64(x)) => OrderedF64::from(x),
            (_f, Value::Null) => return Ok(()), // ignore null
            (f, v) => {
                let expected_datatype = f.signature().input;
                return Err(TypeMismatchSnafu {
                    expected: expected_datatype[0].clone(),
                    actual: v.data_type(),
                }
                .build())?;
            }
        };

        if x.is_nan() {
            self.nans += diff;
        } else if x.is_infinite() {
            if x.is_sign_positive() {
                self.pos_infs += diff;
            } else {
                self.neg_infs += diff;
            }
        } else {
            self.accum += *(x * OrderedF64::from(diff as f64));
        }

        self.non_nulls += diff;
        Ok(())
    }

    fn eval(&self, aggr_fn: &AggregateFunc) -> Result<Value, EvalError> {
        match aggr_fn {
            AggregateFunc::SumFloat32 => Ok(Value::Float32(OrderedF32::from(self.accum.0 as f32))),
            AggregateFunc::SumFloat64 => Ok(Value::Float64(self.accum)),
            _ => Err(InternalSnafu {
                reason: format!(
                    "Float Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
            .build()),
        }
    }
}

/// Accumulates a single `Ord`ed `Value`, useful for min/max aggregations.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OrdValue {
    val: Option<Value>,
    non_nulls: Diff,
}

impl OrdValue {
    pub fn try_from_iter<I>(iter: &mut I) -> Result<Self, EvalError>
    where
        I: Iterator<Item = Value>,
    {
        Ok(Self {
            val: {
                let v = iter.next().ok_or_else(fail_accum::<Self>)?;
                if v == Value::Null {
                    None
                } else {
                    Some(v)
                }
            },
            non_nulls: Diff::try_from(iter.next().ok_or_else(fail_accum::<Self>)?)
                .map_err(err_try_from_val)?,
        })
    }
}

impl TryFrom<Vec<Value>> for OrdValue {
    type Error = EvalError;

    fn try_from(state: Vec<Value>) -> Result<Self, Self::Error> {
        ensure!(
            state.len() == 2,
            InternalSnafu {
                reason: "OrdValue Accumulator state should have 2 values",
            }
        );

        let mut iter = state.into_iter();

        Ok(Self {
            val: {
                let v = iter.next().unwrap();
                if v == Value::Null {
                    None
                } else {
                    Some(v)
                }
            },
            non_nulls: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        })
    }
}

impl Accumulator for OrdValue {
    fn into_state(self) -> Vec<Value> {
        vec![self.val.unwrap_or(Value::Null), self.non_nulls.into()]
    }

    /// min/max try to find results in all non-null values, if all values are null, the result is null.
    /// count(col_name) gives the number of non-null values, count(*) gives the number of rows including nulls.
    /// TODO(discord9): add count(*) as a aggr function
    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError> {
        ensure!(
            aggr_fn.is_max() || aggr_fn.is_min() || matches!(aggr_fn, AggregateFunc::Count),
            InternalSnafu {
                reason: format!(
                    "OrdValue Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
        );
        if diff <= 0 && (aggr_fn.is_max() || aggr_fn.is_min()) {
            return Err(InternalSnafu {
                reason: "OrdValue Accumulator does not support non-monotonic input for min/max aggregation".to_string(),
            }.build());
        }

        // if aggr_fn is count, the incoming value type doesn't matter in type checking
        // otherwise, type need to be the same or value can be null
        let check_type_aggr_fn_and_arg_value =
            ty_eq_without_precision(value.data_type(), aggr_fn.signature().input[0].clone())
                || matches!(aggr_fn, AggregateFunc::Count)
                || value.is_null();
        let check_type_aggr_fn_and_self_val = self
            .val
            .as_ref()
            .map(|zelf| {
                ty_eq_without_precision(zelf.data_type(), aggr_fn.signature().input[0].clone())
            })
            .unwrap_or(true)
            || matches!(aggr_fn, AggregateFunc::Count);

        if !check_type_aggr_fn_and_arg_value {
            return Err(TypeMismatchSnafu {
                expected: aggr_fn.signature().input[0].clone(),
                actual: value.data_type(),
            }
            .build());
        } else if !check_type_aggr_fn_and_self_val {
            return Err(TypeMismatchSnafu {
                expected: aggr_fn.signature().input[0].clone(),
                actual: self
                    .val
                    .as_ref()
                    .map(|v| v.data_type())
                    .unwrap_or(ConcreteDataType::null_datatype()),
            }
            .build());
        }

        let is_null = value.is_null();
        if is_null {
            return Ok(());
        }

        if !is_null {
            // compile count(*) to count(true) to include null/non-nulls
            // And the counts of non-null values are updated here
            self.non_nulls += diff;

            match aggr_fn.signature().generic_fn {
                GenericFn::Max => {
                    self.val = self
                        .val
                        .clone()
                        .map(|v| v.max(value.clone()))
                        .or_else(|| Some(value))
                }
                GenericFn::Min => {
                    self.val = self
                        .val
                        .clone()
                        .map(|v| v.min(value.clone()))
                        .or_else(|| Some(value))
                }

                GenericFn::Count => (),
                _ => unreachable!("already checked by ensure!"),
            }
        };
        // min/max ignore nulls

        Ok(())
    }

    fn eval(&self, aggr_fn: &AggregateFunc) -> Result<Value, EvalError> {
        if aggr_fn.is_max() || aggr_fn.is_min() {
            Ok(self.val.clone().unwrap_or(Value::Null))
        } else if matches!(aggr_fn, AggregateFunc::Count) {
            Ok(self.non_nulls.into())
        } else {
            Err(InternalSnafu {
                reason: format!(
                    "OrdValue Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
            .build())
        }
    }
}

/// Accumulates values for the various types of accumulable aggregations.
///
/// We assume that there are not more than 2^32 elements for the aggregation.
/// Thus we can perform a summation over i32 in an i64 accumulator
/// and not worry about exceeding its bounds.
///
/// The float accumulator performs accumulation with tolerance for floating point error.
///
/// TODO(discord9): check for overflowing
#[enum_dispatch(Accumulator)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Accum {
    /// Accumulates boolean values.
    Bool(Bool),
    /// Accumulates simple numeric values.
    SimpleNumber(SimpleNumber),
    /// Accumulates float values.
    Float(Float),
    /// Accumulate Values that impl `Ord`
    OrdValue(OrdValue),
}

impl Accum {
    /// create a new accumulator from given aggregate function
    pub fn new_accum(aggr_fn: &AggregateFunc) -> Result<Self, EvalError> {
        Ok(match aggr_fn {
            AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::MaxBool
            | AggregateFunc::MinBool => Self::from(Bool {
                trues: 0,
                falses: 0,
            }),
            AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64 => Self::from(SimpleNumber {
                accum: 0,
                non_nulls: 0,
            }),
            AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => Self::from(Float {
                accum: OrderedF64::from(0.0),
                pos_infs: 0,
                neg_infs: 0,
                nans: 0,
                non_nulls: 0,
            }),
            f if f.is_max() || f.is_min() || matches!(f, AggregateFunc::Count) => {
                Self::from(OrdValue {
                    val: None,
                    non_nulls: 0,
                })
            }
            f => {
                return Err(InternalSnafu {
                    reason: format!(
                        "Accumulator does not support this aggregation function: {:?}",
                        f
                    ),
                }
                .build());
            }
        })
    }

    pub fn try_from_iter(
        aggr_fn: &AggregateFunc,
        iter: &mut impl Iterator<Item = Value>,
    ) -> Result<Self, EvalError> {
        match aggr_fn {
            AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::MaxBool
            | AggregateFunc::MinBool => Ok(Self::from(Bool::try_from_iter(iter)?)),
            AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64 => Ok(Self::from(SimpleNumber::try_from_iter(iter)?)),
            AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => {
                Ok(Self::from(Float::try_from_iter(iter)?))
            }
            f if f.is_max() || f.is_min() || matches!(f, AggregateFunc::Count) => {
                Ok(Self::from(OrdValue::try_from_iter(iter)?))
            }
            f => Err(InternalSnafu {
                reason: format!(
                    "Accumulator does not support this aggregation function: {:?}",
                    f
                ),
            }
            .build()),
        }
    }

    /// try to convert a vector of value into given aggregate function's accumulator
    pub fn try_into_accum(aggr_fn: &AggregateFunc, state: Vec<Value>) -> Result<Self, EvalError> {
        match aggr_fn {
            AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::MaxBool
            | AggregateFunc::MinBool => Ok(Self::from(Bool::try_from(state)?)),
            AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64 => Ok(Self::from(SimpleNumber::try_from(state)?)),
            AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => {
                Ok(Self::from(Float::try_from(state)?))
            }
            f if f.is_max() || f.is_min() || matches!(f, AggregateFunc::Count) => {
                Ok(Self::from(OrdValue::try_from(state)?))
            }
            f => Err(InternalSnafu {
                reason: format!(
                    "Accumulator does not support this aggregation function: {:?}",
                    f
                ),
            }
            .build()),
        }
    }
}

fn fail_accum<T>() -> EvalError {
    InternalSnafu {
        reason: format!(
            "list of values exhausted before a accum of type {} can be build from it",
            type_name::<T>()
        ),
    }
    .build()
}

fn err_try_from_val<T: Display>(reason: T) -> EvalError {
    TryFromValueSnafu {
        msg: reason.to_string(),
    }
    .build()
}

/// compare type while ignore their precision, including `TimeStamp`, `Time`,
/// `Duration`, `Interval`
fn ty_eq_without_precision(left: ConcreteDataType, right: ConcreteDataType) -> bool {
    left == right
        || matches!(left, ConcreteDataType::Timestamp(..))
            && matches!(right, ConcreteDataType::Timestamp(..))
        || matches!(left, ConcreteDataType::Time(..)) && matches!(right, ConcreteDataType::Time(..))
        || matches!(left, ConcreteDataType::Duration(..))
            && matches!(right, ConcreteDataType::Duration(..))
        || matches!(left, ConcreteDataType::Interval(..))
            && matches!(right, ConcreteDataType::Interval(..))
}

#[allow(clippy::too_many_lines)]
#[cfg(test)]
mod test {
    use common_time::DateTime;

    use super::*;

    #[test]
    fn test_accum() {
        let testcases = vec![
            (
                AggregateFunc::SumInt32,
                vec![(Value::Int32(1), 1), (Value::Null, 1)],
                (
                    Value::Int64(1),
                    vec![Value::Decimal128(Decimal128::new(1, 38, 0)), 1i64.into()],
                ),
            ),
            (
                AggregateFunc::SumFloat32,
                vec![(Value::Float32(OrderedF32::from(1.0)), 1), (Value::Null, 1)],
                (
                    Value::Float32(OrderedF32::from(1.0)),
                    vec![
                        Value::Float64(OrderedF64::from(1.0)),
                        0i64.into(),
                        0i64.into(),
                        0i64.into(),
                        1i64.into(),
                    ],
                ),
            ),
            (
                AggregateFunc::MaxInt32,
                vec![(Value::Int32(1), 1), (Value::Int32(2), 1), (Value::Null, 1)],
                (Value::Int32(2), vec![Value::Int32(2), 2i64.into()]),
            ),
            (
                AggregateFunc::MinInt32,
                vec![(Value::Int32(2), 1), (Value::Int32(1), 1), (Value::Null, 1)],
                (Value::Int32(1), vec![Value::Int32(1), 2i64.into()]),
            ),
            (
                AggregateFunc::MaxFloat32,
                vec![
                    (Value::Float32(OrderedF32::from(1.0)), 1),
                    (Value::Float32(OrderedF32::from(2.0)), 1),
                    (Value::Null, 1),
                ],
                (
                    Value::Float32(OrderedF32::from(2.0)),
                    vec![Value::Float32(OrderedF32::from(2.0)), 2i64.into()],
                ),
            ),
            (
                AggregateFunc::MaxDateTime,
                vec![
                    (Value::DateTime(DateTime::from(0)), 1),
                    (Value::DateTime(DateTime::from(1)), 1),
                    (Value::Null, 1),
                ],
                (
                    Value::DateTime(DateTime::from(1)),
                    vec![Value::DateTime(DateTime::from(1)), 2i64.into()],
                ),
            ),
            (
                AggregateFunc::Count,
                vec![
                    (Value::Int32(1), 1),
                    (Value::Int32(2), 1),
                    (Value::Null, 1),
                    (Value::Null, 1),
                ],
                (2i64.into(), vec![Value::Null, 2i64.into()]),
            ),
            (
                AggregateFunc::Any,
                vec![
                    (Value::Boolean(false), 1),
                    (Value::Boolean(false), 1),
                    (Value::Boolean(true), 1),
                    (Value::Null, 1),
                ],
                (
                    Value::Boolean(true),
                    vec![Value::from(1i64), Value::from(2i64)],
                ),
            ),
            (
                AggregateFunc::All,
                vec![
                    (Value::Boolean(false), 1),
                    (Value::Boolean(false), 1),
                    (Value::Boolean(true), 1),
                    (Value::Null, 1),
                ],
                (
                    Value::Boolean(false),
                    vec![Value::from(1i64), Value::from(2i64)],
                ),
            ),
            (
                AggregateFunc::MaxBool,
                vec![
                    (Value::Boolean(false), 1),
                    (Value::Boolean(false), 1),
                    (Value::Boolean(true), 1),
                    (Value::Null, 1),
                ],
                (
                    Value::Boolean(true),
                    vec![Value::from(1i64), Value::from(2i64)],
                ),
            ),
            (
                AggregateFunc::MinBool,
                vec![
                    (Value::Boolean(false), 1),
                    (Value::Boolean(false), 1),
                    (Value::Boolean(true), 1),
                    (Value::Null, 1),
                ],
                (
                    Value::Boolean(false),
                    vec![Value::from(1i64), Value::from(2i64)],
                ),
            ),
        ];

        for (aggr_fn, input, (eval_res, state)) in testcases {
            let create_and_insert = || -> Result<Accum, EvalError> {
                let mut acc = Accum::new_accum(&aggr_fn)?;
                acc.update_batch(&aggr_fn, input.clone())?;
                let row = acc.into_state();
                let acc = Accum::try_into_accum(&aggr_fn, row.clone())?;
                let alter_acc = Accum::try_from_iter(&aggr_fn, &mut row.into_iter())?;
                assert_eq!(acc, alter_acc);
                Ok(acc)
            };
            let acc = match create_and_insert() {
                Ok(acc) => acc,
                Err(err) => panic!(
                    "Failed to create accum for {:?} with input {:?} with error: {:?}",
                    aggr_fn, input, err
                ),
            };

            if acc.eval(&aggr_fn).unwrap() != eval_res {
                panic!(
                    "Failed to eval accum for {:?} with input {:?}, expect {:?}, got {:?}",
                    aggr_fn,
                    input,
                    eval_res,
                    acc.eval(&aggr_fn).unwrap()
                );
            }
            let actual_state = acc.into_state();
            if actual_state != state {
                panic!(
                    "Failed to cast into state from accum for {:?} with input {:?}, expect state {:?}, got state {:?}",
                    aggr_fn,
                    input,
                    state,
                    actual_state
                );
            }
        }
    }
    #[test]
    fn test_fail_path_accum() {
        {
            let bool_accum = Bool::try_from(vec![Value::Null]);
            assert!(matches!(bool_accum, Err(EvalError::Internal { .. })));
        }

        {
            let mut bool_accum = Bool::try_from(vec![1i64.into(), 1i64.into()]).unwrap();
            // serde
            let bool_accum_serde = serde_json::to_string(&bool_accum).unwrap();
            let bool_accum_de = serde_json::from_str::<Bool>(&bool_accum_serde).unwrap();
            assert_eq!(bool_accum, bool_accum_de);
            assert!(matches!(
                bool_accum.update(&AggregateFunc::MaxDate, 1.into(), 1),
                Err(EvalError::Internal { .. })
            ));
            assert!(matches!(
                bool_accum.update(&AggregateFunc::Any, 1.into(), 1),
                Err(EvalError::TypeMismatch { .. })
            ));
            assert!(matches!(
                bool_accum.eval(&AggregateFunc::MaxDate),
                Err(EvalError::Internal { .. })
            ));
        }

        {
            let ret = SimpleNumber::try_from(vec![Value::Null]);
            assert!(matches!(ret, Err(EvalError::Internal { .. })));
            let mut accum =
                SimpleNumber::try_from(vec![Decimal128::new(0, 38, 0).into(), 0i64.into()])
                    .unwrap();

            assert!(matches!(
                accum.update(&AggregateFunc::All, 0.into(), 1),
                Err(EvalError::Internal { .. })
            ));
            assert!(matches!(
                accum.update(&AggregateFunc::SumInt64, 0i32.into(), 1),
                Err(EvalError::TypeMismatch { .. })
            ));
            assert!(matches!(
                accum.eval(&AggregateFunc::All),
                Err(EvalError::Internal { .. })
            ));
            accum
                .update(&AggregateFunc::SumInt64, 1i64.into(), 1)
                .unwrap();
            accum
                .update(&AggregateFunc::SumInt64, i64::MAX.into(), 1)
                .unwrap();
            assert!(matches!(
                accum.eval(&AggregateFunc::SumInt64),
                Err(EvalError::Overflow { .. })
            ));
        }

        {
            let ret = Float::try_from(vec![2f64.into(), 0i64.into(), 0i64.into(), 0i64.into()]);
            assert!(matches!(ret, Err(EvalError::Internal { .. })));
            let mut accum = Float::try_from(vec![
                2f64.into(),
                0i64.into(),
                0i64.into(),
                0i64.into(),
                1i64.into(),
            ])
            .unwrap();
            accum
                .update(&AggregateFunc::SumFloat64, 2f64.into(), -1)
                .unwrap();
            assert!(matches!(
                accum.update(&AggregateFunc::All, 0.into(), 1),
                Err(EvalError::Internal { .. })
            ));
            assert!(matches!(
                accum.update(&AggregateFunc::SumFloat64, 0.0f32.into(), 1),
                Err(EvalError::TypeMismatch { .. })
            ));
            // no record, no accum
            assert_eq!(
                accum.eval(&AggregateFunc::SumFloat64).unwrap(),
                0.0f64.into()
            );

            assert!(matches!(
                accum.eval(&AggregateFunc::All),
                Err(EvalError::Internal { .. })
            ));

            accum
                .update(&AggregateFunc::SumFloat64, f64::INFINITY.into(), 1)
                .unwrap();
            accum
                .update(&AggregateFunc::SumFloat64, (-f64::INFINITY).into(), 1)
                .unwrap();
            accum
                .update(&AggregateFunc::SumFloat64, f64::NAN.into(), 1)
                .unwrap();
        }

        {
            let ret = OrdValue::try_from(vec![Value::Null]);
            assert!(matches!(ret, Err(EvalError::Internal { .. })));
            let mut accum = OrdValue::try_from(vec![Value::Null, 0i64.into()]).unwrap();
            assert!(matches!(
                accum.update(&AggregateFunc::All, 0.into(), 1),
                Err(EvalError::Internal { .. })
            ));
            accum
                .update(&AggregateFunc::MaxInt16, 1i16.into(), 1)
                .unwrap();
            assert!(matches!(
                accum.update(&AggregateFunc::MaxInt16, 0i32.into(), 1),
                Err(EvalError::TypeMismatch { .. })
            ));
            assert!(matches!(
                accum.update(&AggregateFunc::MaxInt16, 0i16.into(), -1),
                Err(EvalError::Internal { .. })
            ));
            accum
                .update(&AggregateFunc::MaxInt16, Value::Null, 1)
                .unwrap();
        }

        // insert uint64 into max_int64 should fail
        {
            let mut accum = OrdValue::try_from(vec![Value::Null, 0i64.into()]).unwrap();
            assert!(matches!(
                accum.update(&AggregateFunc::MaxInt64, 0u64.into(), 1),
                Err(EvalError::TypeMismatch { .. })
            ));
        }
    }
}
