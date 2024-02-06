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
//! Currently support sum, count, any, all

use std::fmt::Display;

use common_decimal::Decimal128;
use common_time::{Date, DateTime};
use datatypes::data_type::ConcreteDataType;
use datatypes::value::{OrderedF32, OrderedF64, OrderedFloat, Value};
use hydroflow::futures::stream::Concat;
use serde::{Deserialize, Serialize};

use crate::expr::error::{InternalSnafu, TryFromValueSnafu, TypeMismatchSnafu};
use crate::expr::{AggregateFunc, EvalError};
use crate::repr::Diff;

/// Accumulates values for the various types of accumulable aggregations.
pub trait Accumulator: Sized {
    fn try_from_state(state: Vec<Value>) -> Result<Self, EvalError>;
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
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BoolAccum {
    /// The number of `true` values observed.
    trues: Diff,
    /// The number of `false` values observed.
    falses: Diff,
}

impl Accumulator for BoolAccum {
    fn try_from_state(state: Vec<Value>) -> Result<Self, EvalError> {
        if state.len() != 2 {
            return Err(InternalSnafu {
                reason: "Bool Accumulator state should have 2 values",
            }
            .build());
        }
        let mut iter = state.into_iter();

        Ok(Self {
            trues: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            falses: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        })
    }

    fn into_state(self) -> Vec<Value> {
        vec![self.trues.into(), self.falses.into()]
    }

    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError> {
        match value {
            Value::Boolean(true) => self.trues += diff,
            Value::Boolean(false) => self.falses += diff,
            x => {
                return Err(TypeMismatchSnafu {
                    expected: ConcreteDataType::boolean_datatype(),
                    actual: x.data_type(),
                }
                .build())
            }
        };
        Ok(())
    }
}

/// Accumulates simple numeric values.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SimpleNumberAccum {
    /// The accumulation of all non-NULL values observed.
    accum: i128,
    /// The number of non-NULL values observed.
    non_nulls: Diff,
}

impl Accumulator for SimpleNumberAccum {
    fn try_from_state(state: Vec<Value>) -> Result<Self, EvalError> {
        if state.len() != 2 {
            return Err(InternalSnafu {
                reason: "Number Accumulator state should have 2 values",
            }
            .build());
        }
        let mut iter = state.into_iter();

        Ok(Self {
            accum: Decimal128::try_from(iter.next().unwrap())
                .map_err(err_try_from_val)?
                .val(),
            non_nulls: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        })
    }

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
        let ty = value.data_type();
        let v = match value {
            Value::Int16(x) => Some(i128::from(x)),
            Value::Int32(x) => Some(i128::from(x)),
            Value::Int64(x) => Some(i128::from(x)),
            Value::UInt16(x) => Some(i128::from(x)),
            Value::UInt32(x) => Some(i128::from(x)),
            Value::UInt64(x) => Some(i128::from(x)),
            // Date&DateTime is converted to their inner value
            Value::Date(x) => Some(i128::from(x.val())),
            Value::DateTime(x) => Some(i128::from(x.val())),
            _ => None,
        };
        if let Some(v) = v {
            if aggr_fn.is_sum() {
                self.accum += v * i128::from(diff);
            } else if aggr_fn.is_max() {
                // min/max is accumulative only if input is monotonic(no delete)
                if diff <= 0 {
                    return Err(InternalSnafu {
                                    reason: "SimpleNumber Accumulator does not support non-monotonic input for max aggregation".to_string(),
                                }.build());
                }
                self.accum = std::cmp::max(self.accum, v);
            } else if aggr_fn.is_min() {
                // min/max is accumulative only if input is monotonic(no delete)
                if diff <= 0 {
                    return Err(InternalSnafu {
                                    reason: "SimpleNumber Accumulator does not support non-monotonic input for min aggregation".to_string(),
                                }.build());
                }
                self.accum = std::cmp::min(self.accum, v);
            } else if matches!(aggr_fn, AggregateFunc::Count) {
            } else {
                return Err(InternalSnafu {
                    reason: format!(
                        "SimpleNumber Accumulator does not support this aggregation function: {:?}",
                        aggr_fn
                    ),
                }
                .build());
            }
        } else if matches!(aggr_fn, AggregateFunc::Count) {
        } else {
            let expected_datatype = match aggr_fn {
                AggregateFunc::SumInt16 => ConcreteDataType::int16_datatype(),
                AggregateFunc::SumInt32 => ConcreteDataType::int32_datatype(),
                AggregateFunc::SumInt64 => ConcreteDataType::int64_datatype(),
                AggregateFunc::SumUInt16 => ConcreteDataType::uint16_datatype(),
                AggregateFunc::SumUInt32 => ConcreteDataType::uint32_datatype(),
                AggregateFunc::SumUInt64 => ConcreteDataType::uint64_datatype(),
                _ => unreachable!(),
            };
            Err(TypeMismatchSnafu {
                expected: expected_datatype,
                actual: ty,
            }
            .build())?
        }

        self.non_nulls += diff;
        Ok(())
    }
}
/// Accumulates float values.
pub struct FloatAccum {
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

impl Accumulator for FloatAccum {
    fn try_from_state(state: Vec<Value>) -> Result<Self, EvalError> {
        if state.len() != 5 {
            return Err(InternalSnafu {
                reason: "Float Accumulator state should have 5 values",
            }
            .build());
        }
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

    fn into_state(self) -> Vec<Value> {
        vec![
            self.accum.into(),
            self.pos_infs.into(),
            self.neg_infs.into(),
            self.nans.into(),
            self.non_nulls.into(),
        ]
    }

    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError> {
        let ty = value.data_type();
        let v = match value {
            Value::Float32(x) => Some(OrderedF64::from(*x as f64)),
            Value::Float64(x) => Some(OrderedF64::from(x)),
            _ => None,
        };
        if let Some(x) = v {
            if x.is_nan() {
                self.nans += diff;
            } else if x.is_infinite() {
                if *x > 0.0 {
                    self.pos_infs += diff;
                } else {
                    self.neg_infs += diff;
                }
            } else if aggr_fn.is_sum() {
                self.accum += *(x * OrderedF64::from(diff as f64));
            } else if aggr_fn.is_max() {
                // min/max is accumulative only if input is monotonic(no delete)
                if diff <= 0 {
                    return Err(InternalSnafu {
                                    reason: "SimpleNumber Accumulator does not support non-monotonic input for max aggregation".to_string(),
                                }.build());
                }
                self.accum = std::cmp::max(self.accum, x);
            } else if aggr_fn.is_min() {
                // min/max is accumulative only if input is monotonic(no delete)
                if diff <= 0 {
                    return Err(InternalSnafu {
                                    reason: "SimpleNumber Accumulator does not support non-monotonic input for max aggregation".to_string(),
                                }.build());
                }
                self.accum = std::cmp::min(self.accum, x);
            } else {
                return Err(InternalSnafu {
                    reason: format!(
                        "Float Accumulator does not support this aggregation function: {:?}",
                        aggr_fn
                    ),
                }
                .build());
            }
        } else if matches!(aggr_fn, AggregateFunc::Count) {
            return Err(InternalSnafu {
                reason: "Count aggregation is not suppose to use Float Accumulator".to_string(),
            }
            .build());
        } else {
            let expected_datatype = match aggr_fn {
                AggregateFunc::SumFloat32 => ConcreteDataType::float32_datatype(),
                AggregateFunc::SumFloat64 => ConcreteDataType::float64_datatype(),
                _ => unreachable!(),
            };
            Err(TypeMismatchSnafu {
                expected: expected_datatype,
                actual: ty,
            }
            .build())?
        }
        self.non_nulls += diff;
        Ok(())
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Accum {
    /// Accumulates boolean values.
    Bool {
        /// The number of `true` values observed.
        trues: Diff,
        /// The number of `false` values observed.
        falses: Diff,
    },
    /// Accumulates simple numeric values.
    SimpleNumber {
        /// The accumulation of all non-NULL values observed.
        accum: i128,
        /// The number of non-NULL values observed.
        non_nulls: Diff,
    },
    /// Accumulates float values.
    Float {
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
    },
}

impl Accum {
    pub fn new_accum(aggr_fn: &AggregateFunc) -> Result<Self, EvalError> {
        Ok(match aggr_fn {
            AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::MaxBool
            | AggregateFunc::MinBool => Self::Bool {
                trues: 0,
                falses: 0,
            },
            AggregateFunc::Count
            | AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64 => Self::SimpleNumber {
                accum: 0,
                non_nulls: 0,
            },
            AggregateFunc::MaxInt16
            | AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxUInt16
            | AggregateFunc::MaxUInt32
            | AggregateFunc::MaxUInt64
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxDateTime => Self::SimpleNumber {
                accum: i128::MIN,
                non_nulls: 0,
            },
            AggregateFunc::MinInt16
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinUInt16
            | AggregateFunc::MinUInt32
            | AggregateFunc::MinUInt64
            | AggregateFunc::MinDate
            | AggregateFunc::MinDateTime => Self::SimpleNumber {
                accum: i128::MAX,
                non_nulls: 0,
            },
            AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => Self::Float {
                accum: OrderedF64::from(0.0),
                pos_infs: 0,
                neg_infs: 0,
                nans: 0,
                non_nulls: 0,
            },
            _ => {
                return Err(InternalSnafu {
                    reason: format!("Aggregation function: {:?} is not accumulable.", aggr_fn),
                }
                .build())
            }
        })
    }
    pub fn try_into_accum(aggr_fn: &AggregateFunc, state: Vec<Value>) -> Result<Self, EvalError> {
        match aggr_fn {
            AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::MaxBool
            | AggregateFunc::MinBool => Self::try_into_bool(state),
            AggregateFunc::Count
            | AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64
            | AggregateFunc::MaxInt16
            | AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxUInt16
            | AggregateFunc::MaxUInt32
            | AggregateFunc::MaxUInt64
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxDateTime
            | AggregateFunc::MinInt16
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinUInt16
            | AggregateFunc::MinUInt32
            | AggregateFunc::MinUInt64
            | AggregateFunc::MinDate
            | AggregateFunc::MinDateTime => Self::try_into_number(state),
            AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => Self::try_into_float(state),
            _ => Err(InternalSnafu {
                reason: format!("Aggregation function: {:?} is not accumulable.", aggr_fn),
            }
            .build()),
        }
    }
    pub fn try_into_bool(state: Vec<Value>) -> Result<Self, EvalError> {
        if state.len() != 2 {
            return Err(InternalSnafu {
                reason: "Bool Accumulator state should have 2 values",
            }
            .build());
        }
        let mut iter = state.into_iter();

        Ok(Self::Bool {
            trues: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            falses: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        })
    }

    /// Try convert the state into `SimpleNumber` Accumulator
    pub fn try_into_number(state: Vec<Value>) -> Result<Self, EvalError> {
        if state.len() != 2 {
            return Err(InternalSnafu {
                reason: "Number Accumulator state should have 2 values",
            }
            .build());
        }
        let mut iter = state.into_iter();

        Ok(Self::SimpleNumber {
            accum: Decimal128::try_from(iter.next().unwrap())
                .map_err(err_try_from_val)?
                .val(),
            non_nulls: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        })
    }

    pub fn try_into_float(state: Vec<Value>) -> Result<Self, EvalError> {
        if state.len() != 5 {
            return Err(InternalSnafu {
                reason: "Float Accumulator state should have 5 values",
            }
            .build());
        }
        let mut iter = state.into_iter();

        let mut ret = Self::Float {
            accum: OrderedF64::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            pos_infs: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            neg_infs: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            nans: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            non_nulls: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        };

        // This prevent counter-intuitive behavior of summing over no values
        if let Self::Float {
            accum, non_nulls, ..
        } = &mut ret
        {
            if *non_nulls == 0 {
                *accum = OrderedFloat::from(0.0);
            }
        }

        Ok(ret)
    }

    pub fn into_state(self) -> Vec<Value> {
        match self {
            Self::Bool { trues, falses } => vec![trues.into(), falses.into()],
            Self::SimpleNumber { accum, non_nulls } => vec![
                Value::Decimal128(Decimal128::new(accum, 38, 0)),
                non_nulls.into(),
            ],
            Self::Float {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => vec![
                accum.into(),
                pos_infs.into(),
                neg_infs.into(),
                nans.into(),
                non_nulls.into(),
            ],
        }
    }

    pub fn update_batch<I>(
        &mut self,
        aggr_fn: &AggregateFunc,
        value_diffs: I,
    ) -> Result<(), EvalError>
    where
        I: IntoIterator<Item = (Value, Diff)>,
    {
        match self {
            Self::Bool { trues, falses } => {
                for (v, diff) in value_diffs.into_iter() {
                    match v {
                        Value::Boolean(true) => *trues += diff,
                        Value::Boolean(false) => *falses += diff,
                        x => Err(TypeMismatchSnafu {
                            expected: ConcreteDataType::boolean_datatype(),
                            actual: x.data_type(),
                        }
                        .build())?,
                    }
                }
            }
            Self::SimpleNumber { accum, non_nulls } => {
                for (v, diff) in value_diffs.into_iter() {
                    let ty = v.data_type();
                    let v = match v {
                        Value::Int16(x) => Some(i128::from(x)),
                        Value::Int32(x) => Some(i128::from(x)),
                        Value::Int64(x) => Some(i128::from(x)),
                        Value::UInt16(x) => Some(i128::from(x)),
                        Value::UInt32(x) => Some(i128::from(x)),
                        Value::UInt64(x) => Some(i128::from(x)),
                        // Date&DateTime is converted to their inner value
                        Value::Date(x) => Some(i128::from(x.val())),
                        Value::DateTime(x) => Some(i128::from(x.val())),
                        _ => None,
                    };
                    if let Some(v) = v {
                        if aggr_fn.is_sum() {
                            *accum += v * i128::from(diff);
                        } else if aggr_fn.is_max() {
                            // min/max is accumulative only if input is monotonic(no delete)
                            if diff <= 0 {
                                return Err(InternalSnafu {
                                    reason: "SimpleNumber Accumulator does not support non-monotonic input for max aggregation".to_string(),
                                }.build());
                            }
                            *accum = std::cmp::max(*accum, v);
                        } else if aggr_fn.is_min() {
                            // min/max is accumulative only if input is monotonic(no delete)
                            if diff <= 0 {
                                return Err(InternalSnafu {
                                    reason: "SimpleNumber Accumulator does not support non-monotonic input for min aggregation".to_string(),
                                }.build());
                            }
                            *accum = std::cmp::min(*accum, v);
                        } else if matches!(aggr_fn, AggregateFunc::Count) {
                        } else {
                            return Err(InternalSnafu {
                                reason: format!(
                                    "SimpleNumber Accumulator does not support this aggregation function: {:?}",
                                    aggr_fn
                                ),
                            }.build());
                        }
                    } else if matches!(aggr_fn, AggregateFunc::Count) {
                    } else {
                        let expected_datatype = match aggr_fn {
                            AggregateFunc::SumInt16 => ConcreteDataType::int16_datatype(),
                            AggregateFunc::SumInt32 => ConcreteDataType::int32_datatype(),
                            AggregateFunc::SumInt64 => ConcreteDataType::int64_datatype(),
                            AggregateFunc::SumUInt16 => ConcreteDataType::uint16_datatype(),
                            AggregateFunc::SumUInt32 => ConcreteDataType::uint32_datatype(),
                            AggregateFunc::SumUInt64 => ConcreteDataType::uint64_datatype(),
                            _ => unreachable!(),
                        };
                        Err(TypeMismatchSnafu {
                            expected: expected_datatype,
                            actual: ty,
                        }
                        .build())?
                    }

                    *non_nulls += diff;
                }
            }
            Self::Float {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => {
                for (v, diff) in value_diffs.into_iter() {
                    let ty = v.data_type();
                    let v = match v {
                        Value::Float32(x) => Some(OrderedF64::from(*x as f64)),
                        Value::Float64(x) => Some(OrderedF64::from(x)),
                        _ => None,
                    };
                    if let Some(x) = v {
                        if x.is_nan() {
                            *nans += diff;
                        } else if x.is_infinite() {
                            if *x > 0.0 {
                                *pos_infs += diff;
                            } else {
                                *neg_infs += diff;
                            }
                        } else if aggr_fn.is_sum() {
                            *accum += *(x * OrderedF64::from(diff as f64));
                        } else if aggr_fn.is_max() {
                            // min/max is accumulative only if input is monotonic(no delete)
                            if diff <= 0 {
                                return Err(InternalSnafu {
                                    reason: "SimpleNumber Accumulator does not support non-monotonic input for max aggregation".to_string(),
                                }.build());
                            }
                            *accum = std::cmp::max(*accum, x);
                        } else if aggr_fn.is_min() {
                            // min/max is accumulative only if input is monotonic(no delete)
                            if diff <= 0 {
                                return Err(InternalSnafu {
                                    reason: "SimpleNumber Accumulator does not support non-monotonic input for max aggregation".to_string(),
                                }.build());
                            }
                            *accum = std::cmp::min(*accum, x);
                        } else {
                            return Err(InternalSnafu {
                                reason: format!(
                                    "Float Accumulator does not support this aggregation function: {:?}",
                                    aggr_fn
                                ),
                            }.build());
                        }
                    } else if matches!(aggr_fn, AggregateFunc::Count) {
                        return Err(InternalSnafu {
                            reason: "Count aggregation is not suppose to use Float Accumulator"
                                .to_string(),
                        }
                        .build());
                    } else {
                        let expected_datatype = match aggr_fn {
                            AggregateFunc::SumFloat32 => ConcreteDataType::float32_datatype(),
                            AggregateFunc::SumFloat64 => ConcreteDataType::float64_datatype(),
                            _ => unreachable!(),
                        };
                        Err(TypeMismatchSnafu {
                            expected: expected_datatype,
                            actual: ty,
                        }
                        .build())?
                    }
                    *non_nulls += diff;
                }
            }
        }
        Ok(())
    }

    pub fn eval(&self, aggr_fn: &AggregateFunc) -> Result<Value, EvalError> {
        match self {
            Self::Bool { trues, falses } => match aggr_fn {
                AggregateFunc::Any => Ok(Value::from(*trues > 0)),
                AggregateFunc::All => Ok(Value::from(*falses == 0)),
                AggregateFunc::MaxBool => Ok(Value::from(*trues > 0)),
                AggregateFunc::MinBool => Ok(Value::from(*falses == 0)),
                _ => Err(InternalSnafu {
                    reason: format!(
                        "Bool Accumulator does not support this aggregation function: {:?}",
                        aggr_fn
                    ),
                }
                .build()),
            },
            Self::SimpleNumber { accum, non_nulls } => match aggr_fn {
                AggregateFunc::Count => Ok(Value::UInt64(*non_nulls as u64)),
                AggregateFunc::SumInt16 | AggregateFunc::SumInt32 | AggregateFunc::SumInt64 => {
                    Ok(Value::from(*accum as i64))
                }
                AggregateFunc::SumUInt16 | AggregateFunc::SumUInt32 | AggregateFunc::SumUInt64 => {
                    Ok(Value::from(*accum as u64))
                }
                AggregateFunc::MaxInt16 => Ok(Value::Int16(*accum as i16)),
                AggregateFunc::MaxInt32 => Ok(Value::Int32(*accum as i32)),
                AggregateFunc::MaxInt64 => Ok(Value::Int64(*accum as i64)),
                AggregateFunc::MaxUInt16 => Ok(Value::UInt16(*accum as u16)),
                AggregateFunc::MaxUInt32 => Ok(Value::UInt32(*accum as u32)),
                AggregateFunc::MaxUInt64 => Ok(Value::UInt64(*accum as u64)),
                AggregateFunc::MaxDate => Ok(Value::Date(Date::from(*accum as i32))),
                AggregateFunc::MaxDateTime => Ok(Value::DateTime(DateTime::from(*accum as i64))),
                AggregateFunc::MinInt16 => Ok(Value::Int16(*accum as i16)),
                AggregateFunc::MinInt32 => Ok(Value::Int32(*accum as i32)),
                AggregateFunc::MinInt64 => Ok(Value::Int64(*accum as i64)),
                AggregateFunc::MinUInt16 => Ok(Value::UInt16(*accum as u16)),
                AggregateFunc::MinUInt32 => Ok(Value::UInt32(*accum as u32)),
                AggregateFunc::MinUInt64 => Ok(Value::UInt64(*accum as u64)),
                AggregateFunc::MinDate => Ok(Value::Date(Date::from(*accum as i32))),
                AggregateFunc::MinDateTime => Ok(Value::DateTime(DateTime::from(*accum as i64))),
                _ => Err(InternalSnafu {
                    reason: format!(
                        "SimpleNumber Accumulator does not support this aggregation function: {:?}",
                        aggr_fn
                    ),
                }
                .build()),
            },
            Self::Float {
                accum,
                pos_infs,
                neg_infs,
                nans,
                non_nulls,
            } => match aggr_fn {
                AggregateFunc::SumFloat32 => Ok(Value::Float32(OrderedF32::from(accum.0 as f32))),
                AggregateFunc::SumFloat64 => Ok(Value::Float64(*accum)),
                _ => Err(InternalSnafu {
                    reason: format!(
                        "Float Accumulator does not support this aggregation function: {:?}",
                        aggr_fn
                    ),
                }
                .build()),
            },
        }
    }
}

fn err_try_from_val<T: Display>(reason: T) -> EvalError {
    TryFromValueSnafu {
        msg: reason.to_string(),
    }
    .build()
}

fn update_bool(v: Value, diff: i64, trues: &mut i64, falses: &mut i64) -> Result<(), EvalError> {
    match v {
        Value::Boolean(true) => *trues += diff,
        Value::Boolean(false) => *falses += diff,
        x => {
            return Err(TypeMismatchSnafu {
                expected: ConcreteDataType::boolean_datatype(),
                actual: x.data_type(),
            }
            .build())
        }
    };
    Ok(())
}

fn update_number(
    v: Value,
    diff: i64,
    accum: &mut i128,
    non_nulls: &mut i64,
) -> Result<(), EvalError> {
    todo!("update_number")
}
