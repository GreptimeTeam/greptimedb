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

use std::any::type_name;

use datatypes::prelude::ConcreteDataType;
use datatypes::value::{OrderedF32, OrderedF64, Value};
use serde::{Deserialize, Serialize};

use crate::expr::error::{EvalError, TryFromValueSnafu, TypeMismatchSnafu};
use crate::repr::Diff;

/// sum(i*)->i64, sum(u*)->u64
///
/// count()->i64
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub enum AggregateFunc {
    MaxInt16,
    MaxInt32,
    MaxInt64,
    MaxUInt16,
    MaxUInt32,
    MaxUInt64,
    MaxFloat32,
    MaxFloat64,
    MaxBool,
    MaxString,
    MaxDate,
    MaxTimestamp,
    MaxTimestampTz,
    MinInt16,
    MinInt32,
    MinInt64,
    MinUInt16,
    MinUInt32,
    MinUInt64,
    MinFloat32,
    MinFloat64,
    MinBool,
    MinString,
    MinDate,
    MinTimestamp,
    MinTimestampTz,
    SumInt16,
    SumInt32,
    SumInt64,
    SumUInt16,
    SumUInt32,
    SumUInt64,
    SumFloat32,
    SumFloat64,
    Count,
    Any,
    All,
}

impl AggregateFunc {
    pub fn eval<I>(&self, values: I) -> Result<Value, EvalError>
    where
        I: IntoIterator<Item = Value>,
    {
        // TODO(discord9): impl more functions like min/max/sumTimestamp etc.
        match self {
            AggregateFunc::MaxInt16 => max_value::<I, i16>(values),
            AggregateFunc::MaxInt32 => max_value::<I, i32>(values),
            AggregateFunc::MaxInt64 => max_value::<I, i64>(values),
            AggregateFunc::MaxUInt16 => max_value::<I, u16>(values),
            AggregateFunc::MaxUInt32 => max_value::<I, u32>(values),
            AggregateFunc::MaxUInt64 => max_value::<I, u64>(values),
            AggregateFunc::MaxFloat32 => max_value::<I, OrderedF32>(values),
            AggregateFunc::MaxFloat64 => max_value::<I, OrderedF64>(values),
            AggregateFunc::MaxBool => max_value::<I, bool>(values),
            AggregateFunc::MaxString => max_string(values),

            AggregateFunc::MinInt16 => min_value::<I, i16>(values),
            AggregateFunc::MinInt32 => min_value::<I, i32>(values),
            AggregateFunc::MinInt64 => min_value::<I, i64>(values),
            AggregateFunc::MinUInt16 => min_value::<I, u16>(values),
            AggregateFunc::MinUInt32 => min_value::<I, u32>(values),
            AggregateFunc::MinUInt64 => min_value::<I, u16>(values),
            AggregateFunc::MinFloat32 => min_value::<I, OrderedF32>(values),
            AggregateFunc::MinFloat64 => min_value::<I, OrderedF64>(values),
            AggregateFunc::MinBool => min_value::<I, bool>(values),
            AggregateFunc::MinString => min_string(values),

            AggregateFunc::SumInt16 => sum_value::<I, i16, i64>(values),
            AggregateFunc::SumInt32 => sum_value::<I, i32, i64>(values),
            AggregateFunc::SumInt64 => sum_value::<I, i64, i64>(values),
            AggregateFunc::SumUInt16 => sum_value::<I, u16, u64>(values),
            AggregateFunc::SumUInt32 => sum_value::<I, u32, u64>(values),
            AggregateFunc::SumUInt64 => sum_value::<I, u64, u64>(values),
            AggregateFunc::SumFloat32 => sum_value::<I, f32, f32>(values),
            AggregateFunc::SumFloat64 => sum_value::<I, f64, f64>(values),

            AggregateFunc::Count => count(values),
            AggregateFunc::All => all(values),
            AggregateFunc::Any => any(values),
            _ => todo!(),
        }
    }

    /// Eval value, diff with accum
    ///
    /// TODO(discord9): deal with overflow
    pub fn eval_diff_accum<I>(
        &self,
        accum: Option<Value>,
        value_diffs: I,
    ) -> Result<Value, EvalError>
    where
        I: IntoIterator<Item = (Value, Diff)>,
    {
        // TODO(discord9): sum
        match self {
            AggregateFunc::SumInt16 | AggregateFunc::SumInt32 | AggregateFunc::SumInt64 => {
                let accum = if let Some(accum) = accum {
                    accum.as_value_ref().as_i64().map_err(|err| {
                        TypeMismatchSnafu {
                            expected: ConcreteDataType::int64_datatype(),
                            actual: accum.data_type(),
                        }
                        .build()
                    })?
                } else {
                    None
                };
                match self {
                    AggregateFunc::SumInt16 => sum_accum_diffs::<I, i16, i64>(accum, value_diffs),
                    AggregateFunc::SumInt32 => sum_accum_diffs::<I, i32, i64>(accum, value_diffs),
                    AggregateFunc::SumInt64 => sum_accum_diffs::<I, i64, i64>(accum, value_diffs),

                    _ => unreachable!(),
                }
            }
            AggregateFunc::SumUInt16 | AggregateFunc::SumUInt32 | AggregateFunc::SumUInt64 => {
                let accum = if let Some(accum) = accum {
                    accum.as_value_ref().as_u64().map_err(|err| {
                        TypeMismatchSnafu {
                            expected: ConcreteDataType::uint64_datatype(),
                            actual: accum.data_type(),
                        }
                        .build()
                    })?
                } else {
                    None
                };
                match self {
                    AggregateFunc::SumUInt16 => sum_accum_diffs::<I, u16, u64>(accum, value_diffs),
                    AggregateFunc::SumUInt32 => sum_accum_diffs::<I, u32, u64>(accum, value_diffs),
                    AggregateFunc::SumUInt64 => sum_accum_diffs::<I, u64, u64>(accum, value_diffs),
                    _ => unreachable!(),
                }
            }
            AggregateFunc::SumFloat32 => {
                let accum = if let Some(accum) = accum {
                    accum.as_value_ref().as_f32().map_err(|err| {
                        TypeMismatchSnafu {
                            expected: ConcreteDataType::float32_datatype(),
                            actual: accum.data_type(),
                        }
                        .build()
                    })?
                } else {
                    None
                };
                sum_accum_diffs::<I, f32, f32>(accum, value_diffs)
            }
            AggregateFunc::SumFloat64 => {
                let accum = if let Some(accum) = accum {
                    accum.as_value_ref().as_f64().map_err(|err| {
                        TypeMismatchSnafu {
                            expected: ConcreteDataType::float64_datatype(),
                            actual: accum.data_type(),
                        }
                        .build()
                    })?
                } else {
                    None
                };
                sum_accum_diffs::<I, f64, f64>(accum, value_diffs)
            }

            AggregateFunc::Count => count_accum_diff(accum, value_diffs),
            _ => {
                let values_only = value_diffs.into_iter().map(|e| e.0);
                self.eval(values_only)
            }
        }
    }
}

/// TODO(discord9): deal with overflow
fn sum_accum_diffs<I, ValueType, ResultType>(
    accum: Option<ResultType>,
    value_diffs: I,
) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = (Value, Diff)>,
    ValueType: TryFrom<Value>,
    <ValueType as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<Option<ValueType>>,
    ResultType: From<ValueType>
        + std::iter::Sum
        + Into<Value>
        + std::ops::Add<Output = ResultType>
        + std::ops::Sub<Output = ResultType>
        + std::default::Default,
{
    // If no row qualifies, then the result of COUNT is 0 (zero), and the result of any other aggregate function is the null value.
    let mut values = value_diffs
        .into_iter()
        .filter(|v| !v.0.is_null())
        .peekable();
    let ret = if values.peek().is_none() {
        Value::Null
    } else {
        let res_ty_lst = values
            .map(|(v, d)| {
                ValueType::try_from(v)
                    .map(|v| (ResultType::from(v), d))
                    .map_err(|err| {
                        TryFromValueSnafu {
                            msg: format!("type: {}, msg: {:?}", type_name::<ValueType>(), err),
                        }
                        .build()
                    })
            })
            .collect::<Result<Vec<_>, EvalError>>()?
            .into_iter();
        let x = res_ty_lst
            .fold(accum, |state, next| match (state, next) {
                (Some(state), next) => Some(match next.1 {
                    1 => state + next.0,
                    -1 => state - next.0,
                    _ => unreachable!("multicity of diff not supported"),
                }),
                (None, next) => Some(match next.1 {
                    1 => next.0,
                    -1 => ResultType::default() - next.0,
                    _ => unreachable!("multicity of diff not supported"),
                }),
            })
            .expect("not all values are null");// invariant: values are not null after filter
        x.into()
    };
    Ok(ret)
}

fn count_accum_diff<I>(accum: Option<Value>, value_diffs: I) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = (Value, Diff)>,
{
    let mut accum = if let Some(accum) = accum {
        // invariant: accum is i64
        accum
            .as_value_ref()
            .as_i64()
            .map_err(|err| {
                TryFromValueSnafu {
                    msg: format!("type: i64, msg: {:?}", err),
                }
                .build()
            })?
            .expect("Non Null accumulator")
    } else {
        0
    };
    for (value, diff) in value_diffs {
        match diff {
            1 => accum += 1,
            -1 => accum -= 1,
            _ => unreachable!("multicity of diff not supported"),
        }
    }
    Ok(Value::from(accum))
}

fn max_string<I>(values: I) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = Value>,
{
    let ret_err = || {
        TryFromValueSnafu {
            msg: "String".to_string(),
        }
        .build()
    };
    let str_list = values
        .into_iter()
        .map(|v| v.as_string().ok_or_else(ret_err))
        .collect::<Result<Vec<_>, EvalError>>()?;
    let ret = match str_list.into_iter().max_by(|a, b| a.cmp(b)) {
        Some(v) => Value::from(v),
        None => Value::Null,
    };
    Ok(ret)
}

fn max_value<I, TypedValue>(values: I) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = Value>,
    TypedValue: TryFrom<Value> + Ord,
    <TypedValue as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<Option<TypedValue>>,
{
    let mut x: Option<TypedValue> = None;
    for value in values.into_iter() {
        if value.is_null() {
            continue;
        }
        let v = TypedValue::try_from(value).map_err(|err| {
            TryFromValueSnafu {
                msg: format!("type: {}, msg: {:?}", type_name::<TypedValue>(), err),
            }
            .build()
        })?;
        x = x.map(|x| x.max(v));
    }
    Ok(x.into())
}

fn min_string<I>(values: I) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = Value>,
{
    let ret_err = || {
        TryFromValueSnafu {
            msg: "String".to_string(),
        }
        .build()
    };
    let str_list = values
        .into_iter()
        .map(|v| v.as_string().ok_or_else(ret_err))
        .collect::<Result<Vec<_>, EvalError>>()?;
    let ret = match str_list.into_iter().min_by(|a, b| a.cmp(b)) {
        Some(v) => Value::from(v),
        None => Value::Null,
    };
    Ok(ret)
}

fn min_value<I, TypedValue>(values: I) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = Value>,
    TypedValue: TryFrom<Value> + Ord,
    <TypedValue as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<Option<TypedValue>>,
{
    let mut x: Option<TypedValue> = None;
    for value in values.into_iter() {
        if value.is_null() {
            continue;
        }
        let v = TypedValue::try_from(value).map_err(|err| {
            TryFromValueSnafu {
                msg: format!("type: {}, msg: {:?}", type_name::<TypedValue>(), err),
            }
            .build()
        })?;
        x = x.map(|x| x.min(v));
    }
    Ok(x.into())
}

fn sum_value<I, ValueType, ResultType>(values: I) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = Value>,
    ValueType: TryFrom<Value>,
    <ValueType as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<Option<ValueType>>,
    ResultType: From<ValueType> + std::iter::Sum + Into<Value>,
{
    // If no row qualifies, then the result of COUNT is 0 (zero), and the result of any other aggregate function is the null value.
    let mut values = values.into_iter().filter(|v| !v.is_null()).peekable();
    let ret = if values.peek().is_none() {
        Value::Null
    } else {
        let x: ResultType = values
            .map(|v| {
                ValueType::try_from(v)
                    .map(|v| ResultType::from(v))
                    .map_err(|err| {
                        TryFromValueSnafu {
                            msg: format!("type: {}, msg: {:?}", type_name::<ResultType>(), err),
                        }
                        .build()
                    })
            })
            .sum::<Result<ResultType, EvalError>>()?;
        x.into()
    };
    Ok(ret)
}

fn count<I>(values: I) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = Value>,
{
    let x = values.into_iter().filter(|v| !v.is_null()).count() as i64;
    Ok(Value::from(x))
}

fn any<I>(datums: I) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = Value>,
{
    Ok(datums
        .into_iter()
        .fold(Value::Boolean(false), |state, next| match (state, next) {
            (Value::Boolean(true), _) | (_, Value::Boolean(true)) => Value::Boolean(true),
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            _ => Value::Boolean(false),
        }))
}

fn all<I>(datums: I) -> Result<Value, EvalError>
where
    I: IntoIterator<Item = Value>,
{
    Ok(datums
        .into_iter()
        .fold(Value::Boolean(true), |state, next| match (state, next) {
            (Value::Boolean(false), _) | (_, Value::Boolean(false)) => Value::Boolean(false),
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            _ => Value::Boolean(true),
        }))
}
