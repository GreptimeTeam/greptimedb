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

use common_time::{Date, DateTime};
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{OrderedF32, OrderedF64, Value};
use serde::{Deserialize, Serialize};

use crate::expr::error::{EvalError, TryFromValueSnafu, TypeMismatchSnafu};
use crate::expr::relation::accum::{Accum, Accumulator};
use crate::repr::Diff;

/// Aggregate functions that can be applied to a group of rows.
///
/// `Mean` function is deliberately not included as it can be computed from `Sum` and `Count`, whose state can be better managed.
///
/// type of the input and output of the aggregate function:
///
/// `sum(i*)->i64, sum(u*)->u64`
///
/// `count()->i64`
///
/// `min/max(T)->T`
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
    MaxDateTime,
    MaxTimestamp,
    MaxTime,
    MaxDuration,
    MaxInterval,

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
    MinDateTime,
    MinTimestamp,
    MinTime,
    MinDuration,
    MinInterval,

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
    pub fn is_max(&self) -> bool {
        self.signature().generic_fn == GenericFn::Max
    }

    pub fn is_min(&self) -> bool {
        self.signature().generic_fn == GenericFn::Min
    }

    pub fn is_sum(&self) -> bool {
        self.signature().generic_fn == GenericFn::Sum
    }

    /// Eval value, diff with accumulator
    ///
    /// Expect self to be accumulable aggregate functio, i.e. sum/count
    ///
    /// TODO(discord9): deal with overflow&better accumulator
    pub fn eval_diff_accumulable<I>(
        &self,
        accum: Vec<Value>,
        value_diffs: I,
    ) -> Result<(Value, Vec<Value>), EvalError>
    where
        I: IntoIterator<Item = (Value, Diff)>,
    {
        let mut accum = if accum.is_empty() {
            Accum::new_accum(self)?
        } else {
            Accum::try_into_accum(self, accum)?
        };
        accum.update_batch(self, value_diffs)?;
        let res = accum.eval(self)?;
        Ok((res, accum.into_state()))
    }
}

pub struct Signature {
    pub input: ConcreteDataType,
    pub output: ConcreteDataType,
    pub generic_fn: GenericFn,
}

#[derive(Debug, PartialEq, Eq)]
pub enum GenericFn {
    Max,
    Min,
    Sum,
    Count,
    Any,
    All,
}

impl AggregateFunc {
    /// all concrete datatypes with precision types will be returned with largest possible variant
    /// as a exception, count have a signature of `null -> i64`, but it's actually `anytype -> i64`
    pub fn signature(&self) -> Signature {
        match self {
            AggregateFunc::MaxInt16 => Signature {
                input: ConcreteDataType::int16_datatype(),
                output: ConcreteDataType::int16_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxInt32 => Signature {
                input: ConcreteDataType::int32_datatype(),
                output: ConcreteDataType::int32_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxInt64 => Signature {
                input: ConcreteDataType::int64_datatype(),
                output: ConcreteDataType::int64_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxUInt16 => Signature {
                input: ConcreteDataType::uint16_datatype(),
                output: ConcreteDataType::uint16_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxUInt32 => Signature {
                input: ConcreteDataType::uint32_datatype(),
                output: ConcreteDataType::uint32_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxUInt64 => Signature {
                input: ConcreteDataType::uint64_datatype(),
                output: ConcreteDataType::uint64_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxFloat32 => Signature {
                input: ConcreteDataType::float32_datatype(),
                output: ConcreteDataType::float32_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxFloat64 => Signature {
                input: ConcreteDataType::float64_datatype(),
                output: ConcreteDataType::float64_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxBool => Signature {
                input: ConcreteDataType::boolean_datatype(),
                output: ConcreteDataType::boolean_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxString => Signature {
                input: ConcreteDataType::string_datatype(),
                output: ConcreteDataType::string_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxDate => Signature {
                input: ConcreteDataType::date_datatype(),
                output: ConcreteDataType::date_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxDateTime => Signature {
                input: ConcreteDataType::datetime_datatype(),
                output: ConcreteDataType::datetime_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxTimestamp => Signature {
                input: ConcreteDataType::timestamp_second_datatype(),
                output: ConcreteDataType::timestamp_second_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxTime => Signature {
                input: ConcreteDataType::time_second_datatype(),
                output: ConcreteDataType::time_second_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxDuration => Signature {
                input: ConcreteDataType::duration_second_datatype(),
                output: ConcreteDataType::duration_second_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MaxInterval => Signature {
                input: ConcreteDataType::interval_year_month_datatype(),
                output: ConcreteDataType::interval_year_month_datatype(),
                generic_fn: GenericFn::Max,
            },
            AggregateFunc::MinInt16 => Signature {
                input: ConcreteDataType::int16_datatype(),
                output: ConcreteDataType::int16_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinInt32 => Signature {
                input: ConcreteDataType::int32_datatype(),
                output: ConcreteDataType::int32_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinInt64 => Signature {
                input: ConcreteDataType::int64_datatype(),
                output: ConcreteDataType::int64_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinUInt16 => Signature {
                input: ConcreteDataType::uint16_datatype(),
                output: ConcreteDataType::uint16_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinUInt32 => Signature {
                input: ConcreteDataType::uint32_datatype(),
                output: ConcreteDataType::uint32_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinUInt64 => Signature {
                input: ConcreteDataType::uint64_datatype(),
                output: ConcreteDataType::uint64_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinFloat32 => Signature {
                input: ConcreteDataType::float32_datatype(),
                output: ConcreteDataType::float32_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinFloat64 => Signature {
                input: ConcreteDataType::float64_datatype(),
                output: ConcreteDataType::float64_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinBool => Signature {
                input: ConcreteDataType::boolean_datatype(),
                output: ConcreteDataType::boolean_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinString => Signature {
                input: ConcreteDataType::string_datatype(),
                output: ConcreteDataType::string_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinDate => Signature {
                input: ConcreteDataType::date_datatype(),
                output: ConcreteDataType::date_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinDateTime => Signature {
                input: ConcreteDataType::datetime_datatype(),
                output: ConcreteDataType::datetime_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinTimestamp => Signature {
                input: ConcreteDataType::timestamp_second_datatype(),
                output: ConcreteDataType::timestamp_second_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinTime => Signature {
                input: ConcreteDataType::time_second_datatype(),
                output: ConcreteDataType::time_second_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinDuration => Signature {
                input: ConcreteDataType::duration_second_datatype(),
                output: ConcreteDataType::duration_second_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::MinInterval => Signature {
                input: ConcreteDataType::interval_year_month_datatype(),
                output: ConcreteDataType::interval_year_month_datatype(),
                generic_fn: GenericFn::Min,
            },
            AggregateFunc::SumInt16 => Signature {
                input: ConcreteDataType::int16_datatype(),
                output: ConcreteDataType::int16_datatype(),
                generic_fn: GenericFn::Sum,
            },
            AggregateFunc::SumInt32 => Signature {
                input: ConcreteDataType::int32_datatype(),
                output: ConcreteDataType::int32_datatype(),
                generic_fn: GenericFn::Sum,
            },
            AggregateFunc::SumInt64 => Signature {
                input: ConcreteDataType::int64_datatype(),
                output: ConcreteDataType::int64_datatype(),
                generic_fn: GenericFn::Sum,
            },
            AggregateFunc::SumUInt16 => Signature {
                input: ConcreteDataType::uint16_datatype(),
                output: ConcreteDataType::uint16_datatype(),
                generic_fn: GenericFn::Sum,
            },
            AggregateFunc::SumUInt32 => Signature {
                input: ConcreteDataType::uint32_datatype(),
                output: ConcreteDataType::uint32_datatype(),
                generic_fn: GenericFn::Sum,
            },
            AggregateFunc::SumUInt64 => Signature {
                input: ConcreteDataType::uint64_datatype(),
                output: ConcreteDataType::uint64_datatype(),
                generic_fn: GenericFn::Sum,
            },
            AggregateFunc::SumFloat32 => Signature {
                input: ConcreteDataType::float32_datatype(),
                output: ConcreteDataType::float32_datatype(),
                generic_fn: GenericFn::Sum,
            },
            AggregateFunc::SumFloat64 => Signature {
                input: ConcreteDataType::float64_datatype(),
                output: ConcreteDataType::float64_datatype(),
                generic_fn: GenericFn::Sum,
            },
            AggregateFunc::Count => Signature {
                input: ConcreteDataType::null_datatype(),
                output: ConcreteDataType::int64_datatype(),
                generic_fn: GenericFn::Count,
            },
            AggregateFunc::Any => Signature {
                input: ConcreteDataType::boolean_datatype(),
                output: ConcreteDataType::boolean_datatype(),
                generic_fn: GenericFn::Any,
            },
            AggregateFunc::All => Signature {
                input: ConcreteDataType::boolean_datatype(),
                output: ConcreteDataType::boolean_datatype(),
                generic_fn: GenericFn::All,
            },
        }
    }
}
