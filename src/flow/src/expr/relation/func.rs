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
use smallvec::smallvec;

use crate::expr::error::{EvalError, TryFromValueSnafu, TypeMismatchSnafu};
use crate::expr::relation::accum::{Accum, Accumulator};
use crate::expr::signature::{GenericFn, Signature};
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

macro_rules! generate_signature {
    ($value:ident, { $($user_arm:tt)* },
    [ $(
        $auto_arm:ident=>($con_type:ident,$generic:ident)
        ),*
    ]) => {
        match $value {
            $($user_arm)*,
            $(
                Self::$auto_arm => Signature {
                    input: smallvec![
                        ConcreteDataType::$con_type(),
                        ConcreteDataType::$con_type(),
                    ],
                    output: ConcreteDataType::$con_type(),
                    generic_fn: GenericFn::$generic,
                },
            )*
        }
    };
}

impl AggregateFunc {
    /// all concrete datatypes with precision types will be returned with largest possible variant
    /// as a exception, count have a signature of `null -> i64`, but it's actually `anytype -> i64`
    pub fn signature(&self) -> Signature {
        generate_signature!(self, {
            AggregateFunc::Count => Signature {
                input: smallvec![ConcreteDataType::null_datatype()],
                output: ConcreteDataType::int64_datatype(),
                generic_fn: GenericFn::Count,
            }
        },[
            MaxInt16 => (int16_datatype, Max),
            MaxInt32 => (int32_datatype, Max),
            MaxInt64 => (int64_datatype, Max),
            MaxUInt16 => (uint16_datatype, Max),
            MaxUInt32 => (uint32_datatype, Max),
            MaxUInt64 => (uint64_datatype, Max),
            MaxFloat32 => (float32_datatype, Max),
            MaxFloat64 => (float64_datatype, Max),
            MaxBool => (boolean_datatype, Max),
            MaxString => (string_datatype, Max),
            MaxDate => (date_datatype, Max),
            MaxDateTime => (datetime_datatype, Max),
            MaxTimestamp => (timestamp_second_datatype, Max),
            MaxTime => (time_second_datatype, Max),
            MaxDuration => (duration_second_datatype, Max),
            MaxInterval => (interval_year_month_datatype, Max),
            MinInt16 => (int16_datatype, Min),
            MinInt32 => (int32_datatype, Min),
            MinInt64 => (int64_datatype, Min),
            MinUInt16 => (uint16_datatype, Min),
            MinUInt32 => (uint32_datatype, Min),
            MinUInt64 => (uint64_datatype, Min),
            MinFloat32 => (float32_datatype, Min),
            MinFloat64 => (float64_datatype, Min),
            MinBool => (boolean_datatype, Min),
            MinString => (string_datatype, Min),
            MinDate => (date_datatype, Min),
            MinDateTime => (datetime_datatype, Min),
            MinTimestamp => (timestamp_second_datatype, Min),
            MinTime => (time_second_datatype, Min),
            MinDuration => (duration_second_datatype, Min),
            MinInterval => (interval_year_month_datatype, Min),
            SumInt16 => (int16_datatype, Sum),
            SumInt32 => (int32_datatype, Sum),
            SumInt64 => (int64_datatype, Sum),
            SumUInt16 => (uint16_datatype, Sum),
            SumUInt32 => (uint32_datatype, Sum),
            SumUInt64 => (uint64_datatype, Sum),
            SumFloat32 => (float32_datatype, Sum),
            SumFloat64 => (float64_datatype, Sum),
            Any => (boolean_datatype, Any),
            All => (boolean_datatype, All)
        ])
    }
}
