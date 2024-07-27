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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::OnceLock;

use common_time::{Date, DateTime};
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{OrderedF32, OrderedF64, Value};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use snafu::{IntoError, OptionExt, ResultExt};
use strum::{EnumIter, IntoEnumIterator};

use crate::error::{DatafusionSnafu, Error, InvalidQuerySnafu};
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
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, EnumIter)]
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
    /// if this function is a `max`
    pub fn is_max(&self) -> bool {
        self.signature().generic_fn == GenericFn::Max
    }

    /// if this function is a `min`
    pub fn is_min(&self) -> bool {
        self.signature().generic_fn == GenericFn::Min
    }

    /// if this function is a `sum`
    pub fn is_sum(&self) -> bool {
        self.signature().generic_fn == GenericFn::Sum
    }

    /// Eval value, diff with accumulator
    ///
    /// Expect self to be accumulable aggregate function, i.e. sum/count
    ///
    /// TODO(discord9): deal with overflow&better accumulator
    pub fn eval_diff_accumulable<A, I>(
        &self,
        accum: A,
        value_diffs: I,
    ) -> Result<(Value, Vec<Value>), EvalError>
    where
        A: IntoIterator<Item = Value>,
        I: IntoIterator<Item = (Value, Diff)>,
    {
        let mut accum = accum.into_iter().peekable();

        let mut accum = if accum.peek().is_none() {
            Accum::new_accum(self)?
        } else {
            Accum::try_from_iter(self, &mut accum)?
        };
        accum.update_batch(self, value_diffs)?;
        let res = accum.eval(self)?;
        Ok((res, accum.into_state()))
    }
}

/// Generate signature for each aggregate function
macro_rules! generate_signature {
    ($value:ident,
        { $($user_arm:tt)* },
        [ $(
            $auto_arm:ident=>($($arg:ident),*)
            ),*
        ]
    ) => {
        match $value {
            $($user_arm)*,
            $(
                Self::$auto_arm => gen_one_siginature!($($arg),*),
            )*
        }
    };
}

/// Generate one match arm with optional arguments
macro_rules! gen_one_siginature {
    (
        $con_type:ident, $generic:ident
    ) => {
        Signature {
            input: smallvec![ConcreteDataType::$con_type(), ConcreteDataType::$con_type(),],
            output: ConcreteDataType::$con_type(),
            generic_fn: GenericFn::$generic,
        }
    };
    (
        $in_type:ident, $out_type:ident, $generic:ident
    ) => {
        Signature {
            input: smallvec![ConcreteDataType::$in_type()],
            output: ConcreteDataType::$out_type(),
            generic_fn: GenericFn::$generic,
        }
    };
}

static SPECIALIZATION: OnceLock<HashMap<(GenericFn, ConcreteDataType), AggregateFunc>> =
    OnceLock::new();

impl AggregateFunc {
    /// Create a `AggregateFunc` from a string of the function name and given argument type(optional)
    /// given an None type will be treated as null type,
    /// which in turn for AggregateFunc like `Count` will be treated as any type
    pub fn from_str_and_type(
        name: &str,
        arg_type: Option<ConcreteDataType>,
    ) -> Result<Self, Error> {
        let rule = SPECIALIZATION.get_or_init(|| {
            let mut spec = HashMap::new();
            for func in Self::iter() {
                let sig = func.signature();
                spec.insert((sig.generic_fn, sig.input[0].clone()), func);
            }
            spec
        });
        use datafusion_expr::aggregate_function::AggregateFunction as DfAggrFunc;
        let df_aggr_func = DfAggrFunc::from_str(name).or_else(|err| {
            if let datafusion_common::DataFusionError::NotImplemented(msg) = err {
                InvalidQuerySnafu {
                    reason: format!("Unsupported aggregate function: {}", msg),
                }
                .fail()
            } else {
                Err(DatafusionSnafu {
                    context: "Error when parsing aggregate function",
                }
                .into_error(err))
            }
        })?;

        let generic_fn = match df_aggr_func {
            DfAggrFunc::Max => GenericFn::Max,
            DfAggrFunc::Min => GenericFn::Min,
            DfAggrFunc::Sum => GenericFn::Sum,
            DfAggrFunc::Count => GenericFn::Count,
            DfAggrFunc::BoolOr => GenericFn::Any,
            DfAggrFunc::BoolAnd => GenericFn::All,
            _ => {
                return InvalidQuerySnafu {
                    reason: format!("Unknown aggregate function: {}", name),
                }
                .fail();
            }
        };
        let input_type = if matches!(generic_fn, GenericFn::Count) {
            ConcreteDataType::null_datatype()
        } else {
            arg_type.unwrap_or_else(ConcreteDataType::null_datatype)
        };
        rule.get(&(generic_fn, input_type.clone()))
            .cloned()
            .with_context(|| InvalidQuerySnafu {
                reason: format!(
                    "No specialization found for aggregate function {:?} with input type {:?}",
                    generic_fn, input_type
                ),
            })
    }

    /// all concrete datatypes with precision types will be returned with largest possible variant
    /// as a exception, count have a signature of `null -> i64`, but it's actually `anytype -> i64`
    ///
    /// TODO(discorcd9): fix signature for sum unsign -> u64 sum signed -> i64
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
            SumInt16 => (int16_datatype, int64_datatype, Sum),
            SumInt32 => (int32_datatype, int64_datatype, Sum),
            SumInt64 => (int64_datatype, int64_datatype, Sum),
            SumUInt16 => (uint16_datatype, uint64_datatype, Sum),
            SumUInt32 => (uint32_datatype, uint64_datatype, Sum),
            SumUInt64 => (uint64_datatype, uint64_datatype, Sum),
            SumFloat32 => (float32_datatype, Sum),
            SumFloat64 => (float64_datatype, Sum),
            Any => (boolean_datatype, Any),
            All => (boolean_datatype, All)
        ])
    }
}
