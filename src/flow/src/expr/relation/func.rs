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
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::expr::error::{EvalError, TryFromValueSnafu, TypeMismatchSnafu};
use crate::expr::relation::accum::Accum;
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
        use AggregateFunc::*;
        matches!(
            self,
            MaxInt16
                | MaxInt32
                | MaxInt64
                | MaxUInt16
                | MaxUInt32
                | MaxUInt64
                | MaxFloat32
                | MaxFloat64
                | MaxBool
                | MaxString
                | MaxDate
                | MaxDateTime
                | MaxTimestamp
                | MaxTime
                | MaxDuration
                | MaxInterval
        )
    }

    pub fn is_min(&self) -> bool {
        use AggregateFunc::*;
        matches!(
            self,
            MinInt16
                | MinInt32
                | MinInt64
                | MinUInt16
                | MinUInt32
                | MinUInt64
                | MinFloat32
                | MinFloat64
                | MinBool
                | MinString
                | MinDate
                | MinDateTime
                | MinTimestamp
                | MinTime
                | MinDuration
                | MinInterval
        )
    }

    pub fn is_sum(&self) -> bool {
        use AggregateFunc::*;
        matches!(
            self,
            SumInt16
                | SumInt32
                | SumInt64
                | SumUInt16
                | SumUInt32
                | SumUInt64
                | SumFloat32
                | SumFloat64
        )
    }
}
