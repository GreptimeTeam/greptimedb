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
