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

//! Describes an aggregation function and it's input expression.

pub(crate) use accum::{Accum, Accumulator};
pub(crate) use func::AggregateFunc;

use crate::expr::ScalarExpr;

mod accum;
mod func;

/// Describes an aggregation expression.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct AggregateExpr {
    /// Names the aggregation function.
    pub func: AggregateFunc,
    /// An expression which extracts from each row the input to `func`.
    /// TODO(discord9): currently unused in render phase(because AccumulablePlan remember each Aggr Expr's input/output column),
    /// so it only used in generate KeyValPlan from AggregateExpr
    pub expr: ScalarExpr,
    /// Should the aggregation be applied only to distinct results in each group.
    pub distinct: bool,
}
