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

use serde::{Deserialize, Serialize};

use crate::expr::{AggregateExpr, Id, LocalId, MapFilterProject, SafeMfpPlan, ScalarExpr};

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct KeyValPlan {
    pub key_plan: SafeMfpPlan,
    pub val_plan: SafeMfpPlan,
}

/// TODO(discord9): def&impl of Hierarchical aggregates(for min/max with support to deletion) and
/// basic aggregates(for other aggregate functions) and mixed aggregate
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub enum ReducePlan {
    /// Plan for not computing any aggregations, just determining the set of
    /// distinct keys.
    Distinct,
    /// Plan for computing only accumulable aggregations.
    /// Including simple functions like `sum`, `count`, `min/max`(without deletion)
    Accumulable(AccumulablePlan),
}

/// Accumulable plan for the execution of a reduction.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AccumulablePlan {
    /// All of the aggregations we were asked to compute, stored
    /// in order.
    pub full_aggrs: Vec<AggregateExpr>,
    /// All of the non-distinct accumulable aggregates.
    /// Each element represents:
    /// (index of aggr output, index of value among inputs, aggr expr)
    /// These will all be rendered together in one dataflow fragment.
    pub simple_aggrs: Vec<(usize, usize, AggregateExpr)>,
    /// Same as above but for all of the `DISTINCT` accumulable aggregations.
    pub distinct_aggrs: Vec<(usize, usize, AggregateExpr)>,
}
