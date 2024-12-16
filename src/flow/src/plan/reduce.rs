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

use crate::expr::{AggregateExpr, SafeMfpPlan, ScalarExpr};

/// Describe how to extract key-value pair from a `Row`
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct KeyValPlan {
    /// Extract key from row
    pub key_plan: SafeMfpPlan,
    /// Extract value from row
    pub val_plan: SafeMfpPlan,
}

impl KeyValPlan {
    /// Get nth expr using column ref
    pub fn get_nth_expr(&self, n: usize) -> Option<&ScalarExpr> {
        let key_len = self.key_plan.expressions.len();
        if n < key_len {
            return self.key_plan.expressions.get(n);
        } else {
            return self.val_plan.expressions.get(n - key_len);
        }
    }
}

/// TODO(discord9): def&impl of Hierarchical aggregates(for min/max with support to deletion) and
/// basic aggregates(for other aggregate functions) and mixed aggregate
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum ReducePlan {
    /// Plan for not computing any aggregations, just determining the set of
    /// distinct keys.
    Distinct,
    /// Plan for computing only accumulable aggregations.
    /// Including simple functions like `sum`, `count`, `min/max`(without deletion)
    Accumulable(AccumulablePlan),
}

/// Accumulable plan for the execution of a reduction.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct AccumulablePlan {
    /// All of the aggregations we were asked to compute, stored
    /// in order.
    pub full_aggrs: Vec<AggregateExpr>,
    /// All of the non-distinct accumulable aggregates.
    /// Each element represents:
    /// (index of aggr output, index of value among inputs, aggr expr)
    /// These will all be rendered together in one dataflow fragment.
    ///
    /// Invariant: the output index is the index of the aggregation in `full_aggrs`
    /// which means output index is always smaller than the length of `full_aggrs`
    pub simple_aggrs: Vec<AggrWithIndex>,
    /// Same as `simple_aggrs` but for all of the `DISTINCT` accumulable aggregations.
    pub distinct_aggrs: Vec<AggrWithIndex>,
}

/// Invariant: the output index is the index of the aggregation in `full_aggrs`
/// which means output index is always smaller than the length of `full_aggrs`
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct AggrWithIndex {
    /// aggregation expression
    pub expr: AggregateExpr,
    /// index of aggr input among input row
    pub input_idx: usize,
    /// index of aggr output among output row
    pub output_idx: usize,
}

impl AggrWithIndex {
    /// Create a new `AggrWithIndex`
    pub fn new(expr: AggregateExpr, input_idx: usize, output_idx: usize) -> Self {
        Self {
            expr,
            input_idx,
            output_idx,
        }
    }
}
