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

use crate::expr::ScalarExpr;
use crate::plan::SafeMfpPlan;

/// TODO(discord9): consider impl more join strategies
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum JoinPlan {
    Linear(LinearJoinPlan),
}

/// Determine if a given row should stay in the output. And apply a map filter project before output the row
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct JoinFilter {
    /// each element in the outer vector will check if each expr in itself can be eval to same value
    /// if not, the row will be filtered out. Useful for equi-join(join based on equality of some columns)
    pub ready_equivalences: Vec<Vec<ScalarExpr>>,
    /// Apply a map filter project before output the row
    pub before: SafeMfpPlan,
}

/// A plan for the execution of a linear join.
///
/// A linear join is a sequence of stages, each of which introduces
/// a new collection. Each stage is represented by a [LinearStagePlan].
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct LinearJoinPlan {
    /// The source relation from which we start the join.
    pub source_relation: usize,
    /// The arrangement to use for the source relation, if any
    pub source_key: Option<Vec<ScalarExpr>>,
    /// An initial closure to apply before any stages.
    ///
    /// Values of `None` indicate the identity closure.
    pub initial_closure: Option<JoinFilter>,
    /// A *sequence* of stages to apply one after the other.
    pub stage_plans: Vec<LinearStagePlan>,
    /// A concluding filter to apply after the last stage.
    ///
    /// Values of `None` indicate the identity closure.
    pub final_closure: Option<JoinFilter>,
}

/// A plan for the execution of one stage of a linear join.
///
/// Each stage is a binary join between the current accumulated
/// join results, and a new collection. The former is referred to
/// as the "stream" and the latter the "lookup".
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct LinearStagePlan {
    /// The index of the relation into which we will look up.
    pub lookup_relation: usize,
    /// The key expressions to use for the stream relation.
    pub stream_key: Vec<ScalarExpr>,
    /// Columns to retain from the stream relation.
    /// These columns are those that are not redundant with `stream_key`,
    /// and cannot be read out of the key component of an arrangement.
    pub stream_thinning: Vec<usize>,
    /// The key expressions to use for the lookup relation.
    pub lookup_key: Vec<ScalarExpr>,
    /// The closure to apply to the concatenation of the key columns,
    /// the stream value columns, and the lookup value columns.
    pub closure: JoinFilter,
}
