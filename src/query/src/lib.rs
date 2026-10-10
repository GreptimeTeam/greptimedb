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

mod analyze;
pub mod datafusion;
pub mod dist_plan;
pub mod dummy_catalog;
pub mod error;
pub mod executor;
pub mod log_query;
pub mod metrics;
pub mod optimizer;
pub mod options;
pub mod parser;
mod part_sort;
pub mod physical_wrapper;
pub mod plan;
pub mod planner;
pub mod promql;
pub mod query_engine;
mod range_select;
pub mod region_query;
pub mod sql;
pub(crate) mod window_sort;

/// The maximum depth (the longest chain of nodes) of a logical plan that the
/// recursive plan rewrites are allowed to walk with the worker thread stack.
///
/// Deeper plans are either rewritten with depth-safe fallbacks or rejected with
/// a controlled error instead of risking a stack overflow (see issue #9356).
pub(crate) const MAX_SAFE_PLAN_DEPTH: usize = 256;

/// Returns the depth of `plan` (a leaf plan has the depth 1).
///
/// The plan is traversed iteratively so that scanning a deep plan can not
/// overflow the stack.
pub(crate) fn plan_depth(plan: &datafusion_expr::LogicalPlan) -> usize {
    let mut depth = 0;
    let mut stack = vec![(plan, 1_usize)];
    while let Some((node, level)) = stack.pop() {
        depth = depth.max(level);
        for input in node.inputs() {
            stack.push((input, level + 1));
        }
    }
    depth
}

#[cfg(test)]
pub(crate) mod test_util;
#[cfg(test)]
mod tests;

pub use crate::analyze::{analyze_plan_metrics_to_json_value, dist_analyze_output_schema};
pub use crate::datafusion::DfContextProviderAdapter;
pub use crate::query_engine::{
    QueryEngine, QueryEngineContext, QueryEngineFactory, QueryEngineRef,
};
