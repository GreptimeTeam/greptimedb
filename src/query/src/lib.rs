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

use std::sync::Arc;

use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::Result as DfResult;
use datafusion_expr::LogicalPlan;

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
/// The depth counts both chains of relational inputs and plans embedded in
/// expressions (e.g. `IN (SELECT ...)`) at the node that carries them, because
/// the recursive rewrites this guards (`transform_with_subqueries` and the
/// per-subquery `PlanRewriter` runs) dive into embedded subqueries. Thus the
/// recursion depth of such a rewrite is the outer node depth plus the deepest
/// embedded subquery chain.
///
/// The plan is traversed iteratively so that measuring a deep plan cannot
/// overflow the stack.
pub(crate) fn plan_depth(plan: &LogicalPlan) -> DfResult<usize> {
    let mut max_depth = 0;
    let mut pending = Vec::new();
    measure_subtree(plan, 1, &mut max_depth, &mut pending)?;
    while let Some((subquery_plan, level)) = pending.pop() {
        measure_subtree(&subquery_plan, level, &mut max_depth, &mut pending)?;
    }
    Ok(max_depth)
}

/// Measures the subtree rooted at `root`, whose root node sits at `root_level`,
/// and collects embedded subquery plans into `pending`.
fn measure_subtree(
    root: &LogicalPlan,
    root_level: usize,
    max_depth: &mut usize,
    pending: &mut Vec<(Arc<LogicalPlan>, usize)>,
) -> DfResult<()> {
    let mut stack = vec![(root, root_level)];
    while let Some((node, level)) = stack.pop() {
        *max_depth = (*max_depth).max(level);
        node.apply_subqueries(|subquery_plan| {
            if let LogicalPlan::Subquery(subquery) = subquery_plan {
                // +1 for the `LogicalPlan::Subquery` wrapper the recursive
                // rewrites visit and +1 for the inner plan below it.
                pending.push((Arc::clone(&subquery.subquery), level + 2));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        for input in node.inputs() {
            stack.push((input, level + 1));
        }
    }
    Ok(())
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
