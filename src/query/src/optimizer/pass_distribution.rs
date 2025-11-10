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

use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result as DfResult;
use datafusion_physical_expr::Distribution;

use crate::dist_plan::MergeScanExec;

/// This is a [`PhysicalOptimizerRule`] to pass distribution requirement to
/// [`MergeScanExec`] to avoid unnecessary shuffling.
///
/// This rule is expected to be run before [`EnforceDistribution`].
///
/// [`EnforceDistribution`]: datafusion::physical_optimizer::enforce_distribution::EnforceDistribution
/// [`MergeScanExec`]: crate::dist_plan::MergeScanExec
#[derive(Debug)]
pub struct PassDistribution;

impl PhysicalOptimizerRule for PassDistribution {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Self::do_optimize(plan, config)
    }

    fn name(&self) -> &str {
        "PassDistributionRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

impl PassDistribution {
    fn do_optimize(
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Start from root with no requirement
        Self::rewrite_with_distribution(plan, None)
    }

    /// Top-down rewrite that propagates distribution requirements to children.
    fn rewrite_with_distribution(
        plan: Arc<dyn ExecutionPlan>,
        current_req: Option<Distribution>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // If this is a MergeScanExec, try to apply the current requirement.
        if let Some(merge_scan) = plan.as_any().downcast_ref::<MergeScanExec>()
            && let Some(distribution) = current_req.as_ref()
            && let Some(new_plan) = merge_scan.try_with_new_distribution(distribution.clone())
        {
            // Leaf node; no children to process
            return Ok(Arc::new(new_plan) as _);
        }

        // Compute per-child requirements from the current node.
        let children = plan.children();
        if children.is_empty() {
            return Ok(plan);
        }

        let required = plan.required_input_distribution();
        let mut new_children = Vec::with_capacity(children.len());
        for (idx, child) in children.into_iter().enumerate() {
            let child_req = match required.get(idx) {
                Some(Distribution::UnspecifiedDistribution) => None,
                None => current_req.clone(),
                Some(req) => Some(req.clone()),
            };
            let new_child = Self::rewrite_with_distribution(child.clone(), child_req)?;
            new_children.push(new_child);
        }

        // Rebuild the node only if any child changed (pointer inequality)
        let unchanged = plan
            .children()
            .into_iter()
            .zip(new_children.iter())
            .all(|(old, new)| Arc::ptr_eq(old, new));
        if unchanged {
            Ok(plan)
        } else {
            plan.with_new_children(new_children)
        }
    }
}
