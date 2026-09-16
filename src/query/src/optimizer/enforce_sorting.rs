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

//! Sorting enforcement that runs after GreptimeDB's custom physical rules.
//!
//! DataFusion 55 moved the standalone `EnforceSorting` phases into
//! `EnsureRequirements`. GreptimeDB still needs to rerun those phases after
//! custom rules modify scan partitioning and distribution.

use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_sorting::replace_with_order_preserving_variants::{
    OrderPreservationContext, replace_with_order_preserving_variants,
};
use datafusion::physical_optimizer::enforce_sorting::sort_pushdown::{
    SortPushDown, assign_initial_requirements, pushdown_sorts,
};
use datafusion::physical_optimizer::enforce_sorting::{
    PlanWithCorrespondingCoalescePartitions, PlanWithCorrespondingSort, ensure_sorting,
    parallelize_sorts, replace_with_partial_sort,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};

/// Runs the standalone sorting-enforcement pipeline removed in DataFusion 55.
#[derive(Debug)]
pub struct EnforceSorting;

impl PhysicalOptimizerRule for EnforceSorting {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Phase 1: ensure sorting requirements and remove redundant sorts.
        let sorting = PlanWithCorrespondingSort::new_default(plan);
        let sorting = sorting.transform_up(ensure_sorting)?.data;

        // Phase 2: optionally turn CoalescePartitions + Sort into parallel
        // sorts followed by a SortPreservingMerge.
        let plan = if config.optimizer.repartition_sorts {
            let parallel = PlanWithCorrespondingCoalescePartitions::new_default(sorting.plan)
                .transform_up(parallelize_sorts)
                .data()?;
            parallel.plan
        } else {
            sorting.plan
        };

        // Phase 3: use order-preserving executor variants where appropriate.
        let variants = OrderPreservationContext::new_default(plan);
        let variants = variants
            .transform_up(|context| {
                replace_with_order_preserving_variants(context, false, true, config)
            })
            .data()?;

        // Phase 4: push sorts down through order-preserving operators.
        let mut pushdown = SortPushDown::new_default(variants.plan);
        assign_initial_requirements(&mut pushdown);
        let pushed = pushdown_sorts(pushdown)?;

        // Phase 5: exploit an already-satisfied prefix on unbounded inputs.
        pushed
            .plan
            .transform_up(|plan| Ok(Transformed::yes(replace_with_partial_sort(plan)?)))
            .data()
    }

    fn name(&self) -> &str {
        "EnforceSorting"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
