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
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result as DfResult;
use datafusion_physical_expr::Distribution;
use store_api::storage::TimeSeriesDistribution;
use table::table::scan::RegionScanExec;

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
        let mut distribution_requirement = None;
        let result = plan.transform_down(|plan| {
            if let Some(distribution) = plan.required_input_distribution().first()
                && !matches!(distribution, Distribution::UnspecifiedDistribution)
                // incorrect workaround, doesn't fix the actual issue
                && plan.name() != "HashJoinExec"
            {
                distribution_requirement = Some(distribution.clone());
            }

            if let Some(merge_scan) = plan.as_any().downcast_ref::<MergeScanExec>()
                && let Some(distribution) = distribution_requirement.as_ref()
                && let Some(new_plan) = merge_scan.try_with_new_distribution(distribution.clone())
            {
                Ok(Transformed::yes(Arc::new(new_plan) as _))
            } else if let Some(region_scan) = plan.as_any().downcast_ref::<RegionScanExec>()
            && let Some(TimeSeriesDistribution::PerSeries) = region_scan.distribution() {
                common_telemetry::info!(
                    "[DEBUG] PassDistribution: pass distribution to RegionScanExec, distribution: PerSeries"
                );
                common_telemetry::info!(
                    "[DEBUG] PassDistribution: existing distribution: {:?}",
                    region_scan.properties().partitioning
                );
                Ok(Transformed::no(plan))
            } else{
                Ok(Transformed::no(plan))
            }
        })?;

        Ok(result.data)
    }
}
