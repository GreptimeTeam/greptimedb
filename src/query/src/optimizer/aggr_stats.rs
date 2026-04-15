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

use common_telemetry::debug;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion_common::Result as DfResult;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datatypes::arrow::datatypes::DataType;
use table::table::scan::RegionScanExec;

mod check;
mod split;
#[cfg(test)]
mod tests;

use check::RewriteCheck;

#[derive(Debug)]
pub struct AggregateStats;

/// All supported aggregate from statistics
#[derive(Debug, Clone, PartialEq, Eq)]
enum StatsAgg {
    CountStar,
    CountField {
        column_name: String,
        arg_type: DataType,
    },
    CountTimeIndex {
        arg_type: DataType,
    },
    MinField {
        column_name: String,
        arg_type: DataType,
    },
    MinTimeIndex {
        arg_type: DataType,
    },
    MaxField {
        column_name: String,
        arg_type: DataType,
    },
    MaxTimeIndex {
        arg_type: DataType,
    },
}

impl PhysicalOptimizerRule for AggregateStats {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Self::do_optimize(plan)
    }

    fn name(&self) -> &str {
        "aggregate_stats"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl AggregateStats {
    fn do_optimize(plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan
            .transform_down(|plan| {
                let Some(aggregate_exec) = plan.as_any().downcast_ref::<AggregateExec>() else {
                    return Ok(Transformed::no(plan));
                };

                let Some(region_scan) = Self::extract_region_scan(aggregate_exec) else {
                    return Ok(Transformed::no(plan));
                };

                let check = RewriteCheck::new(aggregate_exec, region_scan);
                if let Some(reason) = check.skip_reason()? {
                    debug!("Skip aggregate stats optimization: {reason}");
                    return Ok(Transformed::no(plan));
                }

                Ok(Transformed::no(plan))
            })?
            .data;

        Ok(result)
    }

    fn extract_region_scan(aggregate_exec: &AggregateExec) -> Option<&RegionScanExec> {
        let child = aggregate_exec.children().into_iter().next()?;
        child.as_any().downcast_ref::<RegionScanExec>()
    }
}
