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

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result as DataFusionResult;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use store_api::region_engine::PartitionRange;
use table::table::scan::RegionScanExec;

use crate::part_sort::PartSortExec;
use crate::window_sort::WindowedSortExec;

/// Optimize rule for windowed sort.
///
/// This is expected to run after [`ScanHint`] and [`ParallelizeScan`].
/// It would change the original sort to a custom plan. To make sure
/// other rules are applied correctly, this rule can be run as later as
/// possible.
///
/// [`ScanHint`]: crate::optimizer::scan_hint::ScanHintRule
/// [`ParallelizeScan`]: crate::optimizer::parallelize_scan::ParallelizeScan
pub struct WindowedSortPhysicalRule;

impl PhysicalOptimizerRule for WindowedSortPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Self::do_optimize(plan, config)
    }

    fn name(&self) -> &str {
        "WindowedSortRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

impl WindowedSortPhysicalRule {
    fn do_optimize(
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let result = plan
            .transform_down(|plan| {
                if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
                    // TODO: support multiple expr in windowed sort
                    if !sort_exec.preserve_partitioning() || sort_exec.expr().len() != 1 {
                        return Ok(Transformed::no(plan));
                    }

                    let Some(scanner_info) = fetch_partition_range(sort_exec.input().clone())?
                    else {
                        return Ok(Transformed::no(plan));
                    };

                    if let Some(first_sort_expr) = sort_exec.expr().first()
                        && !first_sort_expr.options.descending
                        && let Some(column_expr) = first_sort_expr
                            .expr
                            .as_any()
                            .downcast_ref::<PhysicalColumn>()
                        && column_expr.name() == scanner_info.time_index
                    {
                    } else {
                        return Ok(Transformed::no(plan));
                    }

                    let first_sort_expr = sort_exec.expr().first().unwrap().clone();
                    let part_sort_exec = Arc::new(PartSortExec::new(
                        first_sort_expr.clone(),
                        sort_exec.input().clone(),
                    ));
                    let windowed_sort_exec = WindowedSortExec::try_new(
                        first_sort_expr,
                        sort_exec.fetch(),
                        scanner_info.partition_ranges,
                        part_sort_exec,
                    )?;

                    return Ok(Transformed {
                        data: Arc::new(windowed_sort_exec),
                        transformed: true,
                        tnr: datafusion_common::tree_node::TreeNodeRecursion::Stop,
                    });
                }

                Ok(Transformed::no(plan))
            })?
            .data;

        Ok(result)
    }
}

struct ScannerInfo {
    partition_ranges: Vec<Vec<PartitionRange>>,
    time_index: String,
}

fn fetch_partition_range(input: Arc<dyn ExecutionPlan>) -> DataFusionResult<Option<ScannerInfo>> {
    let mut partition_ranges = None;
    let mut time_index = None;

    input.transform_up(|plan| {
        // Unappliable case, reset the state.
        if plan.as_any().is::<RepartitionExec>()
            || plan.as_any().is::<CoalesceBatchesExec>()
            || plan.as_any().is::<CoalescePartitionsExec>()
            || plan.as_any().is::<SortExec>()
            || plan.as_any().is::<WindowedSortExec>()
        {
            partition_ranges = None;
        }

        if let Some(region_scan_exec) = plan.as_any().downcast_ref::<RegionScanExec>() {
            partition_ranges = Some(region_scan_exec.get_uncollapsed_partition_ranges());
            time_index = region_scan_exec.time_index();
        }

        Ok(Transformed::no(plan))
    })?;

    let result = try {
        ScannerInfo {
            partition_ranges: partition_ranges?,
            time_index: time_index?,
        }
    };

    Ok(result)
}
