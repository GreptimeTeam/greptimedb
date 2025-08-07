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

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
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
#[derive(Debug)]
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
                    if sort_exec.expr().len() != 1 {
                        return Ok(Transformed::no(plan));
                    }

                    let preserve_partitioning = sort_exec.preserve_partitioning();

                    let sort_input = remove_repartition(sort_exec.input().clone())?.data;
                    let sort_input =
                        remove_coalesce_batches_exec(sort_input, sort_exec.fetch())?.data;

                    // Gets scanner info from the input without repartition before filter.
                    let Some(scanner_info) = fetch_partition_range(sort_input.clone())? else {
                        return Ok(Transformed::no(plan));
                    };
                    let input_schema = sort_input.schema();

                    let first_sort_expr = sort_exec.expr().first();
                    if let Some(column_expr) = first_sort_expr
                        .expr
                        .as_any()
                        .downcast_ref::<PhysicalColumn>()
                        && scanner_info
                            .time_index
                            .contains(input_schema.field(column_expr.index()).name())
                    {
                    } else {
                        return Ok(Transformed::no(plan));
                    }

                    // PartSortExec is unnecessary if:
                    // - there is no tag column, and
                    // - the sort is ascending on the time index column
                    let new_input = if scanner_info.tag_columns.is_empty()
                        && !first_sort_expr.options.descending
                    {
                        sort_input
                    } else {
                        Arc::new(PartSortExec::new(
                            first_sort_expr.clone(),
                            sort_exec.fetch(),
                            scanner_info.partition_ranges.clone(),
                            sort_input,
                        ))
                    };

                    let windowed_sort_exec = WindowedSortExec::try_new(
                        first_sort_expr.clone(),
                        sort_exec.fetch(),
                        scanner_info.partition_ranges,
                        new_input,
                    )?;

                    if !preserve_partitioning {
                        let order_preserving_merge = SortPreservingMergeExec::new(
                            sort_exec.expr().clone(),
                            Arc::new(windowed_sort_exec),
                        );
                        return Ok(Transformed {
                            data: Arc::new(order_preserving_merge),
                            transformed: true,
                            tnr: datafusion_common::tree_node::TreeNodeRecursion::Stop,
                        });
                    } else {
                        return Ok(Transformed {
                            data: Arc::new(windowed_sort_exec),
                            transformed: true,
                            tnr: datafusion_common::tree_node::TreeNodeRecursion::Stop,
                        });
                    }
                }

                Ok(Transformed::no(plan))
            })?
            .data;

        Ok(result)
    }
}

#[derive(Debug)]
struct ScannerInfo {
    partition_ranges: Vec<Vec<PartitionRange>>,
    time_index: HashSet<String>,
    tag_columns: Vec<String>,
}

fn fetch_partition_range(input: Arc<dyn ExecutionPlan>) -> DataFusionResult<Option<ScannerInfo>> {
    let mut partition_ranges = None;
    let mut time_index = HashSet::new();
    let mut alias_map = Vec::new();
    let mut tag_columns = None;
    let mut is_batch_coalesced = false;

    input.transform_up(|plan| {
        // Unappliable case, reset the state.
        if plan.as_any().is::<RepartitionExec>()
            || plan.as_any().is::<CoalescePartitionsExec>()
            || plan.as_any().is::<SortExec>()
            || plan.as_any().is::<WindowedSortExec>()
        {
            partition_ranges = None;
        }

        if plan.as_any().is::<CoalesceBatchesExec>() {
            is_batch_coalesced = true;
        }

        // only a very limited set of plans can exist between region scan and sort exec
        // other plans might make this optimize wrong, so be safe here by limiting it
        if !(plan.as_any().is::<ProjectionExec>()
            || plan.as_any().is::<FilterExec>()
            || plan.as_any().is::<CoalesceBatchesExec>())
        {
            partition_ranges = None;
        }

        // TODO(discord9): do this in logical plan instead as it's lessy bugy there
        // Collects alias of the time index column.
        if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            for (expr, output_name) in projection.expr() {
                if let Some(column_expr) = expr.as_any().downcast_ref::<PhysicalColumn>() {
                    alias_map.push((column_expr.name().to_string(), output_name.clone()));
                }
            }
            // resolve alias properly
            time_index = resolve_alias(&alias_map, &time_index);
        }

        if let Some(region_scan_exec) = plan.as_any().downcast_ref::<RegionScanExec>() {
            // `PerSeries` distribution is not supported in windowed sort.
            if region_scan_exec.distribution()
                == Some(store_api::storage::TimeSeriesDistribution::PerSeries)
            {
                partition_ranges = None;
                return Ok(Transformed::no(plan));
            }

            partition_ranges = Some(region_scan_exec.get_uncollapsed_partition_ranges());
            // Reset time index column.
            time_index = HashSet::from([region_scan_exec.time_index()]);
            tag_columns = Some(region_scan_exec.tag_columns());

            // set distinguish_partition_ranges to true, this is an incorrect workaround
            if !is_batch_coalesced {
                region_scan_exec.with_distinguish_partition_range(true);
            }
        }

        Ok(Transformed::no(plan))
    })?;

    let result = try {
        ScannerInfo {
            partition_ranges: partition_ranges?,
            time_index,
            tag_columns: tag_columns?,
        }
    };

    Ok(result)
}

/// Removes the repartition plan between the filter and region scan.
fn remove_repartition(
    plan: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Transformed<Arc<dyn ExecutionPlan>>> {
    plan.transform_down(|plan| {
        if plan.as_any().is::<FilterExec>() {
            // Checks child.
            let maybe_repartition = plan.children()[0];
            if maybe_repartition.as_any().is::<RepartitionExec>() {
                let maybe_scan = maybe_repartition.children()[0];
                if maybe_scan.as_any().is::<RegionScanExec>() {
                    let new_filter = plan.clone().with_new_children(vec![maybe_scan.clone()])?;
                    return Ok(Transformed::yes(new_filter));
                }
            }
        }

        Ok(Transformed::no(plan))
    })
}

/// Remove `CoalesceBatchesExec` if the limit is less than the batch size.
///
/// so that if limit is too small we can avoid need to scan for more rows than necessary
fn remove_coalesce_batches_exec(
    plan: Arc<dyn ExecutionPlan>,
    fetch: Option<usize>,
) -> DataFusionResult<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(fetch) = fetch else {
        return Ok(Transformed::no(plan));
    };

    // Avoid removing multiple coalesce batches
    let mut is_done = false;

    plan.transform_down(|plan| {
        if let Some(coalesce_batches_exec) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
            let target_batch_size = coalesce_batches_exec.target_batch_size();
            if fetch < target_batch_size && !is_done {
                is_done = true;
                return Ok(Transformed::yes(coalesce_batches_exec.input().clone()));
            }
        }

        Ok(Transformed::no(plan))
    })
}

/// Resolves alias of the time index column.
///
/// i.e if a is time index, alias= {a:b, b:c}, then result should be {a, b}(not {a, c}) because projection is not transitive
/// if alias={b:a} and a is time index, then return empty
fn resolve_alias(alias_map: &[(String, String)], time_index: &HashSet<String>) -> HashSet<String> {
    // available old name for time index
    let mut avail_old_name = time_index.clone();
    let mut new_time_index = HashSet::new();
    for (old, new) in alias_map {
        if time_index.contains(old) {
            new_time_index.insert(new.clone());
        } else if time_index.contains(new) && old != new {
            // other alias to time index, remove the old name
            avail_old_name.remove(new);
            continue;
        }
    }
    // add the remaining time index that is not in alias map
    new_time_index.extend(avail_old_name);
    new_time_index
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_alias() {
        let testcases = [
            // notice the old name is still in the result
            (
                vec![("a", "b"), ("b", "c")],
                HashSet::from(["a"]),
                HashSet::from(["a", "b"]),
            ),
            // alias swap
            (
                vec![("b", "a"), ("a", "b")],
                HashSet::from(["a"]),
                HashSet::from(["b"]),
            ),
            (
                vec![("b", "a"), ("b", "c")],
                HashSet::from(["a"]),
                HashSet::from([]),
            ),
            // not in alias map
            (
                vec![("c", "d"), ("d", "c")],
                HashSet::from(["a"]),
                HashSet::from(["a"]),
            ),
            // no alias
            (vec![], HashSet::from(["a"]), HashSet::from(["a"])),
            // empty time index
            (vec![], HashSet::from([]), HashSet::from([])),
        ];
        for (alias_map, time_index, expected) in testcases {
            let alias_map = alias_map
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect_vec();
            let time_index = time_index.into_iter().map(|i| i.to_string()).collect();
            let expected: HashSet<String> = expected.into_iter().map(|i| i.to_string()).collect();

            assert_eq!(
                expected,
                resolve_alias(&alias_map, &time_index),
                "alias_map={:?}, time_index={:?}",
                alias_map,
                time_index
            );
        }
    }
}
