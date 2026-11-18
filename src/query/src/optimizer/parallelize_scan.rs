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
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result};
use store_api::region_engine::PartitionRange;
use table::table::scan::RegionScanExec;
pub use table::table::scan::assign_partition_ranges;

use crate::optimizer::global_topk::MAX_GLOBAL_TOPK_FETCH;

#[derive(Debug)]
pub struct ParallelizeScan;

impl PhysicalOptimizerRule for ParallelizeScan {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Self::do_optimize(plan, config)
    }

    fn name(&self) -> &str {
        "parallelize_scan"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl ParallelizeScan {
    fn do_optimize(
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut first_order_expr = None;
        // The fetch of a TopK that reads the scan through row-preserving operators only,
        // and whether one of those operators is a filter.
        let mut topk_fetch = None;
        let mut filtered = false;

        let result = plan
            .transform_down(|plan| {
                if let Some(sort_exec) = plan.downcast_ref::<SortExec>() {
                    // save the first order expr
                    first_order_expr = Some(sort_exec.expr().first()).cloned();
                    topk_fetch = sort_exec.fetch();
                    filtered = false;
                } else if plan.is::<FilterExec>() {
                    filtered = true;
                } else if !Self::preserves_rows(&plan) && !plan.is::<RegionScanExec>() {
                    topk_fetch = None;
                }
                if let Some(region_scan_exec) = plan.downcast_ref::<RegionScanExec>() {
                    let expected_partition_num = config.execution.target_partitions;
                    if region_scan_exec.is_partition_set()
                        || region_scan_exec.scanner_type().as_str() == "SinglePartition"
                    {
                        return Ok(Transformed::no(plan));
                    }

                    let ranges = region_scan_exec.get_partition_ranges();
                    let total_range_num = ranges.len();

                    // assign ranges to each partition
                    let mut partition_ranges = match &first_order_expr {
                        // A TopK stops reading a range once its threshold excludes the
                        // range's time span. Dealing ranges in time order makes the first
                        // range of every partition one of the newest (or oldest), so the
                        // threshold forms before older ranges are read. This only paid off
                        // together with the single heap of `GlobalTopKOverFilteredScan`,
                        // so it follows the same conditions.
                        Some(order_expr)
                            if filtered
                                && topk_fetch
                                    .is_some_and(|fetch| fetch <= MAX_GLOBAL_TOPK_FETCH) =>
                        {
                            assign_partition_ranges_by_time(
                                ranges,
                                expected_partition_num,
                                order_expr.options.descending,
                            )
                        }
                        _ => assign_partition_ranges(ranges, expected_partition_num),
                    };
                    debug!(
                        "Assign {total_range_num} ranges to {expected_partition_num} partitions"
                    );

                    // Sort the ranges in each partition based on the order expr
                    //
                    // This optimistically assumes that the first order expr is on the time index column
                    // to skip the validation of the order expr. As it's not harmful if this condition
                    // is not met.
                    if let Some(order_expr) = &first_order_expr
                        && order_expr.options.descending
                    {
                        for ranges in partition_ranges.iter_mut() {
                            // Primary: end descending (larger end first)
                            // Secondary: start descending (shorter range first when ends are equal)
                            ranges.sort_by(|a, b| {
                                b.end.cmp(&a.end).then_with(|| b.start.cmp(&a.start))
                            });
                        }
                    } else {
                        for ranges in partition_ranges.iter_mut() {
                            // Primary: start ascending (smaller start first)
                            // Secondary: end ascending (shorter range first when starts are equal)
                            ranges.sort_by(|a, b| {
                                a.start.cmp(&b.start).then_with(|| a.end.cmp(&b.end))
                            });
                        }
                    }

                    // update the partition ranges
                    let new_exec = region_scan_exec
                        .with_new_partitions(partition_ranges, expected_partition_num)
                        .map_err(|e| DataFusionError::External(e.into_inner()))?;
                    return Ok(Transformed::yes(Arc::new(new_exec)));
                }

                // The plan might be modified, but it's modified in-place so we always return
                // Transformed::no(plan) to indicate there is no "new child"
                Ok(Transformed::no(plan))
            })?
            .data;

        Ok(result)
    }

    /// Returns true if `plan` neither combines rows nor reorders them, so a TopK above
    /// it still selects among the scanned rows by the sort key.
    fn preserves_rows(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.is::<ProjectionExec>()
            || plan.is::<CoalescePartitionsExec>()
            || plan.is::<RepartitionExec>()
            || plan.is::<CooperativeExec>()
            || plan.is::<SortPreservingMergeExec>()
    }
}

/// Deals ranges to partitions round-robin in time order: newest first when
/// `descending`, oldest first otherwise.
fn assign_partition_ranges_by_time(
    mut ranges: Vec<PartitionRange>,
    expected_partition_num: usize,
    descending: bool,
) -> Vec<Vec<PartitionRange>> {
    if descending {
        ranges.sort_by(|a, b| b.end.cmp(&a.end).then_with(|| b.start.cmp(&a.start)));
    } else {
        ranges.sort_by(|a, b| a.start.cmp(&b.start).then_with(|| a.end.cmp(&b.end)));
    }
    let mut partition_ranges = vec![vec![]; expected_partition_num];
    for (i, range) in ranges.into_iter().enumerate() {
        partition_ranges[i % expected_partition_num].push(range);
    }
    partition_ranges
}

#[cfg(test)]
mod test {
    use common_time::Timestamp;
    use common_time::timestamp::TimeUnit;
    use store_api::region_engine::PartitionRange;

    use super::{assign_partition_ranges, assign_partition_ranges_by_time};

    #[test]
    fn test_assign_by_time_starts_every_partition_with_newest_ranges() {
        // Older ranges are larger, as compacted files usually are. Balancing by rows
        // would start some partitions with the oldest ranges.
        let ranges: Vec<_> = (0..8)
            .map(|i| PartitionRange {
                start: Timestamp::new(i * 10, TimeUnit::Second),
                end: Timestamp::new(i * 10 + 10, TimeUnit::Second),
                num_rows: (8 - i as usize) * 100,
                identifier: i as usize,
            })
            .collect();

        let firsts = |partitions: &Vec<Vec<PartitionRange>>| {
            let mut ids: Vec<_> = partitions.iter().map(|p| p[0].identifier).collect();
            ids.sort();
            ids
        };
        let by_time = assign_partition_ranges_by_time(ranges.clone(), 3, true);
        assert_eq!(vec![5, 6, 7], firsts(&by_time));
        let by_time = assign_partition_ranges_by_time(ranges.clone(), 3, false);
        assert_eq!(vec![0, 1, 2], firsts(&by_time));

        let by_rows = assign_partition_ranges(ranges, 3);
        assert_ne!(vec![5, 6, 7], firsts(&by_rows));
    }

    #[test]
    fn test_assign_unbalance_partition_range() {
        let ranges = vec![
            PartitionRange {
                start: Timestamp::new(0, TimeUnit::Second),
                end: Timestamp::new(10, TimeUnit::Second),
                num_rows: 100,
                identifier: 1,
            },
            PartitionRange {
                start: Timestamp::new(10, TimeUnit::Second),
                end: Timestamp::new(20, TimeUnit::Second),
                num_rows: 200,
                identifier: 2,
            },
            PartitionRange {
                start: Timestamp::new(20, TimeUnit::Second),
                end: Timestamp::new(30, TimeUnit::Second),
                num_rows: 150,
                identifier: 3,
            },
            PartitionRange {
                start: Timestamp::new(30, TimeUnit::Second),
                end: Timestamp::new(40, TimeUnit::Second),
                num_rows: 2500,
                identifier: 4,
            },
        ];

        // assign to 2 partitions
        let expected_partition_num = 2;
        let result = assign_partition_ranges(ranges.clone(), expected_partition_num);
        let expected = vec![
            vec![PartitionRange {
                start: Timestamp::new(30, TimeUnit::Second),
                end: Timestamp::new(40, TimeUnit::Second),
                num_rows: 2500,
                identifier: 4,
            }],
            vec![
                PartitionRange {
                    start: Timestamp::new(10, TimeUnit::Second),
                    end: Timestamp::new(20, TimeUnit::Second),
                    num_rows: 200,
                    identifier: 2,
                },
                PartitionRange {
                    start: Timestamp::new(20, TimeUnit::Second),
                    end: Timestamp::new(30, TimeUnit::Second),
                    num_rows: 150,
                    identifier: 3,
                },
                PartitionRange {
                    start: Timestamp::new(0, TimeUnit::Second),
                    end: Timestamp::new(10, TimeUnit::Second),
                    num_rows: 100,
                    identifier: 1,
                },
            ],
        ];
        assert_eq!(result, expected);
    }
}
