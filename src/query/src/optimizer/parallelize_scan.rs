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
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use store_api::region_engine::PartitionRange;
use table::table::scan::RegionScanExec;
pub use table::table::scan::assign_partition_ranges;

use crate::optimizer::global_topk::filtered_scan_below_topk;

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
        // Scans read by a TopK that keeps one heap, with whether the TopK is descending.
        let mut global_topk_scans = Vec::new();
        plan.apply(|node| {
            if let Some(sort) = node.downcast_ref::<SortExec>()
                && let Some(scan) =
                    filtered_scan_below_topk(sort, config.execution.target_partitions)?
            {
                global_topk_scans.push((
                    scan as *const RegionScanExec,
                    sort.expr().first().options.descending,
                ));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        let result = plan
            .transform_down(|plan| {
                if let Some(sort_exec) = plan.downcast_ref::<SortExec>() {
                    // save the first order expr
                    first_order_expr = Some(sort_exec.expr().first()).cloned();
                } else if let Some(region_scan_exec) = plan.downcast_ref::<RegionScanExec>() {
                    let expected_partition_num = config.execution.target_partitions;
                    if region_scan_exec.is_partition_set()
                        || region_scan_exec.scanner_type().as_str() == "SinglePartition"
                    {
                        return Ok(Transformed::no(plan));
                    }

                    let ranges = region_scan_exec.get_partition_ranges();
                    let total_range_num = ranges.len();

                    // assign ranges to each partition
                    let global_topk_descending = global_topk_scans
                        .iter()
                        .find(|(scan, _)| std::ptr::eq(*scan, region_scan_exec))
                        .map(|(_, descending)| *descending);
                    let mut partition_ranges = match global_topk_descending {
                        // The scan skips a range once the TopK threshold excludes the
                        // range's time span. Dealing ranges in time order makes the first
                        // range of every partition one of the newest (or oldest), so the
                        // threshold forms before older ranges are read.
                        Some(descending) => assign_partition_ranges_by_time(
                            ranges,
                            expected_partition_num,
                            descending,
                        ),
                        None => assign_partition_ranges(ranges, expected_partition_num),
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
}

/// Assigns ranges in time order, newest first when `descending`, each to the partition
/// with the fewest rows so far, then the fewest ranges. Every partition starts with one
/// of the newest ranges, and the rows stay balanced when range sizes vary. Ranges of
/// old files may report 0 rows, meaning unknown; the range count spreads those.
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
    let mut partition_rows = vec![0usize; expected_partition_num];
    for range in ranges {
        let target = (0..expected_partition_num)
            .min_by_key(|&i| (partition_rows[i], partition_ranges[i].len(), i))
            .unwrap();
        partition_rows[target] = partition_rows[target].saturating_add(range.num_rows);
        partition_ranges[target].push(range);
    }
    partition_ranges
}

#[cfg(test)]
mod test {
    use common_time::Timestamp;
    use common_time::timestamp::TimeUnit;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion_physical_expr::expressions::Column;
    use store_api::region_engine::PartitionRange;

    use super::*;
    use crate::optimizer::test_util::{body_is_not_null, sort_by, unassigned_log_region_scan};

    /// Consecutive ten-millisecond ranges with the given row counts, oldest first.
    fn time_ranges(rows: &[usize]) -> Vec<PartitionRange> {
        rows.iter()
            .enumerate()
            .map(|(i, &num_rows)| PartitionRange {
                start: Timestamp::new(i as i64 * 10, TimeUnit::Millisecond),
                end: Timestamp::new(i as i64 * 10 + 10, TimeUnit::Millisecond),
                num_rows,
                identifier: i,
            })
            .collect()
    }

    /// Older ranges are larger, as compacted files usually are, so assigning by rows
    /// alone starts a partition with an old range.
    const SHRINKING: [usize; 8] = [800, 700, 600, 500, 400, 300, 200, 100];

    #[test]
    fn test_assign_by_time() {
        let firsts = |partitions: Vec<Vec<PartitionRange>>| {
            let mut ids: Vec<_> = partitions.iter().map(|p| p[0].identifier).collect();
            ids.sort();
            ids
        };
        let assign = |rows: &[usize], partitions, descending| {
            assign_partition_ranges_by_time(time_ranges(rows), partitions, descending)
        };
        // Every partition starts with one of the newest (or oldest) ranges.
        assert_eq!(vec![5, 6, 7], firsts(assign(&SHRINKING, 3, true)));
        assert_eq!(vec![0, 1, 2], firsts(assign(&SHRINKING, 3, false)));
        // Rows stay balanced: when fewer than `fetch` rows match, every range is read.
        let rows: Vec<usize> = assign(&[1000, 1, 1000, 1], 2, false)
            .iter()
            .map(|p| p.iter().map(|r| r.num_rows).sum())
            .collect();
        assert_eq!(vec![1001, 1001], rows);
        // Ranges of old files report 0 rows, meaning unknown, and are spread by count.
        let counts: Vec<_> = assign(&[0, 0, 0, 0], 2, true)
            .iter()
            .map(Vec::len)
            .collect();
        assert_eq!(vec![2, 2], counts);
    }

    #[test]
    fn test_only_filtered_time_topk_scans_get_time_ordered_ranges() {
        let filter = body_is_not_null(unassigned_log_region_scan(time_ranges(&SHRINKING)));
        let topk = Arc::new(sort_by(filter, Column::new("ts", 1), true).with_fetch(Some(100)));
        // A sibling scan that no TopK reads must keep the row-balanced assignment.
        let plan = UnionExec::try_new(vec![
            topk,
            unassigned_log_region_scan(time_ranges(&SHRINKING)),
        ])
        .unwrap();

        let mut config = ConfigOptions::default();
        config.execution.target_partitions = 3;
        let plan = ParallelizeScan.optimize(plan, &config).unwrap();

        let mut assignments = vec![];
        plan.apply(|node| {
            if let Some(scan) = node.downcast_ref::<RegionScanExec>() {
                assignments.push(members(&scan.get_uncollapsed_partition_ranges()));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert_eq!(
            vec![
                members(&assign_partition_ranges_by_time(
                    time_ranges(&SHRINKING),
                    3,
                    true
                )),
                members(&assign_partition_ranges(time_ranges(&SHRINKING), 3)),
            ],
            assignments
        );
    }

    /// The range identifiers of each partition, ignoring order within and across
    /// partitions.
    fn members(partitions: &[Vec<PartitionRange>]) -> Vec<Vec<usize>> {
        let mut members: Vec<Vec<usize>> = partitions
            .iter()
            .map(|p| {
                let mut ids: Vec<_> = p.iter().map(|r| r.identifier).collect();
                ids.sort();
                ids
            })
            .collect();
        members.sort();
        members
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
