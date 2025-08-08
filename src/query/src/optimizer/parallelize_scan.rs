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

use std::collections::BinaryHeap;
use std::sync::Arc;

use common_telemetry::debug;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result};
use store_api::region_engine::PartitionRange;
use table::table::scan::RegionScanExec;

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

        let result = plan
            .transform_down(|plan| {
                if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
                    // save the first order expr
                    first_order_expr = Some(sort_exec.expr().first()).cloned();
                } else if let Some(region_scan_exec) =
                    plan.as_any().downcast_ref::<RegionScanExec>()
                {
                    let expected_partition_num = config.execution.target_partitions;
                    if region_scan_exec.is_partition_set() {
                        return Ok(Transformed::no(plan));
                    }

                    let ranges = region_scan_exec.get_partition_ranges();
                    let total_range_num = ranges.len();

                    // assign ranges to each partition
                    let mut partition_ranges =
                        Self::assign_partition_range(ranges, expected_partition_num);
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
                            ranges.sort_by(|a, b| b.end.cmp(&a.end));
                        }
                    } else {
                        for ranges in partition_ranges.iter_mut() {
                            ranges.sort_by(|a, b| a.start.cmp(&b.start));
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

    /// Distribute [`PartitionRange`]s to each partition.
    ///
    /// Currently we assign ranges to partitions according to their rows so each partition
    /// has similar number of rows. This method always return `expected_partition_num` partitions.
    fn assign_partition_range(
        mut ranges: Vec<PartitionRange>,
        expected_partition_num: usize,
    ) -> Vec<Vec<PartitionRange>> {
        if ranges.is_empty() {
            // Returns a single partition with no range.
            return vec![vec![]; expected_partition_num];
        }

        if ranges.len() == 1 {
            let mut vec = vec![vec![]; expected_partition_num];
            vec[0] = ranges;
            return vec;
        }

        // Sort ranges by number of rows in descending order.
        ranges.sort_by(|a, b| b.num_rows.cmp(&a.num_rows));
        let mut partition_ranges = vec![vec![]; expected_partition_num];

        #[derive(Eq, PartialEq)]
        struct HeapNode {
            num_rows: usize,
            partition_idx: usize,
        }

        impl Ord for HeapNode {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                // Reverse for min-heap.
                self.num_rows.cmp(&other.num_rows).reverse()
            }
        }

        impl PartialOrd for HeapNode {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut part_heap =
            BinaryHeap::from_iter((0..expected_partition_num).map(|partition_idx| HeapNode {
                num_rows: 0,
                partition_idx,
            }));

        // Assigns the range to the partition with the smallest number of rows.
        for range in ranges {
            let mut node = part_heap.pop().unwrap();
            let partition_idx = node.partition_idx;
            node.num_rows += range.num_rows;
            partition_ranges[partition_idx].push(range);
            part_heap.push(node);
        }

        partition_ranges
    }
}

#[cfg(test)]
mod test {
    use common_time::timestamp::TimeUnit;
    use common_time::Timestamp;

    use super::*;

    #[test]
    fn test_assign_partition_range() {
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
                num_rows: 250,
                identifier: 4,
            },
        ];

        // assign to 2 partitions
        let expected_partition_num = 2;
        let result =
            ParallelizeScan::assign_partition_range(ranges.clone(), expected_partition_num);
        let expected = vec![
            vec![
                PartitionRange {
                    start: Timestamp::new(30, TimeUnit::Second),
                    end: Timestamp::new(40, TimeUnit::Second),
                    num_rows: 250,
                    identifier: 4,
                },
                PartitionRange {
                    start: Timestamp::new(0, TimeUnit::Second),
                    end: Timestamp::new(10, TimeUnit::Second),
                    num_rows: 100,
                    identifier: 1,
                },
            ],
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
            ],
        ];
        assert_eq!(result, expected);

        // assign 4 ranges to 5 partitions.
        let expected_partition_num = 5;
        let result = ParallelizeScan::assign_partition_range(ranges, expected_partition_num);
        let expected = vec![
            vec![PartitionRange {
                start: Timestamp::new(30, TimeUnit::Second),
                end: Timestamp::new(40, TimeUnit::Second),
                num_rows: 250,
                identifier: 4,
            }],
            vec![PartitionRange {
                start: Timestamp::new(0, TimeUnit::Second),
                end: Timestamp::new(10, TimeUnit::Second),
                num_rows: 100,
                identifier: 1,
            }],
            vec![PartitionRange {
                start: Timestamp::new(10, TimeUnit::Second),
                end: Timestamp::new(20, TimeUnit::Second),
                num_rows: 200,
                identifier: 2,
            }],
            vec![],
            vec![PartitionRange {
                start: Timestamp::new(20, TimeUnit::Second),
                end: Timestamp::new(30, TimeUnit::Second),
                num_rows: 150,
                identifier: 3,
            }],
        ];
        assert_eq!(result, expected);

        // assign 0 ranges to 5 partitions. Should return 5 empty ranges.
        let result = ParallelizeScan::assign_partition_range(vec![], 5);
        assert_eq!(result.len(), 5);
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
        let result =
            ParallelizeScan::assign_partition_range(ranges.clone(), expected_partition_num);
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
