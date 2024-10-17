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
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result};
use store_api::region_engine::PartitionRange;
use table::table::scan::RegionScanExec;

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
        let result = plan
            .transform_down(|plan| {
                if let Some(region_scan_exec) = plan.as_any().downcast_ref::<RegionScanExec>() {
                    let ranges = region_scan_exec.get_partition_ranges();
                    let total_range_num = ranges.len();
                    let expected_partition_num = config.execution.target_partitions;

                    // assign ranges to each partition
                    let mut partition_ranges =
                        Self::assign_partition_range(ranges, expected_partition_num);
                    debug!(
                        "Assign {total_range_num} ranges to {expected_partition_num} partitions"
                    );

                    // sort the ranges in each partition
                    // TODO: smart sort!
                    for ranges in partition_ranges.iter_mut() {
                        ranges.sort_by(|a, b| a.start.cmp(&b.start));
                    }

                    // update the partition ranges
                    let new_exec = region_scan_exec
                        .with_new_partitions(partition_ranges)
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
    /// Currently we use a simple round-robin strategy to assign ranges to partitions.
    /// This method may return partitions with smaller number than `expected_partition_num`
    /// if the number of ranges is smaller than `expected_partition_num`. But this will
    /// return at least one partition.
    fn assign_partition_range(
        ranges: Vec<PartitionRange>,
        expected_partition_num: usize,
    ) -> Vec<Vec<PartitionRange>> {
        let actual_partition_num = expected_partition_num.min(ranges.len()).max(1);
        let mut partition_ranges = vec![vec![]; actual_partition_num];

        // round-robin assignment
        for (i, range) in ranges.into_iter().enumerate() {
            let partition_idx = i % expected_partition_num;
            partition_ranges[partition_idx].push(range);
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
                    start: Timestamp::new(0, TimeUnit::Second),
                    end: Timestamp::new(10, TimeUnit::Second),
                    num_rows: 100,
                    identifier: 1,
                },
                PartitionRange {
                    start: Timestamp::new(20, TimeUnit::Second),
                    end: Timestamp::new(30, TimeUnit::Second),
                    num_rows: 150,
                    identifier: 3,
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
                    start: Timestamp::new(30, TimeUnit::Second),
                    end: Timestamp::new(40, TimeUnit::Second),
                    num_rows: 250,
                    identifier: 4,
                },
            ],
        ];
        assert_eq!(result, expected);

        // assign 4 ranges to 5 partitions. Only 4 partitions are returned.
        let expected_partition_num = 5;
        let result = ParallelizeScan::assign_partition_range(ranges, expected_partition_num);
        let expected = vec![
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
            vec![PartitionRange {
                start: Timestamp::new(20, TimeUnit::Second),
                end: Timestamp::new(30, TimeUnit::Second),
                num_rows: 150,
                identifier: 3,
            }],
            vec![PartitionRange {
                start: Timestamp::new(30, TimeUnit::Second),
                end: Timestamp::new(40, TimeUnit::Second),
                num_rows: 250,
                identifier: 4,
            }],
        ];
        assert_eq!(result, expected);

        // assign 0 ranges to 5 partitions. Only 1 partition is returned.
        let result = ParallelizeScan::assign_partition_range(vec![], 5);
        assert_eq!(result.len(), 1);
    }
}
