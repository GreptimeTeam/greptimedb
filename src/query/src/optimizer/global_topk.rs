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
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use table::table::scan::RegionScanExec;

use crate::optimizer::windowed_sort::is_time_index_expr;

/// Keeps one TopK heap for all partitions of a filtered region scan.
///
/// A partitioned TopK publishes its dynamic filter threshold only after one partition
/// alone has collected `fetch` rows. Under a selective filter every partition has to read
/// `fetch / selectivity` rows before that happens. A single heap fed by all partitions
/// fills after `fetch` matching rows in total, so the scan starts skipping older ranges
/// about `partitions` times earlier. Only the filtered rows go through the heap.
///
/// Runs after [`TopKFilterBatchSize`](super::topk_filter_batch::TopKFilterBatchSize),
/// which lets each filter pass on its matches without waiting for more.
#[derive(Debug)]
pub struct GlobalTopKOverFilteredScan;

/// Largest fetch that gets one heap. The heap runs on one thread: with a filter passing
/// 90% of 24M rows, it took about 1.6x as long as per-partition heaps at fetch 100000
/// and showed no loss at fetch 10000.
const MAX_GLOBAL_TOPK_FETCH: usize = 10_000;

impl PhysicalOptimizerRule for GlobalTopKOverFilteredScan {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(sort) = plan.downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(plan));
            };
            if !sort.preserve_partitioning()
                || sort.input().output_partitioning().partition_count() <= 1
                || filtered_scan_below_topk(sort)?.is_none()
            {
                return Ok(Transformed::no(plan));
            }
            let coalesced: Arc<dyn ExecutionPlan> =
                Arc::new(CoalescePartitionsExec::new(sort.input().clone()));
            // Both calls keep the dynamic filter expression already pushed into the scan.
            #[allow(deprecated)]
            let resorted = plan.clone().with_new_children(vec![coalesced])?;
            let global = resorted
                .downcast_ref::<SortExec>()
                .expect("with_new_children keeps the SortExec")
                .clone()
                .with_preserve_partitioning(false);
            Ok(Transformed::yes(Arc::new(global) as Arc<dyn ExecutionPlan>))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "GlobalTopKOverFilteredScan"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Returns the region scan that `sort` reads if `sort` is a TopK on the time index with
/// a fetch up to [`MAX_GLOBAL_TOPK_FETCH`], reading the scan through row-wise operators
/// that include at least one filter.
///
/// Without a filter every scanned row reaches the heaps, so each fills at once. On another
/// sort key the scan cannot skip ranges by time, so one heap only serializes the work.
pub(crate) fn filtered_scan_below_topk(sort: &SortExec) -> Result<Option<&RegionScanExec>> {
    if !sort
        .fetch()
        .is_some_and(|fetch| fetch <= MAX_GLOBAL_TOPK_FETCH)
        || !is_time_index_expr(sort.input(), &sort.expr().first().expr)?
    {
        return Ok(None);
    }
    let mut node = sort.input();
    let mut seen_filter = false;
    loop {
        if let Some(scan) = node.downcast_ref::<RegionScanExec>() {
            return Ok(seen_filter.then_some(scan));
        }
        seen_filter |= node.is::<FilterExec>();
        if !(node.is::<FilterExec>() || node.is::<ProjectionExec>() || node.is::<CooperativeExec>())
        {
            return Ok(None);
        }
        let Some(child) = node.children().into_iter().next() else {
            return Ok(None);
        };
        node = child;
    }
}

#[cfg(test)]
mod tests {
    use datafusion_physical_expr::expressions::{Column, IsNotNullExpr};
    use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};

    use super::*;
    use crate::optimizer::test_util::log_region_scan as region_scan;

    fn filter(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let predicate = Arc::new(IsNotNullExpr::new(Arc::new(Column::new("body", 0))));
        Arc::new(FilterExec::try_new(predicate, input).unwrap())
    }

    fn partitioned_topk(
        input: Arc<dyn ExecutionPlan>,
        sort_key: Column,
        fetch: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(sort_key))]).unwrap();
        Arc::new(
            SortExec::new(ordering, input)
                .with_fetch(Some(fetch))
                .with_preserve_partitioning(true),
        )
    }

    fn ts() -> Column {
        Column::new("ts", 1)
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        GlobalTopKOverFilteredScan
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    fn is_partitioned_topk(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<SortExec>()
            .unwrap()
            .preserve_partitioning()
    }

    #[test]
    fn test_filtered_scan_gets_one_heap() {
        let topk = partitioned_topk(filter(region_scan(4)), ts(), 100);
        let pushed_filter = topk.dynamic_expressions_produced()[0].clone();
        let plan = optimize(topk);

        let sort = plan.downcast_ref::<SortExec>().unwrap();
        assert!(!sort.preserve_partitioning());
        assert_eq!(Some(100), sort.fetch());
        // The scan holds the filter the old TopK produced; the new one must update it.
        assert!(Arc::ptr_eq(
            &pushed_filter,
            &sort.dynamic_expressions_produced()[0]
        ));
        let coalesce = sort.input();
        assert!(coalesce.is::<CoalescePartitionsExec>());
        assert_eq!(
            4,
            coalesce.children()[0]
                .output_partitioning()
                .partition_count()
        );
    }

    #[test]
    fn test_topk_keeps_partitions_when_one_heap_does_not_help() {
        // One thread would merge every filtered row of a weak filter.
        let plan = partitioned_topk(filter(region_scan(4)), ts(), MAX_GLOBAL_TOPK_FETCH + 1);
        assert!(is_partitioned_topk(&optimize(plan)));
        // Without a filter every scanned row reaches the heaps, so each fills at once.
        let plan = partitioned_topk(region_scan(4), ts(), 100);
        assert!(is_partitioned_topk(&optimize(plan)));
        // The scan cannot skip ranges on another sort key.
        let plan = partitioned_topk(filter(region_scan(4)), Column::new("body", 0), 100);
        assert!(is_partitioned_topk(&optimize(plan)));
    }
}
