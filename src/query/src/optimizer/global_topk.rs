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
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
};
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
            let Some(merge_fetch) = plan
                .downcast_ref::<SortPreservingMergeExec>()
                .and_then(|merge| merge.fetch())
            else {
                return Ok(Transformed::no(plan));
            };
            match one_heap_below_merge(plan.children()[0], merge_fetch)? {
                Some(input) => Ok(Transformed::yes(plan.clone().replace_children(
                    vec![input],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )?)),
                None => Ok(Transformed::no(plan)),
            }
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

/// Rewrites the partitioned TopK that a `SortPreservingMergeExec` with `merge_fetch` reads
/// through projections into one TopK over all partitions.
///
/// The partitioned TopK keeps up to `fetch` rows per partition, which equals the global
/// result only because the merge above it takes the first `merge_fetch` rows of all
/// partitions. One heap with `fetch >= merge_fetch` keeps every row the merge can return.
fn one_heap_below_merge(
    plan: &Arc<dyn ExecutionPlan>,
    merge_fetch: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if plan.is::<ProjectionExec>() {
        let Some(input) = one_heap_below_merge(plan.children()[0], merge_fetch)? else {
            return Ok(None);
        };
        return plan
            .clone()
            .replace_children(
                vec![input],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .map(Some);
    }
    let Some(sort) = plan.downcast_ref::<SortExec>() else {
        return Ok(None);
    };
    if !sort.preserve_partitioning()
        || sort.fetch().is_none_or(|fetch| fetch < merge_fetch)
        || sort.input().output_partitioning().partition_count() <= 1
        || filtered_scan_below_topk(sort)?.is_none()
    {
        return Ok(None);
    }
    let coalesced: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(sort.input().clone()));
    // Both calls keep the dynamic filter expression already pushed into the scan.
    // `with_preserve_partitioning` rebuilds its wrapper for one emitter, so the
    // threshold is published as soon as the single heap fills.
    let resorted = plan.clone().replace_children(
        vec![coalesced],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )?;
    let global = resorted
        .downcast_ref::<SortExec>()
        .expect("replace_children keeps the SortExec")
        .clone()
        .with_preserve_partitioning(false);
    Ok(Some(Arc::new(global)))
}

/// Returns the region scan that `sort` reads if `sort` is a TopK on the time index with
/// a fetch up to [`MAX_GLOBAL_TOPK_FETCH`], reading the scan through row-wise operators
/// that include at least one filter.
///
/// Without a filter every scanned row reaches the heaps, so each fills at once. On another
/// sort key the scan cannot skip ranges by time, so one heap only serializes the work. A
/// filter with a fetch keeps the first rows that pass, so reading ranges in another order
/// would change which rows it keeps.
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
        let is_filter = match node.downcast_ref::<FilterExec>() {
            Some(filter) if filter.fetch().is_some() => return Ok(None),
            Some(_) => true,
            None => false,
        };
        seen_filter |= is_filter;
        if !(is_filter || node.is::<ProjectionExec>() || node.is::<CooperativeExec>()) {
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
    use datafusion_physical_expr::expressions::Column;

    use super::*;
    use crate::optimizer::test_util::{body_is_not_null as filter, log_region_scan, sort_by};

    fn partitioned_topk(
        input: Arc<dyn ExecutionPlan>,
        sort_key: Column,
        fetch: usize,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            sort_by(input, sort_key, false)
                .with_fetch(Some(fetch))
                .with_preserve_partitioning(true),
        )
    }

    fn ts() -> Column {
        Column::new("ts", 1)
    }

    fn merged(topk: Arc<dyn ExecutionPlan>, fetch: usize) -> Arc<dyn ExecutionPlan> {
        let ordering = topk.output_ordering().unwrap().clone();
        Arc::new(SortPreservingMergeExec::new(ordering, topk).with_fetch(Some(fetch)))
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        GlobalTopKOverFilteredScan
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    /// Optimizes `topk` under a merge with the same fetch and returns the merge's input.
    fn optimize_merged(topk: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let fetch = topk.fetch().unwrap();
        optimize(merged(topk, fetch)).children()[0].clone()
    }

    fn is_partitioned_topk(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<SortExec>()
            .unwrap()
            .preserve_partitioning()
    }

    #[test]
    fn test_filtered_scan_gets_one_heap() {
        let topk = partitioned_topk(filter(log_region_scan(4)), ts(), 100);
        let pushed_filter = topk.dynamic_expressions_produced()[0].clone();
        let projection = Arc::new(
            ProjectionExec::try_new(vec![(Arc::new(ts()) as _, "ts".to_string())], topk).unwrap(),
        );
        let plan = optimize(merged(projection, 100));

        let projection = plan.children()[0];
        assert!(projection.is::<ProjectionExec>());
        let sort = projection.children()[0].downcast_ref::<SortExec>().unwrap();
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
    fn test_topk_keeps_partitions_without_a_merge_bounding_it() {
        // Without a merge each partition's rows are part of the result.
        let plan = partitioned_topk(filter(log_region_scan(4)), ts(), 100);
        assert!(is_partitioned_topk(&optimize(plan)));
        // A merge returning more rows than one heap keeps needs every partition's rows.
        let plan = merged(partitioned_topk(filter(log_region_scan(4)), ts(), 100), 200);
        assert!(is_partitioned_topk(optimize(plan).children()[0]));
    }

    #[test]
    fn test_topk_keeps_partitions_when_one_heap_does_not_help() {
        // One thread would merge every filtered row of a weak filter.
        let plan = partitioned_topk(filter(log_region_scan(4)), ts(), MAX_GLOBAL_TOPK_FETCH + 1);
        assert!(is_partitioned_topk(&optimize_merged(plan)));
        // Without a filter every scanned row reaches the heaps, so each fills at once.
        let plan = partitioned_topk(log_region_scan(4), ts(), 100);
        assert!(is_partitioned_topk(&optimize_merged(plan)));
        // The scan cannot skip ranges on another sort key.
        let plan = partitioned_topk(filter(log_region_scan(4)), Column::new("body", 0), 100);
        assert!(is_partitioned_topk(&optimize_merged(plan)));
        let limited = filter(log_region_scan(4)).with_fetch(Some(2)).unwrap();
        let plan = partitioned_topk(limited, ts(), 100);
        assert!(is_partitioned_topk(&optimize_merged(plan)));
    }
}
