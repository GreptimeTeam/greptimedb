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

/// Keeps one TopK heap for all partitions of a filtered region scan.
///
/// A partitioned TopK publishes its dynamic filter threshold only after one partition
/// alone has collected `fetch` rows. Under a selective filter every partition has to read
/// `fetch / selectivity` rows before that happens. A single heap fed by all partitions
/// fills after `fetch` matching rows in total, so the scan starts skipping older ranges
/// about `partitions` times earlier. Only the filtered rows go through the heap.
///
/// The single heap runs on one thread, so the rule only applies to fetches up to
/// [`MAX_GLOBAL_TOPK_FETCH`].
#[derive(Debug)]
pub struct GlobalTopKOverFilteredScan;

/// Largest fetch that gets one heap. With a filter passing 90% of 24M rows, a single
/// heap took about 1.6x as long as per-partition heaps at fetch 100000 and showed no
/// loss at fetch 10000.
pub(crate) const MAX_GLOBAL_TOPK_FETCH: usize = 10_000;

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
            let input = sort.input();
            if !sort
                .fetch()
                .is_some_and(|fetch| fetch <= MAX_GLOBAL_TOPK_FETCH)
                || !sort.preserve_partitioning()
                || input.output_partitioning().partition_count() <= 1
                || !filters_region_scan(input, false)
            {
                return Ok(Transformed::no(plan));
            }
            // Every partition now feeds the same heap, so a partition should not wait for
            // `fetch` matches of its own before passing them on. A batch size of 1 passes
            // each filtered batch through whole instead of splitting it.
            let input = flush_filters_per_batch(input.clone())?;
            let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(input));
            #[allow(deprecated)]
            let resorted = plan.clone().with_new_children(vec![coalesced])?;
            // `with_fetch` keeps the dynamic filter expression already pushed into the scan.
            let global = resorted
                .downcast_ref::<SortExec>()
                .expect("with_new_children keeps the SortExec")
                .with_fetch(sort.fetch())
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

fn flush_filters_per_batch(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.is::<RegionScanExec>() {
        return Ok(plan);
    }
    let child = flush_filters_per_batch(plan.children()[0].clone())?;
    if plan.is::<FilterExec>() {
        #[allow(deprecated)]
        let filter = plan.clone().with_new_children(vec![child])?;
        let filter = filter
            .downcast_ref::<FilterExec>()
            .expect("with_new_children keeps the FilterExec")
            .with_batch_size(1)?;
        return Ok(Arc::new(filter));
    }
    #[allow(deprecated)]
    plan.with_new_children(vec![child])
}

/// Returns true if `plan` reads a region scan through row-wise operators that include
/// at least one filter.
fn filters_region_scan(plan: &Arc<dyn ExecutionPlan>, seen_filter: bool) -> bool {
    if plan.is::<RegionScanExec>() {
        return seen_filter;
    }
    let seen_filter = seen_filter || plan.is::<FilterExec>();
    if plan.is::<FilterExec>() || plan.is::<ProjectionExec>() || plan.is::<CooperativeExec>() {
        return plan
            .children()
            .first()
            .is_some_and(|child| filters_region_scan(child, seen_filter));
    }
    false
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use common_recordbatch::RecordBatches;
    use datafusion_physical_expr::expressions::{Column, IsNotNullExpr};
    use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::region_engine::SinglePartitionScanner;
    use store_api::storage::{RegionId, ScanRequest};

    use super::*;

    fn region_scan(partitions: usize) -> Arc<dyn ExecutionPlan> {
        let ts_type = ConcreteDataType::timestamp_millisecond_datatype();
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("body", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("ts", ts_type.clone(), false),
        ]));
        let stream = RecordBatches::try_new(schema, vec![]).unwrap().as_stream();
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("body", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("ts", ts_type, false),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            });
        let scanner = Box::new(SinglePartitionScanner::new(
            stream,
            false,
            Arc::new(builder.build().unwrap()),
            None,
        ));
        let scan = RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap();
        Arc::new(
            scan.with_new_partitions(vec![vec![]; partitions], partitions)
                .unwrap(),
        )
    }

    fn filter(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let predicate = Arc::new(IsNotNullExpr::new(Arc::new(Column::new("body", 0))));
        Arc::new(FilterExec::try_new(predicate, input).unwrap())
    }

    fn partitioned_topk(input: Arc<dyn ExecutionPlan>, fetch: usize) -> Arc<dyn ExecutionPlan> {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("ts", 1),
        ))])
        .unwrap();
        Arc::new(
            SortExec::new(ordering, input)
                .with_fetch(Some(fetch))
                .with_preserve_partitioning(true),
        )
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        GlobalTopKOverFilteredScan
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    #[test]
    fn test_filtered_scan_gets_one_heap() {
        let plan = optimize(partitioned_topk(filter(region_scan(4)), 100));

        let sort = plan.downcast_ref::<SortExec>().unwrap();
        assert!(!sort.preserve_partitioning());
        assert_eq!(Some(100), sort.fetch());
        let coalesce = sort.input();
        assert!(coalesce.is::<CoalescePartitionsExec>());
        let filter = coalesce.children()[0].downcast_ref::<FilterExec>().unwrap();
        assert_eq!(1, filter.batch_size());
        assert_eq!(4, filter.input().output_partitioning().partition_count());
    }

    #[test]
    fn test_large_fetch_keeps_partitioned_topk() {
        // One thread would merge every filtered row of a weak filter.
        let plan = optimize(partitioned_topk(
            filter(region_scan(4)),
            MAX_GLOBAL_TOPK_FETCH + 1,
        ));
        assert!(
            plan.downcast_ref::<SortExec>()
                .unwrap()
                .preserve_partitioning()
        );
    }

    #[test]
    fn test_unfiltered_scan_keeps_partitioned_topk() {
        // Without a filter every scanned row reaches the heaps, so each fills at once.
        let plan = optimize(partitioned_topk(region_scan(4), 100));
        assert!(
            plan.downcast_ref::<SortExec>()
                .unwrap()
                .preserve_partitioning()
        );
    }
}
