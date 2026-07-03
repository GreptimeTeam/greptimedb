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
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::Result as DfResult;
use datafusion_physical_expr::OrderingRequirements;

#[derive(Debug)]
pub struct EnsureGlobalLimitForFetch;

impl PhysicalOptimizerRule for EnsureGlobalLimitForFetch {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Self::optimize_plan(plan, ParentContext::default())
    }

    fn name(&self) -> &str {
        "EnsureGlobalLimitForFetch"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl EnsureGlobalLimitForFetch {
    fn optimize_plan(
        plan: Arc<dyn ExecutionPlan>,
        parent: ParentContext,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let children = plan.children();
        let plan = if children.is_empty() {
            plan
        } else {
            let required_input_ordering = plan.required_input_ordering();
            let maintains_input_order = plan.maintains_input_order();
            let child_parent = ParentContext {
                has_global_fetch: provides_global_fetch(&plan),
                required_ordering: None,
            };
            let children = children
                .into_iter()
                .enumerate()
                .map(|(idx, child)| {
                    let required_ordering = required_input_ordering
                        .get(idx)
                        .cloned()
                        .unwrap_or(None)
                        .or_else(|| {
                            maintains_input_order
                                .get(idx)
                                .copied()
                                .unwrap_or(false)
                                .then(|| parent.required_ordering.clone())
                                .flatten()
                        });
                    let parent = ParentContext {
                        required_ordering,
                        ..child_parent.clone()
                    };
                    Self::optimize_plan(Arc::clone(child), parent)
                })
                .collect::<DfResult<Vec<_>>>()?;
            plan.with_new_children(children)?
        };

        let Some(fetch) = plan.fetch() else {
            return Ok(plan);
        };

        if parent.has_global_fetch
            || !plan.as_any().is::<FilterExec>()
            || plan.output_partitioning().partition_count() <= 1
        {
            return Ok(plan);
        }

        Ok(add_global_fetch(plan, fetch, parent.required_ordering))
    }
}

#[derive(Clone, Default)]
struct ParentContext {
    has_global_fetch: bool,
    required_ordering: Option<OrderingRequirements>,
}

fn provides_global_fetch(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.fetch().is_none() {
        return false;
    }

    plan.as_any().is::<GlobalLimitExec>()
        || plan.as_any().is::<CoalescePartitionsExec>()
        || plan.as_any().is::<SortPreservingMergeExec>()
}

fn add_global_fetch(
    plan: Arc<dyn ExecutionPlan>,
    fetch: usize,
    required_ordering: Option<OrderingRequirements>,
) -> Arc<dyn ExecutionPlan> {
    if required_ordering.is_some()
        && let Some(ordering) = plan.output_ordering().cloned()
    {
        Arc::new(SortPreservingMergeExec::new(ordering, plan).with_fetch(Some(fetch)))
    } else {
        Arc::new(CoalescePartitionsExec::new(plan).with_fetch(Some(fetch)))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::{col, lit};
    use datafusion::physical_plan::filter::FilterExecBuilder;
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::test::TestMemoryExec;
    use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};

    use super::*;

    #[test]
    fn adds_global_limit_for_multi_partition_filter_fetch() {
        let filter = filter_fetch(unordered_input(), 1);

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(filter, ParentContext::default()).unwrap();

        assert!(optimized.as_any().is::<CoalescePartitionsExec>());
        assert_eq!(optimized.fetch(), Some(1));
        assert_eq!(optimized.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn still_visits_subtree_under_global_limit() {
        let filter = filter_fetch(unordered_input(), 5);
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(col("a", filter.schema().as_ref()).unwrap(), "a".to_string())],
                filter,
            )
            .unwrap(),
        );
        let limit =
            Arc::new(GlobalLimitExec::new(projection, 0, Some(10))) as Arc<dyn ExecutionPlan>;

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(limit, ParentContext::default()).unwrap();
        let projection = optimized.children()[0];
        let coalesce = projection.children()[0];

        assert!(coalesce.as_any().is::<CoalescePartitionsExec>());
        assert_eq!(coalesce.fetch(), Some(5));
    }

    #[test]
    fn keeps_filter_under_parent_global_fetch() {
        let (input, ordering) = ordered_input();
        let filter = filter_fetch(input, 1);
        let merge = Arc::new(SortPreservingMergeExec::new(ordering, filter).with_fetch(Some(1)))
            as Arc<dyn ExecutionPlan>;

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(merge, ParentContext::default()).unwrap();
        let child = optimized.children()[0];

        assert!(optimized.as_any().is::<SortPreservingMergeExec>());
        assert!(child.as_any().is::<FilterExec>());
    }

    #[test]
    fn preserves_parent_ordering_requirement() {
        let (input, ordering) = ordered_input();
        let filter = filter_fetch(input, 1);
        let merge =
            Arc::new(SortPreservingMergeExec::new(ordering, filter)) as Arc<dyn ExecutionPlan>;

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(merge, ParentContext::default()).unwrap();
        let child = optimized.children()[0];

        assert!(optimized.as_any().is::<SortPreservingMergeExec>());
        assert!(child.as_any().is::<SortPreservingMergeExec>());
        assert_eq!(child.fetch(), Some(1));
    }

    #[test]
    fn uses_child_output_ordering_for_merge() {
        let schema = schema();
        let required_ordering = ordering(schema.as_ref(), false);
        let actual_ordering = ordering(schema.as_ref(), true);
        let batch = batch(schema.clone());
        let partitions = vec![vec![batch.clone()], vec![batch.clone()], vec![batch]];
        let input = TestMemoryExec::try_new(&partitions, schema, None)
            .unwrap()
            .try_with_sort_information(vec![actual_ordering.clone()])
            .unwrap();
        let filter = filter_fetch(Arc::new(input), 1);

        let optimized = add_global_fetch(
            filter,
            1,
            Some(OrderingRequirements::from(required_ordering)),
        );
        let merge = optimized
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>()
            .unwrap();

        assert_eq!(merge.expr(), &actual_ordering);
    }

    #[test]
    fn preserves_inherited_ordering_requirement_through_projection() {
        let (input, ordering) = ordered_input();
        let filter = filter_fetch(input, 1);
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(col("a", filter.schema().as_ref()).unwrap(), "a".to_string())],
                filter,
            )
            .unwrap(),
        );
        let merge =
            Arc::new(SortPreservingMergeExec::new(ordering, projection)) as Arc<dyn ExecutionPlan>;

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(merge, ParentContext::default()).unwrap();
        let projection = optimized.children()[0];
        let child = projection.children()[0];

        assert!(optimized.as_any().is::<SortPreservingMergeExec>());
        assert!(projection.as_any().is::<ProjectionExec>());
        assert!(child.as_any().is::<SortPreservingMergeExec>());
        assert_eq!(child.fetch(), Some(1));
    }

    fn unordered_input() -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        let batch = batch(schema.clone());
        let partitions = vec![vec![batch.clone()], vec![batch.clone()], vec![batch]];
        Arc::new(TestMemoryExec::try_new(&partitions, schema, None).unwrap())
    }

    fn ordered_input() -> (Arc<dyn ExecutionPlan>, LexOrdering) {
        let schema = schema();
        let ordering = ordering(schema.as_ref(), false);
        let batch = batch(schema.clone());
        let partitions = vec![vec![batch.clone()], vec![batch.clone()], vec![batch]];
        let input = TestMemoryExec::try_new(&partitions, schema, None)
            .unwrap()
            .try_with_sort_information(vec![ordering.clone()])
            .unwrap();

        (Arc::new(input), ordering)
    }

    fn filter_fetch(input: Arc<dyn ExecutionPlan>, fetch: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            FilterExecBuilder::new(lit(true), input)
                .with_fetch(Some(fetch))
                .build()
                .unwrap(),
        )
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn batch(schema: Arc<Schema>) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    fn ordering(schema: &Schema, descending: bool) -> LexOrdering {
        LexOrdering::new([PhysicalSortExpr::new(
            col("a", schema).unwrap(),
            SortOptions {
                descending,
                nulls_first: descending,
            },
        )])
        .unwrap()
    }
}
