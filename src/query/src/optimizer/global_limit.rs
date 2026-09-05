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
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::Result as DfResult;
use datafusion_physical_expr::{Distribution, OrderingRequirements, Partitioning};

use crate::dist_plan::MergeSortExec;

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
            let required_input_distribution = plan.required_input_distribution();
            let required_input_ordering = plan.required_input_ordering();
            let maintains_input_order = plan.maintains_input_order();
            let child_parent = ParentContext {
                global_fetch: provided_global_fetch(&plan),
                local_fetch: plan
                    .as_any()
                    .downcast_ref::<LocalLimitExec>()
                    .map(LocalLimitExec::fetch),
                required_ordering: None,
                required_distribution: Distribution::UnspecifiedDistribution,
                partitioning_to_restore: None,
                preserve_hash_partitioning: false,
            };
            let children = children
                .into_iter()
                .enumerate()
                .map(|(idx, child)| {
                    let required_distribution = required_input_distribution
                        .get(idx)
                        .cloned()
                        .unwrap_or(Distribution::UnspecifiedDistribution);
                    let partitioning_to_restore =
                        partitioning_to_restore_for(child, &required_distribution)
                            .or_else(|| inherited_partitioning_to_restore(&plan, child, &parent));
                    let preserve_hash_partitioning = partitioning_to_restore.is_some();
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
                        required_distribution,
                        partitioning_to_restore,
                        preserve_hash_partitioning,
                        ..child_parent.clone()
                    };
                    Self::optimize_plan(Arc::clone(child), parent)
                })
                .collect::<DfResult<Vec<_>>>()?;
            plan.with_new_children(children)?
        };

        let aggregate_limit = aggregate_soft_limit(&plan);
        let Some(fetch) = plan.fetch().or(aggregate_limit) else {
            return Ok(plan);
        };

        if parent
            .global_fetch
            .is_some_and(|parent_fetch| parent_fetch <= fetch)
            || aggregate_limit.is_some_and(|aggregate_fetch| {
                parent
                    .local_fetch
                    .is_some_and(|local_fetch| local_fetch <= aggregate_fetch)
            })
            || !(plan.as_any().is::<FilterExec>() || aggregate_limit.is_some())
            || plan.output_partitioning().partition_count() <= 1
        {
            return Ok(plan);
        }

        add_global_fetch(
            plan,
            fetch,
            parent.required_ordering,
            parent.partitioning_to_restore,
        )
    }
}

/// Returns the soft limit of a multi-partition-capable [`AggregateExec`], if any.
///
/// The `lim` hint pushed down by DataFusion's `LimitedDistinctAggregation` rule
/// is only enforced per partition (see `group_values_soft_limit` in
/// `GroupedHashAggregateStream`), so a multi-partition final aggregate needs a
/// global fetch on top, just like a multi-partition [`FilterExec`].
///
/// Only unordered soft limits (`FinalPartitioned` or `SinglePartitioned` mode)
/// qualify. Ordered limits come from `TopKAggregation`, where each partition
/// keeps its local top-N candidates and the global top-N is chosen by the sort
/// machinery above; truncating the merged candidates with a coalesce fetch
/// would drop true top-N rows.
fn aggregate_soft_limit(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    plan.as_any()
        .downcast_ref::<AggregateExec>()
        .filter(|agg| {
            matches!(
                agg.mode(),
                AggregateMode::FinalPartitioned | AggregateMode::SinglePartitioned
            )
        })
        .and_then(|agg| {
            agg.limit_options()
                .filter(|options| options.descending().is_none())
                .map(|options| options.limit())
        })
}

#[derive(Clone)]
struct ParentContext {
    global_fetch: Option<usize>,
    local_fetch: Option<usize>,
    required_ordering: Option<OrderingRequirements>,
    required_distribution: Distribution,
    partitioning_to_restore: Option<Partitioning>,
    preserve_hash_partitioning: bool,
}

impl Default for ParentContext {
    fn default() -> Self {
        Self {
            global_fetch: None,
            local_fetch: None,
            required_ordering: None,
            required_distribution: Distribution::UnspecifiedDistribution,
            partitioning_to_restore: None,
            preserve_hash_partitioning: false,
        }
    }
}

fn provided_global_fetch(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let fetch = plan.fetch()?;
    (plan.as_any().is::<GlobalLimitExec>()
        || plan.as_any().is::<CoalescePartitionsExec>()
        || plan.as_any().is::<SortPreservingMergeExec>()
        || plan.as_any().is::<MergeSortExec>())
    .then_some(fetch)
}

fn add_global_fetch(
    plan: Arc<dyn ExecutionPlan>,
    fetch: usize,
    required_ordering: Option<OrderingRequirements>,
    partitioning_to_restore: Option<Partitioning>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let plan = if required_ordering.is_some()
        && let Some(ordering) = plan.output_ordering().cloned()
    {
        Arc::new(SortPreservingMergeExec::new(ordering, plan).with_fetch(Some(fetch)))
            as Arc<dyn ExecutionPlan>
    } else {
        Arc::new(CoalescePartitionsExec::new(plan).with_fetch(Some(fetch)))
            as Arc<dyn ExecutionPlan>
    };

    restore_required_partitioning(plan, partitioning_to_restore)
}

fn restore_required_partitioning(
    plan: Arc<dyn ExecutionPlan>,
    partitioning_to_restore: Option<Partitioning>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let Some(partitioning) = partitioning_to_restore else {
        return Ok(plan);
    };

    if partitioning.partition_count() <= 1 || !matches!(&partitioning, Partitioning::Hash(_, _)) {
        return Ok(plan);
    }

    Ok(Arc::new(
        RepartitionExec::try_new(plan, partitioning)?.with_preserve_order(),
    ))
}

fn partitioning_to_restore_for(
    child: &Arc<dyn ExecutionPlan>,
    required_distribution: &Distribution,
) -> Option<Partitioning> {
    if !matches!(required_distribution, Distribution::HashPartitioned(_))
        || child.output_partitioning().partition_count() <= 1
    {
        return None;
    }

    if child
        .output_partitioning()
        .satisfaction(required_distribution, child.equivalence_properties(), false)
        .is_satisfied()
    {
        Some(child.output_partitioning().clone())
    } else {
        Some(
            required_distribution
                .clone()
                .create_partitioning(child.output_partitioning().partition_count()),
        )
    }
}

fn inherited_partitioning_to_restore(
    plan: &Arc<dyn ExecutionPlan>,
    child: &Arc<dyn ExecutionPlan>,
    parent: &ParentContext,
) -> Option<Partitioning> {
    if child.output_partitioning().partition_count() <= 1
        || !matches!(child.output_partitioning(), Partitioning::Hash(_, _))
        || !matches!(plan.output_partitioning(), Partitioning::Hash(_, _))
        || plan.output_partitioning().partition_count()
            != child.output_partitioning().partition_count()
    {
        return None;
    }

    let satisfies_parent_distribution = matches!(
        parent.required_distribution,
        Distribution::HashPartitioned(_)
    ) && plan
        .output_partitioning()
        .satisfaction(
            &parent.required_distribution,
            plan.equivalence_properties(),
            false,
        )
        .is_satisfied();

    (satisfies_parent_distribution || parent.preserve_hash_partitioning)
        .then(|| child.output_partitioning().clone())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::expressions::{col, lit};
    use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
    use datafusion::physical_plan::aggregates::{
        AggregateExec, AggregateMode, LimitOptions, PhysicalGroupBy,
    };
    use datafusion::physical_plan::filter::FilterExecBuilder;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::test::TestMemoryExec;
    use datafusion_common::{JoinType, NullEquality};
    use datafusion_physical_expr::{LexOrdering, Partitioning, PhysicalSortExpr};

    use super::*;

    #[test]
    fn adds_global_limit_for_multi_partition_aggregate_soft_limit() {
        let agg = final_partitioned_agg_with_limit(unordered_input(), 1);

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(agg, ParentContext::default()).unwrap();

        assert!(optimized.as_any().is::<CoalescePartitionsExec>());
        assert_eq!(optimized.fetch(), Some(1));
        assert_eq!(optimized.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn keeps_aggregate_soft_limit_under_parent_global_fetch() {
        let agg = final_partitioned_agg_with_limit(unordered_input(), 1);
        let coalesce = Arc::new(CoalescePartitionsExec::new(agg).with_fetch(Some(1)))
            as Arc<dyn ExecutionPlan>;

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(coalesce, ParentContext::default()).unwrap();

        // Idempotent: no extra coalesce is inserted under the existing one.
        assert!(optimized.as_any().is::<CoalescePartitionsExec>());
        assert!(optimized.children()[0].as_any().is::<AggregateExec>());
    }

    #[test]
    fn ignores_partial_aggregate_soft_limit() {
        let agg = partial_agg_with_limit(unordered_input(), 1);

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(agg, ParentContext::default()).unwrap();

        assert!(optimized.as_any().is::<AggregateExec>());
    }

    #[test]
    fn ignores_single_partition_aggregate_soft_limit() {
        let schema = schema();
        let batch = batch(schema.clone());
        let input = Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap());
        let agg = final_partitioned_agg_with_limit(input, 1);

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(agg, ParentContext::default()).unwrap();

        assert!(optimized.as_any().is::<AggregateExec>());
    }

    #[tokio::test]
    async fn topk_ordered_aggregate_limit_returns_global_winner() {
        // Ordered limit options come from TopKAggregation: each partition only
        // keeps its local top-N candidates, and the global top-N is chosen by
        // the sort machinery above. Truncating the merged candidates with a
        // coalesce fetch would drop true top-N rows.
        //
        // Partition 0 holds a losing candidate (100), partition 1 the global
        // winner (1) of the ascending TopK. The global sort must see both.
        let schema = schema();
        let losing =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![100]))])
                .unwrap();
        let winning =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
                .unwrap();
        let input = Arc::new(
            TestMemoryExec::try_new(&[vec![losing], vec![winning]], schema.clone(), None).unwrap(),
        );
        let agg = agg_with_limit_options(
            input,
            AggregateMode::FinalPartitioned,
            LimitOptions::new_with_order(1, false),
        );
        // Mirrors the real plan shape after EnforceDistribution: the global
        // sort reads a single partition merged from the multi-partition
        // aggregate. The merged stream must contain candidates from all
        // aggregate partitions.
        let merge = Arc::new(CoalescePartitionsExec::new(agg)) as Arc<dyn ExecutionPlan>;
        let ordering = LexOrdering::new([PhysicalSortExpr::new(
            col("a", schema.as_ref()).unwrap(),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )])
        .unwrap();
        let sort =
            Arc::new(SortExec::new(ordering, merge).with_fetch(Some(1))) as Arc<dyn ExecutionPlan>;

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(sort, ParentContext::default()).unwrap();

        // The rule must not insert any candidate-truncating node below the
        // merge...
        let merge = optimized.children()[0].clone();
        assert!(merge.as_any().is::<CoalescePartitionsExec>());
        assert!(merge.children()[0].as_any().is::<AggregateExec>());

        // ...and the executed plan must return the global winner.
        let batches =
            datafusion::physical_plan::collect(optimized, Arc::new(TaskContext::default()))
                .await
                .unwrap();
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![1]);
    }

    #[tokio::test]
    async fn single_partitioned_soft_limit_returns_one_row_globally() {
        // Production-like shape: CombinePartialFinalAggregate rewrites
        // FinalPartitioned + Partial into SinglePartitioned while preserving
        // limit_options, when the Partial's output is already hash-partitioned
        // by the group key. The duplicate key across raw partitions exercises
        // the hash merge.
        let schema = schema();
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 4, 5]))],
        )
        .unwrap();
        let scan = Arc::new(
            TestMemoryExec::try_new(&[vec![first], vec![second]], schema.clone(), None).unwrap(),
        );
        let repartition = hash_repartition(scan);
        let partial = agg_with_limit(repartition, AggregateMode::Partial, 1);
        let final_agg = agg_with_limit(partial, AggregateMode::FinalPartitioned, 1);

        let combined = CombinePartialFinalAggregate::new()
            .optimize(final_agg, &ConfigOptions::new())
            .unwrap();
        let agg = combined.as_any().downcast_ref::<AggregateExec>().unwrap();
        assert!(matches!(agg.mode(), AggregateMode::SinglePartitioned));
        assert_eq!(agg.limit_options().map(|options| options.limit()), Some(1));
        assert_eq!(combined.output_partitioning().partition_count(), 3);

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(combined, ParentContext::default()).unwrap();

        // A global fetch must be installed above the multi-partition
        // SinglePartitioned aggregate...
        assert!(optimized.as_any().is::<CoalescePartitionsExec>());
        assert_eq!(optimized.fetch(), Some(1));

        // ...and the executed plan must return one row globally, not one row
        // per partition.
        let batches =
            datafusion::physical_plan::collect(optimized, Arc::new(TaskContext::default()))
                .await
                .unwrap();
        let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn local_limit_preserves_per_partition_aggregate_soft_limit() {
        let schema = schema();
        let input = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from((0..100).collect::<Vec<_>>()))],
        )
        .unwrap();
        let scan = Arc::new(TestMemoryExec::try_new(&[vec![input]], schema, None).unwrap());
        // A tighter local limit must retain its per-partition scope.
        let agg = agg_with_limit(hash_repartition(scan), AggregateMode::SinglePartitioned, 2);
        let local_limit = Arc::new(LocalLimitExec::new(agg, 1)) as Arc<dyn ExecutionPlan>;

        let optimized = EnsureGlobalLimitForFetch::optimize_plan(
            Arc::clone(&local_limit),
            ParentContext::default(),
        )
        .unwrap();

        let original_batches =
            datafusion::physical_plan::collect(local_limit, Arc::new(TaskContext::default()))
                .await
                .unwrap();
        let optimized_batches = datafusion::physical_plan::collect(
            Arc::clone(&optimized),
            Arc::new(TaskContext::default()),
        )
        .await
        .unwrap();
        let original_rows: usize = original_batches.iter().map(|batch| batch.num_rows()).sum();
        let optimized_rows: usize = optimized_batches.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(original_rows, 3);
        assert_eq!(optimized_rows, 3);
        assert!(optimized.as_any().is::<LocalLimitExec>());
        assert!(optimized.children()[0].as_any().is::<AggregateExec>());
    }

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
    fn adds_tighter_global_fetch_under_looser_parent_fetch() {
        let (input, ordering) = ordered_input();
        let filter = filter_fetch(input, 5);
        let merge = Arc::new(SortPreservingMergeExec::new(ordering, filter).with_fetch(Some(10)))
            as Arc<dyn ExecutionPlan>;

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(merge, ParentContext::default()).unwrap();
        let child = optimized.children()[0];

        assert!(optimized.as_any().is::<SortPreservingMergeExec>());
        assert!(child.as_any().is::<SortPreservingMergeExec>());
        assert_eq!(child.fetch(), Some(5));
    }

    #[test]
    fn keeps_filter_under_parent_merge_sort_fetch() {
        let (input, ordering) = ordered_input();
        let filter = filter_fetch(input, 1);
        let merge = merge_sort_fetch(ordering, filter, 1);

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(merge, ParentContext::default()).unwrap();
        let child = optimized.children()[0];

        assert!(optimized.as_any().is::<MergeSortExec>());
        assert!(child.as_any().is::<FilterExec>());
    }

    #[test]
    fn adds_tighter_global_fetch_under_looser_merge_sort_fetch() {
        let (input, ordering) = ordered_input();
        let filter = filter_fetch(input, 5);
        let merge = merge_sort_fetch(ordering, filter, 10);

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(merge, ParentContext::default()).unwrap();
        let child = optimized.children()[0];

        assert!(optimized.as_any().is::<MergeSortExec>());
        assert!(child.as_any().is::<SortPreservingMergeExec>());
        assert_eq!(child.fetch(), Some(5));
        assert!(child.children()[0].as_any().is::<FilterExec>());
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
            None,
        )
        .unwrap();
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

    #[test]
    fn restores_parent_hash_distribution_after_global_fetch() {
        let left = filter_fetch(hash_repartition(unordered_input()), 1);
        let right = hash_repartition(unordered_input());
        let on = vec![(
            col("a", left.schema().as_ref()).unwrap(),
            col("a", right.schema().as_ref()).unwrap(),
        )];
        let join = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(join, ParentContext::default()).unwrap();
        let left = optimized.children()[0];
        let repartition = left.as_any().downcast_ref::<RepartitionExec>().unwrap();

        assert!(matches!(
            repartition.partitioning(),
            Partitioning::Hash(_, 3)
        ));
        assert!(repartition.input().as_any().is::<CoalescePartitionsExec>());
        assert_eq!(repartition.input().fetch(), Some(1));
    }

    #[test]
    fn restores_inherited_hash_distribution_through_projection() {
        let filter = filter_fetch(hash_repartition(unordered_input()), 1);
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(col("a", filter.schema().as_ref()).unwrap(), "a".to_string())],
                filter,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let right = hash_repartition(unordered_input());
        let on = vec![(
            col("a", projection.schema().as_ref()).unwrap(),
            col("a", right.schema().as_ref()).unwrap(),
        )];
        let join = Arc::new(
            HashJoinExec::try_new(
                projection,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(join, ParentContext::default()).unwrap();
        let projection = optimized.children()[0];
        let repartition = projection.children()[0]
            .as_any()
            .downcast_ref::<RepartitionExec>()
            .unwrap();

        assert!(projection.as_any().is::<ProjectionExec>());
        assert!(matches!(
            repartition.partitioning(),
            Partitioning::Hash(_, 3)
        ));
        assert!(repartition.input().as_any().is::<CoalescePartitionsExec>());
        assert_eq!(repartition.input().fetch(), Some(1));
    }

    #[test]
    fn restores_inherited_hash_distribution_through_multiple_projections() {
        let filter = filter_fetch(hash_repartition(unordered_input()), 1);
        let projection = project_a(filter);
        let projection = project_a(projection);
        let right = hash_repartition(unordered_input());
        let on = vec![(
            col("a", projection.schema().as_ref()).unwrap(),
            col("a", right.schema().as_ref()).unwrap(),
        )];
        let join = Arc::new(
            HashJoinExec::try_new(
                projection,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let optimized =
            EnsureGlobalLimitForFetch::optimize_plan(join, ParentContext::default()).unwrap();
        let outer_projection = optimized.children()[0];
        let inner_projection = outer_projection.children()[0];
        let repartition = inner_projection.children()[0]
            .as_any()
            .downcast_ref::<RepartitionExec>()
            .unwrap();

        assert!(outer_projection.as_any().is::<ProjectionExec>());
        assert!(inner_projection.as_any().is::<ProjectionExec>());
        assert!(matches!(
            repartition.partitioning(),
            Partitioning::Hash(_, 3)
        ));
        assert!(repartition.input().as_any().is::<CoalescePartitionsExec>());
        assert_eq!(repartition.input().fetch(), Some(1));
    }

    fn unordered_input() -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        let batch = batch(schema.clone());
        let partitions = vec![vec![batch.clone()], vec![batch.clone()], vec![batch]];
        Arc::new(TestMemoryExec::try_new(&partitions, schema, None).unwrap())
    }

    fn final_partitioned_agg_with_limit(
        input: Arc<dyn ExecutionPlan>,
        limit: usize,
    ) -> Arc<dyn ExecutionPlan> {
        agg_with_limit(input, AggregateMode::FinalPartitioned, limit)
    }

    fn partial_agg_with_limit(
        input: Arc<dyn ExecutionPlan>,
        limit: usize,
    ) -> Arc<dyn ExecutionPlan> {
        agg_with_limit(input, AggregateMode::Partial, limit)
    }

    fn agg_with_limit(
        input: Arc<dyn ExecutionPlan>,
        mode: AggregateMode,
        limit: usize,
    ) -> Arc<dyn ExecutionPlan> {
        agg_with_limit_options(input, mode, LimitOptions::new(limit))
    }

    fn agg_with_limit_options(
        input: Arc<dyn ExecutionPlan>,
        mode: AggregateMode,
        limit_options: LimitOptions,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let group_by = PhysicalGroupBy::new_single(vec![(
            col("a", schema.as_ref()).unwrap(),
            "a".to_string(),
        )]);
        Arc::new(
            AggregateExec::try_new(mode, group_by, vec![], vec![], input, schema)
                .unwrap()
                .with_limit_options(Some(limit_options)),
        )
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

    fn merge_sort_fetch(
        ordering: LexOrdering,
        input: Arc<dyn ExecutionPlan>,
        fetch: usize,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(MergeSortExec::new(ordering, input, Some(fetch)))
    }

    fn hash_repartition(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let partitioning = Partitioning::Hash(vec![col("a", input.schema().as_ref()).unwrap()], 3);
        Arc::new(RepartitionExec::try_new(input, partitioning).unwrap())
    }

    fn project_a(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            ProjectionExec::try_new(
                vec![(col("a", input.schema().as_ref()).unwrap(), "a".to_string())],
                input,
            )
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
