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
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion_common::Result;

/// Lets a TopK see filtered rows as soon as it can use them.
///
/// `FilterExec` buffers its output until `batch_size` rows pass. Under a selective
/// filter a TopK above it receives nothing until the whole input partition is
/// scanned, so its heap never fills and it never publishes the dynamic filter
/// threshold that lets the scan skip older files and row groups. This rule sets the
/// batch size of such filters to 1, which passes each filtered input batch on whole:
/// the coalescer never splits a batch into pieces of the target size or holds rows back.
#[derive(Debug)]
pub struct TopKFilterBatchSize;

impl PhysicalOptimizerRule for TopKFilterBatchSize {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        rewrite(plan, false)
    }

    fn name(&self) -> &str {
        "TopKFilterBatchSize"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `under_topk` is true if a TopK reads `plan`'s output through operators that
/// neither combine rows nor reorder them by anything but the sort key.
fn rewrite(plan: Arc<dyn ExecutionPlan>, under_topk: bool) -> Result<Arc<dyn ExecutionPlan>> {
    let (plan, child_under_topk) = if let Some(sort) = plan.downcast_ref::<SortExec>() {
        let is_topk = sort.fetch().is_some();
        (plan, is_topk)
    } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        if under_topk && filter.batch_size() > 1 {
            (
                Arc::new(filter.with_batch_size(1)?) as Arc<dyn ExecutionPlan>,
                true,
            )
        } else {
            (plan, under_topk)
        }
    } else if plan.is::<ProjectionExec>()
        || plan.is::<CoalescePartitionsExec>()
        || plan.is::<RepartitionExec>()
        || plan.is::<CooperativeExec>()
    {
        (plan, under_topk)
    } else {
        (plan, false)
    };

    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let mut changed = false;
    let new_children = children
        .into_iter()
        .map(|child| {
            let new_child = rewrite(child.clone(), child_under_topk)?;
            changed |= !Arc::ptr_eq(&new_child, child);
            Ok(new_child)
        })
        .collect::<Result<Vec<_>>>()?;
    if !changed {
        return Ok(plan);
    }
    #[allow(deprecated)]
    plan.with_new_children(new_children)
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_physical_expr::expressions::{Column, IsNotNullExpr};
    use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
    use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    fn filter_over_empty() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
        ]));
        let predicate = Arc::new(IsNotNullExpr::new(Arc::new(Column::new("body", 1))));
        Arc::new(FilterExec::try_new(predicate, Arc::new(EmptyExec::new(schema))).unwrap())
    }

    fn sort(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Arc<dyn ExecutionPlan> {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("ts", 0),
        ))])
        .unwrap();
        Arc::new(SortExec::new(ordering, input).with_fetch(fetch))
    }

    fn filter_batch_size(plan: &Arc<dyn ExecutionPlan>) -> usize {
        if let Some(filter) = plan.downcast_ref::<FilterExec>() {
            return filter.batch_size();
        }
        filter_batch_size(plan.children()[0])
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        TopKFilterBatchSize
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    #[test]
    fn test_topk_flushes_filter_per_batch() {
        let filter = filter_over_empty();
        let default_size = filter_batch_size(&filter);
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(Arc::new(Column::new("ts", 0)) as _, "ts".to_string())],
                filter.clone(),
            )
            .unwrap(),
        );

        let plan = optimize(sort(projection, Some(100)));
        assert_eq!(1, filter_batch_size(&plan));

        // A full sort needs every row; batching does not delay any decision.
        let plan = optimize(sort(filter.clone(), None));
        assert_eq!(default_size, filter_batch_size(&plan));
    }

    #[test]
    fn test_aggregate_between_topk_and_filter_keeps_batch_size() {
        let filter = filter_over_empty();
        let default_size = filter_batch_size(&filter);
        let aggregate = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                PhysicalGroupBy::new_single(vec![(
                    Arc::new(Column::new("ts", 0)) as _,
                    "ts".to_string(),
                )]),
                vec![],
                vec![],
                filter.clone(),
                filter.schema(),
            )
            .unwrap(),
        );

        let plan = optimize(sort(aggregate, Some(100)));
        assert_eq!(default_size, filter_batch_size(&plan));
    }
}
