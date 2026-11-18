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

use crate::optimizer::windowed_sort::is_time_index_expr;

/// Lets a TopK see filtered rows as soon as it can use them.
///
/// `FilterExec` buffers its output until `batch_size` rows pass. Under a selective
/// filter a TopK above it receives nothing until 8192 rows match or the input
/// partition ends, so its heap fills too late to publish the dynamic filter
/// threshold that lets the scan skip older files and row groups. For a TopK on the
/// time index, this rule sets the batch size of such filters to 1, which passes each
/// filtered input batch on whole: the coalescer never splits a batch into pieces of the
/// target size or holds rows back. Other sort keys are left alone, since file and row
/// group ranges rarely let the scan skip anything by them.
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

/// `under_topk` is true if a TopK on the time index reads `plan`'s output through
/// operators that neither combine rows nor reorder them by anything but the sort key.
fn rewrite(plan: Arc<dyn ExecutionPlan>, under_topk: bool) -> Result<Arc<dyn ExecutionPlan>> {
    let (plan, child_under_topk) = if let Some(sort) = plan.downcast_ref::<SortExec>() {
        let is_time_topk =
            sort.fetch().is_some() && is_time_index_expr(sort.input(), &sort.expr().first().expr)?;
        (plan, is_time_topk)
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
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::collect;
    use datafusion_physical_expr::expressions::{Column, IsNotNullExpr};
    use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
    use datatypes::arrow::array::{StringArray, TimestampMillisecondArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datatypes::arrow::record_batch::RecordBatch;

    use super::*;
    use crate::optimizer::test_util::log_region_scan;

    fn filter_over_scan() -> Arc<dyn ExecutionPlan> {
        let predicate = Arc::new(IsNotNullExpr::new(Arc::new(Column::new("body", 0))));
        Arc::new(FilterExec::try_new(predicate, log_region_scan(1)).unwrap())
    }

    fn sort(
        input: Arc<dyn ExecutionPlan>,
        key: Column,
        fetch: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(key))]).unwrap();
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
        let filter = filter_over_scan();
        let default_size = filter_batch_size(&filter);
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(Arc::new(Column::new("ts", 1)) as _, "ts".to_string())],
                filter.clone(),
            )
            .unwrap(),
        );

        let plan = optimize(sort(projection, Column::new("ts", 0), Some(100)));
        assert_eq!(1, filter_batch_size(&plan));

        // A full sort needs every row; batching does not delay any decision.
        let plan = optimize(sort(filter.clone(), Column::new("ts", 1), None));
        assert_eq!(default_size, filter_batch_size(&plan));
    }

    #[test]
    fn test_filter_batch_size_kept_when_topk_cannot_prune() {
        let filter = filter_over_scan();
        let default_size = filter_batch_size(&filter);

        // The scan cannot skip ranges on a sort key other than the time index.
        let plan = optimize(sort(filter.clone(), Column::new("body", 0), Some(100)));
        assert_eq!(default_size, filter_batch_size(&plan));

        // An aggregate combines rows, so its input order does not decide the TopK.
        let aggregate = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                PhysicalGroupBy::new_single(vec![(
                    Arc::new(Column::new("ts", 1)) as _,
                    "ts".to_string(),
                )]),
                vec![],
                vec![],
                filter.clone(),
                filter.schema(),
            )
            .unwrap(),
        );
        let plan = optimize(sort(aggregate, Column::new("ts", 0), Some(100)));
        assert_eq!(default_size, filter_batch_size(&plan));
    }

    #[tokio::test]
    async fn test_filter_with_batch_size_one_keeps_batches_whole() {
        // The rule relies on the coalescer passing batches through at a target of 1;
        // splitting them into single rows would multiply the per-batch cost.
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = |rows: usize| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![0; rows])),
                    Arc::new(StringArray::from(vec!["a"; rows])),
                ],
            )
            .unwrap()
        };
        let input = DataSourceExec::from_data_source(
            MemorySourceConfig::try_new(
                &[vec![batch(3), batch(100), batch(100)]],
                schema.clone(),
                None,
            )
            .unwrap(),
        );
        let predicate = Arc::new(IsNotNullExpr::new(Arc::new(Column::new("body", 1))));
        let filter = FilterExec::try_new(predicate, input)
            .unwrap()
            .with_batch_size(1)
            .unwrap();

        let output = collect(Arc::new(filter), Arc::new(TaskContext::default()))
            .await
            .unwrap();
        let sizes: Vec<_> = output.iter().map(|b| b.num_rows()).collect();
        assert_eq!(vec![3, 100, 100], sizes);
    }
}
