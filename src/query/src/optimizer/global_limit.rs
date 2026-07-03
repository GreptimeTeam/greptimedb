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
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::Result as DfResult;

#[derive(Debug)]
pub struct EnsureGlobalLimitForFetch;

impl PhysicalOptimizerRule for EnsureGlobalLimitForFetch {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Self::optimize_plan(plan)
    }

    fn name(&self) -> &str {
        "EnsureGlobalLimitForFetch"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl EnsureGlobalLimitForFetch {
    fn optimize_plan(plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        if is_global_fetch(&plan) {
            return Ok(plan);
        }

        let children = plan.children();
        let plan = if children.is_empty() {
            plan
        } else {
            let children = children
                .into_iter()
                .map(|child| Self::optimize_plan(Arc::clone(child)))
                .collect::<DfResult<Vec<_>>>()?;
            plan.with_new_children(children)?
        };

        let Some(fetch) = plan.fetch() else {
            return Ok(plan);
        };

        if !plan.as_any().is::<FilterExec>() || plan.output_partitioning().partition_count() <= 1 {
            return Ok(plan);
        }

        Ok(Arc::new(
            CoalescePartitionsExec::new(plan).with_fetch(Some(fetch)),
        ))
    }
}

fn is_global_fetch(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<GlobalLimitExec>()
        || (plan.as_any().is::<CoalescePartitionsExec>() && plan.fetch().is_some())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::lit;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::filter::FilterExecBuilder;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::test::exec::MockExec;

    use super::*;

    #[test]
    fn adds_global_limit_for_multi_partition_filter_fetch() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let input = Arc::new(MockExec::new(vec![Ok(batch)], schema));
        let repartition =
            Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(3)).unwrap());
        let filter = Arc::new(
            FilterExecBuilder::new(lit(true), repartition)
                .with_fetch(Some(1))
                .build()
                .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let optimized = EnsureGlobalLimitForFetch::optimize_plan(filter).unwrap();

        assert!(optimized.as_any().is::<CoalescePartitionsExec>());
        assert_eq!(optimized.fetch(), Some(1));
        assert_eq!(optimized.output_partitioning().partition_count(), 1);
    }
}
