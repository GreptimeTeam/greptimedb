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
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_common::Result as DfResult;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{Distribution, PhysicalExpr};

use crate::dist_plan::MergeScanExec;

/// This is a [`PhysicalOptimizerRule`] to reduce repartition overhead between
/// partial and final aggregates by pushing a coarser hash distribution
/// requirement through [`AggregateExec`] in partial mode down to [`MergeScanExec`].
///
/// This rule keeps the partition fanout unchanged. It only enables
/// source-aware partition-column coarsening that [`MergeScanExec`] can already
/// satisfy without a full reshuffle.
///
/// This rule is expected to be run before [`EnforceDistribution`].
///
/// [`EnforceDistribution`]: datafusion::physical_optimizer::enforce_distribution::EnforceDistribution
/// [`MergeScanExec`]: crate::dist_plan::MergeScanExec
#[derive(Debug)]
pub struct ReduceAggregateRepartition;

impl PhysicalOptimizerRule for ReduceAggregateRepartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Self::do_optimize(plan, config)
    }

    fn name(&self) -> &str {
        "ReduceAggregateRepartitionRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

impl ReduceAggregateRepartition {
    fn do_optimize(
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Self::rewrite_with_distribution(plan, None)
    }

    fn rewrite_with_distribution(
        plan: Arc<dyn ExecutionPlan>,
        current_req: Option<Distribution>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if let Some(merge_scan) = plan.as_any().downcast_ref::<MergeScanExec>()
            && let Some(distribution) = current_req.as_ref()
            && let Some(new_plan) = merge_scan.try_with_new_distribution(distribution.clone())
        {
            return Ok(Arc::new(new_plan) as _);
        }

        let children = plan.children();
        if children.is_empty() {
            return Ok(plan);
        }

        let required = plan.required_input_distribution();
        let mut new_children = Vec::with_capacity(children.len());
        for (idx, child) in children.into_iter().enumerate() {
            let child_req =
                Self::child_distribution_requirement(&plan, idx, current_req.as_ref(), &required);
            let new_child = Self::rewrite_with_distribution(child.clone(), child_req)?;
            new_children.push(new_child);
        }

        let unchanged = plan
            .children()
            .into_iter()
            .zip(new_children.iter())
            .all(|(old, new)| Arc::ptr_eq(old, new));
        if unchanged {
            Ok(plan)
        } else {
            plan.with_new_children(new_children)
        }
    }

    fn child_distribution_requirement(
        plan: &Arc<dyn ExecutionPlan>,
        child_idx: usize,
        current_req: Option<&Distribution>,
        required: &[Distribution],
    ) -> Option<Distribution> {
        match required.get(child_idx) {
            Some(Distribution::UnspecifiedDistribution) => {
                Self::partial_aggregate_child_distribution(plan, child_idx, current_req)
            }
            None => current_req.cloned(),
            Some(req) => Some(req.clone()),
        }
    }

    fn partial_aggregate_child_distribution(
        plan: &Arc<dyn ExecutionPlan>,
        child_idx: usize,
        current_req: Option<&Distribution>,
    ) -> Option<Distribution> {
        if child_idx == 0 {
            Self::partial_aggregate_input_distribution(plan, current_req)
        } else {
            None
        }
    }

    fn partial_aggregate_input_distribution(
        plan: &Arc<dyn ExecutionPlan>,
        current_req: Option<&Distribution>,
    ) -> Option<Distribution> {
        let agg_exec = plan.as_any().downcast_ref::<AggregateExec>()?;
        if agg_exec.mode() != &AggregateMode::Partial || agg_exec.group_expr().has_grouping_set() {
            return None;
        }

        let Distribution::HashPartitioned(required_exprs) = current_req? else {
            return None;
        };

        let output_group_exprs = agg_exec.output_group_expr();
        let input_group_exprs = agg_exec.group_expr().input_exprs();
        let mut mapped_exprs = Vec::with_capacity(required_exprs.len());
        for required_expr in required_exprs {
            let required_col = required_expr.as_any().downcast_ref::<Column>()?;
            let group_idx = output_group_exprs
                .iter()
                .position(|expr| Self::same_column(expr, required_col))?;
            mapped_exprs.push(input_group_exprs[group_idx].clone());
        }

        Some(Distribution::HashPartitioned(mapped_exprs))
    }

    fn same_column(expr: &Arc<dyn PhysicalExpr>, expected: &Column) -> bool {
        expr.as_any()
            .downcast_ref::<Column>()
            .is_some_and(|column| {
                column.index() == expected.index() && column.name() == expected.name()
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion_common::Result;
    use datafusion_physical_expr::Distribution;
    use datafusion_physical_expr::expressions::{Column, col};

    use super::ReduceAggregateRepartition;

    fn input_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let config = MemorySourceConfig::try_new(&[vec![]], schema, None).unwrap();
        DataSourceExec::from_data_source(config)
    }

    fn group_by(names: &[&str], schema: &SchemaRef) -> Result<PhysicalGroupBy> {
        let groups = names
            .iter()
            .map(|name| Ok((col(name, schema)?, (*name).to_string())))
            .collect::<Result<Vec<_>>>()?;
        Ok(PhysicalGroupBy::new_single(groups))
    }

    fn partial_aggregate(group_keys: &[&str]) -> Result<Arc<dyn ExecutionPlan>> {
        let input = input_exec();
        let schema = input.schema();
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by(group_keys, &schema)?,
            vec![],
            vec![],
            input,
            schema,
        )?))
    }

    fn hash_columns(distribution: Distribution) -> Vec<(String, usize)> {
        let Distribution::HashPartitioned(exprs) = distribution else {
            panic!("expected hash distribution");
        };
        exprs
            .into_iter()
            .map(|expr| {
                let column = expr
                    .as_any()
                    .downcast_ref::<datafusion_physical_expr::expressions::Column>()
                    .unwrap();
                (column.name().to_string(), column.index())
            })
            .collect()
    }

    #[test]
    fn maps_partial_aggregate_hash_requirement_to_input_columns() -> Result<()> {
        let aggregate = partial_aggregate(&["b", "a", "c"])?;
        let required = Distribution::HashPartitioned(vec![
            aggregate
                .as_any()
                .downcast_ref::<AggregateExec>()
                .unwrap()
                .output_group_expr()[0]
                .clone(),
            aggregate
                .as_any()
                .downcast_ref::<AggregateExec>()
                .unwrap()
                .output_group_expr()[1]
                .clone(),
        ]);

        let mapped = ReduceAggregateRepartition::partial_aggregate_input_distribution(
            &aggregate,
            Some(&required),
        )
        .unwrap();

        assert_eq!(
            hash_columns(mapped),
            vec![("b".to_string(), 1), ("a".to_string(), 0)]
        );
        Ok(())
    }

    #[test]
    fn rejects_non_grouping_hash_requirement_for_partial_aggregate() -> Result<()> {
        let aggregate = partial_aggregate(&["b", "a"])?;
        let unrelated = Distribution::HashPartitioned(vec![Arc::new(Column::new("c", 2)) as _]);

        assert!(
            ReduceAggregateRepartition::partial_aggregate_input_distribution(
                &aggregate,
                Some(&unrelated),
            )
            .is_none()
        );
        Ok(())
    }
}
