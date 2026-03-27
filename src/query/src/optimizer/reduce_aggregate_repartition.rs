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
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, InputOrderMode};
use datafusion_common::Result as DfResult;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::{Partitioning, PhysicalExpr, physical_exprs_equal};

/// Replaces a redundant hash repartition before a coarser aggregate with a
/// single fan-in.
///
/// This only applies when the aggregate already receives explicit hash
/// partitioning on its final grouping keys and the repartition input is already
/// hash partitioned on a strict superset of those keys.
#[derive(Debug)]
pub struct ReduceAggregateRepartition;

impl PhysicalOptimizerRule for ReduceAggregateRepartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Self::do_optimize(plan)
    }

    fn name(&self) -> &str {
        "ReduceAggregateRepartition"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

impl ReduceAggregateRepartition {
    fn do_optimize(plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            let Some(agg_exec) = plan.as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(plan));
            };

            if let Some(new_plan) = Self::combine_coalesced_min_max_partial_final(agg_exec)? {
                return Ok(Transformed::yes(new_plan));
            }

            let new_mode = match agg_exec.mode() {
                AggregateMode::FinalPartitioned => AggregateMode::Final,
                AggregateMode::SinglePartitioned => AggregateMode::Single,
                _ => return Ok(Transformed::no(plan)),
            };

            if agg_exec.input_order_mode() != &InputOrderMode::Linear
                || agg_exec.group_expr().has_grouping_set()
            {
                return Ok(Transformed::no(plan));
            }

            let Some(repartition_exec) =
                agg_exec.input().as_any().downcast_ref::<RepartitionExec>()
            else {
                return Ok(Transformed::no(plan));
            };

            if repartition_exec.preserve_order() {
                return Ok(Transformed::no(plan));
            }

            let Partitioning::Hash(final_partition_exprs, _) = repartition_exec.partitioning()
            else {
                return Ok(Transformed::no(plan));
            };

            let Partitioning::Hash(finer_partition_exprs, _) =
                repartition_exec.input().output_partitioning()
            else {
                return Ok(Transformed::no(plan));
            };

            let group_exprs = agg_exec.group_expr().input_exprs();
            if !physical_exprs_equal(group_exprs.as_slice(), final_partition_exprs.as_slice())
                || !is_strict_subset(final_partition_exprs, finer_partition_exprs)
            {
                return Ok(Transformed::no(plan));
            }

            let new_input = Arc::new(CoalescePartitionsExec::new(
                repartition_exec.input().clone(),
            ));
            let new_agg = AggregateExec::try_new(
                new_mode,
                agg_exec.group_expr().clone(),
                agg_exec.aggr_expr().to_vec(),
                agg_exec.filter_expr().to_vec(),
                new_input,
                agg_exec.input_schema(),
            )?
            .with_limit_options(agg_exec.limit_options());

            Ok(Transformed::yes(Arc::new(new_agg)))
        })
        .data()
    }

    fn combine_coalesced_min_max_partial_final(
        agg_exec: &AggregateExec,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        if agg_exec.mode() != &AggregateMode::Final {
            return Ok(None);
        }

        let Some(coalesce_exec) = agg_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
        else {
            return Ok(None);
        };

        let Some(partial_exec) = coalesce_exec
            .input()
            .as_any()
            .downcast_ref::<AggregateExec>()
        else {
            return Ok(None);
        };

        if *partial_exec.mode() != AggregateMode::Partial
            || !supports_min_max_family(agg_exec.aggr_expr())
            || !supports_min_max_family(partial_exec.aggr_expr())
            || !can_combine(
                (
                    agg_exec.group_expr(),
                    agg_exec.aggr_expr(),
                    agg_exec.filter_expr(),
                ),
                (
                    partial_exec.group_expr(),
                    partial_exec.aggr_expr(),
                    partial_exec.filter_expr(),
                ),
            )
        {
            return Ok(None);
        }

        let new_input = Arc::new(CoalescePartitionsExec::new(partial_exec.input().clone()));
        let new_agg = AggregateExec::try_new(
            AggregateMode::Single,
            partial_exec.group_expr().clone(),
            partial_exec.aggr_expr().to_vec(),
            partial_exec.filter_expr().to_vec(),
            new_input,
            partial_exec.input_schema(),
        )?
        .with_limit_options(agg_exec.limit_options());

        Ok(Some(Arc::new(new_agg)))
    }
}

type GroupExprsRef<'a> = (
    &'a datafusion::physical_plan::aggregates::PhysicalGroupBy,
    &'a [Arc<AggregateFunctionExpr>],
    &'a [Option<Arc<dyn PhysicalExpr>>],
);

fn can_combine(final_agg: GroupExprsRef<'_>, partial_agg: GroupExprsRef<'_>) -> bool {
    let (final_group_by, final_aggr_expr, final_filter_expr) = final_agg;
    let (input_group_by, input_aggr_expr, input_filter_expr) = partial_agg;

    physical_exprs_equal(
        &input_group_by.output_exprs(),
        &final_group_by.input_exprs(),
    ) && input_group_by.groups() == final_group_by.groups()
        && input_group_by.null_expr().len() == final_group_by.null_expr().len()
        && input_group_by
            .null_expr()
            .iter()
            .zip(final_group_by.null_expr().iter())
            .all(|((lhs_expr, lhs_str), (rhs_expr, rhs_str))| {
                lhs_expr.eq(rhs_expr) && lhs_str == rhs_str
            })
        && final_aggr_expr.len() == input_aggr_expr.len()
        && final_aggr_expr
            .iter()
            .zip(input_aggr_expr.iter())
            .all(|(final_expr, partial_expr)| final_expr.eq(partial_expr))
        && final_filter_expr.len() == input_filter_expr.len()
        && final_filter_expr.iter().zip(input_filter_expr.iter()).all(
            |(final_expr, partial_expr)| match (final_expr, partial_expr) {
                (Some(l), Some(r)) => l.eq(r),
                (None, None) => true,
                _ => false,
            },
        )
}

fn supports_min_max_family(aggr_exprs: &[Arc<AggregateFunctionExpr>]) -> bool {
    !aggr_exprs.is_empty()
        && aggr_exprs.iter().all(|expr| {
            let name = expr.fun().name();
            name.eq_ignore_ascii_case("min") || name.eq_ignore_ascii_case("max")
        })
}

fn is_strict_subset(
    subset_exprs: &[Arc<dyn datafusion_physical_expr::PhysicalExpr>],
    superset_exprs: &[Arc<dyn datafusion_physical_expr::PhysicalExpr>],
) -> bool {
    if subset_exprs.is_empty() || subset_exprs.len() >= superset_exprs.len() {
        return false;
    }

    subset_exprs
        .iter()
        .all(|subset_expr| superset_exprs.iter().any(|expr| subset_expr.eq(expr)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::functions_aggregate::min_max::min_udaf;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion_common::Result;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::{Partitioning, PhysicalExpr, aggregate::AggregateFunctionExpr};
    use pretty_assertions::assert_eq;

    use super::ReduceAggregateRepartition;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]))
    }

    fn input_exec(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        let config = MemorySourceConfig::try_new(&[vec![]], schema, None).unwrap();
        DataSourceExec::from_data_source(config)
    }

    fn group_by(names: &[&str], schema: &SchemaRef) -> Result<PhysicalGroupBy> {
        let groups: Result<Vec<(Arc<dyn PhysicalExpr>, String)>> = names
            .iter()
            .map(|name| Ok((col(name, schema)?, (*name).to_string())))
            .collect();
        Ok(PhysicalGroupBy::new_single(groups?))
    }

    fn repartition(
        input: Arc<dyn ExecutionPlan>,
        keys: &[&str],
        schema: &SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exprs = keys
            .iter()
            .map(|name| col(name, schema))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::Hash(exprs, 8),
        )?))
    }

    fn aggregate(
        mode: AggregateMode,
        input: Arc<dyn ExecutionPlan>,
        group_by: PhysicalGroupBy,
        input_schema: SchemaRef,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter_expr = vec![None; aggr_expr.len()];
        Ok(Arc::new(AggregateExec::try_new(
            mode,
            group_by,
            aggr_expr,
            filter_expr,
            input,
            input_schema,
        )?))
    }

    fn min_expr(name: &str, schema: &SchemaRef) -> Result<Arc<AggregateFunctionExpr>> {
        Ok(Arc::new(
            AggregateExprBuilder::new(min_udaf(), vec![col(name, schema)?])
                .schema(schema.clone())
                .alias(format!("min({name})"))
                .build()?,
        ))
    }

    fn count_expr(name: &str, schema: &SchemaRef) -> Result<Arc<AggregateFunctionExpr>> {
        Ok(Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col(name, schema)?])
                .schema(schema.clone())
                .alias(format!("count({name})"))
                .build()?,
        ))
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        let optimized = ReduceAggregateRepartition.optimize(plan, &Default::default())?;
        Ok(displayable(optimized.as_ref()).indent(true).to_string())
    }

    #[test]
    fn rewrites_final_partitioned_subset_repartition() -> Result<()> {
        let raw = input_exec(schema());
        let finer = repartition(raw.clone(), &["a", "b"], &raw.schema())?;
        let partial = aggregate(
            AggregateMode::Partial,
            finer,
            group_by(&["a", "b"], &raw.schema())?,
            raw.schema(),
            vec![],
        )?;
        let final_repartition = repartition(partial.clone(), &["a"], &partial.schema())?;
        let final_agg = aggregate(
            AggregateMode::FinalPartitioned,
            final_repartition,
            group_by(&["a"], &partial.schema())?,
            raw.schema(),
            vec![],
        )?;

        assert_eq!(
            optimize(final_agg)?.trim(),
            r#"AggregateExec: mode=Final, gby=[a@0 as a], aggr=[]
  CoalescePartitionsExec
    AggregateExec: mode=Partial, gby=[a@0 as a, b@1 as b], aggr=[]
      RepartitionExec: partitioning=Hash([a@0, b@1], 8), input_partitions=1
        DataSourceExec: partitions=1, partition_sizes=[0]"#
        );
        Ok(())
    }

    #[test]
    fn rewrites_single_partitioned_subset_repartition() -> Result<()> {
        let raw = input_exec(schema());
        let finer = repartition(raw.clone(), &["a", "b"], &raw.schema())?;
        let final_repartition = repartition(finer.clone(), &["a"], &finer.schema())?;
        let final_agg = aggregate(
            AggregateMode::SinglePartitioned,
            final_repartition,
            group_by(&["a"], &finer.schema())?,
            raw.schema(),
            vec![],
        )?;

        assert_eq!(
            optimize(final_agg)?.trim(),
            r#"AggregateExec: mode=Single, gby=[a@0 as a], aggr=[]
  CoalescePartitionsExec
    RepartitionExec: partitioning=Hash([a@0, b@1], 8), input_partitions=1
      DataSourceExec: partitions=1, partition_sizes=[0]"#
        );
        Ok(())
    }

    #[test]
    fn keeps_equal_partitioning_keys() -> Result<()> {
        let raw = input_exec(schema());
        let finer = repartition(raw.clone(), &["a", "b"], &raw.schema())?;
        let partial = aggregate(
            AggregateMode::Partial,
            finer,
            group_by(&["a", "b"], &raw.schema())?,
            raw.schema(),
            vec![],
        )?;
        let final_repartition = repartition(partial.clone(), &["a", "b"], &partial.schema())?;
        let final_agg = aggregate(
            AggregateMode::FinalPartitioned,
            final_repartition,
            group_by(&["a", "b"], &partial.schema())?,
            raw.schema(),
            vec![],
        )?;

        assert_eq!(
            optimize(final_agg)?.trim(),
            r#"AggregateExec: mode=FinalPartitioned, gby=[a@0 as a, b@1 as b], aggr=[]
  RepartitionExec: partitioning=Hash([a@0, b@1], 8), input_partitions=8
    AggregateExec: mode=Partial, gby=[a@0 as a, b@1 as b], aggr=[]
      RepartitionExec: partitioning=Hash([a@0, b@1], 8), input_partitions=1
        DataSourceExec: partitions=1, partition_sizes=[0]"#
        );
        Ok(())
    }

    #[test]
    fn combines_coalesced_partial_final_for_min() -> Result<()> {
        let raw = input_exec(schema());
        let finer = repartition(raw.clone(), &["a", "b"], &raw.schema())?;
        let aggr_expr = vec![min_expr("c", &raw.schema())?];
        let partial = aggregate(
            AggregateMode::Partial,
            finer,
            group_by(&["a"], &raw.schema())?,
            raw.schema(),
            aggr_expr.clone(),
        )?;
        let final_agg = aggregate(
            AggregateMode::Final,
            Arc::new(CoalescePartitionsExec::new(partial)),
            group_by(&["a"], &raw.schema())?,
            raw.schema(),
            aggr_expr,
        )?;

        assert_eq!(
            optimize(final_agg)?.trim(),
            r#"AggregateExec: mode=Single, gby=[a@0 as a], aggr=[min(c)]
  CoalescePartitionsExec
    RepartitionExec: partitioning=Hash([a@0, b@1], 8), input_partitions=1
      DataSourceExec: partitions=1, partition_sizes=[0]"#
        );
        Ok(())
    }

    #[test]
    fn keeps_coalesced_partial_final_for_count() -> Result<()> {
        let raw = input_exec(schema());
        let finer = repartition(raw.clone(), &["a", "b"], &raw.schema())?;
        let aggr_expr = vec![count_expr("c", &raw.schema())?];
        let partial = aggregate(
            AggregateMode::Partial,
            finer,
            group_by(&["a"], &raw.schema())?,
            raw.schema(),
            aggr_expr.clone(),
        )?;
        let final_agg = aggregate(
            AggregateMode::Final,
            Arc::new(CoalescePartitionsExec::new(partial)),
            group_by(&["a"], &raw.schema())?,
            raw.schema(),
            aggr_expr,
        )?;

        assert_eq!(
            optimize(final_agg)?.trim(),
            r#"AggregateExec: mode=Final, gby=[a@0 as a], aggr=[count(c)]
  CoalescePartitionsExec
    AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[count(c)]
      RepartitionExec: partitioning=Hash([a@0, b@1], 8), input_partitions=1
        DataSourceExec: partitions=1, partition_sizes=[0]"#
        );
        Ok(())
    }
}
