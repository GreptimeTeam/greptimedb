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
use datafusion_physical_expr::{Distribution, Partitioning};
use promql::extension_plan::{
    InstantManipulateExec, RangeManipulateExec, SeriesDivideExec, SeriesNormalizeExec,
};

/// Replaces a redundant hash repartition before a coarser aggregate with a
/// single fan-in.
///
/// This only applies when the aggregate already receives hash partitioning
/// satisfying its grouping keys and the repartition input is already hash
/// partitioned on a strict superset of those repartition keys.
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

            let Some(required_distribution) =
                agg_exec.required_input_distribution().into_iter().next()
            else {
                return Ok(Transformed::no(plan));
            };
            let repartition_satisfaction = repartition_exec.partitioning().satisfaction(
                &required_distribution,
                repartition_exec.properties().equivalence_properties(),
                true,
            );
            if !repartition_satisfaction.is_satisfied() {
                return Ok(Transformed::no(plan));
            }

            if !Self::can_reduce_repartition(repartition_exec) {
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

    fn can_reduce_repartition(repartition_exec: &RepartitionExec) -> bool {
        let has_direct_promql_input =
            Self::has_direct_promql_partial_input(repartition_exec.input());
        if Self::contains_promql_exec_deep(repartition_exec.input()) {
            return has_direct_promql_input;
        }

        let Partitioning::Hash(finer_partition_exprs, _) =
            repartition_exec.input().output_partitioning()
        else {
            return false;
        };

        let coarsening_satisfaction = repartition_exec.partitioning().satisfaction(
            &Distribution::HashPartitioned(finer_partition_exprs.clone()),
            repartition_exec
                .input()
                .properties()
                .equivalence_properties(),
            true,
        );
        coarsening_satisfaction.is_subset()
    }

    fn has_direct_promql_partial_input(plan: &Arc<dyn ExecutionPlan>) -> bool {
        let Some(partial_agg) = plan.as_any().downcast_ref::<AggregateExec>() else {
            return false;
        };

        partial_agg.mode() == &AggregateMode::Partial
            && Self::contains_promql_vector_exec(partial_agg.input())
    }

    fn contains_promql_vector_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if Self::is_promql_vector_exec(plan) {
            return true;
        }

        if plan.as_any().is::<AggregateExec>() {
            return false;
        }

        plan.children()
            .into_iter()
            .any(Self::contains_promql_vector_exec)
    }

    fn contains_promql_exec_deep(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if Self::is_promql_vector_exec(plan) {
            return true;
        }

        plan.children()
            .into_iter()
            .any(Self::contains_promql_exec_deep)
    }

    fn is_promql_vector_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
        let plan = plan.as_any();
        plan.is::<SeriesDivideExec>()
            || plan.is::<SeriesNormalizeExec>()
            || plan.is::<RangeManipulateExec>()
            || plan.is::<InstantManipulateExec>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion_common::Result;
    use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::{Partitioning, PhysicalExpr};
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

    fn round_robin_repartition(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::RoundRobinBatch(8),
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

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        let optimized = ReduceAggregateRepartition.optimize(plan, &Default::default())?;
        Ok(displayable(optimized.as_ref()).indent(true).to_string())
    }

    fn project_with_aliases(
        input: Arc<dyn ExecutionPlan>,
        aliases: &[(&str, &str)],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exprs: Result<Vec<(Arc<dyn PhysicalExpr>, String)>> = aliases
            .iter()
            .map(|(from, to)| Ok((col(from, &input.schema())?, (*to).to_string())))
            .collect();
        Ok(Arc::new(ProjectionExec::try_new(exprs?, input)?))
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
    fn rewrites_when_finer_key_order_differs() -> Result<()> {
        let raw = input_exec(schema());
        let finer = repartition(raw.clone(), &["c", "a", "b"], &raw.schema())?;
        let partial = aggregate(
            AggregateMode::Partial,
            finer,
            group_by(&["c", "a", "b"], &raw.schema())?,
            raw.schema(),
            vec![],
        )?;
        let final_repartition = repartition(partial.clone(), &["b", "c"], &partial.schema())?;
        let final_agg = aggregate(
            AggregateMode::FinalPartitioned,
            final_repartition,
            group_by(&["b", "c"], &partial.schema())?,
            raw.schema(),
            vec![],
        )?;

        assert_eq!(
            optimize(final_agg)?.trim(),
            r#"AggregateExec: mode=Final, gby=[b@2 as b, c@0 as c], aggr=[]
  CoalescePartitionsExec
    AggregateExec: mode=Partial, gby=[c@2 as c, a@0 as a, b@1 as b], aggr=[]
      RepartitionExec: partitioning=Hash([c@2, a@0, b@1], 8), input_partitions=1
        DataSourceExec: partitions=1, partition_sizes=[0]"#
        );
        Ok(())
    }

    #[test]
    fn rewrites_when_repartition_satisfies_group_by_with_subset_keys() -> Result<()> {
        let raw = input_exec(schema());
        let finer = repartition(raw.clone(), &["a", "b", "c"], &raw.schema())?;
        let final_repartition = repartition(finer.clone(), &["a"], &finer.schema())?;
        let final_agg = aggregate(
            AggregateMode::FinalPartitioned,
            final_repartition,
            group_by(&["a", "b"], &finer.schema())?,
            raw.schema(),
            vec![],
        )?;

        assert_eq!(
            optimize(final_agg)?.trim(),
            r#"AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[]
  CoalescePartitionsExec
    RepartitionExec: partitioning=Hash([a@0, b@1, c@2], 8), input_partitions=1
      DataSourceExec: partitions=1, partition_sizes=[0]"#
        );
        Ok(())
    }

    #[test]
    fn keeps_non_hash_repartition_child() -> Result<()> {
        let raw = input_exec(schema());
        let finer = repartition(raw.clone(), &["a", "b"], &raw.schema())?;
        let partial = aggregate(
            AggregateMode::Partial,
            finer,
            group_by(&["a", "b"], &raw.schema())?,
            raw.schema(),
            vec![],
        )?;
        let final_repartition = round_robin_repartition(partial.clone())?;
        let final_agg = aggregate(
            AggregateMode::FinalPartitioned,
            final_repartition,
            group_by(&["a"], &partial.schema())?,
            raw.schema(),
            vec![],
        )?;

        assert_eq!(
            optimize(final_agg)?.trim(),
            r#"AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[]
  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=8
    AggregateExec: mode=Partial, gby=[a@0 as a, b@1 as b], aggr=[]
      RepartitionExec: partitioning=Hash([a@0, b@1], 8), input_partitions=1
        DataSourceExec: partitions=1, partition_sizes=[0]"#
        );
        Ok(())
    }

    #[test]
    fn rewrites_subset_partitioning_through_projection() -> Result<()> {
        let raw = input_exec(schema());
        let finer = repartition(raw.clone(), &["a", "b", "c"], &raw.schema())?;
        let projected = project_with_aliases(finer, &[("a", "x"), ("b", "y"), ("c", "z")])?;
        let final_repartition = repartition(projected.clone(), &["x", "y"], &projected.schema())?;
        let final_agg = aggregate(
            AggregateMode::SinglePartitioned,
            final_repartition,
            group_by(&["x", "y"], &projected.schema())?,
            projected.schema(),
            vec![],
        )?;

        let optimized = optimize(final_agg)?;
        assert!(
            optimized.contains("AggregateExec: mode=Single, gby=[x@0 as x, y@1 as y], aggr=[]")
        );
        assert!(optimized.contains("CoalescePartitionsExec"));
        assert!(optimized.contains("ProjectionExec: expr=[a@0 as x, b@1 as y, c@2 as z]"));
        Ok(())
    }

    #[test]
    fn keeps_non_subset_repartition() -> Result<()> {
        let raw = input_exec(schema());
        let coarser = repartition(raw.clone(), &["a"], &raw.schema())?;
        let final_repartition = repartition(coarser.clone(), &["a", "b"], &coarser.schema())?;
        let final_agg = aggregate(
            AggregateMode::FinalPartitioned,
            final_repartition,
            group_by(&["a", "b"], &coarser.schema())?,
            raw.schema(),
            vec![],
        )?;

        let optimized = optimize(final_agg)?;
        assert!(
            optimized.contains(
                "AggregateExec: mode=FinalPartitioned, gby=[a@0 as a, b@1 as b], aggr=[]"
            ),
            "{optimized}"
        );
        assert!(
            optimized.contains("RepartitionExec: partitioning=Hash([a@0, b@1], 8)"),
            "{optimized}"
        );
        assert!(!optimized.contains("CoalescePartitionsExec"), "{optimized}");
        Ok(())
    }
}
