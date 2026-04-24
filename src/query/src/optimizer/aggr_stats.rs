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
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result};
use table::table::scan::RegionScanExec;

use crate::optimizer::aggr_stats::stat_scan::StatsScanExec;
use crate::optimizer::aggr_stats::support_aggr::{
    SupportStatAggr, support_stat_aggr_from_aggr_expr,
};

pub(crate) mod stat_scan;
pub(crate) mod support_aggr;

/// Physical optimizer scaffold for aggregate-stats runtime rewrite.
///
/// This pass only owns query-shape eligibility and rewrite shape.
/// Runtime stats lookup/classification is handled during execution.
#[derive(Debug, Default)]
pub struct AggrStatsPhysicalRule;

impl PhysicalOptimizerRule for AggrStatsPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Self::rewrite_plan_shape(plan)
    }

    fn name(&self) -> &str {
        "aggr_stats_physical"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl AggrStatsPhysicalRule {
    fn rewrite_plan_shape(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            let Some(rewrite_target) = RewriteTarget::extract(&plan) else {
                return Ok(Transformed::no(plan));
            };
            rewrite_target.rewrite().map(Transformed::yes)
        })
        .map(|res| res.data)
    }
}

/// TODO(discord9): support more kind of aggr
#[allow(unused)]
enum RewriteTarget<'a> {
    FinalOverPartial {
        final_exec: &'a AggregateExec,
        partial_exec: &'a AggregateExec,
        region_scan: &'a RegionScanExec,
        keep_coalesce: bool,
        aggr_exprs: Vec<SupportStatAggr>,
    },
}

#[allow(unused)]
impl<'a> RewriteTarget<'a> {
    fn is_supported_rewrite(&self) -> bool {
        let Self::FinalOverPartial {
            final_exec,
            partial_exec,
            region_scan,
            ..
        } = self;

        final_exec.group_expr().is_empty()
            && final_exec
                .aggr_expr()
                .iter()
                .all(|aggr| !aggr.is_distinct())
            && final_exec
                .filter_expr()
                .iter()
                .all(|filter| filter.is_none())
            && partial_exec
                .filter_expr()
                .iter()
                .all(|filter| filter.is_none())
            && region_scan.append_mode()
    }

    #[allow(unused)]
    fn extract(plan: &'a Arc<dyn ExecutionPlan>) -> Option<Self> {
        let aggregate_exec = plan.as_any().downcast_ref::<AggregateExec>()?;

        if !matches!(aggregate_exec.mode(), AggregateMode::Final) {
            return None;
        }

        let (input, keep_coalesce) = if let Some(coalesce) = aggregate_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>(
        ) {
            (coalesce.input(), true)
        } else {
            (aggregate_exec.input(), false)
        };

        let partial_exec = input.as_any().downcast_ref::<AggregateExec>()?;
        if !matches!(partial_exec.mode(), AggregateMode::Partial) {
            return None;
        }

        let region_scan = partial_exec
            .input()
            .as_any()
            .downcast_ref::<RegionScanExec>()?;
        let aggr_exprs = aggregate_exec
            .aggr_expr()
            .iter()
            .map(|aggr_expr| {
                support_stat_aggr_from_aggr_expr(aggr_expr.as_ref(), &region_scan.time_index())
            })
            .try_collect()?;
        let zelf = Self::FinalOverPartial {
            final_exec: aggregate_exec,
            partial_exec,
            region_scan,
            keep_coalesce,
            aggr_exprs,
        };
        if zelf.is_supported_rewrite() {
            Some(zelf)
        } else {
            None
        }
    }

    fn rewrite(&self) -> Result<Arc<dyn ExecutionPlan>> {
        match self {
            Self::FinalOverPartial {
                final_exec,
                partial_exec,
                region_scan,
                keep_coalesce,
                aggr_exprs,
            } => {
                let requirements = aggr_exprs.clone();
                let stats_scan = Arc::new(StatsScanExec::new(
                    partial_exec.schema(),
                    requirements.clone(),
                    region_scan.scanner(),
                ));

                let fallback_scan = Arc::new(
                    region_scan
                        .with_stats_aware_skip_requirements(requirements)
                        .map_err(|error| DataFusionError::External(error.into()))?,
                );
                let fallback_partial = Arc::new(AggregateExec::try_new(
                    *partial_exec.mode(),
                    Arc::new(partial_exec.group_expr().clone()),
                    partial_exec.aggr_expr().to_vec(),
                    partial_exec.filter_expr().to_vec(),
                    fallback_scan,
                    partial_exec.input_schema(),
                )?);

                let union = UnionExec::try_new(vec![stats_scan, fallback_partial])?;
                let merge_input: Arc<dyn ExecutionPlan> =
                    if *keep_coalesce || union.properties().partitioning.partition_count() > 1 {
                        Arc::new(CoalescePartitionsExec::new(union))
                    } else {
                        union
                    };

                let final_aggregate = AggregateExec::try_new(
                    *final_exec.mode(),
                    Arc::new(final_exec.group_expr().clone()),
                    final_exec.aggr_expr().to_vec(),
                    final_exec.filter_expr().to_vec(),
                    merge_input,
                    final_exec.input_schema(),
                )?;

                Ok(Arc::new(final_aggregate))
            }
        }
    }

    fn first_stage_aggregate(&self) -> &'a AggregateExec {
        match self {
            RewriteTarget::FinalOverPartial { partial_exec, .. } => partial_exec,
        }
    }

    fn region_scan(&self) -> &'a RegionScanExec {
        match self {
            RewriteTarget::FinalOverPartial { region_scan, .. } => region_scan,
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use common_recordbatch::EmptyRecordBatchStream;
    use datafusion::functions_aggregate::average::avg_udaf;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::utils::COUNT_STAR_EXPANSION;
    use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
    use datafusion_physical_expr::expressions::{Column as PhysicalColumn, Literal};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::region_engine::SinglePartitionScanner;
    use store_api::storage::{RegionId, ScanRequest};

    use super::*;

    fn build_count_expr(schema: arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![Arc::new(PhysicalColumn::new("v0", 0))])
                .schema(schema)
                .alias("count(v0)")
                .build()
                .unwrap(),
        )
    }

    fn build_count_time_index_expr(schema: arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![Arc::new(PhysicalColumn::new("ts", 1))])
                .schema(schema)
                .alias("count(ts)")
                .build()
                .unwrap(),
        )
    }

    fn build_count_star_expr(schema: arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(
                count_udaf(),
                vec![Arc::new(Literal::new(COUNT_STAR_EXPANSION.clone()))],
            )
            .schema(schema)
            .alias("count(*)")
            .build()
            .unwrap(),
        )
    }

    fn build_count_null_expr(schema: arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(
                count_udaf(),
                vec![Arc::new(Literal::new(ScalarValue::Null))],
            )
            .schema(schema)
            .alias("count(NULL)")
            .build()
            .unwrap(),
        )
    }

    fn build_avg_expr(schema: arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::new(PhysicalColumn::new("v0", 0))])
                .schema(schema)
                .alias("avg(v0)")
                .build()
                .unwrap(),
        )
    }

    fn group_by_v0() -> PhysicalGroupBy {
        PhysicalGroupBy::new_single(vec![(
            Arc::new(PhysicalColumn::new("v0", 0)),
            "v0".to_string(),
        )])
    }

    fn build_region_scan(append_mode: bool) -> Arc<RegionScanExec> {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]));
        let stream = Box::pin(EmptyRecordBatchStream::new(schema.clone()));

        let mut metadata_builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        metadata_builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![]);
        let metadata = Arc::new(metadata_builder.build().unwrap());

        let scanner = Box::new(SinglePartitionScanner::new(
            stream,
            append_mode,
            metadata,
            None,
        ));
        Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap())
    }

    fn build_final_over_partial_plan() -> Arc<dyn ExecutionPlan> {
        build_final_over_partial_plan_with(build_region_scan(true), build_count_expr, None)
    }

    fn build_final_over_partial_plan_with(
        region_scan: Arc<RegionScanExec>,
        build_aggr_expr: fn(arrow_schema::SchemaRef) -> Arc<AggregateFunctionExpr>,
        group_by: Option<PhysicalGroupBy>,
    ) -> Arc<dyn ExecutionPlan> {
        let input_schema = region_scan.schema();
        let aggr_expr = build_aggr_expr(input_schema.clone());
        let group_by = group_by.unwrap_or_default();

        let partial = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                group_by.clone(),
                vec![aggr_expr.clone()],
                vec![None],
                region_scan,
                input_schema.clone(),
            )
            .unwrap(),
        );

        let coalesce = Arc::new(CoalescePartitionsExec::new(partial));
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                group_by,
                vec![aggr_expr],
                vec![None],
                coalesce,
                input_schema,
            )
            .unwrap(),
        )
    }

    #[test]
    fn rewrite_builds_stats_scan_union_with_stats_aware_fallback() {
        let plan = build_final_over_partial_plan();
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        let final_exec = optimized.as_any().downcast_ref::<AggregateExec>().unwrap();
        assert!(matches!(final_exec.mode(), AggregateMode::Final));

        let coalesce = final_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        let union = coalesce
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .unwrap();
        let union_children = union.children();
        assert_eq!(union_children.len(), 2);

        let stats_scan = union_children[0]
            .as_any()
            .downcast_ref::<StatsScanExec>()
            .unwrap();
        assert_eq!(
            stats_scan.requirements(),
            &[SupportStatAggr::CountNonNull {
                column_name: "v0".to_string(),
            }]
        );

        let fallback_partial = union_children[1]
            .as_any()
            .downcast_ref::<AggregateExec>()
            .unwrap();
        assert!(matches!(fallback_partial.mode(), AggregateMode::Partial));

        let fallback_scan = fallback_partial
            .input()
            .as_any()
            .downcast_ref::<RegionScanExec>()
            .unwrap();
        assert_eq!(
            fallback_scan.stats_aware_skip_requirements(),
            &[SupportStatAggr::CountNonNull {
                column_name: "v0".to_string(),
            }]
        );
    }

    #[test]
    fn rewrite_ignores_unsupported_avg_aggregate() {
        let plan =
            build_final_over_partial_plan_with(build_region_scan(true), build_avg_expr, None);
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_final_over_partial_without_union(&optimized);
    }

    #[test]
    fn rewrite_ignores_grouped_aggregate() {
        let plan = build_final_over_partial_plan_with(
            build_region_scan(true),
            build_count_expr,
            Some(group_by_v0()),
        );
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_final_over_partial_without_union(&optimized);
    }

    #[test]
    fn rewrite_ignores_non_append_region_scan() {
        let plan =
            build_final_over_partial_plan_with(build_region_scan(false), build_count_expr, None);
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_final_over_partial_without_union(&optimized);
    }

    #[test]
    fn rewrite_maps_count_star_to_count_rows() {
        let optimized = AggrStatsPhysicalRule
            .optimize(
                build_final_over_partial_plan_with(
                    build_region_scan(true),
                    build_count_star_expr,
                    None,
                ),
                &ConfigOptions::default(),
            )
            .unwrap();

        assert_rewritten_stats_requirement(&optimized, &[SupportStatAggr::CountRows]);
    }

    #[test]
    fn rewrite_maps_count_time_index_to_count_rows() {
        let optimized = AggrStatsPhysicalRule
            .optimize(
                build_final_over_partial_plan_with(
                    build_region_scan(true),
                    build_count_time_index_expr,
                    None,
                ),
                &ConfigOptions::default(),
            )
            .unwrap();

        assert_rewritten_stats_requirement(&optimized, &[SupportStatAggr::CountRows]);
    }

    #[test]
    fn rewrite_ignores_count_null_literal() {
        let plan = build_final_over_partial_plan_with(
            build_region_scan(true),
            build_count_null_expr,
            None,
        );
        let optimized = AggrStatsPhysicalRule
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_final_over_partial_without_union(&optimized);
    }

    fn assert_rewritten_stats_requirement(
        plan: &Arc<dyn ExecutionPlan>,
        expected: &[SupportStatAggr],
    ) {
        let final_exec = plan.as_any().downcast_ref::<AggregateExec>().unwrap();
        let coalesce = final_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        let union = coalesce
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .unwrap();
        let union_children = union.children();
        let stats_scan = union_children[0]
            .as_any()
            .downcast_ref::<StatsScanExec>()
            .unwrap();
        assert_eq!(stats_scan.requirements(), expected);

        let fallback_partial = union_children[1]
            .as_any()
            .downcast_ref::<AggregateExec>()
            .unwrap();
        let fallback_scan = fallback_partial
            .input()
            .as_any()
            .downcast_ref::<RegionScanExec>()
            .unwrap();
        assert_eq!(fallback_scan.stats_aware_skip_requirements(), expected);
    }

    fn assert_final_over_partial_without_union(plan: &Arc<dyn ExecutionPlan>) {
        let final_exec = plan.as_any().downcast_ref::<AggregateExec>().unwrap();
        assert!(matches!(final_exec.mode(), AggregateMode::Final));

        let coalesce = final_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .unwrap();
        assert!(
            coalesce
                .input()
                .as_any()
                .downcast_ref::<UnionExec>()
                .is_none()
        );

        let partial_exec = coalesce
            .input()
            .as_any()
            .downcast_ref::<AggregateExec>()
            .unwrap();
        assert!(matches!(partial_exec.mode(), AggregateMode::Partial));

        let region_scan = partial_exec
            .input()
            .as_any()
            .downcast_ref::<RegionScanExec>()
            .unwrap();
        assert!(region_scan.stats_aware_skip_requirements().is_empty());
    }
}
