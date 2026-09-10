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

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use promql::extension_plan::{RangeManipulateExec, SeriesAggregateExec, SeriesFinalAggregateExec};

/// Replaces the aggregate chain of a PromQL range query with the series-aware operators,
/// when the plan proves its input batches come one series at a time.
#[derive(Debug)]
pub struct PromqlSeriesAggregate;

impl PhysicalOptimizerRule for PromqlSeriesAggregate {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan
            .transform_up(|plan| {
                if let Some(candidate) = SeriesFinalAggregateExec::try_new(&plan)? {
                    return Ok(Transformed::yes(
                        Arc::new(candidate) as Arc<dyn ExecutionPlan>
                    ));
                }
                let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() else {
                    return Ok(Transformed::no(plan));
                };
                let Some((start, end, step)) = series_grid(aggregate) else {
                    return Ok(Transformed::no(plan));
                };
                match SeriesAggregateExec::try_new(aggregate, start, end, step)? {
                    Some(candidate) => Ok(Transformed::yes(
                        Arc::new(candidate) as Arc<dyn ExecutionPlan>
                    )),
                    None => Ok(Transformed::no(plan)),
                }
            })
            .data()?;
        rewrite_root_final(plan)
    }

    fn name(&self) -> &str {
        "PromqlSeriesAggregate"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

fn rewrite_root_final(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    // HTTP range queries remove their output sort. Only root projections can
    // inherit a reduced partition count here; joins, windows and limits retain
    // their existing input contracts and are not traversed by this rewrite.
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let input = rewrite_root_final(projection.input().clone())?;
        if Arc::ptr_eq(&input, projection.input()) {
            return Ok(plan);
        }
        return plan.with_new_children(vec![input]);
    }
    Ok(match SeriesFinalAggregateExec::try_new_unordered(&plan)? {
        Some(candidate) => Arc::new(candidate),
        None => plan,
    })
}

fn series_grid(aggregate: &AggregateExec) -> Option<(i64, i64, i64)> {
    let filter = aggregate.input().as_any().downcast_ref::<FilterExec>()?;
    let mut input = filter.input();
    let mut columns = aggregate
        .group_expr()
        .expr()
        .iter()
        .map(|(expr, _)| expr.as_any().downcast_ref::<Column>().map(Column::index))
        .collect::<Option<Vec<_>>>()?;
    // Trace column lineage, not names or __tsid: only direct projections preserve this proof.
    while let Some(projection) = input.as_any().downcast_ref::<ProjectionExec>() {
        for column in &mut columns {
            *column = projection
                .expr()
                .get(*column)?
                .expr
                .as_any()
                .downcast_ref::<Column>()?
                .index();
        }
        input = projection.input();
    }
    input
        .as_any()
        .downcast_ref::<RangeManipulateExec>()?
        .series_group_grid(&columns)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::{DFSchema, ScalarValue};
    use datafusion::functions_aggregate::average::avg_udaf;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{IsNotNullExpr, Literal};
    use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExecBuilder;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use promql::extension_plan::RangeManipulate;

    use super::*;

    fn aggregate(
        series_origin: bool,
        rebatched: bool,
        computed_label: bool,
        filter_fetch: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("raw_value", DataType::Float64, true),
            Field::new(
                "host",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8View)),
                true,
            ),
        ]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let input = if series_origin {
            let logical = LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::try_from(schema.as_ref().clone()).unwrap()),
            });
            RangeManipulate::new(
                -1000,
                35000,
                1000,
                5000,
                "ts".to_string(),
                vec!["raw_value".to_string()],
                logical,
            )
            .unwrap()
            .to_execution_plan(input)
        } else {
            input
        };
        // Anything that can redistribute rows breaks the one-series-per-batch property.
        let input: Arc<dyn ExecutionPlan> = if rebatched {
            Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(2)).unwrap())
        } else {
            input
        };
        let label: Arc<dyn PhysicalExpr> = if computed_label {
            Arc::new(Literal::new(ScalarValue::Utf8View(Some(
                "derived".to_string(),
            ))))
        } else {
            Arc::new(Column::new("host", 2))
        };
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (label, "renamed_host".to_string()),
                    (
                        Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>,
                        "renamed_ts".to_string(),
                    ),
                    (
                        Arc::new(Literal::new(ScalarValue::Float64(Some(1.0))))
                            as Arc<dyn PhysicalExpr>,
                        "rate".to_string(),
                    ),
                ],
                input,
            )
            .unwrap(),
        );
        let schema = projection.schema();
        let filter = Arc::new(
            FilterExecBuilder::new(
                Arc::new(IsNotNullExpr::new(Arc::new(Column::new("rate", 2)))),
                projection,
            )
            .with_fetch(filter_fetch.then_some(1))
            .build()
            .unwrap(),
        );
        let groups = PhysicalGroupBy::new_single(vec![
            (
                Arc::new(Column::new("renamed_host", 0)),
                "renamed_host".to_string(),
            ),
            (
                Arc::new(Column::new("renamed_ts", 1)),
                "renamed_ts".to_string(),
            ),
        ]);
        let avg = Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::new(Column::new("rate", 2))])
                .schema(schema.clone())
                .alias("avg")
                .build()
                .unwrap(),
        );
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                groups,
                vec![avg],
                vec![None],
                filter,
                schema,
            )
            .unwrap(),
        )
    }

    #[test]
    fn requires_series_origin_and_direct_group_column_lineage() {
        let config = ConfigOptions::default();
        let input = aggregate(true, false, false, false);
        let expected_schema = input.schema();
        let optimized = PromqlSeriesAggregate.optimize(input, &config).unwrap();
        assert!(optimized.as_any().is::<SeriesAggregateExec>());
        assert_eq!(optimized.schema(), expected_schema);
        SanityCheckPlan {}.optimize(optimized, &config).unwrap();
        for input in [
            aggregate(false, false, false, false),
            aggregate(true, true, false, false),
            aggregate(true, false, true, false),
            aggregate(true, false, false, true),
        ] {
            let optimized = PromqlSeriesAggregate.optimize(input, &config).unwrap();
            assert!(optimized.as_any().is::<AggregateExec>());
        }
    }

    #[test]
    fn rewrites_partial_and_sorted_final_in_one_pass() {
        use datafusion::arrow::compute::SortOptions;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_plan::Partitioning;
        use datafusion::physical_plan::repartition::RepartitionExec;
        use datafusion::physical_plan::sorts::sort::SortExec;
        use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

        let input = aggregate(true, false, false, false);
        let partial = input.as_any().downcast_ref::<AggregateExec>().unwrap();
        let columns = partial.output_group_expr();
        let repartition = Arc::new(
            RepartitionExec::try_new(input.clone(), Partitioning::Hash(columns.clone(), 4))
                .unwrap(),
        );
        let final_plan: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::FinalPartitioned,
                partial.group_expr().as_final(),
                partial.aggr_expr().to_vec(),
                vec![None],
                repartition,
                partial.input_schema(),
            )
            .unwrap(),
        );
        let order = LexOrdering::new(columns.clone().into_iter().map(|expr| PhysicalSortExpr {
            expr,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }))
        .unwrap();
        let sort = Arc::new(
            SortExec::new(order.clone(), final_plan.clone()).with_preserve_partitioning(true),
        );
        let root: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(order, sort));
        let expected = root.schema();
        let optimized = PromqlSeriesAggregate
            .optimize(root, &ConfigOptions::default())
            .unwrap();
        assert!(optimized.as_any().is::<SeriesFinalAggregateExec>());
        assert!(optimized.children()[0].as_any().is::<SeriesAggregateExec>());
        assert_eq!(optimized.schema(), expected);
        SanityCheckPlan {}
            .optimize(optimized, &ConfigOptions::default())
            .unwrap();

        for projected in [false, true] {
            let root =
                if projected {
                    Arc::new(
                        ProjectionExec::try_new(
                            final_plan.schema().fields().iter().enumerate().map(
                                |(index, field)| {
                                    (
                                        Arc::new(Column::new(field.name(), index))
                                            as Arc<dyn PhysicalExpr>,
                                        field.name().clone(),
                                    )
                                },
                            ),
                            final_plan.clone(),
                        )
                        .unwrap(),
                    ) as Arc<dyn ExecutionPlan>
                } else {
                    final_plan.clone()
                };
            let optimized = PromqlSeriesAggregate
                .optimize(root, &ConfigOptions::default())
                .unwrap();
            let aggregate = if projected {
                optimized.children()[0].clone()
            } else {
                optimized.clone()
            };
            assert!(aggregate.as_any().is::<SeriesFinalAggregateExec>());
            assert_eq!(optimized.properties().partitioning.partition_count(), 1);
            SanityCheckPlan {}
                .optimize(optimized, &ConfigOptions::default())
                .unwrap();
        }

        let partitioned =
            Arc::new(RepartitionExec::try_new(final_plan, Partitioning::Hash(columns, 4)).unwrap());
        let optimized = PromqlSeriesAggregate
            .optimize(partitioned, &ConfigOptions::default())
            .unwrap();
        assert!(optimized.children()[0].as_any().is::<AggregateExec>());
        assert_eq!(optimized.properties().partitioning.partition_count(), 4);
        SanityCheckPlan {}
            .optimize(optimized, &ConfigOptions::default())
            .unwrap();
    }
}
