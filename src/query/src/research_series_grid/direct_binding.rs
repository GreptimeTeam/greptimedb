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

use common_recordbatch::OrderOption;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use promql::extension_plan::{InstantManipulate, SeriesDivide};
use store_api::storage::TimeSeriesDistribution;

use super::{SeriesFragment, SeriesInstantMax, SeriesRange};
use crate::dummy_catalog::DummyTableProvider;
use crate::research_series_grid::scan_range::{RangeOutput, RangeSpec};
use crate::research_series_grid::series_range::SeriesRangeExec;

pub(super) struct Source {
    pub(super) provider: Arc<dyn TableProvider>,
    pub(super) name: datafusion_common::TableReference,
    pub(super) columns: Vec<usize>,
    pub(super) filters: Vec<Expr>,
    pub(super) ordering: Option<Vec<usize>>,
    check_pushdown: bool,
}

impl Source {
    fn parse(plan: &LogicalPlan) -> Option<Self> {
        Self::parse_inner(plan, true)
    }

    pub(super) fn compile(plan: &LogicalPlan) -> Option<Self> {
        Self::parse_inner(plan, false)
    }

    fn parse_inner(plan: &LogicalPlan, check_pushdown: bool) -> Option<Self> {
        let mut source = match plan {
            LogicalPlan::TableScan(scan) if scan.fetch.is_none() => {
                let table = scan.source.downcast_ref::<DefaultTableSource>()?;
                let provider: Arc<dyn TableProvider> = if check_pushdown {
                    Arc::new(
                        table
                            .table_provider
                            .downcast_ref::<DummyTableProvider>()?
                            .clone_for_scan(),
                    )
                } else {
                    table.table_provider.clone()
                };
                let schema = provider.schema();
                if scan.source.schema() != schema {
                    return None;
                }
                let mut source = Self {
                    provider,
                    name: scan.table_name.clone(),
                    check_pushdown,
                    columns: (0..schema.fields().len()).collect(),
                    filters: Vec::new(),
                    ordering: None,
                };
                let full_schema =
                    DFSchema::try_from_qualified_schema(scan.table_name.clone(), schema.as_ref())
                        .ok()?;
                for filter in &scan.filters {
                    for predicate in datafusion_expr::utils::split_conjunction(filter) {
                        source.add_filter(predicate, &full_schema)?;
                    }
                }
                if let Some(projection) = &scan.projection {
                    source.columns = projection.clone();
                }
                let projected = schema.project(&source.columns).ok()?;
                if !scan.projected_schema.matches_arrow_schema(&projected) {
                    return None;
                }
                source
            }
            LogicalPlan::Projection(projection) => {
                let mut source = Self::parse_inner(&projection.input, check_pushdown)?;
                source.columns = projection
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = if let Expr::Alias(alias) = expr {
                            alias.expr.as_ref()
                        } else {
                            expr
                        };
                        let Expr::Column(column) = expr else {
                            return None;
                        };
                        source.column(column, projection.input.schema())
                    })
                    .collect::<Option<Vec<_>>>()?;
                source
            }
            LogicalPlan::SubqueryAlias(alias) => Self::parse_inner(&alias.input, check_pushdown)?,
            LogicalPlan::Filter(filter) => {
                let mut source = Self::parse_inner(&filter.input, check_pushdown)?;
                for predicate in datafusion_expr::utils::split_conjunction(&filter.predicate) {
                    source.add_filter(predicate, filter.input.schema())?;
                }
                source
            }
            LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
                let mut source = Self::parse_inner(&sort.input, check_pushdown)?;
                let ordering = sort
                    .expr
                    .iter()
                    .map(|order| {
                        let Expr::Column(column) = &order.expr else {
                            return None;
                        };
                        (order.asc && order.nulls_first)
                            .then(|| source.column(column, sort.input.schema()))?
                    })
                    .collect::<Option<Vec<_>>>()?;
                if source.ordering.as_ref().is_some_and(|old| *old != ordering) {
                    return None;
                }
                source.ordering = Some(ordering);
                source
            }
            _ => return None,
        };
        let schema = source.provider.schema();
        if plan.schema().fields().len() != source.columns.len()
            || plan
                .schema()
                .fields()
                .iter()
                .zip(&source.columns)
                .any(|(field, &index)| {
                    schema.fields().get(index).is_none_or(|actual| {
                        field.data_type() != actual.data_type()
                            || field.is_nullable() != actual.is_nullable()
                    })
                })
        {
            return None;
        }
        // Preserve the first occurrence, including conjunctions repeated by the
        // frontend's inexact pushdown. Different predicates on one column remain.
        let mut unique = Vec::with_capacity(source.filters.len());
        for filter in source.filters {
            if !unique.contains(&filter) {
                unique.push(filter);
            }
        }
        source.filters = unique;
        Some(source)
    }

    fn column(&self, column: &Column, schema: &DFSchema) -> Option<usize> {
        self.columns
            .get(schema.index_of_column(column).ok()?)
            .copied()
    }

    fn add_filter(&mut self, filter: &Expr, schema: &DFSchema) -> Option<()> {
        let physical_schema = self.provider.schema();
        let filter = filter
            .clone()
            .transform_up(|expr| {
                if let Expr::Column(column) = expr {
                    let index = self.column(&column, schema).ok_or_else(|| {
                        DataFusionError::Plan("Invalid direct scan column".into())
                    })?;
                    return Ok(Transformed::yes(Expr::Column(Column::from_name(
                        physical_schema.field(index).name(),
                    ))));
                }
                Ok(Transformed::no(expr))
            })
            .ok()?
            .data;
        let refs = filter.column_refs();
        if refs.len() != 1 {
            return None;
        }
        let column = refs.into_iter().next()?;
        let index = physical_schema.index_of(&column.name).ok()?;
        let data_type = physical_schema.field(index).data_type();
        let value_type = match data_type {
            DataType::Dictionary(key, value)
                if **key == DataType::UInt32 && **value == DataType::Utf8 =>
            {
                value.as_ref()
            }
            other => other,
        };
        let mut valid = true;
        filter
            .apply(|expr| {
                valid &= match expr {
                    Expr::Column(_)
                    | Expr::BinaryExpr(_)
                    | Expr::IsNull(_)
                    | Expr::IsNotNull(_) => true,
                    Expr::Literal(value, _) => {
                        let literal_type = value.data_type();
                        literal_type == *value_type || literal_type == *data_type
                    }
                    _ => false,
                };
                Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
            })
            .ok()?;
        if !valid
            || (self.check_pushdown
                && self.provider.supports_filters_pushdown(&[&filter]).ok()?
                    != [TableProviderFilterPushDown::Exact])
        {
            return None;
        }
        let filter = filter
            .transform_up(|expr| {
                if let DataType::Dictionary(key, value) = data_type
                    && **value == DataType::Utf8
                    && let Expr::Literal(ScalarValue::Utf8(text), metadata) = expr
                {
                    return Ok(Transformed::yes(Expr::Literal(
                        ScalarValue::Dictionary(key.clone(), Box::new(ScalarValue::Utf8(text))),
                        metadata,
                    )));
                }
                Ok(Transformed::no(expr))
            })
            .ok()?
            .data;
        if self.check_pushdown
            && self.provider.supports_filters_pushdown(&[&filter]).ok()?
                != [TableProviderFilterPushDown::Exact]
        {
            return None;
        }
        self.filters.push(filter);
        Some(())
    }
}

fn column_projection(projection: &datafusion_expr::Projection) -> Option<Vec<usize>> {
    projection
        .expr
        .iter()
        .map(|expr| {
            let expr = if let Expr::Alias(alias) = expr {
                alias.expr.as_ref()
            } else {
                expr
            };
            let Expr::Column(column) = expr else {
                return None;
            };
            projection.input.schema().index_of_column(column).ok()
        })
        .collect()
}

pub(crate) async fn try_bind(
    plan: &LogicalPlan,
    state: &dyn Session,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let (extension_plan, output_projection) = match plan {
        LogicalPlan::Projection(projection) => (projection.input.as_ref(), Some(projection)),
        _ => (plan, None),
    };
    let LogicalPlan::Extension(extension) = extension_plan else {
        return Ok(None);
    };
    let instant = extension.node.as_any().downcast_ref::<InstantManipulate>();
    if !(extension.node.as_any().is::<SeriesFragment>()
        || extension.node.as_any().is::<SeriesRange>()
        || extension.node.as_any().is::<SeriesInstantMax>()
        || (instant.is_some() && std::env::var_os("GT_RESEARCH_SERIES_FRAGMENT").is_some()))
        || std::env::var_os("GT_RESEARCH_INSTANT_GRID").is_some()
    {
        return Ok(None);
    }
    if output_projection.is_some() && instant.is_none() {
        return Ok(None);
    }
    let projection_columns = if let Some(projection) = output_projection {
        let columns = column_projection(projection);
        let Some(columns) = columns else {
            return Ok(None);
        };
        Some(columns)
    } else {
        None
    };
    let inputs = extension.node.inputs();
    let [input] = inputs.as_slice() else {
        return Ok(None);
    };
    let divide = if let Some(instant) = instant {
        if instant.research_series_instant().is_none() {
            return Ok(None);
        }
        let LogicalPlan::Extension(divide) = input else {
            return Ok(None);
        };
        let Some(divide) = divide.node.as_any().downcast_ref::<SeriesDivide>() else {
            return Ok(None);
        };
        if divide.tags() != ["__tsid"] {
            return Ok(None);
        }
        Some(divide)
    } else {
        None
    };
    let divide_inputs = divide.map(|divide| divide.inputs());
    let input = if let Some(inputs) = &divide_inputs {
        let [input] = inputs.as_slice() else {
            return Ok(None);
        };
        *input
    } else {
        *input
    };
    let Some(source) = Source::parse(input) else {
        return Ok(None);
    };
    let Some(provider) = source.provider.downcast_ref::<DummyTableProvider>() else {
        return Ok(None);
    };
    let schema = provider.schema();
    let metadata = provider.region_metadata();
    let time = &metadata.time_index_column().column_schema.name;
    let Some(ordering) = &source.ordering else {
        return Ok(None);
    };
    if ordering.len() != 2
        || schema.field(ordering[0]).name() != "__tsid"
        || schema.field(ordering[1]).name() != time
    {
        return Ok(None);
    }
    if instant.is_some_and(|instant| {
        instant
            .research_series_instant()
            .is_none_or(|(_, column)| column != time)
    }) {
        return Ok(None);
    }
    provider.with_ordering_hint(
        &ordering
            .iter()
            .map(|&index| OrderOption {
                name: schema.field(index).name().clone(),
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            })
            .collect::<Vec<_>>(),
    );
    provider.with_distribution(TimeSeriesDistribution::PerSeries);
    let mut projection = source.columns.clone();
    projection.sort_unstable();
    projection.dedup();
    let scan = provider
        .scan(state, Some(&projection), &source.filters, None)
        .await?;
    if std::env::var_os("GT_RESEARCH_DIRECT_RANGE_SPEC").is_some() {
        let input_schema = Arc::new(input.schema().as_arrow().clone());
        let spec = if let Some(fragment) = extension.node.as_any().downcast_ref::<SeriesFragment>()
        {
            Some(fragment.specification(input_schema)?)
        } else if let Some(range) = extension.node.as_any().downcast_ref::<SeriesRange>() {
            Some(range.specification(input_schema)?)
        } else {
            None
        };
        if let Some(spec) = spec {
            let columns = source
                .columns
                .iter()
                .map(|index| {
                    projection.binary_search(index).map_err(|_| {
                        DataFusionError::Internal("Missing direct scan projection".into())
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let spec = remap_range_spec(spec, scan.schema(), &columns)?;
            if std::env::var_os("GT_RESEARCH_SERIES_PROGRAM").is_some()
                && let Some(program) =
                    super::scan_range_plan::bind_program(scan.clone(), Arc::new(spec.clone()))?
            {
                if !plan.schema().matches_arrow_schema(&program.schema()) {
                    return Err(DataFusionError::Plan(
                        "Series program output schema mismatch".into(),
                    ));
                }
                common_telemetry::debug!("Research series program bound");
                return Ok(Some(program));
            }
            let offset = spec.offset;
            let physical = Arc::new(SeriesRangeExec::new(scan, Arc::new(spec), offset));
            if !plan.schema().matches_arrow_schema(&physical.schema()) {
                return Err(DataFusionError::Plan(
                    "Direct range spec output schema mismatch".into(),
                ));
            }
            let physical = DefaultPhysicalPlanner::default().optimize_physical_plan(
                physical,
                state,
                |_, _| {},
            )?;
            common_telemetry::debug!(
                fragment = extension.node.name(),
                "Research direct range spec bound"
            );
            common_telemetry::debug!(
                fragment = extension.node.name(),
                "Research direct fragment bound"
            );
            return Ok(Some(physical));
        }
    }
    let expressions = source
        .columns
        .iter()
        .enumerate()
        .map(|(output, index)| {
            let position = projection
                .binary_search(index)
                .map_err(|_| DataFusionError::Internal("Missing direct scan projection".into()))?;
            Ok((
                Arc::new(PhysicalColumn::new(schema.field(*index).name(), position))
                    as Arc<dyn PhysicalExpr>,
                input.schema().field(output).name().clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let physical_input: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(expressions, scan)?);
    if !input
        .schema()
        .matches_arrow_schema(&physical_input.schema())
    {
        return Err(DataFusionError::Plan(
            "Direct fragment input schema mismatch".into(),
        ));
    }
    let input = physical_input;
    let node = extension.node.as_any();
    let physical = if let Some(fragment) = node.downcast_ref::<SeriesFragment>() {
        fragment.physical(input)?
    } else if let Some(range) = node.downcast_ref::<SeriesRange>() {
        range.physical(input)?
    } else if let Some(instant) = node.downcast_ref::<SeriesInstantMax>() {
        instant.physical(input)?
    } else if let (Some(instant), Some(divide)) = (instant, divide) {
        instant.to_execution_plan(divide.to_execution_plan(input))
    } else {
        return Err(DataFusionError::Internal(
            "Unexpected direct fragment".into(),
        ));
    };
    let physical: Arc<dyn ExecutionPlan> = if let Some(columns) = projection_columns {
        let expressions: Vec<_> = columns
            .into_iter()
            .enumerate()
            .map(|(output, index)| {
                (
                    Arc::new(PhysicalColumn::new(
                        physical.schema().field(index).name(),
                        index,
                    )) as Arc<dyn PhysicalExpr>,
                    plan.schema().field(output).name().clone(),
                )
            })
            .collect();
        Arc::new(ProjectionExec::try_new(expressions, physical)?)
    } else {
        physical
    };
    if !plan.schema().matches_arrow_schema(&physical.schema()) {
        return Err(DataFusionError::Plan(
            "Direct fragment output schema mismatch".into(),
        ));
    }
    let physical =
        DefaultPhysicalPlanner::default().optimize_physical_plan(physical, state, |_, _| {})?;
    common_telemetry::debug!(
        fragment = extension.node.name(),
        "Research direct fragment bound"
    );
    Ok(Some(physical))
}

// Keep the scan's sorted, unique projection unchanged. Only the processor's
// column addresses change; packed labels and aggregate groups refer to output
// tag ordinals, not raw scan columns.
pub(crate) fn remap_range_spec(
    mut spec: RangeSpec,
    schema: SchemaRef,
    columns: &[usize],
) -> Result<RangeSpec> {
    if columns.len() != spec.input_schema.fields().len()
        || spec
            .input_schema
            .fields()
            .iter()
            .zip(columns)
            .any(|(expected, &index)| {
                schema.fields().get(index).is_none_or(|actual| {
                    expected.data_type() != actual.data_type()
                        || expected.is_nullable() != actual.is_nullable()
                })
            })
    {
        return Err(DataFusionError::Plan(
            "Direct range spec input schema mismatch".into(),
        ));
    }
    let map = |index: usize| {
        columns
            .get(index)
            .copied()
            .ok_or_else(|| DataFusionError::Plan("Direct range spec column out of bounds".into()))
    };
    spec.timestamp = map(spec.timestamp)?;
    spec.value = map(spec.value)?;
    spec.tsid = map(spec.tsid)?;
    for output in &mut spec.output {
        if let RangeOutput::Tag(index) = output {
            *index = map(*index)?;
        }
    }
    spec.input_schema = schema;
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{LogicalPlanBuilder, col, lit};
    use store_api::storage::RegionId;

    use super::*;
    use crate::optimizer::test_util::mock_table_provider_with_tsid;

    fn table() -> LogicalPlanBuilder {
        LogicalPlanBuilder::scan(
            "physical",
            Arc::new(DefaultTableSource::new(Arc::new(
                mock_table_provider_with_tsid(RegionId::new(1, 1)),
            ))),
            None,
        )
        .unwrap()
    }

    #[test]
    fn instant_output_projection_resolves_qualified_aliases_and_rejects_computation() {
        let input = table().alias("metric").unwrap().build().unwrap();
        let plan = LogicalPlanBuilder::from(input.clone())
            .project(vec![
                col("metric.v0").alias("value"),
                col("metric.k0"),
                col("metric.ts"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let LogicalPlan::Projection(projection) = plan else {
            unreachable!()
        };
        assert_eq!(column_projection(&projection), Some(vec![3, 1, 2]));
        let plan = LogicalPlanBuilder::from(input)
            .project(vec![(col("metric.v0") + lit(1.0)).alias("value")])
            .unwrap()
            .build()
            .unwrap();
        let LogicalPlan::Projection(projection) = plan else {
            unreachable!()
        };
        assert!(column_projection(&projection).is_none());
    }

    #[test]
    fn direct_source_preserves_aliases_column_reordering_and_all_predicates() {
        let filter = col("k0").eq(lit("a"));
        let mut scan = table().build().unwrap();
        let LogicalPlan::TableScan(ref mut scan_node) = scan else {
            unreachable!();
        };
        scan_node.filters.push(filter.clone());
        let plan = LogicalPlanBuilder::from(scan)
            .filter(filter)
            .unwrap()
            .project(vec![
                col("v0"),
                col("ts"),
                col("__tsid"),
                col("k0").alias("label"),
            ])
            .unwrap()
            .alias("metric")
            .unwrap()
            .filter(col("metric.label").not_eq(lit("b")))
            .unwrap()
            .project(vec![
                col("metric.label"),
                col("metric.__tsid"),
                col("metric.ts"),
                col("metric.v0"),
            ])
            .unwrap()
            .sort(vec![
                col("__tsid").sort(true, true),
                col("ts").sort(true, true),
            ])
            .unwrap()
            .build()
            .unwrap();
        let source = Source::parse(&plan).unwrap();
        assert_eq!(source.columns, [1, 0, 2, 3]);
        assert_eq!(source.ordering, Some(vec![0, 2]));
        assert_eq!(
            source.filters,
            [col("k0").eq(lit("a")), col("k0").not_eq(lit("b"))]
        );
    }

    #[test]
    fn direct_source_rejects_nonexact_filters_casts_computations_and_limits() {
        let cases = [
            table()
                .filter(col("v0").gt(lit(1.0)))
                .unwrap()
                .build()
                .unwrap(),
            table()
                .filter(col("k0").eq(lit(1_i64)))
                .unwrap()
                .build()
                .unwrap(),
            table()
                .filter(datafusion_expr::cast(col("k0"), DataType::Int64).eq(lit(1_i64)))
                .unwrap()
                .build()
                .unwrap(),
            table()
                .filter(col("k0").in_list(vec![lit("a"), lit("b")], false))
                .unwrap()
                .build()
                .unwrap(),
            table()
                .project(vec![(col("v0") + lit(1.0)).alias("v0")])
                .unwrap()
                .build()
                .unwrap(),
            table().limit(0, Some(1)).unwrap().build().unwrap(),
            table()
                .sort(vec![col("ts").sort(false, true)])
                .unwrap()
                .build()
                .unwrap(),
        ];
        for plan in cases {
            assert!(Source::parse(&plan).is_none(), "{plan}");
        }
        let mut plan = table().build().unwrap();
        let LogicalPlan::TableScan(ref mut scan) = plan else {
            unreachable!()
        };
        scan.projection = Some(vec![0, 3, 2, 1]);
        assert!(Source::parse(&plan).is_none());
    }

    #[test]
    fn direct_source_keeps_nullable_predicates_and_scan_hints_isolated() {
        let predicate = col("k0").is_null().or(col("k0").not_eq(lit("")));
        let plan = table().filter(predicate.clone()).unwrap().build().unwrap();
        let first = Source::parse(&plan).unwrap();
        let second = Source::parse(&plan).unwrap();
        assert_eq!(first.filters, [predicate]);
        let first = first.provider.downcast_ref::<DummyTableProvider>().unwrap();
        let second = second
            .provider
            .downcast_ref::<DummyTableProvider>()
            .unwrap();
        first.with_distribution(TimeSeriesDistribution::PerSeries);
        assert_eq!(
            first.scan_request().distribution,
            Some(TimeSeriesDistribution::PerSeries)
        );
        assert_eq!(second.scan_request().distribution, None);
    }
}
