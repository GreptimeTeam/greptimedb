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

use api::v1::SemanticType;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::datasource::DefaultTableSource;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Extension, LogicalPlan};
use datatypes::data_type::ConcreteDataType;
use promql::extension_plan::{InstantManipulate, InstantManipulateExec, SeriesDivideExec};
use store_api::metric_engine_consts::is_metric_engine_internal_column;
use table::table::scan::RegionScanExec;

mod fused;

use crate::datafusion::raw_expr_supported;
use crate::dummy_catalog::DummyTableProvider;

/// Selects native execution for a bounded, unmodified shape.
///
/// Tries the fused range-function subtree first, then the instant-over-divide shape, and returns
/// the original plan unchanged for anything else.
pub(crate) async fn try_select(
    logical: &LogicalPlan,
    original: Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    if let Some(fused) = fused::try_select_fused(logical, &original)? {
        return Ok(fused);
    }
    if let Some(local) = try_select_local(logical, &original)? {
        return Ok(local);
    }
    Ok(original)
}

/// Selects native execution only for the bounded, unmodified local instant-over-divide shape.
fn try_select_local(
    logical: &LogicalPlan,
    original: &Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some((instant, divide, scan_node)) = logical_shape(logical) else {
        return Ok(None);
    };
    let Some((instant_exec_schema, scan_exec, root_cooperative, scan_cooperative)) =
        physical_shape(original)
    else {
        return Ok(None);
    };
    if scan_node.fetch.is_some()
        || !scan_node.statistics_requests.is_empty()
        || scan_node
            .filters
            .iter()
            .any(|filter| !raw_expr_supported(filter))
    {
        return Ok(None);
    }
    let Some(source) = scan_node.source.downcast_ref::<DefaultTableSource>() else {
        return Ok(None);
    };
    let Some(provider) = source.table_provider.downcast_ref::<DummyTableProvider>() else {
        return Ok(None);
    };

    let metadata = provider.region_metadata();
    let table_name = scan_node.table_name.table().to_string();
    let logical_tags = metadata
        .column_metadatas
        .iter()
        .filter(|column| {
            column.semantic_type == SemanticType::Tag
                && !is_metric_engine_internal_column(&column.column_schema.name)
        })
        .map(|column| column.column_schema.name.clone())
        .collect::<Vec<_>>();
    let logical_output = scan_node.projected_schema.as_ref();
    let physical_output = scan_exec.schema();
    if logical_output.fields().len() != physical_output.fields().len()
        || logical_output
            .fields()
            .iter()
            .zip(physical_output.fields())
            .any(|(logical, physical)| {
                logical.name() != physical.name() || logical.data_type() != physical.data_type()
            })
    {
        return Ok(None);
    }
    let mut fields = Vec::new();
    let mut tags_found = std::collections::HashSet::new();
    let mut time_found = false;
    for field in logical_output.fields() {
        let Some(column) = metadata.column_by_name(field.name()) else {
            return Ok(None);
        };
        match column.semantic_type {
            SemanticType::Tag if logical_tags.iter().any(|tag| tag == field.name()) => {
                tags_found.insert(field.name().clone());
            }
            SemanticType::Timestamp
                if field.data_type() == &DataType::Timestamp(TimeUnit::Millisecond, None) =>
            {
                time_found = true;
            }
            SemanticType::Field
                if column.column_schema.data_type == ConcreteDataType::float64_datatype()
                    && field.data_type() == &DataType::Float64 =>
            {
                fields.push(field.name().clone());
            }
            _ => return Ok(None),
        }
    }
    // The declared Float64 fields are mapped in declared order; only the configured staleness
    // columns are checked, exactly like the ordinary selector.
    if !time_found
        || logical_output.fields().len() != logical_tags.len() + fields.len() + 1
        || tags_found.len() != logical_tags.len()
        || fields.is_empty()
        || (!divide.tags().is_empty() && divide.tags() != logical_tags)
    {
        return Ok(None);
    }

    let Some(native_scan) =
        scan_exec
            .clone()
            .try_to_named_series_scan(&table_name, &logical_tags, &fields)?
    else {
        return Ok(None);
    };
    let label_source = native_scan.label_source().clone();
    let series_schema = Arc::clone(native_scan.series_schema());
    let mut native_child: Arc<dyn ExecutionPlan> = native_scan;
    if scan_cooperative {
        native_child = Arc::new(datafusion::physical_plan::coop::CooperativeExec::new(
            native_child,
        ));
    }
    let native = instant.try_to_native_execution_plan(
        native_child,
        instant_exec_schema,
        label_source,
        series_schema,
        &logical_tags,
    )?;
    let Some(native) = native else {
        return Ok(None);
    };
    if root_cooperative {
        Arc::clone(original)
            .replace_children(
                vec![native],
                datafusion::physical_plan::ReplaceChildrenOptions::new(
                    datafusion::physical_plan::ChildrenPropertiesMode::Recompute,
                ),
            )
            .map(Some)
    } else {
        Ok(Some(native))
    }
}

fn logical_shape(
    plan: &LogicalPlan,
) -> Option<(
    &InstantManipulate,
    &promql::extension_plan::SeriesDivide,
    &datafusion_expr::logical_plan::TableScan,
)> {
    let LogicalPlan::Extension(Extension { node }) = plan else {
        return None;
    };
    let instant = node.as_any().downcast_ref::<InstantManipulate>()?;
    if instant.is_single_evaluation() {
        return None;
    }
    let instant_inputs = node.inputs();
    let [input] = instant_inputs.as_slice() else {
        return None;
    };
    let LogicalPlan::Extension(Extension { node: divide_node }) = input else {
        return None;
    };
    let divide = divide_node
        .as_any()
        .downcast_ref::<promql::extension_plan::SeriesDivide>()?;
    let divide_inputs = divide_node.inputs();
    let [input] = divide_inputs.as_slice() else {
        return None;
    };
    let scan = table_scan_through_safe_nodes(input)?;
    Some((instant, divide, scan))
}

/// Returns the single table scan a plan reaches through safe nodes: a sort without a fetch, a
/// filter whose predicate the raw path supports, or a positional identity projection.
///
/// The scan is a discovery result only: callers must preserve every node of the discovered tree
/// instead of treating the wrappers around the scan as removable.
pub(crate) fn table_scan_through_safe_nodes(
    plan: &LogicalPlan,
) -> Option<&datafusion_expr::logical_plan::TableScan> {
    match plan {
        LogicalPlan::TableScan(scan) => Some(scan),
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            table_scan_through_safe_nodes(&sort.input)
        }
        LogicalPlan::Filter(filter) if raw_expr_supported(&filter.predicate) => {
            table_scan_through_safe_nodes(&filter.input)
        }
        LogicalPlan::Projection(projection)
            if projection.expr.len() == projection.input.schema().fields().len()
                && projection.expr.iter().enumerate().all(|(index, expr)| {
                    matches!(expr, datafusion_expr::Expr::Column(column)
                        if column.name.as_str() == projection.input.schema().field(index).name())
                }) =>
        {
            table_scan_through_safe_nodes(&projection.input)
        }
        _ => None,
    }
}

fn physical_shape(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<(SchemaRef, Arc<RegionScanExec>, bool, bool)> {
    let (plan, root_cooperative) = if let Some(wrapper) =
        plan.downcast_ref::<datafusion::physical_plan::coop::CooperativeExec>()
    {
        (wrapper.input(), true)
    } else {
        (plan, false)
    };
    let instant = plan.downcast_ref::<InstantManipulateExec>()?;
    let output_schema = instant.schema();
    let instant_children = instant.children();
    let [divide] = instant_children.as_slice() else {
        return None;
    };
    let divide = divide.downcast_ref::<SeriesDivideExec>()?;
    let divide_children = divide.children();
    let [scan] = divide_children.as_slice() else {
        return None;
    };
    let (scan, scan_cooperative) = if let Some(wrapper) =
        scan.downcast_ref::<datafusion::physical_plan::coop::CooperativeExec>()
    {
        (wrapper.input(), true)
    } else {
        (*scan, false)
    };
    let scan = scan.downcast_ref::<RegionScanExec>()?;
    if scan.properties().output_partitioning().partition_count() != 1 {
        return None;
    }
    Some((
        output_schema,
        Arc::new(scan.clone()),
        root_cooperative,
        scan_cooperative,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::datasource::DefaultTableSource;
    use datafusion_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
    use promql::extension_plan::{InstantManipulate, SeriesDivide};
    use store_api::storage::RegionId;

    use super::{logical_shape, table_scan_through_safe_nodes};

    #[test]
    fn native_instant_admission_rejects_non_instant_root() {
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        assert!(logical_shape(&plan).is_none());
    }

    #[test]
    fn native_instant_admission_accepts_one_bounded_instant_scan() {
        let provider = Arc::new(crate::optimizer::test_util::mock_table_provider(
            RegionId::new(1, 1),
        ));
        let source = Arc::new(DefaultTableSource::new(provider));
        let scan = LogicalPlanBuilder::scan("metric", source, None)
            .unwrap()
            .build()
            .unwrap();
        let divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(vec!["k0".into()], "ts".into(), scan)),
        });
        let instant = LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                1000,
                2000,
                30000,
                1000,
                0,
                "ts".into(),
                vec!["k0".into()],
                Some("v0".into()),
                divide,
            )),
        });
        assert!(logical_shape(&instant).is_some());
    }

    #[test]
    fn native_instant_scan_path_rejects_uncontrolled_operators() {
        let plan = LogicalPlanBuilder::empty(false)
            .limit(0, Some(1))
            .unwrap()
            .build()
            .unwrap();
        assert!(table_scan_through_safe_nodes(&plan).is_none());
        assert!(matches!(plan, LogicalPlan::Limit(_)));
    }
}
