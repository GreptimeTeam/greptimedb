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

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use table::table::scan::RegionScanExec;

use crate::extension_plan::{
    Absent, EmptyMetric, FusedSeries, HistogramFold, InstantManipulate, RangeManipulate,
    ScalarCalculate, SeriesDivide, SeriesNormalize, UnionDistinctOn,
};

pub struct PromExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for PromExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<SeriesNormalize>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<InstantManipulate>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<RangeManipulate>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<SeriesDivide>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<EmptyMetric>() {
            Ok(Some(node.to_execution_plan(
                session,
                planner,
                planning_ctx,
            )?))
        } else if let Some(node) = node.as_any().downcast_ref::<ScalarCalculate>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())?))
        } else if let Some(node) = node.as_any().downcast_ref::<HistogramFold>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<UnionDistinctOn>() {
            Ok(Some(node.to_execution_plan(
                physical_inputs[0].clone(),
                physical_inputs[1].clone(),
            )))
        } else if let Some(node) = node.as_any().downcast_ref::<Absent>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<FusedSeries>() {
            plan_fused_series(node, physical_inputs)
        } else {
            Ok(None)
        }
    }
}

/// The label `__name__` is reported under, which is a column of this node's output but not a tag
/// column of any region.
const METRIC_NAME: &str = "__name__";

/// Builds the physical node of a [`FusedSeries`] over the raw series leaf its input can provide.
///
/// The node reads a raw series frame, so what it needs below it is the region scan itself: the
/// scan's own raw-series entrypoint is what packs a series' chunks contiguously and declares their
/// ordering. Anything else in between - a filter, a projection, an exchange - produces a different
/// frame, or no frame at all, and this layer cannot look under it.
///
/// `Ok(None)` is therefore the answer for a child that is not a region scan: the logical plan is
/// already fixed and DataFusion reports an unplanned extension node normally. A region scan that is
/// found but refuses the raw path is a different case — the logical route chose this node for this
/// scan and nothing here can undo that choice, because the logical plan is not available — so it is
/// reported as an error that names the reason instead of silently producing no plan.
fn plan_fused_series(
    fused: &FusedSeries,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    let Some(input) = physical_inputs.first() else {
        return Ok(None);
    };
    let Some(scan) = input.downcast_ref::<RegionScanExec>() else {
        return Ok(None);
    };

    let binding = fused.binding();
    let Some(metric_name) = binding.metric_name.as_deref() else {
        return Err(DataFusionError::Plan(
            "the FusedSeries node binds no metric name, so the region scan below it cannot be \
             asked for the series of one metric"
                .to_string(),
        ));
    };
    // `__name__` is the metric name rather than a column of the region, so it is not one of the
    // logical tags the scan is asked to decode; the frame names each series' own metric.
    let logical_tags = binding
        .tags
        .iter()
        .filter(|tag| tag.as_str() != METRIC_NAME)
        .cloned()
        .collect::<Vec<_>>();
    let field_names = binding
        .series_schema
        .fields
        .iter()
        .map(|field| field.name.to_string())
        .collect::<Vec<_>>();

    let scan = single_partition_scan(scan)?;
    // The scan resolves the label source and the ordering it will declare itself, against its own
    // metadata snapshot, and hands both back through the leaf it builds. Naming them here instead
    // would be a second copy of a binding only the scanner can make: the physical metadata and
    // column ids a `RawLabelSource::PrimaryKey` carries are not reachable from a region scan's
    // public surface, so the layer that resolves them is the scanner.
    let Some(series_scan) =
        scan.try_to_named_series_scan(metric_name, &logical_tags, &field_names)?
    else {
        return Err(DataFusionError::Plan(format!(
            "the region scan under FusedSeries could not serve a raw series frame for metric \
             {metric_name:?}: the scanner refused the raw series path for this scan, so this node \
             has no input to read"
        )));
    };
    let label_source = series_scan.label_source().clone();
    let ordering = series_scan.raw_series_ordering();

    fused
        .to_execution_plan(series_scan, label_source, ordering)
        .map(Some)
}

/// The scan as one partition holding every range it already has.
///
/// A raw-series binding is resolvable only once the scan holds exactly one partition: the label
/// source is decoded against the scanner's own metadata snapshot and the frames it produces carry
/// whole series, so a scan spread over several partitions has no single binding to name. The rule
/// that assigns partition ranges (`ParallelizeScan`) runs *after* physical planning, i.e. after
/// this node is built, so a scan reaching here is unprepared; preparing it here keeps the node from
/// reading one partition of the series it was bound to. A scan that is already prepared — several
/// partitions, assigned by whatever ran before — is passed through as it is, and the binding below
/// declines it if it cannot name one partition.
fn single_partition_scan(scan: &RegionScanExec) -> DfResult<Arc<RegionScanExec>> {
    if scan.is_partition_set() {
        return Ok(Arc::new(scan.clone()));
    }
    let ranges = scan.get_partition_ranges();
    let prepared = scan
        .with_new_partitions(vec![ranges], 1)
        .map_err(|error| DataFusionError::External(error.into_inner()))?;
    Ok(Arc::new(prepared))
}
