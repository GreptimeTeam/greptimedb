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
            plan_fused_series(
                node,
                physical_inputs,
                session.config_options().execution.target_partitions,
            )
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
/// The fused leaf needs a prepared scan: range assignment runs after physical planning and cannot
/// see a `SeriesScanExec`. Preparation keeps the scan's parallel layout, and the series-producer
/// contract guarantees that one series stays in one scan partition.
fn plan_fused_series(
    fused: &FusedSeries,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    target_partitions: usize,
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    if physical_inputs.is_empty() {
        return Ok(None);
    }
    let mut prepared = Vec::new();
    for (index, input) in physical_inputs.iter().enumerate() {
        let Some(scan) = input.downcast_ref::<RegionScanExec>() else {
            return Ok(None);
        };
        let binding = &fused.fused_inputs()[index];
        let Some(metric_name) = binding.metric_name.as_deref() else {
            return Err(DataFusionError::Plan(
                "the FusedSeries node binds no metric name".to_string(),
            ));
        };
        let tags = binding
            .tags
            .iter()
            .filter(|tag| tag.as_str() != METRIC_NAME)
            .cloned()
            .collect::<Vec<_>>();
        let fields = binding
            .series_schema
            .fields
            .iter()
            .map(|field| field.name.to_string())
            .collect::<Vec<_>>();
        let scan = if scan.is_partition_set() {
            Arc::new(scan.clone())
        } else {
            let n = target_partitions.max(1);
            let partitions =
                table::table::scan::assign_partition_ranges(scan.get_partition_ranges(), n);
            Arc::new(
                scan.with_new_partitions(partitions, n)
                    .map_err(|e| DataFusionError::External(e.into_inner()))?,
            )
        };
        let Some(series_scan) = scan.try_to_named_series_scan(metric_name, &tags, &fields)? else {
            return Err(DataFusionError::Plan(format!(
                "the region scan under FusedSeries could not serve raw series for {metric_name:?}"
            )));
        };
        prepared.push((
            series_scan.clone() as Arc<dyn ExecutionPlan>,
            series_scan.label_source().clone(),
            series_scan.raw_series_ordering(),
        ));
    }
    fused.to_execution_plan(prepared).map(Some)
}
