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
use common_query::series_frame::series_frame_ordering;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{FrameContent, FrameLabelMode, SeriesFrameDeclaration};
use store_api::region_engine::RawLabelSource;
use table::table::scan::{RegionScanExec, SeriesScanExec};

use crate::extension_plan::{
    Absent, AggregateFinalize, EmptyMetric, FusedSeries, HistogramFold, InstantManipulate,
    RangeManipulate, RawSeriesSource, ScalarCalculate, SeriesDivide, SeriesNormalize,
    UnionDistinctOn,
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
        } else if let Some(node) = node.as_any().downcast_ref::<RawSeriesSource>() {
            plan_raw_series_source(
                node,
                physical_inputs,
                session.config_options().execution.target_partitions,
            )
            .map(Some)
        } else if let Some(node) = node.as_any().downcast_ref::<AggregateFinalize>() {
            // The merge runs where every partition's state arrives, so its own input is the plan
            // that exports them.
            let Some(input) = physical_inputs.first() else {
                return Ok(None);
            };
            Ok(Some(node.to_execution_plan(Arc::clone(input))?))
        } else {
            Ok(None)
        }
    }
}

/// The label `__name__` is reported under, which is a column of this node's output but not a tag
/// column of any region.
const METRIC_NAME: &str = "__name__";

/// Builds the physical node of a [`FusedSeries`] over the raw series its inputs can provide.
///
/// The node reads raw series frames. Below it is either the region scan itself, when the node runs
/// in the region it reads, or an input that already produces explicit-label raw frames, when the
/// node runs above the regions of several tables and each [`RawSeriesSource`] answered for its own.
/// Anything else in between - a filter, a projection that drops the frame, an exchange - produces a
/// different frame, or no frame at all, and this layer cannot look under it.
///
/// `Ok(None)` is therefore the answer for children that are neither: the logical plan is already
/// fixed and DataFusion reports an unplanned extension node normally. A region scan that is found
/// but refuses the raw path is a different case — the logical route chose this node for this scan
/// and nothing here can undo that choice, because the logical plan is not available — so it is
/// reported as an error that names the reason instead of silently producing no plan.
fn plan_fused_series(
    fused: &FusedSeries,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    target_partitions: usize,
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    if physical_inputs.len() != fused.children().len() {
        return Err(DataFusionError::Plan(
            "FusedSeries physical child count mismatch".into(),
        ));
    }
    if physical_inputs.is_empty() {
        return fused.to_execution_plan(Vec::new()).map(Some);
    }
    if let Some(inputs) = physical_inputs
        .iter()
        .map(|input| {
            explicit_raw_frame_ordering(input.schema().as_ref())
                .map(|ordering| (Arc::clone(input), RawLabelSource::Explicit, ordering))
        })
        .collect::<Option<Vec<_>>>()
    {
        return fused.to_execution_plan(inputs).map(Some);
    }
    if physical_inputs
        .iter()
        .any(|input| input.downcast_ref::<RegionScanExec>().is_none())
    {
        return Ok(None);
    }

    let binding = fused.binding();
    let Some(metric_name) = binding.metric_name.as_deref() else {
        return Err(DataFusionError::Plan(
            "the FusedSeries node binds no metric name, so the region scan below it cannot be \
             asked for the series of one metric"
                .to_string(),
        ));
    };
    let source_fields = super::fused_series::tree_source_fields(fused.tree())?;
    if source_fields.len() != physical_inputs.len() {
        return Err(DataFusionError::Plan(
            "FusedSeries physical plan source count mismatch".into(),
        ));
    }
    let mut series_inputs = Vec::with_capacity(physical_inputs.len());
    for (physical, field_names) in physical_inputs.iter().zip(source_fields) {
        let Some(scan) = physical.downcast_ref::<RegionScanExec>() else {
            return Ok(None);
        };
        let series_scan = series_scan(
            scan,
            metric_name,
            &binding.tags,
            &field_names,
            target_partitions,
        )?;
        series_inputs.push((
            series_scan.clone() as Arc<dyn ExecutionPlan>,
            series_scan.label_source().clone(),
            series_scan.raw_series_ordering(),
        ));
    }
    fused.to_execution_plan(series_inputs).map(Some)
}

/// Builds the region half of a [`RawSeriesSource`]: the region's raw-series leaf for its binding.
///
/// The node only means something over the region scan it binds, so any other child is an error
/// rather than a plan another planner could build.
fn plan_raw_series_source(
    source: &RawSeriesSource,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    target_partitions: usize,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let [input] = physical_inputs else {
        return Err(DataFusionError::Plan(
            "RawSeriesSource requires exactly one physical input".into(),
        ));
    };
    let Some(scan) = input.downcast_ref::<RegionScanExec>() else {
        return Err(DataFusionError::Plan(format!(
            "RawSeriesSource for metric {:?} must be planned over a region scan, got {}",
            source.metric_name(),
            input.name()
        )));
    };
    let series_scan = series_scan(
        scan,
        source.metric_name(),
        source.tags(),
        source.fields(),
        target_partitions,
    )?;
    source.to_execution_plan(series_scan)
}

/// The raw-series leaf of a region scan bound to one metric.
///
/// The leaf needs a prepared scan: range assignment runs after physical planning and cannot see a
/// `SeriesScanExec`. Preparation keeps the scan's parallel layout, and the series-producer contract
/// guarantees that one series stays in one scan partition.
fn series_scan(
    scan: &RegionScanExec,
    metric_name: &str,
    tags: &[String],
    fields: &[String],
    target_partitions: usize,
) -> DfResult<Arc<SeriesScanExec>> {
    let scan = if scan.is_partition_set() {
        Arc::new(scan.clone())
    } else {
        let partitions = table::table::scan::assign_partition_ranges(
            scan.get_partition_ranges(),
            target_partitions.max(1),
        );
        Arc::new(
            scan.with_new_partitions(partitions, target_partitions.max(1))
                .map_err(|error| DataFusionError::External(error.into_inner()))?,
        )
    };
    // `__name__` is the metric name rather than a column of the region, so it is not one of the
    // logical tags the scan is asked to decode; the frame names each series' own metric.
    let logical_tags = tags
        .iter()
        .filter(|tag| tag.as_str() != METRIC_NAME)
        .cloned()
        .collect::<Vec<_>>();
    scan.try_to_named_series_scan(metric_name, &logical_tags, fields)?
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "the region scan could not serve a raw series frame for metric {metric_name:?}: \
                 the scanner refused the raw series path for this scan"
            ))
        })
}

/// The ordering an input declares when it produces raw frames that carry their own labels.
fn explicit_raw_frame_ordering(schema: &Schema) -> Option<SeriesOrdering> {
    let declaration = SeriesFrameDeclaration::parse(schema).ok()?;
    if declaration.content() != FrameContent::Raw
        || declaration.labels() != FrameLabelMode::Explicit
    {
        return None;
    }
    series_frame_ordering(schema).ok()
}
