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

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::config::ConfigOptions;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, ScalarFunctionExpr,
};
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use futures::{StreamExt, TryStreamExt, stream};
use promql::extension_plan::{InstantManipulateExec, SeriesDivideExec};
use store_api::region_engine::ScanProcessor;
use table::table::scan::RegionScanExec;

use super::grid::TimeGrid;
use super::scan_max::{InstantMaxProcessor, InstantMaxSpec};

#[derive(Debug)]
pub(crate) struct ScanMaxRule;

impl PhysicalOptimizerRule for ScanMaxRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let max_enabled = std::env::var_os("GT_RESEARCH_SCAN_AGG").is_some();
        let sum_enabled = std::env::var_os("GT_RESEARCH_SCAN_SUM").is_some();
        let packed_max = std::env::var_os("GT_RESEARCH_PACKED_INSTANT_MAX").is_some();
        let range_enabled = std::env::var_os("GT_RESEARCH_DIRECT_RANGE").is_some()
            || std::env::var_os("GT_RESEARCH_DIRECT_RATE").is_some();
        if !max_enabled && !sum_enabled && !range_enabled && !packed_max {
            return Ok(plan);
        }
        Ok(plan
            .transform_up(|plan| {
                if packed_max && let Some(replacement) = match_packed_max(plan.clone())? {
                    return Ok(Transformed::yes(
                        Arc::new(replacement) as Arc<dyn ExecutionPlan>
                    ));
                }
                if range_enabled
                    && let Some(replacement) = super::scan_range_plan::replace(plan.clone())?
                {
                    return Ok(Transformed::yes(replacement));
                }
                if !max_enabled && !sum_enabled {
                    return Ok(Transformed::no(plan));
                }
                Ok(match match_plan(plan.clone())? {
                    Some(replacement) => {
                        Transformed::yes(Arc::new(replacement) as Arc<dyn ExecutionPlan>)
                    }
                    None => Transformed::no(plan),
                })
            })?
            .data)
    }

    fn name(&self) -> &str {
        "ResearchScanMax"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

fn column(expr: &Arc<dyn PhysicalExpr>) -> Option<usize> {
    expr.downcast_ref::<Column>().map(Column::index)
}

fn match_plan(original: Arc<dyn ExecutionPlan>) -> Result<Option<ScanMaxExec>> {
    let Some(aggregate) = original.downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    if aggregate.mode() != &AggregateMode::Partial
        || !aggregate.group_expr().is_single()
        || aggregate.input_order_mode() != &InputOrderMode::Linear
        || aggregate.limit_options().is_some()
        || aggregate.filter_expr().iter().any(Option::is_some)
    {
        return Ok(None);
    }
    let [function] = aggregate.aggr_expr() else {
        return Ok(None);
    };
    // Max is gated separately: its packed path is preferred and this one was slower for it.
    let sum = match function.fun().name() {
        "max" if std::env::var_os("GT_RESEARCH_SCAN_AGG").is_some() => false,
        "sum" if std::env::var_os("GT_RESEARCH_SCAN_SUM").is_some() => true,
        _ => return Ok(None),
    };
    if function.is_distinct() || !function.order_bys().is_empty() {
        return Ok(None);
    }
    let args = function.expressions();
    let [argument] = args.as_slice() else {
        return Ok(None);
    };
    let Some(value) = column(argument) else {
        return Ok(None);
    };
    let Some(mut indices) = aggregate
        .group_expr()
        .expr()
        .iter()
        .map(|(expr, _)| column(expr))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    indices.push(value);
    let mut input = aggregate.input().clone();
    let mut grid = None;
    let mut divided = false;
    loop {
        if let Some(projection) = input.downcast_ref::<ProjectionExec>() {
            let Some(remapped) = indices
                .iter()
                .map(|&index| {
                    projection
                        .expr()
                        .get(index)
                        .and_then(|expr| column(&expr.expr))
                })
                .collect::<Option<Vec<_>>>()
            else {
                return Ok(None);
            };
            indices = remapped;
            input = projection.input().clone();
        } else if let Some(instant) = input.downcast_ref::<InstantManipulateExec>() {
            if grid.is_some() || divided {
                return Ok(None);
            }
            grid = instant.research_scan_grid();
            if grid.is_none() {
                return Ok(None);
            }
            input = instant.children()[0].clone();
        } else if let Some(divide) = input.downcast_ref::<SeriesDivideExec>() {
            if grid.is_none() || divided || divide.research_tags() != ["__tsid"] {
                return Ok(None);
            }
            divided = true;
            input = divide.children()[0].clone();
        } else {
            break;
        }
    }
    let Some(scan) = input.downcast_ref::<RegionScanExec>() else {
        return Ok(None);
    };
    let Some((start, end, interval, lookback, offset)) = grid else {
        return Ok(None);
    };
    if !divided
        || scan.scanner_type() != "SeriesScan"
        || interval <= 0
        || end < start
        || (i128::from(end) - i128::from(start)) / i128::from(interval) + 1 > 1_000_000
    {
        return Ok(None);
    }
    let schema = scan.schema();
    let Ok(timestamp) = schema.index_of(&scan.time_index()) else {
        return Ok(None);
    };
    let Ok(tsid) = schema.index_of("__tsid") else {
        return Ok(None);
    };
    let value = indices
        .pop()
        .ok_or_else(|| DataFusionError::Internal("Missing max input".into()))?;
    if schema.field(timestamp).data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
        || schema.field(value).data_type() != &DataType::Float64
        || schema.field(value).name() != "greptime_value"
        || schema.field(tsid).data_type() != &DataType::UInt64
        || schema.index_of("greptime_native_histogram").is_ok()
        || indices.iter().filter(|&&index| index == timestamp).count() != 1
    {
        return Ok(None);
    }
    let tags = scan.tag_columns();
    if indices
        .iter()
        .any(|&index| index != timestamp && !tags.contains(schema.field(index).name()))
    {
        return Ok(None);
    }
    let output_schema = Arc::new(
        datatypes::schema::Schema::try_from(original.schema())
            .map_err(|error| DataFusionError::External(Box::new(error)))?,
    );
    let spec = Arc::new(InstantMaxSpec {
        schema: output_schema,
        input_schema: schema.clone(),
        groups: indices,
        timestamp,
        value,
        tsid,
        grid: Arc::new(
            TimeGrid::new(start, end, interval)
                .map_err(|error| DataFusionError::Plan(error.into()))?,
        ),
        lookback,
        offset,
        sum,
    });
    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(original.schema()),
        Partitioning::UnknownPartitioning(original.properties().partitioning.partition_count()),
        EmissionType::Final,
        Boundedness::Bounded,
    ));
    Ok(Some(ScanMaxExec {
        scan: scan.clone(),
        original,
        spec,
        properties,
    }))
}

#[derive(Debug)]
struct ScanMaxExec {
    scan: RegionScanExec,
    original: Arc<dyn ExecutionPlan>,
    spec: Arc<InstantMaxSpec>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for ScanMaxExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ResearchScanInstantMax: child is the pre-consumption fallback"
        )
    }
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

fn match_packed_max(original: Arc<dyn ExecutionPlan>) -> Result<Option<PackedMaxExec>> {
    use datafusion::functions_aggregate::array_agg::ArrayAgg;

    let Some(packed) = original.downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    if packed.mode() != &AggregateMode::Partial
        || !packed.group_expr().is_single()
        || packed.input_order_mode() != &InputOrderMode::Linear
        || packed.limit_options().is_some()
        || packed.filter_expr().iter().any(Option::is_some)
    {
        return Ok(None);
    }
    let [function] = packed.aggr_expr() else {
        return Ok(None);
    };
    if !function.fun().inner().is::<ArrayAgg>()
        || function.is_distinct()
        || !function.order_bys().is_empty()
    {
        return Ok(None);
    }
    let arguments = function.expressions();
    let [argument] = arguments.as_slice() else {
        return Ok(None);
    };
    let Some(point) = argument.downcast_ref::<ScalarFunctionExpr>() else {
        return Ok(None);
    };
    if point.fun().name() != "named_struct" {
        return Ok(None);
    }
    let [time_name, time, value_name, value] = point.args() else {
        return Ok(None);
    };
    let named = |expression: &Arc<dyn PhysicalExpr>, name: &str| {
        expression
            .downcast_ref::<Literal>()
            .is_some_and(|literal| literal.value() == &ScalarValue::Utf8(Some(name.into())))
    };
    if !named(time_name, "time") || !named(value_name, "value") {
        return Ok(None);
    }
    let Some(final_max) = packed.input().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    if final_max.mode() != &AggregateMode::FinalPartitioned
        || !final_max.group_expr().is_single()
        || final_max.limit_options().is_some()
        || final_max.filter_expr().iter().any(Option::is_some)
    {
        return Ok(None);
    }
    let [max] = final_max.aggr_expr() else {
        return Ok(None);
    };
    if max.fun().name() != "max" || max.is_distinct() || !max.order_bys().is_empty() {
        return Ok(None);
    }
    let Some(exchange) = final_max.input().downcast_ref::<RepartitionExec>() else {
        return Ok(None);
    };
    // Inspect the original partial max: no scan has been consumed by the local rule.
    let Some(scan) = match_plan(exchange.input().clone())? else {
        return Ok(None);
    };
    let Some(partial_max) = exchange.input().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    let width = scan.spec.groups.len();
    let Some(timestamp) = scan
        .spec
        .groups
        .iter()
        .position(|&index| index == scan.spec.timestamp)
    else {
        return Ok(None);
    };
    if final_max.aggr_expr() != partial_max.aggr_expr()
        || final_max.input_schema() != partial_max.input_schema()
        || final_max.group_expr().expr().len() != width
        || final_max
            .group_expr()
            .expr()
            .iter()
            .enumerate()
            .any(|(index, (expression, _))| column(expression) != Some(index))
        || column(time) != Some(timestamp)
        || column(value) != Some(width)
    {
        return Ok(None);
    }
    let expected_labels: Vec<_> = (0..width).filter(|&index| index != timestamp).collect();
    if packed
        .group_expr()
        .expr()
        .iter()
        .map(|(expression, _)| column(expression))
        .collect::<Option<Vec<_>>>()
        .as_ref()
        != Some(&expected_labels)
    {
        return Ok(None);
    }
    let schema = original.schema();
    if schema.fields().len() != width {
        return Ok(None);
    }
    let DataType::List(item) = schema.field(width - 1).data_type() else {
        return Ok(None);
    };
    let DataType::Struct(fields) = item.data_type() else {
        return Ok(None);
    };
    if fields.len() != 2
        || fields[0].name() != "time"
        || fields[1].name() != "value"
        || fields[0].data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
        || fields[1].data_type() != &DataType::Float64
    {
        return Ok(None);
    }
    let packed_schema = Arc::new(
        datatypes::schema::Schema::try_from(schema.clone())
            .map_err(|error| DataFusionError::External(Box::new(error)))?,
    );
    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ));
    Ok(Some(PackedMaxExec {
        scan: scan.scan,
        spec: scan.spec,
        original,
        packed_schema,
        properties,
    }))
}

#[derive(Debug)]
struct PackedMaxExec {
    scan: RegionScanExec,
    spec: Arc<InstantMaxSpec>,
    original: Arc<dyn ExecutionPlan>,
    packed_schema: datatypes::schema::SchemaRef,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for PackedMaxExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ResearchPackedInstantMax: child is the pre-consumption fallback"
        )
    }
}

impl ExecutionPlan for PackedMaxExec {
    fn name(&self) -> &str {
        "ResearchPackedInstantMax"
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.original]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [original] = children.as_slice() else {
            return Err(DataFusionError::Plan(
                "Packed max requires one fallback child".into(),
            ));
        };
        Ok(Arc::new(match_packed_max(original.clone())?.ok_or_else(
            || DataFusionError::Plan("Packed max child no longer eligible".into()),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "Packed max has one output partition".into(),
            ));
        }
        let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let mut streams = Vec::new();
        for partition in 0..self.scan.properties().partitioning.partition_count() {
            let memory = MemoryConsumer::new("ResearchPackedInstantMaxLocal")
                .register(context.memory_pool());
            let processor = InstantMaxProcessor::new(self.spec.clone(), memory, guard.0.clone())?
                .with_packed_schema(self.packed_schema.clone());
            let Some(stream) = self.scan.execute_with_processor(
                partition,
                context.clone(),
                Box::new(processor),
            )?
            else {
                if partition == 0 {
                    return CoalescePartitionsExec::new(self.original.clone()).execute(0, context);
                }
                return Err(DataFusionError::Execution(
                    "Packed max scan capability changed across partitions".into(),
                ));
            };
            streams.push(stream);
        }
        common_telemetry::debug!(
            partitions = streams.len(),
            "Research packed instant max activated"
        );
        let memory =
            MemoryConsumer::new("ResearchPackedInstantMaxMerge").register(context.memory_pool());
        let retained = memory.new_empty();
        let spec = self.spec.clone();
        let schema = self.packed_schema.clone();
        let output_schema = schema.arrow_schema().clone();
        let work = stream::once(async move {
            let mut input = stream::select_all(streams);
            let mut batches = Vec::new();
            while let Some(batch) = input.try_next().await? {
                retained.try_grow(batch.get_array_memory_size().saturating_add(256))?;
                batches.push(batch);
            }
            let cancelled = guard.0.clone();
            let output = common_runtime::spawn_blocking_query(move || -> Result<_> {
                let _retained = retained;
                let mut processor =
                    InstantMaxProcessor::new(spec, memory, cancelled)?.with_packed_schema(schema);
                for batch in batches {
                    processor.merge_packed(&batch)?;
                }
                let output = processor
                    .finish()
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                // Keep the reservation alive while Arrow buffers leave the processor.
                Ok((output, processor))
            })
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))??;
            drop(guard);
            Ok::<_, DataFusionError>(output)
        });
        let batches = work
            .map_ok(|(batches, processor)| {
                stream::unfold(
                    (batches.into_iter(), processor),
                    |(mut batches, processor)| async move {
                        batches
                            .next()
                            .map(|batch| (Ok(batch.into_df_record_batch()), (batches, processor)))
                    },
                )
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            batches,
        )))
    }
}

impl ExecutionPlan for ScanMaxExec {
    fn name(&self) -> &str {
        "ResearchScanInstantMax"
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.original]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [original] = children.as_slice() else {
            return Err(DataFusionError::Plan(
                "Scan max requires one fallback child".into(),
            ));
        };
        Ok(Arc::new(match_plan(original.clone())?.ok_or_else(
            || DataFusionError::Plan("Scan max child no longer eligible".into()),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let memory = MemoryConsumer::new("ResearchScanInstantMax").register(context.memory_pool());
        let processor = InstantMaxProcessor::new(self.spec.clone(), memory, guard.0.clone())?;
        let Some(stream) =
            self.scan
                .execute_with_processor(partition, context.clone(), Box::new(processor))?
        else {
            return self.original.execute(partition, context);
        };
        common_telemetry::debug!(partition, "Research scan instant max activated");
        let schema = stream.schema();
        let retained = stream.map(move |batch| {
            let _ = &guard;
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, retained)))
    }
}
