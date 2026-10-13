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

use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::functions_aggregate::sum::Sum;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::InputOrderMode;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coop::{CooperativeExec, cooperative};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_physical_expr::expressions::{BinaryExpr, Column, IsNotNullExpr, Literal};
use datafusion_physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, ScalarFunctionExpr,
};
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use futures::StreamExt;
use promql::extension_plan::{RangeManipulateExec, SeriesDivideExec, SeriesNormalizeExec};
use table::table::scan::RegionScanExec;

use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::scan_range::{
    RangeFunction, RangeOutput, RangeProcessor, RangeSpec,
};

/// Grid length above which counter ranges are not packed into per-series lists.
const PACKED_COUNTER_GRID_LIMIT: usize = 1024;

pub(crate) fn replace(original: Arc<dyn ExecutionPlan>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if std::env::var_os("GT_RESEARCH_PACKED_SERIES").is_some()
        && let Some(plan) = match_packed(original.clone())?
    {
        return Ok(Some(Arc::new(plan)));
    }
    if std::env::var_os("GT_RESEARCH_DIRECT_RATE_AGG").is_some()
        && let Some(plan) = match_aggregate(original.clone())?
    {
        return Ok(Some(Arc::new(plan)));
    }
    Ok(match_plan(original)?.map(|plan| Arc::new(plan) as Arc<dyn ExecutionPlan>))
}

fn match_packed(original: Arc<dyn ExecutionPlan>) -> Result<Option<ScanRangeExec>> {
    use crate::research_series_grid::scan_range::PackedRangeSpec;
    use datafusion::functions_aggregate::array_agg::ArrayAgg;

    let Some(aggregate) = original.downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    // Multiple source series may produce the same label tuple. The final aggregate
    // must merge their lists; replacing a Single aggregate would violate uniqueness.
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
    if !function.fun().inner().is::<ArrayAgg>()
        || function.is_distinct()
        || !function.order_bys().is_empty()
    {
        return Ok(None);
    }
    let args = function.expressions();
    let [argument] = args.as_slice() else {
        return Ok(None);
    };
    let Some(mut groups) = aggregate
        .group_expr()
        .expr()
        .iter()
        .map(|(expr, _)| column(expr))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let mut expression = argument.clone();
    let mut input = aggregate.input().clone();
    while let Some(projection) = input.downcast_ref::<ProjectionExec>() {
        let Some(mapped) = groups
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
        groups = mapped;
        expression = expression
            .transform_up(|expr| {
                Ok(if let Some(index) = column(&expr) {
                    let mapped = projection.expr().get(index).ok_or_else(|| {
                        DataFusionError::Plan("Packed projection index out of bounds".into())
                    })?;
                    Transformed::yes(mapped.expr.clone())
                } else {
                    Transformed::no(expr)
                })
            })?
            .data;
        input = projection.input().clone();
    }
    let Some(range) = input.downcast_ref::<ScanRangeExec>() else {
        return Ok(None);
    };
    if range.spec.aggregate.is_some() || range.spec.packed.is_some() {
        return Ok(None);
    }
    let Some(point) = expression.downcast_ref::<ScalarFunctionExpr>() else {
        return Ok(None);
    };
    if point.fun().name() != "named_struct" {
        return Ok(None);
    }
    let [time_name, time, value_name, value] = point.args() else {
        return Ok(None);
    };
    let named = |expr: &Arc<dyn PhysicalExpr>, name: &str| {
        expr.downcast_ref::<Literal>()
            .is_some_and(|literal| literal.value() == &ScalarValue::Utf8(Some(name.into())))
    };
    if !named(time_name, "time")
        || !named(value_name, "value")
        || !column(time).is_some_and(|index| {
            matches!(range.spec.output.get(index), Some(RangeOutput::Timestamp))
        })
    {
        return Ok(None);
    }
    let Some(value_index) = range
        .spec
        .output
        .iter()
        .position(|output| matches!(output, RangeOutput::Value))
    else {
        return Ok(None);
    };
    if !rate_expression(value, value_index)
        || value.data_type(range.schema().as_ref())? != DataType::Float64
    {
        return Ok(None);
    }
    let Some(labels) = groups
        .iter()
        .map(|&index| {
            matches!(range.spec.output.get(index), Some(RangeOutput::Tag(_))).then(|| {
                range.spec.output[..index]
                    .iter()
                    .filter(|item| matches!(item, RangeOutput::Tag(_)))
                    .count()
            })
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let schema = original.schema();
    if schema.fields().len() != labels.len() + 1 {
        return Ok(None);
    }
    let DataType::List(item) = schema.field(labels.len()).data_type() else {
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
    let expression = value
        .clone()
        .transform_up(|expr| {
            Ok(if column(&expr).is_some() {
                Transformed::yes(Arc::new(Column::new("rate", 0)) as Arc<dyn PhysicalExpr>)
            } else {
                Transformed::no(expr)
            })
        })?
        .data;
    // A packed list holds a full grid per input series. Over long ranges with many
    // short-lived (churned) counter series those lists are mostly empty and exceed the
    // query pool; the unpacked scan emits only present points.
    if range.spec.grid.instants.len() > PACKED_COUNTER_GRID_LIMIT
        && !matches!(
            range.spec.function,
            RangeFunction::Instant | RangeFunction::Max
        )
    {
        return Ok(None);
    }
    let mut spec = range.spec.as_ref().clone();
    spec.packed = Some(Arc::new(PackedRangeSpec {
        batch_output: std::env::var_os("GT_RESEARCH_SERIES_PACK").is_some(),
        schema: Arc::new(
            datatypes::schema::Schema::try_from(schema.clone())
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        ),
        labels,
        expression,
    }));
    Ok(Some(ScanRangeExec {
        cooperative_input: range.cooperative_input,
        scan: range.scan.clone(),
        spec: Arc::new(spec),
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(original.properties().partitioning.partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        )),
        original,
    }))
}

fn match_aggregate(original: Arc<dyn ExecutionPlan>) -> Result<Option<ScanRangeExec>> {
    use super::scan_range_aggregate::RangeAggregateSpec;
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
    if !function.fun().inner().is::<Sum>()
        || function.is_distinct()
        || !function.order_bys().is_empty()
        || !function.groups_accumulator_supported()
    {
        return Ok(None);
    }
    let args = function.expressions();
    let [argument] = args.as_slice() else {
        return Ok(None);
    };
    if argument.data_type(aggregate.input().schema().as_ref())? != DataType::Float64 {
        return Ok(None);
    }
    let Some(mut groups) = aggregate
        .group_expr()
        .expr()
        .iter()
        .map(|(expr, _)| column(expr))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let mut expression = argument.clone();
    let mut input = aggregate.input().clone();
    while let Some(projection) = input.downcast_ref::<ProjectionExec>() {
        let Some(mapped) = groups
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
        groups = mapped;
        expression = expression
            .transform_up(|expr| {
                Ok(if let Some(index) = column(&expr) {
                    let mapped = projection.expr().get(index).ok_or_else(|| {
                        DataFusionError::Plan("Aggregate projection index out of bounds".into())
                    })?;
                    Transformed::yes(mapped.expr.clone())
                } else {
                    Transformed::no(expr)
                })
            })?
            .data;
        input = projection.input().clone();
    }
    let Some(range) = input.downcast_ref::<ScanRangeExec>() else {
        return Ok(None);
    };
    if range.spec.function != RangeFunction::Rate
        || range.spec.aggregate.is_some()
        || range.spec.packed.is_some()
    {
        return Ok(None);
    }
    let Some(value) = range
        .spec
        .output
        .iter()
        .position(|output| matches!(output, RangeOutput::Value))
    else {
        return Ok(None);
    };
    if !rate_expression(&expression, value) {
        return Ok(None);
    }
    expression = expression
        .transform_up(|expr| {
            Ok(if column(&expr).is_some() {
                Transformed::yes(Arc::new(Column::new("rate", 0)) as Arc<dyn PhysicalExpr>)
            } else {
                Transformed::no(expr)
            })
        })?
        .data;
    let mut time_groups = 0;
    let mut mapped = Vec::new();
    for index in groups {
        match range.spec.output.get(index) {
            Some(RangeOutput::Timestamp) => {
                time_groups += 1;
                mapped.push(None);
            }
            Some(RangeOutput::Tag(_)) => mapped.push(Some(
                range.spec.output[..index]
                    .iter()
                    .filter(|v| matches!(v, RangeOutput::Tag(_)))
                    .count(),
            )),
            _ => return Ok(None),
        }
    }
    if time_groups != 1 {
        return Ok(None);
    }
    let mut spec = range.spec.as_ref().clone();
    spec.aggregate = Some(Arc::new(RangeAggregateSpec {
        schema: Arc::new(
            datatypes::schema::Schema::try_from(original.schema())
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ),
        groups: mapped,
        expression,
        function: function.clone(),
    }));
    Ok(Some(ScanRangeExec {
        cooperative_input: range.cooperative_input,
        scan: range.scan.clone(),
        spec: Arc::new(spec),
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(original.schema()),
            Partitioning::UnknownPartitioning(original.properties().partitioning.partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        )),
        original,
    }))
}

fn rate_expression(expr: &Arc<dyn PhysicalExpr>, value: usize) -> bool {
    if let Some(index) = column(expr) {
        return index == value;
    }
    if let Some(literal) = expr.downcast_ref::<Literal>() {
        return matches!(literal.value(), ScalarValue::Float64(Some(_)));
    }
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        return matches!(
            binary.op(),
            Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide
        ) && rate_expression(binary.left(), value)
            && rate_expression(binary.right(), value);
    }
    false
}

fn column(expr: &Arc<dyn PhysicalExpr>) -> Option<usize> {
    expr.downcast_ref::<Column>().map(Column::index)
}

pub(crate) fn bind_program(
    scan: Arc<dyn ExecutionPlan>,
    spec: Arc<RangeSpec>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if scan.properties().partitioning.partition_count() != 1 || spec.packed.is_none() {
        return Ok(None);
    }
    let original = Arc::new(super::series_range::SeriesRangeExec::new(
        scan.clone(),
        spec.clone(),
        spec.offset,
    ));
    let Some(bound) = bind_scan(original, scan, spec.as_ref().clone(), false)? else {
        return Ok(None);
    };
    let scan = bound
        .scan
        .with_new_partitions(bound.scan.get_uncollapsed_partition_ranges(), 1)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(Some(Arc::new(super::series_range::SeriesProgramExec::new(
        scan, bound.spec,
    ))))
}

fn match_plan(original: Arc<dyn ExecutionPlan>) -> Result<Option<ScanRangeExec>> {
    if let Some(range) = original.downcast_ref::<super::series_range::SeriesRangeExec>() {
        let mut spec = range.spec.as_ref().clone();
        spec.offset = range.raw_offset;
        return bind_scan(original.clone(), range.children()[0].clone(), spec, false);
    }
    let Some(filter) = original.downcast_ref::<FilterExec>() else {
        return Ok(None);
    };
    let Some(non_null) = filter.predicate().downcast_ref::<IsNotNullExpr>() else {
        return Ok(None);
    };
    let Some(filtered_value) = column(non_null.arg()) else {
        return Ok(None);
    };
    let Some(projection) = filter.input().downcast_ref::<ProjectionExec>() else {
        return Ok(None);
    };
    if filter.projection().is_some()
        || filter.fetch().is_some()
        || filter.schema() != projection.schema()
    {
        return Ok(None);
    }
    let Some(range) = projection.input().downcast_ref::<RangeManipulateExec>() else {
        return Ok(None);
    };
    let Some((start, end, interval, window, offset, time_name, range_name, value_name)) =
        range.research_scan_window()
    else {
        return Ok(None);
    };
    if interval <= 0
        || window <= 0
        || window > interval
        || end < start
        || (i128::from(end) - i128::from(start)) / i128::from(interval) + 1 > 1_000_000
    {
        return Ok(None);
    }
    let range_schema = range.schema();
    let (Ok(timestamp), Ok(time_range), Ok(value)) = (
        range_schema.index_of(time_name),
        range_schema.index_of(range_name),
        range_schema.index_of(value_name),
    ) else {
        return Ok(None);
    };
    let mut output = Vec::new();
    let mut functions = 0;
    let mut range_function = RangeFunction::Max;
    let mut has_time = false;
    for expr in projection.expr() {
        if let Some(index) = column(&expr.expr) {
            if index == timestamp {
                output.push(RangeOutput::Timestamp);
                has_time = true;
            } else if index == value || index == time_range {
                return Ok(None);
            } else {
                output.push(RangeOutput::Tag(index));
            }
        } else if let Some(function) = expr.expr.downcast_ref::<ScalarFunctionExpr>() {
            match (function.fun().name(), function.args()) {
                ("prom_max_over_time", [time_arg, value_arg])
                    if std::env::var_os("GT_RESEARCH_DIRECT_RANGE").is_some()
                        && column(time_arg) == Some(time_range)
                        && column(value_arg) == Some(value) =>
                {
                    range_function = RangeFunction::Max;
                }
                ("prom_rate", [time_arg, value_arg, eval_arg, length_arg])
                    if std::env::var_os("GT_RESEARCH_DIRECT_RATE").is_some()
                        && column(time_arg) == Some(time_range)
                        && column(value_arg) == Some(value)
                        && column(eval_arg) == Some(timestamp)
                        && length_arg.downcast_ref::<Literal>().is_some_and(|literal| {
                            literal.value() == &ScalarValue::Int64(Some(window))
                        }) =>
                {
                    range_function = RangeFunction::Rate;
                }
                _ => return Ok(None),
            }
            functions += 1;
            output.push(RangeOutput::Value);
        } else {
            return Ok(None);
        }
    }
    if functions != 1
        || !has_time
        || !matches!(output.get(filtered_value), Some(RangeOutput::Value))
    {
        return Ok(None);
    }
    let input = range.children()[0].clone();
    let input_schema = input.schema();
    let Ok(tsid) = input_schema.index_of("__tsid") else {
        return Ok(None);
    };
    let spec = RangeSpec {
        aggregate: None,
        packed: None,
        function: range_function,
        schema: Arc::new(
            datatypes::schema::Schema::try_from(original.schema())
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        ),
        input_schema,
        output,
        timestamp,
        value,
        tsid,
        grid: Arc::new(
            TimeGrid::new(start, end, interval)
                .map_err(|error| DataFusionError::Plan(error.into()))?,
        ),
        window,
        offset,
    };
    bind_scan(original, input, spec, true)
}

fn bind_scan(
    original: Arc<dyn ExecutionPlan>,
    mut input: Arc<dyn ExecutionPlan>,
    mut spec: RangeSpec,
    normalized: bool,
) -> Result<Option<ScanRangeExec>> {
    if normalized {
        let time_name = spec.input_schema.field(spec.timestamp).name();
        let Some(normalize) = input.downcast_ref::<SeriesNormalizeExec>() else {
            return Ok(None);
        };
        if normalize.research_scan_normalization() != (spec.offset, time_name, true) {
            return Ok(None);
        }
        input = normalize.children()[0].clone();
        let Some(divide) = input.downcast_ref::<SeriesDivideExec>() else {
            return Ok(None);
        };
        if divide.research_tags() != ["__tsid"] {
            return Ok(None);
        }
        input = divide.children()[0].clone();
    }
    let mut mapped_time = spec.timestamp;
    let mut mapped_value = spec.value;
    let mut cooperative_input = false;
    loop {
        if let Some(wrapper) = input.downcast_ref::<CooperativeExec>() {
            cooperative_input = true;
            input = wrapper.input().clone();
            continue;
        }
        let Some(projection) = input.downcast_ref::<ProjectionExec>() else {
            break;
        };
        let map = |index: usize| {
            projection
                .expr()
                .get(index)
                .and_then(|expr| column(&expr.expr))
        };
        let (Some(time), Some(value)) = (map(mapped_time), map(mapped_value)) else {
            return Ok(None);
        };
        mapped_time = time;
        mapped_value = value;
        for item in &mut spec.output {
            if let RangeOutput::Tag(index) = item {
                let Some(mapped) = map(*index) else {
                    return Ok(None);
                };
                *index = mapped;
            }
        }
        input = projection.input().clone();
    }
    let Some(scan) = input.downcast_ref::<RegionScanExec>() else {
        return Ok(None);
    };
    let schema = scan.schema();
    let Ok(tsid) = schema.index_of("__tsid") else {
        return Ok(None);
    };
    if scan.scanner_type() != "SeriesScan"
        || schema.index_of(&scan.time_index()).ok() != Some(mapped_time)
        || schema.field(mapped_time).data_type()
            != &DataType::Timestamp(TimeUnit::Millisecond, None)
        || schema.field(mapped_value).data_type() != &DataType::Float64
        || schema.field(mapped_value).name() != "greptime_value"
        || schema.field(tsid).data_type() != &DataType::UInt64
        || schema.index_of("greptime_native_histogram").is_ok()
    {
        return Ok(None);
    }
    let tags = scan.tag_columns();
    if spec.output.iter().any(|output| matches!(output, RangeOutput::Tag(index) if !tags.contains(schema.field(*index).name()))) { return Ok(None); }
    spec.input_schema = schema;
    spec.timestamp = mapped_time;
    spec.value = mapped_value;
    spec.tsid = tsid;
    let spec = Arc::new(spec);
    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(original.schema()),
        Partitioning::UnknownPartitioning(original.properties().partitioning.partition_count()),
        EmissionType::Final,
        Boundedness::Bounded,
    ));
    Ok(Some(ScanRangeExec {
        cooperative_input,
        scan: scan.clone(),
        original,
        spec,
        properties,
    }))
}

#[derive(Debug)]
struct ScanRangeExec {
    cooperative_input: bool,
    scan: RegionScanExec,
    original: Arc<dyn ExecutionPlan>,
    spec: Arc<RangeSpec>,
    properties: Arc<PlanProperties>,
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl DisplayAs for ScanRangeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: child is the pre-consumption fallback", self.name())
    }
}

impl ExecutionPlan for ScanRangeExec {
    fn name(&self) -> &str {
        if self.spec.packed.is_some() {
            return "ResearchPackedSeriesRange";
        }
        if self.spec.aggregate.is_some() {
            return "ResearchScanRangeRateAggregate";
        }
        match self.spec.function {
            RangeFunction::Max => "ResearchScanRangeMax",
            RangeFunction::Rate => "ResearchScanRangeRate",
            RangeFunction::Increase => "ResearchScanRangeIncrease",
            RangeFunction::Irate => "ResearchScanRangeIrate",
            RangeFunction::Instant => "ResearchScanSeriesInstant",
        }
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
                "Range scan requires one fallback child".into(),
            ));
        };
        replace(original.clone())?
            .ok_or_else(|| DataFusionError::Plan("Range scan child no longer eligible".into()))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let memory = MemoryConsumer::new(self.name()).register(context.memory_pool());
        let processor = RangeProcessor::new(self.spec.clone(), memory, guard.0.clone())?;
        let Some(stream) =
            self.scan
                .execute_with_processor(partition, context.clone(), Box::new(processor))?
        else {
            return self.original.execute(partition, context);
        };
        match self.spec.function {
            RangeFunction::Max => {
                common_telemetry::debug!(partition, "Research scan range max activated")
            }
            RangeFunction::Rate => {
                common_telemetry::debug!(partition, "Research scan range rate activated")
            }
            function => {
                common_telemetry::debug!(
                    partition,
                    function = function.name(),
                    "Research scan counter activated"
                )
            }
        }
        let schema = stream.schema();
        let retained = stream.map(move |batch| {
            let _ = &guard;
            batch
        });
        let stream = RecordBatchStreamAdapter::new(schema, retained);
        if self.cooperative_input {
            Ok(Box::pin(cooperative(stream)))
        } else {
            Ok(Box::pin(stream))
        }
    }
}
