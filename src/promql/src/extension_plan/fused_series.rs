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

//! One physical node that evaluates one range function and then 0..N float map kernels over raw
//! series, converting every evaluated chunk directly into one output batch. A chunk carries only
//! the positions that hold a value, so the node emits a sparse axis as the rows it holds instead
//! of paying for the holes between them.
//!
//! There is no `SeriesFrame` pack/unpack and no intermediate `RecordBatch` between the evaluation
//! and the map kernels: both stages run over typed [`Series`] values inside this node.
//!
//! Phase 1 scope: the child produces raw frames of one partition with explicit labels and is not
//! interleaved, and the output schema must classify into exactly one timestamp column, one
//! `Float64` column per series field, and one `Utf8` column per declared tag. Anything outside
//! this scope is rejected at construction instead of silently falling back. The field and tag
//! columns must be declared nullable: a grid point inside the trimmed interval can be missing, and
//! the range functions and the map kernels drop `__name__`.
//!
//! This node computes floats only, and admitting raw samples is the caller's responsibility:
//! validating the declared frame schema cannot prove that the samples are float, so a histogram
//! or mixed sample passes construction and surfaces at run time as [`RangeEvaluator`]'s (or a map
//! kernel's) `NotImplemented`. That runtime type check is kept rather than replaced by a schema
//! check that could not prove the same thing.
//!
//! Duplicate label sets follow Prometheus' range-query rule. `rate` and the map kernels drop
//! `__name__`, so two distinct raw series can reach the same final label set; if both have a
//! sample at the same evaluation timestamp that is an error (`vector cannot contain metrics with
//! the same labelset`), while series whose occupied grid points are disjoint are legal and
//! Prometheus merges them into one output series. This node emits such series as separate batches
//! and defers that merge to matrix assembly, so no sample is dropped.
//!
//! Memory: the only state retained across polls that grows with the output is the per-stream
//! label-set occupancy map, and its bytes are reserved on the task's memory pool. The range-window
//! samples [`RangeEvaluator`] retains across chunks and the batches already handed downstream are
//! deliberately not reserved here.
//!
//! The child's `is_last` is the only completion signal. This node neither recomputes finality
//! across inputs nor emits a series it has not seen finalized, so a child that merges regions must
//! recompute `is_last` globally before handing its frames over; a region-local flag would finalize
//! a series early.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{self, Formatter};
use std::mem::size_of;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common_query::series_frame::unpack_raw_series;
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datatypes::series::samples::{DenseSamplesRef, SampleRef, Samples, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, TimeAxis};
use datatypes::series::{MetricName, SeriesOrdering};
use futures::Stream;
use store_api::region_engine::{RawLabelSource, SendableRawSeriesStream};

use crate::map::{MathOp, map_math};
use crate::range_eval::{RangeEvaluator, RangeFunction, RangeWindow};
use crate::source::materialize_raw_labels;

#[cfg(test)]
mod tests;

/// Where one output column of this node comes from.
#[derive(Clone, Copy, Debug)]
enum OutputColumn {
    /// The evaluation grid timestamps of the current interval.
    Time,
    /// Series field by index in the input series schema.
    Field(usize),
    /// Declared logical tag by index in the caller-supplied tag list.
    Tag(usize),
}

/// Evaluates one range function followed by 0..N float map kernels per raw series.
pub struct FusedSeriesExec {
    input: Arc<dyn ExecutionPlan>,
    /// Raw (no evaluation grid) schema of the input and of the evaluated series.
    series_schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    ordering: SeriesOrdering,
    grid: Arc<EvalGrid>,
    window: RangeWindow,
    function: RangeFunction,
    maps: Vec<MathOp>,
    output_schema: SchemaRef,
    tags: Vec<String>,
    time_column: String,
    /// One entry per output column, in output column order.
    classification: Arc<[OutputColumn]>,
    /// Shared by every partition's stream, so a duplicate label set is caught across partitions.
    labels: SharedLabelSets,
    properties: Arc<PlanProperties>,
}

impl FusedSeriesExec {
    /// Creates the node, rejecting everything the phase 1 contract cannot execute.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        series_schema: Arc<SeriesSchema>,
        label_source: RawLabelSource,
        ordering: SeriesOrdering,
        grid: Arc<EvalGrid>,
        window: RangeWindow,
        function: RangeFunction,
        maps: Vec<MathOp>,
        output_schema: SchemaRef,
        tags: Vec<String>,
        time_column: String,
    ) -> Result<Self> {
        // One evaluator runs per input partition, so the node's partition count is the child's.
        // The child must therefore be a raw-series scan that assigns every series whole to one
        // partition: `PerSeries` distribution is what makes a per-partition evaluation equivalent
        // to evaluating the whole series, because no series is split across partitions and no
        // series appears in two of them.
        let partition_count = input.properties().partitioning.partition_count();
        if partition_count == 0 {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec input has no partitions".into(),
            ));
        }
        if series_schema.eval_grid.is_some() {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec input must be a raw series schema without an evaluation grid"
                    .into(),
            ));
        }
        // Both bindings are accepted: explicit chunks already carry complete labels, and
        // primary-key chunks are decoded through `materialize_raw_labels` in `execute`.
        if ordering == SeriesOrdering::Interleaved {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec does not support interleaved raw series ordering".into(),
            ));
        }
        // Surfaces invalid windows, smoothing/trend factors and predict_linear overflow here
        // rather than on the first chunk.
        drop(RangeEvaluator::try_new(
            &series_schema,
            grid.clone(),
            window,
            function,
        )?);
        validate_raw_frame_schema(&input, &series_schema, &label_source, ordering)?;
        let classification = classify_output(&output_schema, &series_schema, &tags, &time_column)?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        // Shared by every partition's stream. Creating it needs no memory pool; each stream
        // accounts its own additions in its own reservation.
        let labels: SharedLabelSets = Arc::new(Mutex::new(HashMap::new()));
        Ok(Self {
            input,
            series_schema,
            label_source,
            ordering,
            grid,
            window,
            function,
            maps,
            output_schema,
            tags,
            time_column,
            classification: classification.into(),
            labels,
            properties,
        })
    }

    fn name() -> &'static str {
        "FusedSeriesExec"
    }
}

impl std::fmt::Debug for FusedSeriesExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FusedSeriesExec").finish()
    }
}

impl DisplayAs for FusedSeriesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        let maps: Vec<_> = self.maps.iter().map(|op| describe_map(*op)).collect();
        write!(
            f,
            "FusedSeriesExec: eval={}, range={}ms, offset={}ms, maps=[{}]",
            describe_function(self.function),
            self.window.range_ms,
            self.window.offset_ms,
            maps.join(", ")
        )
    }
}

impl ExecutionPlan for FusedSeriesExec {
    fn name(&self) -> &str {
        Self::name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Plan(format!(
                "FusedSeriesExec requires exactly one child, got {}",
                children.len()
            ))
        })?;
        if child.schema() != self.input.schema() {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec child must keep its declared raw series frame schema".into(),
            ));
        }
        let exec = Self::try_new(
            child,
            self.series_schema.clone(),
            self.label_source.clone(),
            self.ordering,
            self.grid.clone(),
            self.window,
            self.function,
            self.maps.clone(),
            self.output_schema.clone(),
            self.tags.clone(),
            self.time_column.clone(),
        )?;
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partitions = self.properties.output_partitioning().partition_count();
        if partition >= partitions {
            return Err(DataFusionError::Execution(format!(
                "partition {partition} out of bounds for {partitions} partitions"
            )));
        }
        let memory_pool = Arc::clone(&context.runtime_env().memory_pool);
        let frame = self.input.execute(partition, context)?;
        let input = unpack_raw_series(
            frame,
            self.series_schema.clone(),
            self.label_source.clone(),
            self.ordering,
        )?;
        // `RangeEvaluator` needs complete labels. Explicit sources already supply them; a
        // scan-bound primary-key source is decoded here instead of in every consumer.
        let input = materialize_raw_labels(input).map_err(|error| {
            DataFusionError::Execution(format!("cannot materialize raw series labels: {error}"))
        })?;
        // The kernels are `FnMut` and not cloneable, so every stream needs its own evaluator.
        let evaluator = RangeEvaluator::try_new(
            &self.series_schema,
            self.grid.clone(),
            self.window,
            self.function,
        )?;
        Ok(Box::pin(FusedSeriesStream {
            input,
            evaluator: Some(evaluator),
            maps: self.maps.clone(),
            tags: self.tags.clone().into(),
            output_schema: self.output_schema.clone(),
            classification: self.classification.clone(),
            // Shared with every other partition's stream: a duplicate label set is a property of
            // the whole output, not of one partition.
            labels: Arc::clone(&self.labels),
            reservation: MemoryConsumer::new("FusedSeriesStream").register(&memory_pool),
            finished: false,
        }))
    }
}

/// Checks that the child's declared schema is a legal raw frame for this binding.
///
/// The check runs at construction time by feeding an empty stream with the declared schema
/// through the same decoder execution uses, and discarding the resulting stream.
fn validate_raw_frame_schema(
    input: &Arc<dyn ExecutionPlan>,
    series_schema: &Arc<SeriesSchema>,
    label_source: &RawLabelSource,
    ordering: SeriesOrdering,
) -> Result<()> {
    let empty = futures::stream::empty::<Result<RecordBatch>>();
    let frame = Box::pin(RecordBatchStreamAdapter::new(input.schema(), empty));
    drop(unpack_raw_series(
        frame,
        series_schema.clone(),
        label_source.clone(),
        ordering,
    )?);
    Ok(())
}

/// Classifies every output column, rejecting anything outside the phase 1 output contract.
fn classify_output(
    output_schema: &SchemaRef,
    series_schema: &SeriesSchema,
    tags: &[String],
    time_column: &str,
) -> Result<Vec<OutputColumn>> {
    for (index, tag) in tags.iter().enumerate() {
        if tags[..index].contains(tag) {
            return Err(DataFusionError::Plan(format!(
                "duplicate tag column {tag:?} in FusedSeriesExec output schema"
            )));
        }
        if tag == time_column {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec cannot be the time column"
            )));
        }
        if series_schema
            .fields
            .iter()
            .any(|field| field.name.as_ref() == tag)
        {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec clashes with a series field"
            )));
        }
    }

    let mut time_seen = false;
    let mut field_seen = vec![false; series_schema.fields.len()];
    let mut tag_seen = vec![false; tags.len()];
    let mut classification = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        let name = field.name();
        if name == time_column {
            if std::mem::replace(&mut time_seen, true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate time column {name:?} in FusedSeriesExec output schema"
                )));
            }
            match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, None) => {}
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "time column {name:?} of FusedSeriesExec must be Timestamp(Millisecond, \
                         None), got {other:?}"
                    )));
                }
            }
            if field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "time column {name:?} of FusedSeriesExec must not be nullable"
                )));
            }
            classification.push(OutputColumn::Time);
            continue;
        }
        if let Some(index) = series_schema
            .fields
            .iter()
            .position(|field| field.name.as_ref() == name)
        {
            if std::mem::replace(&mut field_seen[index], true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate field column {name:?} in FusedSeriesExec output schema"
                )));
            }
            if field.data_type() != &DataType::Float64 {
                return Err(DataFusionError::Plan(format!(
                    "field column {name:?} of FusedSeriesExec must be Float64, got {:?}",
                    field.data_type()
                )));
            }
            // A grid point inside the trimmed interval can have no value in this field.
            if !field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "field column {name:?} of FusedSeriesExec must be nullable, because a grid \
                     point can be missing"
                )));
            }
            classification.push(OutputColumn::Field(index));
            continue;
        }
        if let Some(index) = tags.iter().position(|tag| tag == name) {
            if std::mem::replace(&mut tag_seen[index], true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate tag column {name:?} in FusedSeriesExec output schema"
                )));
            }
            if field.data_type() != &DataType::Utf8 {
                return Err(DataFusionError::Plan(format!(
                    "tag column {name:?} of FusedSeriesExec must be Utf8, got {:?}",
                    field.data_type()
                )));
            }
            // `rate` and the map kernels drop `__name__`, so a tag can be absent. A nullable
            // column says so with a NULL; a non-nullable one -- which is what the PromQL plan
            // declares -- says so with the empty label value Prometheus reads for a label the
            // series does not carry. Both encode the same thing, so either is accepted.
            classification.push(OutputColumn::Tag(index));
            continue;
        }
        // A PromQL plan names a range-function value column after the call that produced it
        // (`prom_rate(timestamp_range,field_0,timestamp,Int64(300000))`), not after the raw field
        // it reads, so a `Float64` column naming no field still carries exactly one field: the
        // next unclaimed one, in the order both were declared. Columns that do name a field were
        // matched above, so only genuinely unnamed value columns reach this branch.
        if field.data_type() == &DataType::Float64
            && let Some(index) = field_seen.iter().position(|seen| !seen)
        {
            // A grid point inside the trimmed interval can have no value in this field.
            if !field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "field column {name:?} of FusedSeriesExec must be nullable, because a grid \
                     point can be missing"
                )));
            }
            field_seen[index] = true;
            classification.push(OutputColumn::Field(index));
            continue;
        }
        return Err(DataFusionError::Plan(format!(
            "unknown column {name:?} in FusedSeriesExec output schema"
        )));
    }

    if !time_seen {
        return Err(DataFusionError::Plan(format!(
            "missing time column {time_column:?} in FusedSeriesExec output schema"
        )));
    }
    for (index, seen) in field_seen.iter().enumerate() {
        if !seen {
            return Err(DataFusionError::Plan(format!(
                "missing field column {:?} in FusedSeriesExec output schema",
                series_schema.fields[index].name
            )));
        }
    }
    for (index, seen) in tag_seen.iter().enumerate() {
        if !seen {
            return Err(DataFusionError::Plan(format!(
                "missing tag column {:?} in FusedSeriesExec output schema",
                tags[index]
            )));
        }
    }
    Ok(classification)
}

/// Converts one evaluated series into exactly one output batch.
///
/// The time column is the carried timestamps of the series, so a sparse axis emits only the rows it
/// holds and skips the holes; missing samples are nulls and ordinary NaN stays a value.
fn series_to_record_batch(
    classification: &[OutputColumn],
    tags: &[String],
    output_schema: &SchemaRef,
    series: &Series,
) -> Result<RecordBatch> {
    let rows = series.time.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(classification.len());
    for (position, column) in classification.iter().enumerate() {
        match column {
            OutputColumn::Time => columns.push(Arc::new(TimestampMillisecondArray::new(
                time_column(&series.time),
                None,
            ))),
            OutputColumn::Field(index) => {
                let samples = series.fields.get(*index).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "evaluated series has no field at index {index}"
                    ))
                })?;
                if samples.len() != rows {
                    return Err(DataFusionError::Execution(format!(
                        "evaluated series field {index} has {} rows, time axis has {rows}",
                        samples.len()
                    )));
                }
                columns.push(field_column(samples)?);
            }
            OutputColumn::Tag(index) => {
                // `__name__` resolves to the metric name, every other tag through the label set.
                let name = tags.get(*index).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "output column {position} declares unavailable tag {index}"
                    ))
                })?;
                let value = series.metric.get(name);
                columns.push(Arc::new(StringArray::from(vec![value; rows])));
            }
        }
    }
    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

/// The timestamps of the carried rows of a time axis, in compact-row order.
///
/// A single-run grid axis lends its own buffer without copying; an axis of several runs, and an
/// explicit axis, build one buffer of the carried rows, so holes in the grid contribute no row.
fn time_column(time: &TimeAxis) -> ScalarBuffer<i64> {
    match time.as_grid_range() {
        Some(range) => range.timestamps(),
        None => ScalarBuffer::from(time.timestamps().into_owned()),
    }
}

/// Converts one evaluated series field into one `Float64` column.
///
/// Dense float samples are reused as they are: the array already carries exactly the right nulls
/// and keeps NaN a value, so copying it through `Samples::iter()` would only cost an allocation
/// and a re-encode. Chunked samples go through the iterator, and histograms have no float column.
fn field_column(samples: &Samples) -> Result<ArrayRef> {
    match samples.dense() {
        Some(DenseSamplesRef::Float(values)) => Ok(Arc::new(values.clone())),
        Some(DenseSamplesRef::Histogram(_)) | Some(DenseSamplesRef::Mixed(_)) => {
            Err(DataFusionError::NotImplemented(
                "FusedSeriesExec cannot output histogram samples".into(),
            ))
        }
        None => {
            let values = samples
                .iter()
                .map(|sample| match sample {
                    SampleRef::Float(value) => Ok(Some(value)),
                    SampleRef::Missing => Ok(None),
                    SampleRef::Histogram(_) => Err(DataFusionError::NotImplemented(
                        "FusedSeriesExec cannot output histogram samples".into(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Float64Array::from(values)))
        }
    }
}

/// The PromQL name of a range function, for `EXPLAIN`.
fn describe_function(function: RangeFunction) -> String {
    match function {
        RangeFunction::Rate => "rate".to_string(),
        RangeFunction::Increase => "increase".to_string(),
        RangeFunction::Delta => "delta".to_string(),
        RangeFunction::Irate => "irate".to_string(),
        RangeFunction::Idelta => "idelta".to_string(),
        RangeFunction::AvgOverTime => "avg_over_time".to_string(),
        RangeFunction::MinOverTime => "min_over_time".to_string(),
        RangeFunction::MaxOverTime => "max_over_time".to_string(),
        RangeFunction::SumOverTime => "sum_over_time".to_string(),
        RangeFunction::CountOverTime => "count_over_time".to_string(),
        RangeFunction::LastOverTime => "last_over_time".to_string(),
        RangeFunction::PresentOverTime => "present_over_time".to_string(),
        RangeFunction::StddevOverTime => "stddev_over_time".to_string(),
        RangeFunction::StdvarOverTime => "stdvar_over_time".to_string(),
        RangeFunction::QuantileOverTime(quantile) => {
            format!("quantile_over_time({quantile})")
        }
        RangeFunction::Resets => "resets".to_string(),
        RangeFunction::Changes => "changes".to_string(),
        RangeFunction::Deriv => "deriv".to_string(),
        RangeFunction::PredictLinear(duration) => format!("predict_linear({duration})"),
        RangeFunction::DoubleExponentialSmoothing { smoothing, trend } => {
            format!("double_exponential_smoothing({smoothing}, {trend})")
        }
        RangeFunction::FirstOverTime => "first_over_time".to_string(),
        RangeFunction::MadOverTime => "mad_over_time".to_string(),
        RangeFunction::TsOfFirstOverTime => "ts_of_first_over_time".to_string(),
        RangeFunction::TsOfLastOverTime => "ts_of_last_over_time".to_string(),
        RangeFunction::TsOfMinOverTime => "ts_of_min_over_time".to_string(),
        RangeFunction::TsOfMaxOverTime => "ts_of_max_over_time".to_string(),
    }
}

/// The PromQL name of a float map kernel, for `EXPLAIN`.
fn describe_map(op: MathOp) -> String {
    match op {
        MathOp::Abs => "abs".to_string(),
        MathOp::Ceil => "ceil".to_string(),
        MathOp::Floor => "floor".to_string(),
        MathOp::Sqrt => "sqrt".to_string(),
        MathOp::Round { nearest } => format!("round({nearest})"),
        MathOp::Exp => "exp".to_string(),
        MathOp::Ln => "ln".to_string(),
        MathOp::Log2 => "log2".to_string(),
        MathOp::Log10 => "log10".to_string(),
        MathOp::Sin => "sin".to_string(),
        MathOp::Cos => "cos".to_string(),
        MathOp::Tan => "tan".to_string(),
        MathOp::Asin => "asin".to_string(),
        MathOp::Acos => "acos".to_string(),
        MathOp::Atan => "atan".to_string(),
        MathOp::Sinh => "sinh".to_string(),
        MathOp::Cosh => "cosh".to_string(),
        MathOp::Tanh => "tanh".to_string(),
        MathOp::Asinh => "asinh".to_string(),
        MathOp::Acosh => "acosh".to_string(),
        MathOp::Atanh => "atanh".to_string(),
        MathOp::Deg => "deg".to_string(),
        MathOp::Rad => "rad".to_string(),
        MathOp::Sgn => "sgn".to_string(),
        MathOp::Clamp { min, max } => format!("clamp({min}, {max})"),
        MathOp::ClampMin { min } => format!("clamp_min({min})"),
        MathOp::ClampMax { max } => format!("clamp_max({max})"),
    }
}

/// Applies every map kernel in order and returns the final series.
///
/// The kernels work on typed [`Series`]; the duplicate-label-set check and the conversion into a
/// record batch both need the result of this stage, so no batch is built in between. A kernel that
/// yields no series at all, which `clamp` does when its maximum is below its minimum, ends the
/// chain: Prometheus emits an empty vector there, so nothing is left to convert.
fn apply_maps(
    evaluator: &RangeEvaluator,
    maps: &[MathOp],
    series: Series,
) -> Result<Option<Series>> {
    let mut series = series;
    for op in maps {
        match map_math(evaluator.output_schema(), &series, *op)? {
            Some(mapped) => series = mapped,
            None => return Ok(None),
        }
    }
    Ok(Some(series))
}

/// Maximum number of raw chunks one `poll_next` call consumes before yielding.
///
/// A child that is always ready can deliver many short-lived series whose evaluation yields no
/// output at all, which would let a single poll monopolize the executor thread;
/// `unpack_raw_series`'s yield only covers consecutive empty record batches, not this case. The
/// budget is per poll, so a poll that returns a batch starts the next one fresh.
const MAX_CHUNKS_PER_POLL: usize = 64;

/// A canonical, collision-free encoding of a final label set.
///
/// [`MetricName`] implements `PartialEq + Eq` but not `Hash`, and it is a dependency-owned type,
/// so the duplicate-label-set state keys on these bytes instead of on the label set itself. Every
/// component carries a length prefix: label names and values are arbitrary UTF-8, so a separator
/// byte could occur inside a component and let two different label sets encode to the same bytes.
/// [`MetricName`] keeps its tags sorted by name, so one label set has exactly one encoding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct LabelsKey(Vec<u8>);

impl LabelsKey {
    /// Encodes the metric name, marked for presence, and then every label, name before value.
    fn new(metric: &MetricName) -> Self {
        let mut bytes = Vec::new();
        match metric.name() {
            // The marker keeps an absent metric name distinct from an empty one.
            Some(name) => {
                bytes.push(1);
                push_component(&mut bytes, name);
            }
            None => bytes.push(0),
        }
        for tag in metric.tags() {
            push_component(&mut bytes, &tag.name);
            push_component(&mut bytes, &tag.value);
        }
        Self(bytes)
    }

    /// Number of bytes this key occupies, for the memory reservation.
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// Appends one length-prefixed UTF-8 component.
fn push_component(bytes: &mut Vec<u8>, component: &str) {
    bytes.extend_from_slice(&component.len().to_le_bytes());
    bytes.extend_from_slice(component.as_bytes());
}

/// Renders a label set the way Prometheus prints one, for the duplicate-label-set error.
fn render_labels(metric: &MetricName) -> String {
    let mut parts = Vec::new();
    if let Some(name) = metric.name() {
        parts.push(format!("__name__={name:?}"));
    }
    parts.extend(
        metric
            .tags()
            .iter()
            .map(|tag| format!("{}={:?}", tag.name, tag.value)),
    );
    format!("{{{}}}", parts.join(", "))
}

/// The grid indices of the trimmed interval that hold a value, as merged runs.
///
/// Each run is `(start, end_exclusive)` over absolute grid indices. A row whose every field is
/// missing is not occupied: Prometheus has no point there, so it cannot collide with another
/// series. A sparse axis occupies only the positions it carries, so the holes between its runs
/// are never reported as occupied.
pub(crate) fn occupied_runs(series: &Series) -> Result<Vec<(usize, usize)>> {
    let rows = series.time.len();
    // A series with one dense, null-free float field holds a value on every row, so the whole
    // range is one occupied run and no row has to be inspected. This is the shape the fused
    // evaluator produces for a range function over ordinary float samples. It only holds when the
    // axis is one contiguous run: with several runs the carried rows are not one absolute range,
    // and the whole-range shortcut would report the holes as occupied.
    if series.time.is_contiguous()
        && series.fields.len() == 1
        && let Some(DenseSamplesRef::Float(values)) =
            series.fields.iter().next().and_then(Samples::dense)
        && values.len() == rows
        && values.null_count() == 0
    {
        return Ok(series
            .time
            .first_position()
            .map(|start| vec![(start, start + rows)])
            .unwrap_or_default());
    }
    let mut runs: Vec<(usize, usize)> = Vec::new();
    for (index, row) in series.time.positions() {
        if !series.fields.iter().any(|samples| has_value(samples, row)) {
            continue;
        }
        match runs.last_mut() {
            Some((_, end)) if *end == index => *end += 1,
            _ => runs.push((index, index + 1)),
        }
    }
    Ok(runs)
}

/// Whether one field holds a value, rather than a missing position, at `index`.
fn has_value(samples: &Samples, index: usize) -> bool {
    match samples.dense() {
        Some(DenseSamplesRef::Float(values)) => values.is_valid(index),
        Some(DenseSamplesRef::Histogram(values)) => values.indices().is_valid(index),
        Some(DenseSamplesRef::Mixed(values)) => {
            values.floats().is_valid(index) || values.histograms().indices().is_valid(index)
        }
        None => !matches!(samples.sample(index), SampleRef::Missing),
    }
}

/// Whether two sorted, disjoint run lists share at least one grid index.
fn runs_overlap(stored: &[(usize, usize)], new: &[(usize, usize)]) -> bool {
    let (mut left, mut right) = (0, 0);
    while left < stored.len() && right < new.len() {
        let (stored_start, stored_end) = stored[left];
        let (new_start, new_end) = new[right];
        if new_start < stored_end && stored_start < new_end {
            return true;
        }
        if stored_end <= new_start {
            left += 1;
        } else {
            right += 1;
        }
    }
    false
}

/// Adds sorted, disjoint `new` runs to the sorted, disjoint `stored` runs, coalescing neighbours.
fn merge_runs(stored: &mut Vec<(usize, usize)>, new: &[(usize, usize)]) {
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(stored.len() + new.len());
    let (mut left, mut right) = (0, 0);
    while left < stored.len() || right < new.len() {
        let run = match (stored.get(left), new.get(right)) {
            (Some(stored), Some(new)) if stored.0 <= new.0 => {
                left += 1;
                *stored
            }
            (Some(_), Some(new)) => {
                right += 1;
                *new
            }
            (Some(stored), None) => {
                left += 1;
                *stored
            }
            (None, Some(new)) => {
                right += 1;
                *new
            }
            (None, None) => break,
        };
        match merged.last_mut() {
            Some(last) if run.0 <= last.1 => last.1 = last.1.max(run.1),
            _ => merged.push(run),
        }
    }
    *stored = merged;
}

/// Turns raw series chunks into one output batch per evaluated chunk.
/// The grid positions each final label set already occupies, as merged, sorted runs.
///
/// Shared by every partition's stream, because a label-set collision is a property of the whole
/// output vector and not of one partition. `PerSeries` distribution keeps each series whole inside
/// one partition, but two series that carry the same labels after the range function and its maps
/// drop a label can still land in different partitions. Checking only within a partition would
/// miss exactly those collisions.
type SharedLabelSets = Arc<Mutex<HashMap<LabelsKey, Vec<(usize, usize)>>>>;

struct FusedSeriesStream {
    input: SendableRawSeriesStream,
    /// `None` once the stream is finished; `finish(self)` consumes the evaluator.
    evaluator: Option<RangeEvaluator>,
    maps: Vec<MathOp>,
    tags: Arc<[String]>,
    output_schema: SchemaRef,
    classification: Arc<[OutputColumn]>,
    /// Shared with the other partitions' streams; this stream holds one reference.
    labels: SharedLabelSets,
    /// Accounts for what this stream adds to `labels`; freed when the stream ends.
    reservation: MemoryReservation,
    finished: bool,
}

impl FusedSeriesStream {
    /// Marks the stream terminal and gives everything it still retained back to the pool.
    fn abort(&mut self) {
        self.finished = true;
        self.evaluator = None;
        // The label-set map is shared, so one partition finishing early must not clear entries
        // another partition is still checking against. Its own reservation is what this stream
        // owns, and that is what it gives back.
        self.reservation.free();
    }

    /// Claims the occupied grid positions of one evaluated chunk for its final label set.
    ///
    /// A collision is an error only when both series hold a sample at the same evaluation
    /// timestamp, which is Prometheus' range-query rule. Series with disjoint points are legal:
    /// Prometheus merges them into one output series, while this node emits them as separate
    /// batches and defers that merge to matrix assembly, so every sample survives.
    fn claim_label_set(&mut self, series: &Series) -> Result<()> {
        let runs = occupied_runs(series)?;
        let key = LabelsKey::new(&series.metric);
        let mut seen = self.labels.lock().unwrap_or_else(|e| e.into_inner());
        match seen.entry(key) {
            Entry::Occupied(mut entry) => {
                let stored = entry.get_mut();
                if runs_overlap(stored, &runs) {
                    return Err(DataFusionError::Execution(format!(
                        "vector cannot contain metrics with the same labelset: {}",
                        render_labels(&series.metric)
                    )));
                }
                merge_runs(stored, &runs);
            }
            Entry::Vacant(entry) => {
                // Only the label-set state is this node's own cross-poll state that grows with
                // the output, so it is the only thing reserved here.
                let bytes = entry.key().len() + runs.len() * 2 * size_of::<usize>();
                self.reservation.try_grow(bytes)?;
                entry.insert(runs);
            }
        }
        Ok(())
    }

    /// Registers the label set of one evaluated chunk and converts it into one output batch.
    fn emit(&mut self, series: &Series) -> Result<RecordBatch> {
        self.claim_label_set(series)?;
        series_to_record_batch(
            &self.classification,
            &self.tags,
            &self.output_schema,
            series,
        )
    }
}

impl RecordBatchStream for FusedSeriesStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for FusedSeriesStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        // Every call starts a fresh budget, so returning a batch below resets it.
        let mut consumed = 0usize;
        loop {
            if consumed == MAX_CHUNKS_PER_POLL {
                // The whole budget went into chunks that produced no batch; hand the executor
                // thread back instead of draining an always-ready child here.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let polled = this.input.as_mut().poll_next(cx);
            let chunk = match polled {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    let Some(evaluator) = this.evaluator.take() else {
                        this.finished = true;
                        return Poll::Ready(None);
                    };
                    return match evaluator.finish() {
                        Ok(()) => {
                            this.finished = true;
                            Poll::Ready(None)
                        }
                        Err(error) => {
                            this.abort();
                            Poll::Ready(Some(Err(error)))
                        }
                    };
                }
                Poll::Ready(Some(Err(error))) => {
                    this.abort();
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
                }
                Poll::Ready(Some(Ok(chunk))) => chunk,
            };
            consumed += 1;
            let evaluated = {
                let maps = &this.maps;
                let Some(evaluator) = this.evaluator.as_mut() else {
                    this.finished = true;
                    return Poll::Ready(None);
                };
                evaluator
                    .push(&chunk)
                    .and_then(|evaluated| match evaluated {
                        // One push hands out one chunk of a series, and a series may take several: a
                        // chunk carries only the positions that hold a value, so converting it on its
                        // own is what makes the output sparse, and nothing is accumulated across polls.
                        Some(evaluated) => apply_maps(evaluator, maps, evaluated.series),
                        None => Ok(None),
                    })
            };
            let series = match evaluated {
                Ok(series) => series,
                Err(error) => {
                    this.abort();
                    return Poll::Ready(Some(Err(error)));
                }
            };
            // A series closes on a chunk of its own rows, or on an empty chunk after them when
            // nothing is left. The latter carries no row, so there is nothing to output for it.
            let Some(series) = series.filter(|series| !series.time.is_empty()) else {
                continue;
            };
            return match this.emit(&series) {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                Err(error) => {
                    this.abort();
                    Poll::Ready(Some(Err(error)))
                }
            };
        }
    }
}
