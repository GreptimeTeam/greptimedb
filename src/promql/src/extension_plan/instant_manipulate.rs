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

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_query::prelude::{greptime_native_histogram, greptime_value};
use datafusion::arrow::array::{Array, TimestampMillisecondArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{
    EmptyRelation, Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion_expr::col;
use datatypes::arrow::compute;
use futures::{Stream, StreamExt, ready};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DeserializeSnafu, Result};
use crate::extension_plan::series_divide::SeriesDivide;
use crate::extension_plan::{
    METRIC_NUM_SERIES, Millisecond, is_prometheus_stale_sample, native_per_nanosecond,
    native_timestamp_values, prometheus_stale_sample_column, resolve_column_name,
    serialize_column_index, timestamp_unit,
};
use crate::metrics::PROMQL_SERIES_COUNT;

const MAX_INSTANT_MANIPULATE_OUTPUT_POINTS: usize = 1_000_000;

fn mixed_sample_fields(field: Option<&str>) -> [Option<&str>; 2] {
    let companion = match field {
        Some(field) if field == greptime_value() => Some(greptime_native_histogram()),
        Some(field) if field == greptime_native_histogram() => Some(greptime_value()),
        _ => None,
    };
    [field, companion]
}

/// Manipulate the input record batch to make it suitable for Instant Operator.
///
/// This plan will try to align the input time series, for every timestamp between
/// `start` and `end` with step `interval`. Find in the `lookback` range if data
/// is missing at the given timestamp.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct InstantManipulate {
    start: Millisecond,
    end: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,
    time_index_column: String,
    // Planner-provided tag-column hint for execution fast paths.
    tag_columns: Vec<String>,
    /// Primary sample column used to derive the columns checked for staleness.
    field_column: Option<String>,
    input: LogicalPlan,
    output_schema: DFSchemaRef,
    unfix: Option<UnfixIndices>,
}

impl PartialOrd for InstantManipulate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            self.start,
            self.end,
            self.lookback_delta,
            self.interval,
            &self.time_index_column,
            &self.tag_columns,
            &self.field_column,
            &self.input,
        )
            .partial_cmp(&(
                other.start,
                other.end,
                other.lookback_delta,
                other.interval,
                &other.time_index_column,
                &other.tag_columns,
                &other.field_column,
                &other.input,
            ))
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
struct UnfixIndices {
    pub time_index_idx: u64,
    pub field_index_idx: u64,
}

impl UserDefinedLogicalNodeCore for InstantManipulate {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        if self.unfix.is_some() {
            return vec![];
        }

        let mut exprs = vec![col(&self.time_index_column)];
        exprs.extend(self.staleness_field_columns().map(col));
        exprs
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        if self.unfix.is_some() {
            return None;
        }

        let input_schema = self.input.schema();
        if output_columns.is_empty() {
            let indices = (0..input_schema.fields().len()).collect::<Vec<_>>();
            return Some(vec![indices]);
        }

        let mut required = output_columns.to_vec();
        required.push(input_schema.index_of_column_by_name(None, &self.time_index_column)?);
        for field in self.staleness_field_columns() {
            required.push(input_schema.index_of_column_by_name(None, field)?);
        }

        required.sort_unstable();
        required.dedup();
        Some(vec![required])
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromInstantManipulate: range=[{}..{}], lookback=[{}], interval=[{}], time index=[{}]",
            self.start, self.end, self.lookback_delta, self.interval, self.time_index_column
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "InstantManipulate should have exact one input".to_string(),
            ));
        }

        let input: LogicalPlan = inputs.into_iter().next().unwrap();
        let input_schema = input.schema();

        if let Some(unfix) = &self.unfix {
            // transform indices to names
            let time_index_column = resolve_column_name(
                unfix.time_index_idx,
                input_schema,
                "InstantManipulate",
                "time index",
            )?;

            let field_column = if unfix.field_index_idx == u64::MAX {
                None
            } else {
                Some(resolve_column_name(
                    unfix.field_index_idx,
                    input_schema,
                    "InstantManipulate",
                    "field",
                )?)
            };

            Ok(Self {
                start: self.start,
                end: self.end,
                lookback_delta: self.lookback_delta,
                interval: self.interval,
                output_schema: Self::calculate_output_schema(&input, &time_index_column)?,
                time_index_column,
                tag_columns: Self::resolve_tag_columns(&input, &self.tag_columns),
                field_column,
                input,
                unfix: None,
            })
        } else {
            Ok(Self {
                start: self.start,
                end: self.end,
                lookback_delta: self.lookback_delta,
                interval: self.interval,
                time_index_column: self.time_index_column.clone(),
                tag_columns: Self::resolve_tag_columns(&input, &self.tag_columns),
                field_column: self.field_column.clone(),
                output_schema: Self::calculate_output_schema(&input, &self.time_index_column)?,
                input,
                unfix: None,
            })
        }
    }
}

impl InstantManipulate {
    fn calculate_output_schema(
        input: &LogicalPlan,
        time_index_column: &str,
    ) -> DataFusionResult<DFSchemaRef> {
        let input_schema = input.schema();
        let time_index = input_schema
            .index_of_column_by_name(None, time_index_column)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "InstantManipulate time index {time_index_column} not found"
                ))
            })?;
        let mut fields = (0..input_schema.fields().len())
            .map(|index| {
                let (qualifier, field) = input_schema.qualified_field(index);
                (qualifier.cloned(), field.clone())
            })
            .collect::<Vec<_>>();
        let (qualifier, field) = input_schema.qualified_field(time_index);
        fields[time_index] = (
            qualifier.cloned(),
            Arc::new(field.as_ref().clone().with_data_type(DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Millisecond,
                None,
            ))),
        );
        Ok(Arc::new(DFSchema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        )?))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        lookback_delta: Millisecond,
        interval: Millisecond,
        time_index_column: String,
        tag_columns: Vec<String>,
        field_column: Option<String>,
        input: LogicalPlan,
    ) -> Self {
        Self {
            start,
            end,
            lookback_delta,
            interval,
            output_schema: Self::calculate_output_schema(&input, &time_index_column)
                .unwrap_or_else(|_| input.schema().clone()),
            time_index_column,
            tag_columns,
            field_column,
            input,
            unfix: None,
        }
    }

    pub const fn name() -> &'static str {
        "InstantManipulate"
    }

    fn staleness_field_columns(&self) -> impl Iterator<Item = &str> {
        let [field, companion] = mixed_sample_fields(self.field_column.as_deref());
        [
            field,
            companion.filter(|companion| {
                self.input
                    .schema()
                    .index_of_column_by_name(None, companion)
                    .is_some()
            }),
        ]
        .into_iter()
        .flatten()
    }

    fn resolve_tag_columns(input: &LogicalPlan, tag_columns: &[String]) -> Vec<String> {
        if !tag_columns.is_empty() {
            return tag_columns.to_vec();
        }

        Self::find_series_divide_tags(input).unwrap_or_default()
    }

    fn find_series_divide_tags(plan: &LogicalPlan) -> Option<Vec<String>> {
        if let LogicalPlan::Extension(Extension { node }) = plan
            && let Some(series_divide) = node.as_any().downcast_ref::<SeriesDivide>()
        {
            return Some(series_divide.tags().to_vec());
        }

        plan.inputs()
            .into_iter()
            .find_map(Self::find_series_divide_tags)
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let reuse_tsid_column = matches!(self.tag_columns.as_slice(), [tag] if tag == "__tsid");

        let mut fields = exec_input.schema().fields().to_vec();
        let time_index = exec_input
            .schema()
            .index_of(&self.time_index_column)
            .expect("time index column not found");
        fields[time_index] = Arc::new(fields[time_index].as_ref().clone().with_data_type(
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
        ));
        let output_schema = Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
            fields,
            exec_input.schema().metadata().clone(),
        ));
        let input_properties = exec_input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            input_properties.partitioning.clone(),
            input_properties.emission_type,
            input_properties.boundedness,
        ));
        Arc::new(InstantManipulateExec {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index_column: self.time_index_column.clone(),
            field_column: self.field_column.clone(),
            reuse_tsid_column,
            input: exec_input,
            output_schema,
            properties,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let time_index_idx = serialize_column_index(self.input.schema(), &self.time_index_column);

        let field_index_idx = self
            .field_column
            .as_ref()
            .map(|name| serialize_column_index(self.input.schema(), name))
            .unwrap_or(u64::MAX);

        pb::InstantManipulate {
            start: self.start,
            end: self.end,
            interval: self.interval,
            lookback_delta: self.lookback_delta,
            time_index_idx,
            field_index_idx,
            ..Default::default()
        }
        .encode_to_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let pb_instant_manipulate =
            pb::InstantManipulate::decode(bytes).context(DeserializeSnafu)?;
        let empty_schema = Arc::new(DFSchema::empty());
        let placeholder_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: empty_schema.clone(),
        });

        let unfix = UnfixIndices {
            time_index_idx: pb_instant_manipulate.time_index_idx,
            field_index_idx: pb_instant_manipulate.field_index_idx,
        };

        Ok(Self {
            start: pb_instant_manipulate.start,
            end: pb_instant_manipulate.end,
            lookback_delta: pb_instant_manipulate.lookback_delta,
            interval: pb_instant_manipulate.interval,
            time_index_column: String::new(),
            tag_columns: Vec::new(),
            field_column: None,
            output_schema: empty_schema,
            input: placeholder_plan,
            unfix: Some(unfix),
        })
    }
}

#[derive(Debug)]
pub struct InstantManipulateExec {
    start: Millisecond,
    end: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,
    time_index_column: String,
    field_column: Option<String>,
    reuse_tsid_column: bool,

    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metric: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for InstantManipulateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input.required_input_distribution()
    }

    // Prevent reordering of input
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(!children.is_empty());
        let input = children[0].clone();
        let input_properties = input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(self.output_schema.clone()),
            input_properties.partitioning.clone(),
            input_properties.emission_type,
            input_properties.boundedness,
        ));
        Ok(Arc::new(Self {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index_column: self.time_index_column.clone(),
            field_column: self.field_column.clone(),
            reuse_tsid_column: self.reuse_tsid_column,
            input,
            output_schema: self.output_schema.clone(),
            properties,
            metric: self.metric.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let baseline_metric = BaselineMetrics::new(&self.metric, partition);
        let num_series = Count::new();
        MetricBuilder::new(&self.metric)
            .with_partition(partition)
            .build(MetricValue::Count {
                name: METRIC_NUM_SERIES.into(),
                count: num_series.clone(),
            });

        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let time_index = schema
            .column_with_name(&self.time_index_column)
            .expect("time index column not found")
            .0;
        let time_unit = timestamp_unit(schema.field(time_index).data_type())?;
        let field_indices = mixed_sample_fields(self.field_column.as_deref()).map(|field| {
            field.and_then(|field| schema.column_with_name(field).map(|(index, _)| index))
        });
        let tsid_index = schema
            .column_with_name("__tsid")
            .filter(|(_, field)| field.data_type() == &DataType::UInt64)
            .map(|(index, _)| index);
        Ok(Box::pin(InstantManipulateStream {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index,
            time_unit,
            field_indices,
            tsid_index,
            reuse_tsid_column: self.reuse_tsid_column && tsid_index.is_some(),
            schema: self.output_schema.clone(),
            input,
            metric: baseline_metric,
            num_series,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Statistics> {
        let input_stats = self.input.partition_statistics(partition)?;

        let estimated_row_num = (self.end - self.start) as f64 / self.interval as f64;
        let estimated_total_bytes = input_stats
            .total_byte_size
            .get_value()
            .zip(input_stats.num_rows.get_value())
            .map(|(size, rows)| {
                Precision::Inexact(((*size as f64 / *rows as f64) * estimated_row_num).floor() as _)
            })
            .unwrap_or(Precision::Absent);

        Ok(Statistics {
            num_rows: Precision::Inexact(estimated_row_num.floor() as _),
            total_byte_size: estimated_total_bytes,
            // TODO(ruihang): support this column statistics
            column_statistics: Statistics::unknown_column(&self.schema()),
        })
    }

    fn name(&self) -> &str {
        "InstantManipulateExec"
    }
}

impl DisplayAs for InstantManipulateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "PromInstantManipulateExec: range=[{}..{}], lookback=[{}], interval=[{}], time index=[{}]",
                    self.start,
                    self.end,
                    self.lookback_delta,
                    self.interval,
                    self.time_index_column
                )
            }
        }
    }
}

pub struct InstantManipulateStream {
    start: Millisecond,
    end: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,
    // Column index of TIME INDEX column's position in schema
    time_index: usize,
    time_unit: datafusion::arrow::datatypes::TimeUnit,
    field_indices: [Option<usize>; 2],
    tsid_index: Option<usize>,
    reuse_tsid_column: bool,

    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
    /// Number of series processed.
    num_series: Count,
}

impl RecordBatchStream for InstantManipulateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for InstantManipulateStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let timer = std::time::Instant::now();
                self.num_series.add(1);
                let result = Ok(batch).and_then(|batch| self.manipulate(batch));
                self.metric.elapsed_compute().add_elapsed(timer);
                Poll::Ready(Some(result))
            }
            None => {
                PROMQL_SERIES_COUNT.observe(self.num_series.value() as f64);
                Poll::Ready(None)
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
        };
        self.metric.record_poll(poll)
    }
}

impl InstantManipulateStream {
    /// Manipulates one complete series sorted by timestamp. The planner enforces
    /// this input contract with a sort followed by [`SeriesDivide`].
    ///
    /// Prometheus `v3.9.1`'s `vectorSelectorSingle` uses a start-exclusive
    /// lookback window `(eval_ts - lookback_delta, eval_ts]`; a sample at exactly
    /// `eval_ts - lookback_delta` is too old.
    pub fn manipulate(&self, input: RecordBatch) -> DataFusionResult<RecordBatch> {
        let ts_column = input.column(self.time_index);
        if ts_column.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let scale = native_per_nanosecond(self.time_unit);
        let stale_sample_columns = self.field_indices.map(|index| {
            index.and_then(|index| prometheus_stale_sample_column(input.column(index).as_ref()))
        });
        let is_stale = |row| {
            stale_sample_columns
                .iter()
                .flatten()
                .any(|column| is_prometheus_stale_sample(*column, row))
        };
        let timestamps = native_timestamp_values(ts_column.as_ref())?;
        let len = timestamps.len();
        let to_nanoseconds = |timestamp: i64| (timestamp as i128) * scale;
        let first_ns = to_nanoseconds(timestamps[0]);
        let last_ns = to_nanoseconds(timestamps[len - 1]);
        // An exact sample remains useful with zero lookback. Otherwise the lower
        // boundary is exclusive, so subtract one nanosecond from its final window.
        let last_useful = if self.lookback_delta == 0 {
            last_ns
        } else {
            last_ns + (self.lookback_delta as i128) * 1_000_000 - 1
        };
        let first_ms = (first_ns + 999_999).div_euclid(1_000_000);
        let last_ms = last_useful.div_euclid(1_000_000);
        let query_start = self.start as i128;
        let query_end = self.end as i128;
        let interval = self.interval as i128;
        let max_start = first_ms.max(query_start);
        let min_end = last_ms.min(query_end);
        let (aligned_start, aligned_end) = if max_start > min_end {
            (1, 0)
        } else {
            (
                query_start + (max_start - query_start) / interval * interval,
                query_end - (query_end - min_end) / interval * interval,
            )
        };
        let estimated_points = if aligned_end >= aligned_start {
            (aligned_end - aligned_start) / interval + 1
        } else {
            0
        };
        if estimated_points > MAX_INSTANT_MANIPULATE_OUTPUT_POINTS as i128 {
            return Err(DataFusionError::Execution(format!(
                "InstantManipulate output points exceed limit: {estimated_points} > {MAX_INSTANT_MANIPULATE_OUTPUT_POINTS}"
            )));
        }
        let estimated_points = estimated_points as usize;
        let aligned_start = aligned_start as i64;
        let aligned_end = aligned_end as i64;
        let mut take_indices = Vec::with_capacity(estimated_points);
        let mut aligned_ts = Vec::with_capacity(estimated_points);
        let mut cursor = 0;
        for expected_ms in (aligned_start..=aligned_end).step_by(self.interval as usize) {
            let expected = (expected_ms as i128) * 1_000_000;
            let mut exact_candidate = None;
            while cursor < len && to_nanoseconds(timestamps[cursor]) <= expected {
                if to_nanoseconds(timestamps[cursor]) == expected && exact_candidate.is_none() {
                    exact_candidate = Some(cursor);
                }
                cursor += 1;
            }
            let Some(candidate) = exact_candidate.or_else(|| cursor.checked_sub(1)) else {
                continue;
            };
            let candidate_ts = to_nanoseconds(timestamps[candidate]);
            let lower = expected - (self.lookback_delta as i128) * 1_000_000;
            if (candidate_ts == expected || candidate_ts > lower)
                && candidate_ts <= expected
                && !is_stale(candidate)
            {
                take_indices.push(candidate as u64);
                aligned_ts.push(expected_ms);
            }
        }
        self.take_record_batch_optional(input, take_indices, aligned_ts)
    }

    /// Helper function to apply "take" on record batch.
    fn take_record_batch_optional(
        &self,
        record_batch: RecordBatch,
        take_indices: Vec<u64>,
        aligned_ts: Vec<Millisecond>,
    ) -> DataFusionResult<RecordBatch> {
        assert_eq!(take_indices.len(), aligned_ts.len());

        let output_len = aligned_ts.len();
        let mut indices_array = None;
        let mut arrays = Vec::with_capacity(record_batch.num_columns());
        let aligned_ts = Arc::new(TimestampMillisecondArray::from(aligned_ts)) as Arc<dyn Array>;

        for (index, array) in record_batch.columns().iter().enumerate() {
            if index == self.time_index {
                arrays.push(aligned_ts.clone());
                continue;
            }

            if self.reuse_tsid_column && self.tsid_index == Some(index) {
                arrays.push(reuse_constant_column(array, output_len)?);
                continue;
            }

            let indices_array =
                indices_array.get_or_insert_with(|| UInt64Array::from(take_indices.clone()));
            arrays.push(compute::take(array, indices_array, None)?);
        }

        let result = RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(result)
    }
}

fn reuse_constant_column(array: &Arc<dyn Array>, len: usize) -> DataFusionResult<Arc<dyn Array>> {
    if len <= array.len() {
        return Ok(array.slice(0, len));
    }

    if array.is_empty() {
        return Ok(array.slice(0, 0));
    }

    ScalarValue::try_from_array(array.as_ref(), 0)?.to_array_of_size(len)
}

#[cfg(test)]
mod test {
    use common_query::native_histogram::build_histogram_array;
    use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
    use datafusion::arrow::array::{
        Float64Array, TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::extension_plan::test_util::{
        TIME_INDEX_COLUMN, native_histogram, prepare_test_data, prepare_test_data_with_stale_marker,
    };

    async fn do_normalize_test(
        start: Millisecond,
        end: Millisecond,
        lookback_delta: Millisecond,
        interval: Millisecond,
        expected: String,
        contains_stale_marker: bool,
    ) {
        let memory_exec = if contains_stale_marker {
            Arc::new(prepare_test_data_with_stale_marker())
        } else {
            Arc::new(prepare_test_data())
        };
        let normalize_exec = Arc::new(InstantManipulateExec {
            start,
            end,
            lookback_delta,
            interval,
            time_index_column: TIME_INDEX_COLUMN.to_string(),
            field_column: Some("value".to_string()),
            reuse_tsid_column: false,
            output_schema: memory_exec.schema(),
            properties: memory_exec.properties().clone(),
            input: memory_exec,
            metric: ExecutionPlanMetricsSet::new(),
        });
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(normalize_exec, session_context.task_ctx())
            .await
            .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn native_timestamps_select_exact_samples_and_keep_ms_output() {
        for (unit, ticks_per_ms) in [
            (TimeUnit::Microsecond, 1_000_i64),
            (TimeUnit::Nanosecond, 1_000_000_i64),
        ] {
            let lower = 1_000 * ticks_per_ms;
            let upper = 1_001 * ticks_per_ms;
            let stale = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
            for (name, timestamps, values, expected_timestamps, expected_values) in [
                (
                    "exact upper sample",
                    vec![lower + 1, upper],
                    vec![1.0, 2.0],
                    vec![1_001],
                    vec![2.0],
                ),
                (
                    "exclusive lower boundary and future sample",
                    vec![lower, upper + 1],
                    vec![1.0, 2.0],
                    vec![1_000],
                    vec![1.0],
                ),
                (
                    "one native tick above lower boundary",
                    vec![lower + 1, upper + 1],
                    vec![1.0, 2.0],
                    vec![1_001],
                    vec![1.0],
                ),
                (
                    "future stale marker does not suppress",
                    vec![lower + 1, upper + 1],
                    vec![1.0, stale],
                    vec![1_001],
                    vec![1.0],
                ),
                (
                    "latest in-window stale marker suppresses",
                    vec![lower + 1, lower + 2, upper + 1],
                    vec![1.0, stale, 3.0],
                    vec![],
                    vec![],
                ),
            ] {
                let schema = Arc::new(Schema::new(vec![
                    Field::new(TIME_INDEX_COLUMN, DataType::Timestamp(unit, None), false),
                    Field::new("value", DataType::Float64, true),
                ]));
                let time: Arc<dyn Array> = match unit {
                    TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(timestamps)),
                    TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(timestamps)),
                    _ => unreachable!(),
                };
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![time, Arc::new(Float64Array::from(values))],
                )
                .unwrap();
                let logical_input = LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: schema.clone().to_dfschema_ref().unwrap(),
                });
                let plan = InstantManipulate::new(
                    1_000,
                    1_001,
                    1,
                    1,
                    TIME_INDEX_COLUMN.to_string(),
                    Vec::new(),
                    Some("value".to_string()),
                    logical_input.clone(),
                );
                let output_schema = Arc::new(Schema::new(vec![
                    Field::new(
                        TIME_INDEX_COLUMN,
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new("value", DataType::Float64, true),
                ]));
                assert_eq!(plan.schema().as_arrow(), output_schema.as_ref());

                let rebuilt = InstantManipulate::deserialize(&plan.serialize())
                    .unwrap()
                    .with_exprs_and_inputs(vec![], vec![logical_input])
                    .unwrap();
                assert_eq!(rebuilt.schema(), plan.schema());
                assert_eq!(rebuilt.input.schema().as_arrow(), schema.as_ref());

                let input = Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(&[vec![batch]], schema.clone(), None).unwrap(),
                )));
                let exec = rebuilt.to_execution_plan(input);
                assert_eq!(exec.schema(), output_schema);
                assert_eq!(exec.children()[0].schema(), schema);

                let batches =
                    datafusion::physical_plan::collect(exec, SessionContext::default().task_ctx())
                        .await
                        .unwrap();
                assert_eq!(batches.len(), 1, "{unit:?}: {name}");
                let output = &batches[0];
                assert_eq!(output.schema(), output_schema);
                let timestamps = output
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let values = output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(
                    timestamps.values().as_ref(),
                    expected_timestamps.as_slice(),
                    "{unit:?}: {name}"
                );
                assert_eq!(
                    values.values().as_ref(),
                    expected_values.as_slice(),
                    "{unit:?}: {name}"
                );
                assert_eq!(values.null_count(), 0, "{unit:?}: {name}");
            }
        }
    }

    #[test]
    fn pruning_should_keep_time_and_field_columns_for_exec() {
        let df_schema = prepare_test_data().schema().to_dfschema_ref().unwrap();
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        });
        let plan = InstantManipulate::new(
            0,
            0,
            0,
            0,
            TIME_INDEX_COLUMN.to_string(),
            Vec::new(),
            Some("value".to_string()),
            input,
        );

        // Simulate a parent projection requesting only the `path` column.
        let output_columns = [2usize];
        let required = plan.necessary_children_exprs(&output_columns).unwrap();
        let required = &required[0];
        assert_eq!(required.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn rebuild_should_recover_tag_columns_from_series_divide_input() {
        let df_schema = prepare_test_data().schema().to_dfschema_ref().unwrap();
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        });
        let series_divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                vec!["__tsid".to_string()],
                TIME_INDEX_COLUMN.to_string(),
                input,
            )),
        });
        let bytes = InstantManipulate::new(
            0,
            0,
            0,
            0,
            TIME_INDEX_COLUMN.to_string(),
            vec!["__tsid".to_string()],
            Some("value".to_string()),
            series_divide.clone(),
        )
        .serialize();
        let plan = InstantManipulate::deserialize(&bytes)
            .unwrap()
            .with_exprs_and_inputs(vec![], vec![series_divide])
            .unwrap();

        assert_eq!(plan.tag_columns, vec!["__tsid".to_string()]);
    }

    #[test]
    fn rebuild_should_recover_tag_columns_from_series_normalize_input() {
        let df_schema = prepare_test_data().schema().to_dfschema_ref().unwrap();
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        });
        let series_divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                vec!["__tsid".to_string()],
                TIME_INDEX_COLUMN.to_string(),
                input,
            )),
        });
        let series_normalize = LogicalPlan::Extension(Extension {
            node: Arc::new(crate::extension_plan::SeriesNormalize::new(
                0,
                TIME_INDEX_COLUMN,
                false,
                vec!["__tsid".to_string()],
                series_divide,
            )),
        });
        let bytes = InstantManipulate::new(
            0,
            0,
            0,
            0,
            TIME_INDEX_COLUMN.to_string(),
            vec!["__tsid".to_string()],
            Some("value".to_string()),
            series_normalize.clone(),
        )
        .serialize();
        let plan = InstantManipulate::deserialize(&bytes)
            .unwrap()
            .with_exprs_and_inputs(vec![], vec![series_normalize])
            .unwrap();

        assert_eq!(plan.tag_columns, vec!["__tsid".to_string()]);
    }

    #[test]
    fn to_execution_plan_enables_tsid_fast_path() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ]));
        let exec_input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[], schema, None).unwrap(),
        )));

        let exec = InstantManipulate::new(
            0,
            0,
            0,
            0,
            TIME_INDEX_COLUMN.to_string(),
            vec!["__tsid".to_string()],
            Some("value".to_string()),
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(datafusion::common::DFSchema::empty()),
            }),
        )
        .to_execution_plan(exec_input);

        assert!(format!("{exec:?}").contains("reuse_tsid_column: true"));
    }

    #[tokio::test]
    async fn tsid_fast_path_reuses_tsid_column_when_output_grows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("__tsid", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0, 1_000])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    "foo", "foo",
                ])),
                Arc::new(UInt64Array::from(vec![42, 42])),
            ],
        )
        .unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap(),
        )));
        let normalize_exec = Arc::new(InstantManipulateExec {
            start: 0,
            end: 1_500,
            lookback_delta: 1_000,
            interval: 500,
            time_index_column: TIME_INDEX_COLUMN.to_string(),
            field_column: Some("value".to_string()),
            reuse_tsid_column: true,
            output_schema: input.schema(),
            properties: input.properties().clone(),
            input,
            metric: ExecutionPlanMetricsSet::new(),
        });
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(normalize_exec, session_context.task_ctx())
            .await
            .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        assert_eq!(
            result_literal,
            "+-------------------------+-------+------+--------+\
            \n| timestamp               | value | host | __tsid |\
            \n+-------------------------+-------+------+--------+\
            \n| 1970-01-01T00:00:00     | 1.0   | foo  | 42     |\
            \n| 1970-01-01T00:00:00.500 | 1.0   | foo  | 42     |\
            \n| 1970-01-01T00:00:01     | 2.0   | foo  | 42     |\
            \n| 1970-01-01T00:00:01.500 | 2.0   | foo  | 42     |\
            \n+-------------------------+-------+------+--------+"
        );
    }

    #[tokio::test]
    async fn tsid_fast_path_still_takes_additional_field_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("value_2", DataType::Float64, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("__tsid", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0, 1_000])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    "foo", "foo",
                ])),
                Arc::new(UInt64Array::from(vec![42, 42])),
            ],
        )
        .unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap(),
        )));
        let normalize_exec = Arc::new(InstantManipulateExec {
            start: 0,
            end: 1_500,
            lookback_delta: 1_000,
            interval: 500,
            time_index_column: TIME_INDEX_COLUMN.to_string(),
            field_column: Some("value".to_string()),
            reuse_tsid_column: true,
            output_schema: input.schema(),
            properties: input.properties().clone(),
            input,
            metric: ExecutionPlanMetricsSet::new(),
        });
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(normalize_exec, session_context.task_ctx())
            .await
            .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        assert_eq!(
            result_literal,
            "+-------------------------+-------+---------+------+--------+\
            \n| timestamp               | value | value_2 | host | __tsid |\
            \n+-------------------------+-------+---------+------+--------+\
            \n| 1970-01-01T00:00:00     | 1.0   | 10.0    | foo  | 42     |\
            \n| 1970-01-01T00:00:00.500 | 1.0   | 10.0    | foo  | 42     |\
            \n| 1970-01-01T00:00:01     | 2.0   | 20.0    | foo  | 42     |\
            \n| 1970-01-01T00:00:01.500 | 2.0   | 20.0    | foo  | 42     |\
            \n+-------------------------+-------+---------+------+--------+"
        );
    }

    #[tokio::test]
    async fn manipulate_should_reject_too_many_output_points() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap(),
        )));
        let too_many_points = MAX_INSTANT_MANIPULATE_OUTPUT_POINTS as Millisecond + 1;
        let normalize_exec = Arc::new(InstantManipulateExec {
            start: 0,
            end: too_many_points,
            lookback_delta: too_many_points + 1,
            interval: 1,
            time_index_column: TIME_INDEX_COLUMN.to_string(),
            field_column: Some("value".to_string()),
            reuse_tsid_column: false,
            output_schema: input.schema(),
            properties: input.properties().clone(),
            input,
            metric: ExecutionPlanMetricsSet::new(),
        });
        let session_context = SessionContext::default();
        let err = datafusion::physical_plan::collect(normalize_exec, session_context.task_ctx())
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("InstantManipulate output points exceed limit")
        );
    }

    #[tokio::test]
    async fn lookback_10s_interval_30s() {
        let expected = String::from(
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:00:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:05:00 | 1.0   | foo  |\
            \n+---------------------+-------+------+",
        );
        do_normalize_test(0, 310_000, 10_000, 30_000, expected, false).await;
    }

    #[tokio::test]
    async fn lookback_10s_interval_10s() {
        let expected = String::from(
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:00:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:05:00 | 1.0   | foo  |\
            \n+---------------------+-------+------+",
        );
        do_normalize_test(0, 300_000, 10_000, 10_000, expected, false).await;
    }

    #[tokio::test]
    async fn lookback_30s_interval_30s() {
        let expected = String::from(
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:00:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:05:00 | 1.0   | foo  |\
            \n+---------------------+-------+------+",
        );
        do_normalize_test(0, 300_000, 30_000, 30_000, expected, false).await;
    }

    #[tokio::test]
    async fn lookback_30s_interval_10s() {
        let expected = String::from(
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:00:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:50 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:50 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:50 | 1.0   | foo  |\
            \n| 1970-01-01T00:05:00 | 1.0   | foo  |\
            \n+---------------------+-------+------+",
        );
        do_normalize_test(0, 300_000, 30_000, 10_000, expected, false).await;
    }

    #[tokio::test]
    async fn lookback_60s_interval_10s() {
        let expected = String::from(
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:00:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:50 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:50 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:50 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:50 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:50 | 1.0   | foo  |\
            \n| 1970-01-01T00:05:00 | 1.0   | foo  |\
            \n+---------------------+-------+------+",
        );
        do_normalize_test(0, 300_000, 60_000, 10_000, expected, false).await;
    }

    #[tokio::test]
    async fn lookback_60s_interval_30s() {
        let expected = String::from(
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:00:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:05:00 | 1.0   | foo  |\
            \n+---------------------+-------+------+",
        );
        do_normalize_test(0, 300_000, 60_000, 30_000, expected, false).await;
    }

    #[tokio::test]
    async fn small_range_lookback_0s_interval_1s() {
        let expected = String::from(
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:04:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:01 | 1.0   | foo  |\
            \n+---------------------+-------+------+",
        );
        do_normalize_test(230_000, 245_000, 0, 1_000, expected, false).await;
    }

    #[tokio::test]
    async fn small_range_lookback_10s_interval_10s() {
        let expected = String::from(
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:00:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:30 | 1.0   | foo  |\
            \n+---------------------+-------+------+",
        );
        do_normalize_test(0, 30_000, 10_000, 10_000, expected, false).await;
    }

    #[tokio::test]
    async fn large_range_lookback_30s_interval_60s() {
        let expected = String::from(
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:00:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:05:00 | 1.0   | foo  |\
            \n+---------------------+-------+------+",
        );
        do_normalize_test(-900_000, 900_000, 30_000, 60_000, expected, false).await;
    }

    #[tokio::test]
    async fn small_range_lookback_30s_interval_30s() {
        let expected = String::from(
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:03:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:04:50 | 1.0   | foo  |\
            \n| 1970-01-01T00:05:00 | 1.0   | foo  |\
            \n+---------------------+-------+------+",
        );
        do_normalize_test(190_000, 300_000, 30_000, 10_000, expected, false).await;
    }

    #[tokio::test]
    async fn lookback_10s_interval_10s_with_stale_marker() {
        let expected = String::from(
            "+---------------------+-------+\
            \n| timestamp           | value |\
            \n+---------------------+-------+\
            \n| 1970-01-01T00:00:00 | 0.0   |\
            \n| 1970-01-01T00:01:00 | 6.0   |\
            \n| 1970-01-01T00:02:00 | 12.0  |\
            \n+---------------------+-------+",
        );
        do_normalize_test(0, 300_000, 10_000, 10_000, expected, true).await;
    }

    #[tokio::test]
    async fn lookback_10s_interval_10s_with_stale_marker_unaligned() {
        let expected = String::from(
            "+-------------------------+-------+\
            \n| timestamp               | value |\
            \n+-------------------------+-------+\
            \n| 1970-01-01T00:00:00.001 | 0.0   |\
            \n| 1970-01-01T00:01:00.001 | 6.0   |\
            \n| 1970-01-01T00:02:00.001 | 12.0  |\
            \n+-------------------------+-------+",
        );
        do_normalize_test(1, 300_001, 10_000, 10_000, expected, true).await;
    }

    #[tokio::test]
    async fn ultra_large_range() {
        let expected = String::from(
            "+-------------------------+-------+\
            \n| timestamp               | value |\
            \n+-------------------------+-------+\
            \n| 1970-01-01T00:00:00.001 | 0.0   |\
            \n| 1970-01-01T00:01:00.001 | 6.0   |\
            \n| 1970-01-01T00:02:00.001 | 12.0  |\
            \n+-------------------------+-------+",
        );
        do_normalize_test(
            -900_000_000_000_000 + 1,
            900_000_000_000_000,
            10_000,
            10_000,
            expected,
            true,
        )
        .await;
    }

    #[test]
    fn exact_ties_select_first_and_lookback_uses_latest() {
        for (values, expected_timestamp) in [
            (vec![42.0, f64::from_bits(PROMETHEUS_STALE_NAN_BITS)], 1_000),
            (vec![f64::from_bits(PROMETHEUS_STALE_NAN_BITS), 42.0], 1_050),
        ] {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    TIME_INDEX_COLUMN,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Float64, true),
            ]));
            let input = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![1_000, 1_000])),
                    Arc::new(Float64Array::from(values)),
                ],
            )
            .unwrap();
            let stream = InstantManipulateStream {
                start: 1_000,
                end: 1_050,
                lookback_delta: 100,
                interval: 50,
                time_index: 0,
                time_unit: TimeUnit::Millisecond,
                field_indices: [Some(1), None],
                tsid_index: None,
                reuse_tsid_column: false,
                schema: schema.clone(),
                input: Box::pin(
                    datafusion::physical_plan::memory::MemoryStream::try_new(vec![], schema, None)
                        .unwrap(),
                ),
                metric: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                num_series: Count::new(),
            };

            let output = stream.manipulate(input).unwrap();
            let timestamps = output
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let values = output
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(timestamps.values(), &[expected_timestamp]);
            assert_eq!(values.values(), &[42.0]);
        }
    }

    #[test]
    fn empty_batch_uses_declared_output_schema() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            TIME_INDEX_COLUMN,
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]));
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            TIME_INDEX_COLUMN,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let input = RecordBatch::new_empty(input_schema.clone());
        let stream = InstantManipulateStream {
            start: 0,
            end: 0,
            lookback_delta: 0,
            interval: 1,
            time_index: 0,
            time_unit: TimeUnit::Second,
            field_indices: [None, None],
            tsid_index: None,
            reuse_tsid_column: false,
            schema: output_schema.clone(),
            input: Box::pin(
                datafusion::physical_plan::memory::MemoryStream::try_new(
                    vec![],
                    input_schema,
                    None,
                )
                .unwrap(),
            ),
            metric: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            num_series: Count::new(),
        };

        let output = stream.manipulate(input).unwrap();
        assert_eq!(output.schema(), output_schema);
        assert_eq!(
            output.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }

    #[test]
    fn extreme_alignment_retains_exact_sample() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ]));
        let input = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![i64::MAX])),
                Arc::new(Float64Array::from(vec![7.0])),
            ],
        )
        .unwrap();
        let stream = InstantManipulateStream {
            start: i64::MIN + 1,
            end: i64::MAX,
            lookback_delta: 0,
            interval: i64::MAX,
            time_index: 0,
            time_unit: TimeUnit::Millisecond,
            field_indices: [Some(1), None],
            tsid_index: None,
            reuse_tsid_column: false,
            schema: schema.clone(),
            input: Box::pin(
                datafusion::physical_plan::memory::MemoryStream::try_new(vec![], schema, None)
                    .unwrap(),
            ),
            metric: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            num_series: Count::new(),
        };

        let output = stream.manipulate(input).unwrap();
        let timestamps = output
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let values = output
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(timestamps.values(), &[i64::MAX]);
        assert_eq!(values.values(), &[7.0]);
    }

    #[tokio::test]
    async fn ordinary_nan_is_selected_for_exact_and_lookback() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1_000])),
                Arc::new(Float64Array::from(vec![f64::NAN])),
            ],
        )
        .unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap(),
        )));
        let exec = Arc::new(InstantManipulateExec {
            start: 1_000,
            end: 1_500,
            lookback_delta: 1_000,
            interval: 500,
            time_index_column: TIME_INDEX_COLUMN.to_string(),
            field_column: Some("value".to_string()),
            reuse_tsid_column: false,
            output_schema: input.schema(),
            properties: input.properties().clone(),
            input,
            metric: ExecutionPlanMetricsSet::new(),
        });

        let context = SessionContext::default();
        let batches = datafusion::physical_plan::collect(exec, context.task_ctx())
            .await
            .unwrap();
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        let timestamps = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();

        assert_eq!(values.len(), 2);
        assert_eq!(timestamps, vec![1_000, 1_500]);
        assert!(values.iter().all(|value| value.is_nan()));
        assert_eq!(
            values
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            vec![f64::NAN.to_bits(); 2]
        );
    }

    #[tokio::test]
    async fn prometheus_stale_nan_selects_before_and_suppresses_after_marker() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![500, 1_000])),
                Arc::new(Float64Array::from(vec![
                    42.0,
                    f64::from_bits(0x7ff0_0000_0000_0002),
                ])),
            ],
        )
        .unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap(),
        )));
        let exec = Arc::new(InstantManipulateExec {
            start: 750,
            end: 1_500,
            lookback_delta: 1_001,
            interval: 250,
            time_index_column: TIME_INDEX_COLUMN.to_string(),
            field_column: Some("value".to_string()),
            reuse_tsid_column: false,
            output_schema: input.schema(),
            properties: input.properties().clone(),
            input,
            metric: ExecutionPlanMetricsSet::new(),
        });

        let context = SessionContext::default();
        let batches = datafusion::physical_plan::collect(exec, context.task_ctx())
            .await
            .unwrap();

        let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let batch = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
        let timestamp = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0);
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            (row_count, timestamp, value),
            (1, 750, 42.0),
            "only the evaluation before the stale marker should select 42.0"
        );
    }

    #[tokio::test]
    async fn native_histogram_stale_nan_suppresses_exact_and_lookback() {
        let histograms = build_histogram_array(&[
            Some(native_histogram(42.0)),
            Some(native_histogram(f64::from_bits(PROMETHEUS_STALE_NAN_BITS))),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", histograms.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![500, 1_000])),
                histograms,
            ],
        )
        .unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap(),
        )));
        let exec = Arc::new(InstantManipulateExec {
            start: 1_000,
            end: 1_500,
            lookback_delta: 1_001,
            interval: 500,
            time_index_column: TIME_INDEX_COLUMN.to_string(),
            field_column: Some("value".to_string()),
            reuse_tsid_column: false,
            output_schema: input.schema(),
            properties: input.properties().clone(),
            input,
            metric: ExecutionPlanMetricsSet::new(),
        });

        let context = SessionContext::default();
        let batches = datafusion::physical_plan::collect(exec, context.task_ctx())
            .await
            .unwrap();

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn null_value_backed_by_stale_bits_is_selected_for_exact_and_lookback() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIME_INDEX_COLUMN,
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ]));
        let field_column = Float64Array::new(
            vec![f64::from_bits(0x7ff0_0000_0000_0002)].into(),
            Some(NullBuffer::from(vec![false])),
        );
        assert!(!field_column.is_valid(0));
        assert_eq!(field_column.value(0).to_bits(), 0x7ff0_0000_0000_0002);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1_000])),
                Arc::new(field_column),
            ],
        )
        .unwrap();
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap(),
        )));
        let exec = Arc::new(InstantManipulateExec {
            start: 1_000,
            end: 1_500,
            lookback_delta: 1_000,
            interval: 500,
            time_index_column: TIME_INDEX_COLUMN.to_string(),
            field_column: Some("value".to_string()),
            reuse_tsid_column: false,
            output_schema: input.schema(),
            properties: input.properties().clone(),
            input,
            metric: ExecutionPlanMetricsSet::new(),
        });

        let context = SessionContext::default();
        let batches = datafusion::physical_plan::collect(exec, context.task_ctx())
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let batch = batches.iter().find(|batch| batch.num_rows() == 2).unwrap();
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(timestamps.values(), &[1_000, 1_500]);
        assert!(!values.is_valid(0));
        assert!(!values.is_valid(1));
    }
}
