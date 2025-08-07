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
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, TimestampMillisecondArray};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{Field, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion::sql::TableReference;
use futures::{ready, Stream, StreamExt};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DeserializeSnafu, Result};
use crate::extension_plan::{Millisecond, METRIC_NUM_SERIES};
use crate::metrics::PROMQL_SERIES_COUNT;
use crate::range_array::RangeArray;

/// Time series manipulator for range function.
///
/// This plan will "fold" time index and value columns into [RangeArray]s, and truncate
/// other columns to the same length with the "folded" [RangeArray] column.
///
/// To pass runtime information to the execution plan (or the range function), This plan
/// will add those extra columns:
/// - timestamp range with type [RangeArray], which is the folded timestamp column.
/// - end of current range with the same type as the timestamp column. (todo)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RangeManipulate {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,

    time_index: String,
    field_columns: Vec<String>,
    input: LogicalPlan,
    output_schema: DFSchemaRef,
}

impl RangeManipulate {
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        range: Millisecond,
        time_index: String,
        field_columns: Vec<String>,
        input: LogicalPlan,
    ) -> DataFusionResult<Self> {
        let output_schema =
            Self::calculate_output_schema(input.schema(), &time_index, &field_columns)?;
        Ok(Self {
            start,
            end,
            interval,
            range,
            time_index,
            field_columns,
            input,
            output_schema,
        })
    }

    pub const fn name() -> &'static str {
        "RangeManipulate"
    }

    pub fn build_timestamp_range_name(time_index: &str) -> String {
        format!("{time_index}_range")
    }

    pub fn internal_range_end_col_name() -> String {
        "__internal_range_end".to_string()
    }

    fn range_timestamp_name(&self) -> String {
        Self::build_timestamp_range_name(&self.time_index)
    }

    fn calculate_output_schema(
        input_schema: &DFSchemaRef,
        time_index: &str,
        field_columns: &[String],
    ) -> DataFusionResult<DFSchemaRef> {
        let columns = input_schema.fields();
        let mut new_columns = Vec::with_capacity(columns.len() + 1);
        for i in 0..columns.len() {
            let x = input_schema.qualified_field(i);
            new_columns.push((x.0.cloned(), Arc::new(x.1.clone())));
        }

        // process time index column
        // the raw timestamp field is preserved. And a new timestamp_range field is appended to the last.
        let Some(ts_col_index) = input_schema.index_of_column_by_name(None, time_index) else {
            return Err(datafusion::common::field_not_found(
                None::<TableReference>,
                time_index,
                input_schema.as_ref(),
            ));
        };
        let ts_col_field = &columns[ts_col_index];
        let timestamp_range_field = Field::new(
            Self::build_timestamp_range_name(time_index),
            RangeArray::convert_field(ts_col_field).data_type().clone(),
            ts_col_field.is_nullable(),
        );
        new_columns.push((None, Arc::new(timestamp_range_field)));

        // process value columns
        for name in field_columns {
            let Some(index) = input_schema.index_of_column_by_name(None, name) else {
                return Err(datafusion::common::field_not_found(
                    None::<TableReference>,
                    name,
                    input_schema.as_ref(),
                ));
            };
            new_columns[index] = (None, Arc::new(RangeArray::convert_field(&columns[index])));
        }

        Ok(Arc::new(DFSchema::new_with_metadata(
            new_columns,
            HashMap::new(),
        )?))
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let output_schema: SchemaRef = SchemaRef::new(self.output_schema.as_ref().into());
        let properties = exec_input.properties();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            properties.partitioning.clone(),
            properties.emission_type,
            properties.boundedness,
        );
        Arc::new(RangeManipulateExec {
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index_column: self.time_index.clone(),
            time_range_column: self.range_timestamp_name(),
            field_columns: self.field_columns.clone(),
            input: exec_input,
            output_schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        pb::RangeManipulate {
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index: self.time_index.clone(),
            tag_columns: self.field_columns.clone(),
        }
        .encode_to_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let pb_range_manipulate = pb::RangeManipulate::decode(bytes).context(DeserializeSnafu)?;
        let empty_schema = Arc::new(DFSchema::empty());
        let placeholder_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: empty_schema.clone(),
        });

        // Unlike `Self::new()`, this method doesn't check the input schema as it will fail
        // because the input schema is empty.
        // But this is Ok since datafusion guarantees to call `with_exprs_and_inputs` for the
        // deserialized plan.
        Ok(Self {
            start: pb_range_manipulate.start,
            end: pb_range_manipulate.end,
            interval: pb_range_manipulate.interval,
            range: pb_range_manipulate.range,
            time_index: pb_range_manipulate.time_index,
            field_columns: pb_range_manipulate.tag_columns,
            input: placeholder_plan,
            output_schema: empty_schema,
        })
    }
}

impl PartialOrd for RangeManipulate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Compare fields in order excluding output_schema
        match self.start.partial_cmp(&other.start) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.end.partial_cmp(&other.end) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.interval.partial_cmp(&other.interval) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.range.partial_cmp(&other.range) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.time_index.partial_cmp(&other.time_index) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.field_columns.partial_cmp(&other.field_columns) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.input.partial_cmp(&other.input)
    }
}

impl UserDefinedLogicalNodeCore for RangeManipulate {
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
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromRangeManipulate: req range=[{}..{}], interval=[{}], eval range=[{}], time index=[{}], values={:?}",
            self.start, self.end, self.interval, self.range, self.time_index, self.field_columns
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "RangeManipulate should have at exact one input".to_string(),
            ));
        }

        let input: LogicalPlan = inputs.pop().unwrap();
        let input_schema = input.schema();
        let output_schema =
            Self::calculate_output_schema(input_schema, &self.time_index, &self.field_columns)?;

        Ok(Self {
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index: self.time_index.clone(),
            field_columns: self.field_columns.clone(),
            input,
            output_schema,
        })
    }
}

#[derive(Debug)]
pub struct RangeManipulateExec {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,
    time_index_column: String,
    time_range_column: String,
    field_columns: Vec<String>,

    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    metric: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl ExecutionPlan for RangeManipulateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let input_requirement = self.input.required_input_distribution();
        if input_requirement.is_empty() {
            // if the input is EmptyMetric, its required_input_distribution() is empty so we can't
            // use its input distribution.
            vec![Distribution::UnspecifiedDistribution]
        } else {
            input_requirement
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(!children.is_empty());
        let exec_input = children[0].clone();
        let properties = exec_input.properties();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(self.output_schema.clone()),
            properties.partitioning.clone(),
            properties.emission_type,
            properties.boundedness,
        );
        Ok(Arc::new(Self {
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index_column: self.time_index_column.clone(),
            time_range_column: self.time_range_column.clone(),
            field_columns: self.field_columns.clone(),
            output_schema: self.output_schema.clone(),
            input: children[0].clone(),
            metric: self.metric.clone(),
            properties,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let baseline_metric = BaselineMetrics::new(&self.metric, partition);
        let metrics_builder = MetricBuilder::new(&self.metric);
        let num_series = Count::new();
        metrics_builder
            .with_partition(partition)
            .build(MetricValue::Count {
                name: METRIC_NUM_SERIES.into(),
                count: num_series.clone(),
            });

        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let time_index = schema
            .column_with_name(&self.time_index_column)
            .unwrap_or_else(|| panic!("time index column {} not found", self.time_index_column))
            .0;
        let field_columns = self
            .field_columns
            .iter()
            .map(|value_col| {
                schema
                    .column_with_name(value_col)
                    .unwrap_or_else(|| panic!("value column {value_col} not found",))
                    .0
            })
            .collect();
        let aligned_ts_array =
            RangeManipulateStream::build_aligned_ts_array(self.start, self.end, self.interval);
        Ok(Box::pin(RangeManipulateStream {
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index,
            field_columns,
            aligned_ts_array,
            output_schema: self.output_schema.clone(),
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
            .unwrap_or_default();

        Ok(Statistics {
            num_rows: Precision::Inexact(estimated_row_num as _),
            total_byte_size: estimated_total_bytes,
            // TODO(ruihang): support this column statistics
            column_statistics: Statistics::unknown_column(&self.schema()),
        })
    }

    fn name(&self) -> &str {
        "RangeManipulateExec"
    }
}

impl DisplayAs for RangeManipulateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "PromRangeManipulateExec: req range=[{}..{}], interval=[{}], eval range=[{}], time index=[{}]",
                   self.start, self.end, self.interval, self.range, self.time_index_column
                )
            }
        }
    }
}

pub struct RangeManipulateStream {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,
    time_index: usize,
    field_columns: Vec<usize>,
    aligned_ts_array: ArrayRef,

    output_schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
    /// Number of series processed.
    num_series: Count,
}

impl RecordBatchStream for RangeManipulateStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for RangeManipulateStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let timer = std::time::Instant::now();
                    let result = self.manipulate(batch);
                    if let Ok(None) = result {
                        self.metric.elapsed_compute().add_elapsed(timer);
                        continue;
                    } else {
                        self.num_series.add(1);
                        self.metric.elapsed_compute().add_elapsed(timer);
                        break Poll::Ready(result.transpose());
                    }
                }
                None => {
                    PROMQL_SERIES_COUNT.observe(self.num_series.value() as f64);
                    break Poll::Ready(None);
                }
                Some(Err(e)) => break Poll::Ready(Some(Err(e))),
            }
        };
        self.metric.record_poll(poll)
    }
}

impl RangeManipulateStream {
    // Prometheus: https://github.com/prometheus/prometheus/blob/e934d0f01158a1d55fa0ebb035346b195fcc1260/promql/engine.go#L1113-L1198
    // But they are not exactly the same, because we don't eager-evaluate on the data in this plan.
    // And the generated timestamp is not aligned to the step. It's expected to do later.
    pub fn manipulate(&self, input: RecordBatch) -> DataFusionResult<Option<RecordBatch>> {
        let mut other_columns = (0..input.columns().len()).collect::<HashSet<_>>();
        // calculate the range
        let (ranges, (start, end)) = self.calculate_range(&input)?;
        // ignore this if all ranges are empty
        if ranges.iter().all(|(_, len)| *len == 0) {
            return Ok(None);
        }

        // transform columns
        let mut new_columns = input.columns().to_vec();
        for index in self.field_columns.iter() {
            let _ = other_columns.remove(index);
            let column = input.column(*index);
            let new_column = Arc::new(
                RangeArray::from_ranges(column.clone(), ranges.clone())
                    .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?
                    .into_dict(),
            );
            new_columns[*index] = new_column;
        }

        // push timestamp range column
        let ts_range_column =
            RangeArray::from_ranges(input.column(self.time_index).clone(), ranges.clone())
                .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?
                .into_dict();
        new_columns.push(Arc::new(ts_range_column));

        // truncate other columns
        let take_indices = Int64Array::from(vec![0; ranges.len()]);
        for index in other_columns.into_iter() {
            new_columns[index] = compute::take(&input.column(index), &take_indices, None)?;
        }
        // replace timestamp with the aligned one
        let new_time_index = if ranges.len() != self.aligned_ts_array.len() {
            Self::build_aligned_ts_array(start, end, self.interval)
        } else {
            self.aligned_ts_array.clone()
        };
        new_columns[self.time_index] = new_time_index;

        RecordBatch::try_new(self.output_schema.clone(), new_columns)
            .map(Some)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    fn build_aligned_ts_array(start: i64, end: i64, interval: i64) -> ArrayRef {
        Arc::new(TimestampMillisecondArray::from_iter_values(
            (start..=end).step_by(interval as _),
        ))
    }

    /// Return values:
    /// - A vector of tuples where each tuple contains the start index and length of the range.
    /// - A tuple of the actual start/end timestamp used to calculate the range.
    #[allow(clippy::type_complexity)]
    fn calculate_range(
        &self,
        input: &RecordBatch,
    ) -> DataFusionResult<(Vec<(u32, u32)>, (i64, i64))> {
        let ts_column = input
            .column(self.time_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Time index Column downcast to TimestampMillisecondArray failed".into(),
                )
            })?;

        let len = ts_column.len();
        if len == 0 {
            return Ok((vec![], (self.start, self.end)));
        }

        // shorten the range to calculate
        let first_ts = ts_column.value(0);
        let first_ts_aligned = (first_ts / self.interval) * self.interval;
        let last_ts = ts_column.value(ts_column.len() - 1);
        let last_ts_aligned = ((last_ts + self.range) / self.interval) * self.interval;
        let start = self.start.max(first_ts_aligned);
        let end = self.end.min(last_ts_aligned);
        if start > end {
            return Ok((vec![], (start, end)));
        }
        let mut ranges = Vec::with_capacity(((self.end - self.start) / self.interval + 1) as usize);

        // calculate for every aligned timestamp (`curr_ts`), assume the ts column is ordered.
        let mut range_start_index = 0usize;
        let mut last_range_start = 0;
        let mut start_delta = 0;
        for curr_ts in (start..=end).step_by(self.interval as _) {
            // determine range start
            let start_ts = curr_ts - self.range;

            // advance cursor based on last range
            let mut range_start = ts_column.len();
            let mut range_end = 0;
            let mut cursor = range_start_index + start_delta;
            // search back to keep the result correct
            while cursor < ts_column.len() && ts_column.value(cursor) > start_ts && cursor > 0 {
                cursor -= 1;
            }

            while cursor < ts_column.len() {
                let ts = ts_column.value(cursor);
                if range_start > cursor && ts >= start_ts {
                    range_start = cursor;
                    range_start_index = range_start;
                }
                if ts <= curr_ts {
                    range_end = range_end.max(cursor);
                } else {
                    range_start_index = range_start_index.checked_sub(1usize).unwrap_or_default();
                    break;
                }
                cursor += 1;
            }
            if range_start > range_end {
                ranges.push((0, 0));
                start_delta = 0;
            } else {
                ranges.push((range_start as _, (range_end + 1 - range_start) as _));
                start_delta = range_start - last_range_start;
                last_range_start = range_start;
            }
        }

        Ok((ranges, (start, end)))
    }
}

#[cfg(test)]
mod test {
    use datafusion::arrow::array::{ArrayRef, DictionaryArray, Float64Array, StringArray};
    use datafusion::arrow::datatypes::{
        ArrowPrimitiveType, DataType, Field, Int64Type, Schema, TimestampMillisecondType,
    };
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::prelude::SessionContext;
    use datatypes::arrow::array::TimestampMillisecondArray;

    use super::*;

    const TIME_INDEX_COLUMN: &str = "timestamp";

    fn prepare_test_data() -> DataSourceExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new("value_1", DataType::Float64, true),
            Field::new("value_2", DataType::Float64, true),
            Field::new("path", DataType::Utf8, true),
        ]));
        let timestamp_column = Arc::new(TimestampMillisecondArray::from(vec![
            0, 30_000, 60_000, 90_000, 120_000, // every 30s
            180_000, 240_000, // every 60s
            241_000, 271_000, 291_000, // others
        ])) as _;
        let field_column: ArrayRef = Arc::new(Float64Array::from(vec![1.0; 10])) as _;
        let path_column = Arc::new(StringArray::from(vec!["foo"; 10])) as _;
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                timestamp_column,
                field_column.clone(),
                field_column,
                path_column,
            ],
        )
        .unwrap();

        DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![data]], schema, None).unwrap(),
        ))
    }

    async fn do_normalize_test(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        range: Millisecond,
        expected: String,
    ) {
        let memory_exec = Arc::new(prepare_test_data());
        let time_index = TIME_INDEX_COLUMN.to_string();
        let field_columns = vec!["value_1".to_string(), "value_2".to_string()];
        let manipulate_output_schema = SchemaRef::new(
            RangeManipulate::calculate_output_schema(
                &memory_exec.schema().to_dfschema_ref().unwrap(),
                &time_index,
                &field_columns,
            )
            .unwrap()
            .as_ref()
            .into(),
        );
        let properties = PlanProperties::new(
            EquivalenceProperties::new(manipulate_output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let normalize_exec = Arc::new(RangeManipulateExec {
            start,
            end,
            interval,
            range,
            field_columns,
            output_schema: manipulate_output_schema,
            time_range_column: RangeManipulate::build_timestamp_range_name(&time_index),
            time_index_column: time_index,
            input: memory_exec,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        });
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(normalize_exec, session_context.task_ctx())
            .await
            .unwrap();
        // DirectoryArray from RangeArray cannot be print as normal arrays.
        let result_literal: String = result
            .into_iter()
            .filter_map(|batch| {
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        if matches!(array.data_type(), &DataType::Dictionary(..)) {
                            let dict_array = array
                                .as_any()
                                .downcast_ref::<DictionaryArray<Int64Type>>()
                                .unwrap()
                                .clone();
                            format!("{:?}", RangeArray::try_new(dict_array).unwrap())
                        } else {
                            format!("{array:?}")
                        }
                    })
                    .reduce(|lhs, rhs| lhs + "\n" + &rhs)
            })
            .reduce(|lhs, rhs| lhs + "\n\n" + &rhs)
            .unwrap();

        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn interval_30s_range_90s() {
        let expected = String::from(
            "PrimitiveArray<Timestamp(Millisecond, None)>\n[\n  \
                1970-01-01T00:00:00,\n  \
                1970-01-01T00:00:30,\n  \
                1970-01-01T00:01:00,\n  \
                1970-01-01T00:01:30,\n  \
                1970-01-01T00:02:00,\n  \
                1970-01-01T00:02:30,\n  \
                1970-01-01T00:03:00,\n  \
                1970-01-01T00:03:30,\n  \
                1970-01-01T00:04:00,\n  \
                1970-01-01T00:04:30,\n  \
                1970-01-01T00:05:00,\n\
            ]\nRangeArray { \
                base array: PrimitiveArray<Float64>\n[\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n], \
                ranges: [Some(0..1), Some(0..2), Some(0..3), Some(0..4), Some(1..5), Some(2..5), Some(3..6), Some(4..6), Some(5..7), Some(5..8), Some(6..10)] \
            }\nRangeArray { \
                base array: PrimitiveArray<Float64>\n[\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n], \
                ranges: [Some(0..1), Some(0..2), Some(0..3), Some(0..4), Some(1..5), Some(2..5), Some(3..6), Some(4..6), Some(5..7), Some(5..8), Some(6..10)] \
            }\nStringArray\n[\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n]\n\
            RangeArray { \
                base array: PrimitiveArray<Timestamp(Millisecond, None)>\n[\n  1970-01-01T00:00:00,\n  1970-01-01T00:00:30,\n  1970-01-01T00:01:00,\n  1970-01-01T00:01:30,\n  1970-01-01T00:02:00,\n  1970-01-01T00:03:00,\n  1970-01-01T00:04:00,\n  1970-01-01T00:04:01,\n  1970-01-01T00:04:31,\n  1970-01-01T00:04:51,\n], \
                ranges: [Some(0..1), Some(0..2), Some(0..3), Some(0..4), Some(1..5), Some(2..5), Some(3..6), Some(4..6), Some(5..7), Some(5..8), Some(6..10)] \
            }",
);
        do_normalize_test(0, 310_000, 30_000, 90_000, expected.clone()).await;

        // dump large range
        do_normalize_test(-300000, 310_000, 30_000, 90_000, expected).await;
    }

    #[tokio::test]
    async fn small_empty_range() {
        let expected = String::from(
        "PrimitiveArray<Timestamp(Millisecond, None)>\n[\n  \
            1970-01-01T00:00:00.001,\n  \
            1970-01-01T00:00:03.001,\n  \
            1970-01-01T00:00:06.001,\n  \
            1970-01-01T00:00:09.001,\n\
        ]\nRangeArray { \
            base array: PrimitiveArray<Float64>\n[\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n], \
            ranges: [Some(0..1), Some(0..0), Some(0..0), Some(0..0)] \
        }\nRangeArray { \
            base array: PrimitiveArray<Float64>\n[\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n  1.0,\n], \
            ranges: [Some(0..1), Some(0..0), Some(0..0), Some(0..0)] \
        }\nStringArray\n[\n  \"foo\",\n  \"foo\",\n  \"foo\",\n  \"foo\",\n]\n\
        RangeArray { \
            base array: PrimitiveArray<Timestamp(Millisecond, None)>\n[\n  1970-01-01T00:00:00,\n  1970-01-01T00:00:30,\n  1970-01-01T00:01:00,\n  1970-01-01T00:01:30,\n  1970-01-01T00:02:00,\n  1970-01-01T00:03:00,\n  1970-01-01T00:04:00,\n  1970-01-01T00:04:01,\n  1970-01-01T00:04:31,\n  1970-01-01T00:04:51,\n], \
            ranges: [Some(0..1), Some(0..0), Some(0..0), Some(0..0)] \
        }");
        do_normalize_test(1, 10_001, 3_000, 1_000, expected).await;
    }
}
