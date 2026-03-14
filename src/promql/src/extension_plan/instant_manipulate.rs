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
use std::cmp::Ordering;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, Float64Array, TimestampMillisecondArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{
    EmptyRelation, Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore,
};
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
    METRIC_NUM_SERIES, Millisecond, resolve_column_name, serialize_column_index,
};
use crate::metrics::PROMQL_SERIES_COUNT;

/// Manipulate the input record batch to make it suitable for Instant Operator.
///
/// This plan will try to align the input time series, for every timestamp between
/// `start` and `end` with step `interval`. Find in the `lookback` range if data
/// is missing at the given timestamp.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct InstantManipulate {
    start: Millisecond,
    end: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,
    time_index_column: String,
    // Planner-provided tag-column hint for execution fast paths.
    tag_columns: Vec<String>,
    /// A optional column for validating staleness
    field_column: Option<String>,
    input: LogicalPlan,
    unfix: Option<UnfixIndices>,
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
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        if self.unfix.is_some() {
            return vec![];
        }

        let mut exprs = vec![col(&self.time_index_column)];
        if let Some(field) = &self.field_column {
            exprs.push(col(field));
        }
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
        if let Some(field) = &self.field_column {
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
                input,
                unfix: None,
            })
        }
    }
}

impl InstantManipulate {
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

    fn resolve_tag_columns(input: &LogicalPlan, tag_columns: &[String]) -> Vec<String> {
        if !tag_columns.is_empty() {
            return tag_columns.to_vec();
        }

        let LogicalPlan::Extension(Extension { node }) = input else {
            return Vec::new();
        };

        node.as_any()
            .downcast_ref::<SeriesDivide>()
            .map(|series_divide| series_divide.tags().to_vec())
            .unwrap_or_default()
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let reuse_all_non_sample_columns =
            matches!(self.tag_columns.as_slice(), [tag] if tag == "__tsid");

        Arc::new(InstantManipulateExec {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index_column: self.time_index_column.clone(),
            field_column: self.field_column.clone(),
            reuse_all_non_sample_columns,
            input: exec_input,
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
        let placeholder_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
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
    reuse_all_non_sample_columns: bool,

    input: Arc<dyn ExecutionPlan>,
    metric: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for InstantManipulateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
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
        Ok(Arc::new(Self {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index_column: self.time_index_column.clone(),
            field_column: self.field_column.clone(),
            reuse_all_non_sample_columns: self.reuse_all_non_sample_columns,
            input: children[0].clone(),
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
        let field_index = self
            .field_column
            .as_ref()
            .and_then(|name| schema.column_with_name(name))
            .map(|x| x.0);
        let reuse_all_non_sample_columns = self.reuse_all_non_sample_columns
            && schema
                .column_with_name("__tsid")
                .is_some_and(|(_, field)| field.data_type() == &DataType::UInt64);
        Ok(Box::pin(InstantManipulateStream {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index,
            field_index,
            reuse_all_non_sample_columns,
            schema,
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
    field_index: Option<usize>,
    reuse_all_non_sample_columns: bool,

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
                if batch.num_rows() == 0 {
                    return Poll::Pending;
                }
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
    // Refer to Prometheus `vectorSelectorSingle` / lookback semantics.
    //
    // Prometheus `v3.9.1` uses a start-exclusive lookback window:
    //   (eval_ts - lookback_delta, eval_ts]
    // i.e. a sample at exactly `eval_ts - lookback_delta` is considered too old.
    pub fn manipulate(&self, input: RecordBatch) -> DataFusionResult<RecordBatch> {
        let ts_column = input
            .column(self.time_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Time index Column downcast to TimestampMillisecondArray failed".into(),
                )
            })?;

        // Early return for empty input
        if ts_column.is_empty() {
            return Ok(input);
        }

        // field column for staleness check
        let field_column = self
            .field_index
            .and_then(|index| input.column(index).as_any().downcast_ref::<Float64Array>());

        // Optimize iteration range based on actual data bounds
        let first_ts = ts_column.value(0);
        let last_ts = ts_column.value(ts_column.len() - 1);
        // A sample at `t` is eligible for eval time `eval_ts` iff:
        //   t > eval_ts - lookback_delta  <=>  eval_ts < t + lookback_delta.
        // Therefore the last eval timestamp for which the last sample is still eligible is:
        //   last_ts + lookback_delta - 1 (millisecond granularity).
        let last_useful = if self.lookback_delta > 0 {
            last_ts + self.lookback_delta - 1
        } else {
            last_ts
        };

        let max_start = first_ts.max(self.start);
        let min_end = last_useful.min(self.end);

        let aligned_start = self.start + (max_start - self.start) / self.interval * self.interval;
        let aligned_end = self.end - (self.end - min_end) / self.interval * self.interval;

        let estimated_points = if aligned_end >= aligned_start {
            ((aligned_end - aligned_start) / self.interval).saturating_add(1) as usize
        } else {
            0
        };
        let mut take_indices = Vec::with_capacity(estimated_points);

        let mut cursor = 0;

        let aligned_ts_iter = (aligned_start..=aligned_end).step_by(self.interval as usize);
        let mut aligned_ts = Vec::with_capacity(estimated_points);

        // calculate the offsets to take
        'next: for expected_ts in aligned_ts_iter {
            // first, search toward end to see if there is matched timestamp
            while cursor < ts_column.len() {
                let curr = ts_column.value(cursor);
                match curr.cmp(&expected_ts) {
                    Ordering::Equal => {
                        if let Some(field_column) = &field_column
                            && field_column.value(cursor).is_nan()
                        {
                            // ignore the NaN value
                        } else {
                            take_indices.push(cursor as u64);
                            aligned_ts.push(expected_ts);
                        }
                        continue 'next;
                    }
                    Ordering::Greater => break,
                    Ordering::Less => {}
                }
                cursor += 1;
            }
            if cursor == ts_column.len() {
                cursor -= 1;
                // short cut this loop
                if ts_column.value(cursor) + self.lookback_delta <= expected_ts {
                    break;
                }
            }

            // then examine the value
            let curr_ts = ts_column.value(cursor);
            if curr_ts + self.lookback_delta <= expected_ts {
                continue;
            }
            if curr_ts > expected_ts {
                // exceeds current expected timestamp, examine the previous value
                if let Some(prev_cursor) = cursor.checked_sub(1) {
                    let prev_ts = ts_column.value(prev_cursor);
                    if prev_ts + self.lookback_delta > expected_ts {
                        // only use the point in the time range
                        if let Some(field_column) = &field_column
                            && field_column.value(prev_cursor).is_nan()
                        {
                            // if the newest value is NaN, it means the value is stale, so we should not use it
                            continue;
                        }
                        // use this point
                        take_indices.push(prev_cursor as u64);
                        aligned_ts.push(expected_ts);
                    }
                }
            } else if let Some(field_column) = &field_column
                && field_column.value(cursor).is_nan()
            {
                // if the newest value is NaN, it means the value is stale, so we should not use it
            } else {
                // use this point
                take_indices.push(cursor as u64);
                aligned_ts.push(expected_ts);
            }
        }

        // take record batch and replace the time index column
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

            if self.reuse_all_non_sample_columns && Some(index) != self.field_index {
                arrays.push(reuse_constant_column(array, output_len)?);
                continue;
            }

            let indices_array =
                indices_array.get_or_insert_with(|| UInt64Array::from(take_indices.clone()));
            arrays.push(compute::take(array, indices_array, None)?);
        }

        let result = RecordBatch::try_new(record_batch.schema(), arrays)
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
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::extension_plan::test_util::{
        TIME_INDEX_COLUMN, prepare_test_data, prepare_test_data_with_nan,
    };

    async fn do_normalize_test(
        start: Millisecond,
        end: Millisecond,
        lookback_delta: Millisecond,
        interval: Millisecond,
        expected: String,
        contains_nan: bool,
    ) {
        let memory_exec = if contains_nan {
            Arc::new(prepare_test_data_with_nan())
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
            reuse_all_non_sample_columns: false,
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

    #[tokio::test]
    async fn tsid_fast_path_reuses_non_sample_columns_when_output_grows() {
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
            reuse_all_non_sample_columns: true,
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
    async fn lookback_10s_interval_10s_with_nan() {
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
    async fn lookback_10s_interval_10s_with_nan_unaligned() {
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
}
