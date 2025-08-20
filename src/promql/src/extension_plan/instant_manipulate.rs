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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datatypes::arrow::compute;
use datatypes::arrow::error::Result as ArrowResult;
use futures::{ready, Stream, StreamExt};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DeserializeSnafu, Result};
use crate::extension_plan::{Millisecond, METRIC_NUM_SERIES};
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
    /// A optional column for validating staleness
    field_column: Option<String>,
    input: LogicalPlan,
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
        vec![]
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

        Ok(Self {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index_column: self.time_index_column.clone(),
            field_column: self.field_column.clone(),
            input: inputs.into_iter().next().unwrap(),
        })
    }
}

impl InstantManipulate {
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        lookback_delta: Millisecond,
        interval: Millisecond,
        time_index_column: String,
        field_column: Option<String>,
        input: LogicalPlan,
    ) -> Self {
        Self {
            start,
            end,
            lookback_delta,
            interval,
            time_index_column,
            field_column,
            input,
        }
    }

    pub const fn name() -> &'static str {
        "InstantManipulate"
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(InstantManipulateExec {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index_column: self.time_index_column.clone(),
            field_column: self.field_column.clone(),
            input: exec_input,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        pb::InstantManipulate {
            start: self.start,
            end: self.end,
            interval: self.interval,
            lookback_delta: self.lookback_delta,
            time_index: self.time_index_column.clone(),
            field_index: self.field_column.clone().unwrap_or_default(),
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
        let field_column = if pb_instant_manipulate.field_index.is_empty() {
            None
        } else {
            Some(pb_instant_manipulate.field_index)
        };
        Ok(Self {
            start: pb_instant_manipulate.start,
            end: pb_instant_manipulate.end,
            lookback_delta: pb_instant_manipulate.lookback_delta,
            interval: pb_instant_manipulate.interval,
            time_index_column: pb_instant_manipulate.time_index,
            field_column,
            input: placeholder_plan,
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

    fn properties(&self) -> &PlanProperties {
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
            .expect("time index column not found")
            .0;
        let field_index = self
            .field_column
            .as_ref()
            .and_then(|name| schema.column_with_name(name))
            .map(|x| x.0);
        Ok(Box::pin(InstantManipulateStream {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index,
            field_index,
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
            column_statistics: vec![
                ColumnStatistics::new_unknown();
                self.schema().flattened_fields().len()
            ],
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
                   self.start,self.end, self.lookback_delta, self.interval, self.time_index_column
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
    // refer to Go version: https://github.com/prometheus/prometheus/blob/e934d0f01158a1d55fa0ebb035346b195fcc1260/promql/engine.go#L1571
    // and the function `vectorSelectorSingle`
    pub fn manipulate(&self, input: RecordBatch) -> DataFusionResult<RecordBatch> {
        let mut take_indices = vec![];

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
        let last_useful = last_ts + self.lookback_delta;

        let max_start = first_ts.max(self.start);
        let min_end = last_useful.min(self.end);

        let aligned_start = self.start + (max_start - self.start) / self.interval * self.interval;
        let aligned_end = self.end - (self.end - min_end) / self.interval * self.interval;

        let mut cursor = 0;

        let aligned_ts_iter = (aligned_start..=aligned_end).step_by(self.interval as usize);
        let mut aligned_ts = vec![];

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
                if ts_column.value(cursor) + self.lookback_delta < expected_ts {
                    break;
                }
            }

            // then examine the value
            let curr_ts = ts_column.value(cursor);
            if curr_ts + self.lookback_delta < expected_ts {
                continue;
            }
            if curr_ts > expected_ts {
                // exceeds current expected timestamp, examine the previous value
                if let Some(prev_cursor) = cursor.checked_sub(1) {
                    let prev_ts = ts_column.value(prev_cursor);
                    if prev_ts + self.lookback_delta >= expected_ts {
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

        let indices_array = UInt64Array::from(take_indices);
        let mut arrays = record_batch
            .columns()
            .iter()
            .map(|array| compute::take(array, &indices_array, None))
            .collect::<ArrowResult<Vec<_>>>()?;
        arrays[self.time_index] = Arc::new(TimestampMillisecondArray::from(aligned_ts));

        let result = RecordBatch::try_new(record_batch.schema(), arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::extension_plan::test_util::{
        prepare_test_data, prepare_test_data_with_nan, TIME_INDEX_COLUMN,
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
            \n| 1970-01-01T00:00:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:00:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:01:40 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:02:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:10 | 1.0   | foo  |\
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
            \n| 1970-01-01T00:02:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:30 | 1.0   | foo  |\
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
            \n| 1970-01-01T00:02:30 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:00 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:10 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:20 | 1.0   | foo  |\
            \n| 1970-01-01T00:03:30 | 1.0   | foo  |\
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
            \n| 1970-01-01T00:00:10 | 1.0   | foo  |\
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
            \n| 1970-01-01T00:03:30 | 1.0   | foo  |\
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
            \n| 1970-01-01T00:00:10 | 0.0   |\
            \n| 1970-01-01T00:01:00 | 6.0   |\
            \n| 1970-01-01T00:01:10 | 6.0   |\
            \n| 1970-01-01T00:02:00 | 12.0  |\
            \n| 1970-01-01T00:02:10 | 12.0  |\
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
