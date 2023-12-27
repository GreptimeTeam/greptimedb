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
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datatypes::arrow::compute;
use datatypes::arrow::error::Result as ArrowResult;
use futures::{Stream, StreamExt};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DeserializeSnafu, Result};
use crate::extension_plan::Millisecond;

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

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert!(!inputs.is_empty());

        Self {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index_column: self.time_index_column.clone(),
            field_column: self.field_column.clone(),
            input: inputs[0].clone(),
        }
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

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
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
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        let input_stats = self.input.statistics();

        let estimated_row_num = (self.end - self.start) as f64 / self.interval as f64;
        let estimated_total_bytes = input_stats
            .total_byte_size
            .zip(input_stats.num_rows)
            .map(|(size, rows)| (size as f64 / rows as f64) * estimated_row_num)
            .map(|size| size.floor() as _);

        Statistics {
            num_rows: Some(estimated_row_num.floor() as _),
            total_byte_size: estimated_total_bytes,
            // TODO(ruihang): support this column statistics
            column_statistics: None,
            is_exact: false,
        }
    }
}

impl DisplayAs for InstantManipulateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
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
}

impl RecordBatchStream for InstantManipulateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for InstantManipulateStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = match self.input.poll_next_unpin(cx) {
            Poll::Ready(batch) => {
                let _timer = self.metric.elapsed_compute().timer();
                Poll::Ready(batch.map(|batch| batch.and_then(|batch| self.manipulate(batch))))
            }
            Poll::Pending => Poll::Pending,
        };
        self.metric.record_poll(poll)
    }
}

impl InstantManipulateStream {
    // refer to Go version: https://github.com/prometheus/prometheus/blob/e934d0f01158a1d55fa0ebb035346b195fcc1260/promql/engine.go#L1571
    // and the function `vectorSelectorSingle`
    pub fn manipulate(&self, input: RecordBatch) -> DataFusionResult<RecordBatch> {
        let mut take_indices = vec![];
        // TODO(ruihang): maybe the input is not timestamp millisecond array
        let ts_column = input
            .column(self.time_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        // field column for staleness check
        let field_column = self
            .field_index
            .and_then(|index| input.column(index).as_any().downcast_ref::<Float64Array>());

        let mut cursor = 0;

        let aligned_ts_iter = (self.start..=self.end).step_by(self.interval as usize);
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
            .map_err(DataFusionError::ArrowError)?;
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
        do_normalize_test(1, 900_000_000_000_000, 10_000, 10_000, expected, true).await;
    }
}
