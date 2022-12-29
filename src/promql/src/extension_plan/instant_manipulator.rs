// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

use datafusion::arrow::array::{Array, TimestampMillisecondArray, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datatypes::arrow::compute;
use datatypes::arrow::error::Result as ArrowResult;
use futures::{Stream, StreamExt};

use crate::extension_plan::Millisecond;

#[derive(Debug)]
pub struct InstantManipulator {
    start: Millisecond,
    end: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,

    time_index_column: String,
    value_columns: Vec<String>,

    exprs: Vec<Expr>,
    input: LogicalPlan,
}

impl UserDefinedLogicalNode for InstantManipulator {
    fn as_any(&self) -> &dyn Any {
        self as _
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.exprs.clone()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromInstantManipulator: lookback=[{}], interval=[{}], time index=[{}]",
            self.lookback_delta, self.interval, self.time_index_column
        )?;
        for (i, expr_item) in self.exprs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}", expr_item)?;
        }
        Ok(())
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        assert!(!inputs.is_empty());

        Arc::new(Self {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index_column: self.time_index_column.clone(),
            value_columns: self.value_columns.clone(),
            exprs: exprs.to_vec(),
            input: inputs[0].clone(),
        })
    }
}

impl InstantManipulator {
    pub fn new<E: AsRef<[Expr]>>(
        start: Millisecond,
        end: Millisecond,
        lookback_delta: Millisecond,
        interval: Millisecond,
        time_index_column: String,
        value_columns: Vec<String>,
        exprs: E,
        input: LogicalPlan,
    ) -> Self {
        Self {
            start,
            end,
            lookback_delta,
            interval,
            time_index_column,
            value_columns,
            exprs: exprs.as_ref().to_vec(),
            input,
        }
    }

    pub fn to_execution_plan(
        &self,
        exec_input: Arc<dyn ExecutionPlan>,
        physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(InstantManipulatorExec {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index_column: self.time_index_column.clone(),
            value_columns: self.value_columns.clone(),
            input: exec_input,
            exprs: physical_exprs,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }
}

#[derive(Debug)]
pub struct InstantManipulatorExec {
    start: Millisecond,
    end: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,

    time_index_column: String,
    value_columns: Vec<String>,

    input: Arc<dyn ExecutionPlan>,
    exprs: Vec<Arc<dyn PhysicalExpr>>,

    metric: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for InstantManipulatorExec {
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

    fn maintains_input_order(&self) -> bool {
        true
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
            value_columns: self.value_columns.clone(),
            exprs: self.exprs.clone(),
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
        Ok(Box::pin(InstantManipulatorStream {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            time_index,
            schema,
            input,
            metric: baseline_metric,
        }))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "PromInstantManipulatorExec: lookback=[{}], interval=[{}], time index=[{}]",
                    self.lookback_delta, self.interval, self.time_index_column
                )
            }
        }
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

pub struct InstantManipulatorStream {
    start: Millisecond,
    end: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,
    // Column index of TIME INDEX column's position in schema
    time_index: usize,

    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
}

impl RecordBatchStream for InstantManipulatorStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for InstantManipulatorStream {
    type Item = ArrowResult<RecordBatch>;

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

impl InstantManipulatorStream {
    /// Manipulate the input record batch to make it suit for Instant Operator.
    ///
    /// This plan will try to align the input time series, for every timestamp between
    /// `start` and `end` with step `interval`. Find in the `lookback` range if data
    /// is missing at the given timestamp.
    pub fn manipulate(&self, input: RecordBatch) -> ArrowResult<RecordBatch> {
        let mut take_indices = Vec::with_capacity(input.num_rows());
        // TODO(ruihang): maybe the input is not timestamp millisecond array
        let ts_column = input
            .column(self.time_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        // prepare two vectors
        let mut cursor = 0;
        let expected_iter = (self.start..=self.end).step_by(self.interval as usize);

        // calculate the offsets to take
        'next: for expected_ts in expected_iter {
            // first, search toward end to see if there is matched timestamp
            while cursor < ts_column.len() {
                let curr = ts_column.value(cursor);
                if curr == expected_ts {
                    take_indices.push(Some(cursor as u64));
                    continue 'next;
                } else if curr > expected_ts {
                    break;
                }
                cursor += 1;
            }
            // then, search backward to lookback
            loop {
                let curr = ts_column.value(cursor);
                if curr + self.lookback_delta < expected_ts {
                    // not found in lookback, leave this field blank
                    take_indices.push(None);
                    break;
                } else if curr < expected_ts && curr + self.lookback_delta >= expected_ts {
                    // find the expected value, push and break
                    take_indices.push(Some(cursor as u64));
                    break;
                } else if cursor == 0 {
                    // reach the first value and not found in lookback, leave this field blank
                    break;
                }
                cursor -= 1;
            }
        }

        // take record batch
        Self::take_record_batch_optional(input, take_indices)
    }

    /// Helper function to apply "take" on record batch.
    fn take_record_batch_optional(
        record_batch: RecordBatch,
        take_indices: Vec<Option<u64>>,
    ) -> ArrowResult<RecordBatch> {
        let indices_array = UInt64Array::from(take_indices);
        let arrays = record_batch
            .columns()
            .iter()
            .map(|array| compute::take(array, &indices_array, None))
            .collect::<ArrowResult<Vec<_>>>()?;

        RecordBatch::try_new(record_batch.schema(), arrays)
    }
}
