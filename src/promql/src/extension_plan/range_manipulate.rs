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

use datafusion::arrow::array::{Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
    Statistics,
};
use datatypes::arrow::error::Result as ArrowResult;
use futures::{Stream, StreamExt};

use crate::extension_plan::Millisecond;
use crate::range_array::RangeArray;

#[derive(Debug)]
pub struct RangeManipulate {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,

    time_index_column: String,
    value_columns: Vec<String>,
    input: LogicalPlan,
    output_schema: DFSchemaRef,
}

impl RangeManipulate {}

impl UserDefinedLogicalNode for RangeManipulate {
    fn as_any(&self) -> &dyn Any {
        self as _
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
            self.start, self.end, self.interval, self.range, self.time_index_column, self.value_columns
        )
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        assert!(!inputs.is_empty());

        Arc::new(Self {
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index_column: self.time_index_column.clone(),
            value_columns: self.value_columns.clone(),
            input: inputs[0].clone(),
            output_schema: self.output_schema.clone(),
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
    value_columns: Vec<String>,

    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    metric: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for RangeManipulateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
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
            interval: self.interval,
            range: self.range,
            time_index_column: self.time_index_column.clone(),
            value_columns: self.value_columns.clone(),
            output_schema: self.output_schema.clone(),
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
            .unwrap_or_else(|| panic!("time index column {} not found", self.time_index_column))
            .0;
        let value_columns = self
            .value_columns
            .iter()
            .map(|value_col| {
                schema
                    .column_with_name(value_col)
                    .unwrap_or_else(|| panic!("value column {value_col} not found",))
                    .0
            })
            .collect();
        Ok(Box::pin(RangeManipulateStream {
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            time_index,
            value_columns,
            output_schema: schema,
            input,
            metric: baseline_metric,
        }))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "PromInstantManipulateExec: req range=[{}..{}], interval=[{}], eval range=[{}], time index=[{}]",
                   self.start, self.end, self.interval, self.range, self.time_index_column
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

pub struct RangeManipulateStream {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,
    time_index: usize,
    value_columns: Vec<usize>,

    output_schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
}

impl RecordBatchStream for RangeManipulateStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for RangeManipulateStream {
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

impl RangeManipulateStream {
    pub fn manipulate(&self, input: RecordBatch) -> ArrowResult<RecordBatch> {
        // calculate the range
        let ranges = self.calculate_range(&input);
        // transform columns
        let mut new_columns = input.columns().to_vec();
        for index in &self.value_columns {
            let column = input.column(*index);
            let new_column = Arc::new(
                RangeArray::from_ranges(column.clone(), ranges.clone())
                    .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?
                    .into_dict(),
            );
            new_columns[*index] = new_column;
        }

        RecordBatch::try_new(self.output_schema.clone(), new_columns)
    }

    fn calculate_range(&self, input: &RecordBatch) -> Vec<(u32, u32)> {
        let ts_column = input
            .column(self.time_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        let mut result = vec![];

        for curr_ts in (self.start..=self.end).step_by(self.interval as _) {
            let mut range_start = ts_column.len();
            let mut range_end = 0;
            for (index, ts) in ts_column.values().iter().enumerate() {
                if ts + self.range >= curr_ts {
                    range_start = range_start.min(index);
                }
                if *ts <= curr_ts {
                    range_end = range_end.max(index);
                }
            }
            result.push((range_start as _, (range_end - range_start + 1) as _));
        }

        result
    }
}
