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

use datafusion::arrow::array::Array;
use datafusion::common::{DFSchemaRef, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{LexRequirement, PhysicalSortRequirement};
use datafusion::physical_plan::expressions::Column as ColumnExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datatypes::arrow::array::{Float64Array, TimestampMillisecondArray};
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::compute::SortOptions;
use futures::{ready, Stream, StreamExt};

use crate::extension_plan::Millisecond;

/// Maximum number of rows per output batch
const ABSENT_BATCH_SIZE: usize = 8192;

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct Absent {
    start: Millisecond,
    end: Millisecond,
    step: Millisecond,
    time_index_column: String,
    value_column: String,
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for Absent {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        // For the absent function, we create a schema with timestamp and value columns
        // This is a simplified approach - in a real implementation you might want to cache this
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromAbsent: start={}, end={}, step={}",
            self.start, self.end, self.step
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "Absent must have at least one input".to_string(),
            ));
        }

        Ok(Self {
            start: self.start,
            end: self.end,
            step: self.step,
            time_index_column: self.time_index_column.clone(),
            value_column: self.value_column.clone(),
            input: inputs[0].clone(),
        })
    }
}

impl Absent {
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        step: Millisecond,
        time_index_column: String,
        value_column: String,
        input: LogicalPlan,
    ) -> Self {
        Self {
            start,
            end,
            step,
            time_index_column,
            value_column,
            input,
        }
    }

    pub const fn name() -> &'static str {
        "prom_absent"
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let output_schema = Arc::new(Schema::new(vec![
            Field::new(
                &self.time_index_column,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(&self.value_column, DataType::Float64, true),
        ]));

        Arc::new(AbsentExec {
            start: self.start,
            end: self.end,
            step: self.step,
            time_index_column: self.time_index_column.clone(),
            value_column: self.value_column.clone(),
            output_schema,
            input: exec_input,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }

    // TODO: Implement serialize/deserialize when protobuf definitions are added
    // pub fn serialize(&self) -> Vec<u8>
    // pub fn deserialize(bytes: &[u8]) -> Result<Self>
}

#[derive(Debug)]
pub struct AbsentExec {
    start: Millisecond,
    end: Millisecond,
    step: Millisecond,
    time_index_column: String,
    value_column: String,
    output_schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    metric: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for AbsentExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<datafusion::physical_expr::LexRequirement>> {
        vec![Some(LexRequirement::new(vec![PhysicalSortRequirement {
            expr: Arc::new(
                ColumnExpr::new_with_schema(&self.time_index_column, &self.input.schema()).unwrap(),
            ),
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        }]))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
            step: self.step,
            time_index_column: self.time_index_column.clone(),
            value_column: self.value_column.clone(),
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

        Ok(Box::pin(AbsentStream {
            end: self.end,
            step: self.step,
            time_index_column_index: self
                .input
                .schema()
                .column_with_name(&self.time_index_column)
                .unwrap()
                .0,
            output_schema: self.output_schema.clone(),
            input,
            metric: baseline_metric,
            // Buffer for streaming output timestamps
            output_timestamps: Vec::new(),
            // Current timestamp in the output range
            output_ts_cursor: self.start,
            input_finished: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn name(&self) -> &str {
        "AbsentExec"
    }
}

impl DisplayAs for AbsentExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "PromAbsentExec: start={}, end={}, step={}",
                    self.start, self.end, self.step
                )
            }
        }
    }
}

pub struct AbsentStream {
    end: Millisecond,
    step: Millisecond,
    time_index_column_index: usize,
    output_schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
    // Buffer for streaming output timestamps
    output_timestamps: Vec<Millisecond>,
    // Current timestamp in the output range
    output_ts_cursor: Millisecond,
    input_finished: bool,
}

impl RecordBatchStream for AbsentStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for AbsentStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if !self.input_finished {
                match ready!(self.input.poll_next_unpin(cx)) {
                    Some(Ok(batch)) => {
                        let timer = std::time::Instant::now();
                        if let Err(e) = self.process_input_batch(&batch) {
                            return Poll::Ready(Some(Err(e)));
                        }
                        self.metric.elapsed_compute().add_elapsed(timer);

                        // If we have enough data for a batch, output it
                        if self.output_timestamps.len() >= ABSENT_BATCH_SIZE {
                            let timer = std::time::Instant::now();
                            let result = self.flush_output_batch();
                            self.metric.elapsed_compute().add_elapsed(timer);

                            match result {
                                Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                                Ok(None) => continue,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                        }
                    }
                    Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    None => {
                        self.input_finished = true;

                        let timer = std::time::Instant::now();
                        // Process any remaining absent timestamps
                        if let Err(e) = self.process_remaining_absent_timestamps() {
                            return Poll::Ready(Some(Err(e)));
                        }
                        let result = self.flush_output_batch();
                        self.metric.elapsed_compute().add_elapsed(timer);
                        return Poll::Ready(result.transpose());
                    }
                }
            } else {
                return Poll::Ready(None);
            }
        }
    }
}

impl AbsentStream {
    fn process_input_batch(&mut self, batch: &RecordBatch) -> DataFusionResult<()> {
        // Extract timestamps from this batch
        let timestamp_array = batch
            .column(self.time_index_column_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        // Process against current output cursor position
        for &input_ts in timestamp_array.values() {
            // Generate absent timestamps up to this input timestamp
            while self.output_ts_cursor < input_ts && self.output_ts_cursor <= self.end {
                self.output_timestamps.push(self.output_ts_cursor);
                self.output_ts_cursor += self.step;
            }

            // Skip the input timestamp if it matches our cursor
            if self.output_ts_cursor == input_ts {
                self.output_ts_cursor += self.step;
            }
        }

        Ok(())
    }

    fn process_remaining_absent_timestamps(&mut self) -> DataFusionResult<()> {
        // Generate all remaining absent timestamps (input is finished)
        while self.output_ts_cursor <= self.end {
            self.output_timestamps.push(self.output_ts_cursor);
            self.output_ts_cursor += self.step;
        }
        Ok(())
    }

    fn flush_output_batch(&mut self) -> DataFusionResult<Option<RecordBatch>> {
        if self.output_timestamps.is_empty() {
            return Ok(None);
        }

        let timestamp_array = Arc::new(TimestampMillisecondArray::from(
            self.output_timestamps.clone(),
        ));
        let value_array = Arc::new(Float64Array::from(vec![1.0; self.output_timestamps.len()]));

        let batch = RecordBatch::try_new(
            self.output_schema.clone(),
            vec![timestamp_array, value_array],
        )?;

        self.output_timestamps.clear();
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;
    use datatypes::arrow::array::{Float64Array, TimestampMillisecondArray};

    use super::*;

    #[tokio::test]
    async fn test_absent_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("value", DataType::Float64, true),
        ]));

        // Input has timestamps: 0, 2000, 4000
        let timestamp_array = Arc::new(TimestampMillisecondArray::from(vec![0, 2000, 4000]));
        let value_array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![timestamp_array, value_array]).unwrap();

        let memory_exec = MemoryExec::try_new(&[vec![batch]], schema, None).unwrap();

        let absent_exec = AbsentExec {
            start: 0,
            end: 5000,
            step: 1000,
            time_index_column: "timestamp".to_string(),
            value_column: "value".to_string(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new("value", DataType::Float64, true),
            ])),
            input: Arc::new(memory_exec),
            metric: ExecutionPlanMetricsSet::new(),
        };

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = absent_exec.execute(0, task_ctx).unwrap();

        // Collect all output batches
        let mut output_timestamps = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            let ts_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            for i in 0..ts_array.len() {
                if !ts_array.is_null(i) {
                    let ts = ts_array.value(i);
                    output_timestamps.push(ts);
                }
            }
        }

        // Should output absent timestamps: 1000, 3000, 5000
        // (0, 2000, 4000 exist in input, so 1000, 3000, 5000 are absent)
        assert_eq!(output_timestamps, vec![1000, 3000, 5000]);
    }

    #[tokio::test]
    async fn test_absent_empty_input() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("value", DataType::Float64, true),
        ]));

        // Empty input
        let memory_exec = MemoryExec::try_new(&[vec![]], schema, None).unwrap();

        let absent_exec = AbsentExec {
            start: 0,
            end: 2000,
            step: 1000,
            time_index_column: "timestamp".to_string(),
            value_column: "value".to_string(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new("value", DataType::Float64, true),
            ])),
            input: Arc::new(memory_exec),
            metric: ExecutionPlanMetricsSet::new(),
        };

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = absent_exec.execute(0, task_ctx).unwrap();

        // Collect all output timestamps
        let mut output_timestamps = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            let ts_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            for i in 0..ts_array.len() {
                if !ts_array.is_null(i) {
                    let ts = ts_array.value(i);
                    output_timestamps.push(ts);
                }
            }
        }

        // Should output all timestamps in range: 0, 1000, 2000
        assert_eq!(output_timestamps, vec![0, 1000, 2000]);
    }
}
