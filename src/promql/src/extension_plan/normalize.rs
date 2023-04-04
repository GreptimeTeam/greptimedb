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

use datafusion::arrow::array::{BooleanArray, Float64Array};
use datafusion::arrow::compute;
use datafusion::common::{DFSchemaRef, Result as DataFusionResult, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::arrow::array::TimestampMillisecondArray;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::error::Result as ArrowResult;
use datatypes::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};

use crate::extension_plan::Millisecond;

/// Normalize the input record batch. Notice that for simplicity, this method assumes
/// the input batch only contains sample points from one time series.
///
/// Roughly speaking, this method does these things:
/// - bias sample's timestamp by offset
/// - sort the record batch based on timestamp column
/// - remove NaN values
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SeriesNormalize {
    offset: Millisecond,
    time_index_column_name: String,

    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for SeriesNormalize {
    fn name(&self) -> &str {
        "SeriesNormalize"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromSeriesNormalize: offset=[{}], time index=[{}]",
            self.offset, self.time_index_column_name
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert!(!inputs.is_empty());

        Self {
            offset: self.offset,
            time_index_column_name: self.time_index_column_name.clone(),
            input: inputs[0].clone(),
        }
    }
}

impl SeriesNormalize {
    pub fn new<N: AsRef<str>>(
        offset: Millisecond,
        time_index_column_name: N,
        input: LogicalPlan,
    ) -> Self {
        Self {
            offset,
            time_index_column_name: time_index_column_name.as_ref().to_string(),
            input,
        }
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SeriesNormalizeExec {
            offset: self.offset,
            time_index_column_name: self.time_index_column_name.clone(),
            input: exec_input,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }
}

#[derive(Debug)]
pub struct SeriesNormalizeExec {
    offset: Millisecond,
    time_index_column_name: String,

    input: Arc<dyn ExecutionPlan>,
    metric: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for SeriesNormalizeExec {
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(!children.is_empty());
        Ok(Arc::new(Self {
            offset: self.offset,
            time_index_column_name: self.time_index_column_name.clone(),
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
            .column_with_name(&self.time_index_column_name)
            .expect("time index column not found")
            .0;
        Ok(Box::pin(SeriesNormalizeStream {
            offset: self.offset,
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
                    "PromSeriesNormalizeExec: offset=[{}], time index=[{}]",
                    self.offset, self.time_index_column_name
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

pub struct SeriesNormalizeStream {
    offset: Millisecond,
    // Column index of TIME INDEX column's position in schema
    time_index: usize,

    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
}

impl SeriesNormalizeStream {
    pub fn normalize(&self, input: RecordBatch) -> DataFusionResult<RecordBatch> {
        // TODO(ruihang): maybe the input is not timestamp millisecond array
        let ts_column = input
            .column(self.time_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        // bias the timestamp column by offset
        let ts_column_biased = if self.offset == 0 {
            ts_column.clone()
        } else {
            TimestampMillisecondArray::from_iter(
                ts_column.iter().map(|ts| ts.map(|ts| ts - self.offset)),
            )
        };
        let mut columns = input.columns().to_vec();
        columns[self.time_index] = Arc::new(ts_column_biased);

        // sort the record batch
        let ordered_indices = compute::sort_to_indices(&columns[self.time_index], None, None)?;
        let ordered_columns = columns
            .iter()
            .map(|array| compute::take(array, &ordered_indices, None))
            .collect::<ArrowResult<Vec<_>>>()?;
        let ordered_batch = RecordBatch::try_new(input.schema(), ordered_columns)?;

        // TODO(ruihang): consider the "special NaN"
        // filter out NaN
        let mut filter = vec![true; input.num_rows()];
        for column in ordered_batch.columns() {
            if let Some(float_column) = column.as_any().downcast_ref::<Float64Array>() {
                for (i, flag) in filter.iter_mut().enumerate() {
                    if float_column.value(i).is_nan() {
                        *flag = false;
                    }
                }
            }
        }

        let result = compute::filter_record_batch(&ordered_batch, &BooleanArray::from(filter))
            .map_err(DataFusionError::ArrowError)?;
        Ok(result)
    }
}

impl RecordBatchStream for SeriesNormalizeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesNormalizeStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = match self.input.poll_next_unpin(cx) {
            Poll::Ready(batch) => {
                let _timer = self.metric.elapsed_compute().timer();
                Poll::Ready(batch.map(|batch| batch.and_then(|batch| self.normalize(batch))))
            }
            Poll::Pending => Poll::Pending,
        };
        self.metric.record_poll(poll)
    }
}

#[cfg(test)]
mod test {
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::datatypes::{
        ArrowPrimitiveType, DataType, Field, Schema, TimestampMillisecondType,
    };
    use datafusion::from_slice::FromSlice;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;
    use datatypes::arrow::array::TimestampMillisecondArray;
    use datatypes::arrow_array::StringArray;

    use super::*;

    const TIME_INDEX_COLUMN: &str = "timestamp";

    fn prepare_test_data() -> MemoryExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new("value", DataType::Float64, true),
            Field::new("path", DataType::Utf8, true),
        ]));
        let timestamp_column = Arc::new(TimestampMillisecondArray::from_slice([
            60_000, 120_000, 0, 30_000, 90_000,
        ])) as _;
        let field_column = Arc::new(Float64Array::from_slice([0.0, 1.0, 10.0, 100.0, 1000.0])) as _;
        let path_column =
            Arc::new(StringArray::from_slice(["foo", "foo", "foo", "foo", "foo"])) as _;
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![timestamp_column, field_column, path_column],
        )
        .unwrap();

        MemoryExec::try_new(&[vec![data]], schema, None).unwrap()
    }

    #[tokio::test]
    async fn test_sort_record_batch() {
        let memory_exec = Arc::new(prepare_test_data());
        let normalize_exec = Arc::new(SeriesNormalizeExec {
            offset: 0,
            time_index_column_name: TIME_INDEX_COLUMN.to_string(),
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

        let expected = String::from(
            "+---------------------+--------+------+\
            \n| timestamp           | value  | path |\
            \n+---------------------+--------+------+\
            \n| 1970-01-01T00:00:00 | 10.0   | foo  |\
            \n| 1970-01-01T00:00:30 | 100.0  | foo  |\
            \n| 1970-01-01T00:01:00 | 0.0    | foo  |\
            \n| 1970-01-01T00:01:30 | 1000.0 | foo  |\
            \n| 1970-01-01T00:02:00 | 1.0    | foo  |\
            \n+---------------------+--------+------+",
        );

        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn test_offset_record_batch() {
        let memory_exec = Arc::new(prepare_test_data());
        let normalize_exec = Arc::new(SeriesNormalizeExec {
            offset: 1_000, // offset 1s
            time_index_column_name: TIME_INDEX_COLUMN.to_string(),
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

        let expected = String::from(
            "+---------------------+--------+------+\
            \n| timestamp           | value  | path |\
            \n+---------------------+--------+------+\
            \n| 1969-12-31T23:59:59 | 10.0   | foo  |\
            \n| 1970-01-01T00:00:29 | 100.0  | foo  |\
            \n| 1970-01-01T00:00:59 | 0.0    | foo  |\
            \n| 1970-01-01T00:01:29 | 1000.0 | foo  |\
            \n| 1970-01-01T00:01:59 | 1.0    | foo  |\
            \n+---------------------+--------+------+",
        );

        assert_eq!(result_literal, expected);
    }
}
