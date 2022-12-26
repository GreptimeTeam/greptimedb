use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use datafusion::arrow::compute;
use datafusion::common::{DFSchemaRef, Statistics};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::arrow::array::{ArrowPrimitiveType, PrimitiveArray, TimestampMillisecondArray};
use datatypes::arrow::datatypes::{SchemaRef, TimestampMillisecondType};
use datatypes::arrow::error::Result as ArrowResult;
use datatypes::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};

type Millisecond = <TimestampMillisecondType as ArrowPrimitiveType>::Native;

const TIMESTAMP_COLUMN_NAME: &str = "timestamp";

/// Normalize the input record batch. Notice that for simplicity, this method assumes
/// the input batch only contains sample points from one time series, and the value
/// column's name is [`VALUE_COLUMN_NAME`] and timestamp column's name is [`TIMESTAMP_COLUMN_NAME`].
///
/// Roughly speaking, this method does these things:
/// - bias sample's timestamp by offset
/// - sort the record batch based on timestamp column
#[derive(Debug)]
pub struct SeriesNormalize {
    offset: Millisecond,

    input: LogicalPlan,
}

impl UserDefinedLogicalNode for SeriesNormalize {
    fn as_any(&self) -> &dyn Any {
        self as _
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
        write!(f, "PromSeriesNormalize: offset=[{}]", self.offset)
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::logical_expr::Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        assert!(!inputs.is_empty());

        Arc::new(Self {
            offset: self.offset,
            input: inputs[0].clone(),
        })
    }
}

impl SeriesNormalize {
    pub fn new(offset: Duration, input: LogicalPlan) -> Self {
        Self {
            offset: offset.as_millis() as i64,
            input,
        }
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SeriesNormalizeExec {
            offset: self.offset,
            input: exec_input,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }
}

#[derive(Debug)]
pub struct SeriesNormalizeExec {
    offset: Millisecond,

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

    fn maintains_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        assert!(!children.is_empty());
        Ok(Arc::new(Self {
            offset: self.offset,
            input: children[0].clone(),
            metric: self.metric.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let baseline_metric = BaselineMetrics::new(&self.metric, partition);

        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SeriesNormalizeStream {
            offset: self.offset,
            schema: input.schema(),
            input,
            metric: baseline_metric,
        }))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "PromSeriesNormalizeExec: offset=[{}]", self.offset)
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        todo!()
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

pub struct SeriesNormalizeStream {
    offset: Millisecond,

    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
}

impl SeriesNormalizeStream {
    pub fn normalize(&self, input: RecordBatch) -> ArrowResult<RecordBatch> {
        let ts_column_idx = self
            .schema
            .column_with_name(TIMESTAMP_COLUMN_NAME)
            .expect("timestamp column not found")
            .0;
        // todo: maybe the input is not timestamp millisecond array
        let ts_column = input
            .column(ts_column_idx)
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
            .unwrap();

        // bias the timestamp column by offset
        let ts_column_biased = Arc::new(TimestampMillisecondArray::from_iter(
            ts_column.iter().map(|ts| ts.map(|ts| ts - self.offset)),
        ));
        let mut columns = input.columns().to_vec();
        columns[ts_column_idx] = ts_column_biased;

        // sort the record batch
        let ordered_indices = compute::sort_to_indices(&columns[ts_column_idx], None, None)?;
        let ordered_columns = columns
            .iter()
            .map(|array| compute::take(array, &ordered_indices, None))
            .collect::<ArrowResult<Vec<_>>>()?;
        RecordBatch::try_new(input.schema(), ordered_columns)
    }
}

impl RecordBatchStream for SeriesNormalizeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesNormalizeStream {
    type Item = ArrowResult<RecordBatch>;

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
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::from_slice::FromSlice;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;
    use datatypes::arrow::array::TimestampMillisecondArray;
    use datatypes::arrow_array::StringArray;

    use super::*;

    fn prepare_test_data() -> MemoryExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIMESTAMP_COLUMN_NAME,
                TimestampMillisecondType::DATA_TYPE,
                true,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("path", DataType::Utf8, true),
        ]));
        let timestamp_column = Arc::new(TimestampMillisecondArray::from_slice([
            60_000, 120_000, 0, 30_000, 90_000,
        ])) as _;
        let value_column = Arc::new(Float64Array::from_slice([0.0, 1.0, 10.0, 100.0, 1000.0])) as _;
        let path_column =
            Arc::new(StringArray::from_slice(["foo", "foo", "foo", "foo", "foo"])) as _;
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![timestamp_column, value_column, path_column],
        )
        .unwrap();

        MemoryExec::try_new(&[vec![data]], schema, None).unwrap()
    }

    #[tokio::test]
    async fn test_sort_record_batch() {
        let memory_exec = Arc::new(prepare_test_data());
        let normalize_exec = Arc::new(SeriesNormalizeExec {
            offset: 0,
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
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1970-01-01T00:00:00 | 10    | foo  |\
            \n| 1970-01-01T00:00:30 | 100   | foo  |\
            \n| 1970-01-01T00:01:00 | 0     | foo  |\
            \n| 1970-01-01T00:01:30 | 1000  | foo  |\
            \n| 1970-01-01T00:02:00 | 1     | foo  |\
            \n+---------------------+-------+------+",
        );

        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn test_offset_record_batch() {
        let memory_exec = Arc::new(prepare_test_data());
        let normalize_exec = Arc::new(SeriesNormalizeExec {
            offset: 1_000, // offset 1s
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
            "+---------------------+-------+------+\
            \n| timestamp           | value | path |\
            \n+---------------------+-------+------+\
            \n| 1969-12-31T23:59:59 | 10    | foo  |\
            \n| 1970-01-01T00:00:29 | 100   | foo  |\
            \n| 1970-01-01T00:00:59 | 0     | foo  |\
            \n| 1970-01-01T00:01:29 | 1000  | foo  |\
            \n| 1970-01-01T00:01:59 | 1     | foo  |\
            \n+---------------------+-------+------+",
        );

        assert_eq!(result_literal, expected);
    }
}
