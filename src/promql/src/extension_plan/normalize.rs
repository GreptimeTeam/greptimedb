use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use datafusion::arrow::compute;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
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
        &self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        todo!()
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::logical_expr::Expr],
        _inputs: &[LogicalPlan],
    ) -> std::sync::Arc<dyn UserDefinedLogicalNode> {
        todo!()
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
        })
    }
}

#[derive(Debug)]
pub struct SeriesNormalizeExec {
    offset: Millisecond,

    input: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for SeriesNormalizeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SeriesNormalizeStream {
            offset: self.offset,
            schema: input.schema(),
            input,
        }))
    }

    fn statistics(&self) -> datafusion::common::Statistics {
        todo!()
    }
}

pub struct SeriesNormalizeStream {
    offset: Millisecond,

    schema: SchemaRef,
    input: SendableRecordBatchStream,
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
            .downcast_ref::<Arc<PrimitiveArray<TimestampMillisecondType>>>()
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
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(batch) => {
                Poll::Ready(batch.map(|batch| batch.map(|batch| self.normalize(batch)).flatten()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
