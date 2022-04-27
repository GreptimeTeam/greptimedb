use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::UInt32Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datafusion::field_util::SchemaExt;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::schema::{Schema, SchemaRef};
use futures::task::{Context, Poll};
use futures::Stream;

use crate::error::Result;
use crate::table::{Expr, Table};

/// numbers table for test
pub struct NumbersTable {
    schema: SchemaRef,
}

impl Default for NumbersTable {
    fn default() -> Self {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "number",
            DataType::UInt32,
            false,
        )]));
        Self {
            schema: Arc::new(Schema::new(arrow_schema)),
        }
    }
}

#[async_trait::async_trait]
impl Table for NumbersTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(NumbersStream {
            limit: limit.unwrap_or(100) as u32,
            schema: self.schema.clone(),
            already_run: false,
        }))
    }
}

// Limited numbers stream
struct NumbersStream {
    limit: u32,
    schema: SchemaRef,
    already_run: bool,
}

impl RecordBatchStream for NumbersStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for NumbersStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.already_run {
            return Poll::Ready(None);
        }
        self.already_run = true;
        let numbers: Vec<u32> = (0..self.limit).collect();
        let batch = DfRecordBatch::try_new(
            self.schema.arrow_schema().clone(),
            vec![Arc::new(UInt32Array::from_slice(&numbers))],
        )
        .unwrap();

        Poll::Ready(Some(Ok(RecordBatch {
            schema: self.schema.clone(),
            df_recordbatch: batch,
        })))
    }
}
