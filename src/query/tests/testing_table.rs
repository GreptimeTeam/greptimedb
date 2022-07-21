use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use common_query::prelude::Expr;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datatypes::prelude::VectorRef;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use futures::task::{Context, Poll};
use futures::Stream;
use table::error::Result;
use table::Table;

#[derive(Debug, Clone)]
pub struct TestingTable {
    records: RecordBatch,
}

impl TestingTable {
    pub fn new(column_name: &str, values: VectorRef) -> Self {
        let column_schemas = vec![ColumnSchema::new(column_name, values.data_type(), false)];
        let schema = Arc::new(Schema::new(column_schemas));
        Self {
            records: RecordBatch::new(schema, vec![values]).unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl Table for TestingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.records.schema.clone()
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(TestingRecordsStream {
            schema: self.records.schema.clone(),
            records: Some(self.records.clone()),
        }))
    }
}

impl RecordBatchStream for TestingRecordsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct TestingRecordsStream {
    schema: SchemaRef,
    records: Option<RecordBatch>,
}

impl Stream for TestingRecordsStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.records.take() {
            Some(records) => Poll::Ready(Some(Ok(records))),
            None => Poll::Ready(None),
        }
    }
}
