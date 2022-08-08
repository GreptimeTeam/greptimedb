#[cfg(test)]
pub mod test_util;

use std::any::Any;
use std::pin::Pin;

use async_trait::async_trait;
use common_query::logical_plan::Expr;
use common_recordbatch::error::{Error as RecordBatchError, Result as RecordBatchResult};
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use futures::task::{Context, Poll};
use futures::Stream;
use snafu::OptionExt;
use store_api::storage::{
    ChunkReader, PutOperation, ReadContext, Region, ScanRequest, SchemaRef, Snapshot, WriteContext,
    WriteRequest,
};
use table::error::{Error as TableError, MissingColumnSnafu, Result as TableResult};
use table::requests::InsertRequest;
use table::{
    metadata::{TableInfo, TableType},
    table::Table,
};

/// [Table] implementation.
pub struct MitoTable<R: Region> {
    table_info: TableInfo,
    //TODO(dennis): a table contains multi regions
    region: R,
}

#[async_trait]
impl<R: Region> Table for MitoTable<R> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_info.meta.schema.clone()
    }

    async fn insert(&self, request: InsertRequest) -> TableResult<usize> {
        if request.columns_values.is_empty() {
            return Ok(0);
        }

        let mut write_request = self.region.write_request(self.schema());

        let mut put_op = write_request.put_op();
        let mut columns_values = request.columns_values;
        let key_columns = self.table_info.meta.row_key_column_names();
        let value_columns = self.table_info.meta.value_column_names();

        //Add row key and columns
        for name in key_columns {
            put_op
                .add_key_column(
                    name,
                    columns_values
                        .get(name)
                        .context(MissingColumnSnafu { name })?
                        .clone(),
                )
                .map_err(TableError::new)?;
        }

        // Add vaue columns
        let mut rows_num = 0;
        for name in value_columns {
            if let Some(v) = columns_values.remove(name) {
                rows_num = v.len();
                put_op.add_value_column(name, v).map_err(TableError::new)?;
            }
        }
        write_request.put(put_op).map_err(TableError::new)?;

        let _resp = self
            .region
            .write(&WriteContext::default(), write_request)
            .await
            .map_err(TableError::new)?;

        Ok(rows_num)
    }

    fn table_type(&self) -> TableType {
        self.table_info.table_type
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> TableResult<SendableRecordBatchStream> {
        let read_ctx = ReadContext::default();
        let snapshot = self.region.snapshot(&read_ctx).map_err(TableError::new)?;

        let mut reader = snapshot
            .scan(&read_ctx, ScanRequest::default())
            .await
            .map_err(TableError::new)?
            .reader;

        let schema = reader.schema().clone();
        let stream_schema = schema.clone();

        let stream = Box::pin(async_stream::try_stream! {

            for chunk in reader.next_chunk()
                .await
                .map_err(RecordBatchError::new)?
            {
                yield RecordBatch::new(stream_schema.clone(), chunk.columns)?
            }
        });

        Ok(Box::pin(ChunkStream { schema, stream }))
    }
}

struct ChunkStream {
    schema: SchemaRef,
    stream: Pin<Box<dyn Stream<Item = RecordBatchResult<RecordBatch>> + Send>>,
}

impl RecordBatchStream for ChunkStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for ChunkStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(ctx)
    }
}

impl<R: Region> MitoTable<R> {
    pub fn new(table_info: TableInfo, region: R) -> Self {
        Self { table_info, region }
    }
}
