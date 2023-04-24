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

use arrow_array::RecordBatch;
use common_datasource::buffered_writer::{
    BufferedWriter as DatasourceBufferedWriter, DefaultBufferedWriter,
};
use common_datasource::share_buffer::SharedBuffer;
use datatypes::schema::SchemaRef;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;
use snafu::ResultExt;
use tokio_util::compat::FuturesAsyncWriteCompatExt;

use crate::error;
use crate::error::{NewRecordBatchSnafu, WriteObjectSnafu, WriteParquetSnafu};
use crate::read::Batch;
/// Parquet writer that buffers row groups in memory and writes buffered data to an underlying
/// storage by chunks to reduce memory consumption.
pub struct BufferedWriter {
    inner: Box<InnerBufferedWriter>,
    arrow_schema: arrow::datatypes::SchemaRef,
}

type InnerBufferedWriter = DefaultBufferedWriter<ArrowWriter<SharedBuffer>>;

impl BufferedWriter {
    pub async fn try_new(
        path: String,
        store: ObjectStore,
        schema: &SchemaRef,
        props: Option<WriterProperties>,
        buffer_threshold: usize,
    ) -> error::Result<Self> {
        let arrow_schema = schema.arrow_schema();
        let buffer = SharedBuffer::with_capacity(buffer_threshold);
        let writer = store
            .writer(&path)
            .await
            .context(WriteObjectSnafu { path: &path })?;

        let arrow_writer = ArrowWriter::try_new(buffer.clone(), arrow_schema.clone(), props)
            .context(WriteParquetSnafu)?;

        let writer = writer.compat_write();

        Ok(Self {
            inner: Box::new(DatasourceBufferedWriter::new(
                buffer_threshold,
                buffer.clone(),
                arrow_writer,
                writer,
            )),
            arrow_schema: arrow_schema.clone(),
        })
    }

    /// Write a record batch to stream writer.
    pub async fn write(&mut self, batch: &Batch) -> error::Result<()> {
        let arrow_batch = RecordBatch::try_new(
            self.arrow_schema.clone(),
            batch
                .columns()
                .iter()
                .map(|v| v.to_arrow_array())
                .collect::<Vec<_>>(),
        )
        .context(NewRecordBatchSnafu)?;

        self.inner
            .write(&arrow_batch)
            .await
            .context(error::WriteBufferSnafu)?;
        self.inner
            .try_flush(false)
            .await
            .context(error::WriteBufferSnafu)?;

        Ok(())
    }

    /// Abort writer.
    pub async fn abort(self) -> bool {
        // TODO(hl): Currently we can do nothing if file's parts have been uploaded to remote storage
        // on abortion, we need to find a way to abort the upload. see https://help.aliyun.com/document_detail/31996.htm?spm=a2c4g.11186623.0.0.3eb42cb7b2mwUz#reference-txp-bvx-wdb
        !self.inner.flushed()
    }

    /// Close parquet writer and ensure all buffered data are written into underlying storage.
    pub async fn close(self) -> error::Result<(FileMetaData, u64)> {
        self.inner.close().await.context(error::WriteBufferSnafu)
    }
}
