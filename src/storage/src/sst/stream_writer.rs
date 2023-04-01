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

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use bytes::{BufMut, BytesMut};
use datatypes::schema::SchemaRef;
use object_store::{ObjectStore, Writer};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;
use snafu::ResultExt;

use crate::error;
use crate::error::{NewRecordBatchSnafu, WriteObjectSnafu, WriteParquetSnafu};
use crate::read::Batch;

#[derive(Clone, Default)]
struct Buffer {
    // It's lightweight since writer/flusher never tries to contend this mutex.
    buffer: Arc<Mutex<BytesMut>>,
}

impl Buffer {
    pub fn with_capacity(size: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(BytesMut::with_capacity(size))),
        }
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        let mut buffer = self.buffer.lock().unwrap();
        buffer.put_slice(buf);
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // This flush implementation is intentionally left to blank.
        // The actual flush is in `BufferedWriter::try_flush`
        Ok(())
    }
}

/// Parquet writer that buffers row groups in memory and writes buffered data to an underlying
/// storage by chunks to reduce memory consumption.
pub struct BufferedWriter {
    path: String,
    arrow_writer: ArrowWriter<Buffer>,
    object_writer: Writer,
    buffer: Buffer,
    bytes_written: AtomicU64,
    flushed: bool,
    threshold: usize,
    arrow_schema: arrow::datatypes::SchemaRef,
}

impl BufferedWriter {
    pub async fn try_new(
        path: String,
        store: ObjectStore,
        schema: &SchemaRef,
        props: Option<WriterProperties>,
        buffer_threshold: usize,
    ) -> error::Result<Self> {
        let arrow_schema = schema.arrow_schema();
        let buffer = Buffer::with_capacity(buffer_threshold);
        let writer = store
            .writer(&path)
            .await
            .context(WriteObjectSnafu { path: &path })?;

        let arrow_writer = ArrowWriter::try_new(buffer.clone(), arrow_schema.clone(), props)
            .context(WriteParquetSnafu)?;

        Ok(Self {
            path,
            arrow_writer,
            object_writer: writer,
            buffer,
            bytes_written: Default::default(),
            flushed: false,
            threshold: buffer_threshold,
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
        self.arrow_writer
            .write(&arrow_batch)
            .context(WriteParquetSnafu)?;
        let written = Self::try_flush(
            &self.path,
            &self.buffer,
            &mut self.object_writer,
            false,
            &mut self.flushed,
            self.threshold,
        )
        .await?;
        self.bytes_written.fetch_add(written, Ordering::Relaxed);
        Ok(())
    }

    /// Abort writer.
    pub async fn abort(self) -> bool {
        // TODO(hl): Currently we can do nothing if file's parts have been uploaded to remote storage
        // on abortion, we need to find a way to abort the upload. see https://help.aliyun.com/document_detail/31996.htm?spm=a2c4g.11186623.0.0.3eb42cb7b2mwUz#reference-txp-bvx-wdb
        !self.flushed
    }

    /// Close parquet writer and ensure all buffered data are written into underlying storage.
    pub async fn close(mut self) -> error::Result<(FileMetaData, u64)> {
        let metadata = self.arrow_writer.close().context(WriteParquetSnafu)?;
        let written = Self::try_flush(
            &self.path,
            &self.buffer,
            &mut self.object_writer,
            true,
            &mut self.flushed,
            self.threshold,
        )
        .await?;
        self.bytes_written.fetch_add(written, Ordering::Relaxed);
        self.object_writer
            .close()
            .await
            .context(WriteObjectSnafu { path: &self.path })?;
        Ok((metadata, self.bytes_written.load(Ordering::Relaxed)))
    }

    /// Try to flush buffered data to underlying storage if it's size exceeds threshold.
    /// Set `all` to true if all buffered data should be flushed regardless of it's size.
    async fn try_flush(
        file_name: &str,
        shared_buffer: &Buffer,
        object_writer: &mut Writer,
        all: bool,
        flushed: &mut bool,
        threshold: usize,
    ) -> error::Result<u64> {
        let mut bytes_written = 0;

        // Once buffered data size reaches threshold, split the data in chunks (typically 4MB)
        // and write to underlying storage.
        while shared_buffer.buffer.lock().unwrap().len() >= threshold {
            let chunk = {
                let mut buffer = shared_buffer.buffer.lock().unwrap();
                buffer.split_to(threshold)
            };
            let size = chunk.len();
            object_writer
                .append(chunk)
                .await
                .context(WriteObjectSnafu { path: file_name })?;
            *flushed = true;
            bytes_written += size;
        }

        if all {
            let remain = shared_buffer.buffer.lock().unwrap().split();
            let size = remain.len();
            object_writer
                .append(remain)
                .await
                .context(WriteObjectSnafu { path: file_name })?;
            *flushed = true;
            bytes_written += size;
        }
        Ok(bytes_written as u64)
    }
}
