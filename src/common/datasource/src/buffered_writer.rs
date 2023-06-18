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

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::parquet::format::FileMetaData;
use object_store::Writer;
use snafu::{OptionExt, ResultExt};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::compat::Compat;

use crate::error::{self, Result};
use crate::share_buffer::SharedBuffer;

pub struct BufferedWriter<T, U> {
    writer: T,
    /// None stands for [`BufferedWriter`] closed.
    encoder: Option<U>,
    buffer: SharedBuffer,
    rows_written: usize,
    bytes_written: u64,
    threshold: usize,
}

pub trait DfRecordBatchEncoder {
    fn write(&mut self, batch: &RecordBatch) -> Result<()>;
}

#[async_trait]
pub trait ArrowWriterCloser {
    async fn close(mut self) -> Result<FileMetaData>;
}

pub type DefaultBufferedWriter<E> = BufferedWriter<Compat<Writer>, E>;

impl<T: AsyncWrite + Send + Unpin, U: DfRecordBatchEncoder + ArrowWriterCloser>
    BufferedWriter<T, U>
{
    pub async fn close_with_arrow_writer(mut self) -> Result<(FileMetaData, u64)> {
        let encoder = self
            .encoder
            .take()
            .context(error::BufferedWriterClosedSnafu)?;
        let metadata = encoder.close().await?;
        if self.rows_written != 0 {
            self.bytes_written += self.try_flush(true).await?;
        } else {
        }
        // It's important to shut down! flushes all pending writes
        self.close().await?;
        Ok((metadata, self.bytes_written))
    }
}

impl<T: AsyncWrite + Send + Unpin, U: DfRecordBatchEncoder> BufferedWriter<T, U> {
    pub async fn close(&mut self) -> Result<()> {
        self.writer.shutdown().await.context(error::AsyncWriteSnafu)
    }

    pub fn new(threshold: usize, buffer: SharedBuffer, encoder: U, writer: T) -> Self {
        Self {
            threshold,
            writer,
            encoder: Some(encoder),
            buffer,
            rows_written: 0,
            bytes_written: 0,
        }
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let encoder = self
            .encoder
            .as_mut()
            .context(error::BufferedWriterClosedSnafu)?;
        encoder.write(batch)?;
        self.rows_written += batch.num_rows();
        self.bytes_written += self.try_flush(false).await?;
        Ok(())
    }

    pub async fn try_flush(&mut self, all: bool) -> Result<u64> {
        let mut bytes_written: u64 = 0;

        // Once buffered data size reaches threshold, split the data in chunks (typically 4MB)
        // and write to underlying storage.
        while self.buffer.buffer.lock().unwrap().len() >= self.threshold {
            let chunk = {
                let mut buffer = self.buffer.buffer.lock().unwrap();
                buffer.split_to(self.threshold)
            };
            let size = chunk.len();

            self.writer
                .write_all(&chunk)
                .await
                .context(error::AsyncWriteSnafu)?;

            bytes_written += size as u64;
        }

        if all {
            bytes_written += self.try_flush_all().await?;
        }
        Ok(bytes_written)
    }

    async fn try_flush_all(&mut self) -> Result<u64> {
        let remain = self.buffer.buffer.lock().unwrap().split();
        let size = remain.len();

        self.writer
            .write_all(&remain)
            .await
            .context(error::AsyncWriteSnafu)?;

        Ok(size as u64)
    }
}
