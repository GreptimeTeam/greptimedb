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
use bytes::Buf;
use datafusion::parquet::format::FileMetaData;
use object_store::Writer;
use snafu::ResultExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::compat::Compat;

use crate::error::{self, Result};
use crate::share_buffer::SharedBuffer;

pub trait DfRecordBatchBuffer: Send {
    fn write(&mut self, batch: RecordBatch) -> Result<()>;

    fn consume_all(&mut self) -> bytes::Bytes;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct ApproximateBufWriter<B, T> {
    threshold: usize,
    buffer: B,
    writer: T,
}

impl<B: DfRecordBatchBuffer, T: AsyncWrite + Send + Unpin> ApproximateBufWriter<B, T> {
    /// A Buffer keeps an in-memory buffer of encoded data and writes it to an underlying
    /// writer in large, infrequent batches.
    ///
    ///  There is no guarantee of the actual written batches size; it only guarantees
    ///  the batches size larger than the `threshold`.
    pub fn new(threshold: usize, buffer: B, writer: T) -> Self {
        Self {
            threshold,
            buffer,
            writer,
        }
    }

    /// Writes batch to inner buffer, flush the buffer when buffer len exceeds the `threshold`.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.write_to_buf(batch)?;

        self.try_flush_all().await
    }

    fn write_to_buf(&mut self, batch: RecordBatch) -> Result<()> {
        self.buffer.write(batch)?;

        Ok(())
    }

    async fn try_flush_all(&mut self) -> Result<()> {
        if self.buffer.len() > self.threshold {
            self.flush_inner().await?;
        }

        Ok(())
    }

    async fn flush_inner(&mut self) -> Result<()> {
        self.writer
            .write_all(self.buffer.consume_all().chunk())
            .await
            .context(error::AsyncWriteSnafu)?;

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.flush_inner().await
    }
}

pub struct BufferedWriter<T, U> {
    writer: T,
    /// None stands for [`BufferedWriter`] closed.
    encoder: Option<U>,
    buffer: SharedBuffer,
    bytes_written: u64,
    flushed: bool,
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
    pub async fn close(mut self) -> Result<(FileMetaData, u64)> {
        if let Some(encoder) = self.encoder.take() {
            let metadata = encoder.close().await?;
            let written = self.try_flush_all().await?;
            return Ok((metadata, written));
        }
        error::BufferedWriterClosedSnafu {}.fail()
    }
}

impl<T: AsyncWrite + Send + Unpin, U: DfRecordBatchEncoder> BufferedWriter<T, U> {
    pub fn new(threshold: usize, buffer: SharedBuffer, encoder: U, writer: T) -> Self {
        Self {
            threshold,
            writer,
            encoder: Some(encoder),
            buffer,
            bytes_written: 0,
            flushed: false,
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if let Some(encoder) = &mut self.encoder {
            encoder.write(batch)?;

            self.try_flush(false).await?;

            return Ok(());
        }
        error::BufferedWriterClosedSnafu {}.fail()
    }

    pub fn flushed(&self) -> bool {
        self.flushed
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

            self.flushed = true;
            bytes_written += size as u64;
        }

        if all {
            bytes_written += self.try_flush_all().await?;
        }

        self.bytes_written += bytes_written;

        Ok(bytes_written)
    }

    pub async fn try_flush_all(&mut self) -> Result<u64> {
        let remain = self.buffer.buffer.lock().unwrap().split();
        let size = remain.len();

        self.writer
            .write_all(&remain)
            .await
            .context(error::AsyncWriteSnafu)?;
        self.flushed = true;

        self.bytes_written += size as u64;

        Ok(size as u64)
    }
}
