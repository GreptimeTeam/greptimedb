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
use bytes::Buf;
use snafu::ResultExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::{self, Result};

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
