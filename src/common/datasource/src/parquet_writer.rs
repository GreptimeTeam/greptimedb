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

use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::Writer;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::ParquetError;

/// Bridges opendal [Writer] with parquet [AsyncFileWriter].
pub struct AsyncWriter {
    inner: Writer,
}

impl AsyncWriter {
    /// Create a [`AsyncWriter`] by given [`Writer`].
    pub fn new(writer: Writer) -> Self {
        Self { inner: writer }
    }
}

impl AsyncFileWriter for AsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.inner
                .write(bs)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.inner
                .close()
                .await
                .map(|_| ())
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }
}
