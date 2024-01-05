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

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{AsyncRead, AsyncSeek, AsyncWrite};
use object_store::ObjectStore;
use pin_project::pin_project;
use prometheus::IntCounter;
use snafu::ResultExt;

use crate::error::{OpenDalSnafu, Result};

/// A wrapper around [`ObjectStore`] that adds instrumentation for monitoring
/// metrics such as bytes read, bytes written, and the number of seek operations.
#[derive(Clone)]
pub(crate) struct InstrumentedStore {
    /// The underlying object store.
    object_store: ObjectStore,
}

impl InstrumentedStore {
    /// Create a new `InstrumentedStore`.
    pub fn new(object_store: ObjectStore) -> Self {
        Self { object_store }
    }

    /// Returns an [`InstrumentedAsyncRead`] for the given path.
    /// Metrics like the number of bytes read, read and seek operations
    /// are recorded using the provided `IntCounter`s.
    pub async fn reader<'a>(
        &self,
        path: &str,
        read_byte_count: &'a IntCounter,
        read_count: &'a IntCounter,
        seek_count: &'a IntCounter,
    ) -> Result<InstrumentedAsyncRead<'a, object_store::Reader>> {
        let reader = self.object_store.reader(path).await.context(OpenDalSnafu)?;
        Ok(InstrumentedAsyncRead::new(
            reader,
            read_byte_count,
            read_count,
            seek_count,
        ))
    }

    /// Returns an [`InstrumentedAsyncWrite`] for the given path.
    /// Metrics like the number of bytes written, write and flush operations
    /// are recorded using the provided `IntCounter`s.
    pub async fn writer<'a>(
        &self,
        path: &str,
        write_byte_count: &'a IntCounter,
        write_count: &'a IntCounter,
        flush_count: &'a IntCounter,
    ) -> Result<InstrumentedAsyncWrite<'a, object_store::Writer>> {
        let writer = self.object_store.writer(path).await.context(OpenDalSnafu)?;
        Ok(InstrumentedAsyncWrite::new(
            writer,
            write_byte_count,
            write_count,
            flush_count,
        ))
    }
}

/// A wrapper around [`AsyncRead`] that adds instrumentation for monitoring
#[pin_project]
pub(crate) struct InstrumentedAsyncRead<'a, R> {
    #[pin]
    inner: R,
    read_byte_count: CounterGuard<'a>,
    read_count: CounterGuard<'a>,
    seek_count: CounterGuard<'a>,
}

impl<'a, R> InstrumentedAsyncRead<'a, R> {
    /// Create a new `InstrumentedAsyncRead`.
    fn new(
        inner: R,
        read_byte_count: &'a IntCounter,
        read_count: &'a IntCounter,
        seek_count: &'a IntCounter,
    ) -> Self {
        Self {
            inner,
            read_byte_count: CounterGuard::new(read_byte_count),
            read_count: CounterGuard::new(read_count),
            seek_count: CounterGuard::new(seek_count),
        }
    }
}

impl<'a, R: AsyncRead + Unpin + Send> AsyncRead for InstrumentedAsyncRead<'a, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let poll = self.as_mut().project().inner.poll_read(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            self.read_count.inc_by(1);
            self.read_byte_count.inc_by(*n);
        }
        poll
    }
}

impl<'a, R: AsyncSeek + Unpin + Send> AsyncSeek for InstrumentedAsyncRead<'a, R> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let poll = self.as_mut().project().inner.poll_seek(cx, pos);
        if let Poll::Ready(Ok(_)) = &poll {
            self.seek_count.inc_by(1);
        }
        poll
    }
}

/// A wrapper around [`AsyncWrite`] that adds instrumentation for monitoring
#[pin_project]
pub(crate) struct InstrumentedAsyncWrite<'a, W> {
    #[pin]
    inner: W,
    write_byte_count: CounterGuard<'a>,
    write_count: CounterGuard<'a>,
    flush_count: CounterGuard<'a>,
}

impl<'a, W> InstrumentedAsyncWrite<'a, W> {
    /// Create a new `InstrumentedAsyncWrite`.
    fn new(
        inner: W,
        write_byte_count: &'a IntCounter,
        write_count: &'a IntCounter,
        flush_count: &'a IntCounter,
    ) -> Self {
        Self {
            inner,
            write_byte_count: CounterGuard::new(write_byte_count),
            write_count: CounterGuard::new(write_count),
            flush_count: CounterGuard::new(flush_count),
        }
    }
}

impl<'a, W: AsyncWrite + Unpin + Send> AsyncWrite for InstrumentedAsyncWrite<'a, W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let poll = self.as_mut().project().inner.poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            self.write_count.inc_by(1);
            self.write_byte_count.inc_by(*n);
        }
        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let poll = self.as_mut().project().inner.poll_flush(cx);
        if let Poll::Ready(Ok(())) = &poll {
            self.flush_count.inc_by(1);
        }
        poll
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_close(cx)
    }
}

/// A guard that increments a counter when dropped.
struct CounterGuard<'a> {
    count: usize,
    counter: &'a IntCounter,
}

impl<'a> CounterGuard<'a> {
    /// Create a new `CounterGuard`.
    fn new(counter: &'a IntCounter) -> Self {
        Self { count: 0, counter }
    }

    /// Increment the counter by `n`.
    fn inc_by(&mut self, n: usize) {
        self.count += n;
    }
}

impl<'a> Drop for CounterGuard<'a> {
    fn drop(&mut self) {
        if self.count > 0 {
            self.counter.inc_by(self.count as _);
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    use object_store::services::Memory;

    use super::*;

    #[tokio::test]
    async fn test_instrumented_store_read_write() {
        let instrumented_store =
            InstrumentedStore::new(ObjectStore::new(Memory::default()).unwrap().finish());

        let read_byte_count = IntCounter::new("read_byte_count", "read_byte_count").unwrap();
        let read_count = IntCounter::new("read_count", "read_count").unwrap();
        let seek_count = IntCounter::new("seek_count", "seek_count").unwrap();
        let write_byte_count = IntCounter::new("write_byte_count", "write_byte_count").unwrap();
        let write_count = IntCounter::new("write_count", "write_count").unwrap();
        let flush_count = IntCounter::new("flush_count", "flush_count").unwrap();

        let mut writer = instrumented_store
            .writer("my_file", &write_byte_count, &write_count, &flush_count)
            .await
            .unwrap();
        writer.write_all(b"hello").await.unwrap();
        writer.flush().await.unwrap();
        writer.close().await.unwrap();
        drop(writer);

        let mut reader = instrumented_store
            .reader("my_file", &read_byte_count, &read_count, &seek_count)
            .await
            .unwrap();
        let mut buf = vec![0; 5];
        reader.read_exact(&mut buf).await.unwrap();
        reader.seek(io::SeekFrom::Start(0)).await.unwrap();
        reader.read_exact(&mut buf).await.unwrap();
        drop(reader);

        assert_eq!(read_byte_count.get(), 10);
        assert_eq!(read_count.get(), 2);
        assert_eq!(seek_count.get(), 1);
        assert_eq!(write_byte_count.get(), 5);
        assert_eq!(write_count.get(), 1);
        assert_eq!(flush_count.get(), 1);
    }
}
