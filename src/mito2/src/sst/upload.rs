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

//! Cooperative byte transfer for SST uploads. Callers own close, abort, and file cleanup.

#[cfg(feature = "enterprise")]
use std::io;
#[cfg(feature = "enterprise")]
use std::pin::Pin;
#[cfg(feature = "enterprise")]
use std::task::{Context, Poll, ready};

#[cfg(feature = "enterprise")]
use bytes::{Buf, Bytes, BytesMut};
#[cfg(feature = "enterprise")]
use futures::{AsyncWrite, Sink, SinkExt};
use futures::{Stream, StreamExt};
use object_store::{Buffer, Error as ObjectStoreError, Result as ObjectStoreResult, Writer};
use snafu::Snafu;
use tokio_util::sync::CancellationToken;

/// Preserves read/write attribution until the caller adds its file context.
#[derive(Debug, Snafu)]
pub(crate) enum UploadError {
    #[snafu(display("Upload cancelled"))]
    Cancelled,
    #[snafu(display("Failed to read upload source: {source}"))]
    Read { source: ObjectStoreError },
    #[snafu(display("Failed to write upload: {source}"))]
    Write { source: ObjectStoreError },
}

/// Copies chunks without dropping an in-flight write when cancellation arrives.
pub(crate) async fn copy(
    reader: &mut (impl Stream<Item = ObjectStoreResult<Buffer>> + Unpin),
    writer: &mut Writer,
    write_buffer_size: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<u64, UploadError> {
    let mut written = 0;
    loop {
        let next = if let Some(cancellation) = cancellation {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(UploadError::Cancelled),
                next = reader.next() => next,
            }
        } else {
            reader.next().await
        };
        let Some(bytes) = next else {
            return Ok(written);
        };
        let bytes = bytes.map_err(|source| UploadError::Read { source })?;
        written += write(writer, bytes, write_buffer_size, cancellation).await?;
    }
}

/// Writes bounded, zero-copy slices, awaiting each started write to completion.
pub(crate) async fn write(
    writer: &mut Writer,
    mut bytes: Buffer,
    write_buffer_size: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<u64, UploadError> {
    let chunks = std::iter::from_fn(|| {
        (!bytes.is_empty()).then(|| bytes.split_to(bytes.len().min(write_buffer_size.max(1))))
    });
    write_chunks(writer, chunks, cancellation).await
}

async fn write_chunks(
    writer: &mut Writer,
    chunks: impl Iterator<Item = Buffer>,
    cancellation: Option<&CancellationToken>,
) -> Result<u64, UploadError> {
    let mut written = 0;
    for chunk in chunks {
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(UploadError::Cancelled);
        }
        let len = chunk.len() as u64;
        writer
            .write(chunk)
            .await
            .map_err(|source| UploadError::Write { source })?;
        written += len;
    }
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(UploadError::Cancelled);
    }
    Ok(written)
}

#[cfg(feature = "enterprise")]
#[derive(Debug, Snafu)]
#[snafu(display("Upload exceeds the backend object limit of {limit} bytes"))]
pub(crate) struct UploadObjectLimit {
    pub(crate) limit: u64,
}

/// Part and object limits for the bounded, serial upload path.
#[cfg(feature = "enterprise")]
#[derive(Clone, Copy)]
pub(crate) struct UploadLimits {
    pub(crate) chunk_bytes: usize,
    pub(crate) max_object_bytes: u64,
}

/// Owns at most one part while assembling bytes from encoder blocks or index blobs.
/// Sending whole owned parts avoids retaining a large producer allocation or a
/// queue of small fragments in OpenDAL. Only the final part may be short.
#[cfg(feature = "enterprise")]
pub(crate) struct UploadBuffer {
    limits: UploadLimits,
    pending: BytesMut,
    object_bytes: u64,
}

#[cfg(feature = "enterprise")]
impl UploadBuffer {
    pub(crate) fn new(limits: UploadLimits) -> Self {
        Self {
            limits,
            pending: BytesMut::new(),
            object_bytes: 0,
        }
    }

    pub(crate) fn check_size(&self, bytes: usize) -> Result<(), UploadObjectLimit> {
        if bytes as u64 > self.limits.max_object_bytes - self.object_bytes {
            return Err(UploadObjectLimit {
                limit: self.limits.max_object_bytes,
            });
        }
        Ok(())
    }

    fn append(&mut self, bytes: &[u8]) -> usize {
        let len = bytes
            .len()
            .min(self.limits.chunk_bytes - self.pending.len());
        if len > 0 {
            if self.pending.capacity() == 0 {
                self.pending = BytesMut::with_capacity(self.limits.chunk_bytes);
            }
            self.pending.extend_from_slice(&bytes[..len]);
            self.object_bytes += len as u64;
        }
        len
    }

    fn is_full(&self) -> bool {
        self.pending.len() == self.limits.chunk_bytes
    }

    fn take_part(&mut self) -> Buffer {
        std::mem::take(&mut self.pending).freeze().into()
    }

    pub(crate) async fn write(
        &mut self,
        writer: &mut Writer,
        mut bytes: Bytes,
        cancellation: Option<&CancellationToken>,
    ) -> Result<u64, UploadError> {
        let len = bytes.len() as u64;
        let parts = std::iter::from_fn(|| {
            let appended = self.append(&bytes);
            bytes.advance(appended);
            self.is_full().then(|| self.take_part())
        });
        write_chunks(writer, parts, cancellation).await?;
        Ok(len)
    }

    pub(crate) async fn write_tail(&mut self, writer: &mut Writer) -> ObjectStoreResult<()> {
        if !self.pending.is_empty() {
            writer.write(self.take_part()).await?;
        }
        Ok(())
    }
}

#[cfg(feature = "enterprise")]
pub(crate) fn buffered_writer(
    writer: Writer,
    limits: UploadLimits,
) -> impl AsyncWrite + Send + Sync + Unpin {
    BufferedUploadWriter {
        sink: writer.into_sink().sink_map_err(io::Error::other),
        buffer: UploadBuffer::new(limits),
    }
}

#[cfg(feature = "enterprise")]
struct BufferedUploadWriter<S> {
    sink: S,
    buffer: UploadBuffer,
}

#[cfg(feature = "enterprise")]
impl<S: Sink<Buffer, Error = io::Error> + Unpin> BufferedUploadWriter<S> {
    fn poll_send_part(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(Pin::new(&mut self.sink).poll_ready(cx))?;
        Poll::Ready(Pin::new(&mut self.sink).start_send(self.buffer.take_part()))
    }
}

#[cfg(feature = "enterprise")]
impl<S: Sink<Buffer, Error = io::Error> + Unpin> AsyncWrite for BufferedUploadWriter<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let writer = self.get_mut();
        writer
            .buffer
            .check_size(bytes.len())
            .map_err(io::Error::other)?;
        if writer.buffer.is_full() {
            ready!(writer.poll_send_part(cx))?;
        }
        // The sink can retain the submitted part. Drain it into OpenDAL before
        // allocating the next assembly buffer so admission covers both states.
        ready!(Pin::new(&mut writer.sink).poll_ready(cx))?;
        Poll::Ready(Ok(writer.buffer.append(bytes)))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let writer = self.get_mut();
        // A blob flush must not fragment the upload queue. Flush complete parts;
        // close is the file boundary and submits the remaining short part.
        if writer.buffer.is_full() {
            ready!(writer.poll_send_part(cx))?;
        }
        Pin::new(&mut writer.sink).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let writer = self.get_mut();
        if !writer.buffer.pending.is_empty() {
            ready!(writer.poll_send_part(cx))?;
        }
        Pin::new(&mut writer.sink).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::stream;
    use object_store::ObjectStore;
    use object_store::layers::mock::{
        Metadata, MockLayerBuilder, ObservedWriter, WriterObservations, oio,
    };
    use object_store::services::Memory;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    use super::*;

    #[cfg(feature = "enterprise")]
    #[tokio::test]
    async fn test_buffered_upload_coalesces_blob_flushes_and_limits_object_size() {
        use futures::io::AsyncWriteExt;

        let observations = Arc::new(Mutex::new(WriterObservations::default()));
        let observer = observations.clone();
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |_, _, inner| {
                Box::new(ObservedWriter::new(inner, observer.clone()))
            }))
            .build()
            .unwrap();
        let store = ObjectStore::new(Memory::default()).unwrap().layer(layer);
        // Observe adapter boundaries without OpenDAL coalescing them for the test.
        let mut writer = buffered_writer(
            store.writer("index").await.unwrap(),
            UploadLimits {
                chunk_bytes: 4,
                max_object_bytes: 9,
            },
        );
        for byte in b"abcdefghi" {
            writer.write_all(std::slice::from_ref(byte)).await.unwrap();
            writer.flush().await.unwrap();
        }
        let error = writer.write_all(b"j").await.unwrap_err();
        assert!(error.get_ref().unwrap().is::<UploadObjectLimit>());
        writer.close().await.unwrap();
        assert_eq!(vec![4, 4, 1], observations.lock().unwrap().write_sizes);
        assert_eq!(
            b"abcdefghi",
            store.read("index").await.unwrap().to_bytes().as_ref()
        );
    }

    #[tokio::test]
    async fn test_copy_cancels_pending_read() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let mut writer = store.writer("pending").await.unwrap();
        let mut reader = stream::pending();
        let cancellation = CancellationToken::new();
        let mut copy = Box::pin(super::copy(
            &mut reader,
            &mut writer,
            4,
            Some(&cancellation),
        ));
        assert!(futures::poll!(copy.as_mut()).is_pending());
        cancellation.cancel();
        let error = timeout(Duration::from_secs(5), copy)
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, UploadError::Cancelled));
        writer.abort().await.unwrap();
        assert!(!store.exists("pending").await.unwrap());
    }

    struct GatedWriter {
        inner: oio::Writer,
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl oio::Write for GatedWriter {
        async fn write(&mut self, bytes: Buffer) -> ObjectStoreResult<()> {
            self.started.notify_one();
            self.release.notified().await;
            self.inner.write(bytes).await
        }

        async fn close(&mut self) -> ObjectStoreResult<Metadata> {
            self.inner.close().await
        }

        async fn abort(&mut self) -> ObjectStoreResult<()> {
            self.inner.abort().await
        }
    }

    #[tokio::test]
    async fn test_copy_cancellation_waits_for_started_write() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let observations = Arc::new(Mutex::new(WriterObservations::default()));
        let observer = observations.clone();
        let started_write = started.clone();
        let release_write = release.clone();
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |_, _, inner| {
                Box::new(ObservedWriter::new(
                    Box::new(GatedWriter {
                        inner,
                        started: started_write.clone(),
                        release: release_write.clone(),
                    }),
                    observer.clone(),
                ))
            }))
            .build()
            .unwrap();
        let store = ObjectStore::new(Memory::default()).unwrap().layer(layer);
        // Leave the OpenDAL writer unbuffered so the observer sees helper boundaries.
        let mut writer = store.writer("gated").await.unwrap();
        let mut reader = stream::iter([Ok(Buffer::from(Bytes::from_static(b"abcdefghijkl")))]);
        let cancellation = CancellationToken::new();
        let mut copy = Box::pin(super::copy(
            &mut reader,
            &mut writer,
            4,
            Some(&cancellation),
        ));
        assert!(futures::poll!(copy.as_mut()).is_pending());
        timeout(Duration::from_secs(5), started.notified())
            .await
            .unwrap();

        cancellation.cancel();
        assert!(futures::poll!(copy.as_mut()).is_pending());
        release.notify_one();
        let error = timeout(Duration::from_secs(5), copy)
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, UploadError::Cancelled));
        {
            let observations = observations.lock().unwrap();
            assert_eq!(vec![4], observations.write_sizes);
            assert_eq!(0, observations.close_calls);
            assert_eq!(0, observations.abort_calls);
        }
        writer.abort().await.unwrap();
        assert_eq!(1, observations.lock().unwrap().abort_calls);
        assert!(!store.exists("gated").await.unwrap());
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_copy_preserves_contents(#[values(false, true)] cancellable: bool) {
        let observations = Arc::new(Mutex::new(WriterObservations::default()));
        let observer = observations.clone();
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |_, _, inner| {
                Box::new(ObservedWriter::new(inner, observer.clone()))
            }))
            .build()
            .unwrap();
        let store = ObjectStore::new(Memory::default()).unwrap().layer(layer);
        let mut writer = store.writer("complete").await.unwrap();
        let mut reader = stream::iter([
            Ok(Buffer::from(Bytes::from_static(b"abcde"))),
            Ok(Buffer::from(Bytes::from_static(b"fghijkl"))),
        ]);
        let cancellation = CancellationToken::new();
        let copied = super::copy(
            &mut reader,
            &mut writer,
            4,
            cancellable.then_some(&cancellation),
        )
        .await
        .unwrap();
        assert_eq!(12, copied);
        {
            let observations = observations.lock().unwrap();
            assert_eq!(vec![4, 1, 4, 3], observations.write_sizes);
            assert_eq!(0, observations.close_calls);
            assert_eq!(0, observations.abort_calls);
        }
        writer.close().await.unwrap();
        assert_eq!(1, observations.lock().unwrap().close_calls);
        assert_eq!(
            Bytes::from_static(b"abcdefghijkl"),
            store.read("complete").await.unwrap().to_bytes()
        );
    }
}
