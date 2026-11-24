// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! Bounded multipart uploads for table compaction SSTs and their Puffin indexes.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::{Buf, Bytes, BytesMut};
use futures::{AsyncWrite, Sink, SinkExt};
use object_store::{Buffer, Result as ObjectStoreResult, Writer};
use snafu::Snafu;
use tokio_util::sync::CancellationToken;

use crate::sst::upload::{self, UploadError};

#[derive(Debug, Snafu)]
#[snafu(display("Upload exceeds the backend object limit of {limit} bytes"))]
pub(crate) struct UploadObjectLimit {
    pub(crate) limit: u64,
}

/// Part and object limits for the bounded, serial upload path.
#[derive(Clone, Copy)]
pub(crate) struct UploadLimits {
    pub(crate) chunk_bytes: usize,
    pub(crate) max_object_bytes: u64,
}

/// Owns at most one part while assembling bytes from encoder blocks or index blobs.
/// Sending whole owned parts avoids retaining a large producer allocation or a
/// queue of small fragments in OpenDAL. Only the final part may be short.
/// Both write paths check the remaining object allowance before buffering bytes.
pub(crate) struct UploadBuffer {
    limits: UploadLimits,
    pending: BytesMut,
    object_bytes: u64,
}

/// A bounded write stopped at the object limit or failed in the store writer.
#[derive(Debug)]
pub(crate) enum BufferedWriteError {
    ObjectLimit(UploadObjectLimit),
    Upload(UploadError),
}

impl UploadBuffer {
    pub(crate) fn new(limits: UploadLimits) -> Self {
        Self {
            limits,
            pending: BytesMut::new(),
            object_bytes: 0,
        }
    }

    fn check_size(&self, bytes: usize) -> Result<(), UploadObjectLimit> {
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
    ) -> Result<u64, BufferedWriteError> {
        self.check_size(bytes.len())
            .map_err(BufferedWriteError::ObjectLimit)?;
        let len = bytes.len() as u64;
        let parts = std::iter::from_fn(|| {
            let appended = self.append(&bytes);
            bytes.advance(appended);
            self.is_full().then(|| self.take_part())
        });
        upload::write_chunks(writer, parts, cancellation)
            .await
            .map_err(BufferedWriteError::Upload)?;
        Ok(len)
    }

    pub(crate) async fn write_tail(&mut self, writer: &mut Writer) -> ObjectStoreResult<()> {
        if !self.pending.is_empty() {
            writer.write(self.take_part()).await?;
        }
        Ok(())
    }
}

/// Coalesces writes across blob flushes until a full part or the end of the object.
pub(crate) fn writer(
    writer: Writer,
    limits: UploadLimits,
) -> impl AsyncWrite + Send + Sync + Unpin {
    BufferedUploadWriter {
        sink: writer.into_sink().sink_map_err(io::Error::other),
        buffer: UploadBuffer::new(limits),
    }
}

struct BufferedUploadWriter<S> {
    sink: S,
    buffer: UploadBuffer,
}

impl<S: Sink<Buffer, Error = io::Error> + Unpin> BufferedUploadWriter<S> {
    fn poll_send_part(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(Pin::new(&mut self.sink).poll_ready(cx))?;
        Poll::Ready(Pin::new(&mut self.sink).start_send(self.buffer.take_part()))
    }
}

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

    use bytes::Bytes;
    use futures::io::AsyncWriteExt;
    use object_store::ObjectStore;
    use object_store::layers::mock::{MockLayerBuilder, ObservedWriter, WriterObservations};
    use object_store::services::Memory;

    use super::*;

    #[tokio::test]
    async fn test_buffered_upload_coalesces_blob_flushes_and_limits_object_size() {
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
        let mut writer = writer(
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
    async fn test_upload_buffer_accepts_the_object_limit_and_rejects_more() {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let mut buffer = UploadBuffer::new(UploadLimits {
            chunk_bytes: 4,
            max_object_bytes: 5,
        });
        let mut writer = store.writer("sst").await.unwrap();
        buffer
            .write(&mut writer, Bytes::from_static(b"abcd"), None)
            .await
            .unwrap();
        buffer
            .write(&mut writer, Bytes::from_static(b"e"), None)
            .await
            .unwrap();
        let error = buffer
            .write(&mut writer, Bytes::from_static(b"f"), None)
            .await
            .unwrap_err();
        assert!(matches!(error, BufferedWriteError::ObjectLimit(_)));
        buffer.write_tail(&mut writer).await.unwrap();
        writer.close().await.unwrap();
        assert_eq!(
            b"abcde",
            store.read("sst").await.unwrap().to_bytes().as_ref()
        );
    }
}
