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
    let len = bytes.len() as u64;
    // Keep each slice nonempty even when the configured size is zero.
    let write_buffer_size = write_buffer_size.max(1);
    while !bytes.is_empty() {
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(UploadError::Cancelled);
        }
        let chunk = bytes.split_to(bytes.len().min(write_buffer_size));
        writer
            .write(chunk)
            .await
            .map_err(|source| UploadError::Write { source })?;
    }
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(UploadError::Cancelled);
    }
    Ok(len)
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
