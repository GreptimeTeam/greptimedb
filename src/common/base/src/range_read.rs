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

use std::future::Future;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use futures::AsyncRead;
use pin_project::pin_project;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
use tokio::sync::Mutex;

/// `Metadata` contains the metadata of a source.
pub struct Metadata {
    /// The length of the source in bytes.
    pub content_length: u64,
}

/// `RangeReader` reads a range of bytes from a source.
#[async_trait]
pub trait RangeReader: Send + Unpin {
    /// Returns the metadata of the source.
    async fn metadata(&mut self) -> io::Result<Metadata>;

    /// Reads the bytes in the given range.
    async fn read(&mut self, range: Range<u64>) -> io::Result<Bytes>;

    /// Reads the bytes in the given range into the buffer.
    ///
    /// Handles the buffer based on its capacity:
    /// - If the buffer is insufficient to hold the bytes, it will either:
    ///   - Allocate additional space (e.g., for `Vec<u8>`)
    ///   - Panic (e.g., for `&mut [u8]`)
    async fn read_into(
        &mut self,
        range: Range<u64>,
        buf: &mut (impl BufMut + Send),
    ) -> io::Result<()> {
        let bytes = self.read(range).await?;
        buf.put_slice(&bytes);
        Ok(())
    }

    /// Reads the bytes in the given ranges.
    async fn read_vec(&mut self, ranges: &[Range<u64>]) -> io::Result<Vec<Bytes>> {
        let mut result = Vec::with_capacity(ranges.len());
        for range in ranges {
            result.push(self.read(range.clone()).await?);
        }
        Ok(result)
    }
}

#[async_trait]
impl<R: ?Sized + RangeReader> RangeReader for &mut R {
    async fn metadata(&mut self) -> io::Result<Metadata> {
        (*self).metadata().await
    }
    async fn read(&mut self, range: Range<u64>) -> io::Result<Bytes> {
        (*self).read(range).await
    }
    async fn read_into(
        &mut self,
        range: Range<u64>,
        buf: &mut (impl BufMut + Send),
    ) -> io::Result<()> {
        (*self).read_into(range, buf).await
    }
    async fn read_vec(&mut self, ranges: &[Range<u64>]) -> io::Result<Vec<Bytes>> {
        (*self).read_vec(ranges).await
    }
}

/// `AsyncReadAdapter` adapts a `RangeReader` to an `AsyncRead`.
#[pin_project]
pub struct AsyncReadAdapter<R> {
    /// The inner `RangeReader`.
    /// Use `Mutex` to get rid of the borrow checker issue.
    inner: Arc<Mutex<R>>,

    /// The current position from the view of the reader.
    position: u64,

    /// The buffer for the read bytes.
    buffer: Vec<u8>,

    /// The length of the content.
    content_length: u64,

    /// The future for reading the next bytes.
    #[pin]
    read_fut: Option<Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send>>>,
}

impl<R: RangeReader + 'static> AsyncReadAdapter<R> {
    pub async fn new(inner: R) -> io::Result<Self> {
        let mut inner = inner;
        let metadata = inner.metadata().await?;
        Ok(AsyncReadAdapter {
            inner: Arc::new(Mutex::new(inner)),
            position: 0,
            buffer: Vec::new(),
            content_length: metadata.content_length,
            read_fut: None,
        })
    }
}

/// The maximum size per read for the inner reader in `AsyncReadAdapter`.
const MAX_SIZE_PER_READ: usize = 8 * 1024 * 1024; // 8MB

impl<R: RangeReader + 'static> AsyncRead for AsyncReadAdapter<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut this = self.as_mut().project();

        if *this.position >= *this.content_length {
            return Poll::Ready(Ok(0));
        }

        if !this.buffer.is_empty() {
            let to_read = this.buffer.len().min(buf.len());
            buf[..to_read].copy_from_slice(&this.buffer[..to_read]);
            this.buffer.drain(..to_read);
            *this.position += to_read as u64;
            return Poll::Ready(Ok(to_read));
        }

        if this.read_fut.is_none() {
            let size = (*this.content_length - *this.position).min(MAX_SIZE_PER_READ as u64);
            let range = *this.position..(*this.position + size);
            let inner = this.inner.clone();
            let fut = async move {
                let mut inner = inner.lock().await;
                inner.read(range).await
            };

            *this.read_fut = Some(Box::pin(fut));
        }

        match this
            .read_fut
            .as_mut()
            .as_pin_mut()
            .expect("checked above")
            .poll(cx)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(bytes)) => {
                *this.read_fut = None;

                if !bytes.is_empty() {
                    this.buffer.extend_from_slice(&bytes);
                    self.poll_read(cx, buf)
                } else {
                    Poll::Ready(Ok(0))
                }
            }
            Poll::Ready(Err(e)) => {
                *this.read_fut = None;
                Poll::Ready(Err(e))
            }
        }
    }
}

#[async_trait]
impl RangeReader for Vec<u8> {
    async fn metadata(&mut self) -> io::Result<Metadata> {
        Ok(Metadata {
            content_length: self.len() as u64,
        })
    }

    async fn read(&mut self, mut range: Range<u64>) -> io::Result<Bytes> {
        range.end = range.end.min(self.len() as u64);

        let bytes = Bytes::copy_from_slice(&self[range.start as usize..range.end as usize]);
        Ok(bytes)
    }
}

/// `FileReader` is a `RangeReader` for reading a file.
pub struct FileReader {
    content_length: u64,
    position: u64,
    file: tokio::fs::File,
}

impl FileReader {
    /// Creates a new `FileReader` for the file at the given path.
    pub async fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = tokio::fs::File::open(path).await?;
        let metadata = file.metadata().await?;
        Ok(FileReader {
            content_length: metadata.len(),
            position: 0,
            file,
        })
    }
}

#[async_trait]
impl RangeReader for FileReader {
    async fn metadata(&mut self) -> io::Result<Metadata> {
        Ok(Metadata {
            content_length: self.content_length,
        })
    }

    async fn read(&mut self, mut range: Range<u64>) -> io::Result<Bytes> {
        if range.start != self.position {
            self.file.seek(io::SeekFrom::Start(range.start)).await?;
            self.position = range.start;
        }

        range.end = range.end.min(self.content_length);
        if range.end <= self.position {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Start of range is out of bounds",
            ));
        }

        let mut buf = vec![0; (range.end - range.start) as usize];

        self.file.read_exact(&mut buf).await?;
        self.position = range.end;

        Ok(Bytes::from(buf))
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_named_temp_file;
    use futures::io::AsyncReadExt as _;

    use super::*;

    #[tokio::test]
    async fn test_async_read_adapter() {
        let data = b"hello world";
        let reader = Vec::from(data);
        let mut adapter = AsyncReadAdapter::new(reader).await.unwrap();

        let mut buf = Vec::new();
        adapter.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_async_read_adapter_large() {
        let data = (0..20 * 1024 * 1024).map(|i| i as u8).collect::<Vec<u8>>();
        let mut adapter = AsyncReadAdapter::new(data.clone()).await.unwrap();

        let mut buf = Vec::new();
        adapter.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_file_reader() {
        let file = create_named_temp_file();
        let path = file.path();
        let data = b"hello world";
        tokio::fs::write(path, data).await.unwrap();

        let mut reader = FileReader::new(path).await.unwrap();
        let metadata = reader.metadata().await.unwrap();
        assert_eq!(metadata.content_length, data.len() as u64);

        let bytes = reader.read(0..metadata.content_length).await.unwrap();
        assert_eq!(&*bytes, data);

        let bytes = reader.read(0..5).await.unwrap();
        assert_eq!(&*bytes, &data[..5]);
    }
}
