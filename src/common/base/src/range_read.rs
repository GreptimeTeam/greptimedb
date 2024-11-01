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
use std::ops::Range;
use std::task::Poll;

use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use pin_project::pin_project;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

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

/// `AsyncReadAdapter` adapts an `AsyncRead` to a `RangeReader`.
#[pin_project]
pub struct AsyncReadAdapter<R> {
    #[pin]
    reader: R,
    position: u64,
    buffer: Vec<u8>,
}

impl<R: RangeReader> AsyncReadAdapter<R> {
    /// Creates a new `AsyncReadAdapter` with the given `RangeReader`.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            position: 0,
            buffer: Vec::new(),
        }
    }
}

impl<R: RangeReader> futures::AsyncRead for AsyncReadAdapter<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut this = self.as_mut().project();

        if this.buffer.is_empty() {
            let range = *this.position..(*this.position + buf.len() as u64);
            let mut bytes = this.reader.read(range);
            let poll = bytes.as_mut().poll(cx);
            let r = match poll {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(r) => r,
            };
            match r {
                Ok(bytes) => {
                    this.buffer.extend_from_slice(&bytes);
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        // Copy data from the internal buffer to the provided buffer
        let len = std::cmp::min(buf.len(), self.buffer.len());
        buf[..len].copy_from_slice(&self.buffer[..len]);
        self.buffer.drain(..len);
        self.position += len as u64;

        Poll::Ready(Ok(len))
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

        let mut buf = vec![0; (range.end - range.start) as usize];
        buf.copy_from_slice(&self[range.start as usize..range.end as usize]);
        Ok(Bytes::from(buf))
    }
}

pub struct TokioFileReader {
    file: tokio::fs::File,
}

impl TokioFileReader {
    pub fn new(file: tokio::fs::File) -> Self {
        Self { file }
    }
}

#[async_trait]
impl RangeReader for TokioFileReader {
    async fn metadata(&mut self) -> io::Result<Metadata> {
        let metadata = self.file.metadata().await?;
        Ok(Metadata {
            content_length: metadata.len(),
        })
    }

    async fn read(&mut self, mut range: Range<u64>) -> io::Result<Bytes> {
        // let metadata = self.metadata().await.unwrap();
        // range.end = range.end.min(metadata.content_length);

        // let mut buf = vec![0; (range.end - range.start) as usize];
        // self.seek(io::SeekFrom::Start(range.start)).await?;
        // self.read_exact(&mut buf).await?;
        // if range != (4..26) {
        //     let mut buf = vec![0; (range.end - range.start) as usize];
        //     self.seek(io::SeekFrom::Start(range.start)).await?;
        //     self.read_exact(&mut buf).await?;
        //     return Ok(Bytes::from(buf));
        // }
        // self.seek(io::SeekFrom::Start(0)).await?;

        // let mut buf = vec![0; 32];
        // let _ = self.read_exact(&mut buf).await.unwrap();
        // let b = &buf[4..26];
        // Ok(Bytes::copy_from_slice(b))

        let metadata = self.metadata().await.unwrap();
        range.end = range.end.min(metadata.content_length);
        let mut buf = vec![0; (range.end - range.start) as usize];
        self.file.seek(io::SeekFrom::Start(range.start)).await?;
        self.file.read_exact(&mut buf).await?;
        Ok(Bytes::from(buf))
    }
}
