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

use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use futures::{AsyncReadExt, AsyncSeekExt};

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
impl<R: RangeReader + Send + Unpin> RangeReader for &mut R {
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

/// `RangeReaderAdapter` bridges `RangeReader` and `AsyncRead + AsyncSeek`.
pub struct RangeReaderAdapter<R>(pub R);

/// Implements `RangeReader` for a type that implements `AsyncRead + AsyncSeek`.
///
/// TODO(zhongzc): It's a temporary solution for porting the codebase from `AsyncRead + AsyncSeek` to `RangeReader`.
/// Until the codebase is fully ported to `RangeReader`, remove this implementation.
#[async_trait]
impl<R: futures::AsyncRead + futures::AsyncSeek + Send + Unpin> RangeReader
    for RangeReaderAdapter<R>
{
    async fn metadata(&mut self) -> io::Result<Metadata> {
        let content_length = self.0.seek(io::SeekFrom::End(0)).await?;
        Ok(Metadata { content_length })
    }

    async fn read(&mut self, range: Range<u64>) -> io::Result<Bytes> {
        let mut buf = vec![0; (range.end - range.start) as usize];
        self.0.seek(io::SeekFrom::Start(range.start)).await?;
        self.0.read_exact(&mut buf).await?;
        Ok(Bytes::from(buf))
    }
}
