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

use async_compression::tokio::write::{BzEncoder, GzipEncoder, XzEncoder, ZstdEncoder};
use snafu::ResultExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::compression::CompressionType;
use crate::error::{self, Result};

/// A compressed writer that wraps an underlying async writer with compression.
///
/// This writer supports multiple compression formats including GZIP, BZIP2, XZ, and ZSTD.
/// It provides transparent compression for any async writer implementation.
pub struct CompressedWriter {
    inner: Box<dyn AsyncWrite + Unpin + Send>,
    compression_type: CompressionType,
}

impl CompressedWriter {
    /// Creates a new compressed writer with the specified compression type.
    ///
    /// # Arguments
    ///
    /// * `writer` - The underlying writer to wrap with compression
    /// * `compression_type` - The type of compression to apply
    pub fn new(
        writer: impl AsyncWrite + Unpin + Send + 'static,
        compression_type: CompressionType,
    ) -> Self {
        let inner: Box<dyn AsyncWrite + Unpin + Send> = match compression_type {
            CompressionType::Gzip => Box::new(GzipEncoder::new(writer)),
            CompressionType::Bzip2 => Box::new(BzEncoder::new(writer)),
            CompressionType::Xz => Box::new(XzEncoder::new(writer)),
            CompressionType::Zstd => Box::new(ZstdEncoder::new(writer)),
            CompressionType::Uncompressed => Box::new(writer),
        };

        Self {
            inner,
            compression_type,
        }
    }

    /// Returns the compression type used by this writer.
    pub fn compression_type(&self) -> CompressionType {
        self.compression_type
    }

    /// Flush the writer and shutdown compression
    pub async fn shutdown(mut self) -> Result<()> {
        self.inner
            .shutdown()
            .await
            .context(error::AsyncWriteSnafu)?;
        Ok(())
    }
}

impl AsyncWrite for CompressedWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A trait for converting async writers into compressed writers.
///
/// This trait is automatically implemented for all types that implement [`AsyncWrite`].
pub trait IntoCompressedWriter {
    /// Converts this writer into a [`CompressedWriter`] with the specified compression type.
    ///
    /// # Arguments
    ///
    /// * `self` - The underlying writer to wrap with compression
    /// * `compression_type` - The type of compression to apply
    fn into_compressed_writer(self, compression_type: CompressionType) -> CompressedWriter
    where
        Self: AsyncWrite + Unpin + Send + 'static + Sized,
    {
        CompressedWriter::new(self, compression_type)
    }
}

impl<W: AsyncWrite + Unpin + Send + 'static> IntoCompressedWriter for W {}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    use super::*;

    #[tokio::test]
    async fn test_compressed_writer_gzip() {
        let (duplex_writer, mut duplex_reader) = duplex(1024);
        let mut writer = duplex_writer.into_compressed_writer(CompressionType::Gzip);
        let original = b"test data for gzip compression";

        writer.write_all(original).await.unwrap();
        writer.shutdown().await.unwrap();

        let mut buffer = Vec::new();
        duplex_reader.read_to_end(&mut buffer).await.unwrap();

        // The compressed data should be different from the original
        assert_ne!(buffer, original);
        assert!(!buffer.is_empty());
    }

    #[tokio::test]
    async fn test_compressed_writer_bzip2() {
        let (duplex_writer, mut duplex_reader) = duplex(1024);
        let mut writer = duplex_writer.into_compressed_writer(CompressionType::Bzip2);
        let original = b"test data for bzip2 compression";

        writer.write_all(original).await.unwrap();
        writer.shutdown().await.unwrap();

        let mut buffer = Vec::new();
        duplex_reader.read_to_end(&mut buffer).await.unwrap();

        // The compressed data should be different from the original
        assert_ne!(buffer, original);
        assert!(!buffer.is_empty());
    }

    #[tokio::test]
    async fn test_compressed_writer_xz() {
        let (duplex_writer, mut duplex_reader) = duplex(1024);
        let mut writer = duplex_writer.into_compressed_writer(CompressionType::Xz);
        let original = b"test data for xz compression";

        writer.write_all(original).await.unwrap();
        writer.shutdown().await.unwrap();

        let mut buffer = Vec::new();
        duplex_reader.read_to_end(&mut buffer).await.unwrap();

        // The compressed data should be different from the original
        assert_ne!(buffer, original);
        assert!(!buffer.is_empty());
    }

    #[tokio::test]
    async fn test_compressed_writer_zstd() {
        let (duplex_writer, mut duplex_reader) = duplex(1024);
        let mut writer = duplex_writer.into_compressed_writer(CompressionType::Zstd);
        let original = b"test data for zstd compression";

        writer.write_all(original).await.unwrap();
        writer.shutdown().await.unwrap();

        let mut buffer = Vec::new();
        duplex_reader.read_to_end(&mut buffer).await.unwrap();

        // The compressed data should be different from the original
        assert_ne!(buffer, original);
        assert!(!buffer.is_empty());
    }

    #[tokio::test]
    async fn test_compressed_writer_uncompressed() {
        let (duplex_writer, mut duplex_reader) = duplex(1024);
        let mut writer = duplex_writer.into_compressed_writer(CompressionType::Uncompressed);
        let original = b"test data for uncompressed";

        writer.write_all(original).await.unwrap();
        writer.shutdown().await.unwrap();

        let mut buffer = Vec::new();
        duplex_reader.read_to_end(&mut buffer).await.unwrap();

        // Uncompressed data should be the same as the original
        assert_eq!(buffer, original);
    }
}
