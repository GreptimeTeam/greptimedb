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

use std::pin::Pin;
use std::task::{Context, Poll};

use async_compression::futures::bufread::ZstdDecoder;
use async_trait::async_trait;
use futures::io::BufReader;
use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncWrite};
use snafu::{ensure, OptionExt, ResultExt};

use crate::blob_metadata::{BlobMetadata, CompressionCodec};
use crate::error::{
    BlobIndexOutOfBoundSnafu, BlobNotFoundSnafu, DeserializeJsonSnafu, FileKeyNotMatchSnafu,
    ReadSnafu, Result, UnsupportedDecompressionSnafu, WriteSnafu,
};
use crate::file_format::reader::{AsyncReader, PuffinFileReader};
use crate::partial_reader::PartialReader;
use crate::puffin_manager::file_accessor::PuffinFileAccessor;
use crate::puffin_manager::fs_puffin_manager::dir_meta::DirMetadata;
use crate::puffin_manager::stager::{BoxWriter, DirWriterProviderRef, Stager};
use crate::puffin_manager::{BlobGuard, PuffinReader};

/// `FsPuffinReader` is a `PuffinReader` that provides fs readers for puffin files.
pub struct FsPuffinReader<S, F> {
    /// The name of the puffin file.
    puffin_file_name: String,

    /// The stager.
    stager: S,

    /// The puffin file accessor.
    puffin_file_accessor: F,
}

impl<S, F> FsPuffinReader<S, F> {
    pub(crate) fn new(puffin_file_name: String, stager: S, puffin_file_accessor: F) -> Self {
        Self {
            puffin_file_name,
            stager,
            puffin_file_accessor,
        }
    }
}

#[async_trait]
impl<S, F> PuffinReader for FsPuffinReader<S, F>
where
    S: Stager + 'static,
    F: PuffinFileAccessor + Clone,
{
    type Blob = Either<RandomReadBlob<F>, S::Blob>;
    type Dir = S::Dir;

    async fn blob(&self, key: &str) -> Result<Self::Blob> {
        let reader = self
            .puffin_file_accessor
            .reader(&self.puffin_file_name)
            .await?;
        let mut file = PuffinFileReader::new(reader);

        // TODO(zhongzc): cache the metadata.
        let metadata = file.metadata().await?;
        let blob_metadata = metadata
            .blobs
            .into_iter()
            .find(|m| m.blob_type == key)
            .context(BlobNotFoundSnafu { blob: key })?;

        let blob = if blob_metadata.compression_codec.is_none() {
            // If the blob is not compressed, we can directly read it from the puffin file.
            Either::L(RandomReadBlob {
                file_name: self.puffin_file_name.clone(),
                accessor: self.puffin_file_accessor.clone(),
                blob_metadata,
            })
        } else {
            // If the blob is compressed, we need to decompress it into staging space before reading.
            let staged_blob = self
                .stager
                .get_blob(
                    self.puffin_file_name.as_str(),
                    key,
                    Box::new(|writer| {
                        Box::pin(Self::init_blob_to_stager(file, blob_metadata, writer))
                    }),
                )
                .await?;

            Either::R(staged_blob)
        };

        Ok(blob)
    }

    async fn dir(&self, key: &str) -> Result<Self::Dir> {
        self.stager
            .get_dir(
                self.puffin_file_name.as_str(),
                key,
                Box::new(|writer_provider| {
                    let accessor = self.puffin_file_accessor.clone();
                    let puffin_file_name = self.puffin_file_name.clone();
                    let key = key.to_string();
                    Box::pin(Self::init_dir_to_stager(
                        puffin_file_name,
                        key,
                        writer_provider,
                        accessor,
                    ))
                }),
            )
            .await
    }
}

impl<S, F> FsPuffinReader<S, F>
where
    S: Stager,
    F: PuffinFileAccessor + Clone,
{
    async fn init_blob_to_stager(
        mut reader: PuffinFileReader<F::Reader>,
        blob_metadata: BlobMetadata,
        mut writer: BoxWriter,
    ) -> Result<u64> {
        let reader = reader.blob_reader(&blob_metadata)?;
        let compression = blob_metadata.compression_codec;
        let size = Self::handle_decompress(reader, &mut writer, compression).await?;
        Ok(size)
    }

    async fn init_dir_to_stager(
        puffin_file_name: String,
        key: String,
        writer_provider: DirWriterProviderRef,
        accessor: F,
    ) -> Result<u64> {
        let reader = accessor.reader(&puffin_file_name).await?;
        let mut file = PuffinFileReader::new(reader);

        let puffin_metadata = file.metadata().await?;
        let blob_metadata = puffin_metadata
            .blobs
            .iter()
            .find(|m| m.blob_type == key.as_str())
            .context(BlobNotFoundSnafu { blob: key })?;

        let mut reader = file.blob_reader(blob_metadata)?;
        let mut buf = vec![];
        reader.read_to_end(&mut buf).await.context(ReadSnafu)?;
        let dir_meta: DirMetadata =
            serde_json::from_slice(buf.as_slice()).context(DeserializeJsonSnafu)?;

        let mut tasks = vec![];
        for file_meta in dir_meta.files {
            let blob_meta = puffin_metadata
                .blobs
                .get(file_meta.blob_index)
                .context(BlobIndexOutOfBoundSnafu {
                    index: file_meta.blob_index,
                    max_index: puffin_metadata.blobs.len(),
                })?
                .clone();
            ensure!(
                blob_meta.blob_type == file_meta.key,
                FileKeyNotMatchSnafu {
                    expected: file_meta.key,
                    actual: &blob_meta.blob_type,
                }
            );

            let reader = accessor.reader(&puffin_file_name).await?;
            let writer = writer_provider.writer(&file_meta.relative_path).await?;
            let task = common_runtime::spawn_global(async move {
                let mut file = PuffinFileReader::new(reader);
                let reader = file.blob_reader(&blob_meta)?;
                let compression = blob_meta.compression_codec;
                let size = Self::handle_decompress(reader, writer, compression).await?;
                Ok(size)
            });
            tasks.push(task);
        }

        let size = futures::future::try_join_all(tasks.into_iter())
            .await
            .into_iter()
            .flatten()
            .sum::<Result<_>>()?;

        Ok(size)
    }

    /// Handles the decompression of the reader and writes the decompressed data to the writer.
    /// Returns the number of bytes written.
    async fn handle_decompress(
        reader: impl AsyncRead,
        mut writer: impl AsyncWrite + Unpin,
        compression: Option<CompressionCodec>,
    ) -> Result<u64> {
        match compression {
            Some(CompressionCodec::Lz4) => UnsupportedDecompressionSnafu {
                decompression: "lz4",
            }
            .fail(),
            Some(CompressionCodec::Zstd) => {
                let reader = ZstdDecoder::new(BufReader::new(reader));
                futures::io::copy(reader, &mut writer)
                    .await
                    .context(WriteSnafu)
            }
            None => futures::io::copy(reader, &mut writer)
                .await
                .context(WriteSnafu),
        }
    }
}

/// `RandomReadBlob` is a `BlobGuard` that directly reads the blob from the puffin file.
pub struct RandomReadBlob<F> {
    file_name: String,
    accessor: F,
    blob_metadata: BlobMetadata,
}

#[async_trait]
impl<F: PuffinFileAccessor + Clone> BlobGuard for RandomReadBlob<F> {
    type Reader = PartialReader<F::Reader>;

    async fn reader(&self) -> Result<Self::Reader> {
        ensure!(
            self.blob_metadata.compression_codec.is_none(),
            UnsupportedDecompressionSnafu {
                decompression: self.blob_metadata.compression_codec.unwrap().to_string()
            }
        );

        let reader = self.accessor.reader(&self.file_name).await?;
        let blob_reader = PuffinFileReader::new(reader).into_blob_reader(&self.blob_metadata);
        Ok(blob_reader)
    }
}

/// `Either` is a type that represents either `A` or `B`.
///
/// Used to:
/// impl `AsyncRead + AsyncSeek` for `Either<A: AsyncRead + AsyncSeek, B: AsyncRead + AsyncSeek>`,
/// impl `BlobGuard` for `Either<A: BlobGuard, B: BlobGuard>`.
pub enum Either<A, B> {
    L(A),
    R(B),
}

impl<A, B> AsyncRead for Either<A, B>
where
    A: AsyncRead + Unpin,
    B: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Either::L(a) => Pin::new(a).poll_read(cx, buf),
            Either::R(b) => Pin::new(b).poll_read(cx, buf),
        }
    }
}

impl<A, B> AsyncSeek for Either<A, B>
where
    A: AsyncSeek + Unpin,
    B: AsyncSeek + Unpin,
{
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: std::io::SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        match self.get_mut() {
            Either::L(a) => Pin::new(a).poll_seek(cx, pos),
            Either::R(b) => Pin::new(b).poll_seek(cx, pos),
        }
    }
}

#[async_trait]
impl<A, B> BlobGuard for Either<A, B>
where
    A: BlobGuard + Sync,
    B: BlobGuard + Sync,
{
    type Reader = Either<A::Reader, B::Reader>;
    async fn reader(&self) -> Result<Self::Reader> {
        match self {
            Either::L(a) => Ok(Either::L(a.reader().await?)),
            Either::R(b) => Ok(Either::R(b.reader().await?)),
        }
    }
}
