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
use std::sync::Arc;

use async_compression::futures::bufread::ZstdDecoder;
use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use common_base::range_read::{AsyncReadAdapter, Metadata, RangeReader, SizeAwareRangeReader};
use futures::io::BufReader;
use futures::{AsyncRead, AsyncWrite};
use snafu::{OptionExt, ResultExt, ensure};

use crate::blob_metadata::{BlobMetadata, CompressionCodec};
use crate::error::{
    BlobIndexOutOfBoundSnafu, BlobNotFoundSnafu, DeserializeJsonSnafu, FileKeyNotMatchSnafu,
    MetadataSnafu, ReadSnafu, Result, UnsupportedDecompressionSnafu, WriteSnafu,
};
use crate::file_format::reader::{AsyncReader, PuffinFileReader};
use crate::file_metadata::FileMetadata;
use crate::partial_reader::PartialReader;
use crate::puffin_manager::file_accessor::PuffinFileAccessor;
use crate::puffin_manager::fs_puffin_manager::PuffinMetadataCacheRef;
use crate::puffin_manager::fs_puffin_manager::dir_meta::DirMetadata;
use crate::puffin_manager::stager::{BoxWriter, DirWriterProviderRef, Stager};
use crate::puffin_manager::{BlobGuard, DirMetrics, GuardWithMetadata, PuffinReader};

/// `FsPuffinReader` is a `PuffinReader` that provides fs readers for puffin files.
pub struct FsPuffinReader<S, F>
where
    S: Stager + 'static,
    F: PuffinFileAccessor + Clone,
{
    /// The handle of the puffin file.
    handle: F::FileHandle,

    /// The file size hint.
    file_size_hint: Option<u64>,

    /// The stager.
    stager: S,

    /// The puffin file accessor.
    puffin_file_accessor: F,

    /// The puffin file metadata cache.
    puffin_file_metadata_cache: Option<PuffinMetadataCacheRef>,
}

impl<S, F> FsPuffinReader<S, F>
where
    S: Stager + 'static,
    F: PuffinFileAccessor + Clone,
{
    pub(crate) fn new(
        handle: F::FileHandle,
        stager: S,
        puffin_file_accessor: F,
        puffin_file_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        Self {
            handle,
            file_size_hint: None,
            stager,
            puffin_file_accessor,
            puffin_file_metadata_cache,
        }
    }
}

#[async_trait]
impl<S, F> PuffinReader for FsPuffinReader<S, F>
where
    F: PuffinFileAccessor + Clone,
    S: Stager<FileHandle = F::FileHandle> + 'static,
{
    type Blob = Either<RandomReadBlob<F>, S::Blob>;
    type Dir = S::Dir;

    fn with_file_size_hint(mut self, file_size_hint: Option<u64>) -> Self {
        self.file_size_hint = file_size_hint;
        self
    }

    async fn metadata(&self) -> Result<Arc<FileMetadata>> {
        let mut file = self.puffin_reader().await?;
        self.get_puffin_file_metadata(&mut file).await
    }

    async fn blob(&self, key: &str) -> Result<GuardWithMetadata<Self::Blob>> {
        let mut file = self.puffin_reader().await?;
        let blob_metadata = self.get_blob_metadata(key, &mut file).await?;
        let blob = if blob_metadata.compression_codec.is_none() {
            // If the blob is not compressed, we can directly read it from the puffin file.
            Either::L(RandomReadBlob {
                handle: self.handle.clone(),
                accessor: self.puffin_file_accessor.clone(),
                blob_metadata: blob_metadata.clone(),
            })
        } else {
            // If the blob is compressed, we need to decompress it into staging space before reading.
            let blob_metadata = blob_metadata.clone();
            let staged_blob = self
                .stager
                .get_blob(
                    &self.handle,
                    key,
                    Box::new(|writer| {
                        Box::pin(Self::init_blob_to_stager(file, blob_metadata, writer))
                    }),
                )
                .await?;

            Either::R(staged_blob)
        };

        Ok(GuardWithMetadata::new(blob, blob_metadata))
    }

    async fn dir(&self, key: &str) -> Result<(GuardWithMetadata<Self::Dir>, DirMetrics)> {
        let mut file = self.puffin_reader().await?;
        let blob_metadata = self.get_blob_metadata(key, &mut file).await?;
        let (dir, metrics) = self
            .stager
            .get_dir(
                &self.handle,
                key,
                Box::new(|writer_provider| {
                    let accessor = self.puffin_file_accessor.clone();
                    let handle = self.handle.clone();
                    let blob_metadata = blob_metadata.clone();
                    Box::pin(Self::init_dir_to_stager(
                        file,
                        blob_metadata,
                        handle,
                        writer_provider,
                        accessor,
                    ))
                }),
            )
            .await?;

        Ok((GuardWithMetadata::new(dir, blob_metadata), metrics))
    }
}

impl<S, F> FsPuffinReader<S, F>
where
    S: Stager,
    F: PuffinFileAccessor + Clone,
{
    async fn get_puffin_file_metadata(
        &self,
        reader: &mut PuffinFileReader<F::Reader>,
    ) -> Result<Arc<FileMetadata>> {
        let id = self.handle.to_string();
        if let Some(cache) = self.puffin_file_metadata_cache.as_ref()
            && let Some(metadata) = cache.get_metadata(&id)
        {
            return Ok(metadata);
        }

        let metadata = Arc::new(reader.metadata().await?);
        if let Some(cache) = self.puffin_file_metadata_cache.as_ref() {
            cache.put_metadata(id, metadata.clone());
        }
        Ok(metadata)
    }

    async fn get_blob_metadata(
        &self,
        key: &str,
        file: &mut PuffinFileReader<F::Reader>,
    ) -> Result<BlobMetadata> {
        let metadata = self.get_puffin_file_metadata(file).await?;
        let blob_metadata = metadata
            .blobs
            .iter()
            .find(|m| m.blob_type == key)
            .context(BlobNotFoundSnafu { blob: key })?
            .clone();

        Ok(blob_metadata)
    }

    async fn puffin_reader(&self) -> Result<PuffinFileReader<F::Reader>> {
        let mut reader = self.puffin_file_accessor.reader(&self.handle).await?;
        if let Some(file_size_hint) = self.file_size_hint {
            reader.with_file_size_hint(file_size_hint);
        }
        Ok(PuffinFileReader::new(reader))
    }

    async fn init_blob_to_stager(
        reader: PuffinFileReader<F::Reader>,
        blob_metadata: BlobMetadata,
        mut writer: BoxWriter,
    ) -> Result<u64> {
        let reader = reader.into_blob_reader(&blob_metadata);
        let reader = AsyncReadAdapter::new(reader).await.context(MetadataSnafu)?;
        let compression = blob_metadata.compression_codec;
        let size = Self::handle_decompress(reader, &mut writer, compression).await?;
        Ok(size)
    }

    async fn init_dir_to_stager(
        mut file: PuffinFileReader<F::Reader>,
        blob_metadata: BlobMetadata,
        handle: F::FileHandle,
        writer_provider: DirWriterProviderRef,
        accessor: F,
    ) -> Result<u64> {
        let puffin_metadata = file.metadata().await?;
        let reader = file.blob_reader(&blob_metadata)?;
        let meta = reader.metadata().await.context(MetadataSnafu)?;
        let buf = reader
            .read(0..meta.content_length)
            .await
            .context(ReadSnafu)?;
        let dir_meta: DirMetadata = serde_json::from_slice(&buf).context(DeserializeJsonSnafu)?;

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

            let reader = accessor.reader(&handle).await?;
            let writer = writer_provider.writer(&file_meta.relative_path).await?;
            let task = common_runtime::spawn_global(async move {
                let reader = PuffinFileReader::new(reader).into_blob_reader(&blob_meta);
                let reader = AsyncReadAdapter::new(reader).await.context(MetadataSnafu)?;
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
pub struct RandomReadBlob<F: PuffinFileAccessor> {
    handle: F::FileHandle,
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

        let reader = self.accessor.reader(&self.handle).await?;
        let blob_reader = PuffinFileReader::new(reader).into_blob_reader(&self.blob_metadata);
        Ok(blob_reader)
    }
}

/// `Either` is a type that represents either `A` or `B`.
///
/// Used to:
/// impl `RangeReader` for `Either<A: RangeReader, B: RangeReader>`,
/// impl `BlobGuard` for `Either<A: BlobGuard, B: BlobGuard>`.
pub enum Either<A, B> {
    L(A),
    R(B),
}

#[async_trait]
impl<A, B> RangeReader for Either<A, B>
where
    A: RangeReader,
    B: RangeReader,
{
    async fn metadata(&self) -> io::Result<Metadata> {
        match self {
            Either::L(a) => a.metadata().await,
            Either::R(b) => b.metadata().await,
        }
    }
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        match self {
            Either::L(a) => a.read(range).await,
            Either::R(b) => b.read(range).await,
        }
    }
    async fn read_into(&self, range: Range<u64>, buf: &mut (impl BufMut + Send)) -> io::Result<()> {
        match self {
            Either::L(a) => a.read_into(range, buf).await,
            Either::R(b) => b.read_into(range, buf).await,
        }
    }
    async fn read_vec(&self, ranges: &[Range<u64>]) -> io::Result<Vec<Bytes>> {
        match self {
            Either::L(a) => a.read_vec(ranges).await,
            Either::R(b) => b.read_vec(ranges).await,
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
