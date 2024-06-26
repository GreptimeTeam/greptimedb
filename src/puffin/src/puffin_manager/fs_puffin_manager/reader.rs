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

use async_compression::futures::bufread::ZstdDecoder;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::io::BufReader;
use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncWrite};
use snafu::{ensure, OptionExt, ResultExt};

use crate::blob_metadata::CompressionCodec;
use crate::error::{
    BlobIndexOutOfBoundSnafu, BlobNotFoundSnafu, DeserializeJsonSnafu, FileKeyNotMatchSnafu,
    ReadSnafu, Result, UnsupportedDecompressionSnafu, WriteSnafu,
};
use crate::file_format::reader::{AsyncReader, PuffinFileReader};
use crate::puffin_manager::file_accessor::PuffinFileAccessorRef;
use crate::puffin_manager::fs_puffin_manager::dir_meta::DirMetadata;
use crate::puffin_manager::stager::{BoxWriter, DirWriterProviderRef, StagerRef};
use crate::puffin_manager::{BlobGuard, DirGuard, PuffinReader};

/// `FsPuffinReader` is a `PuffinReader` that provides fs readers for puffin files.
pub struct FsPuffinReader<B, G, AR, AW> {
    /// The name of the puffin file.
    puffin_file_name: String,

    /// The stager.
    stager: StagerRef<B, G>,

    /// The puffin file accessor.
    puffin_file_accessor: PuffinFileAccessorRef<AR, AW>,
}

impl<B, D, AR, AW> FsPuffinReader<B, D, AR, AW> {
    pub(crate) fn new(
        puffin_file_name: String,
        stager: StagerRef<B, D>,
        puffin_file_accessor: PuffinFileAccessorRef<AR, AW>,
    ) -> Self {
        Self {
            puffin_file_name,
            stager,
            puffin_file_accessor,
        }
    }
}

#[async_trait]
impl<B, D, AR, AW> PuffinReader for FsPuffinReader<B, D, AR, AW>
where
    B: BlobGuard,
    D: DirGuard,
    AR: AsyncRead + AsyncSeek + Send + Unpin + 'static,
    AW: AsyncWrite + 'static,
{
    type Blob = B;
    type Dir = D;

    async fn blob(&self, key: &str) -> Result<Self::Blob> {
        self.stager
            .get_blob(
                self.puffin_file_name.as_str(),
                key,
                Box::new(move |writer| {
                    let accessor = self.puffin_file_accessor.clone();
                    let puffin_file_name = self.puffin_file_name.clone();
                    let key = key.to_string();
                    Self::init_blob_to_cache(puffin_file_name, key, writer, accessor)
                }),
            )
            .await
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
                    Self::init_dir_to_cache(puffin_file_name, key, writer_provider, accessor)
                }),
            )
            .await
    }
}

impl<B, G, AR, AW> FsPuffinReader<B, G, AR, AW>
where
    B: BlobGuard,
    G: DirGuard,
    AR: AsyncRead + AsyncSeek + Send + Unpin + 'static,
    AW: AsyncWrite + 'static,
{
    fn init_blob_to_cache(
        puffin_file_name: String,
        key: String,
        mut writer: BoxWriter,
        accessor: PuffinFileAccessorRef<AR, AW>,
    ) -> BoxFuture<'static, Result<u64>> {
        Box::pin(async move {
            let reader = accessor.reader(&puffin_file_name).await?;
            let mut file = PuffinFileReader::new(reader);

            let metadata = file.metadata().await?;
            let blob_metadata = metadata
                .blobs
                .iter()
                .find(|m| m.blob_type == key.as_str())
                .context(BlobNotFoundSnafu { blob: key })?;
            let reader = file.blob_reader(blob_metadata)?;

            let compression = blob_metadata.compression_codec;
            let size = Self::handle_decompress(reader, &mut writer, compression).await?;

            Ok(size)
        })
    }

    fn init_dir_to_cache(
        puffin_file_name: String,
        key: String,
        writer_provider: DirWriterProviderRef,
        accessor: PuffinFileAccessorRef<AR, AW>,
    ) -> BoxFuture<'static, Result<u64>> {
        Box::pin(async move {
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

            let mut size = 0;
            for file_meta in dir_meta.files {
                let blob_meta = puffin_metadata.blobs.get(file_meta.blob_index).context(
                    BlobIndexOutOfBoundSnafu {
                        index: file_meta.blob_index,
                        max_index: puffin_metadata.blobs.len(),
                    },
                )?;
                ensure!(
                    blob_meta.blob_type == file_meta.key,
                    FileKeyNotMatchSnafu {
                        expected: file_meta.key,
                        actual: &blob_meta.blob_type,
                    }
                );

                let reader = file.blob_reader(blob_meta)?;
                let writer = writer_provider.writer(&file_meta.relative_path).await?;

                let compression = blob_meta.compression_codec;
                size += Self::handle_decompress(reader, writer, compression).await?;
            }

            Ok(size)
        })
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
