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

use std::collections::HashSet;
use std::path::PathBuf;

use async_compression::futures::bufread::ZstdEncoder;
use async_trait::async_trait;
use futures::io::BufReader;
use futures::{AsyncRead, AsyncSeek, AsyncWrite, StreamExt};
use snafu::{ensure, ResultExt};
use tokio_util::compat::TokioAsyncReadCompatExt;
use uuid::Uuid;

use crate::blob_metadata::CompressionCodec;
use crate::error::{
    DuplicateBlobSnafu, MetadataSnafu, OpenSnafu, Result, SerializeJsonSnafu,
    UnsupportedCompressionSnafu, WalkDirSnafu,
};
use crate::file_format::writer::{Blob, PuffinAsyncWriter, PuffinFileWriter};
use crate::puffin_manager::cache_manager::CacheManagerRef;
use crate::puffin_manager::cached_puffin_manager::dir_meta::{DirFileMetadata, DirMetadata};
use crate::puffin_manager::{DirGuard, PuffinWriter, PutOptions};

/// `CachedPuffinWriter` is a `PuffinWriter` that writes blobs and directories to a puffin file.
pub struct CachedPuffinWriter<CR, G, W> {
    /// The name of the puffin file.
    puffin_file_name: String,

    /// The cache manager.
    cache_manager: CacheManagerRef<CR, G>,

    /// The underlying `PuffinFileWriter`.
    puffin_file_writer: PuffinFileWriter<W>,

    /// Written blob keys.
    blob_keys: HashSet<String>,
}

impl<CR, G, W> CachedPuffinWriter<CR, G, W> {
    #[allow(unused)]
    pub(crate) fn new(
        puffin_file_name: String,
        cache_manager: CacheManagerRef<CR, G>,
        writer: W,
    ) -> Self {
        Self {
            puffin_file_name,
            cache_manager,
            puffin_file_writer: PuffinFileWriter::new(writer),
            blob_keys: HashSet::new(),
        }
    }
}

#[async_trait]
impl<CR, G, W> PuffinWriter for CachedPuffinWriter<CR, G, W>
where
    CR: AsyncRead + AsyncSeek,
    G: DirGuard,
    W: AsyncWrite + Unpin + Send,
{
    async fn put_blob<R>(&mut self, key: &str, raw_data: R, options: PutOptions) -> Result<u64>
    where
        R: AsyncRead + Send,
    {
        ensure!(
            !self.blob_keys.contains(key),
            DuplicateBlobSnafu { blob: key }
        );
        ensure!(
            !matches!(options.compression, Some(CompressionCodec::Lz4)),
            UnsupportedCompressionSnafu { codec: "lz4" }
        );

        let written_bytes = match options.compression {
            Some(CompressionCodec::Lz4) => unreachable!("checked above"),
            Some(CompressionCodec::Zstd) => {
                let blob = Blob {
                    blob_type: key.to_string(),
                    compressed_data: ZstdEncoder::new(BufReader::new(raw_data)),
                    compression_codec: options.compression,
                    properties: Default::default(),
                };
                self.puffin_file_writer.add_blob(blob).await?
            }
            None => {
                let blob = Blob {
                    blob_type: key.to_string(),
                    compressed_data: raw_data,
                    compression_codec: options.compression,
                    properties: Default::default(),
                };
                self.puffin_file_writer.add_blob(blob).await?
            }
        };

        self.blob_keys.insert(key.to_string());
        Ok(written_bytes)
    }

    async fn put_dir(&mut self, key: &str, dir_path: PathBuf, options: PutOptions) -> Result<u64> {
        ensure!(
            !self.blob_keys.contains(key),
            DuplicateBlobSnafu { blob: key }
        );
        ensure!(
            !matches!(options.compression, Some(CompressionCodec::Lz4)),
            UnsupportedCompressionSnafu { codec: "lz4" }
        );

        // Walk the directory and add all files to the puffin file.
        let mut wd = async_walkdir::WalkDir::new(&dir_path).filter(|entry| async move {
            match entry.file_type().await {
                // Ignore directories.
                Ok(ft) if ft.is_dir() => async_walkdir::Filtering::Ignore,
                _ => async_walkdir::Filtering::Continue,
            }
        });

        let mut dir_size = 0;
        let mut written_bytes = 0;
        let mut files = vec![];
        while let Some(entry) = wd.next().await {
            let entry = entry.context(WalkDirSnafu)?;
            dir_size += entry.metadata().await.context(MetadataSnafu)?.len();

            let reader = tokio::fs::File::open(entry.path())
                .await
                .context(OpenSnafu)?
                .compat();

            let file_key = Uuid::new_v4().to_string();
            match options.compression {
                Some(CompressionCodec::Lz4) => unreachable!("checked above"),
                Some(CompressionCodec::Zstd) => {
                    let blob = Blob {
                        blob_type: file_key.clone(),
                        compressed_data: ZstdEncoder::new(BufReader::new(reader)),
                        compression_codec: options.compression,
                        properties: Default::default(),
                    };
                    written_bytes += self.puffin_file_writer.add_blob(blob).await?;
                }
                None => {
                    let blob = Blob {
                        blob_type: file_key.clone(),
                        compressed_data: reader,
                        compression_codec: options.compression,
                        properties: Default::default(),
                    };
                    written_bytes += self.puffin_file_writer.add_blob(blob).await?;
                }
            }

            let relative_path = entry
                .path()
                .strip_prefix(&dir_path)
                .expect("entry path is under dir path")
                .to_string_lossy()
                .into_owned();

            files.push(DirFileMetadata {
                relative_path,
                key: file_key.clone(),
                blob_index: self.blob_keys.len(),
            });
            self.blob_keys.insert(file_key);
        }

        let dir_metadata = DirMetadata { files };
        let encoded = serde_json::to_vec(&dir_metadata).context(SerializeJsonSnafu)?;
        let dir_meta_blob = Blob {
            blob_type: key.to_string(),
            compressed_data: encoded.as_slice(),
            compression_codec: None,
            properties: Default::default(),
        };

        written_bytes += self.puffin_file_writer.add_blob(dir_meta_blob).await?;
        self.blob_keys.insert(key.to_string());

        // Move the directory into the cache.
        self.cache_manager
            .put_dir(&self.puffin_file_name, key, dir_path, dir_size)
            .await?;
        Ok(written_bytes)
    }

    fn set_footer_lz4_compressed(&mut self, lz4_compressed: bool) {
        self.puffin_file_writer
            .set_footer_lz4_compressed(lz4_compressed);
    }

    async fn finish(mut self) -> Result<u64> {
        let size = self.puffin_file_writer.finish().await?;
        Ok(size)
    }
}
