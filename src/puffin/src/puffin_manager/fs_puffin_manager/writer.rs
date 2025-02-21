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
use futures::{AsyncRead, AsyncWrite, StreamExt};
use snafu::{ensure, ResultExt};
use tokio_util::compat::TokioAsyncReadCompatExt;
use uuid::Uuid;

use crate::blob_metadata::CompressionCodec;
use crate::error::{
    DuplicateBlobSnafu, MetadataSnafu, OpenSnafu, Result, SerializeJsonSnafu,
    UnsupportedCompressionSnafu, WalkDirSnafu,
};
use crate::file_format::writer::{AsyncWriter, Blob, PuffinFileWriter};
use crate::puffin_manager::fs_puffin_manager::dir_meta::{DirFileMetadata, DirMetadata};
use crate::puffin_manager::stager::Stager;
use crate::puffin_manager::{PuffinWriter, PutOptions};

/// `FsPuffinWriter` is a `PuffinWriter` that writes blobs and directories to a puffin file.
pub struct FsPuffinWriter<S: Stager, W> {
    /// The name of the puffin file.
    handle: S::FileHandle,

    /// The stager.
    stager: S,

    /// The underlying `PuffinFileWriter`.
    puffin_file_writer: PuffinFileWriter<W>,

    /// Written blob keys.
    blob_keys: HashSet<String>,
}

impl<S: Stager, W> FsPuffinWriter<S, W> {
    pub(crate) fn new(handle: S::FileHandle, stager: S, writer: W) -> Self {
        Self {
            handle,
            stager,
            puffin_file_writer: PuffinFileWriter::new(writer),
            blob_keys: HashSet::new(),
        }
    }
}

#[async_trait]
impl<S, W> PuffinWriter for FsPuffinWriter<S, W>
where
    S: Stager,
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

        let written_bytes = self
            .handle_compress(key.to_string(), raw_data, options.compression)
            .await?;

        self.blob_keys.insert(key.to_string());
        Ok(written_bytes)
    }

    async fn put_dir(&mut self, key: &str, dir_path: PathBuf, options: PutOptions) -> Result<u64> {
        ensure!(
            !self.blob_keys.contains(key),
            DuplicateBlobSnafu { blob: key }
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
            written_bytes += self
                .handle_compress(file_key.clone(), reader, options.compression)
                .await?;

            let path = entry.path();
            let relative_path = path
                .strip_prefix(&dir_path)
                .expect("entry path is under dir path");

            let unified_rel_path = if cfg!(windows) {
                relative_path.to_string_lossy().replace('\\', "/")
            } else {
                relative_path.to_string_lossy().to_string()
            };

            files.push(DirFileMetadata {
                relative_path: unified_rel_path,
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

        // Move the directory into the stager.
        self.stager
            .put_dir(&self.handle, key, dir_path, dir_size)
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

impl<S, W> FsPuffinWriter<S, W>
where
    S: Stager,
    W: AsyncWrite + Unpin + Send,
{
    /// Compresses the raw data and writes it to the puffin file.
    async fn handle_compress(
        &mut self,
        key: String,
        raw_data: impl AsyncRead + Send,
        compression: Option<CompressionCodec>,
    ) -> Result<u64> {
        match compression {
            Some(CompressionCodec::Lz4) => UnsupportedCompressionSnafu { codec: "lz4" }.fail(),
            Some(CompressionCodec::Zstd) => {
                let blob = Blob {
                    blob_type: key,
                    compressed_data: ZstdEncoder::new(BufReader::new(raw_data)),
                    compression_codec: compression,
                    properties: Default::default(),
                };
                self.puffin_file_writer.add_blob(blob).await
            }
            None => {
                let blob = Blob {
                    blob_type: key,
                    compressed_data: raw_data,
                    compression_codec: compression,
                    properties: Default::default(),
                };
                self.puffin_file_writer.add_blob(blob).await
            }
        }
    }
}
