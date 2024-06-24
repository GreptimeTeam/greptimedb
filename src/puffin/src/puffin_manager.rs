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

use std::path::PathBuf;

use async_trait::async_trait;
use futures::{AsyncRead, AsyncSeek};

use crate::blob_metadata::CompressionCodec;
use crate::error::Result;

/// The `PuffinManager` trait provides a unified interface for creating `PuffinReader` and `PuffinWriter`.
#[async_trait]
pub trait PuffinManager {
    type Reader: PuffinReader;
    type Writer: PuffinWriter;

    /// Creates a `PuffinReader` for the specified `puffin_file_name`.
    async fn reader(&self, puffin_file_name: &str) -> Result<Self::Reader>;

    /// Creates a `PuffinWriter` for the specified `puffin_file_name`.
    async fn writer(&self, puffin_file_name: &str) -> Result<Self::Writer>;
}

/// The `PuffinWriter` trait provides methods for writing blobs and directories to a Puffin file.
#[async_trait]
pub trait PuffinWriter {
    /// Writes a blob to the Puffin file.
    async fn put_blob(
        &mut self,
        key: &str,
        raw_data: impl AsyncRead + Send,
        options: Option<PutOptions>,
    ) -> Result<u64>;

    /// Writes a directory from the filesystem to the Puffin file.
    /// The specified `dir` should be accessible from the filesystem.
    async fn put_dir(
        &mut self,
        key: &str,
        dir: PathBuf,
        options: Option<PutOptions>,
    ) -> Result<u64>;

    /// Sets whether the footer should be LZ4 compressed.
    fn set_footer_lz4_compressed(&mut self, lz4_compressed: bool);

    /// Finalizes the Puffin file after writing.
    async fn finish(self) -> Result<u64>;
}

/// Options available for `put_blob` and `put_dir` methods.
pub struct PutOptions {
    /// The compression codec to use for blob data.
    pub data_compression: Option<CompressionCodec>,
}

/// The `PuffinReader` trait provides methods for reading blobs and directories from a Puffin file.
#[async_trait]
pub trait PuffinReader {
    type Reader: AsyncRead + AsyncSeek;

    /// Reads a blob from the Puffin file.
    async fn blob(&self, key: &str) -> Result<Self::Reader>;

    /// Reads a directory from the Puffin file.
    /// The returned `PathBuf` can be used to access the directory in the filesystem.
    async fn dir(&self, key: &str) -> Result<PathBuf>;
}
