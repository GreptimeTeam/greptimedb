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

mod file;
mod footer;

use std::collections::HashMap;

use async_trait::async_trait;

use crate::blob_metadata::CompressionCodec;
use crate::error::Result;
pub use crate::file_format::writer::file::PuffinFileWriter;

/// Blob ready to be written
pub struct Blob<R> {
    // TODO(zhongzc): ignore `input_fields`, `snapshot_id`, `sequence_number`
    // and `compression_codec` for now to keep thing simple
    /// The type of the blob
    pub blob_type: String,

    /// The data of the blob
    pub compressed_data: R,

    /// The codec used to compress the blob.
    pub compression_codec: Option<CompressionCodec>,

    /// The properties of the blob
    pub properties: HashMap<String, String>,
}

/// `SyncWriter` defines a synchronous writer for puffin data.
pub trait SyncWriter {
    /// Set the properties of the Puffin file
    fn set_properties(&mut self, properties: HashMap<String, String>);

    /// Sets whether the footer payload should be LZ4 compressed.
    fn set_footer_lz4_compressed(&mut self, lz4_compressed: bool);

    /// Add a blob to the Puffin file
    fn add_blob<R: std::io::Read>(&mut self, blob: Blob<R>) -> Result<u64>;

    /// Finish writing the Puffin file, returns the number of bytes written
    fn finish(&mut self) -> Result<u64>;
}

/// `AsyncWriter` defines an asynchronous writer for puffin data.
#[async_trait]
pub trait AsyncWriter {
    /// Set the properties of the Puffin file
    fn set_properties(&mut self, properties: HashMap<String, String>);

    /// Sets whether the footer payload should be LZ4 compressed.
    fn set_footer_lz4_compressed(&mut self, lz4_compressed: bool);

    /// Add a blob to the Puffin file
    async fn add_blob<R: futures::AsyncRead + Send>(&mut self, blob: Blob<R>) -> Result<u64>;

    /// Finish writing the Puffin file, returns the number of bytes written
    async fn finish(&mut self) -> Result<u64>;
}
