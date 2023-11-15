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

use async_trait::async_trait;

use crate::blob_metadata::BlobMetadata;
use crate::error::Result;
pub use crate::file_format::reader::file::PuffinFileReader;
use crate::file_metadata::FileMetadata;

/// `PuffinSyncReader` defines a synchronous reader for puffin data.
pub trait PuffinSyncReader<'a> {
    type Reader: std::io::Read + std::io::Seek;

    /// fetch the FileMetadata
    fn metadata(&'a mut self) -> Result<FileMetadata>;

    /// read particular blob data based on given metadata
    fn blob_reader(&'a mut self, blob_metadata: &BlobMetadata) -> Result<Self::Reader>;
}

/// `PuffinAsyncReader` defines an asynchronous reader for puffin data.
#[async_trait]
pub trait PuffinAsyncReader<'a> {
    type Reader: futures::AsyncRead + futures::AsyncSeek;

    /// fetch the FileMetadata
    async fn metadata(&'a mut self) -> Result<FileMetadata>;

    /// read particular blob data based on given metadata
    fn blob_reader(&'a mut self, blob_metadata: &BlobMetadata) -> Result<Self::Reader>;
}
