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

use std::sync::Arc;

use async_trait::async_trait;
use common_base::BitVec;
use greptime_proto::v1::index::InvertedIndexMetas;
use snafu::ResultExt;

use crate::inverted_index::error::{DecodeFstSnafu, Result};
pub use crate::inverted_index::format::reader::blob::InvertedIndexBlobReader;
use crate::inverted_index::FstMap;

mod blob;
mod footer;

/// InvertedIndexReader defines an asynchronous reader of inverted index data
#[mockall::automock]
#[async_trait]
pub trait InvertedIndexReader: Send {
    /// Reads all data to dest.
    async fn read_all(&mut self, dest: &mut Vec<u8>) -> Result<usize>;

    /// Seeks to given offset and reads data with exact size as provided.
    async fn seek_read(&mut self, offset: u64, size: u32) -> Result<Vec<u8>>;

    /// Retrieves metadata of all inverted indices stored within the blob.
    async fn metadata(&mut self) -> Result<Arc<InvertedIndexMetas>>;

    /// Retrieves the finite state transducer (FST) map from the given offset and size.
    async fn fst(&mut self, offset: u64, size: u32) -> Result<FstMap> {
        let fst_data = self.seek_read(offset, size).await?;
        FstMap::new(fst_data).context(DecodeFstSnafu)
    }

    /// Retrieves the bitmap from the given offset and size.
    async fn bitmap(&mut self, offset: u64, size: u32) -> Result<BitVec> {
        self.seek_read(offset, size).await.map(BitVec::from_vec)
    }
}
