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

mod blob;
mod footer;

use async_trait::async_trait;
use common_base::BitVec;
use greptime_proto::v1::index::{InvertedIndexMeta, InvertedIndexMetas};

use crate::inverted_index::error::Result;
pub use crate::inverted_index::format::reader::blob::InvertedIndexBlobReader;
use crate::inverted_index::FstMap;

/// InvertedIndexReader defines an asynchronous reader of inverted index data
#[mockall::automock]
#[async_trait]
pub trait InvertedIndexReader: Send {
    /// Retrieve metadata of all inverted indices stored within the blob.
    async fn metadata(&mut self) -> Result<InvertedIndexMetas>;

    /// Retrieve the finite state transducer (FST) map for a given inverted index metadata entry.
    async fn fst(&mut self, meta: &InvertedIndexMeta) -> Result<FstMap>;

    /// Retrieve the bitmap for a given inverted index metadata entry at the specified offset and size.
    async fn bitmap(
        &mut self,
        meta: &InvertedIndexMeta,
        relative_offset: u32,
        size: u32,
    ) -> Result<BitVec>;
}
