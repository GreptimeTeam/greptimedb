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
mod single;

use async_trait::async_trait;
use common_base::BitVec;
use futures::Stream;

use crate::inverted_index::error::Result;
pub use crate::inverted_index::format::writer::blob::InvertedIndexBlobWriter;
use crate::inverted_index::Bytes;

/// Trait for writing inverted index data to underlying storage.
#[async_trait]
pub trait InvertedIndexWriter {
    /// Adds entries to an index.
    ///
    /// * `name` is the index identifier.
    /// * `null_bitmap` marks positions of null entries.
    /// * `values` is a stream of values and their locations, yielded lexicographically.
    ///    Errors occur if the values are out of order.
    async fn add_index<S>(&mut self, name: String, null_bitmap: BitVec, values: S) -> Result<()>
    where
        S: Stream<Item = Result<(Bytes, BitVec)>> + Send + Unpin;

    /// Finalizes the index writing process, ensuring all data is written.
    async fn finish(&mut self) -> Result<()>;
}
