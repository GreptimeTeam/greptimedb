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

use std::num::NonZeroUsize;

use async_trait::async_trait;
use futures::Stream;

use crate::bitmap::{Bitmap, BitmapType};
use crate::inverted_index::error::Result;
pub use crate::inverted_index::format::writer::blob::InvertedIndexBlobWriter;
use crate::Bytes;

pub type ValueStream = Box<dyn Stream<Item = Result<(Bytes, Bitmap)>> + Send + Unpin>;

/// Trait for writing inverted index data to underlying storage.
#[mockall::automock]
#[async_trait]
pub trait InvertedIndexWriter: Send {
    /// Adds entries to an index.
    ///
    /// * `name` is the index identifier.
    /// * `null_bitmap` marks positions of null entries.
    /// * `values` is a stream of values and their locations, yielded lexicographically.
    ///    Errors occur if the values are out of order.
    /// * `bitmap_type` is the type of bitmap to encode.
    async fn add_index(
        &mut self,
        name: String,
        null_bitmap: Bitmap,
        values: ValueStream,
        bitmap_type: BitmapType,
    ) -> Result<()>;

    /// Finalizes the index writing process, ensuring all data is written.
    /// `total_row_count` and `segment_row_count` is used to fill in the metadata.
    async fn finish(&mut self, total_row_count: u64, segment_row_count: NonZeroUsize)
        -> Result<()>;
}
