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

pub mod external_sort;
mod intermediate_rw;
mod merge_stream;

use async_trait::async_trait;
use futures::Stream;

use crate::bitmap::Bitmap;
use crate::inverted_index::error::Result;
use crate::inverted_index::format::writer::ValueStream;
use crate::{Bytes, BytesRef};

/// A stream of sorted values along with their associated bitmap
pub type SortedStream = Box<dyn Stream<Item = Result<(Bytes, Bitmap)>> + Send + Unpin>;

/// Output of a sorting operation, encapsulating a bitmap for null values and a stream of sorted items
pub struct SortOutput {
    /// Bitmap indicating which segments have null values
    pub segment_null_bitmap: Bitmap,

    /// Stream of sorted items
    pub sorted_stream: ValueStream,

    /// Total number of rows in the sorted data
    pub total_row_count: usize,
}

/// Handles data sorting, supporting incremental input and retrieval of sorted output
#[async_trait]
pub trait Sorter: Send {
    /// Buffers `n` identical non-null or null values in memory.
    ///
    /// Returns true when the buffer should be spilled with [`Sorter::spill`] before more
    /// values are pushed. Kept synchronous so the per-row path does not allocate a future.
    fn push_n(&mut self, value: Option<BytesRef<'_>>, n: usize) -> bool;

    /// Moves the in-memory buffer to external storage.
    async fn spill(&mut self) -> Result<()>;

    /// Completes the sorting process and returns the sorted data
    async fn output(&mut self) -> Result<SortOutput>;
}
