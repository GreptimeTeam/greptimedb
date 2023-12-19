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

mod external_provider;
mod external_sort;
mod intermediate_rw;
mod merge_stream;

use async_trait::async_trait;
use common_base::BitVec;
use futures::Stream;

use crate::inverted_index::error::Result;
use crate::inverted_index::{Bytes, BytesRef};

/// A stream of sorted values along with their associated bitmap
pub type SortedStream = Box<dyn Stream<Item = Result<(Bytes, BitVec)>> + Send + Unpin>;

/// Output of a sorting operation, encapsulating a bitmap for null values and a stream of sorted items
pub struct SortOutput {
    /// Bitmap indicating which segments have null values
    pub segment_null_bitmap: BitVec,

    /// Stream of sorted items
    pub sorted_stream: SortedStream,

    /// Total number of rows in the sorted data
    pub total_row_count: usize,
}

/// Handles data sorting, supporting incremental input and retrieval of sorted output
#[async_trait]
pub trait Sorter: Send {
    /// Inputs a non-null or null value into the sorter.
    /// Should be equivalent to calling `push_n` with n = 1
    async fn push(&mut self, value: Option<BytesRef<'_>>) -> Result<()> {
        self.push_n(value, 1).await
    }

    /// Pushing n identical non-null or null values into the sorter.
    /// Should be equivalent to calling `push` n times
    async fn push_n(&mut self, value: Option<BytesRef<'_>>, n: usize) -> Result<()>;

    /// Completes the sorting process and returns the sorted data
    async fn output(&mut self) -> Result<SortOutput>;
}
