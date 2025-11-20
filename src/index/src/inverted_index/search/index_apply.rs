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

mod predicates_apply;

use async_trait::async_trait;
pub use predicates_apply::PredicatesIndexApplier;

use crate::bitmap::Bitmap;
use crate::inverted_index::error::Result;
use crate::inverted_index::format::reader::{InvertedIndexReadMetrics, InvertedIndexReader};

/// The output of an apply operation.
#[derive(Clone, Debug, PartialEq)]
pub struct ApplyOutput {
    /// Bitmap of indices that match the predicates.
    pub matched_segment_ids: Bitmap,

    /// The total number of rows in the index.
    pub total_row_count: usize,

    /// The number of rows in each segment.
    pub segment_row_count: usize,
}

/// A trait for processing and transforming indices obtained from an inverted index.
///
/// Applier instances are reusable and work with various `InvertedIndexReader` instances,
/// avoiding repeated compilation of fixed predicates such as regex patterns.
#[mockall::automock]
#[async_trait]
pub trait IndexApplier: Send + Sync {
    /// Applies the predefined predicates to the data read by the given index reader, returning
    /// a list of relevant indices (e.g., post IDs, group IDs, row IDs).
    #[allow(unused_parens)]
    async fn apply<'a, 'b>(
        &self,
        context: SearchContext,
        reader: &mut (dyn InvertedIndexReader + 'a),
        metrics: Option<&'b mut InvertedIndexReadMetrics>,
    ) -> Result<ApplyOutput>;

    /// Returns the memory usage of the applier.
    fn memory_usage(&self) -> usize;
}

/// A context for searching the inverted index.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct SearchContext {
    /// `index_not_found_strategy` controls the behavior of the applier when the index is not found.
    pub index_not_found_strategy: IndexNotFoundStrategy,
}

/// Defines the behavior of an applier when the index is not found.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum IndexNotFoundStrategy {
    /// Return an empty list of indices.
    #[default]
    ReturnEmpty,

    /// Ignore the index and continue.
    Ignore,

    /// Throw an error.
    ThrowError,
}
