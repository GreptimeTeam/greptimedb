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

use std::collections::BTreeSet;

use async_trait::async_trait;
pub use predicates_apply::PredicatesIndexApplier;

use crate::inverted_index::error::Result;
use crate::inverted_index::format::reader::InvertedIndexReader;

/// A trait for processing and transforming indices obtained from an inverted index.
///
/// Applier instances are reusable and work with various `InvertedIndexReader` instances,
/// avoiding repeated compilation of fixed predicates such as regex patterns.
#[mockall::automock]
#[async_trait]
pub trait IndexApplier {
    /// Applies the predefined predicates to the data read by the given index reader, returning
    /// a list of relevant indices (e.g., post IDs, group IDs, row IDs).
    async fn apply<'a>(
        &self,
        context: SearchContext,
        reader: &mut (dyn InvertedIndexReader + 'a),
    ) -> Result<BTreeSet<usize>>;

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
