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

//! Index identities shared by planning, execution and publication.

use crate::sst::file::IndexType;

/// An index implementation known to the storage engine or supplied by a plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexKind {
    /// An existing built-in index format.
    Builtin(IndexType),
    /// An index whose implementation is supplied by the embedding application.
    Extension(ExtensionIndexKind),
}

/// Persisted extension algorithm identities; implementations remain outside mito2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExtensionIndexKind {
    /// HyperLogLog distinct-value estimate.
    Hll,
    /// KLL rank estimate.
    Kll,
    /// Misra–Gries frequency estimate.
    Mg,
}

impl ExtensionIndexKind {
    /// Stable name used in persisted metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hll => "hll",
            Self::Kll => "kll",
            Self::Mg => "mg",
        }
    }
}

impl IndexKind {
    /// Stable label used by the existing index metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Builtin(IndexType::InvertedIndex) => "inverted_index",
            Self::Builtin(IndexType::FulltextIndex) => "fulltext_index",
            Self::Builtin(IndexType::BloomFilterIndex) => "bloom_filter_index",
            Self::Builtin(IndexType::VectorIndex) => "vector_index",
            Self::Builtin(IndexType::Hll) => "hll",
            Self::Builtin(IndexType::Kll) => "kll",
            Self::Builtin(IndexType::Mg) => "mg",
            Self::Extension(kind) => kind.as_str(),
        }
    }

    /// Returns the identity recorded in file and column index metadata.
    pub fn index_type(self) -> IndexType {
        match self {
            Self::Builtin(kind) => kind,
            Self::Extension(ExtensionIndexKind::Hll) => IndexType::Hll,
            Self::Extension(ExtensionIndexKind::Kll) => IndexType::Kll,
            Self::Extension(ExtensionIndexKind::Mg) => IndexType::Mg,
        }
    }

    /// Whether this kind is implemented by the storage engine.
    pub fn is_builtin(self) -> bool {
        matches!(self, Self::Builtin(_))
    }

    /// Orders built-ins as before, followed by extension algorithms.
    pub fn sort_key(self) -> u8 {
        match self {
            Self::Builtin(IndexType::InvertedIndex) => 0,
            Self::Builtin(IndexType::FulltextIndex) => 1,
            Self::Builtin(IndexType::BloomFilterIndex) => 2,
            Self::Builtin(IndexType::VectorIndex) => 3,
            Self::Builtin(IndexType::Hll) | Self::Extension(ExtensionIndexKind::Hll) => 4,
            Self::Builtin(IndexType::Kll) | Self::Extension(ExtensionIndexKind::Kll) => 5,
            Self::Builtin(IndexType::Mg) | Self::Extension(ExtensionIndexKind::Mg) => 6,
        }
    }
}
