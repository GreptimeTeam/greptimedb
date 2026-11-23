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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexKind {
    /// Inverted index.
    Inverted,
    /// Full-text index.
    Fulltext,
    /// Bloom filter index.
    Bloom,
    /// An index whose implementation is supplied by the embedding application.
    Extension(ExtensionIndexKind),
}

/// Extension algorithm identities; implementations remain outside mito2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExtensionIndexKind {
    /// HyperLogLog distinct-value estimate.
    Hll,
    /// KLL rank estimate.
    Kll,
    /// Misra–Gries frequency estimate.
    Mg,
}

impl ExtensionIndexKind {
    /// Known extension algorithms checked by operation-level provider validation.
    pub const ALL: [Self; 3] = [Self::Hll, Self::Kll, Self::Mg];

    /// Column metadata key that requires this algorithm's provider.
    pub const fn required_metadata_key(self) -> &'static str {
        match self {
            Self::Hll => datatypes::schema::HLL_KEY,
            Self::Kll => datatypes::schema::KLL_KEY,
            Self::Mg => datatypes::schema::MG_KEY,
        }
    }

    /// Stable label used by index diagnostics and metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hll => "hll_index",
            Self::Kll => "kll_index",
            Self::Mg => "mg_index",
        }
    }
}

impl IndexKind {
    /// Stable label used by the existing index metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inverted => "inverted_index",
            Self::Fulltext => "fulltext_index",
            Self::Bloom => "bloom_filter_index",
            Self::Extension(kind) => kind.as_str(),
        }
    }

    /// Returns the identity recorded in file and column index metadata.
    pub fn index_type(self) -> IndexType {
        match self {
            Self::Inverted => IndexType::InvertedIndex,
            Self::Fulltext => IndexType::FulltextIndex,
            Self::Bloom => IndexType::BloomFilterIndex,
            Self::Extension(ExtensionIndexKind::Hll) => IndexType::HllIndex,
            Self::Extension(ExtensionIndexKind::Kll) => IndexType::KllIndex,
            Self::Extension(ExtensionIndexKind::Mg) => IndexType::MgIndex,
        }
    }

    /// Whether this kind is implemented by the storage engine.
    pub fn is_builtin(self) -> bool {
        !matches!(self, Self::Extension(_))
    }
}
