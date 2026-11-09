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

/// Index implementations supplied by the storage engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IndexKind {
    Inverted,
    Fulltext,
    Bloom,
}

impl IndexKind {
    /// Stable label used by the existing index metrics.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Inverted => "inverted_index",
            Self::Fulltext => "fulltext_index",
            Self::Bloom => "bloom_filter_index",
        }
    }

    /// Returns the identity recorded in file and column index metadata.
    pub(crate) fn index_type(self) -> IndexType {
        match self {
            Self::Inverted => IndexType::InvertedIndex,
            Self::Fulltext => IndexType::FulltextIndex,
            Self::Bloom => IndexType::BloomFilterIndex,
        }
    }
}
