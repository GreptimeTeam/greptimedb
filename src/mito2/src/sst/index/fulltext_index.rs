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

use store_api::storage::ColumnId;

pub(crate) mod applier;
pub(crate) mod creator;

const INDEX_BLOB_TYPE_TANTIVY: &str = "greptime-fulltext-index-v1";
const INDEX_BLOB_TYPE_BLOOM: &str = "greptime-fulltext-index-bloom";

#[derive(Debug, Clone)]
pub(crate) enum FulltextPredicate {
    Matches(MatchesPredicate),
    MatchesTerm(MatchesTermPredicate),
}

#[derive(Debug, Clone)]
pub(crate) struct MatchesPredicate {
    column_id: ColumnId,
    query: String,
}

#[derive(Debug, Clone)]
pub(crate) struct MatchesTermPredicate {
    column_id: ColumnId,
    term_to_lowercase: bool,
    term: String,
}

impl FulltextPredicate {
    pub(crate) fn column_id(&self) -> ColumnId {
        match self {
            FulltextPredicate::Matches(m) => m.column_id,
            FulltextPredicate::MatchesTerm(m) => m.column_id,
        }
    }
}
