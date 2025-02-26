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

mod tantivy;

use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::fulltext_index::error::Result;
pub use crate::fulltext_index::search::tantivy::TantivyFulltextIndexSearcher;

pub type RowId = u32;

/// `FulltextIndexSearcher` is a trait for searching fulltext index.
#[async_trait]
pub trait FulltextIndexSearcher {
    async fn search(&self, query: &str) -> Result<BTreeSet<RowId>>;
}
