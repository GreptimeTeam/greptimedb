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

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

use async_trait::async_trait;
use common_telemetry::debug;
use snafu::ResultExt;
use tantivy::collector::DocSetCollector;
use tantivy::query::QueryParser;
use tantivy::schema::Field;
use tantivy::{Index, IndexReader, ReloadPolicy};

use crate::fulltext_index::create::TEXT_FIELD_NAME;
use crate::fulltext_index::error::{Result, TantivyParserSnafu, TantivySnafu};
use crate::fulltext_index::search::{FulltextIndexSearcher, RowId};

/// `TantivyFulltextIndexSearcher` is a searcher using Tantivy.
pub struct TantivyFulltextIndexSearcher {
    /// Tanitvy index.
    index: Index,
    /// Tanitvy index reader.
    reader: IndexReader,
    /// The default field used to build `QueryParser`
    default_field: Field,
}

impl TantivyFulltextIndexSearcher {
    /// Creates a new `TantivyFulltextIndexSearcher`.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let now = Instant::now();

        let index = Index::open_in_dir(path.as_ref()).context(TantivySnafu)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .num_warming_threads(0)
            .try_into()
            .context(TantivySnafu)?;
        let default_field = index
            .schema()
            .get_field(TEXT_FIELD_NAME)
            .context(TantivySnafu)?;

        debug!(
            "Opened tantivy index on {:?} in {:?}",
            path.as_ref(),
            now.elapsed()
        );

        Ok(Self {
            index,
            reader,
            default_field,
        })
    }
}

#[async_trait]
impl FulltextIndexSearcher for TantivyFulltextIndexSearcher {
    async fn search(&self, query: &str) -> Result<BTreeSet<RowId>> {
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.default_field]);
        let query = query_parser
            .parse_query(query)
            .context(TantivyParserSnafu)?;
        let docs = searcher
            .search(&query, &DocSetCollector)
            .context(TantivySnafu)?;
        Ok(docs.into_iter().map(|d| d.doc_id).collect())
    }
}
