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

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::time::Instant;

use async_trait::async_trait;
use common_telemetry::debug;
use snafu::{OptionExt, ResultExt};
use tantivy::collector::DocSetCollector;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Value};
use tantivy::{Index, IndexReader, ReloadPolicy, TantivyDocument};

use crate::fulltext_index::create::{ROWID_FIELD_NAME, TEXT_FIELD_NAME};
use crate::fulltext_index::error::{
    Result, TantivyDocNotFoundSnafu, TantivyParserSnafu, TantivySnafu,
};
use crate::fulltext_index::search::{FulltextIndexSearcher, RowId};
use crate::fulltext_index::Config;

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
    pub fn new(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let now = Instant::now();

        let mut index = Index::open_in_dir(path.as_ref()).context(TantivySnafu)?;
        index.set_tokenizers(config.build_tantivy_tokenizer());
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
        let doc_addrs = searcher
            .search(&query, &DocSetCollector)
            .context(TantivySnafu)?;

        let seg_metas = self
            .index
            .searchable_segment_metas()
            .context(TantivySnafu)?;

        // FAST PATH: only one segment, the doc id is the same as the row id.
        //            Also for compatibility with the old version.
        if seg_metas.len() == 1 {
            return Ok(doc_addrs.into_iter().map(|d| d.doc_id).collect());
        }

        // SLOW PATH: multiple segments, need to calculate the row id.
        let rowid_field = searcher
            .schema()
            .get_field(ROWID_FIELD_NAME)
            .context(TantivySnafu)?;
        let mut seg_offsets = HashMap::with_capacity(seg_metas.len());
        let mut res = BTreeSet::new();
        for doc_addr in doc_addrs {
            let offset = if let Some(offset) = seg_offsets.get(&doc_addr.segment_ord) {
                *offset
            } else {
                // Calculate the offset at the first time meeting the segment and cache it since
                // the offset is the same for all rows in the same segment.
                let doc: TantivyDocument = searcher.doc(doc_addr).context(TantivySnafu)?;
                let rowid = doc
                    .get_first(rowid_field)
                    .and_then(|v| v.as_u64())
                    .context(TantivyDocNotFoundSnafu { doc_addr })?;

                let offset = rowid as u32 - doc_addr.doc_id;
                seg_offsets.insert(doc_addr.segment_ord, offset);
                offset
            };

            res.insert(doc_addr.doc_id + offset);
        }

        Ok(res)
    }
}
