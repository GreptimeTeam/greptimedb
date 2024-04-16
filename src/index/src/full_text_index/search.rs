// Copyright 2024 Greptime Team
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

use std::collections::HashSet;
use std::path::Path;

use snafu::ResultExt;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::Value;
use tantivy::{Index, IndexReader, TantivyDocument, TantivyError};

use super::error::ParseQuerySnafu;
use crate::full_text_index::error::{OpenDirectorySnafu, Result, TantivySnafu};

pub struct FullTextIndexSearcher {
    index: Index,
    count_field: tantivy::schema::Field,
    text_field: tantivy::schema::Field,
    reader: IndexReader,
}

impl FullTextIndexSearcher {
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let index = Index::open_in_dir(path).context(TantivySnafu)?;
        let schema = index.schema();
        let count_field = schema.get_field("seg_count").unwrap();
        let text_field = schema.get_field("text").unwrap();
        let reader = index.reader().context(TantivySnafu)?;

        Ok(Self {
            index,
            count_field,
            text_field,
            reader,
        })
    }

    pub fn search(&self, query: &str) -> Result<Vec<usize>> {
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.text_field]);
        let query = query_parser.parse_query(query).context(ParseQuerySnafu)?;
        let top_docs = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(100))
            .context(TantivySnafu)?;
        let mut result = HashSet::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc = searcher
                .doc::<TantivyDocument>(doc_address)
                .context(TantivySnafu)?;
            let seg_count = retrieved_doc
                .get_first(self.count_field)
                .unwrap()
                .as_i64()
                .unwrap();
            result.insert(seg_count);
        }
        Ok(result.into_iter().map(|x| x as _).collect())
    }
}
