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

use std::collections::BTreeMap;
use std::path::Path;

use snafu::ResultExt;
use tantivy::schema::{OwnedValue, Schema, INDEXED, STORED, TEXT};
use tantivy::{Document, Index, IndexWriter, TantivyDocument};

use super::error::TantivySnafu;
use crate::full_text_index::error::Result;

pub struct FullTextIndexCreater {
    index: Index,
    writer: IndexWriter,
    count_field: tantivy::schema::Field,
    text_field: tantivy::schema::Field,

    row_count: usize,
    segment_size: usize,
}

impl FullTextIndexCreater {
    pub fn new<P>(segment_size: usize, path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        // build schema
        let mut schema_builder = Schema::builder();
        let count_field = schema_builder.add_i64_field("seg_count", INDEXED | STORED);
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        // build index
        let index = Index::create_in_dir(path, schema).context(TantivySnafu)?;

        // build writer
        // 100 MB
        let writer = index.writer(100_000_000).context(TantivySnafu)?;

        Ok(Self {
            index,
            writer,
            count_field,
            text_field,
            row_count: 0,
            segment_size,
        })
    }

    pub fn push_string(&mut self, content: String) -> Result<()> {
        let mut doc = TantivyDocument::new();
        doc.add_text(self.text_field, content);
        doc.add_i64(self.count_field, (self.row_count / self.segment_size) as _);
        self.writer.add_document(doc).context(TantivySnafu)?;
        self.row_count += 1;

        self.writer.commit().context(TantivySnafu)?;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.row_count = 0;
        self.writer.commit().context(TantivySnafu)?;
        Ok(())
    }
}
