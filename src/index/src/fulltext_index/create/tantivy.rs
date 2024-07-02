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

use std::path::Path;

use async_trait::async_trait;
use snafu::ResultExt;
use tantivy::schema::{Schema, TEXT};
use tantivy::store::{Compressor, ZstdCompressor};
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer, TokenizerManager};
use tantivy::{doc, Index, SingleSegmentIndexWriter};
use tantivy_jieba::JiebaTokenizer;

use crate::fulltext_index::create::FulltextIndexCreator;
use crate::fulltext_index::error::{IOSnafu, Result, TantivySnafu};
use crate::fulltext_index::{Analyzer, Config};

pub const TEXT_FIELD_NAME: &str = "greptime_fulltext_text";

/// `TantivyFulltextIndexCreator` is a fulltext index creator using tantivy.
///
/// Here use a single segment to store the index so the maximum capacity for
/// the index is limited to 2<<31 rows (around 2 billion rows).
pub struct TantivyFulltextIndexCreator {
    /// The tantivy index writer.
    writer: Option<SingleSegmentIndexWriter>,

    /// The field for the text.
    text_field: tantivy::schema::Field,
}

impl TantivyFulltextIndexCreator {
    /// Creates a new `TantivyFulltextIndexCreator`.
    ///
    /// The `path` is the directory path in filesystem to store the index.
    pub async fn new(path: impl AsRef<Path>, config: Config, memory_limit: usize) -> Result<Self> {
        tokio::fs::create_dir_all(path.as_ref())
            .await
            .context(IOSnafu)?;

        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field(TEXT_FIELD_NAME, TEXT);
        let schema = schema_builder.build();

        let mut index = Index::create_in_dir(path, schema).context(TantivySnafu)?;
        index.settings_mut().docstore_compression = Compressor::Zstd(ZstdCompressor::default());
        index.set_tokenizers(Self::build_tokenizer(&config));

        let memory_limit = Self::sanitize_memory_limit(memory_limit);

        let writer = SingleSegmentIndexWriter::new(index, memory_limit).context(TantivySnafu)?;
        Ok(Self {
            writer: Some(writer),
            text_field,
        })
    }

    fn build_tokenizer(config: &Config) -> TokenizerManager {
        let mut builder = match config.analyzer {
            Analyzer::English => TextAnalyzer::builder(SimpleTokenizer::default()).dynamic(),
            Analyzer::Chinese => TextAnalyzer::builder(JiebaTokenizer {}).dynamic(),
        };

        if !config.case_sensitive {
            builder = builder.filter_dynamic(LowerCaser);
        }

        let tokenizer = builder.build();
        let tokenizer_manager = TokenizerManager::new();
        tokenizer_manager.register("default", tokenizer);
        tokenizer_manager
    }

    fn sanitize_memory_limit(memory_limit: usize) -> usize {
        // Port from tantivy::indexer::index_writer::{MEMORY_BUDGET_NUM_BYTES_MIN, MEMORY_BUDGET_NUM_BYTES_MAX}
        const MARGIN_IN_BYTES: usize = 1_000_000;
        const MEMORY_BUDGET_NUM_BYTES_MIN: usize = ((MARGIN_IN_BYTES as u32) * 15u32) as usize;
        const MEMORY_BUDGET_NUM_BYTES_MAX: usize = u32::MAX as usize - MARGIN_IN_BYTES;

        memory_limit.clamp(MEMORY_BUDGET_NUM_BYTES_MIN, MEMORY_BUDGET_NUM_BYTES_MAX)
    }
}

#[async_trait]
impl FulltextIndexCreator for TantivyFulltextIndexCreator {
    async fn push_text(&mut self, text: &str) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer
                .add_document(doc!(self.text_field => text))
                .context(TantivySnafu)?;
        }

        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.finalize().context(TantivySnafu)?;
        }
        Ok(())
    }

    fn memory_usage(&self) -> usize {
        self.writer
            .as_ref()
            .map(|writer| writer.mem_usage())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use common_test_util::temp_dir::create_temp_dir;
    use tantivy::collector::DocSetCollector;
    use tantivy::query::QueryParser;

    use super::*;

    #[tokio::test]
    async fn test_creator_basic() {
        let memory_limits = [1, 64_000_000, usize::MAX];

        for memory_limit in memory_limits {
            let temp_dir = create_temp_dir("test_creator_basic_");

            let mut creator =
                TantivyFulltextIndexCreator::new(temp_dir.path(), Config::default(), memory_limit)
                    .await
                    .unwrap();

            let texts = vec!["hello", "world", "hello, world", "foo!", "Bar"];

            for text in texts {
                creator.push_text(text).await.unwrap();
            }

            creator.finish().await.unwrap();

            let cases = [
                ("hello", HashSet::from_iter([0u32, 2].into_iter())),
                ("world", HashSet::from_iter([1, 2].into_iter())),
                ("foo", HashSet::from_iter([3].into_iter())),
                ("bar", HashSet::from_iter([4].into_iter())),
            ];

            let index = Index::open_in_dir(temp_dir.path()).unwrap();
            let reader = index.reader().unwrap();

            let searcher = reader.searcher();
            for (query, expected) in cases {
                let query_parser = QueryParser::for_index(
                    &index,
                    vec![index.schema().get_field(TEXT_FIELD_NAME).unwrap()],
                );
                let query = query_parser.parse_query(query).unwrap();
                let docs = searcher.search(&query, &DocSetCollector).unwrap();
                let res = docs.into_iter().map(|d| d.doc_id).collect::<HashSet<_>>();
                assert_eq!(res, expected);
            }
        }
    }

    #[tokio::test]
    async fn test_creator_case_sensitive() {
        let memory_limits = [1, 64_000_000, usize::MAX];

        for memory_limit in memory_limits {
            let temp_dir = create_temp_dir("test_creator_case_sensitive_");

            let config = Config {
                case_sensitive: true,
                ..Config::default()
            };
            let mut creator =
                TantivyFulltextIndexCreator::new(temp_dir.path(), config, memory_limit)
                    .await
                    .unwrap();

            let texts = vec!["hello", "world", "hello, world", "foo!", "Bar"];

            for text in texts {
                creator.push_text(text).await.unwrap();
            }

            creator.finish().await.unwrap();

            let cases = [
                ("hello", HashSet::from_iter([0u32, 2].into_iter())),
                ("world", HashSet::from_iter([1, 2].into_iter())),
                ("foo", HashSet::from_iter([3].into_iter())),
                ("bar", HashSet::from_iter([].into_iter())),
            ];

            let index = Index::open_in_dir(temp_dir.path()).unwrap();
            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            for (query, expected) in cases {
                let query_parser = QueryParser::for_index(
                    &index,
                    vec![index.schema().get_field(TEXT_FIELD_NAME).unwrap()],
                );
                let query = query_parser.parse_query(query).unwrap();
                let docs = searcher.search(&query, &DocSetCollector).unwrap();
                let res = docs.into_iter().map(|d| d.doc_id).collect::<HashSet<_>>();
                assert_eq!(res, expected);
            }
        }
    }

    #[tokio::test]
    async fn test_creator_chinese() {
        let memory_limits = [1, 64_000_000, usize::MAX];

        for memory_limit in memory_limits {
            let temp_dir = create_temp_dir("test_creator_chinese_");

            let config = Config {
                analyzer: Analyzer::Chinese,
                ..Config::default()
            };
            let mut creator =
                TantivyFulltextIndexCreator::new(temp_dir.path(), config, memory_limit)
                    .await
                    .unwrap();

            let texts = vec!["你好", "世界", "你好，世界", "你好世界", "foo!", "Bar"];

            for text in texts {
                creator.push_text(text).await.unwrap();
            }

            creator.finish().await.unwrap();

            let cases = [
                ("你好", HashSet::from_iter([0u32, 2, 3].into_iter())),
                ("世界", HashSet::from_iter([1, 2, 3].into_iter())),
                ("foo", HashSet::from_iter([4].into_iter())),
                ("bar", HashSet::from_iter([5].into_iter())),
            ];

            let index = Index::open_in_dir(temp_dir.path()).unwrap();
            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            for (query, expected) in cases {
                let query_parser = QueryParser::for_index(
                    &index,
                    vec![index.schema().get_field(TEXT_FIELD_NAME).unwrap()],
                );
                let query = query_parser.parse_query(query).unwrap();
                let docs = searcher.search(&query, &DocSetCollector).unwrap();
                let res = docs.into_iter().map(|d| d.doc_id).collect::<HashSet<_>>();
                assert_eq!(res, expected);
            }
        }
    }

    #[tokio::test]
    async fn test_creator_chinese_case_sensitive() {
        let memory_limits = [1, 64_000_000, usize::MAX];

        for memory_limit in memory_limits {
            let temp_dir = create_temp_dir("test_creator_chinese_case_sensitive_");

            let config = Config {
                analyzer: Analyzer::Chinese,
                case_sensitive: true,
            };
            let mut creator =
                TantivyFulltextIndexCreator::new(temp_dir.path(), config, memory_limit)
                    .await
                    .unwrap();

            let texts = vec!["你好", "世界", "你好，世界", "你好世界", "foo!", "Bar"];

            for text in texts {
                creator.push_text(text).await.unwrap();
            }

            creator.finish().await.unwrap();

            let cases = [
                ("你好", HashSet::from_iter([0u32, 2, 3].into_iter())),
                ("世界", HashSet::from_iter([1, 2, 3].into_iter())),
                ("foo", HashSet::from_iter([4].into_iter())),
                ("bar", HashSet::from_iter([].into_iter())),
            ];

            let index = Index::open_in_dir(temp_dir.path()).unwrap();
            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            for (query, expected) in cases {
                let query_parser = QueryParser::for_index(
                    &index,
                    vec![index.schema().get_field(TEXT_FIELD_NAME).unwrap()],
                );
                let query = query_parser.parse_query(query).unwrap();
                let docs = searcher.search(&query, &DocSetCollector).unwrap();
                let res = docs.into_iter().map(|d| d.doc_id).collect::<HashSet<_>>();
                assert_eq!(res, expected);
            }
        }
    }
}
