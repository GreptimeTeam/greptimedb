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
use std::sync::Arc;

use common_test_util::temp_dir::{TempDir, create_temp_dir};
use puffin::puffin_manager::file_accessor::MockFileAccessor;
use puffin::puffin_manager::fs_puffin_manager::FsPuffinManager;
use puffin::puffin_manager::stager::BoundedStager;
use puffin::puffin_manager::{PuffinManager, PuffinReader, PuffinWriter, PutOptions};

use crate::fulltext_index::create::{FulltextIndexCreator, TantivyFulltextIndexCreator};
use crate::fulltext_index::search::{FulltextIndexSearcher, RowId, TantivyFulltextIndexSearcher};
use crate::fulltext_index::{Analyzer, Config};

async fn new_bounded_stager(prefix: &str) -> (TempDir, Arc<BoundedStager<String>>) {
    let staging_dir = create_temp_dir(prefix);
    let path = staging_dir.path().to_path_buf();
    (
        staging_dir,
        Arc::new(BoundedStager::new(path, 102400, None, None).await.unwrap()),
    )
}

async fn create_index(
    prefix: &str,
    puffin_writer: &mut (impl PuffinWriter + Send),
    blob_key: &str,
    texts: Vec<&str>,
    config: Config,
) {
    let tantivy_path = create_temp_dir(prefix);

    let mut creator = TantivyFulltextIndexCreator::new(tantivy_path.path(), config, 1024 * 1024)
        .await
        .unwrap();

    for text in texts {
        creator.push_text(text).await.unwrap();
    }

    creator
        .finish(puffin_writer, blob_key, PutOptions::default())
        .await
        .unwrap();
}

async fn test_search(
    prefix: &str,
    config: Config,
    texts: Vec<&str>,
    query_expected: Vec<(&str, impl IntoIterator<Item = RowId>)>,
) {
    let (_staging_dir, stager) = new_bounded_stager(prefix).await;
    let file_accessor = Arc::new(MockFileAccessor::new(prefix));
    let puffin_manager = FsPuffinManager::new(stager, file_accessor);

    let file_name = "fulltext_index".to_string();
    let blob_key = "fulltext_index".to_string();
    let mut writer = puffin_manager.writer(&file_name).await.unwrap();
    create_index(prefix, &mut writer, &blob_key, texts, config).await;
    writer.finish().await.unwrap();

    let reader = puffin_manager.reader(&file_name).await.unwrap();
    let (index_dir, _metrics) = reader.dir(&blob_key).await.unwrap();
    let searcher = TantivyFulltextIndexSearcher::new(index_dir.path(), config).unwrap();
    for (query, expected) in query_expected {
        let results = searcher.search(query).await.unwrap();
        let expected = expected.into_iter().collect::<BTreeSet<_>>();
        assert_eq!(results, expected);
    }
}

#[tokio::test]
async fn test_simple_term() {
    test_search(
        "test_simple_term_",
        Config::default(),
        vec![
            "This is a sample text containing Barack Obama",
            "Another document mentioning Barack",
        ],
        vec![("Barack Obama", [0, 1])],
    )
    .await;
}

#[tokio::test]
async fn test_negative_term() {
    test_search(
        "test_negative_term_",
        Config::default(),
        vec!["apple is a fruit", "I like apple", "fruit is healthy"],
        vec![("apple -fruit", [1])],
    )
    .await;
}

#[tokio::test]
async fn test_must_term() {
    test_search(
        "test_must_term_",
        Config::default(),
        vec![
            "apple is tasty",
            "I love apples and fruits",
            "apple and fruit are good",
        ],
        vec![("+apple +fruit", [2])],
    )
    .await;
}

#[tokio::test]
async fn test_boolean_operators() {
    test_search(
        "test_boolean_operators_",
        Config::default(),
        vec!["a b c", "a b", "b c", "c"],
        vec![("a AND b OR c", [0, 1, 2, 3])],
    )
    .await;
}

#[tokio::test]
async fn test_phrase_term() {
    test_search(
        "test_phrase_term_",
        Config::default(),
        vec![
            "This is a sample text containing Barack Obama",
            "Another document mentioning Barack",
        ],
        vec![("\"Barack Obama\"", [0])],
    )
    .await;
}

#[tokio::test]
async fn test_config_english_analyzer_case_insensitive() {
    test_search(
        "test_config_english_analyzer_case_insensitive_",
        Config {
            case_sensitive: false,
            ..Config::default()
        },
        vec!["Banana is a fruit", "I like apple", "Fruit is healthy"],
        vec![("banana", [0]), ("Banana", [0]), ("BANANA", [0])],
    )
    .await;
}

#[tokio::test]
async fn test_config_english_analyzer_case_sensitive() {
    test_search(
        "test_config_english_analyzer_case_sensitive_",
        Config {
            case_sensitive: true,
            ..Config::default()
        },
        vec!["Banana is a fruit", "I like banana", "Fruit is healthy"],
        vec![("banana", [1]), ("Banana", [0])],
    )
    .await;
}

#[tokio::test]
async fn test_config_chinese_analyzer() {
    test_search(
        "test_config_chinese_analyzer_",
        Config {
            analyzer: Analyzer::Chinese,
            ..Default::default()
        },
        vec!["苹果是一种水果", "我喜欢苹果", "水果很健康"],
        vec![("苹果", [0, 1])],
    )
    .await;
}
