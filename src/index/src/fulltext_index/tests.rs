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

use common_test_util::temp_dir::{create_temp_dir, TempDir};

use crate::fulltext_index::create::{FulltextIndexCreator, TantivyFulltextIndexCreator};
use crate::fulltext_index::search::{FulltextIndexSearcher, RowId, TantivyFulltextIndexSearcher};
use crate::fulltext_index::{Analyzer, Config};

async fn create_index(prefix: &str, texts: Vec<&str>, config: Config) -> TempDir {
    let tempdir = create_temp_dir(prefix);

    let mut creator = TantivyFulltextIndexCreator::new(tempdir.path(), config, 1024 * 1024)
        .await
        .unwrap();

    for text in texts {
        creator.push_text(text).await.unwrap();
    }

    creator.finish().await.unwrap();
    tempdir
}

async fn test_search(
    prefix: &str,
    config: Config,
    texts: Vec<&str>,
    query: &str,
    expected: impl IntoIterator<Item = RowId>,
) {
    let index_path = create_index(prefix, texts, config).await;

    let searcher = TantivyFulltextIndexSearcher::new(index_path.path()).unwrap();
    let results = searcher.search(query).await.unwrap();

    let expected = expected.into_iter().collect::<BTreeSet<_>>();
    assert_eq!(results, expected);
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
        "Barack Obama",
        [0, 1],
    )
    .await;
}

#[tokio::test]
async fn test_negative_term() {
    test_search(
        "test_negative_term_",
        Config::default(),
        vec!["apple is a fruit", "I like apple", "fruit is healthy"],
        "apple -fruit",
        [1],
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
        "+apple +fruit",
        [2],
    )
    .await;
}

#[tokio::test]
async fn test_boolean_operators() {
    test_search(
        "test_boolean_operators_",
        Config::default(),
        vec!["a b c", "a b", "b c", "c"],
        "a AND b OR c",
        [0, 1, 2, 3],
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
        "\"Barack Obama\"",
        [0],
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
        "banana",
        [0],
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
        vec!["Banana is a fruit", "I like apple", "Fruit is healthy"],
        "banana",
        [],
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
        "苹果",
        [0, 1],
    )
    .await;
}
