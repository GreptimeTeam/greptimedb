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

use std::collections::BTreeMap;
use std::sync::Arc;

use index::bloom_filter::applier::InListPredicate;
use index::inverted_index::search::predicate::{Predicate, RangePredicate};
use moka::notification::RemovalCause;
use moka::sync::Cache;
use store_api::storage::ColumnId;

use crate::metrics::{CACHE_BYTES, CACHE_EVICTION, CACHE_HIT, CACHE_MISS};
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::applier::builder::{
    FulltextQuery, FulltextRequest, FulltextTerm,
};
use crate::sst::parquet::row_selection::RowGroupSelection;

const INDEX_RESULT_TYPE: &str = "index_result";

/// Cache for storing index query results.
pub struct IndexResultCache {
    cache: Cache<(PredicateKey, FileId), Arc<RowGroupSelection>>,
}

impl IndexResultCache {
    /// Creates a new cache with the given capacity.
    pub fn new(capacity: u64) -> Self {
        fn to_str(cause: RemovalCause) -> &'static str {
            match cause {
                RemovalCause::Expired => "expired",
                RemovalCause::Explicit => "explicit",
                RemovalCause::Replaced => "replaced",
                RemovalCause::Size => "size",
            }
        }

        let cache = Cache::builder()
            .max_capacity(capacity)
            .weigher(Self::index_result_cache_weight)
            .eviction_listener(|k, v, cause| {
                let size = Self::index_result_cache_weight(&k, &v);
                CACHE_BYTES
                    .with_label_values(&[INDEX_RESULT_TYPE])
                    .sub(size.into());
                CACHE_EVICTION
                    .with_label_values(&[INDEX_RESULT_TYPE, to_str(cause)])
                    .inc();
            })
            .build();
        Self { cache }
    }

    /// Puts a query result into the cache.
    pub fn put(&self, key: PredicateKey, file_id: FileId, result: Arc<RowGroupSelection>) {
        let key = (key, file_id);
        let size = Self::index_result_cache_weight(&key, &result);
        CACHE_BYTES
            .with_label_values(&[INDEX_RESULT_TYPE])
            .add(size.into());
        self.cache.insert(key, result);
    }

    /// Gets a query result from the cache.
    pub fn get(&self, key: &PredicateKey, file_id: FileId) -> Option<Arc<RowGroupSelection>> {
        let res = self.cache.get(&(key.clone(), file_id));
        if res.is_some() {
            CACHE_HIT.with_label_values(&[INDEX_RESULT_TYPE]).inc();
        } else {
            CACHE_MISS.with_label_values(&[INDEX_RESULT_TYPE]).inc()
        }
        res
    }

    /// Calculates the memory usage of a cache entry.
    fn index_result_cache_weight(k: &(PredicateKey, FileId), v: &Arc<RowGroupSelection>) -> u32 {
        k.0.mem_usage() as u32 + v.mem_usage() as u32
    }
}

/// Key for different types of index predicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PredicateKey {
    /// Fulltext index predicate.
    Fulltext(FulltextIndexKey),
    /// Bloom filter predicate.
    Bloom(BloomFilterKey),
    /// Inverted index predicate.
    Inverted(InvertedIndexKey),
}

impl PredicateKey {
    /// Creates a new fulltext index key.
    pub fn new_fulltext(predicates: Arc<BTreeMap<ColumnId, FulltextRequest>>) -> Self {
        Self::Fulltext(FulltextIndexKey::new(predicates))
    }

    /// Creates a new bloom filter key.
    pub fn new_bloom(predicates: Arc<BTreeMap<ColumnId, Vec<InListPredicate>>>) -> Self {
        Self::Bloom(BloomFilterKey::new(predicates))
    }

    /// Creates a new inverted index key.
    pub fn new_inverted(predicates: Arc<BTreeMap<ColumnId, Vec<Predicate>>>) -> Self {
        Self::Inverted(InvertedIndexKey::new(predicates))
    }

    /// Returns the memory usage of this key.
    pub fn mem_usage(&self) -> usize {
        match self {
            Self::Fulltext(key) => key.mem_usage,
            Self::Bloom(key) => key.mem_usage,
            Self::Inverted(key) => key.mem_usage,
        }
    }
}

/// Key for fulltext index queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct FulltextIndexKey {
    predicates: Arc<BTreeMap<ColumnId, FulltextRequest>>,
    mem_usage: usize,
}

impl FulltextIndexKey {
    /// Creates a new fulltext index key with the given predicates.
    /// Calculates memory usage based on the size of queries and terms.
    pub fn new(predicates: Arc<BTreeMap<ColumnId, FulltextRequest>>) -> Self {
        let mem_usage = predicates
            .values()
            .map(|request| {
                let query_size = request
                    .queries
                    .iter()
                    .map(|query| query.0.len() + size_of::<FulltextQuery>())
                    .sum::<usize>();
                let term_size = request
                    .terms
                    .iter()
                    .map(|term| term.term.len() + size_of::<FulltextTerm>())
                    .sum::<usize>();
                query_size + term_size
            })
            .sum();
        Self {
            predicates,
            mem_usage,
        }
    }
}

/// Key for bloom filter queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct BloomFilterKey {
    predicates: Arc<BTreeMap<ColumnId, Vec<InListPredicate>>>,
    mem_usage: usize,
}

impl BloomFilterKey {
    /// Creates a new bloom filter key with the given predicates.
    /// Calculates memory usage based on the size of predicate lists.
    pub fn new(predicates: Arc<BTreeMap<ColumnId, Vec<InListPredicate>>>) -> Self {
        let mem_usage = predicates
            .values()
            .map(|predicates| {
                predicates
                    .iter()
                    .map(|predicate| predicate.list.iter().map(|list| list.len()).sum::<usize>())
                    .sum::<usize>()
            })
            .sum();
        Self {
            predicates,
            mem_usage,
        }
    }
}

/// Key for inverted index queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct InvertedIndexKey {
    predicates: Arc<BTreeMap<ColumnId, Vec<Predicate>>>,
    mem_usage: usize,
}

impl InvertedIndexKey {
    /// Creates a new inverted index key with the given predicates.
    /// Calculates memory usage based on the type and size of predicates.
    pub fn new(predicates: Arc<BTreeMap<ColumnId, Vec<Predicate>>>) -> Self {
        let mem_usage = predicates
            .values()
            .map(|predicates| {
                predicates
                    .iter()
                    .map(|predicate| match predicate {
                        Predicate::InList(predicate) => {
                            predicate.list.iter().map(|list| list.len()).sum::<usize>()
                        }
                        Predicate::Range(_) => size_of::<RangePredicate>(),
                        Predicate::RegexMatch(predicate) => predicate.pattern.len(),
                    })
                    .sum::<usize>()
            })
            .sum();

        Self {
            predicates,
            mem_usage,
        }
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use index::bloom_filter::applier::InListPredicate as BloomInListPredicate;
    use index::inverted_index::search::predicate::{Predicate, Range, RangePredicate};

    use super::*;
    use crate::sst::index::fulltext_index::applier::builder::{
        FulltextQuery, FulltextRequest, FulltextTerm,
    };
    use crate::sst::parquet::row_selection::RowGroupSelection;

    #[test]
    fn test_cache_basic_operations() {
        let cache = IndexResultCache::new(1000);
        let file_id = FileId::random();

        // Create a test key and value
        let predicates = BTreeMap::new();
        let key = PredicateKey::new_fulltext(Arc::new(predicates));
        let selection = Arc::new(RowGroupSelection::from_row_ids(
            [1, 2, 3].into_iter().collect(),
            1,
            10,
        ));

        // Test put and get
        cache.put(key.clone(), file_id, selection.clone());
        let retrieved = cache.get(&key, file_id);
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.unwrap().as_ref().row_count(),
            selection.as_ref().row_count()
        );

        // Test get non-existent key
        let non_existent_file_id = FileId::random();
        assert!(cache.get(&key, non_existent_file_id).is_none());
    }

    #[test]
    fn test_cache_capacity_limit() {
        // Create a cache with small capacity (100 bytes)
        let cache = IndexResultCache::new(100);
        let file_id1 = FileId::random();
        let file_id2 = FileId::random();

        // Create two large keys that will exceed capacity
        let mut predicates1 = BTreeMap::new();
        let request1 = FulltextRequest {
            queries: vec![
                FulltextQuery(
                    "test query 1 with a very long string to ensure large weight".to_string(),
                ),
                FulltextQuery("another long query string".to_string()),
            ],
            terms: vec![],
        };
        predicates1.insert(1, request1);
        let key1 = PredicateKey::new_fulltext(Arc::new(predicates1));
        let selection1 = Arc::new(RowGroupSelection::default());

        let mut predicates2 = BTreeMap::new();
        let request2 = FulltextRequest {
            queries: vec![
                FulltextQuery(
                    "test query 2 with a very long string to ensure large weight".to_string(),
                ),
                FulltextQuery("another long query string".to_string()),
            ],
            terms: vec![],
        };
        predicates2.insert(1, request2);
        let key2 = PredicateKey::new_fulltext(Arc::new(predicates2));
        let selection2 = Arc::new(RowGroupSelection::default());

        // Calculate weights
        let weight1 =
            IndexResultCache::index_result_cache_weight(&(key1.clone(), file_id1), &selection1);
        let weight2 =
            IndexResultCache::index_result_cache_weight(&(key2.clone(), file_id2), &selection2);
        assert!(weight1 > 100);
        assert!(weight2 > 100);

        // Put first key-value pair
        cache.put(key1.clone(), file_id1, selection1.clone());

        // Verify first key is in cache
        let retrieved1 = cache.get(&key1, file_id1);
        assert!(retrieved1.is_some());
        assert_eq!(
            retrieved1.unwrap().as_ref().row_count(),
            selection1.as_ref().row_count()
        );

        // Put second key-value pair, which should trigger eviction
        cache.put(key2.clone(), file_id2, selection2.clone());

        // Verify second key is in cache
        let retrieved2 = cache.get(&key2, file_id2);
        assert!(retrieved2.is_some());
        assert_eq!(
            retrieved2.unwrap().as_ref().row_count(),
            selection2.as_ref().row_count()
        );

        // Verify first key was evicted
        cache.cache.run_pending_tasks();
        let retrieved1_after_eviction = cache.get(&key1, file_id1);
        assert!(
            retrieved1_after_eviction.is_none(),
            "First key should have been evicted"
        );
    }

    #[test]
    fn test_index_result_cache_weight() {
        let file_id = FileId::random();

        // Test empty values
        let empty_predicates = BTreeMap::new();
        let empty_key = PredicateKey::new_fulltext(Arc::new(empty_predicates));
        let empty_selection = Arc::new(RowGroupSelection::default());
        let empty_weight = IndexResultCache::index_result_cache_weight(
            &(empty_key.clone(), file_id),
            &empty_selection,
        );
        assert_eq!(empty_weight, 0);
        assert_eq!(
            empty_weight,
            empty_key.mem_usage() as u32 + empty_selection.mem_usage() as u32
        );

        // Test 1: FulltextIndexKey
        let mut predicates1 = BTreeMap::new();
        let request1 = FulltextRequest {
            queries: vec![FulltextQuery("test query".to_string())],
            terms: vec![FulltextTerm {
                col_lowered: false,
                term: "test term".to_string(),
            }],
        };
        predicates1.insert(1, request1);
        let key1 = PredicateKey::new_fulltext(Arc::new(predicates1));
        let selection1 = Arc::new(RowGroupSelection::new(100, 250));
        let weight1 =
            IndexResultCache::index_result_cache_weight(&(key1.clone(), file_id), &selection1);
        assert!(weight1 > 0);
        assert_eq!(
            weight1,
            key1.mem_usage() as u32 + selection1.mem_usage() as u32
        );

        // Test 2: BloomFilterKey
        let mut predicates2 = BTreeMap::new();
        let predicate2 = BloomInListPredicate {
            list: BTreeSet::from([b"test1".to_vec(), b"test2".to_vec()]),
        };
        predicates2.insert(1, vec![predicate2]);
        let key2 = PredicateKey::new_bloom(Arc::new(predicates2));
        let selection2 = Arc::new(RowGroupSelection::from_row_ids(
            [1, 2, 3].into_iter().collect(),
            100,
            1,
        ));
        let weight2 =
            IndexResultCache::index_result_cache_weight(&(key2.clone(), file_id), &selection2);
        assert!(weight2 > 0);
        assert_eq!(
            weight2,
            key2.mem_usage() as u32 + selection2.mem_usage() as u32
        );

        // Test 3: InvertedIndexKey
        let mut predicates3 = BTreeMap::new();
        let predicate3 = Predicate::Range(RangePredicate {
            range: Range {
                lower: None,
                upper: None,
            },
        });
        predicates3.insert(1, vec![predicate3]);
        let key3 = PredicateKey::new_inverted(Arc::new(predicates3));
        let selection3 = Arc::new(RowGroupSelection::from_row_ranges(
            vec![(0, vec![5..15])],
            20,
        ));
        let weight3 =
            IndexResultCache::index_result_cache_weight(&(key3.clone(), file_id), &selection3);
        assert!(weight3 > 0);
        assert_eq!(
            weight3,
            key3.mem_usage() as u32 + selection3.mem_usage() as u32
        );
    }
}
