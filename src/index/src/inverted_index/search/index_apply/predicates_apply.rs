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

use async_trait::async_trait;
use common_base::BitVec;
use greptime_proto::v1::index::InvertedIndexMetas;

use super::SearchContext;
use crate::inverted_index::error::{IndexNotFoundSnafu, Result};
use crate::inverted_index::format::reader::InvertedIndexReader;
use crate::inverted_index::search::fst_apply::{
    FstApplier, IntersectionFstApplier, KeysFstApplier,
};
use crate::inverted_index::search::fst_values_mapper::FstValuesMapper;
use crate::inverted_index::search::index_apply::IndexApplier;
use crate::inverted_index::search::predicate::Predicate;

type IndexName = String;

/// `PredicatesIndexApplier` contains a collection of `FstApplier`s, each associated with an index name,
/// to process and filter index data based on compiled predicates.
pub struct PredicatesIndexApplier {
    /// A list of `FstApplier`s, each associated with a specific index name
    /// (e.g. a tag field uses its column name as index name)
    fst_appliers: Vec<(IndexName, Box<dyn FstApplier>)>,

    /// Options for reading an inverted index (e.g., how to handle missing index).
    options: ReadOptions,
}

#[async_trait]
impl IndexApplier for PredicatesIndexApplier {
    /// Applies all `FstApplier`s to the data in the inverted index reader, intersecting the individual
    /// bitmaps obtained for each index to result in a final set of indices.
    async fn apply(
        &self,
        _context: SearchContext,
        reader: &mut dyn InvertedIndexReader,
    ) -> Result<Vec<usize>> {
        let metadata = reader.metadata().await?;

        let mut bitmap = Self::bitmap_full_range(&metadata);
        // TODO(zhongzc): optimize the order of applying to make it quicker to return empty.
        for (name, fst_applier) in &self.fst_appliers {
            if bitmap.count_ones() == 0 {
                break;
            }

            let Some(meta) = metadata.metas.get(name) else {
                match self.options.index_not_found_strategy {
                    IndexNotFoundStrategy::ReturnEmpty => {
                        return Ok(vec![]);
                    }
                    IndexNotFoundStrategy::ReturnFullRange => {
                        continue;
                    }
                    IndexNotFoundStrategy::ThrowError => {
                        return IndexNotFoundSnafu { name }.fail();
                    }
                }
            };

            let fst = reader.fst(meta).await?;
            let values = fst_applier.apply(&fst);

            let mut mapper = FstValuesMapper::new(&mut *reader, meta);
            let bm = mapper.map_values(&values).await?;

            bitmap &= bm;
        }

        Ok(bitmap.iter_ones().collect())
    }
}

impl PredicatesIndexApplier {
    /// Constructs an instance of `PredicatesIndexApplier` based on a list of tag predicates.
    /// Chooses an appropriate `FstApplier` for each index name based on the nature of its predicates.
    ///
    /// * `predicates`: A list of predicates for each index name.
    /// * `options`: Options for reading an inverted index (e.g., how to handle missing index).
    pub fn try_new(
        mut predicates: Vec<(IndexName, Vec<Predicate>)>,
        options: ReadOptions,
    ) -> Result<Self> {
        let mut fst_appliers = Vec::with_capacity(predicates.len());

        // InList predicates are applied first to benefit from higher selectivity.
        let in_list_index = predicates
            .iter_mut()
            .partition_in_place(|(_, ps)| ps.iter().any(|p| matches!(p, Predicate::InList(_))));
        let mut iter = predicates.into_iter();
        for _ in 0..in_list_index {
            let (tag_name, predicates) = iter.next().unwrap();
            let fst_applier = Box::new(KeysFstApplier::try_from(predicates)?) as _;
            fst_appliers.push((tag_name, fst_applier));
        }

        for (tag_name, predicates) in iter {
            if predicates.is_empty() {
                continue;
            }
            let fst_applier = Box::new(IntersectionFstApplier::try_from(predicates)?) as _;
            fst_appliers.push((tag_name, fst_applier));
        }

        Ok(PredicatesIndexApplier {
            fst_appliers,
            options,
        })
    }

    /// Creates a `BitVec` representing the full range of data in the index for initial scanning.
    fn bitmap_full_range(metadata: &InvertedIndexMetas) -> BitVec {
        let total_count = metadata.total_row_count;
        let segment_count = metadata.segment_row_count;
        let len = (total_count + segment_count - 1) / segment_count;
        BitVec::repeat(true, len as _)
    }
}

/// Options for reading an inverted index.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct ReadOptions {
    /// `index_not_found_strategy` controls the behavior of the applier when the index is not found.
    pub index_not_found_strategy: IndexNotFoundStrategy,
}

/// Defines the behavior of an applier when the index is not found.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum IndexNotFoundStrategy {
    /// Return an empty list of indices.
    #[default]
    ReturnEmpty,

    /// Return the full range of indices.
    ReturnFullRange,

    /// Throw an error.
    ThrowError,
}

#[cfg(test)]
mod tests {
    use common_base::bit_vec::prelude::*;
    use greptime_proto::v1::index::InvertedIndexMeta;

    use super::*;
    use crate::inverted_index::error::Error;
    use crate::inverted_index::format::reader::MockInvertedIndexReader;
    use crate::inverted_index::search::fst_apply::MockFstApplier;
    use crate::inverted_index::FstMap;

    fn s(s: &'static str) -> String {
        s.to_owned()
    }

    fn mock_metas(tags: impl IntoIterator<Item = &'static str>) -> InvertedIndexMetas {
        let mut metas = InvertedIndexMetas {
            total_row_count: 8,
            segment_row_count: 1,
            ..Default::default()
        };
        for tag in tags.into_iter() {
            let meta = InvertedIndexMeta {
                name: s(tag),
                ..Default::default()
            };
            metas.metas.insert(s(tag), meta);
        }
        metas
    }

    fn key_fst_applier(value: &'static str) -> Box<dyn FstApplier> {
        let mut mock_fst_applier = MockFstApplier::new();
        mock_fst_applier
            .expect_apply()
            .returning(move |fst| fst.get(value).into_iter().collect());
        Box::new(mock_fst_applier)
    }

    fn fst_value(offset: u32, size: u32) -> u64 {
        bytemuck::cast::<_, u64>([offset, size])
    }

    #[tokio::test]
    async fn test_index_applier_apply_get_key() {
        // An index applier that point-gets "tag-0_value-0" on tag "tag-0"
        let applier = PredicatesIndexApplier {
            fst_appliers: vec![(s("tag-0"), key_fst_applier("tag-0_value-0"))],
            options: ReadOptions::default(),
        };

        // An index reader with a single tag "tag-0" and a corresponding value "tag-0_value-0"
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|| Ok(mock_metas(["tag-0"])));
        mock_reader
            .expect_fst()
            .returning(|meta| match meta.name.as_str() {
                "tag-0" => Ok(FstMap::from_iter([(b"tag-0_value-0", fst_value(2, 1))]).unwrap()),
                _ => unreachable!(),
            });
        mock_reader.expect_bitmap().returning(|meta, offset, size| {
            match (meta.name.as_str(), offset, size) {
                ("tag-0", 2, 1) => Ok(bitvec![u8, Lsb0; 1, 0, 1, 0, 1, 0, 1, 0]),
                _ => unreachable!(),
            }
        });
        let indices = applier
            .apply(SearchContext::default(), &mut mock_reader)
            .await
            .unwrap();
        assert_eq!(indices, vec![0, 2, 4, 6]);

        // An index reader with a single tag "tag-0" but without value "tag-0_value-0"
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|| Ok(mock_metas(["tag-0"])));
        mock_reader
            .expect_fst()
            .returning(|meta| match meta.name.as_str() {
                "tag-0" => Ok(FstMap::from_iter([(b"tag-0_value-1", fst_value(2, 1))]).unwrap()),
                _ => unreachable!(),
            });
        let indices = applier
            .apply(SearchContext::default(), &mut mock_reader)
            .await
            .unwrap();
        assert!(indices.is_empty());
    }

    #[tokio::test]
    async fn test_index_applier_apply_intersection_with_two_tags() {
        // An index applier that intersects "tag-0_value-0" on tag "tag-0" and "tag-1_value-a" on tag "tag-1"
        let applier = PredicatesIndexApplier {
            fst_appliers: vec![
                (s("tag-0"), key_fst_applier("tag-0_value-0")),
                (s("tag-1"), key_fst_applier("tag-1_value-a")),
            ],
            options: ReadOptions::default(),
        };

        // An index reader with two tags "tag-0" and "tag-1" and respective values "tag-0_value-0" and "tag-1_value-a"
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|| Ok(mock_metas(["tag-0", "tag-1"])));
        mock_reader
            .expect_fst()
            .returning(|meta| match meta.name.as_str() {
                "tag-0" => Ok(FstMap::from_iter([(b"tag-0_value-0", fst_value(1, 1))]).unwrap()),
                "tag-1" => Ok(FstMap::from_iter([(b"tag-1_value-a", fst_value(2, 1))]).unwrap()),
                _ => unreachable!(),
            });
        mock_reader.expect_bitmap().returning(|meta, offset, size| {
            match (meta.name.as_str(), offset, size) {
                ("tag-0", 1, 1) => Ok(bitvec![u8, Lsb0; 1, 0, 1, 0, 1, 0, 1, 0]),
                ("tag-1", 2, 1) => Ok(bitvec![u8, Lsb0; 1, 1, 0, 1, 1, 0, 1, 1]),
                _ => unreachable!(),
            }
        });

        let indices = applier
            .apply(SearchContext::default(), &mut mock_reader)
            .await
            .unwrap();
        assert_eq!(indices, vec![0, 4, 6]);
    }

    #[tokio::test]
    async fn test_index_applier_without_predicates() {
        let applier = PredicatesIndexApplier {
            fst_appliers: vec![],
            options: ReadOptions::default(),
        };

        let mut mock_reader: MockInvertedIndexReader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|| Ok(mock_metas(["tag-0"])));

        let indices = applier
            .apply(SearchContext::default(), &mut mock_reader)
            .await
            .unwrap();
        assert_eq!(indices, vec![0, 1, 2, 3, 4, 5, 6, 7]); // full range to scan
    }

    #[tokio::test]
    async fn test_index_applier_with_empty_index() {
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader.expect_metadata().returning(move || {
            Ok(InvertedIndexMetas {
                total_row_count: 0, // No rows
                segment_row_count: 1,
                ..Default::default()
            })
        });

        let mut mock_fst_applier = MockFstApplier::new();
        mock_fst_applier.expect_apply().never();

        let applier = PredicatesIndexApplier {
            fst_appliers: vec![(s("tag-0"), Box::new(mock_fst_applier))],
            options: ReadOptions::default(),
        };

        let indices = applier
            .apply(SearchContext::default(), &mut mock_reader)
            .await
            .unwrap();
        assert!(indices.is_empty());
    }

    #[tokio::test]
    async fn test_index_applier_with_nonexistent_index() {
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|| Ok(mock_metas(vec![])));

        let applier = PredicatesIndexApplier {
            fst_appliers: vec![(s("tag-0"), key_fst_applier("tag-0_value-0"))],
            options: ReadOptions {
                index_not_found_strategy: IndexNotFoundStrategy::ThrowError,
            },
        };

        let result = applier
            .apply(SearchContext::default(), &mut mock_reader)
            .await;
        assert!(matches!(result, Err(Error::IndexNotFound { .. })));

        let applier = PredicatesIndexApplier {
            fst_appliers: vec![(s("tag-0"), key_fst_applier("tag-0_value-0"))],
            options: ReadOptions {
                index_not_found_strategy: IndexNotFoundStrategy::ReturnEmpty,
            },
        };
        let indices = applier
            .apply(SearchContext::default(), &mut mock_reader)
            .await
            .unwrap();
        assert!(indices.is_empty());

        let applier = PredicatesIndexApplier {
            fst_appliers: vec![(s("tag-0"), key_fst_applier("tag-0_value-0"))],
            options: ReadOptions {
                index_not_found_strategy: IndexNotFoundStrategy::ReturnFullRange,
            },
        };
        let indices = applier
            .apply(SearchContext::default(), &mut mock_reader)
            .await
            .unwrap();
        assert_eq!(indices, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }
}
