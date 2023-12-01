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
use snafu::OptionExt;

use crate::inverted_index::error::{Result, TagNotFoundInIndexSnafu};
use crate::inverted_index::format::reader::InvertedIndexReader;
use crate::inverted_index::search::fst_apply::{
    FstApplier, IntersectionFstApplier, KeysFstApplier,
};
use crate::inverted_index::search::fst_values_mapper::FstValuesMapper;
use crate::inverted_index::search::index_apply::IndexApplier;
use crate::inverted_index::search::predicate::Predicate;

/// `PredicatesIndexApplier` contains a collection of `FstApplier`s, each associated with a tag field,
/// to process and filter index data based on compiled predicates.
pub struct PredicatesIndexApplier {
    /// A list of `FstApplier`s, each associated with a tag field.
    fst_appliers: Vec<(String, Box<dyn FstApplier>)>,
}

#[async_trait]
impl IndexApplier for PredicatesIndexApplier {
    /// Applies all `FstApplier`s to the data in the index, intersecting the individual
    /// bitmaps obtained for each tag to result in a final set of indices.
    async fn apply(&self, reader: &mut dyn InvertedIndexReader) -> Result<Vec<usize>> {
        let metadata = reader.metadata().await?;

        let mut bitmap = Self::bitmap_full_range(&metadata);
        for (tag_name, fst_applier) in &self.fst_appliers {
            if bitmap.count_ones() == 0 {
                break;
            }

            // TODO(zhongzc): how to handle missing columns?
            let meta = metadata
                .metas
                .get(tag_name)
                .with_context(|| TagNotFoundInIndexSnafu {
                    tag_name: tag_name.to_owned(),
                })?;

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
    /// Chooses an appropriate `FstApplier` for each tag based on the nature of its predicates.
    pub fn try_from(predicates: Vec<(String, Vec<Predicate>)>) -> Result<Self> {
        let mut fst_appliers = Vec::with_capacity(predicates.len());

        for (tag_name, predicates) in predicates {
            if predicates.is_empty() {
                continue;
            }

            let exists_in_list = predicates.iter().any(|p| matches!(p, Predicate::InList(_)));
            let fst_applier = if exists_in_list {
                Box::new(KeysFstApplier::try_from(predicates)?) as _
            } else {
                Box::new(IntersectionFstApplier::try_from(predicates)?) as _
            };

            fst_appliers.push((tag_name, fst_applier));
        }

        Ok(PredicatesIndexApplier { fst_appliers })
    }

    /// Creates a `BitVec` representing the full range of data in the index for initial scanning.
    fn bitmap_full_range(metadata: &InvertedIndexMetas) -> BitVec {
        let total_count = metadata.total_row_count;
        let segment_count = metadata.segment_row_count;
        let len = (total_count + segment_count - 1) / segment_count;
        BitVec::repeat(true, len as _)
    }
}

impl TryFrom<Vec<(String, Vec<Predicate>)>> for PredicatesIndexApplier {
    type Error = crate::inverted_index::error::Error;
    fn try_from(predicates: Vec<(String, Vec<Predicate>)>) -> Result<Self> {
        Self::try_from(predicates)
    }
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

    fn mock_metas<'a>(tags: impl IntoIterator<Item = &'a str>) -> InvertedIndexMetas {
        let mut metas = InvertedIndexMetas {
            total_row_count: 8,
            segment_row_count: 1,
            ..Default::default()
        };
        for tag in tags.into_iter() {
            let meta = InvertedIndexMeta {
                name: "".to_owned(),
                ..Default::default()
            };
            metas.metas.insert(tag.into(), meta);
        }
        metas
    }

    #[tokio::test]
    async fn test_index_applier_apply_point_get() {
        // A point get fst applier
        let mut mock_fst_applier = MockFstApplier::new();
        mock_fst_applier
            .expect_apply()
            .returning(|fst| fst.get("tag0_value0").into_iter().collect());

        // A index applier that applies point get "tag0_value0" to tag "tag0"
        let applier = PredicatesIndexApplier {
            fst_appliers: vec![("tag0".to_owned(), Box::new(mock_fst_applier))],
        };

        // index reader with a single tag "tag0" and a single value "tag0_value0"
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|| Ok(mock_metas(["tag0"])));
        mock_reader
            .expect_fst()
            .returning(|meta| match meta.name.as_str() {
                "tag0" => Ok(FstMap::from_iter([(b"tag0_value0", (2 << 32) | 1)]).unwrap()),
                _ => unreachable!(),
            });
        mock_reader.expect_bitmap().returning(|meta, offset, size| {
            match (meta.name.as_str(), offset, size) {
                ("tag0", 2, 1) => Ok(bitvec![u8, Lsb0; 1, 0, 1, 0, 1, 0, 1, 0]),
                _ => unreachable!(),
            }
        });
        let indices = applier.apply(&mut mock_reader).await.unwrap();
        assert_eq!(indices, vec![0, 2, 4, 6]);

        // index reader with a single tag "tag0" and without value "tag0_value0"
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|| Ok(mock_metas(["tag0"])));
        mock_reader
            .expect_fst()
            .returning(|meta| match meta.name.as_str() {
                "tag0" => Ok(FstMap::from_iter([(b"tag0_value1", (2 << 32) | 1)]).unwrap()),
                _ => unreachable!(),
            });
        let indices = applier.apply(&mut mock_reader).await.unwrap();
        assert!(indices.is_empty());

        // // index reader without tag "tag0"
        // let mut mock_reader = MockInvertedIndexReader::new();
        // mock_reader
        //     .expect_metadata()
        //     .returning(|| Ok(mock_metas(["tag1"])));
        // let result = applier.apply(&mut mock_reader).await;
        // assert!(matches!(result, Err(Error::TagNotFoundInIndex { .. })));
    }

    #[tokio::test]
    async fn test_index_applier_apply_intersection_with_two_tags() {
        // A point get fst applier for tag "tag0"
        let mut mock_fst_applier = MockFstApplier::new();
        mock_fst_applier
            .expect_apply()
            .returning(|fst| fst.get("tag0_value0").into_iter().collect());
        // Another point get fst applier for tag "tag1"
        let mut mock_fst_applier2 = MockFstApplier::new();
        mock_fst_applier2
            .expect_apply()
            .returning(|fst| fst.get("tag1_value0").into_iter().collect());

        // A index applier that intersects point get "tag0_value0" and "tag1_value0"
        let applier = PredicatesIndexApplier {
            fst_appliers: vec![
                ("tag0".to_owned(), Box::new(mock_fst_applier)),
                ("tag1".to_owned(), Box::new(mock_fst_applier2)),
            ],
        };

        // index reader with two tags "tag0" and "tag1" and a single value "tag0_value0" and "tag1_value0"
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|| Ok(mock_metas(["tag0", "tag1"])));
        mock_reader
            .expect_fst()
            .returning(|meta| match meta.name.as_str() {
                "tag0" => Ok(FstMap::from_iter([(b"tag0_value0", (1 << 32) | 1)]).unwrap()),
                "tag1" => Ok(FstMap::from_iter([(b"tag1_value0", (2 << 32) | 1)]).unwrap()),
                _ => unreachable!(),
            });
        mock_reader.expect_bitmap().returning(|meta, offset, size| {
            match (meta.name.as_str(), offset, size) {
                ("tag0", 1, 1) => Ok(bitvec![u8, Lsb0; 1, 0, 1, 0, 1, 0, 1, 0]),
                ("tag1", 2, 1) => Ok(bitvec![u8, Lsb0; 1, 1, 0, 1, 1, 0, 1, 1]),
                _ => unreachable!(),
            }
        });

        let indices = applier.apply(&mut mock_reader).await.unwrap();
        assert_eq!(indices, vec![0, 4, 6]);
    }

    #[tokio::test]
    async fn test_index_applier_without_predicates() {
        let applier = PredicatesIndexApplier {
            fst_appliers: vec![],
        };

        let mut mock_reader: MockInvertedIndexReader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|| Ok(mock_metas(["tag0"])));

        let indices = applier.apply(&mut mock_reader).await.unwrap();
        assert_eq!(indices, vec![0, 1, 2, 3, 4, 5, 6, 7]); // scan all
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
            fst_appliers: vec![("tag0".to_owned(), Box::new(mock_fst_applier))],
        };

        let indices = applier.apply(&mut mock_reader).await.unwrap();
        assert!(indices.is_empty());
    }

    #[tokio::test]
    async fn test_index_applier_with_nonexistent_tag() {
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|| Ok(mock_metas(vec![])));

        let mut mock_fst_applier = MockFstApplier::new();
        mock_fst_applier.expect_apply().never();

        let applier = PredicatesIndexApplier {
            fst_appliers: vec![("tag0".to_owned(), Box::new(mock_fst_applier))],
        };

        let result = applier.apply(&mut mock_reader).await;
        assert!(matches!(result, Err(Error::TagNotFoundInIndex { .. })));
    }
}
