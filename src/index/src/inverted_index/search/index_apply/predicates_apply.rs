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

use std::mem::size_of;

use async_trait::async_trait;
use greptime_proto::v1::index::InvertedIndexMetas;

use crate::bitmap::Bitmap;
use crate::inverted_index::error::{IndexNotFoundSnafu, Result};
use crate::inverted_index::format::reader::{InvertedIndexReadMetrics, InvertedIndexReader};
use crate::inverted_index::search::fst_apply::{
    FstApplier, IntersectionFstApplier, KeysFstApplier,
};
use crate::inverted_index::search::fst_values_mapper::ParallelFstValuesMapper;
use crate::inverted_index::search::index_apply::{
    ApplyOutput, IndexApplier, IndexNotFoundStrategy, SearchContext,
};
use crate::inverted_index::search::predicate::Predicate;

type IndexName = String;

/// `PredicatesIndexApplier` contains a collection of `FstApplier`s, each associated with an index name,
/// to process and filter index data based on compiled predicates.
pub struct PredicatesIndexApplier {
    /// A list of `FstApplier`s, each associated with a specific index name
    /// (e.g. a tag field uses its column name as index name)
    fst_appliers: Vec<(IndexName, Box<dyn FstApplier>)>,
}

#[async_trait]
impl IndexApplier for PredicatesIndexApplier {
    /// Applies all `FstApplier`s to the data in the inverted index reader, intersecting the individual
    /// bitmaps obtained for each index to result in a final set of indices.
    async fn apply<'a, 'b>(
        &self,
        context: SearchContext,
        reader: &mut (dyn InvertedIndexReader + 'a),
        metrics: Option<&'b mut InvertedIndexReadMetrics>,
    ) -> Result<ApplyOutput> {
        let mut metrics = metrics;
        let metadata = reader.metadata(metrics.as_deref_mut()).await?;
        let mut output = ApplyOutput {
            matched_segment_ids: Bitmap::new_bitvec(),
            total_row_count: metadata.total_row_count as _,
            segment_row_count: metadata.segment_row_count as _,
        };

        // TODO(zhongzc): optimize the order of applying to make it quicker to return empty.
        let mut appliers = Vec::with_capacity(self.fst_appliers.len());
        let mut fst_ranges = Vec::with_capacity(self.fst_appliers.len());

        for (name, fst_applier) in &self.fst_appliers {
            let Some(meta) = metadata.metas.get(name) else {
                match context.index_not_found_strategy {
                    IndexNotFoundStrategy::ReturnEmpty => {
                        return Ok(output);
                    }
                    IndexNotFoundStrategy::Ignore => {
                        continue;
                    }
                    IndexNotFoundStrategy::ThrowError => {
                        return IndexNotFoundSnafu { name }.fail();
                    }
                }
            };
            let fst_offset = meta.base_offset + meta.relative_fst_offset as u64;
            let fst_size = meta.fst_size as u64;
            appliers.push((fst_applier, meta));
            fst_ranges.push(fst_offset..fst_offset + fst_size);
        }

        if fst_ranges.is_empty() {
            output.matched_segment_ids = Self::bitmap_full_range(&metadata);
            return Ok(output);
        }

        let fsts = reader.fst_vec(&fst_ranges, metrics.as_deref_mut()).await?;
        let value_and_meta_vec = fsts
            .into_iter()
            .zip(appliers)
            .map(|(fst, (fst_applier, meta))| (fst_applier.apply(&fst), meta))
            .collect::<Vec<_>>();

        let mut mapper = ParallelFstValuesMapper::new(reader);
        let mut bm_vec = mapper.map_values_vec(&value_and_meta_vec, metrics).await?;

        let mut bitmap = bm_vec.pop().unwrap(); // SAFETY: `fst_ranges` is not empty
        for bm in bm_vec {
            if bm.count_ones() == 0 {
                break;
            }

            bitmap.intersect(bm);
        }

        output.matched_segment_ids = bitmap;
        Ok(output)
    }

    /// Returns the memory usage of the applier.
    fn memory_usage(&self) -> usize {
        let mut size = self.fst_appliers.capacity() * size_of::<(IndexName, Box<dyn FstApplier>)>();
        for (name, fst_applier) in &self.fst_appliers {
            size += name.capacity();
            size += fst_applier.memory_usage();
        }
        size
    }
}

impl PredicatesIndexApplier {
    /// Constructs an instance of `PredicatesIndexApplier` based on a list of tag predicates.
    /// Chooses an appropriate `FstApplier` for each index name based on the nature of its predicates.
    pub fn try_from(mut predicates: Vec<(IndexName, Vec<Predicate>)>) -> Result<Self> {
        let mut fst_appliers = Vec::with_capacity(predicates.len());

        // InList predicates are applied first to benefit from higher selectivity.
        let in_list_index = predicates
            .iter_mut()
            .partition_in_place(|(_, ps)| ps.iter().any(|p| matches!(p, Predicate::InList(_))));
        let mut iter = predicates.into_iter();
        for _ in 0..in_list_index {
            let (column_name, predicates) = iter.next().unwrap();
            let fst_applier = Box::new(KeysFstApplier::try_from(predicates)?) as _;
            fst_appliers.push((column_name, fst_applier));
        }

        for (column_name, predicates) in iter {
            if predicates.is_empty() {
                continue;
            }
            let fst_applier = Box::new(IntersectionFstApplier::try_from(predicates)?) as _;
            fst_appliers.push((column_name, fst_applier));
        }

        Ok(PredicatesIndexApplier { fst_appliers })
    }

    /// Creates a `Bitmap` representing the full range of data in the index for initial scanning.
    fn bitmap_full_range(metadata: &InvertedIndexMetas) -> Bitmap {
        let total_count = metadata.total_row_count;
        let segment_count = metadata.segment_row_count;
        let len = total_count.div_ceil(segment_count);
        Bitmap::full_bitvec(len as _)
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
    use std::collections::VecDeque;
    use std::sync::Arc;

    use greptime_proto::v1::index::{BitmapType, InvertedIndexMeta};

    use super::*;
    use crate::bitmap::Bitmap;
    use crate::inverted_index::FstMap;
    use crate::inverted_index::error::Error;
    use crate::inverted_index::format::reader::MockInvertedIndexReader;
    use crate::inverted_index::search::fst_apply::MockFstApplier;

    fn s(s: &'static str) -> String {
        s.to_owned()
    }

    fn mock_metas(tags: impl IntoIterator<Item = (&'static str, u32)>) -> Arc<InvertedIndexMetas> {
        let mut metas = InvertedIndexMetas {
            total_row_count: 8,
            segment_row_count: 1,
            ..Default::default()
        };
        for (tag, idx) in tags.into_iter() {
            let meta = InvertedIndexMeta {
                name: s(tag),
                relative_fst_offset: idx,
                bitmap_type: BitmapType::Roaring.into(),
                ..Default::default()
            };
            metas.metas.insert(s(tag), meta);
        }
        Arc::new(metas)
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
        };

        // An index reader with a single tag "tag-0" and a corresponding value "tag-0_value-0"
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|_| Ok(mock_metas([("tag-0", 0)])));
        mock_reader.expect_fst_vec().returning(|_ranges, _metrics| {
            Ok(vec![
                FstMap::from_iter([(b"tag-0_value-0", fst_value(2, 1))]).unwrap(),
            ])
        });

        mock_reader
            .expect_bitmap_deque()
            .returning(|arg, _metrics| {
                assert_eq!(arg.len(), 1);
                let range = &arg[0].0;
                let bitmap_type = arg[0].1;
                assert_eq!(*range, 2..3);
                assert_eq!(bitmap_type, BitmapType::Roaring);
                Ok(VecDeque::from([Bitmap::from_lsb0_bytes(
                    &[0b10101010],
                    bitmap_type,
                )]))
            });
        let output = applier
            .apply(SearchContext::default(), &mut mock_reader, None)
            .await
            .unwrap();
        assert_eq!(
            output.matched_segment_ids,
            Bitmap::from_lsb0_bytes(&[0b10101010], BitmapType::Roaring)
        );

        // An index reader with a single tag "tag-0" but without value "tag-0_value-0"
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|_| Ok(mock_metas([("tag-0", 0)])));
        mock_reader.expect_fst_vec().returning(|_range, _metrics| {
            Ok(vec![
                FstMap::from_iter([(b"tag-0_value-1", fst_value(2, 1))]).unwrap(),
            ])
        });
        let output = applier
            .apply(SearchContext::default(), &mut mock_reader, None)
            .await
            .unwrap();
        assert_eq!(output.matched_segment_ids.count_ones(), 0);
    }

    #[tokio::test]
    async fn test_index_applier_apply_intersection_with_two_tags() {
        // An index applier that intersects "tag-0_value-0" on tag "tag-0" and "tag-1_value-a" on tag "tag-1"
        let applier = PredicatesIndexApplier {
            fst_appliers: vec![
                (s("tag-0"), key_fst_applier("tag-0_value-0")),
                (s("tag-1"), key_fst_applier("tag-1_value-a")),
            ],
        };

        // An index reader with two tags "tag-0" and "tag-1" and respective values "tag-0_value-0" and "tag-1_value-a"
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|_| Ok(mock_metas([("tag-0", 0), ("tag-1", 1)])));
        mock_reader.expect_fst_vec().returning(|ranges, _metrics| {
            let mut output = vec![];
            for range in ranges {
                match range.start {
                    0 => output
                        .push(FstMap::from_iter([(b"tag-0_value-0", fst_value(1, 1))]).unwrap()),
                    1 => output
                        .push(FstMap::from_iter([(b"tag-1_value-a", fst_value(2, 1))]).unwrap()),
                    _ => unreachable!(),
                }
            }
            Ok(output)
        });
        mock_reader
            .expect_bitmap_deque()
            .returning(|ranges, _metrics| {
                let mut output = VecDeque::new();
                for (range, bitmap_type) in ranges {
                    let offset = range.start;
                    let size = range.end - range.start;
                    match (offset, size, bitmap_type) {
                        (1, 1, BitmapType::Roaring) => {
                            output.push_back(Bitmap::from_lsb0_bytes(&[0b10101010], *bitmap_type))
                        }
                        (2, 1, BitmapType::Roaring) => {
                            output.push_back(Bitmap::from_lsb0_bytes(&[0b11011011], *bitmap_type))
                        }
                        _ => unreachable!(),
                    }
                }

                Ok(output)
            });

        let output = applier
            .apply(SearchContext::default(), &mut mock_reader, None)
            .await
            .unwrap();
        assert_eq!(
            output.matched_segment_ids,
            Bitmap::from_lsb0_bytes(&[0b10001010], BitmapType::Roaring)
        );
    }

    #[tokio::test]
    async fn test_index_applier_without_predicates() {
        let applier = PredicatesIndexApplier {
            fst_appliers: vec![],
        };

        let mut mock_reader: MockInvertedIndexReader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|_| Ok(mock_metas([("tag-0", 0)])));

        let output = applier
            .apply(SearchContext::default(), &mut mock_reader, None)
            .await
            .unwrap();
        assert_eq!(output.matched_segment_ids, Bitmap::full_bitvec(8)); // full range to scan
    }

    #[tokio::test]
    async fn test_index_applier_with_empty_index() {
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader.expect_metadata().returning(move |_| {
            Ok(Arc::new(InvertedIndexMetas {
                total_row_count: 0, // No rows
                segment_row_count: 1,
                ..Default::default()
            }))
        });

        let mut mock_fst_applier = MockFstApplier::new();
        mock_fst_applier.expect_apply().never();

        let applier = PredicatesIndexApplier {
            fst_appliers: vec![(s("tag-0"), Box::new(mock_fst_applier))],
        };

        let output = applier
            .apply(SearchContext::default(), &mut mock_reader, None)
            .await
            .unwrap();
        assert!(output.matched_segment_ids.is_empty());
    }

    #[tokio::test]
    async fn test_index_applier_with_nonexistent_index() {
        let mut mock_reader = MockInvertedIndexReader::new();
        mock_reader
            .expect_metadata()
            .returning(|_| Ok(mock_metas(vec![])));

        let mut mock_fst_applier = MockFstApplier::new();
        mock_fst_applier.expect_apply().never();

        let applier = PredicatesIndexApplier {
            fst_appliers: vec![(s("tag-0"), Box::new(mock_fst_applier))],
        };

        let result = applier
            .apply(
                SearchContext {
                    index_not_found_strategy: IndexNotFoundStrategy::ThrowError,
                },
                &mut mock_reader,
                None,
            )
            .await;
        assert!(matches!(result, Err(Error::IndexNotFound { .. })));

        let output = applier
            .apply(
                SearchContext {
                    index_not_found_strategy: IndexNotFoundStrategy::ReturnEmpty,
                },
                &mut mock_reader,
                None,
            )
            .await
            .unwrap();
        assert!(output.matched_segment_ids.is_empty());

        let output = applier
            .apply(
                SearchContext {
                    index_not_found_strategy: IndexNotFoundStrategy::Ignore,
                },
                &mut mock_reader,
                None,
            )
            .await
            .unwrap();
        assert_eq!(output.matched_segment_ids, Bitmap::full_bitvec(8));
    }

    #[test]
    fn test_index_applier_memory_usage() {
        let mut mock_fst_applier = MockFstApplier::new();
        mock_fst_applier.expect_memory_usage().returning(|| 100);

        let applier = PredicatesIndexApplier {
            fst_appliers: vec![(s("tag-0"), Box::new(mock_fst_applier))],
        };

        assert_eq!(
            applier.memory_usage(),
            size_of::<(IndexName, Box<dyn FstApplier>)>() + 5 + 100
        );
    }
}
