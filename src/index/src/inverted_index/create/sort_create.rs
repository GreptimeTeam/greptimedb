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

use std::collections::HashMap;

use async_trait::async_trait;
use snafu::ensure;

use crate::inverted_index::create::sort::{SortOutput, Sorter};
use crate::inverted_index::create::IndexCreator;
use crate::inverted_index::error::{InconsistentRowCountSnafu, Result};
use crate::inverted_index::format::writer::InvertedIndexWriter;
use crate::inverted_index::Bytes;

type IndexName = String;
type SegmentRowCount = usize;

/// Factory type to produce `Sorter` instances associated with an index name and segment row count
pub type SorterFactory = Box<dyn Fn(IndexName, SegmentRowCount) -> Box<dyn Sorter> + Send>;

/// `SortIndexCreator` orchestrates indexing by sorting input data for each named index
/// and writing to an inverted index writer
pub struct SortIndexCreator {
    /// Factory for producing `Sorter` instances
    sorter_factory: SorterFactory,

    /// Map of index names to sorters
    sorters: HashMap<IndexName, Box<dyn Sorter>>,

    /// Writer for inverted index data
    index_writer: Box<dyn InvertedIndexWriter>,

    /// Number of rows in each segment, used to produce sorters
    segment_row_count: usize,
}

#[async_trait]
impl IndexCreator for SortIndexCreator {
    /// Inserts a value or null into the sorter for the specified index
    async fn push_with_name(&mut self, index_name: &str, value: Option<Bytes>) -> Result<()> {
        match self.sorters.get_mut(index_name) {
            Some(sorter) => sorter.push(value).await,
            None => {
                let mut sorter =
                    (self.sorter_factory)(index_name.to_owned(), self.segment_row_count);
                sorter.push(value).await?;
                self.sorters.insert(index_name.to_owned(), sorter);
                Ok(())
            }
        }
    }

    /// Finalizes the sorting for all indexes and writes them using the inverted index writer
    async fn finish(&mut self) -> Result<()> {
        let mut row_count = None;

        for (index_name, mut sorter) in self.sorters.drain() {
            let SortOutput {
                null_bitmap,
                sorted_stream,
                total_row_count,
            } = sorter.output().await?;

            let expected_row_count = *row_count.get_or_insert(total_row_count);
            ensure!(
                expected_row_count == total_row_count,
                InconsistentRowCountSnafu {
                    index_name,
                    total_row_count,
                    expected_row_count,
                }
            );

            self.index_writer
                .add_index(index_name, null_bitmap, sorted_stream)
                .await?;
        }

        self.index_writer
            .finish(
                row_count.unwrap_or_default() as _,
                self.segment_row_count as _,
            )
            .await
    }
}

impl SortIndexCreator {
    /// Creates a new `SortIndexCreator` with the given sorter factory and index writer
    pub fn new(
        sorter_factory: SorterFactory,
        index_writer: Box<dyn InvertedIndexWriter>,
        segment_row_count: usize,
    ) -> Self {
        Self {
            sorter_factory,
            sorters: HashMap::new(),
            index_writer,
            segment_row_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use common_base::BitVec;
    use futures::{stream, StreamExt};

    use super::*;
    use crate::inverted_index::create::sort::SortedStream;
    use crate::inverted_index::error::Error;
    use crate::inverted_index::format::writer::MockInvertedIndexWriter;

    fn stream_to_values(stream: SortedStream) -> Vec<Bytes> {
        futures::executor::block_on(async {
            stream.map(|r| r.unwrap().0).collect::<Vec<Bytes>>().await
        })
    }

    #[tokio::test]
    async fn test_sort_index_creator_basic() {
        let mut mock_writer = MockInvertedIndexWriter::new();
        mock_writer
            .expect_add_index()
            .times(3)
            .returning(|name, null_bitmap, stream| {
                assert!(null_bitmap.is_empty());
                match name.as_str() {
                    "a" => assert_eq!(stream_to_values(stream), vec![b"1", b"2", b"3"]),
                    "b" => assert_eq!(stream_to_values(stream), vec![b"4", b"5", b"6"]),
                    "c" => assert_eq!(stream_to_values(stream), vec![b"1", b"2", b"3"]),
                    _ => panic!("unexpected index name: {}", name),
                }
                Ok(())
            });
        mock_writer
            .expect_finish()
            .times(1)
            .returning(|total_row_count, segment_row_count| {
                assert_eq!(total_row_count, 3);
                assert_eq!(segment_row_count, 1);
                Ok(())
            });

        let mut creator = SortIndexCreator::new(NaiveSorter::factory(), Box::new(mock_writer), 1);

        let index_values = vec![
            ("a", vec![b"3", b"2", b"1"]),
            ("b", vec![b"6", b"5", b"4"]),
            ("c", vec![b"1", b"2", b"3"]),
        ];

        for (index_name, values) in index_values {
            for value in values {
                creator
                    .push_with_name(index_name, Some(value.into()))
                    .await
                    .unwrap();
            }
        }

        creator.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_sort_index_creator_inconsistant_row_count() {
        let mut mock_writer = MockInvertedIndexWriter::new();
        mock_writer
            .expect_add_index()
            .returning(|name, null_bitmap, stream| {
                assert!(null_bitmap.is_empty());
                match name.as_str() {
                    "a" => assert_eq!(stream_to_values(stream), vec![b"1", b"2", b"3"]),
                    "b" => assert_eq!(stream_to_values(stream), vec![b"4", b"5", b"6"]),
                    "c" => assert_eq!(stream_to_values(stream), vec![b"1", b"2"]),
                    _ => panic!("unexpected index name: {}", name),
                }
                Ok(())
            });
        mock_writer.expect_finish().never();

        let mut creator = SortIndexCreator::new(NaiveSorter::factory(), Box::new(mock_writer), 1);

        let index_values = vec![
            ("a", vec![b"3", b"2", b"1"]),
            ("b", vec![b"6", b"5", b"4"]),
            ("c", vec![b"1", b"2"]),
        ];

        for (index_name, values) in index_values {
            for value in values {
                creator
                    .push_with_name(index_name, Some(value.into()))
                    .await
                    .unwrap();
            }
        }

        let res = creator.finish().await;
        assert!(matches!(res, Err(Error::InconsistentRowCount { .. })));
    }

    fn set_bit(bit_vec: &mut BitVec, index: usize) {
        if index >= bit_vec.len() {
            bit_vec.resize(index + 1, false);
        }
        bit_vec.set(index, true);
    }

    struct NaiveSorter {
        total_row_count: usize,
        segment_row_count: usize,
        values: BTreeMap<Option<Bytes>, BitVec>,
    }

    impl NaiveSorter {
        fn factory() -> SorterFactory {
            Box::new(|_index_name, segment_row_count| {
                Box::new(NaiveSorter {
                    total_row_count: 0,
                    segment_row_count,
                    values: BTreeMap::new(),
                })
            })
        }
    }

    #[async_trait]
    impl Sorter for NaiveSorter {
        async fn push(&mut self, value: Option<Bytes>) -> Result<()> {
            let segment_index = self.total_row_count / self.segment_row_count;
            self.total_row_count += 1;

            let bitmap = self.values.entry(value).or_default();
            set_bit(bitmap, segment_index);

            Ok(())
        }

        async fn output(&mut self) -> Result<SortOutput> {
            let null_bitmap = self.values.remove(&None).unwrap_or_default();

            Ok(SortOutput {
                null_bitmap,
                sorted_stream: Box::new(stream::iter(
                    std::mem::take(&mut self.values)
                        .into_iter()
                        .map(|(v, b)| Ok((v.unwrap(), b))),
                )),
                total_row_count: self.total_row_count,
            })
        }
    }
}
