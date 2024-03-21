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

use std::ops::Range;

use crate::error::Result;
use crate::memtable::partition_tree::data::DataBatch;
use crate::memtable::partition_tree::shard::DataBatchSource;
use crate::memtable::partition_tree::PkId;

/// A reader that dedup sorted batches from a merger.
pub struct DedupReader<T> {
    prev_batch_last_row: Option<(PkId, i64)>,
    current_batch_range: Option<Range<usize>>,
    inner: T,
}

impl<T: DataBatchSource> DedupReader<T> {
    /// Creates a new dedup reader.
    pub fn try_new(inner: T) -> Result<Self> {
        let mut res = Self {
            prev_batch_last_row: None,
            current_batch_range: None,
            inner,
        };
        res.next()?;
        Ok(res)
    }
}

impl<T: DataBatchSource> DataBatchSource for DedupReader<T> {
    fn is_valid(&self) -> bool {
        self.current_batch_range.is_some()
    }

    fn next(&mut self) -> Result<()> {
        while self.inner.is_valid() {
            match &mut self.prev_batch_last_row {
                None => {
                    // First shot, fill prev_batch_last_row and current_batch_range with first batch.
                    let current_batch = self.inner.current_data_batch();
                    let pk_id = self.inner.current_pk_id();
                    let (last_ts, _) = current_batch.last_row();
                    self.prev_batch_last_row = Some((pk_id, last_ts));
                    self.current_batch_range = Some(0..current_batch.num_rows());
                    break;
                }
                Some(prev_last_row) => {
                    self.inner.next()?;
                    if !self.inner.is_valid() {
                        // Resets current_batch_range if inner reader is exhausted.
                        self.current_batch_range = None;
                        break;
                    }
                    let current_batch = self.inner.current_data_batch();
                    let current_pk_id = self.inner.current_pk_id();
                    let (first_ts, _) = current_batch.first_row();
                    let rows_in_batch = current_batch.num_rows();

                    let (start, end) = if &(current_pk_id, first_ts) == prev_last_row {
                        // First row in this batch duplicated with the last row in previous batch
                        if rows_in_batch == 1 {
                            // If batch is exhausted, move to next batch.
                            continue;
                        } else {
                            // Skip the first row, start from offset 1.
                            (1, rows_in_batch)
                        }
                    } else {
                        // No duplicates found, yield whole batch.
                        (0, rows_in_batch)
                    };

                    let (last_ts, _) = current_batch.last_row();
                    *prev_last_row = (current_pk_id, last_ts);
                    self.current_batch_range = Some(start..end);
                    break;
                }
            }
        }
        Ok(())
    }

    fn current_pk_id(&self) -> PkId {
        self.inner.current_pk_id()
    }

    fn current_key(&self) -> Option<&[u8]> {
        self.inner.current_key()
    }

    fn current_data_batch(&self) -> DataBatch {
        let range = self.current_batch_range.as_ref().unwrap();
        let data_batch = self.inner.current_data_batch();
        data_batch.slice(range.start, range.len())
    }
}

#[cfg(test)]
mod tests {
    use store_api::metadata::RegionMetadataRef;

    use super::*;
    use crate::memtable::partition_tree::data::{DataBuffer, DataParts, DataPartsReader};
    use crate::test_util::memtable_util::{
        extract_data_batch, metadata_for_test, write_rows_to_buffer,
    };

    struct MockSource(DataPartsReader);

    impl DataBatchSource for MockSource {
        fn is_valid(&self) -> bool {
            self.0.is_valid()
        }

        fn next(&mut self) -> Result<()> {
            self.0.next()
        }

        fn current_pk_id(&self) -> PkId {
            PkId {
                shard_id: 0,
                pk_index: self.0.current_data_batch().pk_index(),
            }
        }

        fn current_key(&self) -> Option<&[u8]> {
            None
        }

        fn current_data_batch(&self) -> DataBatch {
            self.0.current_data_batch()
        }
    }

    fn build_data_buffer(
        meta: RegionMetadataRef,
        rows: Vec<(u16, Vec<i64>)>,
        seq: &mut u64,
    ) -> DataBuffer {
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, true);

        for row in rows {
            let (pk_index, timestamps) = row;
            let num_rows = timestamps.len() as u64;
            let v = timestamps.iter().map(|v| Some(*v as f64)).collect();

            write_rows_to_buffer(&mut buffer, &meta, pk_index, timestamps, v, *seq);
            *seq += num_rows;
        }
        buffer
    }

    fn check_data_parts_reader_dedup(
        parts: Vec<Vec<(u16, Vec<i64>)>>,
        expected: Vec<(u16, Vec<(i64, u64)>)>,
    ) {
        let meta = metadata_for_test();
        let mut seq = 0;

        let mut frozens = Vec::with_capacity(parts.len());
        for part in parts {
            let mut buffer1 = build_data_buffer(meta.clone(), part, &mut seq);
            let part1 = buffer1.freeze(None, false).unwrap();
            frozens.push(part1);
        }

        let parts = DataParts::new(meta, 10, true).with_frozen(frozens);

        let mut res = Vec::with_capacity(expected.len());
        let mut reader =
            DedupReader::try_new(MockSource(parts.read().unwrap().build().unwrap())).unwrap();
        while reader.is_valid() {
            let batch = reader.current_data_batch();
            res.push(extract_data_batch(&batch));
            reader.next().unwrap();
        }

        assert_eq!(expected, res);
    }

    #[test]
    fn test_data_parts_reader_dedup() {
        check_data_parts_reader_dedup(vec![vec![(0, vec![1, 2])]], vec![(0, vec![(1, 0), (2, 1)])]);

        check_data_parts_reader_dedup(
            vec![
                vec![(0, vec![1, 2])],
                vec![(0, vec![1, 2])],
                vec![(0, vec![2, 3])],
            ],
            vec![(0, vec![(1, 2)]), (0, vec![(2, 4)]), (0, vec![(3, 5)])],
        );

        check_data_parts_reader_dedup(
            vec![vec![(0, vec![1])], vec![(0, vec![2])], vec![(0, vec![3])]],
            vec![(0, vec![(1, 0)]), (0, vec![(2, 1)]), (0, vec![(3, 2)])],
        );

        check_data_parts_reader_dedup(
            vec![vec![(0, vec![1])], vec![(0, vec![1])], vec![(0, vec![1])]],
            vec![(0, vec![(1, 2)])],
        );

        check_data_parts_reader_dedup(
            vec![vec![(0, vec![1])], vec![(1, vec![1])], vec![(2, vec![1])]],
            vec![(0, vec![(1, 0)]), (1, vec![(1, 1)]), (2, vec![(1, 2)])],
        );
    }
}
