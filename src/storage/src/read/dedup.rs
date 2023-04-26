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
use datatypes::prelude::ScalarVector;
use datatypes::vectors::BooleanVector;
use store_api::storage::SstStatistics;

use crate::error::Result;
use crate::read::{Batch, BatchOp, BatchReader};
use crate::schema::ProjectedSchemaRef;

/// A reader that dedup rows from inner reader.
pub struct DedupReader<R> {
    /// Projected schema to read.
    schema: ProjectedSchemaRef,
    /// The inner reader.
    reader: R,
    /// Previous batch from the reader.
    prev_batch: Option<Batch>,
    /// Reused bitmap buffer.
    selected: BitVec,
}

impl<R> DedupReader<R> {
    pub fn new(schema: ProjectedSchemaRef, reader: R) -> DedupReader<R> {
        DedupReader {
            schema,
            reader,
            prev_batch: None,
            selected: BitVec::default(),
        }
    }

    /// Take `batch` and then returns a new batch with no duplicated rows.
    ///
    /// This method may returns empty `Batch`.
    fn dedup_batch(&mut self, batch: Batch) -> Result<Batch> {
        if batch.is_empty() {
            // No need to update `prev_batch` if current batch is empty.
            return Ok(batch);
        }

        // Reinitialize the bit map to zeros.
        self.selected.clear();
        self.selected.resize(batch.num_rows(), false);
        self.schema
            .find_unique(&batch, &mut self.selected, self.prev_batch.as_ref());

        // Store current batch to `prev_batch` so we could compare the next batch
        // with this batch. We store batch before filtering it mainly for correctness, as
        // once we supports `DELETE`, rows with `OpType::Delete` would be removed from the
        // batch after filter, then we may store an incorrect `last row` of previous batch.
        self.prev_batch
            .get_or_insert_with(Batch::default)
            .clone_from(&batch); // Use `clone_from` to reuse allocated memory if possible.

        // Find all rows whose op_types are `OpType::Delete`, mark their `selected` to false.
        self.schema.unselect_deleted(&batch, &mut self.selected);

        let filter = BooleanVector::from_iterator(self.selected.iter().by_vals());
        // Filter duplicate rows.
        self.schema.filter(&batch, &filter)
    }
}

#[async_trait]
impl<R: BatchReader> BatchReader for DedupReader<R> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.reader.next_batch().await? {
            let filtered = self.dedup_batch(batch)?;
            // Skip empty batch.
            if !filtered.is_empty() {
                return Ok(Some(filtered));
            }
        }

        Ok(None)
    }

    fn statistics(&self) -> SstStatistics {
        self.reader.statistics()
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::OpType;

    use super::*;
    use crate::test_util::read_util;

    #[tokio::test]
    async fn test_dedup_reader_empty() {
        let schema = read_util::new_projected_schema();
        let reader = read_util::build_vec_reader(&[]);
        let mut reader = DedupReader::new(schema, reader);

        assert!(reader.next_batch().await.unwrap().is_none());
        // Call next_batch() again is allowed.
        assert!(reader.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_dedup_by_sequence() {
        let schema = read_util::new_projected_schema();
        let reader = read_util::build_full_vec_reader(&[
            // key, value, sequence, op_type
            &[
                (100, 1, 1000, OpType::Put),
                (100, 2, 999, OpType::Put),
                (100, 3, 998, OpType::Put),
                (101, 1, 1000, OpType::Put),
            ],
            &[
                (101, 2, 999, OpType::Put),
                (102, 12, 1000, OpType::Put),
                (103, 13, 1000, OpType::Put),
            ],
            &[(103, 2, 999, OpType::Put)],
        ]);
        let mut reader = DedupReader::new(schema, reader);

        let result = read_util::collect_kv_batch(&mut reader).await;
        let expect = [
            (100, Some(1)),
            (101, Some(1)),
            (102, Some(12)),
            (103, Some(13)),
        ];
        assert_eq!(&expect, &result[..]);
    }

    #[tokio::test]
    async fn test_dedup_contains_empty_input() {
        let schema = read_util::new_projected_schema();
        let reader = read_util::build_full_vec_reader(&[
            // key, value, sequence, op_type
            &[
                (100, 1, 1000, OpType::Put),
                (100, 2, 999, OpType::Put),
                (101, 1, 1000, OpType::Put),
            ],
            &[],
            &[(101, 2, 999, OpType::Put), (102, 12, 1000, OpType::Put)],
        ]);
        let mut reader = DedupReader::new(schema, reader);

        let result = read_util::collect_kv_batch(&mut reader).await;
        let expect = [(100, Some(1)), (101, Some(1)), (102, Some(12))];
        assert_eq!(&expect, &result[..]);
    }

    #[tokio::test]
    async fn test_dedup_contains_empty_output() {
        let schema = read_util::new_projected_schema();
        let reader = read_util::build_full_vec_reader(&[
            // key, value, sequence, op_type
            &[
                (100, 1, 1000, OpType::Put),
                (100, 2, 999, OpType::Put),
                (101, 1, 1000, OpType::Put),
            ],
            &[(101, 2, 999, OpType::Put)],
            &[(101, 3, 998, OpType::Put), (101, 4, 997, OpType::Put)],
            &[(102, 12, 998, OpType::Put)],
        ]);
        let mut reader = DedupReader::new(schema, reader);

        let result = read_util::collect_kv_batch(&mut reader).await;
        let expect = [(100, Some(1)), (101, Some(1)), (102, Some(12))];
        assert_eq!(&expect, &result[..]);
    }
}
