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

//! Utilities to remove duplicate rows from a sorted batch.

use api::v1::OpType;
use async_trait::async_trait;
use common_base::BitVec;
use common_telemetry::debug;
use datatypes::scalars::ScalarVector;
use datatypes::vectors::{BooleanVector, UInt8Vector, VectorRef};

use crate::error::Result;
use crate::metrics::MERGE_FILTER_ROWS_TOTAL;
use crate::read::{Batch, BatchReader};

/// A reader that dedup sorted batches from a source based on the
/// dedup strategy.
pub(crate) struct DedupReader<R> {
    source: R,
    strategy: Box<dyn DedupStrategy>,
}

impl<R> DedupReader<R> {
    /// Creates a new dedup reader.
    pub(crate) fn new(source: R, strategy: Box<dyn DedupStrategy>) -> Self {
        Self { source, strategy }
    }
}

impl<R: BatchReader> DedupReader<R> {
    /// Returns the next deduplicated batch.
    async fn fetch_next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.source.next_batch().await? {
            if let Some(batch) = self.strategy.push_batch(batch)? {
                return Ok(Some(batch));
            }
        }

        self.strategy.finish()
    }
}

#[async_trait]
impl<R: BatchReader> BatchReader for DedupReader<R> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.fetch_next_batch().await
    }
}

impl<R> Drop for DedupReader<R> {
    fn drop(&mut self) {
        let metrics = self.strategy.metrics();

        debug!("Dedup reader finished, metrics: {:?}", metrics);

        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["dedup"])
            .inc_by(metrics.num_unselected_rows as u64);
        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["delete"])
            .inc_by(metrics.num_unselected_rows as u64);
    }
}

#[cfg(test)]
impl<R> DedupReader<R> {
    fn metrics(&self) -> &DedupMetrics {
        self.strategy.metrics()
    }
}

/// Strategy to remove duplicate rows from sorted batches.
pub(crate) trait DedupStrategy: Send {
    /// Pushes a batch to the dedup strategy.
    /// Returns the deduplicated batch.
    fn push_batch(&mut self, batch: Batch) -> Result<Option<Batch>>;

    /// Finishes the deduplication and resets the strategy.
    ///
    /// Users must ensure that `push_batch` is called for all batches before
    /// calling this method.
    fn finish(&mut self) -> Result<Option<Batch>>;

    /// Returns the metrics of the deduplication.
    fn metrics(&self) -> &DedupMetrics;
}

/// State of the batch for dedup.
struct BatchState {
    primary_key: Vec<u8>,
    timestamps: VectorRef,
}

/// Dedup strategy that keeps the row with latest sequence of each key.
pub(crate) struct LastRow {
    /// Previous batch that has the same key as the batch to push.
    prev_batch: Option<BatchState>,
    /// Reusable bitmap for selection. `true` means the row is selected.
    selected: BitVec,
    /// Filter deleted rows.
    filter_deleted: bool,
    metrics: DedupMetrics,
}

impl LastRow {
    /// Creates a new strategy with the given `filter_deleted` flag.
    pub(crate) fn new(filter_deleted: bool) -> Self {
        Self {
            prev_batch: None,
            selected: BitVec::new(),
            filter_deleted,
            metrics: DedupMetrics::default(),
        }
    }
}

impl DedupStrategy for LastRow {
    fn push_batch(&mut self, mut batch: Batch) -> Result<Option<Batch>> {
        if batch.is_empty() {
            return Ok(None);
        }

        // Reinitializes the bit map to zeros.
        self.selected.clear();
        self.selected.resize(batch.num_rows(), false);

        let prev_timestamps = match &self.prev_batch {
            Some(prev_batch) => {
                if prev_batch.primary_key != batch.primary_key() {
                    // The key has changed. This is the first batch of the
                    // new key.
                    None
                } else {
                    Some(prev_batch.timestamps.as_ref())
                }
            }
            None => None,
        };
        // Finds unique timestamps. The last row of a key has the largest sequence and we
        // sort rows by (timestamp, sequence desc).
        batch
            .timestamps()
            .find_unique(&mut self.selected, prev_timestamps);
        if self.filter_deleted {
            let num_deleted = unselect_deleted(batch.op_types(), &mut self.selected);
            self.metrics.num_deleted_rows += num_deleted;
        }

        // Store current batch to `prev_batch` so we could compare the next batch
        // with this batch. We store batch before filtering it as rows with `OpType::Delete`
        // would be removed from the batch after filter, then we may store an incorrect `last row`
        // of previous batch.
        match &mut self.prev_batch {
            Some(prev) => {
                // Reuse the primary key buffer.
                prev.primary_key.clone_from(&batch.primary_key);
                prev.timestamps = batch.timestamps.clone();
            }
            None => {
                self.prev_batch = Some(BatchState {
                    primary_key: batch.primary_key().to_vec(),
                    timestamps: batch.timestamps.clone(),
                })
            }
        }

        // Filters the batch if not all rows are needed.
        let num_unselected = self.selected.count_zeros();
        if num_unselected > 0 {
            let predicate = BooleanVector::from_iterator(self.selected.iter().by_vals());
            batch.filter(&predicate)?;
            self.metrics.num_unselected_rows += num_unselected;
        }

        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }

    fn finish(&mut self) -> Result<Option<Batch>> {
        Ok(None)
    }

    fn metrics(&self) -> &DedupMetrics {
        &self.metrics
    }
}

/// Metrics for deduplication.
#[derive(Debug, Default)]
pub(crate) struct DedupMetrics {
    /// Number of rows removed during deduplication.
    pub(crate) num_unselected_rows: usize,
    /// Number of deleted rows.
    pub(crate) num_deleted_rows: usize,
}

/// Finds deleted rows and unselects them.
fn unselect_deleted(op_types: &UInt8Vector, selected: &mut BitVec) -> usize {
    let mut num_deleted = 0;
    for (i, op_type) in op_types.as_arrow().values().iter().enumerate() {
        if *op_type == OpType::Delete as u8 {
            selected.set(i, false);
            num_deleted += 1;
        }
    }
    num_deleted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{check_reader_result, new_batch, VecBatchReader};

    #[tokio::test]
    async fn test_dedup_reader_no_duplications() {
        let input = [
            new_batch(
                b"k1",
                &[1, 2],
                &[11, 12],
                &[OpType::Put, OpType::Put],
                &[21, 22],
            ),
            new_batch(b"k1", &[3], &[13], &[OpType::Put], &[23]),
            new_batch(
                b"k2",
                &[1, 2],
                &[111, 112],
                &[OpType::Put, OpType::Put],
                &[31, 32],
            ),
        ];
        let reader = VecBatchReader::new(&input);
        let mut reader = DedupReader::new(reader, Box::new(LastRow::new(true)));
        check_reader_result(&mut reader, &input).await;
        assert_eq!(0, reader.metrics().num_unselected_rows);
        assert_eq!(0, reader.metrics().num_deleted_rows);
    }

    #[tokio::test]
    async fn test_dedup_reader_duplications() {
        let input = [
            new_batch(
                b"k1",
                &[1, 1, 2],
                &[13, 12, 11],
                &[OpType::Put, OpType::Put, OpType::Put],
                &[11, 1, 12],
            ),
            // Duplicate with the previous batch.
            new_batch(
                b"k1",
                &[2, 3, 3, 4],
                &[10, 13, 12, 13],
                &[OpType::Put, OpType::Put, OpType::Put, OpType::Delete],
                &[2, 13, 3, 14],
            ),
            new_batch(
                b"k2",
                &[1, 2, 2],
                &[20, 20, 19],
                &[OpType::Put, OpType::Delete, OpType::Put],
                &[101, 0, 102],
            ),
            new_batch(
                b"k3",
                &[2, 2],
                &[20, 19],
                &[OpType::Put, OpType::Delete],
                &[202, 0],
            ),
        ];
        let reader = VecBatchReader::new(&input);
        // Filter deleted.
        let mut reader = DedupReader::new(reader, Box::new(LastRow::new(true)));
        check_reader_result(
            &mut reader,
            &[
                new_batch(
                    b"k1",
                    &[1, 2],
                    &[13, 11],
                    &[OpType::Put, OpType::Put],
                    &[11, 12],
                ),
                new_batch(b"k1", &[3], &[13], &[OpType::Put], &[13]),
                new_batch(b"k2", &[1], &[20], &[OpType::Put], &[101]),
                new_batch(b"k3", &[2], &[20], &[OpType::Put], &[202]),
            ],
        )
        .await;
        assert_eq!(7, reader.metrics().num_unselected_rows);
        assert_eq!(3, reader.metrics().num_deleted_rows);

        // Does not filter deleted.
        let reader = VecBatchReader::new(&input);
        let mut reader = DedupReader::new(reader, Box::new(LastRow::new(false)));
        check_reader_result(
            &mut reader,
            &[
                new_batch(
                    b"k1",
                    &[1, 2],
                    &[13, 11],
                    &[OpType::Put, OpType::Put],
                    &[11, 12],
                ),
                new_batch(
                    b"k1",
                    &[3, 4],
                    &[13, 13],
                    &[OpType::Put, OpType::Delete],
                    &[13, 14],
                ),
                new_batch(
                    b"k2",
                    &[1, 2],
                    &[20, 20],
                    &[OpType::Put, OpType::Delete],
                    &[101, 0],
                ),
                new_batch(b"k3", &[2], &[20], &[OpType::Put], &[202]),
            ],
        )
        .await;
        assert_eq!(5, reader.metrics().num_unselected_rows);
        assert_eq!(0, reader.metrics().num_deleted_rows);
    }
}
