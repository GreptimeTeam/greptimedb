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

use async_trait::async_trait;
use common_telemetry::debug;
use common_time::Timestamp;
use datatypes::data_type::DataType;
use datatypes::value::Value;
use datatypes::vectors::MutableVector;

use crate::error::Result;
use crate::metrics::MERGE_FILTER_ROWS_TOTAL;
use crate::read::{Batch, BatchColumn, BatchReader};

/// A reader that dedup sorted batches from a source based on the
/// dedup strategy.
pub(crate) struct DedupReader<R, S> {
    source: R,
    strategy: S,
    metrics: DedupMetrics,
}

impl<R, S> DedupReader<R, S> {
    /// Creates a new dedup reader.
    pub(crate) fn new(source: R, strategy: S) -> Self {
        Self {
            source,
            strategy,
            metrics: DedupMetrics::default(),
        }
    }
}

impl<R: BatchReader, S: DedupStrategy> DedupReader<R, S> {
    /// Returns the next deduplicated batch.
    async fn fetch_next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.source.next_batch().await? {
            if let Some(batch) = self.strategy.push_batch(batch, &mut self.metrics)? {
                return Ok(Some(batch));
            }
        }

        self.strategy.finish(&mut self.metrics)
    }
}

#[async_trait]
impl<R: BatchReader, S: DedupStrategy> BatchReader for DedupReader<R, S> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.fetch_next_batch().await
    }
}

impl<R, S> Drop for DedupReader<R, S> {
    fn drop(&mut self) {
        debug!("Dedup reader finished, metrics: {:?}", self.metrics);

        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["dedup"])
            .inc_by(self.metrics.num_unselected_rows as u64);
        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["delete"])
            .inc_by(self.metrics.num_unselected_rows as u64);
    }
}

#[cfg(test)]
impl<R, S> DedupReader<R, S> {
    fn metrics(&self) -> &DedupMetrics {
        &self.metrics
    }
}

/// Strategy to remove duplicate rows from sorted batches.
pub(crate) trait DedupStrategy: Send {
    /// Pushes a batch to the dedup strategy.
    /// Returns the deduplicated batch.
    fn push_batch(&mut self, batch: Batch, metrics: &mut DedupMetrics) -> Result<Option<Batch>>;

    /// Finishes the deduplication and resets the strategy.
    ///
    /// Users must ensure that `push_batch` is called for all batches before
    /// calling this method.
    fn finish(&mut self, metrics: &mut DedupMetrics) -> Result<Option<Batch>>;
}

/// State of the last row in a batch for dedup.
struct BatchLastRow {
    primary_key: Vec<u8>,
    /// The last timestamp of the batch.
    timestamp: Timestamp,
}

/// Dedup strategy that keeps the row with latest sequence of each key.
///
/// This strategy is optimized specially based on the properties of the SST files,
/// memtables and the merge reader. It assumes that batches from files and memtables
/// don't contain duplicate rows and the merge reader never concatenates batches from
/// different source.
///
/// We might implement a new strategy if we need to process files with duplicate rows.
pub(crate) struct LastRow {
    /// Meta of the last row in the previous batch that has the same key
    /// as the batch to push.
    prev_batch: Option<BatchLastRow>,
    /// Filter deleted rows.
    filter_deleted: bool,
}

impl LastRow {
    /// Creates a new strategy with the given `filter_deleted` flag.
    pub(crate) fn new(filter_deleted: bool) -> Self {
        Self {
            prev_batch: None,
            filter_deleted,
        }
    }
}

impl DedupStrategy for LastRow {
    fn push_batch(
        &mut self,
        mut batch: Batch,
        metrics: &mut DedupMetrics,
    ) -> Result<Option<Batch>> {
        if batch.is_empty() {
            return Ok(None);
        }
        debug_assert!(batch.first_timestamp().is_some());
        let prev_timestamp = match &self.prev_batch {
            Some(prev_batch) => {
                if prev_batch.primary_key != batch.primary_key() {
                    // The key has changed. This is the first batch of the
                    // new key.
                    None
                } else {
                    Some(prev_batch.timestamp)
                }
            }
            None => None,
        };
        if batch.first_timestamp() == prev_timestamp {
            metrics.num_unselected_rows += 1;
            // This batch contains a duplicate row, skip it.
            if batch.num_rows() == 1 {
                // We don't need to update `prev_batch` because they have the same
                // key and timestamp.
                return Ok(None);
            }
            // Skips the first row.
            batch = batch.slice(1, batch.num_rows() - 1);
        }

        // Store current batch to `prev_batch` so we could compare the next batch
        // with this batch. We store batch before filtering it as rows with `OpType::Delete`
        // would be removed from the batch after filter, then we may store an incorrect `last row`
        // of previous batch.
        match &mut self.prev_batch {
            Some(prev) => {
                // Reuse the primary key buffer.
                prev.primary_key.clone_from(&batch.primary_key);
                prev.timestamp = batch.last_timestamp().unwrap();
            }
            None => {
                self.prev_batch = Some(BatchLastRow {
                    primary_key: batch.primary_key().to_vec(),
                    timestamp: batch.last_timestamp().unwrap(),
                })
            }
        }

        // Filters deleted rows.
        if self.filter_deleted {
            let num_rows = batch.num_rows();
            batch.filter_deleted()?;
            let num_rows_after_filter = batch.num_rows();
            let num_deleted = num_rows - num_rows_after_filter;
            metrics.num_deleted_rows += num_deleted;
            metrics.num_unselected_rows += num_deleted;
        }

        // The batch can become empty if all rows are deleted.
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }

    fn finish(&mut self, _metrics: &mut DedupMetrics) -> Result<Option<Batch>> {
        Ok(None)
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

/// Buffer to store fields in the last row to merge.
struct LastFieldsBuilder {
    /// Filter deleted rows.
    filter_deleted: bool,
    /// Fields builders, lazy initialized.
    builders: Vec<Box<dyn MutableVector>>,
    /// Last fields to merge, lazy initialized.
    last_fields: Vec<Value>,
    /// Whether the last row has null.
    has_null: bool,
    /// Whether the builder is initialized.
    initialized: bool,
}

impl LastFieldsBuilder {
    /// Returns a new builder with the given `filter_deleted` flag.
    fn new(filter_deleted: bool) -> Self {
        Self {
            filter_deleted,
            builders: Vec::new(),
            last_fields: Vec::new(),
            has_null: false,
            initialized: false,
        }
    }

    /// Initializes the builders with the last row of the batch.
    fn maybe_init(&mut self, batch: &Batch) {
        debug_assert!(!batch.is_empty());

        if self.initialized || batch.fields().is_empty() {
            // Already initialized or no fields to merge.
            return;
        }

        self.initialized = true;

        let last_idx = batch.num_rows() - 1;
        let fields = batch.fields();
        self.has_null = fields.iter().any(|col| col.data.is_null(last_idx));
        if !self.has_null {
            return;
        }
        if self.builders.is_empty() {
            self.builders = fields
                .iter()
                .map(|col| col.data.data_type().create_mutable_vector(1))
                .collect();
        }
        self.last_fields = fields.iter().map(|col| col.data.get(last_idx)).collect();
    }

    /// Merges last fields, builds a new batch and resets the builder.
    /// It may overwrites the last row of the `buffer`.
    fn merge_last_fields(
        &mut self,
        buffer: Batch,
        metrics: &mut DedupMetrics,
    ) -> Result<Option<Batch>> {
        // Initializes the builder if needed.
        self.maybe_init(&buffer);

        if !self.has_null {
            debug_assert!(self.last_fields.is_empty());
            // No need to overwrite the last row.
            return Ok(Some(buffer));
        }

        // Builds last fields.
        for (builder, value) in self.builders.iter_mut().zip(&self.last_fields) {
            // Safety: Vectors of the batch has the same type.
            builder.push_value_ref(value.as_value_ref());
        }
        let fields = self
            .builders
            .iter_mut()
            .zip(buffer.fields())
            .map(|(builder, col)| BatchColumn {
                column_id: col.column_id,
                data: builder.to_vector(),
            })
            .collect();

        // Resets itself. `self.builders` already reset in `to_vector()`.
        self.clear();

        let mut merged = if buffer.num_rows() == 1 {
            // Replaces the buffer directly if it only has one row.
            buffer.with_fields(fields)?
        } else {
            // Replaces the last row of the buffer.
            let front = buffer.slice(0, buffer.num_rows() - 1);
            let last = buffer.slice(buffer.num_rows() - 1, 1);
            let last = last.with_fields(fields)?;
            Batch::concat(vec![front, last])?
        };

        if self.filter_deleted {
            let num_rows = merged.num_rows();
            merged.filter_deleted()?;
            let num_rows_after_filter = merged.num_rows();
            let num_deleted = num_rows - num_rows_after_filter;
            metrics.num_deleted_rows += num_deleted;
        }

        if merged.is_empty() {
            Ok(None)
        } else {
            Ok(Some(merged))
        }
    }

    /// Pushes first row of a batch to the builder.
    fn push_first_row(&mut self, batch: &Batch) {
        debug_assert!(self.initialized);
        debug_assert!(!batch.is_empty());

        if !self.has_null {
            // Skips this batch.
            return;
        }

        let fields = batch.fields();
        for (idx, value) in self.last_fields.iter_mut().enumerate() {
            if value.is_null() && !fields[idx].data.is_null(0) {
                // Updates the value.
                *value = fields[idx].data.get(0);
            }
        }
        // Updates the flag.
        self.has_null = self.last_fields.iter().any(Value::is_null);
    }

    /// Clears the builder.
    fn clear(&mut self) {
        self.last_fields.clear();
        self.has_null = false;
        self.initialized = false;
    }
}

/// Dedup strategy that keeps the last not null field for the same key.
///
/// It assumes that batches from files and memtables don't contain duplicate rows
/// and the merge reader never concatenates batches from different source.
///
/// We might implement a new strategy if we need to process files with duplicate rows.
pub(crate) struct LastNotNull {
    /// Buffered batch that fields in the last row may be updated.
    buffer: Option<Batch>,
    /// Fields that overlaps with the last row of the `buffer`.
    last_fields: LastFieldsBuilder,
}

impl LastNotNull {
    /// Creates a new strategy with the given `filter_deleted` flag.
    pub(crate) fn new(filter_deleted: bool) -> Self {
        Self {
            buffer: None,
            last_fields: LastFieldsBuilder::new(filter_deleted),
        }
    }
}

impl DedupStrategy for LastNotNull {
    fn push_batch(&mut self, batch: Batch, metrics: &mut DedupMetrics) -> Result<Option<Batch>> {
        if batch.is_empty() {
            return Ok(None);
        }

        let Some(buffer) = self.buffer.as_mut() else {
            // The buffer is empty, store the batch and return. We need to observe the next batch.
            self.buffer = Some(batch);
            return Ok(None);
        };

        if buffer.primary_key() != batch.primary_key() {
            // Next key is different.
            let buffer = std::mem::replace(buffer, batch);
            let merged = self.last_fields.merge_last_fields(buffer, metrics)?;
            return Ok(merged);
        }

        if buffer.last_timestamp() != batch.first_timestamp() {
            // The next batch has a different timestamp.
            let buffer = std::mem::replace(buffer, batch);
            let merged = self.last_fields.merge_last_fields(buffer, metrics)?;
            return Ok(merged);
        }

        // The next batch has the same key and timestamp.
        metrics.num_unselected_rows += 1;
        // We assumes each batch doesn't contain duplicate rows so we only need to check the first row.
        if batch.num_rows() == 1 {
            self.last_fields.push_first_row(&batch);
            return Ok(None);
        }

        // The next batch has the same key and timestamp but contains multiple rows.
        // We can merge the first row and buffer the remaining rows.
        let first = batch.slice(0, 1);
        self.last_fields.push_first_row(&first);
        // Moves the remaining rows to the buffer.
        let batch = batch.slice(1, batch.num_rows() - 1);
        let buffer = std::mem::replace(buffer, batch);
        let merged = self.last_fields.merge_last_fields(buffer, metrics)?;

        Ok(merged)
    }

    fn finish(&mut self, metrics: &mut DedupMetrics) -> Result<Option<Batch>> {
        let Some(buffer) = self.buffer.take() else {
            return Ok(None);
        };

        let merged = self.last_fields.merge_last_fields(buffer, metrics)?;

        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::OpType;

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

        // Test last row.
        let reader = VecBatchReader::new(&input);
        let mut reader = DedupReader::new(reader, LastRow::new(true));
        check_reader_result(&mut reader, &input).await;
        assert_eq!(0, reader.metrics().num_unselected_rows);
        assert_eq!(0, reader.metrics().num_deleted_rows);

        // Test last not null.
        let reader = VecBatchReader::new(&input);
        let mut reader = DedupReader::new(reader, LastNotNull::new(true));
        check_reader_result(&mut reader, &input).await;
        assert_eq!(0, reader.metrics().num_unselected_rows);
        assert_eq!(0, reader.metrics().num_deleted_rows);
    }

    #[tokio::test]
    async fn test_dedup_reader_duplications() {
        let input = [
            new_batch(
                b"k1",
                &[1, 2],
                &[13, 11],
                &[OpType::Put, OpType::Put],
                &[11, 12],
            ),
            // empty batch.
            new_batch(b"k1", &[], &[], &[], &[]),
            // Duplicate with the previous batch.
            new_batch(
                b"k1",
                &[2, 3, 4],
                &[10, 13, 13],
                &[OpType::Put, OpType::Put, OpType::Delete],
                &[2, 13, 14],
            ),
            new_batch(
                b"k2",
                &[1, 2],
                &[20, 20],
                &[OpType::Put, OpType::Delete],
                &[101, 0],
            ),
            new_batch(b"k2", &[2], &[19], &[OpType::Put], &[102]),
            new_batch(b"k3", &[2], &[20], &[OpType::Put], &[202]),
            // This batch won't increase the deleted rows count as it
            // is filtered out by the previous batch.
            new_batch(b"k3", &[2], &[19], &[OpType::Delete], &[0]),
        ];
        let reader = VecBatchReader::new(&input);
        // Filter deleted.
        let mut reader = DedupReader::new(reader, LastRow::new(true));
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
        assert_eq!(5, reader.metrics().num_unselected_rows);
        assert_eq!(2, reader.metrics().num_deleted_rows);

        // Does not filter deleted.
        let reader = VecBatchReader::new(&input);
        let mut reader = DedupReader::new(reader, LastRow::new(false));
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
        assert_eq!(3, reader.metrics().num_unselected_rows);
        assert_eq!(0, reader.metrics().num_deleted_rows);
    }
}
