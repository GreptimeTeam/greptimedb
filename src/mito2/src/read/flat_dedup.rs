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

//! Dedup implementation for flat format.

use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use api::v1::OpType;
use async_stream::try_stream;
use common_telemetry::debug;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, BooleanBufferBuilder, UInt8Array, UInt64Array,
    make_comparator,
};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::compute::kernels::cmp::distinct;
use datatypes::arrow::compute::kernels::partition::{Partitions, partition};
use datatypes::arrow::compute::kernels::take::take;
use datatypes::arrow::compute::{
    SortOptions, TakeOptions, concat_batches, filter_record_batch, take_record_batch,
};
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::record_batch::RecordBatch;
use futures::{Stream, TryStreamExt};
use snafu::ResultExt;

use crate::error::{ComputeArrowSnafu, NewRecordBatchSnafu, Result};
use crate::memtable::partition_tree::data::timestamp_array_to_i64_slice;
use crate::metrics::MERGE_FILTER_ROWS_TOTAL;
use crate::read::dedup::{DedupMetrics, DedupMetricsReport};
use crate::sst::parquet::flat_format::{
    op_type_column_index, primary_key_column_index, time_index_column_index,
};
use crate::sst::parquet::format::{FIXED_POS_COLUMN_NUM, PrimaryKeyArray};

/// An iterator to dedup sorted batches from an iterator based on the dedup strategy.
pub struct FlatDedupIterator<I, S> {
    iter: I,
    strategy: S,
    metrics: DedupMetrics,
}

impl<I, S> FlatDedupIterator<I, S> {
    /// Creates a new dedup iterator.
    pub fn new(iter: I, strategy: S) -> Self {
        Self {
            iter,
            strategy,
            metrics: DedupMetrics::default(),
        }
    }
}

impl<I: Iterator<Item = Result<RecordBatch>>, S: RecordBatchDedupStrategy> FlatDedupIterator<I, S> {
    /// Returns the next deduplicated batch.
    fn fetch_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(batch) = self.iter.next().transpose()? {
            if let Some(batch) = self.strategy.push_batch(batch, &mut self.metrics)? {
                return Ok(Some(batch));
            }
        }

        self.strategy.finish(&mut self.metrics)
    }
}

impl<I: Iterator<Item = Result<RecordBatch>>, S: RecordBatchDedupStrategy> Iterator
    for FlatDedupIterator<I, S>
{
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fetch_next_batch().transpose()
    }
}

/// An async reader to dedup sorted record batches from a stream based on the dedup strategy.
pub struct FlatDedupReader<I, S> {
    stream: I,
    strategy: S,
    metrics: DedupMetrics,
    /// Optional metrics reporter.
    metrics_reporter: Option<Arc<dyn DedupMetricsReport>>,
}

impl<I, S> FlatDedupReader<I, S> {
    /// Creates a new dedup reader.
    pub fn new(
        stream: I,
        strategy: S,
        metrics_reporter: Option<Arc<dyn DedupMetricsReport>>,
    ) -> Self {
        Self {
            stream,
            strategy,
            metrics: DedupMetrics::default(),
            metrics_reporter,
        }
    }
}

impl<I: Stream<Item = Result<RecordBatch>> + Unpin, S: RecordBatchDedupStrategy>
    FlatDedupReader<I, S>
{
    /// Returns the next deduplicated batch.
    async fn fetch_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(batch) = self.stream.try_next().await? {
            if let Some(batch) = self.strategy.push_batch(batch, &mut self.metrics)? {
                self.metrics.maybe_report(&self.metrics_reporter);
                return Ok(Some(batch));
            }
        }

        let result = self.strategy.finish(&mut self.metrics)?;
        self.metrics.maybe_report(&self.metrics_reporter);
        Ok(result)
    }

    /// Converts the reader into a stream.
    pub fn into_stream(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        try_stream! {
            while let Some(batch) = self.fetch_next_batch().await? {
                yield batch;
            }
        }
    }
}

impl<I, S> Drop for FlatDedupReader<I, S> {
    fn drop(&mut self) {
        debug!("Flat dedup reader finished, metrics: {:?}", self.metrics);

        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["dedup"])
            .inc_by(self.metrics.num_unselected_rows as u64);
        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["delete"])
            .inc_by(self.metrics.num_deleted_rows as u64);

        // Report any remaining metrics.
        if let Some(reporter) = &self.metrics_reporter {
            reporter.report(&mut self.metrics);
        }
    }
}

/// Strategy to remove duplicate rows from sorted record batches.
pub trait RecordBatchDedupStrategy: Send {
    /// Pushes a batch to the dedup strategy.
    /// Returns a batch if the strategy ensures there is no duplications based on
    /// the input batch.
    fn push_batch(
        &mut self,
        batch: RecordBatch,
        metrics: &mut DedupMetrics,
    ) -> Result<Option<RecordBatch>>;

    /// Finishes the deduplication process and returns any remaining batch.
    ///
    /// Users must ensure that `push_batch` is called for all batches before
    /// calling this method.
    fn finish(&mut self, metrics: &mut DedupMetrics) -> Result<Option<RecordBatch>>;
}

/// Dedup strategy that keeps the row with latest sequence of each key.
pub struct FlatLastRow {
    /// Meta of the last row in the previous batch that has the same key
    /// as the batch to push.
    prev_batch: Option<BatchLastRow>,
    /// Filter deleted rows.
    filter_deleted: bool,
}

impl FlatLastRow {
    /// Creates a new strategy with the given `filter_deleted` flag.
    pub fn new(filter_deleted: bool) -> Self {
        Self {
            prev_batch: None,
            filter_deleted,
        }
    }

    /// Remove duplications from the batch without considering previous rows.
    fn dedup_one_batch(batch: RecordBatch) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows < 2 {
            return Ok(batch);
        }

        let num_columns = batch.num_columns();
        let timestamps = batch.column(time_index_column_index(num_columns));
        // Checks duplications based on the timestamp.
        let mask = find_boundaries(timestamps).context(ComputeArrowSnafu)?;
        if mask.count_set_bits() == num_rows - 1 {
            // Fast path: No duplication.
            return Ok(batch);
        }

        // The batch has duplicated timestamps, but it doesn't mean it must
        // has duplicated rows.
        // Partitions the batch by the primary key and time index.
        let columns: Vec<_> = [
            primary_key_column_index(num_columns),
            time_index_column_index(num_columns),
        ]
        .iter()
        .map(|index| batch.column(*index).clone())
        .collect();
        let partitions = partition(&columns).context(ComputeArrowSnafu)?;

        Self::dedup_by_partitions(batch, &partitions)
    }

    /// Remove duplications for each partition.
    fn dedup_by_partitions(batch: RecordBatch, partitions: &Partitions) -> Result<RecordBatch> {
        let ranges = partitions.ranges();
        // Each range at least has 1 row.
        let num_duplications: usize = ranges.iter().map(|r| r.end - r.start - 1).sum();
        if num_duplications == 0 {
            // Fast path, no duplications.
            return Ok(batch);
        }

        // Always takes the first row in each range.
        let take_indices: UInt64Array = ranges.iter().map(|r| Some(r.start as u64)).collect();
        take_record_batch(&batch, &take_indices).context(ComputeArrowSnafu)
    }
}

impl RecordBatchDedupStrategy for FlatLastRow {
    fn push_batch(
        &mut self,
        batch: RecordBatch,
        metrics: &mut DedupMetrics,
    ) -> Result<Option<RecordBatch>> {
        let start = Instant::now();

        if batch.num_rows() == 0 {
            return Ok(None);
        }

        // Dedup current batch to ensure no duplication before we checking the previous row.
        let row_before_dedup = batch.num_rows();
        let mut batch = Self::dedup_one_batch(batch)?;

        if let Some(prev_batch) = &self.prev_batch {
            // If we have previous batch.
            if prev_batch.is_last_row_duplicated(&batch) {
                // Duplicated with the last batch, skip the first row.
                batch = batch.slice(1, batch.num_rows() - 1);
            }
        }
        metrics.num_unselected_rows += row_before_dedup - batch.num_rows();

        let Some(batch_last_row) = BatchLastRow::try_new(batch.clone()) else {
            // The batch after dedup is empty.
            // We don't need to update `prev_batch` because they have the same
            // key and timestamp.
            metrics.dedup_cost += start.elapsed();
            return Ok(None);
        };

        // Store current batch to `prev_batch` so we could compare the next batch
        // with this batch. We store batch before filtering it as rows with `OpType::Delete`
        // would be removed from the batch after filter, then we may store an incorrect `last row`
        // of previous batch.
        // Safety: We checked the batch is not empty before.
        self.prev_batch = Some(batch_last_row);

        // Filters deleted rows at last.
        let result = maybe_filter_deleted(batch, self.filter_deleted, metrics);

        metrics.dedup_cost += start.elapsed();

        result
    }

    fn finish(&mut self, _metrics: &mut DedupMetrics) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}

/// Dedup strategy that keeps the last non-null field for the same key.
pub struct FlatLastNonNull {
    /// The start index of field columns:
    field_column_start: usize,
    /// Filter deleted rows.
    filter_deleted: bool,
    /// Buffered batch to check whether the next batch have duplicated rows with this batch.
    /// Fields in the last row of this batch may be updated by the next batch.
    /// The buffered batch should contain no duplication.
    buffer: Option<BatchLastRow>,
    /// Whether the last row range contains a delete operation.
    /// If so, we don't need to update null fields.
    contains_delete: bool,
}

impl RecordBatchDedupStrategy for FlatLastNonNull {
    fn push_batch(
        &mut self,
        batch: RecordBatch,
        metrics: &mut DedupMetrics,
    ) -> Result<Option<RecordBatch>> {
        let start = Instant::now();

        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let row_before_dedup = batch.num_rows();

        let Some(buffer) = self.buffer.take() else {
            // If the buffer is None, dedup the batch, put the batch into the buffer and return.
            // There is no previous batch with the same key, we can pass contains_delete as false.
            let (record_batch, contains_delete) =
                Self::dedup_one_batch(batch, self.field_column_start, false)?;
            metrics.num_unselected_rows += row_before_dedup - record_batch.num_rows();
            self.buffer = BatchLastRow::try_new(record_batch);
            self.contains_delete = contains_delete;

            metrics.dedup_cost += start.elapsed();
            return Ok(None);
        };

        if !buffer.is_last_row_duplicated(&batch) {
            // The first row of batch has different key from the buffer.
            // We can replace the buffer with the new batch.
            // Dedup the batch.
            // There is no previous batch with the same key, we can pass contains_delete as false.
            let (record_batch, contains_delete) =
                Self::dedup_one_batch(batch, self.field_column_start, false)?;
            metrics.num_unselected_rows += row_before_dedup - record_batch.num_rows();
            debug_assert!(record_batch.num_rows() > 0);
            self.buffer = BatchLastRow::try_new(record_batch);
            self.contains_delete = contains_delete;

            let result = maybe_filter_deleted(buffer.last_batch, self.filter_deleted, metrics);
            metrics.dedup_cost += start.elapsed();
            return result;
        }

        // The next batch has duplicated rows.
        // We can return rows except the last row in the buffer.
        let output = if buffer.last_batch.num_rows() > 1 {
            let dedup_batch = buffer.last_batch.slice(0, buffer.last_batch.num_rows() - 1);
            debug_assert_eq!(buffer.last_batch.num_rows() - 1, dedup_batch.num_rows());

            maybe_filter_deleted(dedup_batch, self.filter_deleted, metrics)?
        } else {
            None
        };
        let last_row = buffer.last_batch.slice(buffer.last_batch.num_rows() - 1, 1);

        // We concat the last row with the next batch.
        let schema = batch.schema();
        let merged = concat_batches(&schema, &[last_row, batch]).context(ComputeArrowSnafu)?;
        let merged_row_count = merged.num_rows();
        // Dedup the merged batch and update the buffer.
        let (record_batch, contains_delete) =
            Self::dedup_one_batch(merged, self.field_column_start, self.contains_delete)?;
        metrics.num_unselected_rows += merged_row_count - record_batch.num_rows();
        debug_assert!(record_batch.num_rows() > 0);
        self.buffer = BatchLastRow::try_new(record_batch);
        self.contains_delete = contains_delete;

        metrics.dedup_cost += start.elapsed();

        Ok(output)
    }

    fn finish(&mut self, metrics: &mut DedupMetrics) -> Result<Option<RecordBatch>> {
        let Some(buffer) = self.buffer.take() else {
            return Ok(None);
        };

        let start = Instant::now();

        let result = maybe_filter_deleted(buffer.last_batch, self.filter_deleted, metrics);

        metrics.dedup_cost += start.elapsed();

        result
    }
}

impl FlatLastNonNull {
    /// Creates a new strategy with the given `filter_deleted` flag.
    pub fn new(field_column_start: usize, filter_deleted: bool) -> Self {
        Self {
            field_column_start,
            filter_deleted,
            buffer: None,
            contains_delete: false,
        }
    }

    /// Remove duplications from the batch without considering the previous and next rows.
    /// Returns a tuple containing the deduplicated batch and a boolean indicating whether the last range contains deleted rows.
    fn dedup_one_batch(
        batch: RecordBatch,
        field_column_start: usize,
        prev_batch_contains_delete: bool,
    ) -> Result<(RecordBatch, bool)> {
        // Get op type array for checking delete operations
        let op_type_column = batch
            .column(op_type_column_index(batch.num_columns()))
            .clone();
        let op_types = op_type_column
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let num_rows = batch.num_rows();
        if num_rows < 2 {
            let contains_delete = if num_rows > 0 {
                op_types.value(0) == OpType::Delete as u8
            } else {
                false
            };
            return Ok((batch, contains_delete));
        }

        let num_columns = batch.num_columns();
        let timestamps = batch.column(time_index_column_index(num_columns));
        // Checks duplications based on the timestamp.
        let mask = find_boundaries(timestamps).context(ComputeArrowSnafu)?;
        if mask.count_set_bits() == num_rows - 1 {
            let contains_delete = op_types.value(num_rows - 1) == OpType::Delete as u8;
            // Fast path: No duplication.
            return Ok((batch, contains_delete));
        }

        // The batch has duplicated timestamps, but it doesn't mean it must
        // has duplicated rows.
        // Partitions the batch by the primary key and time index.
        let columns: Vec<_> = [
            primary_key_column_index(num_columns),
            time_index_column_index(num_columns),
        ]
        .iter()
        .map(|index| batch.column(*index).clone())
        .collect();
        let partitions = partition(&columns).context(ComputeArrowSnafu)?;

        Self::dedup_by_partitions(
            batch,
            &partitions,
            field_column_start,
            op_types,
            prev_batch_contains_delete,
        )
    }

    /// Remove depulications for each partition.
    /// Returns a tuple containing the deduplicated batch and a boolean indicating whether the last range contains deleted rows.
    fn dedup_by_partitions(
        batch: RecordBatch,
        partitions: &Partitions,
        field_column_start: usize,
        op_types: &UInt8Array,
        first_range_contains_delete: bool,
    ) -> Result<(RecordBatch, bool)> {
        let ranges = partitions.ranges();
        let contains_delete = Self::last_range_has_delete(&ranges, op_types);

        // Each range at least has 1 row.
        let num_duplications: usize = ranges.iter().map(|r| r.end - r.start - 1).sum();
        if num_duplications == 0 {
            // Fast path, no duplication.
            return Ok((batch, contains_delete));
        }

        let field_column_end = batch.num_columns() - FIXED_POS_COLUMN_NUM;
        let take_options = Some(TakeOptions {
            check_bounds: false,
        });
        // Always takes the first value for non-field columns in each range.
        let non_field_indices: UInt64Array = ranges.iter().map(|r| Some(r.start as u64)).collect();
        let new_columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(col_idx, column)| {
                if col_idx >= field_column_start && col_idx < field_column_end {
                    let field_indices = Self::compute_field_indices(
                        &ranges,
                        column,
                        op_types,
                        first_range_contains_delete,
                    );
                    take(column, &field_indices, take_options.clone()).context(ComputeArrowSnafu)
                } else {
                    take(column, &non_field_indices, take_options.clone())
                        .context(ComputeArrowSnafu)
                }
            })
            .collect::<Result<Vec<ArrayRef>>>()?;

        let record_batch =
            RecordBatch::try_new(batch.schema(), new_columns).context(NewRecordBatchSnafu)?;
        Ok((record_batch, contains_delete))
    }

    /// Returns an array of indices of the latest non null value for
    /// each input range.
    /// If all values in a range are null, the returned index is unspecific.
    /// Stops when encountering a delete operation and ignores all subsequent rows.
    fn compute_field_indices(
        ranges: &[Range<usize>],
        field_array: &ArrayRef,
        op_types: &UInt8Array,
        first_range_contains_delete: bool,
    ) -> UInt64Array {
        ranges
            .iter()
            .enumerate()
            .map(|(range_idx, r)| {
                let mut value_index = r.start as u64;
                if range_idx == 0 && first_range_contains_delete {
                    return Some(value_index);
                }

                // Iterate through the range to find the first valid non-null value
                // but stop if we encounter a delete operation.
                for i in r.clone() {
                    if op_types.value(i) == OpType::Delete as u8 {
                        break;
                    }
                    if field_array.is_valid(i) {
                        value_index = i as u64;
                        break;
                    }
                }

                Some(value_index)
            })
            .collect()
    }

    /// Checks whether the last range contains a delete operation.
    fn last_range_has_delete(ranges: &[Range<usize>], op_types: &UInt8Array) -> bool {
        if let Some(last_range) = ranges.last() {
            last_range
                .clone()
                .any(|i| op_types.value(i) == OpType::Delete as u8)
        } else {
            false
        }
    }
}

/// State of the batch with the last row for dedup.
struct BatchLastRow {
    /// The record batch that contains the last row.
    /// It must has at least one row.
    last_batch: RecordBatch,
    /// Primary keys of the last batch.
    primary_key: PrimaryKeyArray,
    /// Last timestamp value.
    timestamp: i64,
}

impl BatchLastRow {
    /// Returns a new [BatchLastRow] if the record batch is not empty.
    fn try_new(record_batch: RecordBatch) -> Option<Self> {
        if record_batch.num_rows() > 0 {
            let num_columns = record_batch.num_columns();
            let primary_key = record_batch
                .column(primary_key_column_index(num_columns))
                .as_any()
                .downcast_ref::<PrimaryKeyArray>()
                .unwrap()
                .clone();
            let timestamp_array = record_batch.column(time_index_column_index(num_columns));
            let timestamp = timestamp_value(timestamp_array, timestamp_array.len() - 1);

            Some(Self {
                last_batch: record_batch,
                primary_key,
                timestamp,
            })
        } else {
            None
        }
    }

    /// Returns true if the first row of the input `batch` is duplicated with the last row.
    fn is_last_row_duplicated(&self, batch: &RecordBatch) -> bool {
        if batch.num_rows() == 0 {
            return false;
        }

        // The first timestamp in the batch.
        let batch_timestamp = timestamp_value(
            batch.column(time_index_column_index(batch.num_columns())),
            0,
        );
        if batch_timestamp != self.timestamp {
            return false;
        }

        let last_key = primary_key_at(&self.primary_key, self.last_batch.num_rows() - 1);
        let primary_key = batch
            .column(primary_key_column_index(batch.num_columns()))
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        // Primary key of the first row in the batch.
        let batch_key = primary_key_at(primary_key, 0);

        last_key == batch_key
    }
}

// TODO(yingwen): We only compares timestamp arrays, we can modify this function
// to simplify the comparator.
// Port from https://github.com/apache/arrow-rs/blob/55.0.0/arrow-ord/src/partition.rs#L155-L168
/// Returns a mask with bits set whenever the value or nullability changes
fn find_boundaries(v: &dyn Array) -> Result<BooleanBuffer, ArrowError> {
    let slice_len = v.len() - 1;
    let v1 = v.slice(0, slice_len);
    let v2 = v.slice(1, slice_len);

    if !v.data_type().is_nested() {
        return Ok(distinct(&v1, &v2)?.values().clone());
    }
    // Given that we're only comparing values, null ordering in the input or
    // sort options do not matter.
    let cmp = make_comparator(&v1, &v2, SortOptions::default())?;
    Ok((0..slice_len).map(|i| !cmp(i, i).is_eq()).collect())
}

/// Filters deleted rows from the record batch if `filter_deleted` is true.
fn maybe_filter_deleted(
    record_batch: RecordBatch,
    filter_deleted: bool,
    metrics: &mut DedupMetrics,
) -> Result<Option<RecordBatch>> {
    if !filter_deleted {
        return Ok(Some(record_batch));
    }
    let batch = filter_deleted_from_batch(record_batch, metrics)?;
    // Skips empty batches.
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    Ok(Some(batch))
}

/// Removes deleted rows from the batch and updates metrics.
fn filter_deleted_from_batch(
    batch: RecordBatch,
    metrics: &mut DedupMetrics,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let op_type_column = batch.column(op_type_column_index(batch.num_columns()));
    // Safety: The column should be op type.
    let op_types = op_type_column
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let has_delete = op_types
        .values()
        .iter()
        .any(|op_type| *op_type != OpType::Put as u8);
    if !has_delete {
        return Ok(batch);
    }

    let mut builder = BooleanBufferBuilder::new(op_types.len());
    for op_type in op_types.values() {
        if *op_type == OpType::Delete as u8 {
            builder.append(false);
        } else {
            builder.append(true);
        }
    }
    let predicate = BooleanArray::new(builder.into(), None);
    let new_batch = filter_record_batch(&batch, &predicate).context(ComputeArrowSnafu)?;
    let num_deleted = num_rows - new_batch.num_rows();
    metrics.num_deleted_rows += num_deleted;
    metrics.num_unselected_rows += num_deleted;

    Ok(new_batch)
}

/// Gets the primary key at `index`.
fn primary_key_at(primary_key: &PrimaryKeyArray, index: usize) -> &[u8] {
    let key = primary_key.keys().value(index);
    let binary_values = primary_key
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    binary_values.value(key as usize)
}

/// Gets the timestamp value from the timestamp array.
///
/// # Panics
/// Panics if the array is not a timestamp array or
/// the index is out of bound.
pub(crate) fn timestamp_value(array: &ArrayRef, idx: usize) -> i64 {
    timestamp_array_to_i64_slice(array)[idx]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::OpType;
    use datatypes::arrow::array::{
        ArrayRef, BinaryDictionaryBuilder, Int64Array, StringDictionaryBuilder,
        TimestampMillisecondArray, UInt8Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;

    use super::*;

    /// Creates a test RecordBatch in flat format with given parameters.
    fn new_record_batch(
        primary_keys: &[&[u8]],
        timestamps: &[i64],
        sequences: &[u64],
        op_types: &[OpType],
        fields: &[u64],
    ) -> RecordBatch {
        let num_rows = timestamps.len();
        debug_assert_eq!(sequences.len(), num_rows);
        debug_assert_eq!(op_types.len(), num_rows);
        debug_assert_eq!(fields.len(), num_rows);
        debug_assert_eq!(primary_keys.len(), num_rows);

        let columns: Vec<ArrayRef> = vec![
            // k0 column (primary key as string dictionary)
            build_test_pk_string_dict_array(primary_keys),
            // field0 column
            Arc::new(Int64Array::from_iter(
                fields.iter().map(|v| Some(*v as i64)),
            )),
            // ts column (time index)
            Arc::new(TimestampMillisecondArray::from_iter_values(
                timestamps.iter().copied(),
            )),
            // __primary_key column
            build_test_pk_array(primary_keys),
            // __sequence column
            Arc::new(UInt64Array::from_iter_values(sequences.iter().copied())),
            // __op_type column
            Arc::new(UInt8Array::from_iter_values(
                op_types.iter().map(|v| *v as u8),
            )),
        ];

        RecordBatch::try_new(build_test_flat_schema(), columns).unwrap()
    }

    /// Creates a test RecordBatch in flat format with multiple fields for testing FlatLastNonNull.
    fn new_record_batch_multi_fields(
        primary_keys: &[&[u8]],
        timestamps: &[i64],
        sequences: &[u64],
        op_types: &[OpType],
        fields: &[(Option<u64>, Option<u64>)],
    ) -> RecordBatch {
        let num_rows = timestamps.len();
        debug_assert_eq!(sequences.len(), num_rows);
        debug_assert_eq!(op_types.len(), num_rows);
        debug_assert_eq!(fields.len(), num_rows);
        debug_assert_eq!(primary_keys.len(), num_rows);

        let columns: Vec<ArrayRef> = vec![
            // k0 column (primary key as string dictionary)
            build_test_pk_string_dict_array(primary_keys),
            // field0 column
            Arc::new(Int64Array::from_iter(
                fields.iter().map(|field| field.0.map(|v| v as i64)),
            )),
            // field1 column
            Arc::new(Int64Array::from_iter(
                fields.iter().map(|field| field.1.map(|v| v as i64)),
            )),
            // ts column (time index)
            Arc::new(TimestampMillisecondArray::from_iter_values(
                timestamps.iter().copied(),
            )),
            // __primary_key column
            build_test_pk_array(primary_keys),
            // __sequence column
            Arc::new(UInt64Array::from_iter_values(sequences.iter().copied())),
            // __op_type column
            Arc::new(UInt8Array::from_iter_values(
                op_types.iter().map(|v| *v as u8),
            )),
        ];

        RecordBatch::try_new(build_test_multi_field_schema(), columns).unwrap()
    }

    /// Creates a test string dictionary primary key array for given primary keys.
    fn build_test_pk_string_dict_array(primary_keys: &[&[u8]]) -> ArrayRef {
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        for &pk in primary_keys {
            let pk_str = std::str::from_utf8(pk).unwrap();
            builder.append(pk_str).unwrap();
        }
        Arc::new(builder.finish())
    }

    /// Creates a test primary key array for given primary keys.
    fn build_test_pk_array(primary_keys: &[&[u8]]) -> ArrayRef {
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for &pk in primary_keys {
            builder.append(pk).unwrap();
        }
        Arc::new(builder.finish())
    }

    /// Builds the arrow schema for test flat format.
    fn build_test_flat_schema() -> SchemaRef {
        let fields = vec![
            Field::new(
                "k0",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("field0", DataType::Int64, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ];
        Arc::new(Schema::new(fields))
    }

    /// Builds the arrow schema for test flat format with multiple fields.
    fn build_test_multi_field_schema() -> SchemaRef {
        let fields = vec![
            Field::new(
                "k0",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("field0", DataType::Int64, true),
            Field::new("field1", DataType::Int64, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ];
        Arc::new(Schema::new(fields))
    }

    /// Asserts that two RecordBatch vectors are equal.
    fn check_record_batches_equal(expected: &[RecordBatch], actual: &[RecordBatch]) {
        for (i, (exp, act)) in expected.iter().zip(actual.iter()).enumerate() {
            assert_eq!(exp, act, "RecordBatch {} differs", i);
        }
        assert_eq!(
            expected.len(),
            actual.len(),
            "Number of batches don't match"
        );
    }

    /// Helper function to collect iterator results.
    fn collect_iterator_results<I>(iter: I) -> Vec<RecordBatch>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        iter.map(|result| result.unwrap()).collect()
    }

    #[test]
    fn test_flat_last_row_no_duplications() {
        let input = vec![
            new_record_batch(
                &[b"k1", b"k1"],
                &[1, 2],
                &[11, 12],
                &[OpType::Put, OpType::Put],
                &[21, 22],
            ),
            new_record_batch(&[b"k1"], &[3], &[13], &[OpType::Put], &[23]),
            new_record_batch(
                &[b"k2", b"k2"],
                &[1, 2],
                &[111, 112],
                &[OpType::Put, OpType::Put],
                &[31, 32],
            ),
        ];

        // Test with filter_deleted = true
        let iter = input.clone().into_iter().map(Ok);
        let mut dedup_iter = FlatDedupIterator::new(iter, FlatLastRow::new(true));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&input, &result);
        assert_eq!(0, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(0, dedup_iter.metrics.num_deleted_rows);

        // Test with filter_deleted = false
        let iter = input.clone().into_iter().map(Ok);
        let mut dedup_iter = FlatDedupIterator::new(iter, FlatLastRow::new(false));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&input, &result);
        assert_eq!(0, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(0, dedup_iter.metrics.num_deleted_rows);
    }

    #[test]
    fn test_flat_last_row_duplications() {
        let input = vec![
            new_record_batch(
                &[b"k1", b"k1"],
                &[1, 2],
                &[13, 11],
                &[OpType::Put, OpType::Put],
                &[11, 12],
            ),
            // empty batch.
            new_record_batch(&[], &[], &[], &[], &[]),
            // Duplicate with the previous batch.
            new_record_batch(
                &[b"k1", b"k1", b"k1"],
                &[2, 3, 4],
                &[10, 13, 13],
                &[OpType::Put, OpType::Put, OpType::Delete],
                &[2, 13, 14],
            ),
            new_record_batch(
                &[b"k2", b"k2"],
                &[1, 2],
                &[20, 20],
                &[OpType::Put, OpType::Delete],
                &[101, 0],
            ),
            new_record_batch(&[b"k2"], &[2], &[19], &[OpType::Put], &[102]),
            new_record_batch(&[b"k3"], &[2], &[20], &[OpType::Put], &[202]),
            // This batch won't increase the deleted rows count as it
            // is filtered out by the previous batch.
            new_record_batch(&[b"k3"], &[2], &[19], &[OpType::Delete], &[0]),
        ];

        // Test with filter_deleted = true
        let expected_filter_deleted = vec![
            new_record_batch(
                &[b"k1", b"k1"],
                &[1, 2],
                &[13, 11],
                &[OpType::Put, OpType::Put],
                &[11, 12],
            ),
            new_record_batch(&[b"k1"], &[3], &[13], &[OpType::Put], &[13]),
            new_record_batch(&[b"k2"], &[1], &[20], &[OpType::Put], &[101]),
            new_record_batch(&[b"k3"], &[2], &[20], &[OpType::Put], &[202]),
        ];

        let iter = input.clone().into_iter().map(Ok);
        let mut dedup_iter = FlatDedupIterator::new(iter, FlatLastRow::new(true));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&expected_filter_deleted, &result);
        assert_eq!(5, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(2, dedup_iter.metrics.num_deleted_rows);

        // Test with filter_deleted = false
        let expected_no_filter = vec![
            new_record_batch(
                &[b"k1", b"k1"],
                &[1, 2],
                &[13, 11],
                &[OpType::Put, OpType::Put],
                &[11, 12],
            ),
            new_record_batch(
                &[b"k1", b"k1"],
                &[3, 4],
                &[13, 13],
                &[OpType::Put, OpType::Delete],
                &[13, 14],
            ),
            new_record_batch(
                &[b"k2", b"k2"],
                &[1, 2],
                &[20, 20],
                &[OpType::Put, OpType::Delete],
                &[101, 0],
            ),
            new_record_batch(&[b"k3"], &[2], &[20], &[OpType::Put], &[202]),
        ];

        let iter = input.clone().into_iter().map(Ok);
        let mut dedup_iter = FlatDedupIterator::new(iter, FlatLastRow::new(false));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&expected_no_filter, &result);
        assert_eq!(3, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(0, dedup_iter.metrics.num_deleted_rows);
    }

    #[test]
    fn test_flat_last_non_null_no_duplications() {
        let input = vec![
            new_record_batch(
                &[b"k1", b"k1"],
                &[1, 2],
                &[11, 12],
                &[OpType::Put, OpType::Put],
                &[21, 22],
            ),
            new_record_batch(&[b"k1"], &[3], &[13], &[OpType::Put], &[23]),
            new_record_batch(
                &[b"k2", b"k2"],
                &[1, 2],
                &[111, 112],
                &[OpType::Put, OpType::Put],
                &[31, 32],
            ),
        ];

        // Test with filter_deleted = true
        let iter = input.clone().into_iter().map(Ok);
        let mut dedup_iter = FlatDedupIterator::new(iter, FlatLastNonNull::new(1, true));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&input, &result);
        assert_eq!(0, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(0, dedup_iter.metrics.num_deleted_rows);

        // Test with filter_deleted = false
        let iter = input.clone().into_iter().map(Ok);
        let mut dedup_iter = FlatDedupIterator::new(iter, FlatLastNonNull::new(1, false));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&input, &result);
        assert_eq!(0, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(0, dedup_iter.metrics.num_deleted_rows);
    }

    #[test]
    fn test_flat_last_non_null_field_merging() {
        let input = vec![
            new_record_batch_multi_fields(
                &[b"k1", b"k1"],
                &[1, 2],
                &[13, 11],
                &[OpType::Put, OpType::Put],
                &[(Some(11), Some(11)), (None, None)],
            ),
            // empty batch
            new_record_batch_multi_fields(&[], &[], &[], &[], &[]),
            // Duplicate with the previous batch - should merge fields
            new_record_batch_multi_fields(
                &[b"k1"],
                &[2],
                &[10],
                &[OpType::Put],
                &[(Some(12), None)],
            ),
            new_record_batch_multi_fields(
                &[b"k1", b"k1", b"k1"],
                &[2, 3, 4],
                &[10, 13, 13],
                &[OpType::Put, OpType::Put, OpType::Delete],
                &[(Some(2), Some(22)), (Some(13), None), (None, Some(14))],
            ),
            new_record_batch_multi_fields(
                &[b"k2", b"k2"],
                &[1, 2],
                &[20, 20],
                &[OpType::Put, OpType::Delete],
                &[(Some(101), Some(101)), (None, None)],
            ),
            new_record_batch_multi_fields(
                &[b"k2"],
                &[2],
                &[19],
                &[OpType::Put],
                &[(Some(102), Some(102))],
            ),
            new_record_batch_multi_fields(
                &[b"k3"],
                &[2],
                &[20],
                &[OpType::Put],
                &[(Some(202), Some(202))],
            ),
            // This batch won't increase the deleted rows count as it
            // is filtered out by the previous batch. (All fields are null).
            new_record_batch_multi_fields(
                &[b"k3"],
                &[2],
                &[19],
                &[OpType::Delete],
                &[(None, None)],
            ),
        ];

        // Test with filter_deleted = true
        let expected_filter_deleted = vec![
            new_record_batch_multi_fields(
                &[b"k1"],
                &[1],
                &[13],
                &[OpType::Put],
                &[(Some(11), Some(11))],
            ),
            new_record_batch_multi_fields(
                &[b"k1", b"k1"],
                &[2, 3],
                &[11, 13],
                &[OpType::Put, OpType::Put],
                &[(Some(12), Some(22)), (Some(13), None)],
            ),
            new_record_batch_multi_fields(
                &[b"k2"],
                &[1],
                &[20],
                &[OpType::Put],
                &[(Some(101), Some(101))],
            ),
            new_record_batch_multi_fields(
                &[b"k3"],
                &[2],
                &[20],
                &[OpType::Put],
                &[(Some(202), Some(202))],
            ),
        ];

        let iter = input.clone().into_iter().map(Ok);
        let mut dedup_iter = FlatDedupIterator::new(iter, FlatLastNonNull::new(1, true));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&expected_filter_deleted, &result);
        assert_eq!(6, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(2, dedup_iter.metrics.num_deleted_rows);

        // Test with filter_deleted = false
        let expected_no_filter = vec![
            new_record_batch_multi_fields(
                &[b"k1"],
                &[1],
                &[13],
                &[OpType::Put],
                &[(Some(11), Some(11))],
            ),
            new_record_batch_multi_fields(
                &[b"k1", b"k1", b"k1"],
                &[2, 3, 4],
                &[11, 13, 13],
                &[OpType::Put, OpType::Put, OpType::Delete],
                &[(Some(12), Some(22)), (Some(13), None), (None, Some(14))],
            ),
            new_record_batch_multi_fields(
                &[b"k2"],
                &[1],
                &[20],
                &[OpType::Put],
                &[(Some(101), Some(101))],
            ),
            new_record_batch_multi_fields(
                &[b"k2"],
                &[2],
                &[20],
                &[OpType::Delete],
                &[(None, None)],
            ),
            new_record_batch_multi_fields(
                &[b"k3"],
                &[2],
                &[20],
                &[OpType::Put],
                &[(Some(202), Some(202))],
            ),
        ];

        let iter = input.clone().into_iter().map(Ok);
        let mut dedup_iter = FlatDedupIterator::new(iter, FlatLastNonNull::new(1, false));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&expected_no_filter, &result);
        assert_eq!(4, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(0, dedup_iter.metrics.num_deleted_rows);
    }

    #[test]
    fn test_flat_last_non_null_skip_merge_no_null() {
        let input = vec![
            new_record_batch_multi_fields(
                &[b"k1", b"k1"],
                &[1, 2],
                &[13, 11],
                &[OpType::Put, OpType::Put],
                &[(Some(11), Some(11)), (Some(12), Some(12))],
            ),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[2],
                &[10],
                &[OpType::Put],
                &[(None, Some(22))],
            ),
            new_record_batch_multi_fields(
                &[b"k1", b"k1"],
                &[2, 3],
                &[9, 13],
                &[OpType::Put, OpType::Put],
                &[(Some(32), None), (Some(13), Some(13))],
            ),
        ];

        let expected = vec![
            new_record_batch_multi_fields(
                &[b"k1"],
                &[1],
                &[13],
                &[OpType::Put],
                &[(Some(11), Some(11))],
            ),
            new_record_batch_multi_fields(
                &[b"k1", b"k1"],
                &[2, 3],
                &[11, 13],
                &[OpType::Put, OpType::Put],
                &[(Some(12), Some(12)), (Some(13), Some(13))],
            ),
        ];

        let iter = input.into_iter().map(Ok);
        let mut dedup_iter = FlatDedupIterator::new(iter, FlatLastNonNull::new(1, true));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&expected, &result);
        assert_eq!(2, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(0, dedup_iter.metrics.num_deleted_rows);
    }

    #[test]
    fn test_flat_last_non_null_merge_null() {
        let input = vec![
            new_record_batch_multi_fields(
                &[b"k1", b"k1"],
                &[1, 2],
                &[13, 11],
                &[OpType::Put, OpType::Put],
                &[(Some(11), Some(11)), (None, None)],
            ),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[2],
                &[10],
                &[OpType::Put],
                &[(None, Some(22))],
            ),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[3],
                &[13],
                &[OpType::Put],
                &[(Some(33), None)],
            ),
        ];

        let expected = vec![
            new_record_batch_multi_fields(
                &[b"k1"],
                &[1],
                &[13],
                &[OpType::Put],
                &[(Some(11), Some(11))],
            ),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[2],
                &[11],
                &[OpType::Put],
                &[(None, Some(22))],
            ),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[3],
                &[13],
                &[OpType::Put],
                &[(Some(33), None)],
            ),
        ];

        let iter = input.into_iter().map(Ok);
        let mut dedup_iter = FlatDedupIterator::new(iter, FlatLastNonNull::new(1, true));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&expected, &result);
        assert_eq!(1, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(0, dedup_iter.metrics.num_deleted_rows);
    }

    /// Helper function to check dedup strategy behavior directly.
    fn check_flat_dedup_strategy(
        input: &[RecordBatch],
        strategy: &mut dyn RecordBatchDedupStrategy,
        expect: &[RecordBatch],
    ) {
        let mut actual = Vec::new();
        let mut metrics = DedupMetrics::default();
        for batch in input {
            if let Some(out) = strategy.push_batch(batch.clone(), &mut metrics).unwrap() {
                actual.push(out);
            }
        }
        if let Some(out) = strategy.finish(&mut metrics).unwrap() {
            actual.push(out);
        }

        check_record_batches_equal(expect, &actual);
    }

    #[test]
    fn test_flat_last_non_null_strategy_delete_last() {
        let input = vec![
            new_record_batch_multi_fields(
                &[b"k1"],
                &[1],
                &[6],
                &[OpType::Put],
                &[(Some(11), None)],
            ),
            new_record_batch_multi_fields(
                &[b"k1", b"k1"],
                &[1, 2],
                &[1, 7],
                &[OpType::Put, OpType::Put],
                &[(Some(1), None), (Some(22), Some(222))],
            ),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[2],
                &[4],
                &[OpType::Put],
                &[(Some(12), None)],
            ),
            new_record_batch_multi_fields(
                &[b"k2", b"k2"],
                &[2, 3],
                &[2, 5],
                &[OpType::Put, OpType::Delete],
                &[(None, None), (Some(13), None)],
            ),
            new_record_batch_multi_fields(&[b"k2"], &[3], &[3], &[OpType::Put], &[(None, Some(3))]),
        ];

        let mut strategy = FlatLastNonNull::new(1, true);
        check_flat_dedup_strategy(
            &input,
            &mut strategy,
            &[
                new_record_batch_multi_fields(
                    &[b"k1"],
                    &[1],
                    &[6],
                    &[OpType::Put],
                    &[(Some(11), None)],
                ),
                new_record_batch_multi_fields(
                    &[b"k1"],
                    &[2],
                    &[7],
                    &[OpType::Put],
                    &[(Some(22), Some(222))],
                ),
                new_record_batch_multi_fields(
                    &[b"k2"],
                    &[2],
                    &[2],
                    &[OpType::Put],
                    &[(None, None)],
                ),
            ],
        );
    }

    #[test]
    fn test_flat_last_non_null_strategy_delete_one() {
        let input = vec![
            new_record_batch_multi_fields(&[b"k1"], &[1], &[1], &[OpType::Delete], &[(None, None)]),
            new_record_batch_multi_fields(
                &[b"k2"],
                &[1],
                &[6],
                &[OpType::Put],
                &[(Some(11), None)],
            ),
        ];

        let mut strategy = FlatLastNonNull::new(1, true);
        check_flat_dedup_strategy(
            &input,
            &mut strategy,
            &[new_record_batch_multi_fields(
                &[b"k2"],
                &[1],
                &[6],
                &[OpType::Put],
                &[(Some(11), None)],
            )],
        );
    }

    #[test]
    fn test_flat_last_non_null_strategy_delete_all() {
        let input = vec![
            new_record_batch_multi_fields(&[b"k1"], &[1], &[1], &[OpType::Delete], &[(None, None)]),
            new_record_batch_multi_fields(
                &[b"k2"],
                &[1],
                &[6],
                &[OpType::Delete],
                &[(Some(11), None)],
            ),
        ];

        let mut strategy = FlatLastNonNull::new(1, true);
        check_flat_dedup_strategy(&input, &mut strategy, &[]);
    }

    #[test]
    fn test_flat_last_non_null_strategy_same_batch() {
        let input = vec![
            new_record_batch_multi_fields(
                &[b"k1"],
                &[1],
                &[6],
                &[OpType::Put],
                &[(Some(11), None)],
            ),
            new_record_batch_multi_fields(
                &[b"k1", b"k1"],
                &[1, 2],
                &[1, 7],
                &[OpType::Put, OpType::Put],
                &[(Some(1), None), (Some(22), Some(222))],
            ),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[2],
                &[4],
                &[OpType::Put],
                &[(Some(12), None)],
            ),
            new_record_batch_multi_fields(
                &[b"k1", b"k1"],
                &[2, 3],
                &[2, 5],
                &[OpType::Put, OpType::Put],
                &[(None, None), (Some(13), None)],
            ),
            new_record_batch_multi_fields(&[b"k1"], &[3], &[3], &[OpType::Put], &[(None, Some(3))]),
        ];

        let mut strategy = FlatLastNonNull::new(1, true);
        check_flat_dedup_strategy(
            &input,
            &mut strategy,
            &[
                new_record_batch_multi_fields(
                    &[b"k1"],
                    &[1],
                    &[6],
                    &[OpType::Put],
                    &[(Some(11), None)],
                ),
                new_record_batch_multi_fields(
                    &[b"k1"],
                    &[2],
                    &[7],
                    &[OpType::Put],
                    &[(Some(22), Some(222))],
                ),
                new_record_batch_multi_fields(
                    &[b"k1"],
                    &[3],
                    &[5],
                    &[OpType::Put],
                    &[(Some(13), Some(3))],
                ),
            ],
        );
    }

    #[test]
    fn test_flat_last_non_null_strategy_delete_middle() {
        let input = vec![
            new_record_batch_multi_fields(
                &[b"k1"],
                &[1],
                &[7],
                &[OpType::Put],
                &[(Some(11), None)],
            ),
            new_record_batch_multi_fields(&[b"k1"], &[1], &[4], &[OpType::Delete], &[(None, None)]),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[1],
                &[1],
                &[OpType::Put],
                &[(Some(12), Some(1))],
            ),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[2],
                &[8],
                &[OpType::Put],
                &[(Some(21), None)],
            ),
            new_record_batch_multi_fields(&[b"k1"], &[2], &[5], &[OpType::Delete], &[(None, None)]),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[2],
                &[2],
                &[OpType::Put],
                &[(Some(22), Some(2))],
            ),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[3],
                &[9],
                &[OpType::Put],
                &[(Some(31), None)],
            ),
            new_record_batch_multi_fields(&[b"k1"], &[3], &[6], &[OpType::Delete], &[(None, None)]),
            new_record_batch_multi_fields(
                &[b"k1"],
                &[3],
                &[3],
                &[OpType::Put],
                &[(Some(32), Some(3))],
            ),
        ];

        let mut strategy = FlatLastNonNull::new(1, true);
        check_flat_dedup_strategy(
            &input,
            &mut strategy,
            &[
                new_record_batch_multi_fields(
                    &[b"k1"],
                    &[1],
                    &[7],
                    &[OpType::Put],
                    &[(Some(11), None)],
                ),
                new_record_batch_multi_fields(
                    &[b"k1"],
                    &[2],
                    &[8],
                    &[OpType::Put],
                    &[(Some(21), None)],
                ),
                new_record_batch_multi_fields(
                    &[b"k1"],
                    &[3],
                    &[9],
                    &[OpType::Put],
                    &[(Some(31), None)],
                ),
            ],
        );
    }
}
