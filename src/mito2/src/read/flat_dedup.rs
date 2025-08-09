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

use api::v1::OpType;
use datatypes::arrow::array::{
    make_comparator, Array, ArrayRef, BinaryArray, BooleanArray, BooleanBufferBuilder, UInt64Array,
    UInt8Array,
};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::compute::kernels::cmp::distinct;
use datatypes::arrow::compute::kernels::partition::{partition, Partitions};
use datatypes::arrow::compute::{filter_record_batch, take_record_batch, SortOptions};
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::record_batch::RecordBatch;
use snafu::ResultExt;

use crate::error::{ComputeArrowSnafu, Result};
use crate::memtable::partition_tree::data::timestamp_array_to_i64_slice;
use crate::read::dedup::DedupMetrics;
use crate::sst::parquet::flat_format::{
    op_type_column_index, primary_key_column_index, time_index_column_index,
};
use crate::sst::parquet::format::PrimaryKeyArray;

/// An iterator to dedup sorted batches from an iterator based on the dedup strategy.
pub struct DedupIterator<I, S> {
    iter: I,
    strategy: S,
    metrics: DedupMetrics,
}

impl<I, S> DedupIterator<I, S> {
    /// Creates a new dedup iterator.
    pub fn new(iter: I, strategy: S) -> Self {
        Self {
            iter,
            strategy,
            metrics: DedupMetrics::default(),
        }
    }
}

impl<I: Iterator<Item = Result<RecordBatch>>, S: RecordBatchDedupStrategy> DedupIterator<I, S> {
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
    for DedupIterator<I, S>
{
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fetch_next_batch().transpose()
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

    /// Remove depulications for each partition.
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
            return Ok(None);
        };

        // Store current batch to `prev_batch` so we could compare the next batch
        // with this batch. We store batch before filtering it as rows with `OpType::Delete`
        // would be removed from the batch after filter, then we may store an incorrect `last row`
        // of previous batch.
        // Safety: We checked the batch is not empty before.
        self.prev_batch = Some(batch_last_row);

        // Filters deleted rows at last.
        maybe_filter_deleted(batch, self.filter_deleted, metrics)
    }

    fn finish(&mut self, _metrics: &mut DedupMetrics) -> Result<Option<RecordBatch>> {
        Ok(None)
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
        ArrayRef, BinaryDictionaryBuilder, Int64Array, TimestampMillisecondArray, UInt64Array,
        UInt8Array,
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

    /// Asserts that two RecordBatch vectors are equal.
    fn check_record_batches_equal(expected: &[RecordBatch], actual: &[RecordBatch]) {
        assert_eq!(
            expected.len(),
            actual.len(),
            "Number of batches don't match"
        );
        for (i, (exp, act)) in expected.iter().zip(actual.iter()).enumerate() {
            assert_eq!(
                exp, act,
                "RecordBatch {} differs:\nExpected: {:?}\nActual: {:?}",
                i, exp, act
            );
        }
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
        let mut dedup_iter = DedupIterator::new(iter, FlatLastRow::new(true));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&input, &result);
        assert_eq!(0, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(0, dedup_iter.metrics.num_deleted_rows);

        // Test with filter_deleted = false
        let iter = input.clone().into_iter().map(Ok);
        let mut dedup_iter = DedupIterator::new(iter, FlatLastRow::new(false));
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
        let mut dedup_iter = DedupIterator::new(iter, FlatLastRow::new(true));
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
        let mut dedup_iter = DedupIterator::new(iter, FlatLastRow::new(false));
        let result = collect_iterator_results(&mut dedup_iter);
        check_record_batches_equal(&expected_no_filter, &result);
        assert_eq!(3, dedup_iter.metrics.num_unselected_rows);
        assert_eq!(0, dedup_iter.metrics.num_deleted_rows);
    }
}
