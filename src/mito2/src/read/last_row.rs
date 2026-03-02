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

//! Utilities to read the last row of each time series.

use std::sync::Arc;

use async_trait::async_trait;
use datatypes::arrow::array::BinaryArray;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::UInt32Vector;
use store_api::storage::{FileId, TimeSeriesRowSelector};

use crate::cache::{
    CacheStrategy, SelectorResult, SelectorResultKey, SelectorResultValue,
    selector_result_cache_hit, selector_result_cache_miss,
};
use crate::error::Result;
use crate::read::{Batch, BatchReader, BoxedBatchReader};
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::format::PrimaryKeyArray;
use crate::sst::parquet::reader::{FlatRowGroupReader, ReaderMetrics, RowGroupReader};

/// Reader to keep the last row for each time series.
/// It assumes that batches from the input reader are
/// - sorted
/// - all deleted rows has been filtered.
/// - not empty
///
/// This reader is different from the [MergeMode](crate::region::options::MergeMode) as
/// it focus on time series (the same key).
pub(crate) struct LastRowReader {
    /// Inner reader.
    reader: BoxedBatchReader,
    /// The last batch pending to return.
    selector: LastRowSelector,
}

impl LastRowReader {
    /// Creates a new `LastRowReader`.
    pub(crate) fn new(reader: BoxedBatchReader) -> Self {
        Self {
            reader,
            selector: LastRowSelector::default(),
        }
    }

    /// Returns the last row of the next key.
    pub(crate) async fn next_last_row(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.reader.next_batch().await? {
            if let Some(yielded) = self.selector.on_next(batch) {
                return Ok(Some(yielded));
            }
        }
        Ok(self.selector.finish())
    }
}

#[async_trait]
impl BatchReader for LastRowReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.next_last_row().await
    }
}

/// Cached last row reader for specific row group.
/// If the last rows for current row group are already cached, this reader returns the cached value.
/// If cache misses, [RowGroupLastRowReader] reads last rows from row group and updates the cache
/// upon finish.
pub(crate) enum RowGroupLastRowCachedReader {
    /// Cache hit, reads last rows from cached value.
    Hit(LastRowCacheReader),
    /// Cache miss, reads from row group reader and update cache.
    Miss(RowGroupLastRowReader),
}

impl RowGroupLastRowCachedReader {
    pub(crate) fn new(
        file_id: FileId,
        row_group_idx: usize,
        cache_strategy: CacheStrategy,
        row_group_reader: RowGroupReader,
    ) -> Self {
        let key = SelectorResultKey {
            file_id,
            row_group_idx,
            selector: TimeSeriesRowSelector::LastRow,
        };

        if let Some(value) = cache_strategy.get_selector_result(&key) {
            let is_primary_key = matches!(&value.result, SelectorResult::PrimaryKey(_));
            let schema_matches =
                value.projection == row_group_reader.read_format().projection_indices();
            if is_primary_key && schema_matches {
                // Format and schema match, use cache batches.
                Self::new_hit(value)
            } else {
                Self::new_miss(key, row_group_reader, cache_strategy)
            }
        } else {
            Self::new_miss(key, row_group_reader, cache_strategy)
        }
    }

    /// Gets the underlying reader metrics if uncached.
    pub(crate) fn metrics(&self) -> Option<&ReaderMetrics> {
        match self {
            RowGroupLastRowCachedReader::Hit(_) => None,
            RowGroupLastRowCachedReader::Miss(reader) => Some(reader.metrics()),
        }
    }

    /// Creates new Hit variant and updates metrics.
    fn new_hit(value: Arc<SelectorResultValue>) -> Self {
        selector_result_cache_hit();
        Self::Hit(LastRowCacheReader { value, idx: 0 })
    }

    /// Creates new Miss variant and updates metrics.
    fn new_miss(
        key: SelectorResultKey,
        row_group_reader: RowGroupReader,
        cache_strategy: CacheStrategy,
    ) -> Self {
        selector_result_cache_miss();
        Self::Miss(RowGroupLastRowReader::new(
            key,
            row_group_reader,
            cache_strategy,
        ))
    }
}

#[async_trait]
impl BatchReader for RowGroupLastRowCachedReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            RowGroupLastRowCachedReader::Hit(r) => r.next_batch().await,
            RowGroupLastRowCachedReader::Miss(r) => r.next_batch().await,
        }
    }
}

/// Last row reader that returns the cached last rows for row group.
pub(crate) struct LastRowCacheReader {
    value: Arc<SelectorResultValue>,
    idx: usize,
}

impl LastRowCacheReader {
    /// Iterates cached last rows.
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let batches = match &self.value.result {
            SelectorResult::PrimaryKey(batches) => batches,
            SelectorResult::Flat(_) => return Ok(None),
        };
        if self.idx < batches.len() {
            let res = Ok(Some(batches[self.idx].clone()));
            self.idx += 1;
            res
        } else {
            Ok(None)
        }
    }
}

pub(crate) struct RowGroupLastRowReader {
    key: SelectorResultKey,
    reader: RowGroupReader,
    selector: LastRowSelector,
    yielded_batches: Vec<Batch>,
    cache_strategy: CacheStrategy,
    /// Index buffer to take a new batch from the last row.
    take_index: UInt32Vector,
}

impl RowGroupLastRowReader {
    fn new(key: SelectorResultKey, reader: RowGroupReader, cache_strategy: CacheStrategy) -> Self {
        Self {
            key,
            reader,
            selector: LastRowSelector::default(),
            yielded_batches: vec![],
            cache_strategy,
            take_index: UInt32Vector::from_vec(vec![0]),
        }
    }

    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.reader.next_batch().await? {
            if let Some(yielded) = self.selector.on_next(batch) {
                push_yielded_batches(yielded.clone(), &self.take_index, &mut self.yielded_batches)?;
                return Ok(Some(yielded));
            }
        }
        let last_batch = if let Some(last_batch) = self.selector.finish() {
            push_yielded_batches(
                last_batch.clone(),
                &self.take_index,
                &mut self.yielded_batches,
            )?;
            Some(last_batch)
        } else {
            None
        };

        // All last rows in row group are yielded, update cache.
        self.maybe_update_cache();
        Ok(last_batch)
    }

    /// Updates row group's last row cache if cache manager is present.
    fn maybe_update_cache(&mut self) {
        if self.yielded_batches.is_empty() {
            // we always expect that row groups yields batches.
            return;
        }
        let value = Arc::new(SelectorResultValue::new(
            std::mem::take(&mut self.yielded_batches),
            self.reader.read_format().projection_indices().to_vec(),
        ));
        self.cache_strategy.put_selector_result(self.key, value);
    }

    fn metrics(&self) -> &ReaderMetrics {
        self.reader.metrics()
    }
}

/// Push last row into `yielded_batches`.
fn push_yielded_batches(
    mut batch: Batch,
    take_index: &UInt32Vector,
    yielded_batches: &mut Vec<Batch>,
) -> Result<()> {
    assert_eq!(1, batch.num_rows());
    batch.take_in_place(take_index)?;
    yielded_batches.push(batch);

    Ok(())
}

/// Common struct that selects only the last row of each time series.
#[derive(Default)]
pub struct LastRowSelector {
    last_batch: Option<Batch>,
}

impl LastRowSelector {
    /// Handles next batch. Return the yielding batch if present.
    pub fn on_next(&mut self, batch: Batch) -> Option<Batch> {
        if let Some(last) = &self.last_batch {
            if last.primary_key() == batch.primary_key() {
                // Same key, update last batch.
                self.last_batch = Some(batch);
                None
            } else {
                // Different key, return the last row in `last` and update `last_batch` by
                // current batch.
                debug_assert!(!last.is_empty());
                let last_row = last.slice(last.num_rows() - 1, 1);
                self.last_batch = Some(batch);
                Some(last_row)
            }
        } else {
            self.last_batch = Some(batch);
            None
        }
    }

    /// Finishes the selector and returns the pending batch if any.
    pub fn finish(&mut self) -> Option<Batch> {
        if let Some(last) = self.last_batch.take() {
            // This is the last key.
            let last_row = last.slice(last.num_rows() - 1, 1);
            return Some(last_row);
        }
        None
    }
}

/// Cached last row reader for flat format row group.
/// If the last rows are already cached (as flat `RecordBatch`), returns cached values.
/// Otherwise, reads from the row group, selects last rows, and updates the cache.
pub(crate) enum FlatRowGroupLastRowCachedReader {
    /// Cache hit, reads last rows from cached value.
    Hit(FlatLastRowCacheReader),
    /// Cache miss, reads from row group reader and updates cache.
    Miss(FlatRowGroupLastRowReader),
}

impl FlatRowGroupLastRowCachedReader {
    pub(crate) fn new(
        file_id: FileId,
        row_group_idx: usize,
        cache_strategy: CacheStrategy,
        projection: &[usize],
        reader: FlatRowGroupReader,
    ) -> Self {
        let key = SelectorResultKey {
            file_id,
            row_group_idx,
            selector: TimeSeriesRowSelector::LastRow,
        };

        if let Some(value) = cache_strategy.get_selector_result(&key) {
            let is_flat = matches!(&value.result, SelectorResult::Flat(_));
            let schema_matches = value.projection == projection;
            if is_flat && schema_matches {
                Self::new_hit(value)
            } else {
                Self::new_miss(key, projection, reader, cache_strategy)
            }
        } else {
            Self::new_miss(key, projection, reader, cache_strategy)
        }
    }

    /// Gets the underlying reader metrics if uncached.
    pub(crate) fn metrics(&self) -> Option<&ReaderMetrics> {
        match self {
            FlatRowGroupLastRowCachedReader::Hit(_) => None,
            FlatRowGroupLastRowCachedReader::Miss(reader) => Some(&reader.metrics),
        }
    }

    /// Returns the next RecordBatch.
    pub(crate) fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self {
            FlatRowGroupLastRowCachedReader::Hit(r) => r.next_batch(),
            FlatRowGroupLastRowCachedReader::Miss(r) => r.next_batch(),
        }
    }

    fn new_hit(value: Arc<SelectorResultValue>) -> Self {
        selector_result_cache_hit();
        Self::Hit(FlatLastRowCacheReader { value, idx: 0 })
    }

    fn new_miss(
        key: SelectorResultKey,
        projection: &[usize],
        reader: FlatRowGroupReader,
        cache_strategy: CacheStrategy,
    ) -> Self {
        selector_result_cache_miss();
        Self::Miss(FlatRowGroupLastRowReader::new(
            key,
            projection.to_vec(),
            reader,
            cache_strategy,
        ))
    }
}

/// Iterates over cached flat last rows.
pub(crate) struct FlatLastRowCacheReader {
    value: Arc<SelectorResultValue>,
    idx: usize,
}

impl FlatLastRowCacheReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let batches = match &self.value.result {
            SelectorResult::Flat(batches) => batches,
            SelectorResult::PrimaryKey(_) => return Ok(None),
        };
        if self.idx < batches.len() {
            let res = Ok(Some(batches[self.idx].clone()));
            self.idx += 1;
            res
        } else {
            Ok(None)
        }
    }
}

/// Reads last rows from a flat format row group and caches the results.
pub(crate) struct FlatRowGroupLastRowReader {
    key: SelectorResultKey,
    reader: FlatRowGroupReader,
    selector: FlatLastTimestampSelector,
    yielded_batches: Vec<RecordBatch>,
    cache_strategy: CacheStrategy,
    projection: Vec<usize>,
    metrics: ReaderMetrics,
}

impl FlatRowGroupLastRowReader {
    fn new(
        key: SelectorResultKey,
        projection: Vec<usize>,
        reader: FlatRowGroupReader,
        cache_strategy: CacheStrategy,
    ) -> Self {
        Self {
            key,
            reader,
            selector: FlatLastTimestampSelector::default(),
            yielded_batches: vec![],
            cache_strategy,
            projection,
            metrics: ReaderMetrics::default(),
        }
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // First, drain any pending results from the selector.
        if let Some(yielded) = self.selector.take_yielded() {
            self.yielded_batches.push(yielded.clone());
            return Ok(Some(yielded));
        }

        while let Some(batch) = self.reader.next_batch()? {
            self.metrics.num_rows += batch.num_rows();
            self.metrics.num_batches += 1;
            if let Some(yielded) = self.selector.on_next(batch) {
                self.yielded_batches.push(yielded.clone());
                return Ok(Some(yielded));
            }
        }

        let last_batch = self.selector.finish();
        if let Some(ref last) = last_batch {
            self.yielded_batches.push(last.clone());
        }

        // All last rows in row group are yielded, update cache.
        self.maybe_update_cache();
        Ok(last_batch)
    }

    fn maybe_update_cache(&mut self) {
        if self.yielded_batches.is_empty() {
            return;
        }
        let value = Arc::new(SelectorResultValue::new_flat(
            std::mem::take(&mut self.yielded_batches),
            self.projection.clone(),
        ));
        self.cache_strategy.put_selector_result(self.key, value);
    }
}

/// Selects the last-timestamp row per primary key from flat `RecordBatch`.
///
/// Assumes that input batches are sorted by primary key then by timestamp,
/// and contain only PUT operations (no DELETEs).
#[derive(Default)]
pub(crate) struct FlatLastTimestampSelector {
    /// Pending batch from a previous `on_next()` that needs to be yielded.
    pending_yield: Option<RecordBatch>,
    /// The last batch we saw - we hold onto it in case the next batch continues the same key.
    last_batch: Option<RecordBatch>,
}

impl FlatLastTimestampSelector {
    /// Takes a pending yielded batch, if any.
    fn take_yielded(&mut self) -> Option<RecordBatch> {
        self.pending_yield.take()
    }

    /// Processes the next batch. Returns the first completed key result if available.
    /// Additional results may be available via `take_yielded()`.
    pub(crate) fn on_next(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        if batch.num_rows() == 0 {
            return None;
        }

        let num_columns = batch.num_columns();
        let pk_col_idx = primary_key_column_index(num_columns);

        // Split the batch by primary key boundaries.
        let key_ranges = split_by_key(&batch, pk_col_idx);

        if key_ranges.len() == 1 {
            // The entire batch has one key.
            if let Some(last) = &self.last_batch {
                let last_key = primary_key_bytes_at(last, pk_col_idx, last.num_rows() - 1);
                let curr_key = primary_key_bytes_at(&batch, pk_col_idx, 0);
                if last_key == curr_key {
                    // Same key, just update last_batch.
                    self.last_batch = Some(batch);
                    return None;
                }
                // Different key - yield last row from previous batch.
                let yielded = last_timestamp_row(last);
                self.last_batch = Some(batch);
                return Some(yielded);
            }
            // No previous batch.
            self.last_batch = Some(batch);
            return None;
        }

        // Multiple keys in the batch.
        // Handle the first range - it may continue the previous key.
        let first_range = &key_ranges[0];
        let first_result = if let Some(last) = self.last_batch.take() {
            let last_key = primary_key_bytes_at(&last, pk_col_idx, last.num_rows() - 1);
            let curr_key = primary_key_bytes_at(&batch, pk_col_idx, first_range.start);
            if last_key == curr_key {
                // Same key as previous batch - yield last row from the first range
                // (which extends the previous key).
                let sub_batch = batch.slice(first_range.start, first_range.end - first_range.start);
                last_timestamp_row(&sub_batch)
            } else {
                // Different key - yield last row from previous batch, and also from first range.
                let sub_batch = batch.slice(first_range.start, first_range.end - first_range.start);
                self.pending_yield = Some(last_timestamp_row(&sub_batch));
                last_timestamp_row(&last)
            }
        } else {
            // No previous batch - yield last row from first range.
            let sub_batch = batch.slice(first_range.start, first_range.end - first_range.start);
            last_timestamp_row(&sub_batch)
        };

        // Handle middle ranges (all complete keys).
        for range in &key_ranges[1..key_ranges.len() - 1] {
            let sub_batch = batch.slice(range.start, range.end - range.start);
            let row = last_timestamp_row(&sub_batch);
            if self.pending_yield.is_none() {
                self.pending_yield = Some(row);
            }
        }

        // The last range becomes the new pending batch.
        let last_range = &key_ranges[key_ranges.len() - 1];
        let last_sub = batch.slice(last_range.start, last_range.end - last_range.start);
        self.last_batch = Some(last_sub);

        Some(first_result)
    }

    /// Finishes the selector and returns the pending batch if any.
    pub(crate) fn finish(&mut self) -> Option<RecordBatch> {
        debug_assert!(self.pending_yield.is_none());
        if let Some(last) = self.last_batch.take()
            && last.num_rows() > 0
        {
            return Some(last_timestamp_row(&last));
        }
        None
    }
}

/// Gets the primary key bytes at `index` from the primary key dictionary column.
fn primary_key_bytes_at(batch: &RecordBatch, pk_col_idx: usize, index: usize) -> &[u8] {
    let pk_dict = batch
        .column(pk_col_idx)
        .as_any()
        .downcast_ref::<PrimaryKeyArray>()
        .unwrap();
    let key = pk_dict.keys().value(index);
    let binary_values = pk_dict
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    binary_values.value(key as usize)
}

/// Splits a batch into ranges of contiguous same-key rows.
fn split_by_key(batch: &RecordBatch, pk_col_idx: usize) -> Vec<std::ops::Range<usize>> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return vec![];
    }

    let mut ranges = Vec::new();
    let mut start = 0;
    for i in 1..num_rows {
        if primary_key_bytes_at(batch, pk_col_idx, i)
            != primary_key_bytes_at(batch, pk_col_idx, start)
        {
            ranges.push(start..i);
            start = i;
        }
    }
    ranges.push(start..num_rows);
    ranges
}

/// Returns the row with the maximum timestamp in the batch.
/// Assumes the batch is sorted by timestamp and has at least one row.
fn last_timestamp_row(batch: &RecordBatch) -> RecordBatch {
    debug_assert!(batch.num_rows() > 0);
    // Since the batch is sorted by timestamp, the last row has the max timestamp.
    batch.slice(batch.num_rows() - 1, 1)
}

#[cfg(test)]
mod tests {
    use api::v1::OpType;

    use super::*;
    use crate::test_util::{VecBatchReader, check_reader_result, new_batch};

    #[tokio::test]
    async fn test_last_row_one_batch() {
        let input = [new_batch(
            b"k1",
            &[1, 2],
            &[11, 11],
            &[OpType::Put, OpType::Put],
            &[21, 22],
        )];
        let reader = VecBatchReader::new(&input);
        let mut reader = LastRowReader::new(Box::new(reader));
        check_reader_result(
            &mut reader,
            &[new_batch(b"k1", &[2], &[11], &[OpType::Put], &[22])],
        )
        .await;

        // Only one row.
        let input = [new_batch(b"k1", &[1], &[11], &[OpType::Put], &[21])];
        let reader = VecBatchReader::new(&input);
        let mut reader = LastRowReader::new(Box::new(reader));
        check_reader_result(
            &mut reader,
            &[new_batch(b"k1", &[1], &[11], &[OpType::Put], &[21])],
        )
        .await;
    }

    #[tokio::test]
    async fn test_last_row_multi_batch() {
        let input = [
            new_batch(
                b"k1",
                &[1, 2],
                &[11, 11],
                &[OpType::Put, OpType::Put],
                &[21, 22],
            ),
            new_batch(
                b"k1",
                &[3, 4],
                &[11, 11],
                &[OpType::Put, OpType::Put],
                &[23, 24],
            ),
            new_batch(
                b"k2",
                &[1, 2],
                &[11, 11],
                &[OpType::Put, OpType::Put],
                &[31, 32],
            ),
        ];
        let reader = VecBatchReader::new(&input);
        let mut reader = LastRowReader::new(Box::new(reader));
        check_reader_result(
            &mut reader,
            &[
                new_batch(b"k1", &[4], &[11], &[OpType::Put], &[24]),
                new_batch(b"k2", &[2], &[11], &[OpType::Put], &[32]),
            ],
        )
        .await;
    }

    mod flat_selector_tests {
        use std::sync::Arc;

        use datatypes::arrow::array::{
            ArrayRef, BinaryDictionaryBuilder, Int64Array, TimestampMillisecondArray, UInt8Array,
            UInt64Array,
        };
        use datatypes::arrow::datatypes::{
            DataType, Field, Schema, SchemaRef, TimeUnit, UInt32Type,
        };
        use datatypes::arrow::record_batch::RecordBatch;

        use super::super::FlatLastTimestampSelector;

        /// Helper to build a flat format RecordBatch for testing.
        fn new_flat_batch(
            primary_keys: &[&[u8]],
            timestamps: &[i64],
            fields: &[i64],
        ) -> RecordBatch {
            let num_rows = timestamps.len();
            assert_eq!(primary_keys.len(), num_rows);
            assert_eq!(fields.len(), num_rows);

            let columns: Vec<ArrayRef> = vec![
                // field0 column
                Arc::new(Int64Array::from_iter_values(fields.iter().copied())),
                // ts column (time index)
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    timestamps.iter().copied(),
                )),
                // __primary_key column (dictionary(uint32, binary))
                {
                    let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
                    for &pk in primary_keys {
                        builder.append(pk).unwrap();
                    }
                    Arc::new(builder.finish())
                },
                // __sequence column
                Arc::new(UInt64Array::from_iter_values(vec![1u64; num_rows])),
                // __op_type column
                Arc::new(UInt8Array::from_iter_values(vec![1u8; num_rows])),
            ];

            RecordBatch::try_new(test_flat_schema(), columns).unwrap()
        }

        fn test_flat_schema() -> SchemaRef {
            let fields = vec![
                Field::new("field0", DataType::Int64, false),
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

        /// Collects all results from the selector.
        fn collect_results(
            selector: &mut FlatLastTimestampSelector,
            batches: Vec<RecordBatch>,
        ) -> Vec<RecordBatch> {
            let mut results = Vec::new();
            for batch in batches {
                if let Some(r) = selector.on_next(batch) {
                    results.push(r);
                    while let Some(pending) = selector.take_yielded() {
                        results.push(pending);
                    }
                }
            }
            if let Some(r) = selector.finish() {
                results.push(r);
            }
            results
        }

        fn assert_batch_eq(batch: &RecordBatch, expected_pk: &[u8], expected_ts: i64) {
            assert_eq!(1, batch.num_rows(), "Expected single row batch");
            let ts_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            assert_eq!(expected_ts, ts_col.value(0));

            let pk_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<super::super::PrimaryKeyArray>()
                .unwrap();
            let key_idx = pk_col.keys().value(0);
            let binary_values = pk_col
                .values()
                .as_any()
                .downcast_ref::<super::super::BinaryArray>()
                .unwrap();
            assert_eq!(expected_pk, binary_values.value(key_idx as usize));
        }

        #[test]
        fn test_single_batch_one_key() {
            let mut selector = FlatLastTimestampSelector::default();
            let batch = new_flat_batch(&[b"k1", b"k1", b"k1"], &[1, 2, 3], &[10, 20, 30]);
            let results = collect_results(&mut selector, vec![batch]);
            assert_eq!(1, results.len());
            assert_batch_eq(&results[0], b"k1", 3);
        }

        #[test]
        fn test_single_batch_multiple_keys() {
            let mut selector = FlatLastTimestampSelector::default();
            let batch = new_flat_batch(
                &[b"k1", b"k1", b"k2", b"k2", b"k3"],
                &[1, 2, 3, 4, 5],
                &[10, 20, 30, 40, 50],
            );
            let results = collect_results(&mut selector, vec![batch]);
            assert_eq!(3, results.len());
            assert_batch_eq(&results[0], b"k1", 2);
            assert_batch_eq(&results[1], b"k2", 4);
            assert_batch_eq(&results[2], b"k3", 5);
        }

        #[test]
        fn test_multiple_batches_same_key() {
            let mut selector = FlatLastTimestampSelector::default();
            let batches = vec![
                new_flat_batch(&[b"k1", b"k1"], &[1, 2], &[10, 20]),
                new_flat_batch(&[b"k1", b"k1"], &[3, 4], &[30, 40]),
            ];
            let results = collect_results(&mut selector, batches);
            assert_eq!(1, results.len());
            assert_batch_eq(&results[0], b"k1", 4);
        }

        #[test]
        fn test_multiple_batches_different_keys() {
            let mut selector = FlatLastTimestampSelector::default();
            let batches = vec![
                new_flat_batch(&[b"k1", b"k1"], &[1, 2], &[10, 20]),
                new_flat_batch(&[b"k2", b"k2"], &[3, 4], &[30, 40]),
            ];
            let results = collect_results(&mut selector, batches);
            assert_eq!(2, results.len());
            assert_batch_eq(&results[0], b"k1", 2);
            assert_batch_eq(&results[1], b"k2", 4);
        }

        #[test]
        fn test_single_row() {
            let mut selector = FlatLastTimestampSelector::default();
            let batch = new_flat_batch(&[b"k1"], &[42], &[100]);
            let results = collect_results(&mut selector, vec![batch]);
            assert_eq!(1, results.len());
            assert_batch_eq(&results[0], b"k1", 42);
        }

        #[test]
        fn test_key_spans_batches() {
            let mut selector = FlatLastTimestampSelector::default();
            let batches = vec![
                new_flat_batch(&[b"k1", b"k1"], &[1, 2], &[10, 20]),
                new_flat_batch(&[b"k1", b"k2"], &[3, 4], &[30, 40]),
                new_flat_batch(&[b"k2", b"k3"], &[5, 6], &[50, 60]),
            ];
            let results = collect_results(&mut selector, batches);
            assert_eq!(3, results.len());
            assert_batch_eq(&results[0], b"k1", 3);
            assert_batch_eq(&results[1], b"k2", 5);
            assert_batch_eq(&results[2], b"k3", 6);
        }
    }
}
