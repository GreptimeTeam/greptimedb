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
use datatypes::arrow::array::{Array, BinaryArray};
use datatypes::arrow::compute::concat_batches;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::UInt32Vector;
use mito_codec::row_converter::PrimaryKeyFilter;
use snafu::ResultExt;
use store_api::storage::{FileId, TimeSeriesRowSelector};

use crate::cache::{
    CacheStrategy, SelectorResult, SelectorResultKey, SelectorResultValue,
    selector_result_cache_hit, selector_result_cache_miss,
};
use crate::error::{ComputeArrowSnafu, Result};
use crate::memtable::partition_tree::data::timestamp_array_to_i64_slice;
use crate::read::{Batch, BatchReader, BoxedBatchReader};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::flat_format::{primary_key_column_index, time_index_column_index};
use crate::sst::parquet::format::{PrimaryKeyArray, primary_key_offsets};
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
            SelectorResult::Flat(_) => unreachable!(),
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
        primary_key_filter: Option<Box<dyn PrimaryKeyFilter>>,
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
                Self::new_miss(key, projection, reader, cache_strategy, primary_key_filter)
            }
        } else {
            Self::new_miss(key, projection, reader, cache_strategy, primary_key_filter)
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
        primary_key_filter: Option<Box<dyn PrimaryKeyFilter>>,
    ) -> Self {
        selector_result_cache_miss();
        Self::Miss(FlatRowGroupLastRowReader::new(
            key,
            projection.to_vec(),
            reader,
            cache_strategy,
            primary_key_filter,
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
            SelectorResult::PrimaryKey(_) => unreachable!(),
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

/// Buffer that accumulates small `RecordBatch`es and tracks total row count.
pub(crate) struct BatchBuffer {
    batches: Vec<RecordBatch>,
    num_rows: usize,
}

impl BatchBuffer {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            num_rows: 0,
        }
    }

    /// Returns true if total buffered rows reaches `DEFAULT_READ_BATCH_SIZE`.
    fn is_full(&self) -> bool {
        self.num_rows >= DEFAULT_READ_BATCH_SIZE
    }

    /// Extends the buffer from a slice of batches.
    fn extend_from_slice(&mut self, batches: &[RecordBatch]) {
        for batch in batches {
            self.num_rows += batch.num_rows();
        }
        self.batches.extend_from_slice(batches);
    }

    /// Returns true if the buffer has no batches.
    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Concatenates all buffered batches into one, resets the buffer, and returns the result.
    fn concat(&mut self) -> Result<RecordBatch> {
        debug_assert!(!self.batches.is_empty());
        let schema = self.batches[0].schema();
        let merged = concat_batches(&schema, &self.batches).context(ComputeArrowSnafu)?;
        self.batches.clear();
        self.num_rows = 0;
        Ok(merged)
    }
}

/// Reads last rows from a flat format row group and caches the results.
pub(crate) struct FlatRowGroupLastRowReader {
    key: SelectorResultKey,
    reader: FlatRowGroupReader,
    primary_key_filter: Option<Box<dyn PrimaryKeyFilter>>,
    selector: FlatLastTimestampSelector,
    yielded_batches: Vec<RecordBatch>,
    cache_strategy: CacheStrategy,
    projection: Vec<usize>,
    /// Accumulates small selector-output batches before concatenating.
    pending: BatchBuffer,
}

impl FlatRowGroupLastRowReader {
    fn new(
        key: SelectorResultKey,
        projection: Vec<usize>,
        reader: FlatRowGroupReader,
        cache_strategy: CacheStrategy,
        primary_key_filter: Option<Box<dyn PrimaryKeyFilter>>,
    ) -> Self {
        Self {
            key,
            reader,
            primary_key_filter,
            selector: FlatLastTimestampSelector::default(),
            yielded_batches: vec![],
            cache_strategy,
            projection,
            pending: BatchBuffer::new(),
        }
    }

    /// Concatenates pending batches and records the result in `yielded_batches`.
    fn flush_pending(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let merged = self.pending.concat()?;
        self.yielded_batches.push(merged.clone());
        Ok(Some(merged))
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending.is_full() {
            return self.flush_pending();
        }

        while let Some(raw_batch) = self.reader.next_raw_batch()? {
            let Some(raw_batch) = self.prefilter_primary_keys(raw_batch)? else {
                continue;
            };
            let batch = self.reader.convert_batch(raw_batch)?;
            self.selector.on_next(batch, &mut self.pending)?;
            if self.pending.is_full() {
                return self.flush_pending();
            }
        }

        // Reader exhausted — flush remaining selector state.
        self.selector.finish(&mut self.pending)?;
        if !self.pending.is_empty() {
            let result = self.flush_pending();
            // All last rows in row group are yielded, update cache.
            self.maybe_update_cache();
            return result;
        }

        // All last rows in row group are yielded, update cache.
        self.maybe_update_cache();
        Ok(None)
    }

    fn prefilter_primary_keys(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        let Some(primary_key_filter) = self.primary_key_filter.as_mut() else {
            return Ok(Some(batch));
        };

        self.reader
            .prefilter_raw_batch_by_primary_key(batch, primary_key_filter.as_mut())
    }

    fn maybe_update_cache(&mut self) {
        if self.yielded_batches.is_empty() {
            return;
        }

        // Filtered flat last-row scans only contain the subset of series that matched the
        // encoded primary-key prefilter, so they cannot be published under the shared
        // selector cache key.
        if self.primary_key_filter.is_some() {
            return;
        }
        let batches = std::mem::take(&mut self.yielded_batches);
        let value = Arc::new(SelectorResultValue::new_flat(
            batches,
            self.projection.clone(),
        ));
        self.cache_strategy.put_selector_result(self.key, value);
    }
}

/// Selects the last-timestamp rows per primary key from flat `RecordBatch`.
///
/// Assumes that input batches are sorted by primary key then by timestamp,
/// and contain only PUT operations (no DELETE).
#[derive(Default)]
pub(crate) struct FlatLastTimestampSelector {
    /// State for the currently in-progress primary key.
    current_key: Option<LastKeyState>,
}

#[derive(Debug)]
struct LastKeyState {
    key: Vec<u8>,
    last_timestamp: i64,
    slices: Vec<RecordBatch>,
}

impl LastKeyState {
    fn new(key: Vec<u8>, last_timestamp: i64, first_slice: RecordBatch) -> Self {
        Self {
            key,
            last_timestamp,
            slices: vec![first_slice],
        }
    }
}

impl FlatLastTimestampSelector {
    /// Processes the next batch and appends completed-key results into `output_buffer`.
    pub(crate) fn on_next(
        &mut self,
        batch: RecordBatch,
        output_buffer: &mut BatchBuffer,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let num_columns = batch.num_columns();
        let pk_col_idx = primary_key_column_index(num_columns);
        let ts_col_idx = time_index_column_index(num_columns);

        let pk_array = batch
            .column(pk_col_idx)
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        let offsets = primary_key_offsets(pk_array)?;
        if offsets.is_empty() {
            return Ok(());
        }

        let ts_values = timestamp_array_to_i64_slice(batch.column(ts_col_idx));
        for i in 0..offsets.len() - 1 {
            let range_start = offsets[i];
            let range_end = offsets[i + 1];
            let range_key = primary_key_bytes_at(&batch, pk_col_idx, range_start);
            let range_last_ts = ts_values[range_end - 1];
            let range_last_ts_start = last_timestamp_start(ts_values, range_start, range_end);
            let range_slice = batch.slice(range_last_ts_start, range_end - range_last_ts_start);

            match self.current_key.as_mut() {
                Some(state) if state.key.as_slice() == range_key => {
                    if range_last_ts > state.last_timestamp {
                        state.last_timestamp = range_last_ts;
                        state.slices.clear();
                        state.slices.push(range_slice);
                    } else if range_last_ts == state.last_timestamp {
                        state.slices.push(range_slice);
                    }
                }
                Some(_) => {
                    self.flush_current_key(output_buffer);
                    self.current_key = Some(LastKeyState::new(
                        range_key.to_vec(),
                        range_last_ts,
                        range_slice,
                    ));
                }
                None => {
                    self.current_key = Some(LastKeyState::new(
                        range_key.to_vec(),
                        range_last_ts,
                        range_slice,
                    ));
                }
            }
        }

        Ok(())
    }

    /// Finishes the selector and appends remaining results into `output_buffer`.
    pub(crate) fn finish(&mut self, output_buffer: &mut BatchBuffer) -> Result<()> {
        self.flush_current_key(output_buffer);
        Ok(())
    }

    fn flush_current_key(&mut self, output_buffer: &mut BatchBuffer) {
        let Some(state) = self.current_key.take() else {
            return;
        };
        output_buffer.extend_from_slice(&state.slices);
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

/// Finds the start index of rows sharing the last (maximum) timestamp
/// within the range `[range_start, range_end)`.
fn last_timestamp_start(ts_values: &[i64], range_start: usize, range_end: usize) -> usize {
    debug_assert!(range_start < range_end);

    let last_ts = ts_values[range_end - 1];
    let mut start = range_end - 1;
    while start > range_start && ts_values[start - 1] == last_ts {
        start -= 1;
    }
    start
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::OpType;
    use datatypes::arrow::array::{
        ArrayRef, BinaryDictionaryBuilder, Int64Array, TimestampMillisecondArray, UInt8Array,
        UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;

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

    /// Helper to build a flat format RecordBatch for testing.
    fn new_flat_batch(primary_keys: &[&[u8]], timestamps: &[i64], fields: &[i64]) -> RecordBatch {
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

    /// Collects all rows from the selector across all result batches.
    fn collect_flat_results(
        selector: &mut FlatLastTimestampSelector,
        batches: Vec<RecordBatch>,
    ) -> Vec<(Vec<u8>, i64)> {
        let mut output_buffer = BatchBuffer::new();
        let mut results = Vec::new();
        for batch in batches {
            selector.on_next(batch, &mut output_buffer).unwrap();
            for r in output_buffer.batches.drain(..) {
                extract_flat_rows(&r, &mut results);
            }
            output_buffer.num_rows = 0;
        }
        selector.finish(&mut output_buffer).unwrap();
        for r in output_buffer.batches.drain(..) {
            extract_flat_rows(&r, &mut results);
        }
        results
    }

    /// Extracts (primary_key, timestamp) pairs from a result batch.
    fn extract_flat_rows(batch: &RecordBatch, out: &mut Vec<(Vec<u8>, i64)>) {
        let ts_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let pk_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        let binary_values = pk_col
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let key_idx = pk_col.keys().value(i);
            let pk = binary_values.value(key_idx as usize).to_vec();
            let ts = ts_col.value(i);
            out.push((pk, ts));
        }
    }

    #[test]
    fn test_flat_single_batch_one_key() {
        let mut selector = FlatLastTimestampSelector::default();
        let batch = new_flat_batch(&[b"k1", b"k1", b"k1"], &[1, 2, 3], &[10, 20, 30]);
        let results = collect_flat_results(&mut selector, vec![batch]);
        assert_eq!(vec![(b"k1".to_vec(), 3)], results);
    }

    #[test]
    fn test_flat_single_batch_multiple_keys() {
        let mut selector = FlatLastTimestampSelector::default();
        let batch = new_flat_batch(
            &[b"k1", b"k1", b"k2", b"k2", b"k3"],
            &[1, 2, 3, 4, 5],
            &[10, 20, 30, 40, 50],
        );
        let results = collect_flat_results(&mut selector, vec![batch]);
        assert_eq!(
            vec![
                (b"k1".to_vec(), 2),
                (b"k2".to_vec(), 4),
                (b"k3".to_vec(), 5),
            ],
            results
        );
    }

    #[test]
    fn test_flat_key_spans_batches() {
        let mut selector = FlatLastTimestampSelector::default();
        let batches = vec![
            new_flat_batch(&[b"k1", b"k1"], &[1, 2], &[10, 20]),
            new_flat_batch(&[b"k1", b"k2"], &[3, 4], &[30, 40]),
            new_flat_batch(&[b"k2", b"k3"], &[5, 6], &[50, 60]),
        ];
        let results = collect_flat_results(&mut selector, batches);
        assert_eq!(
            vec![
                (b"k1".to_vec(), 3),
                (b"k2".to_vec(), 5),
                (b"k3".to_vec(), 6),
            ],
            results
        );
    }

    #[test]
    fn test_flat_duplicate_last_timestamps() {
        let mut selector = FlatLastTimestampSelector::default();
        // k1 has two rows with the same last timestamp (3).
        let batch = new_flat_batch(
            &[b"k1", b"k1", b"k1", b"k2"],
            &[1, 3, 3, 5],
            &[10, 20, 30, 40],
        );
        let results = collect_flat_results(&mut selector, vec![batch]);
        assert_eq!(
            vec![
                (b"k1".to_vec(), 3),
                (b"k1".to_vec(), 3),
                (b"k2".to_vec(), 5),
            ],
            results
        );
    }

    #[test]
    fn test_flat_duplicate_last_timestamps_across_batches() {
        let mut selector = FlatLastTimestampSelector::default();
        // k1's last timestamp (3) spans two batches.
        let batches = vec![
            new_flat_batch(&[b"k1", b"k1"], &[1, 3], &[10, 20]),
            new_flat_batch(&[b"k1", b"k2"], &[3, 5], &[30, 40]),
        ];
        let results = collect_flat_results(&mut selector, batches);
        assert_eq!(
            vec![
                (b"k1".to_vec(), 3),
                (b"k1".to_vec(), 3),
                (b"k2".to_vec(), 5),
            ],
            results
        );
    }

    #[test]
    fn test_flat_pending_chain_dropped_by_higher_timestamp() {
        let mut selector = FlatLastTimestampSelector::default();
        let batches = vec![
            new_flat_batch(&[b"k1", b"k1"], &[1, 3], &[10, 20]),
            new_flat_batch(&[b"k1", b"k1"], &[3, 3], &[21, 22]),
            new_flat_batch(&[b"k1", b"k1"], &[4, 4], &[23, 24]),
        ];
        let results = collect_flat_results(&mut selector, batches);
        assert_eq!(vec![(b"k1".to_vec(), 4), (b"k1".to_vec(), 4)], results);
    }

    #[test]
    fn test_flat_finish_is_one_shot() {
        let mut selector = FlatLastTimestampSelector::default();
        let batch = new_flat_batch(&[b"k1", b"k1", b"k2"], &[1, 2, 3], &[10, 20, 30]);
        let mut output_buffer = BatchBuffer::new();

        // Feed one batch: completed keys can be emitted before EOF.
        selector.on_next(batch, &mut output_buffer).unwrap();
        let mut pre_finish = Vec::new();
        for r in output_buffer.batches.drain(..) {
            extract_flat_rows(&r, &mut pre_finish);
        }
        output_buffer.num_rows = 0;
        assert_eq!(vec![(b"k1".to_vec(), 2)], pre_finish);

        // Simulate EOF by calling finish().
        selector.finish(&mut output_buffer).unwrap();
        assert!(!output_buffer.is_empty());
        output_buffer.batches.clear();
        output_buffer.num_rows = 0;

        // A second finish after EOF should not yield any more rows.
        selector.finish(&mut output_buffer).unwrap();
        assert!(output_buffer.is_empty());
    }
}
