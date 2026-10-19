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

use datatypes::arrow::array::{Array, BinaryArray, UInt32Array};
use datatypes::arrow::compute::{concat_batches, take_record_batch};
use datatypes::arrow::record_batch::RecordBatch;
use futures::{Stream, TryStreamExt};
use snafu::ResultExt;
use store_api::storage::{FileId, TimeSeriesRowSelector};

use crate::cache::{
    CacheStrategy, SelectorResult, SelectorResultKey, SelectorResultValue,
    selector_result_cache_hit, selector_result_cache_miss,
};
use crate::error::{ComputeArrowSnafu, Result};
use crate::read::read_columns::JsonTargetTypes;
use crate::read::{BoxedRecordBatchStream, timestamp_array_to_i64_slice};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::flat_format::{primary_key_column_index, time_index_column_index};
use crate::sst::parquet::format::{PrimaryKeyArray, primary_key_offsets};
use crate::sst::parquet::read_columns::ParquetReadColumns;
use crate::sst::parquet::reader::FlatRowGroupReader;

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
        read_cols: &ParquetReadColumns,
        json_target_types: JsonTargetTypes,
        reader: FlatRowGroupReader,
    ) -> Self {
        let key = SelectorResultKey {
            file_id,
            row_group_idx,
            selector: TimeSeriesRowSelector::LastRow { after_merge: false },
        };

        if let Some(value) = cache_strategy.get_selector_result(&key) {
            let is_flat = matches!(&value.result, SelectorResult::Flat(_));
            let schema_matches = value.read_cols == *read_cols;
            let json_target_types_matches = value.json_target_types == json_target_types;
            if is_flat && schema_matches && json_target_types_matches {
                Self::new_hit(value)
            } else {
                Self::new_miss(key, read_cols, json_target_types, reader, cache_strategy)
            }
        } else {
            Self::new_miss(key, read_cols, json_target_types, reader, cache_strategy)
        }
    }

    /// Returns the next RecordBatch.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self {
            FlatRowGroupLastRowCachedReader::Hit(r) => r.next_batch(),
            FlatRowGroupLastRowCachedReader::Miss(r) => r.next_batch().await,
        }
    }

    fn new_hit(value: Arc<SelectorResultValue>) -> Self {
        selector_result_cache_hit();
        Self::Hit(FlatLastRowCacheReader { value, idx: 0 })
    }

    fn new_miss(
        key: SelectorResultKey,
        read_cols: &ParquetReadColumns,
        json_target_types: JsonTargetTypes,
        reader: FlatRowGroupReader,
        cache_strategy: CacheStrategy,
    ) -> Self {
        selector_result_cache_miss();
        Self::Miss(FlatRowGroupLastRowReader::new(
            key,
            read_cols.clone(),
            json_target_types,
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

    fn push(&mut self, batch: RecordBatch) {
        self.num_rows += batch.num_rows();
        self.batches.push(batch);
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
    selector: FlatLastTimestampSelector,
    yielded_batches: Vec<RecordBatch>,
    cache_strategy: CacheStrategy,
    read_cols: ParquetReadColumns,
    json_target_types: JsonTargetTypes,
    /// Accumulates small selector-output batches before concatenating.
    pending: BatchBuffer,
}

impl FlatRowGroupLastRowReader {
    fn new(
        key: SelectorResultKey,
        read_cols: ParquetReadColumns,
        json_target_types: JsonTargetTypes,
        reader: FlatRowGroupReader,
        cache_strategy: CacheStrategy,
    ) -> Self {
        Self {
            key,
            reader,
            selector: FlatLastTimestampSelector::default(),
            yielded_batches: vec![],
            cache_strategy,
            read_cols,
            json_target_types,
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

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending.is_full() {
            return self.flush_pending();
        }

        while let Some(batch) = self.reader.next_batch().await? {
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

    fn maybe_update_cache(&mut self) {
        if self.yielded_batches.is_empty() {
            return;
        }
        let batches = std::mem::take(&mut self.yielded_batches);
        let value = Arc::new(SelectorResultValue::new_flat(
            batches,
            self.read_cols.clone(),
            self.json_target_types.clone(),
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

        // Dictionary keys may differ for the same primary key, e.g. readers that wrap binary
        // keys into a dictionary with identity keys, so merge adjacent ranges by key bytes.
        let mut series_ranges: Vec<(usize, usize)> = Vec::with_capacity(offsets.len() - 1);
        for window in offsets.windows(2) {
            let (start, end) = (window[0], window[1]);
            match series_ranges.last_mut() {
                Some(last)
                    if primary_key_bytes_at(&batch, pk_col_idx, last.0)
                        == primary_key_bytes_at(&batch, pk_col_idx, start) =>
                {
                    last.1 = end;
                }
                _ => series_ranges.push((start, end)),
            }
        }

        let ts_values = timestamp_array_to_i64_slice(batch.column(ts_col_idx));
        let num_ranges = series_ranges.len();
        // Rows of keys that start and end inside this batch. They are gathered with one
        // `take` instead of buffering a slice per key: concatenating thousands of single-row
        // slices merges their dictionaries once per slice.
        let mut completed_rows = Vec::new();
        for (i, &(range_start, range_end)) in series_ranges.iter().enumerate() {
            let range_key = primary_key_bytes_at(&batch, pk_col_idx, range_start);
            let range_last_ts = ts_values[range_end - 1];
            let range_last_ts_start = last_timestamp_start(ts_values, range_start, range_end);
            let is_last_range = i + 1 == num_ranges;

            // Only the first range can continue the key carried over from previous batches.
            if let Some(state) = self.current_key.as_mut()
                && state.key.as_slice() == range_key
            {
                let range_slice = batch.slice(range_last_ts_start, range_end - range_last_ts_start);
                if range_last_ts > state.last_timestamp {
                    state.last_timestamp = range_last_ts;
                    state.slices.clear();
                    state.slices.push(range_slice);
                } else if range_last_ts == state.last_timestamp {
                    state.slices.push(range_slice);
                }
                if !is_last_range {
                    self.flush_current_key(output_buffer);
                }
                continue;
            }

            // Flushes the carried-over key before rows of this batch to keep the output sorted.
            self.flush_current_key(output_buffer);
            if is_last_range {
                // The key may continue in the next batch.
                self.current_key = Some(LastKeyState::new(
                    range_key.to_vec(),
                    range_last_ts,
                    batch.slice(range_last_ts_start, range_end - range_last_ts_start),
                ));
            } else {
                completed_rows.extend(range_last_ts_start as u32..range_end as u32);
            }
        }

        if !completed_rows.is_empty() {
            let indices = UInt32Array::from(completed_rows);
            let selected = take_record_batch(&batch, &indices).context(ComputeArrowSnafu)?;
            output_buffer.push(selected);
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

/// Reader that keeps only the last row of each time series from a flat RecordBatch stream.
/// Assumes input is sorted, deduped, and contains no delete operations.
pub(crate) struct FlatLastRowReader {
    stream: BoxedRecordBatchStream,
    selector: FlatLastTimestampSelector,
    pending: BatchBuffer,
}

impl FlatLastRowReader {
    /// Creates a new `FlatLastRowReader`.
    pub(crate) fn new(stream: BoxedRecordBatchStream) -> Self {
        Self {
            stream,
            selector: FlatLastTimestampSelector::default(),
            pending: BatchBuffer::new(),
        }
    }

    /// Converts the reader into a stream of RecordBatches.
    pub(crate) fn into_stream(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        async_stream::try_stream! {
            while let Some(batch) = self.stream.try_next().await? {
                self.selector.on_next(batch, &mut self.pending)?;
                if self.pending.is_full() {
                    yield self.pending.concat()?;
                }
            }
            self.selector.finish(&mut self.pending)?;
            if !self.pending.is_empty() {
                yield self.pending.concat()?;
            }
        }
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

    use datatypes::arrow::array::{
        ArrayRef, BinaryDictionaryBuilder, DictionaryArray, Int64Array, TimestampMillisecondArray,
        UInt8Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;

    use super::*;

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
    fn test_flat_concat_across_parent_dictionaries() {
        let mut selector = FlatLastTimestampSelector::default();
        let mut output_buffer = BatchBuffer::new();
        let batches = vec![
            new_flat_batch(&[b"a", b"b", b"b", b"c"], &[1, 1, 2, 3], &[10, 20, 21, 30]),
            new_flat_batch(
                &[b"c", b"d", b"d", b"e", b"e"],
                &[3, 4, 4, 5, 5],
                &[31, 40, 41, 50, 51],
            ),
            new_flat_batch(&[b"f", b"g"], &[6, 7], &[60, 70]),
        ];
        for batch in batches {
            selector.on_next(batch, &mut output_buffer).unwrap();
        }
        selector.finish(&mut output_buffer).unwrap();
        let merged = output_buffer.concat().unwrap();

        let mut rows = Vec::new();
        extract_flat_rows(&merged, &mut rows);
        let fields = merged
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            vec![
                (b"a".to_vec(), 1),
                (b"b".to_vec(), 2),
                (b"c".to_vec(), 3),
                (b"c".to_vec(), 3),
                (b"d".to_vec(), 4),
                (b"d".to_vec(), 4),
                (b"e".to_vec(), 5),
                (b"e".to_vec(), 5),
                (b"f".to_vec(), 6),
                (b"g".to_vec(), 7),
            ],
            rows
        );
        assert_eq!(vec![10, 21, 30, 31, 40, 41, 50, 51, 60, 70], fields);
    }

    /// Rebuilds the primary key column with one dictionary key per row, as
    /// `wrap_pk_binary_to_dict` does for binary primary keys.
    fn with_identity_pk_keys(batch: RecordBatch) -> RecordBatch {
        let pk_col_idx = primary_key_column_index(batch.num_columns());
        let values = (0..batch.num_rows())
            .map(|i| Some(primary_key_bytes_at(&batch, pk_col_idx, i)))
            .collect::<BinaryArray>();
        let keys = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
        let mut columns = batch.columns().to_vec();
        columns[pk_col_idx] = Arc::new(DictionaryArray::new(keys, Arc::new(values)));
        RecordBatch::try_new(batch.schema(), columns).unwrap()
    }

    #[test]
    fn test_flat_identity_dictionary_keys() {
        let mut selector = FlatLastTimestampSelector::default();
        let batches = vec![
            with_identity_pk_keys(new_flat_batch(&[b"a", b"a", b"b"], &[1, 2, 3], &[0; 3])),
            with_identity_pk_keys(new_flat_batch(&[b"b", b"c", b"c"], &[4, 5, 6], &[0; 3])),
        ];
        let results = collect_flat_results(&mut selector, batches);
        assert_eq!(
            vec![(b"a".to_vec(), 2), (b"b".to_vec(), 4), (b"c".to_vec(), 6)],
            results
        );
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
