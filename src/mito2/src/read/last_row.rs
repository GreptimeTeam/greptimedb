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
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch;
use futures::{Stream, TryStreamExt};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{FileId, TimeSeriesRowSelector};

use crate::cache::{
    CacheStrategy, SelectorResult, SelectorResultKey, SelectorResultValue,
    selector_result_cache_hit, selector_result_cache_miss,
};
use crate::error::{ComputeArrowSnafu, InvalidBatchSnafu, Result};
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

/// Prunes only proven-unused samples. Batch boundaries retain extra candidates,
/// so independently pruned streams can be merged without losing a grid winner.
/// Values, including stale markers and nulls, must remain untouched: downstream
/// instant evaluation checks staleness after selecting the newest timestamp.
pub(crate) fn prune_instant_grid(
    batch: RecordBatch,
    selector: TimeSeriesRowSelector,
) -> Result<RecordBatch> {
    let TimeSeriesRowSelector::InstantGrid {
        start,
        end,
        interval,
        lookback,
        offset,
    } = selector
    else {
        return Ok(batch);
    };
    if batch.num_rows() == 0 || interval <= 0 || lookback < 0 {
        return Ok(batch);
    }
    let ts_idx = time_index_column_index(batch.num_columns());
    // The first prototype changes only the native millisecond scan path.
    if !matches!(
        batch.column(ts_idx).data_type(),
        DataType::Timestamp(TimeUnit::Millisecond, _)
    ) {
        return Ok(batch);
    }
    let pk_idx = primary_key_column_index(batch.num_columns());
    let pk_array = batch
        .column(pk_idx)
        .as_any()
        .downcast_ref::<PrimaryKeyArray>()
        .context(InvalidBatchSnafu {
            reason: "instant grid requires a flat primary-key dictionary",
        })?;
    let offsets = primary_key_offsets(pk_array)?;
    let timestamps = timestamp_array_to_i64_slice(batch.column(ts_idx));
    let start = i128::from(start);
    let end = i128::from(end);
    let interval = i128::from(interval);
    let offset = i128::from(offset);
    let lookback = i128::from(lookback);
    let mut selected = Vec::new();
    for bounds in offsets.windows(2) {
        let mut row = bounds[0];
        let mut evaluation = start;
        while row < bounds[1] {
            let timestamp = timestamps[row];
            let mut next = row + 1;
            while next < bounds[1] && timestamps[next] == timestamp {
                next += 1;
            }
            let sample = i128::from(timestamp) + offset;
            if sample > evaluation {
                let delta = sample - evaluation;
                evaluation += if delta <= interval {
                    interval
                } else {
                    ((delta + interval - 1) / interval) * interval
                };
            }
            let valid =
                evaluation <= end && (evaluation == sample || evaluation - sample < lookback);
            if valid && (next == bounds[1] || evaluation < i128::from(timestamps[next]) + offset) {
                // Keep every equal-timestamp row; selection must not invent a tie order.
                selected.extend((row..next).map(|index| index as u32));
            }
            row = next;
        }
    }
    if selected.len() == batch.num_rows() {
        return Ok(batch);
    }
    take_record_batch(&batch, &UInt32Array::from(selected)).context(ComputeArrowSnafu)
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
        ArrayRef, BinaryDictionaryBuilder, Int64Array, TimestampMillisecondArray, UInt8Array,
        UInt64Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;

    use super::*;

    #[test]
    fn instant_grid_preserves_winners_across_batches() {
        // Duplicate timestamps preserve their original order. Distinct row IDs
        // let the independent oracle detect accidental tie-breaking changes.
        let timestamps = [-20, -11, -11, -4, 0, 1, 3, 8, 8, 17, 29, 41];
        let ids: Vec<i64> = (0..timestamps.len() as i64).collect();
        let batch = new_flat_batch(&vec![b"a".as_slice(); timestamps.len()], &timestamps, &ids);
        for interval in [1, 3, 10, 30] {
            for lookback in [0, 1, 7, 35] {
                for offset in [-17, 0, 23] {
                    for chunk_size in [1, 2, 5, 12] {
                        let selector = TimeSeriesRowSelector::InstantGrid {
                            start: -10,
                            end: 50,
                            interval,
                            lookback,
                            offset,
                        };
                        let mut retained = Vec::new();
                        for start in (0..timestamps.len()).step_by(chunk_size) {
                            let output = prune_instant_grid(
                                batch.slice(start, chunk_size.min(timestamps.len() - start)),
                                selector,
                            )
                            .unwrap();
                            retained.extend_from_slice(
                                output
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .values(),
                            );
                        }
                        for evaluation in (-10..=50).step_by(interval as usize) {
                            let winner = |candidates: &[i64]| {
                                candidates.iter().copied().rev().find(|id| {
                                    let sample = timestamps[*id as usize] + offset;
                                    sample <= evaluation
                                        && (sample == evaluation || evaluation - sample < lookback)
                                })
                            };
                            assert_eq!(
                                winner(&ids),
                                winner(&retained),
                                "{selector:?}, chunk={chunk_size}, evaluation={evaluation}"
                            );
                        }
                    }
                }
            }
        }
        let reduced = prune_instant_grid(
            batch,
            TimeSeriesRowSelector::InstantGrid {
                start: 0,
                end: 40,
                interval: 10,
                lookback: 30,
                offset: 0,
            },
        )
        .unwrap();
        assert!(reduced.num_rows() < timestamps.len());

        let batch = new_flat_batch(&[b"a", b"a", b"b", b"b"], &[0, 4, 0, 4], &[1, 2, 3, 4]);
        let selected = prune_instant_grid(
            batch,
            TimeSeriesRowSelector::InstantGrid {
                start: 5,
                end: 5,
                interval: 10,
                lookback: 10,
                offset: 0,
            },
        )
        .unwrap();
        assert_eq!(
            selected
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[2, 4]
        );

        let batch = new_flat_batch(&[b"a", b"a"], &[i64::MAX - 1, i64::MAX], &[1, 2]);
        let selected = prune_instant_grid(
            batch,
            TimeSeriesRowSelector::InstantGrid {
                start: -1,
                end: 0,
                interval: 1,
                lookback: 1,
                offset: -i64::MAX,
            },
        )
        .unwrap();
        assert_eq!(selected.num_rows(), 2);
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
