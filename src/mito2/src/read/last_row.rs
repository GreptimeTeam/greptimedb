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

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use datatypes::arrow::array::{Array, BinaryArray, DictionaryArray, UInt32Array, UInt64Array};
use datatypes::arrow::compute::take_record_batch;
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::UInt32Vector;
use snafu::ResultExt;
use store_api::storage::{FileId, TimeSeriesRowSelector};

use crate::cache::{
    CacheStrategy, SelectorResult, SelectorResultKey, SelectorResultValue,
    selector_result_cache_hit, selector_result_cache_miss,
};
use crate::error::{ComputeArrowSnafu, Result};
use crate::memtable::partition_tree::data::timestamp_array_to_i64_slice;
use crate::read::{Batch, BatchReader, BoxedBatchReader};
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
    output_buffer: VecDeque<RecordBatch>,
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
            output_buffer: VecDeque::new(),
        }
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(yielded) = self.output_buffer.pop_front() {
            self.yielded_batches.push(yielded.clone());
            return Ok(Some(yielded));
        }

        while let Some(batch) = self.reader.next_batch()? {
            self.metrics.num_rows += batch.num_rows();
            self.metrics.num_batches += 1;
            self.selector.on_next(batch, &mut self.output_buffer)?;
            if let Some(yielded) = self.output_buffer.pop_front() {
                self.yielded_batches.push(yielded.clone());
                return Ok(Some(yielded));
            }
        }

        self.selector.finish(&mut self.output_buffer)?;
        if let Some(last) = self.output_buffer.pop_front() {
            self.yielded_batches.push(last.clone());
            return Ok(Some(last));
        }

        // All last rows in row group are yielded, update cache.
        self.maybe_update_cache();
        Ok(None)
    }

    fn maybe_update_cache(&mut self) {
        if self.yielded_batches.is_empty() {
            return;
        }
        let batches = std::mem::take(&mut self.yielded_batches)
            .into_iter()
            .map(compact_pk_dictionary)
            .collect();
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
/// and contain only PUT operations (no DELETEs).
#[derive(Default)]
pub(crate) struct FlatLastTimestampSelector {
    /// Row indices of last-timestamp rows from completed keys within `last_batch`.
    indices: Vec<u64>,
    /// The batch that `indices` and `last_key_start`/`last_key_len` refer to.
    last_batch: Option<RecordBatch>,
    /// Start offset within `last_batch` for the last (incomplete) primary key range.
    last_key_start: usize,
    /// Length of the last (incomplete) primary key range within `last_batch`.
    last_key_len: usize,
    /// Pending rows for the same key and timestamp across multiple batches.
    pending_batches: Vec<RecordBatch>,
}

impl FlatLastTimestampSelector {
    /// Processes the next batch and appends completed-key results into `output_buffer`.
    pub(crate) fn on_next(
        &mut self,
        batch: RecordBatch,
        output_buffer: &mut VecDeque<RecordBatch>,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let num_columns = batch.num_columns();
        let pk_col_idx = primary_key_column_index(num_columns);
        let ts_col_idx = time_index_column_index(num_columns);

        let Some(batch) =
            self.handle_pending_batches(batch, pk_col_idx, ts_col_idx, output_buffer)?
        else {
            return Ok(());
        };
        debug_assert!(
            self.pending_batches.is_empty(),
            "pending_batches must be empty when handle_pending_batches returns Some"
        );

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
        let first_start = offsets[0];
        let first_len = offsets[1] - first_start;
        let curr_key = primary_key_bytes_at(&batch, pk_col_idx, first_start);
        let curr_first_ts = ts_values[first_start];

        if let Some(last) = &self.last_batch {
            let last_key = primary_key_bytes_at(last, pk_col_idx, self.last_key_start);
            let last_ts = self.last_range_last_timestamp(last, ts_col_idx);
            if last_key == curr_key {
                if last_ts == curr_first_ts
                    && is_single_key_single_timestamp_batch(&batch, pk_col_idx, ts_col_idx)
                {
                    self.pending_batches.push(batch);
                    return Ok(());
                }
            } else {
                // Different key — the previous held range is complete.
                collect_last_timestamp_indices(
                    &mut self.indices,
                    last,
                    ts_col_idx,
                    self.last_key_start,
                    self.last_key_len,
                );
            }
        }
        self.last_key_start = first_start;
        self.last_key_len = first_len;

        // Flush pending indices from the previous batch.
        self.push_flushed(output_buffer)?;

        // Switch to the new batch. From here on, all indices refer to this batch.
        self.last_batch = Some(batch);

        // Process remaining sub-ranges (all within the new batch).
        for i in 1..offsets.len() - 1 {
            let range_start = offsets[i];
            let range_len = offsets[i + 1] - range_start;

            // All ranges after the first are within the same batch, and each
            // boundary means a different key from the previous range.
            collect_last_timestamp_indices(
                &mut self.indices,
                self.last_batch.as_ref().unwrap(),
                ts_col_idx,
                self.last_key_start,
                self.last_key_len,
            );
            self.last_key_start = range_start;
            self.last_key_len = range_len;
        }

        Ok(())
    }

    /// Resolves pending state and returns a batch for normal processing if any.
    fn handle_pending_batches(
        &mut self,
        batch: RecordBatch,
        pk_col_idx: usize,
        ts_col_idx: usize,
        output_buffer: &mut VecDeque<RecordBatch>,
    ) -> Result<Option<RecordBatch>> {
        if self.pending_batches.is_empty() {
            return Ok(Some(batch));
        }

        let pk_array = batch
            .column(pk_col_idx)
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        let offsets = primary_key_offsets(pk_array)?;
        if offsets.is_empty() {
            return Ok(None);
        }

        let first_start = offsets[0];
        let first_end = offsets[1];
        let ts_values = timestamp_array_to_i64_slice(batch.column(ts_col_idx));
        let curr_key = primary_key_bytes_at(&batch, pk_col_idx, first_start);
        let curr_first_ts = ts_values[first_start];
        let curr_first_range_last_ts = ts_values[first_end - 1];
        // Safety: pending_batches is not empty.
        let pending_key =
            primary_key_bytes_at(self.pending_batches.last().unwrap(), pk_col_idx, 0).to_vec();
        // Safety: pending_batches must have last_batch.
        let pending_ts =
            self.last_range_last_timestamp(self.last_batch.as_ref().unwrap(), ts_col_idx);

        if curr_key != pending_key {
            self.flush_and_emit_pending(ts_col_idx, output_buffer)?;
            return Ok(Some(batch));
        }

        if curr_first_range_last_ts > pending_ts {
            // A newer timestamp in current batch supersedes pending rows.
            self.pending_batches.clear();
            return Ok(Some(batch));
        }

        if curr_first_ts == pending_ts {
            if is_single_key_single_timestamp_batch(&batch, pk_col_idx, ts_col_idx) {
                self.pending_batches.push(batch);
                return Ok(None);
            }

            // Same key + timestamp at first range but batch continues with next key.
            let overlap = batch.slice(first_start, first_end - first_start);
            self.pending_batches.push(overlap);
            self.flush_and_emit_pending(ts_col_idx, output_buffer)?;
            let remaining = batch.slice(first_end, batch.num_rows() - first_end);
            return Ok(Some(remaining));
        }

        // If we continue with normal processing, pending rows must not remain.
        self.pending_batches.clear();
        Ok(Some(batch))
    }

    /// Finishes the selector and appends remaining results into `output_buffer`.
    pub(crate) fn finish(&mut self, output_buffer: &mut VecDeque<RecordBatch>) -> Result<()> {
        self.push_flushed(output_buffer)?;

        if !self.pending_batches.is_empty() {
            // Safety: pending_batches must have last_batch.
            let ts_col_idx =
                time_index_column_index(self.last_batch.as_ref().unwrap().num_columns());
            self.emit_pending_and_reset(ts_col_idx, output_buffer)?;
            return Ok(());
        }

        let Some(last) = self.last_batch.take() else {
            return Ok(());
        };

        let ts_col_idx = time_index_column_index(last.num_columns());
        collect_last_timestamp_indices(
            &mut self.indices,
            &last,
            ts_col_idx,
            self.last_key_start,
            self.last_key_len,
        );

        self.last_key_start = 0;
        self.last_key_len = 0;

        if self.indices.is_empty() {
            return Ok(());
        }

        let take_indices = UInt64Array::from(std::mem::take(&mut self.indices));
        let result = take_record_batch(&last, &take_indices).context(ComputeArrowSnafu)?;
        output_buffer.push_back(result);

        Ok(())
    }

    /// Flushes pending indices into a `RecordBatch` via `take`.
    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.indices.is_empty() {
            return Ok(None);
        }

        let last = self
            .last_batch
            .as_ref()
            .expect("indices are non-empty so last_batch must exist");

        let take_indices = UInt64Array::from(std::mem::take(&mut self.indices));
        let result = take_record_batch(last, &take_indices).context(ComputeArrowSnafu)?;

        Ok(Some(result))
    }

    fn push_flushed(&mut self, output_buffer: &mut VecDeque<RecordBatch>) -> Result<()> {
        if let Some(flushed) = self.flush()? {
            output_buffer.push_back(flushed);
        }
        Ok(())
    }

    fn last_range_last_timestamp(&self, batch: &RecordBatch, ts_col_idx: usize) -> i64 {
        let ts_values = timestamp_array_to_i64_slice(batch.column(ts_col_idx));
        ts_values[self.last_key_start + self.last_key_len - 1]
    }

    /// Emits pending rows: last timestamp rows from `last_batch` tail range, then buffered batches.
    fn emit_pending(
        &mut self,
        ts_col_idx: usize,
        output_buffer: &mut VecDeque<RecordBatch>,
    ) -> Result<()> {
        if self.pending_batches.is_empty() {
            return Ok(());
        }

        let last = self
            .last_batch
            .as_ref()
            .expect("pending_batches must have last_batch");
        let pending_from_last =
            take_last_timestamp_rows(last, ts_col_idx, self.last_key_start, self.last_key_len)?;
        output_buffer.push_back(pending_from_last);
        for batch in self.pending_batches.drain(..) {
            output_buffer.push_back(batch);
        }

        Ok(())
    }

    fn emit_pending_and_reset(
        &mut self,
        ts_col_idx: usize,
        output_buffer: &mut VecDeque<RecordBatch>,
    ) -> Result<()> {
        self.emit_pending(ts_col_idx, output_buffer)?;
        self.last_batch = None;
        self.last_key_start = 0;
        self.last_key_len = 0;
        self.indices.clear();
        Ok(())
    }

    fn flush_and_emit_pending(
        &mut self,
        ts_col_idx: usize,
        output_buffer: &mut VecDeque<RecordBatch>,
    ) -> Result<()> {
        self.push_flushed(output_buffer)?;
        self.emit_pending_and_reset(ts_col_idx, output_buffer)?;
        Ok(())
    }
}

fn take_last_timestamp_rows(
    batch: &RecordBatch,
    ts_col_idx: usize,
    key_start: usize,
    key_len: usize,
) -> Result<RecordBatch> {
    let ts_values = timestamp_array_to_i64_slice(batch.column(ts_col_idx));
    let range_end = key_start + key_len;
    let start = last_timestamp_start(ts_values, key_start, range_end);
    Ok(batch.slice(start, range_end - start))
}

fn is_single_key_single_timestamp_batch(
    batch: &RecordBatch,
    pk_col_idx: usize,
    ts_col_idx: usize,
) -> bool {
    if batch.num_rows() == 0 {
        return false;
    }

    let last_row = batch.num_rows() - 1;
    // Input is sorted by primary key and timestamp, so checking first/last rows
    // is sufficient to verify the whole batch has one key and one timestamp.
    if primary_key_bytes_at(batch, pk_col_idx, 0)
        != primary_key_bytes_at(batch, pk_col_idx, last_row)
    {
        return false;
    }

    let ts_values = timestamp_array_to_i64_slice(batch.column(ts_col_idx));
    ts_values[0] == ts_values[last_row]
}

/// Collects indices of last-timestamp rows from a key range into `indices`.
fn collect_last_timestamp_indices(
    indices: &mut Vec<u64>,
    batch: &RecordBatch,
    ts_col_idx: usize,
    last_key_start: usize,
    last_key_len: usize,
) {
    let range_end = last_key_start + last_key_len;
    debug_assert!(range_end <= batch.num_rows());
    debug_assert!(last_key_len > 0);

    let ts_values = timestamp_array_to_i64_slice(batch.column(ts_col_idx));
    let start = last_timestamp_start(ts_values, last_key_start, range_end);
    for idx in start..range_end {
        indices.push(idx as u64);
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

/// Compacts the `__primary_key` dictionary column in a record batch by removing
/// unreferenced values. This reduces memory usage when caching batches whose
/// dictionary values array contains entries not referenced by any key.
///
/// Only compacts when the values array has more than 4 entries to avoid
/// unnecessary work for small dictionaries.
fn compact_pk_dictionary(batch: RecordBatch) -> RecordBatch {
    let pk_idx = primary_key_column_index(batch.num_columns());
    let pk_col = match batch
        .column(pk_idx)
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()
    {
        Some(dict) if dict.values().len() > 4 => dict,
        _ => return batch,
    };

    let old_values = pk_col
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("primary key dictionary values must be BinaryArray");
    let keys = pk_col.keys();

    // Single linear pass: since keys are sorted/grouped, we track when the key changes.
    let mut remap = vec![0u32; old_values.len()];
    let mut new_values: Vec<&[u8]> = Vec::new();
    let mut prev_key: Option<u32> = None;

    for key in keys.iter().flatten() {
        if prev_key != Some(key) {
            let new_index = new_values.len() as u32;
            new_values.push(old_values.value(key as usize));
            remap[key as usize] = new_index;
            prev_key = Some(key);
        }
    }

    if new_values.len() == old_values.len() {
        return batch; // all values are in use
    }

    let new_keys = UInt32Array::from_iter(keys.iter().map(|k| k.map(|v| remap[v as usize])));
    let new_values_array = BinaryArray::from_iter_values(new_values);
    let new_dict = DictionaryArray::new(new_keys, Arc::new(new_values_array));

    let mut columns: Vec<_> = batch.columns().to_vec();
    columns[pk_idx] = Arc::new(new_dict);
    RecordBatch::try_new(batch.schema(), columns).expect("schema should match after compaction")
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
        let mut output_buffer = VecDeque::new();
        let mut results = Vec::new();
        for batch in batches {
            selector.on_next(batch, &mut output_buffer).unwrap();
            while let Some(r) = output_buffer.pop_front() {
                extract_flat_rows(&r, &mut results);
            }
        }
        selector.finish(&mut output_buffer).unwrap();
        while let Some(r) = output_buffer.pop_front() {
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
    fn test_flat_multiple_batches_same_key() {
        let mut selector = FlatLastTimestampSelector::default();
        let batches = vec![
            new_flat_batch(&[b"k1", b"k1"], &[1, 2], &[10, 20]),
            new_flat_batch(&[b"k1", b"k1"], &[3, 4], &[30, 40]),
        ];
        let results = collect_flat_results(&mut selector, batches);
        assert_eq!(vec![(b"k1".to_vec(), 4)], results);
    }

    #[test]
    fn test_flat_multiple_batches_different_keys() {
        let mut selector = FlatLastTimestampSelector::default();
        let batches = vec![
            new_flat_batch(&[b"k1", b"k1"], &[1, 2], &[10, 20]),
            new_flat_batch(&[b"k2", b"k2"], &[3, 4], &[30, 40]),
        ];
        let results = collect_flat_results(&mut selector, batches);
        assert_eq!(vec![(b"k1".to_vec(), 2), (b"k2".to_vec(), 4)], results);
    }

    #[test]
    fn test_flat_single_row() {
        let mut selector = FlatLastTimestampSelector::default();
        let batch = new_flat_batch(&[b"k1"], &[42], &[100]);
        let results = collect_flat_results(&mut selector, vec![batch]);
        assert_eq!(vec![(b"k1".to_vec(), 42)], results);
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
        // The held batch is replaced, so we only get the last-timestamp rows
        // from the final batch for k1 (the row with ts=3 from batch 2).
        assert_eq!(vec![(b"k1".to_vec(), 3), (b"k2".to_vec(), 5)], results);
    }

    #[test]
    fn test_flat_duplicate_last_timestamps_across_three_uniform_batches() {
        let mut selector = FlatLastTimestampSelector::default();
        let batches = vec![
            new_flat_batch(&[b"k1", b"k1"], &[1, 3], &[10, 20]),
            new_flat_batch(&[b"k1", b"k1"], &[3, 3], &[21, 22]),
            new_flat_batch(&[b"k1", b"k1", b"k2"], &[3, 3, 5], &[23, 24, 30]),
        ];
        let results = collect_flat_results(&mut selector, batches);
        assert_eq!(
            vec![
                (b"k1".to_vec(), 3),
                (b"k1".to_vec(), 3),
                (b"k1".to_vec(), 3),
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
        let mut output_buffer = VecDeque::new();

        // Seed the selector with one batch, then simulate EOF by calling finish().
        selector.on_next(batch, &mut output_buffer).unwrap();
        assert!(output_buffer.is_empty());

        selector.finish(&mut output_buffer).unwrap();
        assert!(!output_buffer.is_empty());
        while output_buffer.pop_front().is_some() {}

        // A second finish after EOF should not yield any more rows.
        selector.finish(&mut output_buffer).unwrap();
        assert!(output_buffer.is_empty());
    }

    /// Builds a flat RecordBatch with an explicit dictionary whose values array
    /// contains entries not referenced by any key (simulating a sliced batch).
    /// `all_values` is the full dictionary values array; `key_indices` maps each
    /// row to an index in `all_values`.
    fn new_flat_batch_with_dict(
        all_values: &[&[u8]],
        key_indices: &[u32],
        timestamps: &[i64],
        fields: &[i64],
    ) -> RecordBatch {
        let num_rows = key_indices.len();
        assert_eq!(timestamps.len(), num_rows);
        assert_eq!(fields.len(), num_rows);

        let keys = UInt32Array::from_iter_values(key_indices.iter().copied());
        let values = BinaryArray::from_iter_values(all_values.iter().copied());
        let dict = DictionaryArray::new(keys, Arc::new(values));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(fields.iter().copied())),
            Arc::new(TimestampMillisecondArray::from_iter_values(
                timestamps.iter().copied(),
            )),
            Arc::new(dict),
            Arc::new(UInt64Array::from_iter_values(vec![1u64; num_rows])),
            Arc::new(UInt8Array::from_iter_values(vec![1u8; num_rows])),
        ];

        RecordBatch::try_new(test_flat_schema(), columns).unwrap()
    }

    /// Helper: returns the number of entries in the dictionary values array.
    fn pk_dict_values_len(batch: &RecordBatch) -> usize {
        let pk_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap();
        pk_col.values().len()
    }

    #[test]
    fn test_compact_pk_dictionary_removes_unreferenced() {
        // Dictionary has 6 values but only indices 1 and 4 are referenced.
        let all_values: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e", b"f"];
        let batch = new_flat_batch_with_dict(&all_values, &[1, 1, 4], &[10, 20, 30], &[1, 2, 3]);
        assert_eq!(6, pk_dict_values_len(&batch));

        let compacted = compact_pk_dictionary(batch);
        // Only 2 referenced values should remain.
        assert_eq!(2, pk_dict_values_len(&compacted));
        assert_eq!(3, compacted.num_rows());

        // Verify data is still correct by reading back pk bytes.
        let mut rows = Vec::new();
        extract_flat_rows(&compacted, &mut rows);
        assert_eq!(
            vec![
                (b"b".to_vec(), 10),
                (b"b".to_vec(), 20),
                (b"e".to_vec(), 30),
            ],
            rows
        );
    }

    #[test]
    fn test_compact_pk_dictionary_skips_small() {
        // Dictionary has 4 values — should not be compacted (threshold is >4).
        let all_values: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];
        let batch = new_flat_batch_with_dict(&all_values, &[0, 3], &[10, 20], &[1, 2]);
        assert_eq!(4, pk_dict_values_len(&batch));

        let compacted = compact_pk_dictionary(batch);
        // Unchanged — still 4 values.
        assert_eq!(4, pk_dict_values_len(&compacted));
    }

    #[test]
    fn test_compact_pk_dictionary_all_referenced() {
        // All 5 values are referenced — nothing to compact.
        let all_values: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
        let batch =
            new_flat_batch_with_dict(&all_values, &[0, 1, 2, 3, 4], &[1, 2, 3, 4, 5], &[1; 5]);
        assert_eq!(5, pk_dict_values_len(&batch));

        let compacted = compact_pk_dictionary(batch);
        assert_eq!(5, pk_dict_values_len(&compacted));

        let mut rows = Vec::new();
        extract_flat_rows(&compacted, &mut rows);
        assert_eq!(5, rows.len());
        assert_eq!(b"a".to_vec(), rows[0].0);
        assert_eq!(b"e".to_vec(), rows[4].0);
    }

    #[test]
    fn test_compact_pk_dictionary_preserves_schema() {
        let all_values: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e", b"f"];
        let batch = new_flat_batch_with_dict(&all_values, &[2, 5], &[10, 20], &[1, 2]);
        let original_schema = batch.schema();

        let compacted = compact_pk_dictionary(batch);
        assert_eq!(original_schema, compacted.schema());
        assert_eq!(2, pk_dict_values_len(&compacted));
    }

    #[test]
    fn test_compact_pk_dictionary_auto_deduped_batch() {
        // A batch built via BinaryDictionaryBuilder already has a minimal dictionary.
        // compact_pk_dictionary should be a no-op (values <= 4 or all referenced).
        let batch = new_flat_batch(&[b"k1", b"k1", b"k2"], &[1, 2, 3], &[10, 20, 30]);
        let values_before = pk_dict_values_len(&batch);

        let compacted = compact_pk_dictionary(batch);
        assert_eq!(values_before, pk_dict_values_len(&compacted));
    }
}
