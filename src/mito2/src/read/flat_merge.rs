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

#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_telemetry::debug;
use datatypes::arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, Int64Array, UInt32Array, UInt64Array,
};
use datatypes::arrow::compute::interleave;
use datatypes::arrow::datatypes::{ArrowNativeType, BinaryType, DataType, SchemaRef, Utf8Type};
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::arrow_array::BinaryArray;
use datatypes::timestamp::timestamp_array_to_primitive;
use futures::{Stream, TryStreamExt};
use snafu::ResultExt;
use store_api::storage::SequenceNumber;

use crate::error::{ComputeArrowSnafu, Result};
use crate::memtable::BoxedRecordBatchIterator;
use crate::metrics::READ_STAGE_ELAPSED;
use crate::read::BoxedRecordBatchStream;
use crate::sst::parquet::flat_format::{
    primary_key_column_index, sequence_column_index, time_index_column_index,
};
use crate::sst::parquet::format::PrimaryKeyArray;

/// Checks whether interleaving the selected rows from byte columns would overflow
/// i32 offsets. Similar to arrow-rs `interleave_bytes()`, accumulates offsets and
/// returns an error if the capacity exceeds `i32::MAX`.
///
/// TODO(yingwen): Remove this after upgrading to arrow >= 58.1.0, which handles
/// offset overflow in `interleave_bytes()` natively.
///
/// See: <https://github.com/apache/arrow-rs/blob/65ad652f2410fc51ad77da1805e85c0a76d9a7ea/arrow-select/src/interleave.rs#L208-L225>
fn check_interleave_bytes_overflow<T: datatypes::arrow::datatypes::ByteArrayType>(
    batches: &[(usize, RecordBatch)],
    col_idx: usize,
    indices: &[(usize, usize)],
) -> std::result::Result<(), ArrowError> {
    // Quick check: if concatenating all value data won't overflow, interleaving
    // a subset of rows definitely won't either.
    let total: usize = batches
        .iter()
        .map(|(_, batch)| batch.column(col_idx).as_bytes::<T>().value_data().len())
        .sum();
    if T::Offset::from_usize(total).is_some() {
        return Ok(());
    }
    // Total exceeds the offset limit, do the precise per-row check.
    let mut capacity: usize = 0;
    for &(a, b) in indices {
        let array = batches[a].1.column(col_idx).as_bytes::<T>();
        let o = array.value_offsets();
        let element_len = o[b + 1].as_usize() - o[b].as_usize();
        capacity += element_len;
        T::Offset::from_usize(capacity).ok_or(ArrowError::OffsetOverflowError(capacity))?;
    }
    Ok(())
}

/// Checks whether `interleave()` would overflow i32 offsets for `Utf8` or `Binary` columns.
fn check_interleave_overflow(
    batches: &[(usize, RecordBatch)],
    schema: &SchemaRef,
    indices: &[(usize, usize)],
) -> Result<()> {
    for (col_idx, field) in schema.fields.iter().enumerate() {
        match field.data_type() {
            DataType::Utf8 => {
                check_interleave_bytes_overflow::<Utf8Type>(batches, col_idx, indices)
                    .context(ComputeArrowSnafu)?;
            }
            DataType::Binary => {
                check_interleave_bytes_overflow::<BinaryType>(batches, col_idx, indices)
                    .context(ComputeArrowSnafu)?;
            }
            _ => continue,
        }
    }
    Ok(())
}

/// Interleaves the non-null internal primary-key column from globally sorted rows.
fn interleave_primary_key(
    arrays: &[&dyn Array],
    indices: &[(usize, usize)],
) -> std::result::Result<ArrayRef, ArrowError> {
    if arrays.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "interleave requires input of at least one array".to_string(),
        ));
    }

    let dictionaries = arrays
        .iter()
        .map(|array| {
            let dictionary = array
                .as_any()
                .downcast_ref::<PrimaryKeyArray>()
                .ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "expected Dictionary(UInt32, Binary) primary key, got {}",
                        array.data_type()
                    ))
                })?;
            let values = dictionary
                .values()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "expected Binary primary-key dictionary values, got {}",
                        dictionary.values().data_type()
                    ))
                })?;
            Ok((dictionary, values))
        })
        .collect::<std::result::Result<Vec<_>, ArrowError>>()?;

    let mut keys = Vec::with_capacity(indices.len());
    let mut values = BinaryBuilder::with_capacity(indices.len(), 0);
    let mut previous_primary_key = None;
    let mut current_key = 0;
    let mut num_dictionary_values = 0_usize;
    let mut value_bytes = 0_usize;

    for &(array_idx, row_idx) in indices {
        let (dictionary, dictionary_values) = dictionaries.get(array_idx).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "primary-key source index {array_idx} is out of bounds for {} arrays",
                dictionaries.len()
            ))
        })?;
        if row_idx >= dictionary.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "primary-key row index {row_idx} is out of bounds for array of length {}",
                dictionary.len()
            )));
        }
        let source_key = dictionary.key(row_idx).ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "internal primary-key dictionary contains a null key".to_string(),
            )
        })?;
        if dictionary_values.is_null(source_key) {
            return Err(ArrowError::InvalidArgumentError(
                "internal primary-key dictionary contains a null dictionary value".to_string(),
            ));
        }
        let primary_key = dictionary_values.value(source_key);

        if previous_primary_key != Some(primary_key) {
            current_key = u32::try_from(num_dictionary_values)
                .map_err(|_| ArrowError::DictionaryKeyOverflowError)?;
            value_bytes = value_bytes.checked_add(primary_key.len()).ok_or_else(|| {
                ArrowError::ArithmeticOverflow(
                    "primary-key dictionary value length overflow".to_string(),
                )
            })?;
            if value_bytes > i32::MAX as usize {
                return Err(ArrowError::OffsetOverflowError(value_bytes));
            }
            values.append_value(primary_key);
            num_dictionary_values += 1;
            previous_primary_key = Some(primary_key);
        }
        keys.push(current_key);
    }

    let dictionary = PrimaryKeyArray::try_new(UInt32Array::from(keys), Arc::new(values.finish()))?;
    Ok(Arc::new(dictionary))
}

/// Keeps track of the current position in a batch
#[derive(Debug, Copy, Clone, Default)]
struct BatchCursor {
    /// The index into BatchBuilder::batches
    batch_idx: usize,
    /// The row index within the given batch
    row_idx: usize,
}

/// Trait for reporting merge metrics.
pub trait MergeMetricsReport: Send + Sync {
    /// Reports and resets the metrics.
    fn report(&self, metrics: &mut MergeMetrics);
}

/// Metrics for the merge reader.
#[derive(Default)]
pub struct MergeMetrics {
    /// Cost to initialize the reader.
    pub(crate) init_cost: Duration,
    /// Total scan cost of the reader.
    pub(crate) scan_cost: Duration,
    /// Number of times to fetch batches.
    pub(crate) num_fetch_by_batches: usize,
    /// Number of times to fetch rows.
    pub(crate) num_fetch_by_rows: usize,
    /// Cost to fetch batches from sources.
    pub(crate) fetch_cost: Duration,
}

impl fmt::Debug for MergeMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scan_cost.is_zero() {
            return write!(f, "{{}}");
        }

        write!(f, r#"{{"scan_cost":"{:?}""#, self.scan_cost)?;

        if !self.init_cost.is_zero() {
            write!(f, r#", "init_cost":"{:?}""#, self.init_cost)?;
        }
        if self.num_fetch_by_batches > 0 {
            write!(
                f,
                r#", "num_fetch_by_batches":{}"#,
                self.num_fetch_by_batches
            )?;
        }
        if self.num_fetch_by_rows > 0 {
            write!(f, r#", "num_fetch_by_rows":{}"#, self.num_fetch_by_rows)?;
        }
        if !self.fetch_cost.is_zero() {
            write!(f, r#", "fetch_cost":"{:?}""#, self.fetch_cost)?;
        }

        write!(f, "}}")
    }
}

impl MergeMetrics {
    /// Merges metrics from another MergeMetrics instance.
    pub(crate) fn merge(&mut self, other: &MergeMetrics) {
        let MergeMetrics {
            init_cost,
            scan_cost,
            num_fetch_by_batches,
            num_fetch_by_rows,
            fetch_cost,
        } = other;

        self.init_cost += *init_cost;
        self.scan_cost += *scan_cost;
        self.num_fetch_by_batches += *num_fetch_by_batches;
        self.num_fetch_by_rows += *num_fetch_by_rows;
        self.fetch_cost += *fetch_cost;
    }

    /// Reports the metrics if scan_cost exceeds 10ms and resets them.
    pub(crate) fn maybe_report(&mut self, reporter: &Option<Arc<dyn MergeMetricsReport>>) {
        if self.scan_cost.as_millis() > 10
            && let Some(r) = reporter
        {
            r.report(self);
        }
    }
}

/// Provides an API to incrementally build a [`RecordBatch`] from partitioned [`RecordBatch`]
// Ports from https://github.com/apache/datafusion/blob/49.0.0/datafusion/physical-plan/src/sorts/builder.rs
// Adds the `take_remaining_rows()` method.
#[derive(Debug)]
pub struct BatchBuilder {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// Maintain a list of [`RecordBatch`] and their corresponding stream
    batches: Vec<(usize, RecordBatch)>,

    /// The current [`BatchCursor`] for each stream
    cursors: Vec<BatchCursor>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_idx, row_idx)`
    indices: Vec<(usize, usize)>,
}

impl BatchBuilder {
    /// Create a new [`BatchBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(schema: SchemaRef, stream_count: usize, batch_size: usize) -> Self {
        Self {
            schema,
            batches: Vec::with_capacity(stream_count * 2),
            cursors: vec![BatchCursor::default(); stream_count],
            indices: Vec::with_capacity(batch_size),
        }
    }

    /// Append a new batch in `stream_idx`
    pub fn push_batch(&mut self, stream_idx: usize, batch: RecordBatch) {
        let batch_idx = self.batches.len();
        self.batches.push((stream_idx, batch));
        self.cursors[stream_idx] = BatchCursor {
            batch_idx,
            row_idx: 0,
        };
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize) {
        let cursor = &mut self.cursors[stream_idx];
        let row_idx = cursor.row_idx;
        cursor.row_idx += 1;
        self.indices.push((cursor.batch_idx, row_idx));
    }

    /// Returns the number of in-progress rows in this [`BatchBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`BatchBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Returns the schema of this [`BatchBuilder`]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output
    ///
    /// Returns `None` if no pending rows
    pub fn build_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }

        check_interleave_overflow(&self.batches, &self.schema, &self.indices)?;

        let primary_key_column_idx = (self.schema.fields.len() >= 3)
            .then(|| primary_key_column_index(self.schema.fields.len()));
        let columns = (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = self
                    .batches
                    .iter()
                    .map(|(_, batch)| batch.column(column_idx).as_ref())
                    .collect();
                if Some(column_idx) == primary_key_column_idx {
                    interleave_primary_key(&arrays, &self.indices).context(ComputeArrowSnafu)
                } else {
                    interleave(&arrays, &self.indices).context(ComputeArrowSnafu)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        self.indices.clear();

        // New cursors are only created once the previous cursor for the stream
        // is finished. This means all remaining rows from all but the last batch
        // for each stream have been yielded to the newly created record batch
        //
        // We can therefore drop all but the last batch for each stream
        self.retain_batches();

        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .context(ComputeArrowSnafu)
            .map(Some)
    }

    /// Slice and take remaining rows from the last batch of `stream_idx` and push
    /// the next batch if available.
    pub fn take_remaining_rows(
        &mut self,
        stream_idx: usize,
        next: Option<RecordBatch>,
    ) -> RecordBatch {
        let cursor = &mut self.cursors[stream_idx];
        let batch = &self.batches[cursor.batch_idx];
        let output = batch
            .1
            .slice(cursor.row_idx, batch.1.num_rows() - cursor.row_idx);
        cursor.row_idx = batch.1.num_rows();

        if let Some(b) = next {
            self.push_batch(stream_idx, b);
            self.retain_batches();
        }

        output
    }

    fn retain_batches(&mut self) {
        let mut batch_idx = 0;
        let mut retained = 0;
        self.batches.retain(|(stream_idx, _)| {
            let stream_cursor = &mut self.cursors[*stream_idx];
            let retain = stream_cursor.batch_idx == batch_idx;
            batch_idx += 1;

            if retain {
                stream_cursor.batch_idx = retained;
                retained += 1;
            }
            retain
        });
    }
}

struct RootHeap<T: Ord> {
    data: Vec<T>,
}

impl<T: Ord> RootHeap<T> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn peek(&self) -> Option<&T> {
        self.data.first()
    }

    fn root_mut(&mut self) -> Option<&mut T> {
        self.data.first_mut()
    }

    fn best_child(&self) -> Option<&T> {
        match self.data.len() {
            0 | 1 => None,
            2 => self.data.get(1),
            _ if self.data[2] > self.data[1] => self.data.get(2),
            _ => self.data.get(1),
        }
    }

    fn push(&mut self, value: T) {
        self.data.push(value);
        self.sift_up(self.data.len() - 1);
    }

    fn pop(&mut self) -> Option<T> {
        let last = self.data.pop()?;
        if self.data.is_empty() {
            return Some(last);
        }

        let root = std::mem::replace(&mut self.data[0], last);
        self.sift_down(0);
        Some(root)
    }

    fn repair_root(&mut self) {
        self.sift_down(0);
    }

    fn sift_up(&mut self, mut index: usize) {
        while index > 0 {
            let parent = (index - 1) / 2;
            if self.data[parent] >= self.data[index] {
                break;
            }
            self.data.swap(parent, index);
            index = parent;
        }
    }

    fn sift_down(&mut self, mut index: usize) {
        loop {
            let left = index * 2 + 1;
            if left >= self.data.len() {
                break;
            }

            let right = left + 1;
            let child = if right < self.data.len() && self.data[right] > self.data[left] {
                right
            } else {
                left
            };
            if self.data[index] >= self.data[child] {
                break;
            }
            self.data.swap(index, child);
            index = child;
        }
    }
}

/// A comparable node of the heap.
trait NodeCmp: Eq + Ord {
    /// Returns whether the node still has batch to read.
    fn is_eof(&self) -> bool;

    /// Returns true if the key range of current batch in `self` is behind (exclusive) current
    /// batch in `other`.
    ///
    /// # Panics
    /// Panics if either `self` or `other` is EOF.
    fn is_behind(&self, other: &Self) -> bool;
}

/// Common algorithm of merging sorted batches from multiple nodes.
struct MergeAlgo<T: Ord> {
    /// Holds nodes whose key range of current batch **is** overlapped with the merge window.
    /// Each node yields batches from a `source`.
    ///
    /// Node in this heap **MUST** not be empty. A `merge window` is the (primary key, timestamp)
    /// range of the **root node** in the `hot` heap.
    hot: RootHeap<T>,
    /// Holds nodes whose key range of current batch **isn't** overlapped with the merge window.
    ///
    /// Nodes in this heap **MUST** not be empty.
    cold: BinaryHeap<T>,
}

impl<T: NodeCmp> MergeAlgo<T> {
    /// Creates a new merge algorithm from `nodes`.
    ///
    /// All nodes must be initialized.
    fn new(mut nodes: Vec<T>) -> Self {
        // Skips EOF nodes.
        nodes.retain(|node| !node.is_eof());
        let hot = RootHeap::with_capacity(nodes.len());
        let cold = BinaryHeap::from(nodes);

        let mut algo = MergeAlgo { hot, cold };
        // Initializes the algorithm.
        algo.refill_hot();

        algo
    }

    /// Moves nodes in `cold` heap, whose key range is overlapped with current merge
    /// window to `hot` heap.
    fn refill_hot(&mut self) {
        while !self.cold.is_empty() {
            if let Some(merge_window) = self.hot.peek() {
                let warmest = self.cold.peek().unwrap();
                if warmest.is_behind(merge_window) {
                    // if the warmest node in the `cold` heap is totally after the
                    // `merge_window`, then no need to add more nodes into the `hot`
                    // heap for merge sorting.
                    break;
                }
            }

            let warmest = self.cold.pop().unwrap();
            self.hot.push(warmest);
        }
    }

    /// Returns the hottest node mutably.
    fn hottest_mut(&mut self) -> Option<&mut T> {
        self.hot.root_mut()
    }

    /// Removes the hottest node before a transition that can fetch a batch.
    fn pop_hot_for_batch_transition(&mut self) -> Option<T> {
        self.hot.pop()
    }

    /// Returns a node to the appropriate heap after a batch transition.
    fn reheap_after_batch_transition(&mut self, node: T) {
        if node.is_eof() {
            self.refill_hot();
            return;
        }

        let node_is_cold = self
            .hot
            .peek()
            .is_none_or(|hottest| node.is_behind(hottest));
        if node_is_cold {
            self.cold.push(node);
        } else {
            self.hot.push(node);
        }
        self.refill_hot();
    }

    /// Repairs the hot heap after mutating its root and refills the merge window.
    fn repair_hot_root(&mut self) {
        if self.hot.peek().is_some_and(NodeCmp::is_eof) {
            self.hot.pop();
        } else {
            let root_is_cold = self
                .hot
                .best_child()
                .is_some_and(|best| self.hot.peek().unwrap().is_behind(best));
            if root_is_cold {
                self.cold.push(self.hot.pop().unwrap());
            } else {
                self.hot.repair_root();
            }
        }

        self.refill_hot();
    }

    /// Returns true if there are rows in the hot heap.
    fn has_rows(&self) -> bool {
        !self.hot.is_empty()
    }

    /// Returns true if we can fetch a batch directly instead of a row.
    fn can_fetch_batch(&self) -> bool {
        self.hot.len() == 1
    }
}

// TODO(yingwen): Further downcast and store arrays in this struct.
/// Columns to compare for a [RecordBatch].
struct SortColumns {
    primary_key: PrimaryKeyArray,
    primary_key_values: BinaryArray,
    timestamp: Int64Array,
    sequence: UInt64Array,
    #[cfg(test)]
    primary_key_lookups: Cell<usize>,
}

impl SortColumns {
    /// Creates a new [SortColumns] from a [RecordBatch] and the position of the time index column.
    ///
    /// # Panics
    /// Panics if the input batch doesn't have correct internal columns.
    fn new(batch: &RecordBatch) -> Self {
        let num_columns = batch.num_columns();
        let primary_key = batch
            .column(primary_key_column_index(num_columns))
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .unwrap()
            .clone();
        let primary_key_values = primary_key
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .clone();
        let timestamp = batch.column(time_index_column_index(num_columns));
        let (timestamp, _unit) = timestamp_array_to_primitive(timestamp).unwrap();
        let sequence = batch
            .column(sequence_column_index(num_columns))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .clone();

        Self {
            primary_key,
            primary_key_values,
            timestamp,
            sequence,
            #[cfg(test)]
            primary_key_lookups: Cell::new(0),
        }
    }

    fn primary_key_at(&self, index: usize) -> &[u8] {
        let range = self.primary_key_range_at(index);
        &self.primary_key_values.value_data()[range]
    }

    fn primary_key_range_at(&self, index: usize) -> Range<usize> {
        #[cfg(test)]
        self.primary_key_lookups
            .set(self.primary_key_lookups.get() + 1);
        let key = self.primary_key.keys().value(index) as usize;
        let offsets = self.primary_key_values.value_offsets();
        offsets[key].as_usize()..offsets[key + 1].as_usize()
    }

    #[cfg(test)]
    fn primary_key_lookups(&self) -> usize {
        self.primary_key_lookups.get()
    }

    fn timestamp_at(&self, index: usize) -> i64 {
        self.timestamp.value(index)
    }

    fn sequence_at(&self, index: usize) -> SequenceNumber {
        self.sequence.value(index)
    }

    fn num_rows(&self) -> usize {
        self.timestamp.len()
    }
}

/// Cursor to a row in the [RecordBatch].
///
/// It compares batches by rows. During comparison, it ignores op type as sequence is enough to
/// distinguish different rows.
struct RowCursor {
    /// Current row offset.
    offset: usize,
    /// Byte range of the current primary key in the dictionary values.
    primary_key_range: Range<usize>,
    /// Keys of the batch.
    columns: SortColumns,
}

impl RowCursor {
    fn new(columns: SortColumns) -> Self {
        debug_assert!(columns.num_rows() > 0);
        let primary_key_range = columns.primary_key_range_at(0);

        Self {
            offset: 0,
            primary_key_range,
            columns,
        }
    }

    fn is_finished(&self) -> bool {
        self.offset >= self.columns.num_rows()
    }

    /// Returns whether advancing this cursor will finish the current batch.
    fn is_last_row(&self) -> bool {
        self.offset.checked_add(1) == Some(self.columns.num_rows())
    }

    fn advance(&mut self) {
        self.offset += 1;
        if !self.is_finished() {
            self.primary_key_range = self.columns.primary_key_range_at(self.offset);
        }
    }

    fn first_primary_key(&self) -> &[u8] {
        &self.columns.primary_key_values.value_data()[self.primary_key_range.clone()]
    }

    fn first_timestamp(&self) -> i64 {
        self.columns.timestamp_at(self.offset)
    }

    fn first_sequence(&self) -> SequenceNumber {
        self.columns.sequence_at(self.offset)
    }

    fn last_primary_key(&self) -> &[u8] {
        self.columns.primary_key_at(self.columns.num_rows() - 1)
    }

    fn last_timestamp(&self) -> i64 {
        self.columns.timestamp_at(self.columns.num_rows() - 1)
    }
}

impl PartialEq for RowCursor {
    fn eq(&self, other: &Self) -> bool {
        self.first_primary_key() == other.first_primary_key()
            && self.first_timestamp() == other.first_timestamp()
            && self.first_sequence() == other.first_sequence()
    }
}

impl Eq for RowCursor {}

impl PartialOrd for RowCursor {
    fn partial_cmp(&self, other: &RowCursor) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowCursor {
    /// Compares by primary key, time index, sequence desc.
    fn cmp(&self, other: &RowCursor) -> Ordering {
        self.first_primary_key()
            .cmp(other.first_primary_key())
            .then_with(|| self.first_timestamp().cmp(&other.first_timestamp()))
            .then_with(|| other.first_sequence().cmp(&self.first_sequence()))
    }
}

/// Iterator to merge multiple sorted iterators into a single sorted iterator.
///
/// All iterators must be sorted by primary key, time index, sequence desc.
pub struct FlatMergeIterator {
    /// The merge algorithm to maintain heaps.
    algo: MergeAlgo<IterNode>,
    /// Current buffered rows to output.
    in_progress: BatchBuilder,
    /// Non-empty batch to output.
    output_batch: Option<RecordBatch>,
    /// Batch size to merge rows.
    /// This is not a hard limit, the iterator may return smaller batches to avoid concatenating
    /// rows.
    batch_size: usize,
}

impl FlatMergeIterator {
    /// Creates a new iterator to merge sorted `iters`.
    pub fn new(
        schema: SchemaRef,
        iters: Vec<BoxedRecordBatchIterator>,
        batch_size: usize,
    ) -> Result<Self> {
        let mut in_progress = BatchBuilder::new(schema, iters.len(), batch_size);
        let mut nodes = Vec::with_capacity(iters.len());
        // Initialize nodes and the buffer.
        for (node_index, iter) in iters.into_iter().enumerate() {
            let mut node = IterNode {
                node_index,
                iter,
                cursor: None,
            };
            if let Some(batch) = node.advance_batch()? {
                in_progress.push_batch(node_index, batch);
                nodes.push(node);
            }
        }

        let algo = MergeAlgo::new(nodes);

        let iter = Self {
            algo,
            in_progress,
            output_batch: None,
            batch_size,
        };

        Ok(iter)
    }

    /// Fetches next sorted batch.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while self.algo.has_rows() && self.output_batch.is_none() {
            if self.algo.can_fetch_batch() && !self.in_progress.is_empty() {
                // Only one batch in the hot heap, but we have pending rows, output the pending rows first.
                self.output_batch = self.in_progress.build_record_batch()?;
                debug_assert!(self.output_batch.is_some());
            } else if self.algo.can_fetch_batch() {
                self.fetch_batch_from_hottest()?;
            } else {
                self.fetch_row_from_hottest()?;
            }
        }

        Ok(self.output_batch.take())
    }

    /// Fetches a batch from the hottest node.
    fn fetch_batch_from_hottest(&mut self) -> Result<()> {
        debug_assert!(self.in_progress.is_empty());

        // Safety: next_batch() ensures the heap is not empty.
        let mut hottest = self.algo.pop_hot_for_batch_transition().unwrap();
        debug_assert!(!hottest.current_cursor().is_finished());
        let node_index = hottest.node_index;
        let next = hottest.advance_batch()?;
        // The node is the heap is not empty, so it must have existing rows in the builder.
        let batch = self.in_progress.take_remaining_rows(node_index, next);
        Self::maybe_output_batch(batch, &mut self.output_batch);
        self.algo.reheap_after_batch_transition(hottest);

        Ok(())
    }

    /// Fetches a row from the hottest node.
    fn fetch_row_from_hottest(&mut self) -> Result<()> {
        let (node_index, at_batch_boundary) = {
            // Safety: next_batch() ensures the heap has more than 1 element.
            let hottest = self.algo.hottest_mut().unwrap();
            debug_assert!(!hottest.current_cursor().is_finished());
            (hottest.node_index, hottest.current_cursor().is_last_row())
        };
        let mut boundary_node =
            at_batch_boundary.then(|| self.algo.pop_hot_for_batch_transition().unwrap());
        self.in_progress.push_row(node_index);
        if self.in_progress.len() >= self.batch_size {
            // We buffered enough rows.
            if let Some(output) = self.in_progress.build_record_batch()? {
                Self::maybe_output_batch(output, &mut self.output_batch);
            }
        }

        let next = if let Some(hottest) = &mut boundary_node {
            hottest.advance_row()?
        } else {
            self.algo.hottest_mut().unwrap().advance_row()?
        };
        if let Some(next) = next {
            self.in_progress.push_batch(node_index, next);
        }

        if let Some(hottest) = boundary_node {
            self.algo.reheap_after_batch_transition(hottest);
        } else {
            self.algo.repair_hot_root();
        }
        Ok(())
    }

    /// Adds the batch to the output batch if it is not empty.
    fn maybe_output_batch(batch: RecordBatch, output_batch: &mut Option<RecordBatch>) {
        debug_assert!(output_batch.is_none());
        if batch.num_rows() > 0 {
            *output_batch = Some(batch);
        }
    }
}

impl Iterator for FlatMergeIterator {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

/// Iterator to merge multiple sorted iterators into a single sorted iterator.
///
/// All iterators must be sorted by primary key, time index, sequence desc.
pub struct FlatMergeReader {
    /// The merge algorithm to maintain heaps.
    algo: MergeAlgo<StreamNode>,
    /// Current buffered rows to output.
    in_progress: BatchBuilder,
    /// Non-empty batch to output.
    output_batch: Option<RecordBatch>,
    /// Batch size to merge rows.
    /// This is not a hard limit, the iterator may return smaller batches to avoid concatenating
    /// rows.
    batch_size: usize,
    /// Local metrics.
    metrics: MergeMetrics,
    /// Optional metrics reporter.
    metrics_reporter: Option<Arc<dyn MergeMetricsReport>>,
}

impl FlatMergeReader {
    /// Creates a new iterator to merge sorted `iters`.
    pub async fn new(
        schema: SchemaRef,
        iters: Vec<BoxedRecordBatchStream>,
        batch_size: usize,
        metrics_reporter: Option<Arc<dyn MergeMetricsReport>>,
    ) -> Result<Self> {
        let start = Instant::now();
        let metrics = MergeMetrics::default();
        let mut in_progress = BatchBuilder::new(schema, iters.len(), batch_size);
        let mut nodes = Vec::with_capacity(iters.len());
        // Initialize nodes and the buffer.
        for (node_index, iter) in iters.into_iter().enumerate() {
            let mut node = StreamNode {
                node_index,
                iter,
                cursor: None,
            };
            if let Some(batch) = node.advance_batch().await? {
                in_progress.push_batch(node_index, batch);
                nodes.push(node);
            }
        }

        let algo = MergeAlgo::new(nodes);

        let mut reader = Self {
            algo,
            in_progress,
            output_batch: None,
            batch_size,
            metrics,
            metrics_reporter,
        };
        let elapsed = start.elapsed();
        reader.metrics.init_cost += elapsed;
        reader.metrics.scan_cost += elapsed;

        Ok(reader)
    }

    /// Fetches next sorted batch.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let start = Instant::now();
        while self.algo.has_rows() && self.output_batch.is_none() {
            if self.algo.can_fetch_batch() && !self.in_progress.is_empty() {
                // Only one batch in the hot heap, but we have pending rows, output the pending rows first.
                self.output_batch = self.in_progress.build_record_batch()?;
                debug_assert!(self.output_batch.is_some());
            } else if self.algo.can_fetch_batch() {
                self.fetch_batch_from_hottest().await?;
                self.metrics.num_fetch_by_batches += 1;
            } else {
                self.fetch_row_from_hottest().await?;
                self.metrics.num_fetch_by_rows += 1;
            }
        }

        if let Some(batch) = self.output_batch.take() {
            self.metrics.scan_cost += start.elapsed();
            self.metrics.maybe_report(&self.metrics_reporter);
            Ok(Some(batch))
        } else {
            // No more batches.
            self.metrics.scan_cost += start.elapsed();
            self.metrics.maybe_report(&self.metrics_reporter);
            Ok(None)
        }
    }

    /// Converts the reader into a stream.
    pub fn into_stream(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        try_stream! {
            while let Some(batch) = self.next_batch().await? {
                yield batch;
            }
        }
    }

    /// Fetches a batch from the hottest node.
    async fn fetch_batch_from_hottest(&mut self) -> Result<()> {
        debug_assert!(self.in_progress.is_empty());

        // Safety: next_batch() ensures the heap is not empty.
        let mut hottest = self.algo.pop_hot_for_batch_transition().unwrap();
        debug_assert!(!hottest.current_cursor().is_finished());
        let node_index = hottest.node_index;
        let start = Instant::now();
        let next = hottest.advance_batch().await?;
        self.metrics.fetch_cost += start.elapsed();
        // The node is the heap is not empty, so it must have existing rows in the builder.
        let batch = self.in_progress.take_remaining_rows(node_index, next);
        Self::maybe_output_batch(batch, &mut self.output_batch);
        self.algo.reheap_after_batch_transition(hottest);

        Ok(())
    }

    /// Fetches a row from the hottest node.
    async fn fetch_row_from_hottest(&mut self) -> Result<()> {
        let (node_index, at_batch_boundary) = {
            // Safety: next_batch() ensures the heap has more than 1 element.
            let hottest = self.algo.hottest_mut().unwrap();
            debug_assert!(!hottest.current_cursor().is_finished());
            (hottest.node_index, hottest.current_cursor().is_last_row())
        };
        let mut boundary_node =
            at_batch_boundary.then(|| self.algo.pop_hot_for_batch_transition().unwrap());
        self.in_progress.push_row(node_index);
        if self.in_progress.len() >= self.batch_size {
            // We buffered enough rows.
            if let Some(output) = self.in_progress.build_record_batch()? {
                Self::maybe_output_batch(output, &mut self.output_batch);
            }
        }

        let start = at_batch_boundary.then(Instant::now);
        let next = if let Some(hottest) = &mut boundary_node {
            hottest.advance_row().await?
        } else {
            self.algo.hottest_mut().unwrap().advance_row().await?
        };
        if let Some(start) = start {
            self.metrics.fetch_cost += start.elapsed();
        }
        if let Some(next) = next {
            self.in_progress.push_batch(node_index, next);
        }

        if let Some(hottest) = boundary_node {
            self.algo.reheap_after_batch_transition(hottest);
        } else {
            self.algo.repair_hot_root();
        }
        Ok(())
    }

    /// Adds the batch to the output batch if it is not empty.
    fn maybe_output_batch(batch: RecordBatch, output_batch: &mut Option<RecordBatch>) {
        debug_assert!(output_batch.is_none());
        if batch.num_rows() > 0 {
            *output_batch = Some(batch);
        }
    }
}

impl Drop for FlatMergeReader {
    fn drop(&mut self) {
        debug!("Flat merge reader finished, metrics: {:?}", self.metrics);

        READ_STAGE_ELAPSED
            .with_label_values(&["flat_merge"])
            .observe(self.metrics.scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["flat_merge_fetch"])
            .observe(self.metrics.fetch_cost.as_secs_f64());

        // Report any remaining metrics.
        if let Some(reporter) = &self.metrics_reporter {
            reporter.report(&mut self.metrics);
        }
    }
}

/// A sync node in the merge iterator.
struct GenericNode<T> {
    /// Index of the node.
    node_index: usize,
    /// Iterator of this `Node`.
    iter: T,
    /// Current batch to be read. The node should ensure the batch is not empty (The
    /// cursor is not finished).
    ///
    /// `None` means the `iter` has reached EOF.
    cursor: Option<RowCursor>,
}

impl<T> NodeCmp for GenericNode<T> {
    fn is_eof(&self) -> bool {
        self.cursor.is_none()
    }

    fn is_behind(&self, other: &Self) -> bool {
        debug_assert!(!self.current_cursor().is_finished());
        debug_assert!(!other.current_cursor().is_finished());

        // We only compare pk and timestamp so nodes in the cold
        // heap don't have overlapping timestamps with the hottest node
        // in the hot heap.
        self.current_cursor()
            .first_primary_key()
            .cmp(other.current_cursor().last_primary_key())
            .then_with(|| {
                self.current_cursor()
                    .first_timestamp()
                    .cmp(&other.current_cursor().last_timestamp())
            })
            == Ordering::Greater
    }
}

impl<T> PartialEq for GenericNode<T> {
    fn eq(&self, other: &GenericNode<T>) -> bool {
        self.cursor == other.cursor
    }
}

impl<T> Eq for GenericNode<T> {}

impl<T> PartialOrd for GenericNode<T> {
    fn partial_cmp(&self, other: &GenericNode<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for GenericNode<T> {
    fn cmp(&self, other: &GenericNode<T>) -> Ordering {
        // The std binary heap is a max heap, but we want the nodes are ordered in
        // ascend order, so we compare the nodes in reverse order.
        other.cursor.cmp(&self.cursor)
    }
}

impl<T> GenericNode<T> {
    /// Returns current cursor.
    ///
    /// # Panics
    /// Panics if the node has reached EOF.
    fn current_cursor(&self) -> &RowCursor {
        self.cursor.as_ref().unwrap()
    }
}

impl GenericNode<BoxedRecordBatchIterator> {
    /// Fetches a new batch from the iter and updates the cursor.
    /// It advances the current batch.
    /// Returns the fetched new batch.
    fn advance_batch(&mut self) -> Result<Option<RecordBatch>> {
        let batch = self.advance_inner_iter()?;
        let columns = batch.as_ref().map(SortColumns::new);
        self.cursor = columns.map(RowCursor::new);

        Ok(batch)
    }

    /// Skips one row.
    /// Returns the next batch if the current batch is finished.
    fn advance_row(&mut self) -> Result<Option<RecordBatch>> {
        let cursor = self.cursor.as_mut().unwrap();
        cursor.advance();
        if !cursor.is_finished() {
            return Ok(None);
        }

        // Finished current batch, need to fetch a new batch.
        self.advance_batch()
    }

    /// Fetches a non-empty batch from the iter.
    fn advance_inner_iter(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(batch) = self.iter.next().transpose()? {
            if batch.num_rows() > 0 {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }
}

type StreamNode = GenericNode<BoxedRecordBatchStream>;
type IterNode = GenericNode<BoxedRecordBatchIterator>;

impl GenericNode<BoxedRecordBatchStream> {
    /// Fetches a new batch from the iter and updates the cursor.
    /// It advances the current batch.
    /// Returns the fetched new batch.
    async fn advance_batch(&mut self) -> Result<Option<RecordBatch>> {
        let batch = self.advance_inner_iter().await?;
        let columns = batch.as_ref().map(SortColumns::new);
        self.cursor = columns.map(RowCursor::new);

        Ok(batch)
    }

    /// Skips one row.
    /// Returns the next batch if the current batch is finished.
    async fn advance_row(&mut self) -> Result<Option<RecordBatch>> {
        let cursor = self.cursor.as_mut().unwrap();
        cursor.advance();
        if !cursor.is_finished() {
            return Ok(None);
        }

        // Finished current batch, need to fetch a new batch.
        self.advance_batch().await
    }

    /// Fetches a non-empty batch from the iter.
    async fn advance_inner_iter(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(batch) = self.iter.try_next().await? {
            if batch.num_rows() > 0 {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::task::Poll;

    use api::v1::OpType;
    use datatypes::arrow::array::builder::BinaryDictionaryBuilder;
    use datatypes::arrow::array::{Int64Array, TimestampMillisecondArray, UInt8Array, UInt64Array};
    use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;
    use futures::FutureExt;

    use super::*;
    use crate::error::UnexpectedSnafu;

    #[derive(Debug, Eq, PartialEq)]
    struct TestNode {
        id: usize,
        current_rank: Option<usize>,
        end_rank: usize,
    }

    impl TestNode {
        fn new(id: usize, current_rank: usize, end_rank: usize) -> Self {
            Self {
                id,
                current_rank: Some(current_rank),
                end_rank,
            }
        }
    }

    impl NodeCmp for TestNode {
        fn is_eof(&self) -> bool {
            self.current_rank.is_none()
        }

        fn is_behind(&self, other: &Self) -> bool {
            self.current_rank.unwrap() > other.end_rank
        }
    }

    impl PartialOrd for TestNode {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for TestNode {
        fn cmp(&self, other: &Self) -> Ordering {
            Reverse((self.current_rank, self.id)).cmp(&Reverse((other.current_rank, other.id)))
        }
    }

    #[test]
    fn test_merge_algo_repairs_overlapping_hot_root_in_place() {
        let mut algo = MergeAlgo::new(vec![
            TestNode::new(0, 0, 10),
            TestNode::new(1, 5, 15),
            TestNode::new(2, 20, 25),
        ]);
        assert_eq!(0, algo.hot.peek().unwrap().id);
        assert_eq!((2, 1), (algo.hot.len(), algo.cold.len()));

        algo.hottest_mut().unwrap().current_rank = Some(6);
        algo.repair_hot_root();

        assert_eq!(1, algo.hot.peek().unwrap().id);
        assert_eq!((2, 1), (algo.hot.len(), algo.cold.len()));
    }

    #[test]
    fn test_merge_algo_moves_root_beyond_remaining_hot_range_to_cold() {
        let mut algo = MergeAlgo::new(vec![
            TestNode::new(0, 0, 10),
            TestNode::new(1, 5, 7),
            TestNode::new(2, 20, 25),
        ]);

        algo.hottest_mut().unwrap().current_rank = Some(8);
        algo.repair_hot_root();

        assert_eq!(1, algo.hot.peek().unwrap().id);
        assert_eq!((1, 2), (algo.hot.len(), algo.cold.len()));
    }

    #[test]
    fn test_merge_algo_removes_eof_root_and_refills_hot() {
        let mut algo = MergeAlgo::new(vec![TestNode::new(0, 0, 4), TestNode::new(1, 10, 14)]);
        assert_eq!((1, 1), (algo.hot.len(), algo.cold.len()));

        algo.hottest_mut().unwrap().current_rank = None;
        algo.repair_hot_root();

        assert_eq!(1, algo.hot.peek().unwrap().id);
        assert_eq!((1, 0), (algo.hot.len(), algo.cold.len()));
    }

    #[test]
    fn test_merge_algo_single_hot_node_can_fetch_batch() {
        let algo = MergeAlgo::new(vec![TestNode::new(0, 0, 4), TestNode::new(1, 10, 14)]);

        assert_eq!(0, algo.hot.peek().unwrap().id);
        assert_eq!((1, 1), (algo.hot.len(), algo.cold.len()));
        assert!(algo.can_fetch_batch());
    }

    fn drain_root_heap<T: Ord>(mut heap: RootHeap<T>) -> Vec<T> {
        let mut values = Vec::with_capacity(heap.len());
        while let Some(value) = heap.pop() {
            values.push(value);
        }
        values
    }

    #[test]
    fn test_root_heap_repairs_mutated_root() {
        let mut heap = RootHeap::with_capacity(4);
        for value in [7, 3, 9, 5] {
            heap.push(value);
        }

        assert_eq!(Some(&9), heap.peek());
        *heap.root_mut().unwrap() = 1;
        heap.repair_root();

        assert_eq!(vec![7, 5, 3, 1], drain_root_heap(heap));
    }

    #[test]
    fn test_root_heap_sifts_across_multiple_levels() {
        let mut heap = RootHeap::with_capacity(8);
        for value in [7, 6, 5, 4, 3, 2, 1, 9] {
            heap.push(value);
        }

        assert_eq!(Some(&9), heap.peek());
        *heap.root_mut().unwrap() = 0;
        heap.repair_root();

        assert_eq!(vec![7, 6, 5, 4, 3, 2, 1, 0], drain_root_heap(heap));
    }

    #[test]
    fn test_root_heap_empty() {
        let mut heap = RootHeap::<i32>::with_capacity(0);

        assert!(heap.is_empty());
        assert_eq!(0, heap.len());
        assert_eq!(None, heap.peek());
        assert_eq!(None, heap.root_mut());
        assert_eq!(None, heap.best_child());
        assert_eq!(None, heap.pop());
        heap.repair_root();
    }

    #[test]
    fn test_root_heap_one_element() {
        let mut heap = RootHeap::with_capacity(1);
        heap.push(7);

        assert!(!heap.is_empty());
        assert_eq!(1, heap.len());
        assert_eq!(Some(&7), heap.peek());
        assert_eq!(None, heap.best_child());
        assert_eq!(Some(7), heap.pop());
        assert!(heap.is_empty());
    }

    #[test]
    fn test_root_heap_repairs_root_with_only_left_child() {
        let mut heap = RootHeap::with_capacity(2);
        heap.push(9);
        heap.push(7);

        *heap.root_mut().unwrap() = 1;
        heap.repair_root();

        assert_eq!(Some(&7), heap.peek());
        assert_eq!(Some(7), heap.pop());
        assert_eq!(Some(1), heap.pop());
    }

    #[test]
    fn test_root_heap_repairs_root_with_greater_right_child() {
        let mut heap = RootHeap::with_capacity(3);
        for value in [9, 3, 7] {
            heap.push(value);
        }

        *heap.root_mut().unwrap() = 1;
        heap.repair_root();

        assert_eq!(Some(&7), heap.peek());
        assert_eq!(Some(7), heap.pop());
        assert_eq!(Some(3), heap.pop());
        assert_eq!(Some(1), heap.pop());
    }

    #[test]
    fn test_root_heap_best_child_is_greatest_node_excluding_root() {
        let mut heap = RootHeap::with_capacity(5);
        for value in [9, 3, 7, 1, 2] {
            heap.push(value);
        }

        assert_eq!(Some(&7), heap.best_child());
    }

    /// Drives RootHeap and a std BinaryHeap oracle with the same seeded op
    /// sequence (push / pop / mutate-root + repair) and compares observable
    /// behavior after every op.
    fn assert_root_heap_matches_oracle(seed: u64, value_range: u32, num_ops: usize) {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        let mut rng = StdRng::seed_from_u64(seed);
        let mut heap = RootHeap::<u32>::with_capacity(0);
        let mut oracle = BinaryHeap::<u32>::new();
        let mut next_value = 0_u32;

        for _ in 0..num_ops {
            match rng.random_range(0..3) {
                0 => {
                    let pushed_value = next_value % value_range;
                    next_value += 1;
                    heap.push(pushed_value);
                    oracle.push(pushed_value);
                }
                1 => {
                    assert_eq!(oracle.pop(), heap.pop());
                }
                _ => {
                    let new_value = rng.random_range(0..value_range);
                    if let Some(root) = heap.root_mut() {
                        *root = new_value;
                        heap.repair_root();

                        oracle.pop();
                        oracle.push(new_value);
                    }
                }
            }

            assert_eq!(oracle.peek(), heap.peek());
            assert_eq!(oracle.len(), heap.len());
            if let Some(best_child) = heap.best_child() {
                let mut rest = oracle.clone();
                rest.pop();
                assert_eq!(rest.peek(), Some(best_child));
            }
        }

        // Both heaps must drain in the same non-increasing order.
        let mut oracle_values = Vec::with_capacity(oracle.len());
        while let Some(value) = oracle.pop() {
            oracle_values.push(value);
        }
        assert_eq!(oracle_values, drain_root_heap(heap));
    }

    #[test]
    fn test_root_heap_matches_binary_heap_oracle() {
        for seed in [0x5eed, 0xdead_beef, 42] {
            assert_root_heap_matches_oracle(seed, 1000, 2000);
        }
    }

    #[test]
    fn test_root_heap_matches_oracle_with_duplicate_heavy_values() {
        // A tiny value range makes duplicates dominate, which exercises the
        // equal-key branches of sift_up/sift_down and best_child.
        assert_root_heap_matches_oracle(0xc0ffee, 3, 2000);
    }

    /// Creates a test RecordBatch with the specified data.
    fn create_test_record_batch(
        primary_keys: &[&[u8]],
        timestamps: &[i64],
        sequences: &[u64],
        op_types: &[OpType],
        field_values: &[i64],
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int64, false),
            Field::new(
                "timestamp",
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
        ]));

        let field1 = Arc::new(Int64Array::from_iter_values(field_values.iter().copied()));
        let timestamp = Arc::new(TimestampMillisecondArray::from_iter_values(
            timestamps.iter().copied(),
        ));

        // Create primary key dictionary array using BinaryDictionaryBuilder
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for &key in primary_keys {
            builder.append(key).unwrap();
        }
        let primary_key = Arc::new(builder.finish());

        let sequence = Arc::new(UInt64Array::from_iter_values(sequences.iter().copied()));
        let op_type = Arc::new(UInt8Array::from_iter_values(
            op_types.iter().map(|&v| v as u8),
        ));

        RecordBatch::try_new(
            schema,
            vec![field1, timestamp, primary_key, sequence, op_type],
        )
        .unwrap()
    }

    fn new_test_iter(batches: Vec<RecordBatch>) -> BoxedRecordBatchIterator {
        Box::new(batches.into_iter().map(Ok))
    }

    fn boundary_test_batches() -> (RecordBatch, RecordBatch, RecordBatch) {
        let first = create_test_record_batch(
            &[b"k1", b"k1"],
            &[1000, 2000],
            &[1, 2],
            &[OpType::Put, OpType::Put],
            &[10, 12],
        );
        let second = create_test_record_batch(
            &[b"k1", b"k1", b"k1"],
            &[1500, 2000, 2500],
            &[1, 1, 1],
            &[OpType::Put, OpType::Put, OpType::Put],
            &[11, 13, 14],
        );
        let pending = create_test_record_batch(
            &[b"k1", b"k1", b"k1"],
            &[1000, 1500, 2000],
            &[1, 1, 2],
            &[OpType::Put, OpType::Put, OpType::Put],
            &[10, 11, 12],
        );
        (first, second, pending)
    }

    fn test_source_error() -> crate::error::Error {
        UnexpectedSnafu {
            reason: "test source failed".to_string(),
        }
        .build()
    }

    #[test]
    fn test_row_cursor_last_row() {
        let batch = create_test_record_batch(
            &[b"k1", b"k1"],
            &[1000, 2000],
            &[21, 22],
            &[OpType::Put, OpType::Put],
            &[11, 12],
        );
        let mut cursor = RowCursor::new(SortColumns::new(&batch));

        assert!(!cursor.is_last_row());
        cursor.advance();
        assert!(cursor.is_last_row());
        cursor.advance();
        assert!(!cursor.is_last_row());
    }

    /// Helper function to check if two record batches are equivalent.
    fn assert_record_batches_eq(expected: &[RecordBatch], actual: &[RecordBatch]) {
        for (exp, act) in expected.iter().zip(actual.iter()) {
            assert_eq!(exp, act,);
        }
    }

    /// Helper function to collect all batches from a FlatMergeIterator.
    fn collect_merge_iterator_batches(iter: FlatMergeIterator) -> Vec<RecordBatch> {
        iter.map(|result| result.unwrap()).collect()
    }

    #[test]
    fn test_merge_iterator_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int64, false),
            Field::new(
                "timestamp",
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
        ]));

        let mut merge_iter = FlatMergeIterator::new(schema, vec![], 1024).unwrap();
        assert!(merge_iter.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_merge_iterator_single_batch() {
        let batch = create_test_record_batch(
            &[b"k1", b"k1"],
            &[1000, 2000],
            &[21, 22],
            &[OpType::Put, OpType::Put],
            &[11, 12],
        );

        let schema = batch.schema();
        let iter = Box::new(new_test_iter(vec![batch.clone()]));

        let merge_iter = FlatMergeIterator::new(schema, vec![iter], 1024).unwrap();
        let result = collect_merge_iterator_batches(merge_iter);

        assert_eq!(result.len(), 1);
        assert_record_batches_eq(&[batch], &result);
    }

    #[test]
    fn test_merge_iterator_non_overlapping() {
        let batch1 = create_test_record_batch(
            &[b"k1", b"k1"],
            &[1000, 2000],
            &[21, 22],
            &[OpType::Put, OpType::Put],
            &[11, 12],
        );
        let batch2 = create_test_record_batch(
            &[b"k1", b"k1"],
            &[4000, 5000],
            &[24, 25],
            &[OpType::Put, OpType::Put],
            &[14, 15],
        );
        let batch3 = create_test_record_batch(
            &[b"k2", b"k2"],
            &[2000, 3000],
            &[22, 23],
            &[OpType::Delete, OpType::Put],
            &[12, 13],
        );

        let schema = batch1.schema();
        let iter1 = Box::new(new_test_iter(vec![batch1.clone(), batch3.clone()]));
        let iter2 = Box::new(new_test_iter(vec![batch2.clone()]));

        let merge_iter = FlatMergeIterator::new(schema, vec![iter1, iter2], 1024).unwrap();
        let result = collect_merge_iterator_batches(merge_iter);

        // Results should be sorted by primary key, timestamp, sequence desc
        let expected = vec![batch1, batch2, batch3];
        assert_record_batches_eq(&expected, &result);
    }

    #[test]
    fn test_merge_iterator_overlapping_timestamps() {
        // Create batches with overlapping timestamps but different sequences
        let batch1 = create_test_record_batch(
            &[b"k1", b"k1"],
            &[1000, 2000],
            &[21, 22],
            &[OpType::Put, OpType::Put],
            &[11, 12],
        );
        let batch2 = create_test_record_batch(
            &[b"k1", b"k1"],
            &[1500, 2500],
            &[31, 32],
            &[OpType::Put, OpType::Put],
            &[15, 25],
        );

        let schema = batch1.schema();
        let iter1 = Box::new(new_test_iter(vec![batch1]));
        let iter2 = Box::new(new_test_iter(vec![batch2]));

        let merge_iter = FlatMergeIterator::new(schema, vec![iter1, iter2], 1024).unwrap();
        let result = collect_merge_iterator_batches(merge_iter);

        let expected = vec![
            create_test_record_batch(
                &[b"k1", b"k1"],
                &[1000, 1500],
                &[21, 31],
                &[OpType::Put, OpType::Put],
                &[11, 15],
            ),
            create_test_record_batch(&[b"k1"], &[2000], &[22], &[OpType::Put], &[12]),
            create_test_record_batch(&[b"k1"], &[2500], &[32], &[OpType::Put], &[25]),
        ];
        assert_record_batches_eq(&expected, &result);
    }

    #[test]
    fn test_merge_iterator_duplicate_keys_sequences() {
        // Test with same primary key and timestamp but different sequences
        let batch1 = create_test_record_batch(
            &[b"k1", b"k1"],
            &[1000, 1000],
            &[20, 10],
            &[OpType::Put, OpType::Put],
            &[1, 2],
        );
        let batch2 = create_test_record_batch(
            &[b"k1"],
            &[1000],
            &[15], // Middle sequence
            &[OpType::Put],
            &[3],
        );

        let schema = batch1.schema();
        let iter1 = Box::new(new_test_iter(vec![batch1]));
        let iter2 = Box::new(new_test_iter(vec![batch2]));

        let merge_iter = FlatMergeIterator::new(schema, vec![iter1, iter2], 1024).unwrap();
        let result = collect_merge_iterator_batches(merge_iter);

        // Should be sorted by sequence descending for same key/timestamp
        let expected = vec![
            create_test_record_batch(
                &[b"k1", b"k1"],
                &[1000, 1000],
                &[20, 15],
                &[OpType::Put, OpType::Put],
                &[1, 3],
            ),
            create_test_record_batch(&[b"k1"], &[1000], &[10], &[OpType::Put], &[2]),
        ];
        assert_record_batches_eq(&expected, &result);
    }

    #[test]
    fn test_merge_iterator_retry_after_row_boundary_error_removes_source() {
        let (first, second, pending) = boundary_test_batches();
        let schema = first.schema();
        let first_source = Box::new(vec![Ok(first), Err(test_source_error())].into_iter())
            as BoxedRecordBatchIterator;
        let second_source = new_test_iter(vec![second.clone()]);
        let mut merge =
            FlatMergeIterator::new(schema, vec![first_source, second_source], 1024).unwrap();

        assert!(merge.next_batch().is_err());
        assert_eq!(pending, merge.next_batch().unwrap().unwrap());
        assert_eq!(second.slice(1, 2), merge.next_batch().unwrap().unwrap());
        assert!(merge.next_batch().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_merge_reader_retry_after_row_boundary_error_removes_source() {
        let (first, second, pending) = boundary_test_batches();
        let schema = first.schema();
        let first_source = Box::pin(futures::stream::iter(vec![
            Ok(first),
            Err(test_source_error()),
        ])) as BoxedRecordBatchStream;
        let second_source =
            Box::pin(futures::stream::iter(vec![Ok(second.clone())])) as BoxedRecordBatchStream;
        let mut merge = FlatMergeReader::new(schema, vec![first_source, second_source], 1024, None)
            .await
            .unwrap();

        assert!(merge.next_batch().await.is_err());
        assert_eq!(pending, merge.next_batch().await.unwrap().unwrap());
        assert_eq!(
            second.slice(1, 2),
            merge.next_batch().await.unwrap().unwrap()
        );
        assert!(merge.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_merge_reader_cancelled_row_boundary_fetch_removes_source() {
        let (first, second, pending) = boundary_test_batches();
        let schema = first.schema();
        let fetch_pending = Arc::new(AtomicBool::new(false));
        let fetch_pending_on_poll = Arc::clone(&fetch_pending);
        let mut first_batch = Some(first);
        let first_source = Box::pin(futures::stream::poll_fn(move |_cx| {
            if let Some(batch) = first_batch.take() {
                Poll::Ready(Some(Ok(batch)))
            } else {
                fetch_pending_on_poll.store(true, AtomicOrdering::Relaxed);
                Poll::Pending
            }
        })) as BoxedRecordBatchStream;
        let second_source =
            Box::pin(futures::stream::iter(vec![Ok(second.clone())])) as BoxedRecordBatchStream;
        let mut merge = FlatMergeReader::new(schema, vec![first_source, second_source], 1024, None)
            .await
            .unwrap();

        assert!(Box::pin(merge.next_batch()).now_or_never().is_none());
        assert!(fetch_pending.load(AtomicOrdering::Relaxed));
        assert_eq!(pending, merge.next_batch().await.unwrap().unwrap());
        assert_eq!(
            second.slice(1, 2),
            merge.next_batch().await.unwrap().unwrap()
        );
        assert!(merge.next_batch().await.unwrap().is_none());
    }

    #[test]
    fn test_batch_builder_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));

        let mut builder = BatchBuilder::new(schema.clone(), 2, 1024);
        assert!(builder.is_empty());

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(TimestampMillisecondArray::from(vec![1000, 2000])),
            ],
        )
        .unwrap();

        builder.push_batch(0, batch);
        builder.push_row(0);
        builder.push_row(0);

        assert!(!builder.is_empty());
        assert_eq!(builder.len(), 2);

        let result_batch = builder.build_record_batch().unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
    }

    fn assert_primary_key_dictionary(
        array: &dyn Array,
        expected_decoded: &[&[u8]],
        expected_values: &[&[u8]],
    ) {
        let dictionary = array.as_any().downcast_ref::<PrimaryKeyArray>().unwrap();
        let values = dictionary
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let decoded: Vec<_> = dictionary
            .keys()
            .iter()
            .map(|key| values.value(key.unwrap() as usize))
            .collect();
        let dictionary_values: Vec<_> = values.iter().map(Option::unwrap).collect();

        assert_eq!(expected_decoded, decoded);
        assert_eq!(expected_values, dictionary_values);
    }

    #[test]
    fn test_interleave_primary_key_deduplicates_separate_dictionaries() {
        let batch0 = create_test_record_batch(
            &[b"k1", b"k2"],
            &[1000, 2000],
            &[1, 1],
            &[OpType::Put, OpType::Put],
            &[10, 20],
        );
        let batch1 = create_test_record_batch(
            &[b"k1", b"k2"],
            &[1000, 2000],
            &[1, 1],
            &[OpType::Put, OpType::Put],
            &[11, 21],
        );
        let pk_idx = primary_key_column_index(batch0.num_columns());
        let arrays: Vec<_> = [&batch0, &batch1]
            .into_iter()
            .map(|batch| batch.column(pk_idx).as_ref())
            .collect();

        let output = interleave_primary_key(&arrays, &[(0, 0), (1, 0), (0, 1), (1, 1)]).unwrap();

        assert_primary_key_dictionary(
            output.as_ref(),
            &[b"k1", b"k1", b"k2", b"k2"],
            &[b"k1", b"k2"],
        );
    }

    #[test]
    fn test_interleave_primary_key_rejects_null_dictionary_value() {
        let primary_key = PrimaryKeyArray::try_new(
            UInt32Array::from(vec![0]),
            Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
        )
        .unwrap();

        let error = interleave_primary_key(&[&primary_key], &[(0, 0)]).unwrap_err();

        assert!(error.to_string().contains("null dictionary value"));
    }

    #[test]
    fn test_batch_builder_primary_key_has_no_state_between_builds() {
        let long_k1 = vec![b'a'; 4096];
        let long_k2 = vec![b'b'; 8192];
        let batch0 =
            create_test_record_batch(&[long_k1.as_slice()], &[1000], &[1], &[OpType::Put], &[10]);
        let batch1 =
            create_test_record_batch(&[long_k1.as_slice()], &[1000], &[1], &[OpType::Put], &[11]);
        let mut builder = BatchBuilder::new(batch0.schema(), 2, 4);
        builder.push_batch(0, batch0);
        builder.push_batch(1, batch1);
        builder.push_row(0);
        builder.push_row(1);

        let first = builder.build_record_batch().unwrap().unwrap();
        let pk_idx = primary_key_column_index(first.num_columns());
        assert_primary_key_dictionary(
            first.column(pk_idx).as_ref(),
            &[long_k1.as_slice(), long_k1.as_slice()],
            &[long_k1.as_slice()],
        );

        let batch0 =
            create_test_record_batch(&[long_k2.as_slice()], &[2000], &[2], &[OpType::Put], &[20]);
        let batch1 =
            create_test_record_batch(&[long_k2.as_slice()], &[2000], &[2], &[OpType::Put], &[21]);
        builder.push_batch(0, batch0);
        builder.push_batch(1, batch1);
        builder.push_row(0);
        builder.push_row(1);

        let second = builder.build_record_batch().unwrap().unwrap();
        assert_primary_key_dictionary(
            second.column(pk_idx).as_ref(),
            &[long_k2.as_slice(), long_k2.as_slice()],
            &[long_k2.as_slice()],
        );
    }

    #[test]
    fn test_row_cursor_comparison() {
        // Create test batches for cursor comparison
        let batch1 = create_test_record_batch(
            &[b"k1", b"k1"],
            &[1000, 2000],
            &[22, 21],
            &[OpType::Put, OpType::Put],
            &[11, 12],
        );
        let batch2 = create_test_record_batch(
            &[b"k1", b"k1"],
            &[1000, 2000],
            &[23, 20], // Different sequences
            &[OpType::Put, OpType::Put],
            &[11, 12],
        );

        let columns1 = SortColumns::new(&batch1);
        let columns2 = SortColumns::new(&batch2);

        let cursor1 = RowCursor::new(columns1);
        let cursor2 = RowCursor::new(columns2);

        // cursors with same pk and timestamp should be ordered by sequence desc
        // cursor1 has sequence 22, cursor2 has sequence 23, so cursor2 < cursor1 (higher sequence comes first)
        assert!(cursor2 < cursor1);
    }

    #[test]
    fn test_row_cursor_caches_current_primary_key() {
        let batch1 = create_test_record_batch(&[b"k1"], &[1000], &[1], &[OpType::Put], &[11]);
        let batch2 = create_test_record_batch(&[b"k2"], &[1000], &[1], &[OpType::Put], &[12]);
        let cursor1 = RowCursor::new(SortColumns::new(&batch1));
        let cursor2 = RowCursor::new(SortColumns::new(&batch2));

        for _ in 0..5 {
            assert_eq!(Ordering::Less, cursor1.cmp(&cursor2));
        }

        assert_eq!(1, cursor1.columns.primary_key_lookups());
        assert_eq!(1, cursor2.columns.primary_key_lookups());
    }
}
