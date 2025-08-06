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

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use datatypes::arrow::array::{Int64Array, UInt64Array};
use datatypes::arrow::compute::interleave;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::arrow_array::BinaryArray;
use datatypes::timestamp::timestamp_array_to_primitive;
use snafu::ResultExt;
use store_api::storage::SequenceNumber;

use crate::error::{ComputeArrowSnafu, Result};
use crate::memtable::BoxedRecordBatchIterator;
use crate::sst::parquet::flat_format::{
    primary_key_column_index, sequence_column_index, time_index_column_index,
};
use crate::sst::parquet::format::PrimaryKeyArray;

/// Keeps track of the current position in a batch
#[derive(Debug, Copy, Clone, Default)]
struct BatchCursor {
    /// The index into BatchBuilder::batches
    batch_idx: usize,
    /// The row index within the given batch
    row_idx: usize,
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

        let columns = (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = self
                    .batches
                    .iter()
                    .map(|(_, batch)| batch.column(column_idx).as_ref())
                    .collect();
                interleave(&arrays, &self.indices).context(ComputeArrowSnafu)
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
struct MergeAlgo<T> {
    /// Holds nodes whose key range of current batch **is** overlapped with the merge window.
    /// Each node yields batches from a `source`.
    ///
    /// Node in this heap **MUST** not be empty. A `merge window` is the (primary key, timestamp)
    /// range of the **root node** in the `hot` heap.
    hot: BinaryHeap<T>,
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
        let hot = BinaryHeap::with_capacity(nodes.len());
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

    /// Push the node popped from `hot` back to a proper heap.
    fn reheap(&mut self, node: T) {
        if node.is_eof() {
            // If the node is EOF, don't put it into the heap again.
            // The merge window would be updated, need to refill the hot heap.
            self.refill_hot();
        } else {
            // Find a proper heap for this node.
            let node_is_cold = if let Some(hottest) = self.hot.peek() {
                // If key range of this node is behind the hottest node's then we can
                // push it to the cold heap. Otherwise we should push it to the hot heap.
                node.is_behind(hottest)
            } else {
                // The hot heap is empty, but we don't known whether the current
                // batch of this node is still the hottest.
                true
            };

            if node_is_cold {
                self.cold.push(node);
            } else {
                self.hot.push(node);
            }
            // Anyway, the merge window has been changed, we need to refill the hot heap.
            self.refill_hot();
        }
    }

    /// Pops the hottest node.
    fn pop_hot(&mut self) -> Option<T> {
        self.hot.pop()
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
    timestamp: Int64Array,
    sequence: UInt64Array,
}

impl SortColumns {
    /// Creates a new [SortColumns] from a [RecordBatch] and the position of the time index column.
    ///
    /// # Panics
    /// Panics if the input batch doesn't have corrent internal columns.
    fn new(batch: &RecordBatch) -> Self {
        let num_columns = batch.num_columns();
        let primary_key = batch
            .column(primary_key_column_index(num_columns))
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
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
            timestamp,
            sequence,
        }
    }

    fn primary_key_at(&self, index: usize) -> &[u8] {
        let key = self.primary_key.keys().value(index);
        let binary_values = self
            .primary_key
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        binary_values.value(key as usize)
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
    /// Keys of the batch.
    columns: SortColumns,
}

impl RowCursor {
    fn new(columns: SortColumns) -> Self {
        debug_assert!(columns.num_rows() > 0);

        Self { offset: 0, columns }
    }

    fn is_finished(&self) -> bool {
        self.offset >= self.columns.num_rows()
    }

    fn advance(&mut self) {
        self.offset += 1;
    }

    fn first_primary_key(&self) -> &[u8] {
        self.columns.primary_key_at(self.offset)
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
pub struct MergeIterator {
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

impl MergeIterator {
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

        Ok(Self {
            algo,
            in_progress,
            output_batch: None,
            batch_size,
        })
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

        if let Some(batch) = self.output_batch.take() {
            Ok(Some(batch))
        } else {
            // No more batches.
            Ok(None)
        }
    }

    /// Fetches a batch from the hottest node.
    fn fetch_batch_from_hottest(&mut self) -> Result<()> {
        debug_assert!(self.in_progress.is_empty());

        // Safety: next_batch() ensures the heap is not empty.
        let mut hottest = self.algo.pop_hot().unwrap();
        debug_assert!(!hottest.current_cursor().is_finished());
        let next = hottest.advance_batch()?;
        // The node is the heap is not empty, so it must have existing rows in the builder.
        let batch = self
            .in_progress
            .take_remaining_rows(hottest.node_index, next);
        Self::maybe_output_batch(batch, &mut self.output_batch);
        self.algo.reheap(hottest);

        Ok(())
    }

    /// Fetches a row from the hottest node.
    fn fetch_row_from_hottest(&mut self) -> Result<()> {
        // Safety: next_batch() ensures the heap has more than 1 element.
        let mut hottest = self.algo.pop_hot().unwrap();
        debug_assert!(!hottest.current_cursor().is_finished());
        self.in_progress.push_row(hottest.node_index);
        if self.in_progress.len() >= self.batch_size {
            // We buffered enough rows.
            if let Some(output) = self.in_progress.build_record_batch()? {
                Self::maybe_output_batch(output, &mut self.output_batch);
            }
        }

        if let Some(next) = hottest.advance_row()? {
            self.in_progress.push_batch(hottest.node_index, next);
        }

        self.algo.reheap(hottest);
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

/// A sync node in the merge iterator.
struct IterNode {
    /// Index of the node.
    node_index: usize,
    /// Iterator of this `Node`.
    iter: BoxedRecordBatchIterator,
    /// Current batch to be read. The node should ensure the batch is not empty (The
    /// cursor is not finished).
    ///
    /// `None` means the `iter` has reached EOF.
    cursor: Option<RowCursor>,
}

impl IterNode {
    /// Returns current cursor.
    ///
    /// # Panics
    /// Panics if the node has reached EOF.
    fn current_cursor(&self) -> &RowCursor {
        self.cursor.as_ref().unwrap()
    }

    /// Fetches a new batch from the iter and updates the cursor.
    /// It advances the current batch.
    /// Returns the fetched new batch.
    fn advance_batch(&mut self) -> Result<Option<RecordBatch>> {
        let batch = self.advance_inner_iter()?;
        let columns = batch.as_ref().map(|rb| SortColumns::new(rb));
        self.cursor = columns.map(|c| RowCursor::new(c));

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

impl NodeCmp for IterNode {
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

impl PartialEq for IterNode {
    fn eq(&self, other: &IterNode) -> bool {
        self.cursor == other.cursor
    }
}

impl Eq for IterNode {}

impl PartialOrd for IterNode {
    fn partial_cmp(&self, other: &IterNode) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IterNode {
    fn cmp(&self, other: &IterNode) -> Ordering {
        // The std binary heap is a max heap, but we want the nodes are ordered in
        // ascend order, so we compare the nodes in reverse order.
        other.cursor.cmp(&self.cursor)
    }
}
