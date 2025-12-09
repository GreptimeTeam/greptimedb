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
use std::time::Instant;

use async_stream::try_stream;
use common_telemetry::debug;
use datatypes::arrow::array::{Int64Array, UInt64Array};
use datatypes::arrow::compute::interleave;
use datatypes::arrow::datatypes::SchemaRef;
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
use crate::read::merge::{MergeMetrics, MergeMetricsReport};
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
    /// Panics if the input batch doesn't have correct internal columns.
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
        reader.metrics.scan_cost += start.elapsed();

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
        let mut hottest = self.algo.pop_hot().unwrap();
        debug_assert!(!hottest.current_cursor().is_finished());
        let start = Instant::now();
        let next = hottest.advance_batch().await?;
        self.metrics.fetch_cost += start.elapsed();
        // The node is the heap is not empty, so it must have existing rows in the builder.
        let batch = self
            .in_progress
            .take_remaining_rows(hottest.node_index, next);
        Self::maybe_output_batch(batch, &mut self.output_batch);
        self.algo.reheap(hottest);

        Ok(())
    }

    /// Fetches a row from the hottest node.
    async fn fetch_row_from_hottest(&mut self) -> Result<()> {
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

        let start = Instant::now();
        if let Some(next) = hottest.advance_row().await? {
            self.metrics.fetch_cost += start.elapsed();
            self.in_progress.push_batch(hottest.node_index, next);
        } else {
            self.metrics.fetch_cost += start.elapsed();
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

impl Drop for FlatMergeReader {
    fn drop(&mut self) {
        debug!("Flat merge reader finished, metrics: {:?}", self.metrics);

        // Report any remaining metrics.
        if let Some(reporter) = &self.metrics_reporter {
            reporter.report(&mut self.metrics);
        }

        READ_STAGE_ELAPSED
            .with_label_values(&["flat_merge"])
            .observe(self.metrics.scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["flat_merge_fetch"])
            .observe(self.metrics.fetch_cost.as_secs_f64());
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
    use std::sync::Arc;

    use api::v1::OpType;
    use datatypes::arrow::array::builder::BinaryDictionaryBuilder;
    use datatypes::arrow::array::{Int64Array, TimestampMillisecondArray, UInt8Array, UInt64Array};
    use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;

    use super::*;

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
}
