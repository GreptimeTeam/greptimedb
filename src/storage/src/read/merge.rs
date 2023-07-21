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

//! Merge reader.
//!
//! The implementation of [`MergeReader`] is inspired by
//!  [`kudu's MergeIterator`](https://github.com/apache/kudu/blob/9021f275824faa2bdfe699786957c40c219697c1/src/kudu/common/generic_iterators.cc#L107)
//! and [`CeresDB's MergeIterator`](https://github.com/CeresDB/ceresdb/blob/02a7e3100f47cf16aa6c245ed529a6978be20fbd/analytic_engine/src/row_iter/merge.rs)
//!
//! The main idea of the merge algorithm is to maintain a `merge window`. The window describes,
//! at any given time, the key range where we expect to find the row with the smallest key.
//! A [`Node`] (known as the sub-iterator in kudu) whose NEXT overlaps with the `merge window`
//! is said to be actively participating in the merge.
//!
//! The `merge window` is defined as follows:
//! 1.  The window's start is the smallest lower bound of all nodes. We
//!     refer to the node that owns this lower bound as LOW.
//! 2.  The window’s end is the smallest upper bound of all nodes whose
//!     lower bounds are less than or equal to LOW's upper bound.
//! 2a. The window's end could be LOW's upper bound itself, if it is the smallest
//!     upper bound, but this isn't necessarily the case.
//! 3.  The merge window's dimensions change as the merge proceeds, though it
//!     only ever moves "to the right" (i.e. the window start/end only increase).
//!
//! We can divide the nodes into two sets, one for whose next rows overlap with the `merge window`,
//! another for whose next rows do not. The merge steady state resembles that of a traditional
//! heap-based merge: the top-most node is popped from HOT, the lower bound is copied to the output
//! and advanced, and the node is pushed back to HOT.
//!
//! In the steady state, we need to move nodes from COLD to HOT whenever the end of the merge window
//! moves; that's a sign that the window may now overlap with a NEXT belonging to a nodes in the
//! second set (COLD). The end of the merge window moves when a node is fully exhausted (i.e. all rows have
//! been copied to the output), or when a node finishes its NEXT and needs to peek again.
//!
//! At any given time, the NEXT belonging to the top-most node in COLD is nearest the merge window.
//! When the merge window's end has moved and we need to refill HOT, the top-most node in COLD is
//! the best candidate. To figure out whether it should be moved, we compare its NEXT's lower bound
//! against the upper bound in HOT's first node: if the lower bound is less than or equal to the key,
//! we move the node from COLD to HOT. On the flip side, when a node from HOT finishes its NEXT and peeks
//! again, we also need to check whether it has exited the merge window. The approach is similar: if
//! its NEXT's lower bound is greater than the upper bound of HOT'S first node, it's time to move it to COLD.
//!
//! A full description of the merge algorithm could be found in [`kudu's comment`](https://github.com/apache/kudu/blob/9021f275824faa2bdfe699786957c40c219697c1/src/kudu/common/generic_iterators.cc#L349)
//!  and the [google doc](https://docs.google.com/document/d/1uP0ubjM6ulnKVCRrXtwT_dqrTWjF9tlFSRk0JN2e_O0/edit#).

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;

use async_trait::async_trait;
use store_api::storage::consts;

use crate::error::Result;
use crate::memtable::BoxedBatchIterator;
use crate::read::{Batch, BatchBuilder, BatchOp, BatchReader, BoxedBatchReader};
use crate::schema::{ProjectedSchema, ProjectedSchemaRef};

/// Batch data source.
enum Source {
    // To avoid the overhead of async-trait (typically a heap allocation), wraps the
    // BatchIterator into an enum instead of converting the iterator into a BatchReader.
    Iter(BoxedBatchIterator),
    Reader(BoxedBatchReader),
}

impl Source {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Source::Iter(iter) => iter.next().transpose(),
            Source::Reader(reader) => reader.next_batch().await,
        }
    }

    /// Fetch next non empty batch.
    async fn next_non_empty_batch(&mut self) -> Result<Option<Batch>> {
        while let Some(batch) = self.next_batch().await? {
            if !batch.is_empty() {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }
}

impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Source::Iter(_) => write!(f, "Iter(..)"),
            Source::Reader(_) => write!(f, "Reader(..)"),
        }
    }
}

/// Reference to a row in [BatchCursor].
#[derive(Debug)]
struct RowCursor<'a> {
    batch: &'a Batch,
    pos: usize,
}

impl<'a> RowCursor<'a> {
    #[inline]
    fn compare(&self, schema: &ProjectedSchema, other: &RowCursor) -> Ordering {
        schema.compare_row(self.batch, self.pos, other.batch, other.pos)
    }
}

/// A `BatchCursor` wraps the `Batch` and allows reading the `Batch` by row.
#[derive(Debug)]
struct BatchCursor {
    /// Current buffered `Batch`.
    ///
    /// `Batch` must contains at least one row.
    batch: Batch,
    /// Index of current row.
    ///
    /// `pos == batch.num_rows()` indicates no more rows to read.
    pos: usize,
}

impl BatchCursor {
    /// Create a new `BatchCursor`.
    ///
    /// # Panics
    /// Panics if `batch` is empty.
    fn new(batch: Batch) -> BatchCursor {
        assert!(!batch.is_empty());

        BatchCursor { batch, pos: 0 }
    }

    /// Returns true if there are remaining rows to read.
    #[inline]
    fn is_valid(&self) -> bool {
        !self.is_empty()
    }

    /// Returns first row of current batch.
    ///
    /// # Panics
    /// Panics if `self` is invalid.
    fn first_row(&self) -> RowCursor {
        assert!(self.is_valid());

        RowCursor {
            batch: &self.batch,
            pos: self.pos,
        }
    }

    /// Returns last row of current batch.
    ///
    /// # Panics
    /// Panics if `self` is invalid.
    fn last_row(&self) -> RowCursor {
        assert!(self.is_valid());

        RowCursor {
            batch: &self.batch,
            pos: self.batch.num_rows() - 1,
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.pos >= self.batch.num_rows()
    }

    /// Take slice of batch with at most `length` rows from the cursor, then
    /// advance the cursor.
    ///
    /// # Panics
    /// Panics if `self` is invalid.
    fn take_batch_slice(&mut self, length: usize) -> Batch {
        let length = length.min(self.batch.num_rows() - self.pos);
        let batch = self.batch.slice(self.pos, length);
        self.pos += batch.num_rows();

        batch
    }

    /// Push at most `length` rows from `self` to the `builder` and advance the cursor.
    ///
    /// # Panics
    /// Panics if `self` is invalid.
    fn push_rows_to(&mut self, builder: &mut BatchBuilder, length: usize) -> Result<()> {
        let length = length.min(self.batch.num_rows() - self.pos);
        builder.extend_slice_of(&self.batch, self.pos, length)?;
        self.pos += length;

        Ok(())
    }

    /// Push next row from `self` to the `builder` and advance the cursor.
    ///
    /// # Panics
    /// Panics if `self` is invalid.
    fn push_next_row_to(&mut self, builder: &mut BatchBuilder) -> Result<()> {
        builder.push_row_of(&self.batch, self.pos)?;
        self.pos += 1;

        Ok(())
    }
}

/// A `Node` represent an individual input data source to be merged.
struct Node {
    /// Schema of data source.
    schema: ProjectedSchemaRef,
    /// Data source of this `Node`.
    source: Source,
    /// Current batch to be read.
    ///
    /// `None` means the `source` has reached EOF.
    cursor: Option<BatchCursor>,
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Node")
            .field("source", &self.source)
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

impl Node {
    async fn new(schema: ProjectedSchemaRef, mut source: Source) -> Result<Node> {
        let cursor = source.next_non_empty_batch().await?.map(BatchCursor::new);
        Ok(Node {
            schema,
            source,
            cursor,
        })
    }

    /// Returns the reference to the cursor.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn cursor_ref(&self) -> &BatchCursor {
        self.cursor.as_ref().unwrap()
    }

    /// Returns first row in cursor.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn first_row(&self) -> RowCursor {
        self.cursor_ref().first_row()
    }

    /// Returns last row in cursor.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn last_row(&self) -> RowCursor {
        self.cursor_ref().last_row()
    }

    /// Compare first row of two nodes.
    ///
    /// # Panics
    /// Panics if
    /// - either `self` or `other` is EOF.
    fn compare_first_row(&self, other: &Node) -> Ordering {
        self.first_row().compare(&self.schema, &other.first_row())
    }

    /// Returns true if no more batch could be fetched from this node.
    fn is_eof(&self) -> bool {
        self.cursor.is_none()
    }

    /// Returns true if the key range of current batch in `self` is behind (exclusive) current
    /// batch in `other`.
    ///
    /// # Panics
    /// Panics if
    /// - either `self` or `other` is EOF.
    fn is_behind(&self, other: &Node) -> bool {
        let first = self.first_row();
        let last = other.last_row();
        // `self` is after `other` if min (first) row of `self` is greater than
        // max (last) row of `other`.
        first.compare(&self.schema, &last) == Ordering::Greater
    }

    /// Fetch next batch and reset its cursor if `self` isn't EOF and the cursor
    /// is empty.
    ///
    /// Returns true if a new batch has been fetched.
    async fn maybe_fetch_next_batch(&mut self) -> Result<bool> {
        let need_fetch = !self.is_eof() && self.cursor_ref().is_empty();
        if !need_fetch {
            // Still has remaining rows, no need to fetch.
            return Ok(false);
        }

        // This ensure the cursor is either non empty or None (EOF).
        match self.source.next_non_empty_batch().await? {
            Some(batch) => {
                self.cursor = Some(BatchCursor::new(batch));
                Ok(true)
            }
            None => {
                // EOF
                self.cursor = None;
                Ok(false)
            }
        }
    }

    /// Returns the mutable reference to the cursor.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn cursor_mut(&mut self) -> &mut BatchCursor {
        self.cursor.as_mut().unwrap()
    }

    /// Take batch from this node.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn take_batch_slice(&mut self, length: usize) -> Batch {
        self.cursor_mut().take_batch_slice(length)
    }

    /// Push at most `length` rows from `self` to the `builder`.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn push_rows_to(&mut self, builder: &mut BatchBuilder, length: usize) -> Result<()> {
        self.cursor_mut().push_rows_to(builder, length)
    }

    /// Push next row from `self` to the `builder`.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn push_next_row_to(&mut self, builder: &mut BatchBuilder) -> Result<()> {
        self.cursor_mut().push_next_row_to(builder)
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.compare_first_row(other) == Ordering::Equal
    }
}

impl Eq for Node {}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Node) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Node) -> Ordering {
        // The std binary heap is a max heap, but we want the nodes are ordered in
        // ascend order, so we compare the nodes in reverse order.
        other.compare_first_row(self)
    }
}

/// A reader that would sort and merge `Batch` from multiple sources by key.
///
/// `Batch` from each `Source` **must** be sorted.
pub struct MergeReader {
    /// Whether the reader has been initialized.
    initialized: bool,
    /// Schema of data source.
    schema: ProjectedSchemaRef,
    /// Input data sources.
    ///
    /// All data source must have same schema. Initialize the reader would
    /// convert all `Source`s into `Node`s and then clear this vector.
    sources: Vec<Source>,
    /// Holds `Node` whose key range of current batch **is** overlapped with the merge window.
    ///
    /// `Node` in this heap **must** not be empty. A `merge window` is the key range of the
    /// root node in the `hot` heap.
    hot: BinaryHeap<Node>,
    /// Holds `Node` whose key range of current batch **isn't** overlapped with the merge window.
    ///
    /// `Node` in this heap **must** not be empty.
    cold: BinaryHeap<Node>,
    /// Suggested row number of each batch.
    ///
    /// The size of the batch yield from this reader may not always equal to this suggested size.
    batch_size: usize,
    /// Buffered batch.
    batch_builder: BatchBuilder,
}

#[async_trait]
impl BatchReader for MergeReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.fetch_next_batch().await
    }
}

pub struct MergeReaderBuilder {
    schema: ProjectedSchemaRef,
    sources: Vec<Source>,
    batch_size: usize,
}

impl MergeReaderBuilder {
    pub fn new(schema: ProjectedSchemaRef) -> Self {
        MergeReaderBuilder::with_capacity(schema, 0)
    }

    pub fn with_capacity(schema: ProjectedSchemaRef, capacity: usize) -> Self {
        MergeReaderBuilder {
            schema,
            sources: Vec::with_capacity(capacity),
            batch_size: consts::READ_BATCH_SIZE,
        }
    }

    pub fn push_batch_iter(mut self, iter: BoxedBatchIterator) -> Self {
        self.sources.push(Source::Iter(iter));
        self
    }

    pub fn push_batch_reader(mut self, reader: BoxedBatchReader) -> Self {
        self.sources.push(Source::Reader(reader));
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn build(self) -> MergeReader {
        let num_sources = self.sources.len();
        let column_schemas = self.schema.schema_to_read().schema().column_schemas();
        let batch_builder = BatchBuilder::with_capacity(
            column_schemas.iter().map(|c| &c.data_type),
            self.batch_size,
        );

        MergeReader {
            initialized: false,
            schema: self.schema,
            sources: self.sources,
            hot: BinaryHeap::with_capacity(num_sources),
            cold: BinaryHeap::with_capacity(num_sources),
            batch_size: self.batch_size,
            batch_builder,
        }
    }
}

impl MergeReader {
    /// Initialize the reader if it has not yet been initialized.
    async fn try_init(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        if self.sources.is_empty() {
            self.initialized = true;
            return Ok(());
        }

        for source in self.sources.drain(..) {
            let node = Node::new(self.schema.clone(), source).await?;

            if !node.is_eof() {
                self.cold.push(node);
            }
        }

        self.refill_hot();

        self.initialized = true;

        Ok(())
    }

    async fn fetch_next_batch(&mut self) -> Result<Option<Batch>> {
        self.try_init().await?;

        while !self.hot.is_empty() && self.batch_builder.num_rows() < self.batch_size {
            if self.hot.len() == 1 {
                // No need to do merge sort if only one batch in the hot heap.
                let fetch_row_num = self.batch_size - self.batch_builder.num_rows();
                if let Some(batch) = self.fetch_batch_from_hottest(fetch_row_num).await? {
                    // The builder is empty and we have fetched a new batch from this node.
                    return Ok(Some(batch));
                }
                // Otherwise, some rows may have been pushed into the builder.
            } else {
                // We could only fetch one row from the hottest node.
                self.fetch_one_row_from_hottest().await?;
            }
        }

        // Check buffered rows in the builder.
        if self.batch_builder.is_empty() {
            Ok(None)
        } else {
            self.batch_builder.build().map(Some)
        }
    }

    /// Move nodes in `cold` heap, whose key range is overlapped with current merge
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

    /// Fetch at most `fetch_row_num` from the hottest node and attempt to return them directly
    /// instead of pushing into the builder if the `self.batch_builder` is empty.
    async fn fetch_batch_from_hottest(&mut self, fetch_row_num: usize) -> Result<Option<Batch>> {
        assert_eq!(1, self.hot.len());

        let mut hottest = self.hot.pop().unwrap();
        let batch = if self.batch_builder.is_empty() {
            Some(hottest.take_batch_slice(fetch_row_num))
        } else {
            hottest.push_rows_to(&mut self.batch_builder, fetch_row_num)?;

            None
        };

        self.reheap(hottest).await?;

        Ok(batch)
    }

    /// Fetch one row from the hottest node.
    async fn fetch_one_row_from_hottest(&mut self) -> Result<()> {
        let mut hottest = self.hot.pop().unwrap();
        hottest.push_next_row_to(&mut self.batch_builder)?;

        self.reheap(hottest).await
    }

    /// Fetch next batch from this node and reset its cursor, then push the node back to a
    /// proper heap.
    async fn reheap(&mut self, mut node: Node) -> Result<()> {
        let fetched_new_batch = node.maybe_fetch_next_batch().await?;

        if node.is_eof() {
            // The merge window would be updated, need to refill the hot heap.
            self.refill_hot();
        } else if fetched_new_batch {
            // A new batch has been fetched from the node, thus the key range of this node
            // has been changed. Try to find a proper heap for this node.
            let node_is_cold = if let Some(hottest) = self.hot.peek() {
                // Now key range of this node is behind the hottest node's.
                node.is_behind(hottest)
            } else {
                false
            };

            if node_is_cold {
                self.cold.push(node);
            } else {
                self.hot.push(node);
            }
            // Anyway, the merge window has been changed, we need to refill the hot heap.
            self.refill_hot();
        } else {
            // No new batch has been fetched, so the end key of merge window has not been
            // changed, we could just put the node back to the hot heap.
            self.hot.push(node);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::ScalarVector;
    use datatypes::vectors::{Int64Vector, TimestampMillisecondVector};

    use super::*;
    use crate::test_util::read_util::{self, Batches};

    #[tokio::test]
    async fn test_merge_reader_empty() {
        let schema = read_util::new_projected_schema();

        let mut reader = MergeReaderBuilder::new(schema).build();

        assert!(reader.next_batch().await.unwrap().is_none());
        // Call next_batch() again is allowed.
        assert!(reader.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_node() {
        let schema = read_util::new_projected_schema();
        let left_source = read_util::build_boxed_iter(&[&[(1, None), (3, None), (5, None)]]);
        let mut left = Node::new(schema.clone(), Source::Iter(left_source))
            .await
            .unwrap();

        let right_source = read_util::build_boxed_reader(&[&[(2, None), (3, None), (6, None)]]);
        let mut right = Node::new(schema.clone(), Source::Reader(right_source))
            .await
            .unwrap();

        // We use reverse order for a node.
        assert!(left > right);
        assert_ne!(left, right);

        // Advance the left and right node.
        left.cursor_mut().pos += 1;
        right.cursor_mut().pos += 1;
        assert_eq!(left, right);

        // Check Debug is implemented.
        let output = format!("{left:?}");
        assert!(output.contains("cursor"));
        assert!(output.contains("pos: 1"));
        let output = format!("{right:?}");
        assert!(output.contains("cursor"));
        let output = format!("{:?}", left.first_row());
        assert!(output.contains("pos: 1"));
    }

    fn build_merge_reader(sources: &[Batches], num_iter: usize, batch_size: usize) -> MergeReader {
        let schema = read_util::new_projected_schema();
        let mut builder =
            MergeReaderBuilder::with_capacity(schema, sources.len()).batch_size(batch_size);

        for (i, source) in sources.iter().enumerate() {
            if i < num_iter {
                builder = builder.push_batch_iter(read_util::build_boxed_iter(source));
            } else {
                builder = builder.push_batch_reader(read_util::build_boxed_reader(source));
            }
        }

        builder.build()
    }

    async fn check_merge_reader_result(mut reader: MergeReader, input: &[Batches<'_>]) {
        let mut expect: Vec<_> = input
            .iter()
            .flat_map(|v| v.iter())
            .flat_map(|v| v.iter().copied())
            .collect();
        expect.sort_by_key(|k| k.0);

        let result = read_util::collect_kv_batch(&mut reader).await;
        assert_eq!(expect, result);

        // Call next_batch() again is allowed.
        assert!(reader.next_batch().await.unwrap().is_none());
    }

    async fn check_merge_reader_by_batch(mut reader: MergeReader, expect_batches: Batches<'_>) {
        let mut result = Vec::new();
        while let Some(batch) = reader.next_batch().await.unwrap() {
            let key = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondVector>()
                .unwrap();
            let value = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Vector>()
                .unwrap();

            let batch: Vec<_> = key
                .iter_data()
                .zip(value.iter_data())
                .map(|(k, v)| (k.unwrap().into(), v))
                .collect();
            result.push(batch);
        }

        for (expect, actual) in expect_batches.iter().zip(result.iter()) {
            assert_eq!(expect, actual);
        }
    }

    #[tokio::test]
    async fn test_merge_multiple_interleave() {
        common_telemetry::init_default_ut_logging();

        let input: &[Batches] = &[
            &[&[(1, Some(1)), (5, Some(5)), (9, Some(9))]],
            &[&[(2, Some(2)), (3, Some(3)), (8, Some(8))]],
            &[&[(7, Some(7)), (12, Some(12))]],
        ];
        let reader = build_merge_reader(input, 1, 3);
        check_merge_reader_result(reader, input).await;

        let input: &[Batches] = &[
            &[
                &[(1, Some(1)), (2, Some(2))],
                &[(3, Some(3)), (4, Some(4))],
                &[(5, Some(5)), (12, Some(12))],
            ],
            &[&[(6, Some(6)), (7, Some(7)), (18, Some(18))]],
            &[&[(13, Some(13)), (15, Some(15))]],
        ];
        let reader = build_merge_reader(input, 1, 3);
        check_merge_reader_by_batch(
            reader,
            &[
                // The former two batches could be returned directly.
                &[(1, Some(1)), (2, Some(2))],
                &[(3, Some(3)), (4, Some(4))],
                &[(5, Some(5)), (6, Some(6)), (7, Some(7))],
                &[(12, Some(12)), (13, Some(13)), (15, Some(15))],
                &[(18, Some(18))],
            ],
        )
        .await;

        let input: &[Batches] = &[
            &[
                &[(1, Some(1)), (2, Some(2))],
                &[(5, Some(5)), (9, Some(9))],
                &[(14, Some(14)), (17, Some(17))],
            ],
            &[&[(6, Some(6)), (7, Some(7))], &[(15, Some(15))]],
        ];
        let reader = build_merge_reader(input, 1, 2);
        check_merge_reader_by_batch(
            reader,
            &[
                &[(1, Some(1)), (2, Some(2))],
                // Could not return batch (6, 7) directly.
                &[(5, Some(5)), (6, Some(6))],
                &[(7, Some(7)), (9, Some(9))],
                &[(14, Some(14)), (15, Some(15))],
                &[(17, Some(17))],
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_one_source() {
        common_telemetry::init_default_ut_logging();

        let input: &[Batches] = &[&[
            &[(1, Some(1)), (2, Some(2)), (3, Some(3))],
            &[(4, Some(4)), (5, Some(5)), (6, Some(6))],
        ]];
        let reader = build_merge_reader(input, 1, 2);

        check_merge_reader_result(reader, input).await;
    }

    #[tokio::test]
    async fn test_merge_with_empty_batch() {
        let input: &[Batches] = &[
            &[
                &[(1, Some(1)), (2, Some(2))],
                &[(3, Some(3)), (6, Some(6))],
                &[],
                &[],
                &[(8, Some(8)), (12, Some(12))],
                &[],
            ],
            &[
                &[(4, Some(4)), (5, Some(5))],
                &[],
                &[(15, None), (18, None), (20, None)],
            ],
            &[&[(13, Some(13)), (19, None)], &[], &[]],
        ];
        let reader = build_merge_reader(input, 1, 2);

        check_merge_reader_result(reader, input).await;
    }

    #[tokio::test]
    async fn test_merge_duplicate_key() {
        let input: &[Batches] = &[
            &[
                &[(1, Some(1)), (5, Some(5)), (8, Some(8))],
                &[(9, None), (11, None)],
                &[(12, Some(12)), (15, None)],
            ],
            &[&[(1, Some(1)), (3, Some(3)), (8, Some(8))], &[(16, None)]],
            &[
                &[(7, Some(7)), (12, Some(12))],
                &[(15, None), (16, None), (17, None)],
            ],
            &[&[(15, None)]],
        ];
        let reader = build_merge_reader(input, 2, 2);
        check_merge_reader_result(reader, input).await;
    }
}
