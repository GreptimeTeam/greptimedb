//! Merge reader.
//!
//! This implementation is inspired by [kudu's MergeIterator](https://github.com/apache/kudu/blob/9021f275824faa2bdfe699786957c40c219697c1/src/kudu/common/generic_iterators.cc#L107)
//! and [CeresDB's MergeIterator](https://github.com/CeresDB/ceresdb/blob/02a7e3100f47cf16aa6c245ed529a6978be20fbd/analytic_engine/src/row_iter/merge.rs)

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use async_trait::async_trait;
use store_api::storage::consts;

use crate::error::Result;
use crate::memtable::BoxedBatchIterator;
use crate::read::{Batch, BatchBuilder, BatchReader, BoxedBatchReader};
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

/// Reference to a row in [BatchCursor].
struct RowCursor<'a> {
    batch: &'a Batch,
    pos: usize,
}

impl<'a> RowCursor<'a> {
    #[inline]
    fn compare(&self, schema: &ProjectedSchema, other: &RowCursor) -> Ordering {
        schema.compare_row_of_batch(self.batch, self.pos, other.batch, other.pos)
    }
}

/// A `BatchCursor` wraps the `Batch` and allows reading the `Batch` by row.
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

    #[inline]
    fn first_row_index(&self) -> usize {
        self.pos
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

impl Node {
    async fn new(schema: ProjectedSchemaRef, mut source: Source) -> Result<Node> {
        let cursor = source.next_non_empty_batch().await?.map(BatchCursor::new);
        Ok(Node {
            schema,
            source,
            cursor,
        })
    }

    /// Returns first row in cursor.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn first_row(&self) -> RowCursor {
        self.cursor.as_ref().unwrap().first_row()
    }

    /// Returns last row in cursor.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn last_row(&self) -> RowCursor {
        self.cursor.as_ref().unwrap().last_row()
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
        matches!(first.compare(&self.schema, &last), Ordering::Greater)
    }

    /// Fetch next batch and reset its cursor if `self` isn't EOF and the cursor
    /// is empty.
    ///
    /// Returns true if a new batch has been fetched.
    async fn maybe_fetch_next_batch(&mut self) -> Result<bool> {
        let need_fetch = !self.is_eof() && self.cursor.as_ref().unwrap().is_empty();
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

    /// Take batch from this node.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn take_batch_slice(&mut self, length: usize) -> Batch {
        self.cursor.as_mut().unwrap().take_batch_slice(length)
    }

    /// Push at most `length` rows from `self` to the `builder`.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn push_rows_to(&mut self, builder: &mut BatchBuilder, length: usize) -> Result<()> {
        self.cursor.as_mut().unwrap().push_rows_to(builder, length)
    }

    /// Push next row from `self` to the `builder`.
    ///
    /// # Panics
    /// Panics if `self` is EOF.
    fn push_next_row_to(&mut self, builder: &mut BatchBuilder) -> Result<()> {
        self.cursor.as_mut().unwrap().push_next_row_to(builder)
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
        self.compare_first_row(other)
    }
}

/// A reader that would sort and merge `Batch` from multiple sources by key.
///
/// `Batch` from each `Source` **must** be sorted.
struct MergeReader {
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

struct MergeReaderBuilder {
    schema: ProjectedSchemaRef,
    sources: Vec<Source>,
    batch_size: usize,
}

impl MergeReaderBuilder {
    fn new(schema: ProjectedSchemaRef) -> Self {
        MergeReaderBuilder {
            schema,
            sources: Vec::new(),
            batch_size: consts::READ_BATCH_SIZE,
        }
    }

    fn push_batch_iter(mut self, iter: BoxedBatchIterator) -> Self {
        self.sources.push(Source::Iter(iter));
        self
    }

    fn push_batch_reader(mut self, reader: BoxedBatchReader) -> Self {
        self.sources.push(Source::Reader(reader));
        self
    }

    fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    fn build(self) -> MergeReader {
        let num_sources = self.sources.len();

        MergeReader {
            schema: self.schema,
            sources: self.sources,
            hot: BinaryHeap::with_capacity(num_sources),
            cold: BinaryHeap::with_capacity(num_sources),
            batch_size: self.batch_size,
            // FIXME(yingwen): create BatchBuilder.
            batch_builder: BatchBuilder::new(),
        }
    }
}

impl MergeReader {
    async fn maybe_init(&mut self) -> Result<()> {
        if self.sources.is_empty() {
            return Ok(());
        }

        for source in self.sources.drain(..) {
            let node = Node::new(self.schema.clone(), source).await?;

            if !node.is_eof() {
                self.cold.push(node);
            }
        }

        self.refill_hot();

        // The reader has been initialized.
        assert!(self.sources.is_empty());

        Ok(())
    }

    async fn fetch_next_batch(&mut self) -> Result<Option<Batch>> {
        self.maybe_init().await?;

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
            // TODO(yingwen): append rows to builder.
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
        let fetched = node.maybe_fetch_next_batch().await?;

        if node.is_eof() {
            // The merge window would be updated, need to refill the hot heap.
            self.refill_hot();
        } else if fetched {
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
