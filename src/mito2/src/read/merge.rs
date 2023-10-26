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

//! Merge reader implementation.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::mem;

use async_trait::async_trait;
use common_time::Timestamp;

use crate::error::Result;
use crate::memtable::BoxedBatchIterator;
use crate::read::{Batch, BatchReader, BoxedBatchReader, Source};

/// Minimum batch size to output.
const MIN_BATCH_SIZE: usize = 64;

/// Reader to merge sorted batches.
///
/// The merge reader merges [Batch]es from multiple sources that yield sorted batches.
/// 1. Batch is ordered by primary key, time index, sequence desc, op type desc (we can
/// ignore op type as sequence is already unique).
/// 2. Batch doesn't have duplicate elements (elements with the same primary key and time index).
pub struct MergeReader {
    /// Holds [Node]s whose key range of current batch **is** overlapped with the merge window.
    /// Each node yields batches from a `source`.
    ///
    /// [Node] in this heap **must** not be empty. A `merge window` is the (primary key, timestamp)
    /// range of the **root node** in the `hot` heap.
    hot: BinaryHeap<Node>,
    /// Holds `Node` whose key range of current batch **isn't** overlapped with the merge window.
    ///
    /// `Node` in this heap **must** not be empty.
    cold: BinaryHeap<Node>,
    /// Batches to output.
    batch_merger: BatchMerger,
    /// Suggested size of each batch. The batch returned by the reader can have more rows than the
    /// batch size.
    batch_size: usize,
}

#[async_trait]
impl BatchReader for MergeReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        while !self.hot.is_empty() && self.batch_merger.num_rows() < self.batch_size {
            if let Some(current_key) = self.batch_merger.primary_key() {
                // If the hottest node has a different key, we have finish collecting current key.
                // Safety: hot is not empty.
                if self.hot.peek().unwrap().primary_key() != current_key {
                    break;
                }
            }

            if self.hot.len() == 1 {
                // No need to do merge sort if only one batch in the hot heap.
                self.fetch_batch_from_hottest().await?;
            } else {
                // We could only fetch rows that less than the next node from the hottest node.
                self.fetch_rows_from_hottest().await?;
            }
        }

        if self.batch_merger.is_empty() {
            // Nothing fetched.
            Ok(None)
        } else {
            self.batch_merger.merge_batches()
        }
    }
}

impl MergeReader {
    /// Creates and initializes a new [MergeReader].
    pub async fn new(sources: Vec<Source>, batch_size: usize) -> Result<MergeReader> {
        let mut cold = BinaryHeap::with_capacity(sources.len());
        let hot = BinaryHeap::with_capacity(sources.len());
        for source in sources {
            let node = Node::new(source).await?;
            if !node.is_eof() {
                // Ensure `cold` don't have eof nodes.
                cold.push(node);
            }
        }

        let mut reader = MergeReader {
            hot,
            cold,
            batch_merger: BatchMerger::new(),
            batch_size,
        };
        // Initializes the reader.
        reader.refill_hot();

        Ok(reader)
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

    /// Fetches one batch from the hottest node.
    async fn fetch_batch_from_hottest(&mut self) -> Result<()> {
        assert_eq!(1, self.hot.len());

        let mut hottest = self.hot.pop().unwrap();
        let batch = hottest.fetch_batch().await?;
        self.batch_merger.push(batch)?;
        self.reheap(hottest)
    }

    /// Fetches non-duplicated rows from the hottest node and skips the timestamp duplicated
    /// with the first timestamp in the next node.
    async fn fetch_rows_from_hottest(&mut self) -> Result<()> {
        // Safety: `fetch_batches_to_output()` ensures the hot heap has more than 1 element.
        // Pop hottest node.
        let mut top_node = self.hot.pop().unwrap();
        let top = top_node.current_batch();
        // Min timestamp and its sequence in the next batch.
        let next_min_ts = {
            let next_node = self.hot.peek().unwrap();
            let next = next_node.current_batch();
            // top and next have overlapping rows so they must have same primary keys.
            debug_assert_eq!(top.primary_key(), next.primary_key());
            // Safety: Batches in the heap is not empty, so we can use unwrap here.
            next.first_timestamp().unwrap()
        };

        // Safety: Batches in the heap is not empty, so we can use unwrap here.
        let timestamps = top.timestamps_native().unwrap();
        // Binary searches the timestamp in the top batch.
        // Safety: Batches should have the same timestamp resolution so we can compare the native
        // value directly.
        match timestamps.binary_search(&next_min_ts.value()) {
            Ok(pos) => {
                // They have duplicate timestamps. Outputs timestamps before the duplciated timestamp.
                // Batch itself doesn't contain duplicate timestamps so timestamps before `pos`
                // must be less than `next_min_ts`.
                self.batch_merger.push(top.slice(0, pos))?;
                // This keep the duplicate timestamp in the node.
                top_node.skip_rows(pos).await?;
                // The merge window should contain this timestamp so only nodes in the hot heap
                // have this timestamp.
                self.filter_first_duplicate_timestamp_in_hot(top_node, next_min_ts)
                    .await?;
            }
            Err(pos) => {
                // No duplicate timestamp. Outputs timestamp before `pos`.
                self.batch_merger.push(top.slice(0, pos))?;
                top_node.skip_rows(pos).await?;
                self.reheap(top_node)?;
            }
        }

        Ok(())
    }

    /// Filters the first duplicate `timestamp` in `top_node` and `hot` heap. Only keeps the timestamp
    /// with the maximum sequence.
    async fn filter_first_duplicate_timestamp_in_hot(
        &mut self,
        top_node: Node,
        timestamp: Timestamp,
    ) -> Result<()> {
        debug_assert_eq!(
            top_node.current_batch().first_timestamp().unwrap(),
            timestamp
        );

        // The node with maximum sequence.
        let mut max_seq_node = top_node;
        let mut max_seq = max_seq_node.current_batch().first_sequence().unwrap();
        while let Some(mut next_node) = self.hot.pop() {
            // Safety: Batches in the heap is not empty.
            let next_first_ts = next_node.current_batch().first_timestamp().unwrap();
            let next_first_seq = next_node.current_batch().first_sequence().unwrap();

            if next_first_ts != timestamp {
                // We are done, push the node with max seq.
                self.cold.push(next_node);
                break;
            }

            if max_seq < next_first_seq {
                // The next node has larger seq.
                max_seq_node.skip_rows(1).await?;
                if !max_seq_node.is_eof() {
                    self.cold.push(max_seq_node);
                }
                max_seq_node = next_node;
                max_seq = next_first_seq;
            } else {
                next_node.skip_rows(1).await?;
                if !next_node.is_eof() {
                    // If the next node is
                    self.cold.push(next_node);
                }
            }
        }
        debug_assert!(!max_seq_node.is_eof());
        self.cold.push(max_seq_node);

        // The merge window is updated, we need to refill the hot heap.
        self.refill_hot();

        Ok(())
    }

    /// Push the node popped from `hot` back to a proper heap.
    fn reheap(&mut self, node: Node) -> Result<()> {
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

        Ok(())
    }
}

/// Builder to build and initialize a [MergeReader].
pub struct MergeReaderBuilder {
    /// Input sources.
    ///
    /// All source must yield batches with the same schema.
    sources: Vec<Source>,
    /// Batch size of the reader.
    batch_size: usize,
}

impl MergeReaderBuilder {
    /// Returns an empty builder.
    pub fn new() -> MergeReaderBuilder {
        MergeReaderBuilder::default()
    }

    /// Pushes a batch reader to sources.
    pub fn push_batch_reader(&mut self, reader: BoxedBatchReader) -> &mut Self {
        self.sources.push(Source::Reader(reader));
        self
    }

    /// Pushes a batch iterator to sources.
    pub fn push_batch_iter(&mut self, iter: BoxedBatchIterator) -> &mut Self {
        self.sources.push(Source::Iter(iter));
        self
    }

    /// Sets the batch size of the reader.
    pub fn batch_size(&mut self, size: usize) -> &mut Self {
        self.batch_size = if size == 0 { MIN_BATCH_SIZE } else { size };
        self
    }

    /// Builds and initializes the reader, then resets the builder.
    pub async fn build(&mut self) -> Result<MergeReader> {
        let sources = mem::take(&mut self.sources);
        MergeReader::new(sources, self.batch_size).await
    }
}

impl Default for MergeReaderBuilder {
    fn default() -> Self {
        MergeReaderBuilder {
            sources: Vec::new(),
            batch_size: MIN_BATCH_SIZE,
        }
    }
}

/// Helper to collect and merge small batches for same primary key.
struct BatchMerger {
    /// Buffered non-empty batches to merge.
    batches: Vec<Batch>,
    /// Number of rows in the batch.
    num_rows: usize,
}

impl BatchMerger {
    /// Returns a empty merger.
    fn new() -> BatchMerger {
        BatchMerger {
            batches: Vec::new(),
            num_rows: 0,
        }
    }

    /// Returns the number of rows.
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns true if the merger is empty.
    fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Returns the primary key of current merger and `None` if the merger is empty.
    fn primary_key(&self) -> Option<&[u8]> {
        self.batches.first().map(|batch| batch.primary_key())
    }

    /// Removeds deleted entries and pushes a `batch` into the merger.
    ///
    /// Ignores the `batch` if it is empty.
    ///
    /// # Panics
    /// Panics if the `batch` has another primary key.
    fn push(&mut self, mut batch: Batch) -> Result<()> {
        debug_assert!(self
            .batches
            .last()
            .map(|b| b.primary_key() == batch.primary_key())
            .unwrap_or(true));

        batch.filter_deleted()?;
        if batch.is_empty() {
            return Ok(());
        }

        self.num_rows += batch.num_rows();
        self.batches.push(batch);

        Ok(())
    }

    /// Merge all buffered batches and returns the merged batch. Then
    /// reset the buffer.
    fn merge_batches(&mut self) -> Result<Option<Batch>> {
        if self.batches.is_empty() {
            return Ok(None);
        }

        // Reset number of rows.
        self.num_rows = 0;
        if self.batches.len() == 0 {
            return Ok(self.batches.pop());
        }
        let batches = mem::take(&mut self.batches);
        Batch::concat(batches).map(Some)
    }
}

/// A `Node` represent an individual input data source to be merged.
struct Node {
    /// Data source of this `Node`.
    source: Source,
    /// Current batch to be read. The node ensures the batch is not empty.
    ///
    /// `None` means the `source` has reached EOF.
    current_batch: Option<CompareFirst>,
}

impl Node {
    /// Initialize a node.
    ///
    /// It tries to fetch one batch from the `source`.
    async fn new(mut source: Source) -> Result<Node> {
        // Ensures batch is not empty.
        let current_batch = source.next_non_empty_batch().await?.map(CompareFirst);
        Ok(Node {
            source,
            current_batch,
        })
    }

    /// Returns whether the node still has batch to read.
    fn is_eof(&self) -> bool {
        self.current_batch.is_none()
    }

    /// Returns the primary key of current batch.
    ///
    /// # Panics
    /// Panics if the node has reached EOF.
    fn primary_key(&self) -> &[u8] {
        self.current_batch().primary_key()
    }

    /// Returns current batch.
    ///
    /// # Panics
    /// Panics if the node has reached EOF.
    fn current_batch(&self) -> &Batch {
        &self.current_batch.as_ref().unwrap().0
    }

    /// Returns current batch and fetches next batch
    /// from the source.
    ///
    /// # Panics
    /// Panics if the node has reached EOF.
    async fn fetch_batch(&mut self) -> Result<Batch> {
        let current = self.current_batch.take().unwrap();
        // Ensures batch is not empty.
        self.current_batch = self.source.next_non_empty_batch().await?.map(CompareFirst);
        Ok(current.0)
    }

    /// Returns true if the key range of current batch in `self` is behind (exclusive) current
    /// batch in `other`.
    ///
    /// # Panics
    /// Panics if either `self` or `other` is EOF.
    fn is_behind(&self, other: &Node) -> bool {
        debug_assert!(!self.current_batch().is_empty());
        debug_assert!(!other.current_batch().is_empty());

        // We only compare pk and timestamp so nodes in the cold
        // heap don't have overlapping timestamps with the hottest node
        // in the hot heap.
        self.primary_key().cmp(other.primary_key()).then_with(|| {
            self.current_batch()
                .first_timestamp()
                .cmp(&other.current_batch().last_timestamp())
        }) == Ordering::Greater
    }

    /// Skips first `num_to_skip` rows from node's current batch. If current batch is empty it fetches
    /// next batch from the node.
    ///
    /// # Panics
    /// Panics if the node is EOF.
    async fn skip_rows(&mut self, num_to_skip: usize) -> Result<()> {
        let batch = self.current_batch();
        debug_assert!(batch.num_rows() >= num_to_skip);
        let remaining = batch.num_rows() - num_to_skip;
        if remaining == 0 {
            // Nothing remains, we need to fetch next batch to ensure the batch is not empty.
            self.fetch_batch().await?;
        } else {
            debug_assert!(!batch.is_empty());
            self.current_batch = Some(CompareFirst(batch.slice(num_to_skip, remaining)));
        }

        Ok(())
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.current_batch == other.current_batch
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
        other.current_batch.cmp(&self.current_batch)
    }
}

/// Type to compare [Batch] by first row.
///
/// It ignores op type as sequence is enough to distinguish different rows.
struct CompareFirst(Batch);

impl PartialEq for CompareFirst {
    fn eq(&self, other: &Self) -> bool {
        self.0.primary_key() == other.0.primary_key()
            && self.0.first_timestamp() == other.0.first_timestamp()
            && self.0.first_sequence() == other.0.first_sequence()
    }
}

impl Eq for CompareFirst {}

impl PartialOrd for CompareFirst {
    fn partial_cmp(&self, other: &CompareFirst) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompareFirst {
    /// Compares by primary key, time index, sequence desc.
    fn cmp(&self, other: &CompareFirst) -> Ordering {
        self.0
            .primary_key()
            .cmp(other.0.primary_key())
            .then_with(|| self.0.first_timestamp().cmp(&other.0.first_timestamp()))
            .then_with(|| other.0.first_sequence().cmp(&self.0.first_sequence()))
    }
}

#[cfg(test)]
mod tests {
    use api::v1::OpType;

    use super::*;
    use crate::test_util::{check_reader_result, new_batch, VecBatchReader};

    #[tokio::test]
    async fn test_merge_reader_empty() {
        let mut reader = MergeReaderBuilder::new().build().await.unwrap();
        assert!(reader.next_batch().await.unwrap().is_none());
        assert!(reader.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_merge_non_overlapping() {
        let reader1 = VecBatchReader::new(&[
            new_batch(
                b"k1",
                &[1, 2],
                &[11, 12],
                &[OpType::Put, OpType::Put],
                &[21, 22],
            ),
            new_batch(
                b"k1",
                &[7, 8],
                &[17, 18],
                &[OpType::Put, OpType::Delete],
                &[27, 28],
            ),
            new_batch(
                b"k2",
                &[2, 3],
                &[12, 13],
                &[OpType::Delete, OpType::Put],
                &[22, 23],
            ),
        ]);
        let reader2 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[4, 5],
            &[14, 15],
            &[OpType::Put, OpType::Put],
            &[24, 25],
        )]);
        let mut reader = MergeReaderBuilder::new()
            .push_batch_reader(Box::new(reader1))
            .push_batch_iter(Box::new(reader2))
            .build()
            .await
            .unwrap();
        check_reader_result(
            &mut reader,
            &[
                new_batch(
                    b"k1",
                    &[1, 2, 4, 5, 7],
                    &[11, 12, 14, 15, 17],
                    &[
                        OpType::Put,
                        OpType::Put,
                        OpType::Put,
                        OpType::Put,
                        OpType::Put,
                    ],
                    &[21, 22, 24, 25, 27],
                ),
                new_batch(b"k2", &[3], &[13], &[OpType::Put], &[23]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_overlapping() {
        let reader1 = VecBatchReader::new(&[
            new_batch(
                b"k1",
                &[1, 2],
                &[11, 12],
                &[OpType::Put, OpType::Put],
                &[21, 22],
            ),
            new_batch(
                b"k1",
                &[4, 5],
                &[14, 15],
                // This override 4 and deletes 5.
                &[OpType::Put, OpType::Delete],
                &[24, 25],
            ),
            new_batch(
                b"k2",
                &[2, 3],
                &[12, 13],
                // This delete 2.
                &[OpType::Delete, OpType::Put],
                &[22, 23],
            ),
        ]);
        let reader2 = VecBatchReader::new(&[
            new_batch(
                b"k1",
                &[3, 4, 5],
                &[10, 10, 10],
                &[OpType::Put, OpType::Put, OpType::Put],
                &[33, 34, 35],
            ),
            new_batch(
                b"k2",
                &[1, 10],
                &[11, 20],
                &[OpType::Put, OpType::Put],
                &[21, 30],
            ),
        ]);
        let mut reader = MergeReaderBuilder::new()
            .push_batch_reader(Box::new(reader1))
            .push_batch_iter(Box::new(reader2))
            .build()
            .await
            .unwrap();
        check_reader_result(
            &mut reader,
            &[
                new_batch(
                    b"k1",
                    &[1, 2, 3, 4],
                    &[11, 12, 10, 14],
                    &[OpType::Put, OpType::Put, OpType::Put, OpType::Put],
                    &[21, 22, 33, 24],
                ),
                new_batch(
                    b"k2",
                    &[1, 3, 10],
                    &[11, 13, 20],
                    &[OpType::Put, OpType::Put, OpType::Put],
                    &[21, 23, 30],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_empty() {
        let reader1 = VecBatchReader::new(&[new_batch(b"k1", &[], &[], &[], &[])]);
        let reader2 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[1, 2],
            &[11, 12],
            &[OpType::Put, OpType::Put],
            &[21, 22],
        )]);
        let mut reader = MergeReaderBuilder::new()
            .push_batch_reader(Box::new(reader1))
            .push_batch_iter(Box::new(reader2))
            .build()
            .await
            .unwrap();
        check_reader_result(
            &mut reader,
            &[new_batch(
                b"k1",
                &[1, 2],
                &[11, 12],
                &[OpType::Put, OpType::Put],
                &[21, 22],
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_empty_2() {
        let reader1 = VecBatchReader::new(&[
            new_batch(b"k1", &[1], &[11], &[OpType::Put], &[21]),
            new_batch(b"k1", &[], &[], &[], &[]),
            new_batch(b"k1", &[2], &[11], &[OpType::Put], &[22]),
        ]);
        let reader2 = VecBatchReader::new(&[
            new_batch(
                b"k1",
                &[1, 2],
                &[10, 12],
                &[OpType::Put, OpType::Put],
                &[31, 32],
            ),
            new_batch(b"k1", &[], &[], &[], &[]),
        ]);
        let mut reader = MergeReaderBuilder::new()
            .push_batch_reader(Box::new(reader1))
            .push_batch_iter(Box::new(reader2))
            .build()
            .await
            .unwrap();
        check_reader_result(
            &mut reader,
            &[new_batch(
                b"k1",
                &[1, 2],
                &[11, 12],
                &[OpType::Put, OpType::Put],
                &[21, 32],
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_deleted() {
        let reader1 = VecBatchReader::new(&[
            new_batch(
                b"k1",
                &[1, 2],
                &[11, 12],
                &[OpType::Delete, OpType::Delete],
                &[21, 22],
            ),
            new_batch(
                b"k2",
                &[2, 3],
                &[12, 13],
                &[OpType::Delete, OpType::Put],
                &[22, 23],
            ),
        ]);
        let reader2 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[4, 5],
            &[14, 15],
            &[OpType::Delete, OpType::Delete],
            &[24, 25],
        )]);
        let mut reader = MergeReaderBuilder::new()
            .push_batch_reader(Box::new(reader1))
            .push_batch_iter(Box::new(reader2))
            .build()
            .await
            .unwrap();
        check_reader_result(
            &mut reader,
            &[new_batch(b"k2", &[3], &[13], &[OpType::Put], &[23])],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_next_node_empty() {
        let reader1 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[1, 2],
            &[11, 12],
            &[OpType::Put, OpType::Put],
            &[21, 22],
        )]);
        // This reader will be empty after skipping the timestamp.
        let reader2 = VecBatchReader::new(&[new_batch(b"k1", &[1], &[10], &[OpType::Put], &[33])]);
        let mut reader = MergeReaderBuilder::new()
            .push_batch_reader(Box::new(reader1))
            .push_batch_iter(Box::new(reader2))
            .build()
            .await
            .unwrap();
        check_reader_result(
            &mut reader,
            &[new_batch(
                b"k1",
                &[1, 2],
                &[11, 12],
                &[OpType::Put, OpType::Put],
                &[21, 22],
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_top_node_empty() {
        // This reader will be empty after skipping the timestamp 2.
        let reader1 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[1, 2],
            &[10, 10],
            &[OpType::Put, OpType::Put],
            &[21, 22],
        )]);
        let reader2 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[2, 3],
            &[11, 11],
            &[OpType::Put, OpType::Put],
            &[32, 33],
        )]);
        let mut reader = MergeReaderBuilder::new()
            .push_batch_reader(Box::new(reader1))
            .push_batch_iter(Box::new(reader2))
            .build()
            .await
            .unwrap();
        check_reader_result(
            &mut reader,
            &[new_batch(
                b"k1",
                &[1, 2, 3],
                &[10, 11, 11],
                &[OpType::Put, OpType::Put, OpType::Put],
                &[21, 32, 33],
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_large_range() {
        let reader1 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[1, 10],
            &[10, 10],
            &[OpType::Put, OpType::Put],
            &[21, 30],
        )]);
        let reader2 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[1, 20],
            &[11, 11],
            &[OpType::Put, OpType::Put],
            &[31, 40],
        )]);
        // The hot heap have a node that doesn't have duplicate
        // timestamps.
        let reader3 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[6, 8],
            &[11, 11],
            &[OpType::Put, OpType::Put],
            &[36, 38],
        )]);
        let mut reader = MergeReaderBuilder::new()
            .push_batch_reader(Box::new(reader1))
            .push_batch_iter(Box::new(reader2))
            .push_batch_reader(Box::new(reader3))
            .build()
            .await
            .unwrap();
        check_reader_result(
            &mut reader,
            &[new_batch(
                b"k1",
                &[1, 6, 8, 10, 20],
                &[11, 11, 11, 10, 11],
                &[
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                ],
                &[31, 36, 38, 30, 40],
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_more_than_batch_size() {
        let batches: Vec<_> = (0..MIN_BATCH_SIZE as i64 * 2)
            .map(|ts| new_batch(b"k1", &[ts], &[10], &[OpType::Put], &[100]))
            .collect();
        let reader = VecBatchReader::new(&batches);
        let mut reader = MergeReaderBuilder::new()
            .push_batch_reader(Box::new(reader))
            // Still use the default batch size.
            .batch_size(0)
            .build()
            .await
            .unwrap();
        let ts1: Vec<_> = (0..MIN_BATCH_SIZE as i64).collect();
        let ts2: Vec<_> = (MIN_BATCH_SIZE as i64..MIN_BATCH_SIZE as i64 * 2).collect();
        let seqs = vec![10; MIN_BATCH_SIZE];
        let op_types = vec![OpType::Put; MIN_BATCH_SIZE];
        let fields = vec![100; MIN_BATCH_SIZE];
        check_reader_result(
            &mut reader,
            &[
                new_batch(b"k1", &ts1, &seqs, &op_types, &fields),
                new_batch(b"k1", &ts2, &seqs, &op_types, &fields),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_more_than_batch_size_overlapping() {
        let reader1 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[11, 10, 11, 10, 11, 10, 11, 10, 11],
            &[
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
            ],
            &[21, 22, 23, 24, 25, 26, 27, 28, 29],
        )]);
        let reader2 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[10, 11, 10, 11, 10, 11, 10, 11, 10],
            &[
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
            ],
            &[31, 32, 33, 34, 35, 36, 37, 38, 39],
        )]);
        let mut reader = MergeReaderBuilder::new()
            .push_batch_iter(Box::new(reader1))
            .push_batch_reader(Box::new(reader2))
            .batch_size(3)
            .build()
            .await
            .unwrap();
        check_reader_result(
            &mut reader,
            &[
                new_batch(
                    b"k1",
                    &[1, 2, 3],
                    &[11, 11, 11],
                    &[OpType::Put, OpType::Put, OpType::Put],
                    &[21, 32, 23],
                ),
                new_batch(
                    b"k1",
                    &[4, 5, 6],
                    &[11, 11, 11],
                    &[OpType::Put, OpType::Put, OpType::Put],
                    &[34, 25, 36],
                ),
                new_batch(
                    b"k1",
                    &[7, 8, 9],
                    &[11, 11, 11],
                    &[OpType::Put, OpType::Put, OpType::Put],
                    &[27, 38, 29],
                ),
            ],
        )
        .await;
    }

    #[test]
    fn test_batch_merger_empty() {
        let mut merger = BatchMerger::new();
        assert!(merger.is_empty());
        assert!(merger.merge_batches().unwrap().is_none());
        assert!(merger.primary_key().is_none());
    }

    #[test]
    fn test_merge_batches() {
        let mut merger = BatchMerger::new();
        merger
            .push(new_batch(b"k1", &[1], &[10], &[OpType::Put], &[21]))
            .unwrap();
        assert_eq!(1, merger.num_rows());
        assert!(!merger.is_empty());
        merger
            .push(new_batch(b"k1", &[2], &[10], &[OpType::Put], &[22]))
            .unwrap();
        assert_eq!(2, merger.num_rows());
        let batch = merger.merge_batches().unwrap().unwrap();
        assert_eq!(2, batch.num_rows());
        assert_eq!(
            batch,
            new_batch(
                b"k1",
                &[1, 2],
                &[10, 10],
                &[OpType::Put, OpType::Put,],
                &[21, 22]
            )
        );
        assert!(merger.is_empty());
    }
}
