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

use crate::error::Result;
use crate::memtable::BoxedBatchIterator;
use crate::read::{Batch, BatchReader, BoxedBatchReader, Source};

/// Reader to merge sorted batches.
///
/// The merge reader merges [Batch]es from multiple sources that yield sorted batches.
/// 1. Batch is ordered by primary key, time index, sequence desc, op type desc (we can
/// ignore op type as sequence is already unique).
/// 2. Batch doesn't have duplicate elements (elements with the same primary key and time index).
pub struct MergeReader {
    /// Holds a min-heap for all [Node]s. Each node yields batches from a `source`.
    ///
    /// `Node` in this heap **must** not be EOF.
    nodes: BinaryHeap<Node>,
    /// Batches for the next primary key.
    batch_merger: BatchMerger,
}

#[async_trait]
impl BatchReader for MergeReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        // Collect batches from sources for the same primary key and return
        // the collected batch.
        while !self.nodes.is_empty() {
            // Peek current key.
            let Some(current_key) = self.batch_merger.primary_key() else {
                // The merger is empty, we could push it directly.
                self.take_batch_from_heap().await?;
                // Try next node.
                continue;
            };
            // If next node has a different key, we have finish collecting current key.
            // Safety: node is not empty.
            if self.nodes.peek().unwrap().primary_key() != current_key {
                break;
            }
            // They have the same primary key, we could take it and try next node.
            self.take_batch_from_heap().await?;
        }

        // Merge collected batches.
        self.batch_merger.merge_batches()
    }
}

impl MergeReader {
    /// Creates a new [MergeReader].
    pub async fn new(sources: Vec<Source>) -> Result<MergeReader> {
        let mut nodes = BinaryHeap::with_capacity(sources.len());
        for source in sources {
            let node = Node::new(source).await?;
            if !node.is_eof() {
                // Ensure `nodes` don't have eof node.
                nodes.push(node);
            }
        }

        Ok(MergeReader {
            nodes,
            batch_merger: BatchMerger::new(),
        })
    }

    /// Takes batch from heap top and reheap.
    async fn take_batch_from_heap(&mut self) -> Result<()> {
        let mut next_node = self.nodes.pop().unwrap();
        let batch = next_node.fetch_batch().await?;
        self.batch_merger.push(batch);

        // Insert the node back to the heap.
        // If the node reaches EOF, ignores it. This ensures nodes in the heap is always not EOF.
        if next_node.is_eof() {
            return Ok(());
        }
        self.nodes.push(next_node);

        Ok(())
    }
}

/// Builder to build and initialize a [MergeReader].
#[derive(Default)]
pub struct MergeReaderBuilder {
    /// Input sources.
    ///
    /// All source must yield batches with the same schema.
    sources: Vec<Source>,
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

    /// Push a batch iterator to sources.
    pub fn push_batch_iter(&mut self, iter: BoxedBatchIterator) -> &mut Self {
        self.sources.push(Source::Iter(iter));
        self
    }

    /// Builds and initializes the reader, then resets the builder.
    pub async fn build(&mut self) -> Result<MergeReader> {
        let sources = mem::take(&mut self.sources);
        MergeReader::new(sources).await
    }
}

/// Helper to merge batches for same primary key.
struct BatchMerger {
    /// Buffered non-empty batches to merge.
    batches: Vec<Batch>,
    /// Whether the batch buffer is still sorted.
    is_sorted: bool,
}

impl BatchMerger {
    /// Returns a empty merger.
    fn new() -> BatchMerger {
        BatchMerger {
            batches: Vec::new(),
            is_sorted: true, // An empty merger is always sorted.
        }
    }

    /// Returns the primary key of current merger and `None` if the merger is empty.
    fn primary_key(&self) -> Option<&[u8]> {
        self.batches.first().map(|batch| batch.primary_key())
    }

    /// Push a `batch` into the merger.
    ///
    /// Ignore the `batch` if it is empty.
    ///
    /// # Panics
    /// Panics if the `batch` has another primary key.
    fn push(&mut self, batch: Batch) {
        if batch.is_empty() {
            return;
        }

        if self.batches.is_empty() || !self.is_sorted {
            // Merger is empty or is not sorted, we can push the batch directly.
            self.batches.push(batch);
            return;
        }

        // Merger is sorted, checks whether we can still preserve sorted state.
        let last_batch = self.batches.last().unwrap();
        assert_eq!(last_batch.primary_key(), batch.primary_key());
        match last_batch.last_timestamp().cmp(&batch.first_timestamp()) {
            Ordering::Less => {
                // Still sorted.
                self.batches.push(batch);
                return;
            }
            Ordering::Equal => {
                // Check sequence.
                if last_batch.last_sequence() > batch.first_sequence() {
                    // Still sorted.
                    self.batches.push(batch);
                    return;
                }
            }
            Ordering::Greater => (),
        }

        // Merger is no longer sorted.
        self.batches.push(batch);
        self.is_sorted = false;
    }

    /// Merge all buffered batches and returns the merged batch. Then
    /// reset the buffer.
    fn merge_batches(&mut self) -> Result<Option<Batch>> {
        if self.batches.is_empty() {
            return Ok(None);
        }

        let batches = mem::take(&mut self.batches);
        // Concat all batches.
        let mut batch = Batch::concat(batches)?;

        // TODO(yingwen): metrics for sorted and unsorted batches.
        if !self.is_sorted {
            // Slow path. We need to merge overlapping batches. For simplicity, we
            // just sort the all batches and remove duplications.
            batch.sort_and_dedup()?;
            // We don't need to remove duplications if timestamps of batches
            // are not overlapping.
        }

        // Filter rows by op type. Currently, the reader only removes deleted rows but doesn't filter
        // rows by sequence for simplicity and performance reason.
        batch.filter_deleted()?;

        // Reset merger.
        self.is_sorted = true;

        Ok(Some(batch))
    }
}

/// A `Node` represent an individual input data source to be merged.
struct Node {
    /// Data source of this `Node`.
    source: Source,
    /// Current batch to be read.
    ///
    /// `None` means the `source` has reached EOF.
    current_batch: Option<CompareFirst>,
}

impl Node {
    /// Initialize a node.
    ///
    /// It tries to fetch one batch from the `source`.
    async fn new(mut source: Source) -> Result<Node> {
        let current_batch = source.next_batch().await?.map(CompareFirst);
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
        self.current_batch = self.source.next_batch().await?.map(CompareFirst);
        Ok(current.0)
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

    #[test]
    fn test_batch_merger_empty() {
        let mut merger = BatchMerger::new();
        assert!(merger.merge_batches().unwrap().is_none());
    }

    #[test]
    fn test_batch_merger_unsorted() {
        let mut merger = BatchMerger::new();
        merger.push(new_batch(
            b"k1",
            &[1, 3, 5],
            &[10, 10, 10],
            &[OpType::Put, OpType::Put, OpType::Put],
            &[21, 23, 25],
        ));
        assert!(merger.is_sorted);
        merger.push(new_batch(
            b"k1",
            &[2, 4],
            &[11, 11],
            &[OpType::Put, OpType::Put],
            &[22, 24],
        ));
        assert!(!merger.is_sorted);
        let batch = merger.merge_batches().unwrap().unwrap();
        assert_eq!(
            batch,
            new_batch(
                b"k1",
                &[1, 2, 3, 4, 5],
                &[10, 11, 10, 11, 10],
                &[
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                    OpType::Put
                ],
                &[21, 22, 23, 24, 25]
            )
        );
        assert!(merger.is_sorted);
    }
}
