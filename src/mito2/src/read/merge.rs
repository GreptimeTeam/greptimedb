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

use crate::error::Result;
use crate::read::{Batch, Source};

/// Reader to merge sorted batches.
///
/// The merge reader merges [Batch]es from multiple sources that yields sorted batches.
/// Batches from each source **must** obey the following rules:
/// 1. Batch is ordered by primary key, time index, sequence desc, op type desc (we can
/// ignore op type as sequence is already unique).
/// 2. Batch doesn't have duplicate elements (element with same key).
pub struct MergeReader {
    /// Whether the reader has been initialized.
    initialized: bool,
    // TODO(yingwen): Init nodes in builder.
    /// Input sources.
    ///
    /// All source must yield batches with the same schema. Initialize the reader would
    /// convert all `Source`s into `Node`s and then clear this vector.
    sources: Vec<Source>,
    /// Holds a min-heap for all [Node]s. Each node yields batches from a `source`.
    ///
    /// `Node` in this heap **must** not be EOF.
    nodes: BinaryHeap<Node>,
    /// Batches for the next primary key.
    batch_merger: BatchMerger,
}

impl MergeReader {
    /// Collect batches from sources for the same primary key and return
    /// the collected batch.
    async fn collect_batches_for_same_key(&mut self) -> Result<Option<Batch>> {
        loop {
            // Peek next node from heap.
            let Some(next_node) = self.nodes.peek() else {
                // heap is empty.
                break;
            };
            // Peek current key.
            let Some(current_key) = self.batch_merger.primary_key() else {
                // The merger is empty, we could push it directly.
                self.take_batch_from_heap().await?;
                // Try next node.
                continue;
            };
            // If next node has a different key, we have finish collecting current key.
            if next_node.primary_key() != current_key {
                break;
            }
            // They have the same primary key, we could take it and try next node.
            self.take_batch_from_heap().await?;
        }

        // Merge collected batches.
        self.batch_merger.merge_batches()
    }

    /// Insert a node back to the heap.
    ///
    /// If the node reaches EOF, ignores it. This ensures nodes in the heap is always not EOF.
    fn reheap(&mut self, node: Node) {
        if node.is_eof() {
            return;
        }

        self.nodes.push(node);
    }

    /// Takes batch from heap top and reheap.
    async fn take_batch_from_heap(&mut self) -> Result<()> {
        let next_node = self.nodes.pop().unwrap();
        let batch = next_node.fetch_batch().await?;
        self.batch_merger.push(batch);
        self.reheap(next_node);

        Ok(())
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

        let batches = std::mem::take(&mut self.batches);
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

        // Filter rows by op type.
        batch.filter_deleted()?;

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
