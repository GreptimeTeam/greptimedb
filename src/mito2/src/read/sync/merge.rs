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

//! Sync merge reader implementation.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::Instant;

use common_telemetry::debug;

use crate::error;
use crate::memtable::BoxedBatchIterator;
use crate::metrics::READ_STAGE_ELAPSED;
use crate::read::{Batch};
use crate::read::merge::{CompareFirst, Metrics};

/// A `Node` represent an individual input data source to be merged.
pub(crate) struct Node {
    /// Data source of this `Node`.
    source: BoxedBatchIterator,
    /// Current batch to be read. The node ensures the batch is not empty.
    ///
    /// `None` means the `source` has reached EOF.
    current_batch: Option<CompareFirst>,
}

impl Node {
    /// Initialize a node.
    ///
    /// It tries to fetch one batch from the `source`.
    pub(crate) fn new(
        mut source: BoxedBatchIterator,
        metrics: &mut Metrics,
    ) -> error::Result<Node> {
        // Ensures batch is not empty.
        let start = Instant::now();
        let current_batch = source.next().transpose()?.map(CompareFirst);
        metrics.fetch_cost += start.elapsed();
        metrics.num_input_rows += current_batch.as_ref().map(|b| b.0.num_rows()).unwrap_or(0);

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
    fn fetch_batch(&mut self, metrics: &mut Metrics) -> error::Result<Batch> {
        let current = self.current_batch.take().unwrap();
        let start = Instant::now();
        // Ensures batch is not empty.
        self.current_batch = self.source.next().transpose()?.map(CompareFirst);
        metrics.fetch_cost += start.elapsed();
        metrics.num_input_rows += self
            .current_batch
            .as_ref()
            .map(|b| b.0.num_rows())
            .unwrap_or(0);
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
    fn skip_rows(&mut self, num_to_skip: usize, metrics: &mut Metrics) -> error::Result<()> {
        let batch = self.current_batch();
        debug_assert!(batch.num_rows() >= num_to_skip);

        let remaining = batch.num_rows() - num_to_skip;
        if remaining == 0 {
            // Nothing remains, we need to fetch next batch to ensure the batch is not empty.
            self.fetch_batch(metrics)?;
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

/// Reader to merge sorted batches.
///
/// The merge reader merges [Batch]es from multiple sources that yield sorted batches.
/// 1. Batch is ordered by primary key, time index, sequence desc, op type desc (we can
///    ignore op type as sequence is already unique).
/// 2. Batches from sources **must** not be empty.
///
/// The reader won't concatenate batches. Each batch returned by the reader also doesn't
/// contain duplicate rows. But the last (primary key, timestamp) of a batch may be the same
/// as the first one in the next batch.
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
    /// Batch to output.
    output_batch: Option<Batch>,
    /// Local metrics.
    metrics: Metrics,
}

impl Drop for MergeReader {
    fn drop(&mut self) {
        debug!("Merge reader(sync) finished, metrics: {:?}", self.metrics);

        READ_STAGE_ELAPSED
            .with_label_values(&["merge"])
            .observe(self.metrics.scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["merge_fetch"])
            .observe(self.metrics.fetch_cost.as_secs_f64());
    }
}

impl Iterator for MergeReader {
    type Item = error::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = Instant::now();
        while !self.hot.is_empty() && self.output_batch.is_none() {
            if self.hot.len() == 1 {
                // No need to do merge sort if only one batch in the hot heap.
                if let Err(e) = self.fetch_batch_from_hottest() {
                    return Some(Err(e));
                }
                self.metrics.num_fetch_by_batches += 1;
            } else {
                // We could only fetch rows that less than the next node from the hottest node.
                if let Err(e) = self.fetch_rows_from_hottest() {
                    return Some(Err(e));
                }
                self.metrics.num_fetch_by_rows += 1;
            }
        }

        if let Some(batch) = self.output_batch.take() {
            self.metrics.scan_cost += start.elapsed();
            self.metrics.num_output_rows += batch.num_rows();
            Some(Ok(batch))
        } else {
            // Nothing fetched.
            self.metrics.scan_cost += start.elapsed();
            None
        }
    }
}

impl MergeReader {
    /// Creates and initializes a new [MergeReader].
    pub fn new(sources: Vec<BoxedBatchIterator>) -> error::Result<MergeReader> {
        let start = Instant::now();
        let mut metrics = Metrics::default();

        let mut cold = BinaryHeap::with_capacity(sources.len());
        let hot = BinaryHeap::with_capacity(sources.len());
        for source in sources {
            let node = Node::new(source, &mut metrics)?;
            if !node.is_eof() {
                // Ensure `cold` don't have eof nodes.
                cold.push(node);
            }
        }

        let mut reader = MergeReader {
            hot,
            cold,
            output_batch: None,
            metrics,
        };
        // Initializes the reader.
        reader.refill_hot();

        reader.metrics.scan_cost += start.elapsed();
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
    fn fetch_batch_from_hottest(&mut self) -> error::Result<()> {
        assert_eq!(1, self.hot.len());

        let mut hottest = self.hot.pop().unwrap();
        let batch = hottest.fetch_batch(&mut self.metrics)?;
        Self::maybe_output_batch(batch, &mut self.output_batch)?;
        self.reheap(hottest)
    }

    /// Fetches non-duplicated rows from the hottest node.
    fn fetch_rows_from_hottest(&mut self) -> error::Result<()> {
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
        let duplicate_pos = match timestamps.binary_search(&next_min_ts.value()) {
            Ok(pos) => pos,
            Err(pos) => {
                // No duplicate timestamp. Outputs timestamp before `pos`.
                Self::maybe_output_batch(top.slice(0, pos), &mut self.output_batch)?;
                top_node.skip_rows(pos, &mut self.metrics)?;
                return self.reheap(top_node);
            }
        };

        // No need to remove duplicate timestamps.
        let output_end = if duplicate_pos == 0 {
            // If the first timestamp of the top node is duplicate. We can simply return the first row
            // as the heap ensure it is the one with largest sequence.
            1
        } else {
            // We don't know which one has the larger sequence so we use the range before
            // the duplicate pos.
            duplicate_pos
        };
        Self::maybe_output_batch(top.slice(0, output_end), &mut self.output_batch)?;
        top_node.skip_rows(output_end, &mut self.metrics)?;
        self.reheap(top_node)
    }

    /// Push the node popped from `hot` back to a proper heap.
    fn reheap(&mut self, node: Node) -> crate::error::Result<()> {
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

    /// If `filter_deleted` is set to true, removes deleted entries and sets the `batch` to the `output_batch`.
    ///
    /// Ignores the `batch` if it is empty.
    fn maybe_output_batch(
        batch: Batch,
        output_batch: &mut Option<Batch>,
    ) -> crate::error::Result<()> {
        debug_assert!(output_batch.is_none());
        if batch.is_empty() {
            return Ok(());
        }
        *output_batch = Some(batch);

        Ok(())
    }
}
