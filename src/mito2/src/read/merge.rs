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
use std::time::{Duration, Instant};

use async_trait::async_trait;
use common_telemetry::debug;
use common_time::Timestamp;

use crate::error::Result;
use crate::memtable::BoxedBatchIterator;
use crate::metrics::{MERGE_FILTER_ROWS_TOTAL, READ_STAGE_ELAPSED};
use crate::read::{Batch, BatchReader, BoxedBatchReader, Source};

/// Reader to merge sorted batches.
///
/// The merge reader merges [Batch]es from multiple sources that yield sorted batches.
/// 1. Batch is ordered by primary key, time index, sequence desc, op type desc (we can
/// ignore op type as sequence is already unique).
/// 2. Batch doesn't have duplicate elements (elements with the same primary key and time index).
/// 3. Batches from sources **must** not be empty.
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

#[async_trait]
impl BatchReader for MergeReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let start = Instant::now();
        while !self.hot.is_empty() && self.output_batch.is_none() {
            if self.hot.len() == 1 {
                // No need to do merge sort if only one batch in the hot heap.
                self.fetch_batch_from_hottest().await?;
                self.metrics.num_fetch_by_batches += 1;
            } else {
                // We could only fetch rows that less than the next node from the hottest node.
                self.fetch_rows_from_hottest().await?;
                self.metrics.num_fetch_by_rows += 1;
            }
        }

        if let Some(batch) = self.output_batch.take() {
            self.metrics.scan_cost += start.elapsed();
            self.metrics.num_output_rows += batch.num_rows();
            Ok(Some(batch))
        } else {
            // Nothing fetched.
            self.metrics.scan_cost += start.elapsed();
            Ok(None)
        }
    }
}

impl Drop for MergeReader {
    fn drop(&mut self) {
        debug!("Merge reader finished, metrics: {:?}", self.metrics);

        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["dedup"])
            .inc_by(self.metrics.num_duplicate_rows as u64);
        MERGE_FILTER_ROWS_TOTAL
            .with_label_values(&["delete"])
            .inc_by(self.metrics.num_deleted_rows as u64);
        READ_STAGE_ELAPSED
            .with_label_values(&["merge"])
            .observe(self.metrics.scan_cost.as_secs_f64());
    }
}

impl MergeReader {
    /// Creates and initializes a new [MergeReader].
    pub async fn new(sources: Vec<Source>) -> Result<MergeReader> {
        let start = Instant::now();
        let mut metrics = Metrics::default();

        let mut cold = BinaryHeap::with_capacity(sources.len());
        let hot = BinaryHeap::with_capacity(sources.len());
        for source in sources {
            let node = Node::new(source, &mut metrics).await?;
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
    async fn fetch_batch_from_hottest(&mut self) -> Result<()> {
        assert_eq!(1, self.hot.len());

        let mut hottest = self.hot.pop().unwrap();
        let batch = hottest.fetch_batch(&mut self.metrics).await?;
        Self::maybe_output_batch(batch, &mut self.output_batch, &mut self.metrics)?;
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
                // They have duplicate timestamps. Outputs timestamps before the duplicated timestamp.
                // Batch itself doesn't contain duplicate timestamps so timestamps before `pos`
                // must be less than `next_min_ts`.
                Self::maybe_output_batch(
                    top.slice(0, pos),
                    &mut self.output_batch,
                    &mut self.metrics,
                )?;
                // This keep the duplicate timestamp in the node.
                top_node.skip_rows(pos, &mut self.metrics).await?;
                // The merge window should contain this timestamp so only nodes in the hot heap
                // have this timestamp.
                self.filter_first_duplicate_timestamp_in_hot(top_node, next_min_ts)
                    .await?;
            }
            Err(pos) => {
                // No duplicate timestamp. Outputs timestamp before `pos`.
                Self::maybe_output_batch(
                    top.slice(0, pos),
                    &mut self.output_batch,
                    &mut self.metrics,
                )?;
                top_node.skip_rows(pos, &mut self.metrics).await?;
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
                max_seq_node.skip_rows(1, &mut self.metrics).await?;
                self.metrics.num_duplicate_rows += 1;
                if !max_seq_node.is_eof() {
                    self.cold.push(max_seq_node);
                }
                max_seq_node = next_node;
                max_seq = next_first_seq;
            } else {
                next_node.skip_rows(1, &mut self.metrics).await?;
                self.metrics.num_duplicate_rows += 1;
                if !next_node.is_eof() {
                    // If the next node has smaller seq, skip that row.
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

    /// Removeds deleted entries and sets the `batch` to the `output_batch`.
    ///
    /// Ignores the `batch` if it is empty.
    fn maybe_output_batch(
        mut batch: Batch,
        output_batch: &mut Option<Batch>,
        metrics: &mut Metrics,
    ) -> Result<()> {
        debug_assert!(output_batch.is_none());

        let num_rows = batch.num_rows();
        batch.filter_deleted()?;
        // Update deleted rows metrics.
        metrics.num_deleted_rows += num_rows - batch.num_rows();
        if batch.is_empty() {
            return Ok(());
        }
        *output_batch = Some(batch);

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

    /// Creates a builder from sources.
    pub fn from_sources(sources: Vec<Source>) -> MergeReaderBuilder {
        MergeReaderBuilder { sources }
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

    /// Builds and initializes the reader, then resets the builder.
    pub async fn build(&mut self) -> Result<MergeReader> {
        let sources = mem::take(&mut self.sources);
        MergeReader::new(sources).await
    }
}

/// Metrics for the merge reader.
#[derive(Debug, Default)]
struct Metrics {
    /// Total scan cost of the reader.
    scan_cost: Duration,
    /// Number of times to fetch batches.
    num_fetch_by_batches: usize,
    /// Number of times to fetch rows.
    num_fetch_by_rows: usize,
    /// Number of input rows.
    num_input_rows: usize,
    /// Number of skipped duplicate rows.
    num_duplicate_rows: usize,
    /// Number of output rows.
    num_output_rows: usize,
    /// Number of deleted rows.
    num_deleted_rows: usize,
    /// Cost to fetch batches from sources.
    fetch_cost: Duration,
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
    async fn new(mut source: Source, metrics: &mut Metrics) -> Result<Node> {
        // Ensures batch is not empty.
        let start = Instant::now();
        let current_batch = source.next_batch().await?.map(CompareFirst);
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
    async fn fetch_batch(&mut self, metrics: &mut Metrics) -> Result<Batch> {
        let current = self.current_batch.take().unwrap();
        let start = Instant::now();
        // Ensures batch is not empty.
        self.current_batch = self.source.next_batch().await?.map(CompareFirst);
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
    async fn skip_rows(&mut self, num_to_skip: usize, metrics: &mut Metrics) -> Result<()> {
        let batch = self.current_batch();
        debug_assert!(batch.num_rows() >= num_to_skip);

        let remaining = batch.num_rows() - num_to_skip;
        if remaining == 0 {
            // Nothing remains, we need to fetch next batch to ensure the batch is not empty.
            self.fetch_batch(metrics).await?;
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
                    &[1, 2],
                    &[11, 12],
                    &[OpType::Put, OpType::Put],
                    &[21, 22],
                ),
                new_batch(
                    b"k1",
                    &[4, 5],
                    &[14, 15],
                    &[OpType::Put, OpType::Put],
                    &[24, 25],
                ),
                new_batch(b"k1", &[7], &[17], &[OpType::Put], &[27]),
                new_batch(b"k2", &[3], &[13], &[OpType::Put], &[23]),
            ],
        )
        .await;

        assert_eq!(8, reader.metrics.num_input_rows);
        assert_eq!(6, reader.metrics.num_output_rows);
        assert_eq!(2, reader.metrics.num_deleted_rows);
    }

    #[tokio::test]
    async fn test_merge_reheap_hot() {
        let reader1 = VecBatchReader::new(&[
            new_batch(
                b"k1",
                &[1, 3],
                &[10, 10],
                &[OpType::Put, OpType::Put],
                &[21, 23],
            ),
            new_batch(b"k2", &[3], &[10], &[OpType::Put], &[23]),
        ]);
        let reader2 = VecBatchReader::new(&[new_batch(
            b"k1",
            &[2, 4],
            &[11, 11],
            &[OpType::Put, OpType::Put],
            &[32, 34],
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
                new_batch(b"k1", &[1], &[10], &[OpType::Put], &[21]),
                new_batch(b"k1", &[2], &[11], &[OpType::Put], &[32]),
                new_batch(b"k1", &[3], &[10], &[OpType::Put], &[23]),
                new_batch(b"k1", &[4], &[11], &[OpType::Put], &[34]),
                new_batch(b"k2", &[3], &[10], &[OpType::Put], &[23]),
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
                    &[1, 2],
                    &[11, 12],
                    &[OpType::Put, OpType::Put],
                    &[21, 22],
                ),
                new_batch(b"k1", &[3], &[10], &[OpType::Put], &[33]),
                new_batch(b"k1", &[4], &[14], &[OpType::Put], &[24]),
                new_batch(b"k2", &[1], &[11], &[OpType::Put], &[21]),
                new_batch(b"k2", &[3], &[13], &[OpType::Put], &[23]),
                new_batch(b"k2", &[10], &[20], &[OpType::Put], &[30]),
            ],
        )
        .await;

        assert_eq!(11, reader.metrics.num_input_rows);
        assert_eq!(7, reader.metrics.num_output_rows);
        assert_eq!(2, reader.metrics.num_deleted_rows);
        assert_eq!(2, reader.metrics.num_duplicate_rows);
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
            &[
                new_batch(b"k1", &[1], &[10], &[OpType::Put], &[21]),
                new_batch(
                    b"k1",
                    &[2, 3],
                    &[11, 11],
                    &[OpType::Put, OpType::Put],
                    &[32, 33],
                ),
            ],
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
            &[
                new_batch(b"k1", &[1], &[11], &[OpType::Put], &[31]),
                new_batch(
                    b"k1",
                    &[6, 8],
                    &[11, 11],
                    &[OpType::Put, OpType::Put],
                    &[36, 38],
                ),
                new_batch(b"k1", &[10], &[10], &[OpType::Put], &[30]),
                new_batch(b"k1", &[20], &[11], &[OpType::Put], &[40]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_many_duplicates() {
        let mut builder = MergeReaderBuilder::new();
        for i in 0..10 {
            let batches: Vec<_> = (0..8)
                .map(|ts| new_batch(b"k1", &[ts], &[i], &[OpType::Put], &[100]))
                .collect();
            let reader = VecBatchReader::new(&batches);
            builder.push_batch_reader(Box::new(reader));
        }
        let mut reader = builder.build().await.unwrap();
        let expect: Vec<_> = (0..8)
            .map(|ts| new_batch(b"k1", &[ts], &[9], &[OpType::Put], &[100]))
            .collect();
        check_reader_result(&mut reader, &expect).await;
    }
}
