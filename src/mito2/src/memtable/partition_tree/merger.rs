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

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::ops::Range;

use crate::error::Result;
use crate::memtable::partition_tree::data::{DataBatch, DataBufferReader, DataPartReader};
use crate::memtable::partition_tree::PkIndex;

/// Nodes of merger's heap.
pub trait Node: Ord {
    /// Returns true if current node is not exhausted.
    fn is_valid(&self) -> bool;

    /// Whether the other node is behind (exclusive) current node.
    fn is_behind(&self, other: &Self) -> bool;

    /// Advances `len` rows from current batch. If current batch is empty it fetches
    /// next batch from the node.
    ///
    /// # Panics
    /// If the node is invalid.
    fn advance(&mut self, len: usize) -> Result<()>;

    /// Length of current item.
    fn current_item_len(&self) -> usize;

    /// Searches first key of `other` in current item and returns the index.
    fn search_key_in_current_item(&self, other: &Self) -> Result<usize, usize>;
}

pub struct Merger<T: Node> {
    /// Heap to find node to read.
    ///
    /// Nodes in the heap are always valid.
    heap: BinaryHeap<T>,
    /// Current node to read.
    ///
    /// The node is always valid if it is not None.
    current_node: Option<T>,
    /// The number of rows in current node that are valid to read.
    current_rows: usize,
}

impl<T: Node> Merger<T> {
    pub(crate) fn try_new(nodes: Vec<T>) -> Result<Self> {
        let mut heap = BinaryHeap::with_capacity(nodes.len());
        for node in nodes {
            if node.is_valid() {
                heap.push(node);
            }
        }
        let mut merger = Merger {
            heap,
            current_node: None,
            current_rows: 0,
        };
        merger.next()?;
        Ok(merger)
    }

    /// Returns true if current merger is still valid.
    pub(crate) fn is_valid(&self) -> bool {
        self.current_node.is_some()
    }

    /// Returns current node to read. Only [Self::current_rows] rows in current node
    /// are valid to read.
    ///
    /// # Panics
    /// Panics if the merger is invalid.
    pub(crate) fn current_node(&self) -> &T {
        self.current_node.as_ref().unwrap()
    }

    /// Returns rows of current node to read.
    pub(crate) fn current_rows(&self) -> usize {
        self.current_rows
    }

    /// Advances the merger to the next item.
    pub(crate) fn next(&mut self) -> Result<()> {
        self.maybe_advance_current_node()?;
        debug_assert!(self.current_node.is_none());

        // Finds node and range to read from the heap.
        let Some(top_node) = self.heap.pop() else {
            // Heap is empty.
            return Ok(());
        };
        if let Some(next_node) = self.heap.peek() {
            if next_node.is_behind(&top_node) {
                // Does not overlap.
                self.current_rows = top_node.current_item_len();
            } else {
                // Note that the heap ensures the top node always has the minimal row.
                match top_node.search_key_in_current_item(next_node) {
                    Ok(pos) => {
                        if pos == 0 {
                            // If the first item of top node has duplicate key with the next node,
                            // we can simply return the first row in the top node as it must be the one
                            // with max sequence.
                            self.current_rows = 1;
                        } else {
                            // We don't know which one has the larger sequence so we use the range before
                            // the duplicate pos.
                            self.current_rows = pos;
                        }
                    }
                    Err(pos) => {
                        // No duplication. Output rows before pos.
                        debug_assert!(pos > 0);
                        self.current_rows = pos;
                    }
                }
            }
        } else {
            // Top is the only node left. We can read all rows in it.
            self.current_rows = top_node.current_item_len();
        }
        self.current_node = Some(top_node);

        Ok(())
    }

    fn maybe_advance_current_node(&mut self) -> Result<()> {
        let Some(mut node) = self.current_node.take() else {
            return Ok(());
        };

        // Advances current node.
        node.advance(self.current_rows)?;
        self.current_rows = 0;
        if !node.is_valid() {
            return Ok(());
        }

        // Puts the node into the heap.
        self.heap.push(node);
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct DataBatchKey {
    pub(crate) pk_index: PkIndex,
    pub(crate) timestamp: i64,
}

pub(crate) enum DataSource {
    Buffer(DataBufferReader),
    Part(DataPartReader),
}

impl DataSource {
    fn current_data_batch(&self) -> DataBatch {
        match self {
            DataSource::Buffer(buffer) => buffer.current_data_batch(),
            DataSource::Part(p) => p.current_data_batch(),
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            DataSource::Buffer(b) => b.is_valid(),
            DataSource::Part(p) => p.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self {
            DataSource::Buffer(b) => b.next(),
            DataSource::Part(p) => p.next(),
        }
    }
}

pub(crate) struct DataNode {
    source: DataSource,
    /// Current range of the batch in the source.
    current_range: Option<Range<usize>>,
}

impl DataNode {
    pub(crate) fn new(source: DataSource) -> Self {
        let current_range = source
            .is_valid()
            .then(|| 0..source.current_data_batch().range().len());

        Self {
            source,
            current_range,
        }
    }

    pub(crate) fn current_data_batch(&self) -> DataBatch {
        let range = self.current_range();
        let batch = self.source.current_data_batch();
        batch.slice(range.start, range.len())
    }

    fn current_range(&self) -> Range<usize> {
        self.current_range.clone().unwrap()
    }
}

impl Ord for DataNode {
    fn cmp(&self, other: &Self) -> Ordering {
        let weight = self.current_data_batch().pk_index();
        let (ts_start, sequence) = self.current_data_batch().first_row();
        let other_weight = other.current_data_batch().pk_index();
        let (other_ts_start, other_sequence) = other.current_data_batch().first_row();
        (weight, ts_start, Reverse(sequence))
            .cmp(&(other_weight, other_ts_start, Reverse(other_sequence)))
            .reverse()
    }
}

impl Eq for DataNode {}

impl PartialEq<Self> for DataNode {
    fn eq(&self, other: &Self) -> bool {
        self.current_data_batch()
            .first_row()
            .eq(&other.current_data_batch().first_row())
    }
}

impl PartialOrd<Self> for DataNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Node for DataNode {
    fn is_valid(&self) -> bool {
        self.current_range.is_some()
    }

    fn is_behind(&self, other: &Self) -> bool {
        let pk_weight = self.current_data_batch().pk_index();
        let (start, seq) = self.current_data_batch().first_row();
        let other_pk_weight = other.current_data_batch().pk_index();
        let (other_end, other_seq) = other.current_data_batch().last_row();
        (pk_weight, start, Reverse(seq)) > (other_pk_weight, other_end, Reverse(other_seq))
    }

    fn advance(&mut self, len: usize) -> Result<()> {
        let mut range = self.current_range();
        debug_assert!(range.len() >= len);

        let remaining = range.len() - len;
        if remaining == 0 {
            // Nothing remains, we need to fetch next batch to ensure the current batch is not empty.
            self.source.next()?;
            if self.source.is_valid() {
                self.current_range = Some(0..self.source.current_data_batch().range().len());
            } else {
                // The node is exhausted.
                self.current_range = None;
            }
        } else {
            range.start += len;
            self.current_range = Some(range);
        }

        Ok(())
    }

    fn current_item_len(&self) -> usize {
        self.current_range.clone().unwrap().len()
    }

    fn search_key_in_current_item(&self, other: &Self) -> Result<usize, usize> {
        let key = other.current_data_batch().first_key();
        self.current_data_batch().search_key(&key)
    }
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::array::UInt64Array;
    use store_api::metadata::RegionMetadataRef;

    use super::*;
    use crate::memtable::partition_tree::data::{timestamp_array_to_i64_slice, DataBuffer};
    use crate::test_util::memtable_util::{build_key_values_with_ts_seq_values, metadata_for_test};

    fn write_rows_to_buffer(
        buffer: &mut DataBuffer,
        schema: &RegionMetadataRef,
        pk_index: u16,
        ts: Vec<i64>,
        sequence: &mut u64,
    ) {
        let rows = ts.len() as u64;
        let v0 = ts.iter().map(|v| Some(*v as f64)).collect::<Vec<_>>();
        let kvs = build_key_values_with_ts_seq_values(
            schema,
            "whatever".to_string(),
            1,
            ts.into_iter(),
            v0.into_iter(),
            *sequence,
        );

        for kv in kvs.iter() {
            buffer.write_row(pk_index, &kv);
        }

        *sequence += rows;
    }

    fn check_merger_read(nodes: Vec<DataNode>, expected: &[(u16, Vec<(i64, u64)>)]) {
        let mut merger = Merger::try_new(nodes).unwrap();

        let mut res = vec![];
        while merger.is_valid() {
            let data_batch = merger.current_node().current_data_batch();
            let data_batch = data_batch.slice(0, merger.current_rows());
            let batch = data_batch.slice_record_batch();
            let ts_array = batch.column(1);
            let ts_values: Vec<_> = timestamp_array_to_i64_slice(ts_array).to_vec();
            let ts_and_seq = ts_values
                .into_iter()
                .zip(
                    batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .iter(),
                )
                .map(|(ts, seq)| (ts, seq.unwrap()))
                .collect::<Vec<_>>();

            res.push((data_batch.pk_index(), ts_and_seq));
            merger.next().unwrap();
        }
        assert_eq!(expected, &res);
    }

    #[test]
    fn test_merger() {
        let metadata = metadata_for_test();
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        let weight = &[2, 1, 0];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 1, vec![2, 3], &mut seq);
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![1, 2], &mut seq);
        let node1 = DataNode::new(DataSource::Part(
            buffer1.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        write_rows_to_buffer(&mut buffer2, &metadata, 1, vec![3], &mut seq);
        write_rows_to_buffer(&mut buffer2, &metadata, 0, vec![1], &mut seq);
        let node2 = DataNode::new(DataSource::Part(
            buffer2.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        check_merger_read(
            vec![node1, node2],
            &[
                (1, vec![(2, 0)]),
                (1, vec![(3, 4)]),
                (1, vec![(3, 1)]),
                (2, vec![(1, 5)]),
                (2, vec![(1, 2), (2, 3)]),
            ],
        );
    }

    #[test]
    fn test_merger2() {
        let metadata = metadata_for_test();
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        let weight = &[2, 1, 0];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 1, vec![2, 3], &mut seq);
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![1, 2], &mut seq);
        let node1 = DataNode::new(DataSource::Part(
            buffer1.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        write_rows_to_buffer(&mut buffer2, &metadata, 1, vec![3], &mut seq);
        let node2 = DataNode::new(DataSource::Part(
            buffer2.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        let mut buffer3 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        write_rows_to_buffer(&mut buffer3, &metadata, 0, vec![2, 3], &mut seq);
        let node3 = DataNode::new(DataSource::Part(
            buffer3.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        check_merger_read(
            vec![node1, node3, node2],
            &[
                (1, vec![(2, 0)]),
                (1, vec![(3, 4)]),
                (1, vec![(3, 1)]),
                (2, vec![(1, 2)]),
                (2, vec![(2, 5)]),
                (2, vec![(2, 3)]),
                (2, vec![(3, 6)]),
            ],
        );
    }

    #[test]
    fn test_merger_overlapping() {
        let metadata = metadata_for_test();
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        let weight = &[0, 1, 2];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![1, 2, 3], &mut seq);
        let node1 = DataNode::new(DataSource::Part(
            buffer1.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        write_rows_to_buffer(&mut buffer2, &metadata, 1, vec![2, 3], &mut seq);
        let node2 = DataNode::new(DataSource::Part(
            buffer2.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        let mut buffer3 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        write_rows_to_buffer(&mut buffer3, &metadata, 0, vec![2, 3], &mut seq);
        let node3 = DataNode::new(DataSource::Part(
            buffer3.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        check_merger_read(
            vec![node1, node3, node2],
            &[
                (0, vec![(1, 0)]),
                (0, vec![(2, 5)]),
                (0, vec![(2, 1)]),
                (0, vec![(3, 6)]),
                (0, vec![(3, 2)]),
                (1, vec![(2, 3), (3, 4)]),
            ],
        );
    }

    #[test]
    fn test_merger_parts_and_buffer() {
        let metadata = metadata_for_test();
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        let weight = &[0, 1, 2];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![1, 2, 3], &mut seq);
        let node1 = DataNode::new(DataSource::Buffer(
            buffer1.read().unwrap().build(Some(weight)).unwrap(),
        ));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        write_rows_to_buffer(&mut buffer2, &metadata, 1, vec![2, 3], &mut seq);
        let node2 = DataNode::new(DataSource::Part(
            buffer2.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        let mut buffer3 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        write_rows_to_buffer(&mut buffer3, &metadata, 0, vec![2, 3], &mut seq);
        let node3 = DataNode::new(DataSource::Part(
            buffer3.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        check_merger_read(
            vec![node1, node3, node2],
            &[
                (0, vec![(1, 0)]),
                (0, vec![(2, 5)]),
                (0, vec![(2, 1)]),
                (0, vec![(3, 6)]),
                (0, vec![(3, 2)]),
                (1, vec![(2, 3), (3, 4)]),
            ],
        );
    }

    #[test]
    fn test_merger_overlapping_2() {
        let metadata = metadata_for_test();
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        let weight = &[0, 1, 2];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![1, 2, 2], &mut seq);
        let node1 = DataNode::new(DataSource::Part(
            buffer1.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        write_rows_to_buffer(&mut buffer2, &metadata, 0, vec![2], &mut seq);
        let node2 = DataNode::new(DataSource::Part(
            buffer2.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        let mut buffer3 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        write_rows_to_buffer(&mut buffer3, &metadata, 0, vec![2], &mut seq);
        let node3 = DataNode::new(DataSource::Part(
            buffer3.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        check_merger_read(
            vec![node1, node2, node3],
            &[
                (0, vec![(1, 0)]),
                (0, vec![(2, 4)]),
                (0, vec![(2, 3)]),
                (0, vec![(2, 2)]),
            ],
        );
    }

    #[test]
    fn test_merger_overlapping_3() {
        let metadata = metadata_for_test();
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        let weight = &[0, 1, 2];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![0, 1], &mut seq);
        let node1 = DataNode::new(DataSource::Part(
            buffer1.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10, true);
        write_rows_to_buffer(&mut buffer2, &metadata, 0, vec![1], &mut seq);
        let node2 = DataNode::new(DataSource::Part(
            buffer2.freeze(Some(weight), true).unwrap().read().unwrap(),
        ));

        check_merger_read(
            vec![node1, node2],
            &[(0, vec![(0, 0)]), (0, vec![(1, 2)]), (0, vec![(1, 1)])],
        );
    }
}
