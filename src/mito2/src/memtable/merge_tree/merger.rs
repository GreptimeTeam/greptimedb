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

use datatypes::arrow::array::{
    ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, TimeUnit};

use crate::error::Result;
use crate::memtable::merge_tree::data::{DataBatch, DataBufferReader, DataPartReader};
use crate::memtable::merge_tree::PkIndex;

/// Nodes of merger's heap.
pub trait Node: Ord {
    type Item;

    /// Returns current item of node and fetch next.
    fn fetch_next(&mut self) -> Result<Self::Item>;

    /// Returns true if current node is not exhausted.
    fn is_valid(&self) -> bool;

    /// Current item of node.
    fn current_item(&self) -> &Self::Item;

    /// Whether the other node is behind (exclusive) current node.
    fn is_behind(&self, other: &Self) -> bool;

    /// Skips first `num_to_skip` rows from node's current batch. If current batch is empty it fetches
    /// next batch from the node.
    ///
    /// # Panics
    /// If the node is EOF.
    fn skip(&mut self, offset_to_skip: usize) -> Result<()>;

    /// Searches given item in node's current item and returns the index.
    fn search_key_in_current_item(&self, key: &Self::Item) -> std::result::Result<usize, usize>;

    /// Slice current item.
    fn slice_current_item(&self, range: Range<usize>) -> Self::Item;
}

pub struct Merger<T: Node> {
    heap: BinaryHeap<T>,
    current_item: Option<T::Item>,
}

impl<T> Merger<T>
where
    T: Node,
{
    pub(crate) fn try_new(nodes: Vec<T>) -> Result<Self> {
        let mut heap = BinaryHeap::with_capacity(nodes.len());
        for node in nodes {
            if node.is_valid() {
                heap.push(node);
            }
        }
        let mut merger = Merger {
            heap,
            current_item: None,
        };
        merger.next()?;
        Ok(merger)
    }

    /// Returns true if current merger is still valid.
    pub(crate) fn is_valid(&self) -> bool {
        self.current_item.is_some()
    }

    /// Advances current merger to next item.
    pub(crate) fn next(&mut self) -> Result<()> {
        let Some(mut top_node) = self.heap.pop() else {
            // heap is empty
            self.current_item = None;
            return Ok(());
        };
        if let Some(next_node) = self.heap.peek() {
            if next_node.is_behind(&top_node) {
                // does not overlap
                self.current_item = Some(top_node.fetch_next()?);
            } else {
                let res = match top_node.search_key_in_current_item(next_node.current_item()) {
                    Ok(pos) => {
                        if pos == 0 {
                            // if the first item of top node has duplicate ts with next node,
                            // we can simply return the first row in that it must be the one
                            // with max sequence.
                            let to_yield = top_node.slice_current_item(0..1);
                            top_node.skip(1)?;
                            to_yield
                        } else {
                            let to_yield = top_node.slice_current_item(0..pos);
                            top_node.skip(pos)?;
                            to_yield
                        }
                    }
                    Err(pos) => {
                        // no duplicated timestamp
                        let to_yield = top_node.slice_current_item(0..pos);
                        top_node.skip(pos)?;
                        to_yield
                    }
                };
                self.current_item = Some(res);
            }
        } else {
            // top is the only node left.
            self.current_item = Some(top_node.fetch_next()?);
        }
        if top_node.is_valid() {
            self.heap.push(top_node);
        }
        Ok(())
    }

    /// Returns current item held by merger.
    pub(crate) fn current_item(&self) -> &T::Item {
        self.current_item.as_ref().unwrap()
    }
}

#[derive(Debug)]
pub struct DataBatchKey {
    pk_index: PkIndex,
    timestamp: i64,
}

impl Eq for DataBatchKey {}

impl PartialEq<Self> for DataBatchKey {
    fn eq(&self, other: &Self) -> bool {
        self.pk_index == other.pk_index && self.timestamp == other.timestamp
    }
}

impl PartialOrd<Self> for DataBatchKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataBatchKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.pk_index
            .cmp(&other.pk_index)
            .then(self.timestamp.cmp(&other.timestamp))
            .reverse()
    }
}

impl DataBatch {
    fn first_row(&self) -> (i64, u64) {
        let range = self.range();
        let ts_values = timestamp_array_to_i64_slice(self.rb.column(1));
        let sequence_values = self
            .rb
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values();
        (ts_values[range.start], sequence_values[range.start])
    }

    fn last_row(&self) -> (i64, u64) {
        let range = self.range();
        let ts_values = timestamp_array_to_i64_slice(self.rb.column(1));
        let sequence_values = self
            .rb
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values();
        (ts_values[range.end - 1], sequence_values[range.end - 1])
    }
}

impl DataBatch {
    fn remaining(&self) -> usize {
        self.range().len()
    }

    fn first_key(&self) -> DataBatchKey {
        let range = self.range();
        let batch = self.record_batch();
        let pk_index = self.pk_index();
        let ts_array = batch.column(1);

        // maybe safe the result somewhere.
        let ts_values = timestamp_array_to_i64_slice(ts_array);
        let timestamp = ts_values[range.start];
        DataBatchKey {
            pk_index,
            timestamp,
        }
    }

    fn search_key(&self, key: &DataBatchKey) -> std::result::Result<usize, usize> {
        let DataBatchKey {
            pk_index,
            timestamp,
        } = key;
        assert_eq!(*pk_index, self.pk_index);
        let ts_values = timestamp_array_to_i64_slice(self.record_batch().column(1));
        ts_values.binary_search(timestamp)
    }

    fn slice(&self, range: Range<usize>) -> Self {
        let rb = self.rb.slice(range.start, range.len());
        let range = 0..rb.num_rows();
        Self {
            pk_index: self.pk_index,
            rb,
            range,
        }
    }
}

pub struct DataNode {
    source: DataSource,
    current_data_batch: Option<DataBatch>,
}

impl DataNode {
    pub(crate) fn new(source: DataSource) -> Self {
        let current_data_batch = source.current_data_batch();
        Self {
            source,
            current_data_batch: Some(current_data_batch),
        }
    }

    fn next(&mut self) -> Result<()> {
        self.current_data_batch = self.source.fetch_next()?;
        Ok(())
    }

    fn current_data_batch(&self) -> &DataBatch {
        self.current_data_batch.as_ref().unwrap()
    }
}

pub enum DataSource {
    Buffer(DataBufferReader),
    Part(DataPartReader),
}

impl DataSource {
    pub(crate) fn current_data_batch(&self) -> DataBatch {
        match self {
            DataSource::Buffer(buffer) => buffer.current_data_batch(),
            DataSource::Part(p) => p.current_data_batch(),
        }
    }

    fn fetch_next(&mut self) -> Result<Option<DataBatch>> {
        let res = match self {
            DataSource::Buffer(b) => {
                b.next()?;
                if b.is_valid() {
                    Some(b.current_data_batch())
                } else {
                    None
                }
            }
            DataSource::Part(p) => {
                p.next()?;
                if p.is_valid() {
                    Some(p.current_data_batch())
                } else {
                    None
                }
            }
        };
        Ok(res)
    }
}

impl Ord for DataNode {
    fn cmp(&self, other: &Self) -> Ordering {
        let weight = self.current_data_batch().pk_index;
        let (ts_start, sequence) = self.current_data_batch().first_row();
        let other_weight = other.current_data_batch().pk_index;
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
    type Item = DataBatch;

    fn fetch_next(&mut self) -> Result<Self::Item> {
        let current = self.current_data_batch.take();
        self.next()?;
        Ok(current.unwrap())
    }

    fn is_valid(&self) -> bool {
        self.current_data_batch.is_some()
    }

    fn current_item(&self) -> &Self::Item {
        self.current_data_batch()
    }

    fn is_behind(&self, other: &Self) -> bool {
        let pk_weight = self.current_data_batch().pk_index;
        let (start, seq) = self.current_data_batch().first_row();
        let other_pk_weight = other.current_data_batch().pk_index;
        let (other_end, other_seq) = other.current_data_batch().last_row();
        (pk_weight, start, Reverse(seq)) > (other_pk_weight, other_end, Reverse(other_seq))
    }

    fn skip(&mut self, offset_to_skip: usize) -> Result<()> {
        let current = self.current_item();
        let remaining = current.remaining() - offset_to_skip;
        if remaining == 0 {
            self.next()?;
        } else {
            let end = current.remaining();
            self.current_data_batch = Some(current.slice(offset_to_skip..end));
        }

        Ok(())
    }

    fn search_key_in_current_item(&self, key: &Self::Item) -> std::result::Result<usize, usize> {
        let key = key.first_key();
        self.current_data_batch.as_ref().unwrap().search_key(&key)
    }

    fn slice_current_item(&self, range: Range<usize>) -> Self::Item {
        self.current_data_batch.as_ref().unwrap().slice(range)
    }
}

fn timestamp_array_to_i64_slice(arr: &ArrayRef) -> &[i64] {
    match arr.data_type() {
        DataType::Timestamp(t, _) => match t {
            TimeUnit::Second => arr
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap()
                .values(),
            TimeUnit::Millisecond => arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .values(),
            TimeUnit::Microsecond => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .values(),
            TimeUnit::Nanosecond => arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .values(),
        },
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::array::UInt64Array;
    use store_api::metadata::RegionMetadataRef;

    use super::*;
    use crate::memtable::merge_tree::data::DataBuffer;
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
            buffer.write_row(pk_index, kv);
        }

        *sequence += rows;
    }

    fn check_merger_read(nodes: Vec<DataNode>, expected: &[(u16, Vec<(i64, u64)>)]) {
        let mut merger = Merger::try_new(nodes).unwrap();

        let mut res = vec![];
        while merger.is_valid() {
            let data_batch = merger.current_item();
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

            res.push((data_batch.pk_index, ts_and_seq));
            merger.next().unwrap();
        }
        assert_eq!(expected, &res);
    }

    #[test]
    fn test_merger() {
        let metadata = metadata_for_test();
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10);
        let weight = &[2, 1, 0];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 1, vec![2, 3], &mut seq);
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![1, 2], &mut seq);
        let node1 = DataNode::new(DataSource::Buffer(buffer1.read(weight).unwrap()));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10);
        write_rows_to_buffer(&mut buffer2, &metadata, 1, vec![3], &mut seq);
        write_rows_to_buffer(&mut buffer2, &metadata, 0, vec![1], &mut seq);
        let node2 = DataNode::new(DataSource::Buffer(buffer2.read(weight).unwrap()));

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
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10);
        let weight = &[2, 1, 0];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 1, vec![2, 3], &mut seq);
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![1, 2], &mut seq);
        let node1 = DataNode::new(DataSource::Buffer(buffer1.read(weight).unwrap()));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10);
        write_rows_to_buffer(&mut buffer2, &metadata, 1, vec![3], &mut seq);
        let node2 = DataNode::new(DataSource::Buffer(buffer2.read(weight).unwrap()));

        let mut buffer3 = DataBuffer::with_capacity(metadata.clone(), 10);
        write_rows_to_buffer(&mut buffer3, &metadata, 0, vec![2, 3], &mut seq);
        let node3 = DataNode::new(DataSource::Buffer(buffer3.read(weight).unwrap()));

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
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10);
        let weight = &[0, 1, 2];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![1, 2, 3], &mut seq);
        let node1 = DataNode::new(DataSource::Buffer(buffer1.read(weight).unwrap()));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10);
        write_rows_to_buffer(&mut buffer2, &metadata, 1, vec![2, 3], &mut seq);
        let node2 = DataNode::new(DataSource::Buffer(buffer2.read(weight).unwrap()));

        let mut buffer3 = DataBuffer::with_capacity(metadata.clone(), 10);
        write_rows_to_buffer(&mut buffer3, &metadata, 0, vec![2, 3], &mut seq);
        let node3 = DataNode::new(DataSource::Buffer(buffer3.read(weight).unwrap()));

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
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10);
        let weight = &[0, 1, 2];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![1, 2, 3], &mut seq);
        let node1 = DataNode::new(DataSource::Buffer(buffer1.read(weight).unwrap()));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10);
        write_rows_to_buffer(&mut buffer2, &metadata, 1, vec![2, 3], &mut seq);
        let node2 = DataNode::new(DataSource::Part(
            buffer2.freeze(weight).unwrap().read().unwrap(),
        ));

        let mut buffer3 = DataBuffer::with_capacity(metadata.clone(), 10);
        write_rows_to_buffer(&mut buffer3, &metadata, 0, vec![2, 3], &mut seq);
        let node3 = DataNode::new(DataSource::Part(
            buffer3.freeze(weight).unwrap().read().unwrap(),
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
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10);
        let weight = &[0, 1, 2];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![1, 2, 2], &mut seq);
        let node1 = DataNode::new(DataSource::Buffer(buffer1.read(weight).unwrap()));

        let mut buffer3 = DataBuffer::with_capacity(metadata.clone(), 10);
        write_rows_to_buffer(&mut buffer3, &metadata, 0, vec![2], &mut seq);
        let node3 = DataNode::new(DataSource::Buffer(buffer3.read(weight).unwrap()));

        let mut buffer4 = DataBuffer::with_capacity(metadata.clone(), 10);
        write_rows_to_buffer(&mut buffer4, &metadata, 0, vec![2], &mut seq);
        let node4 = DataNode::new(DataSource::Buffer(buffer4.read(weight).unwrap()));

        check_merger_read(
            vec![node1, node3, node4],
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
        let mut buffer1 = DataBuffer::with_capacity(metadata.clone(), 10);
        let weight = &[0, 1, 2];
        let mut seq = 0;
        write_rows_to_buffer(&mut buffer1, &metadata, 0, vec![0, 1], &mut seq);
        let node1 = DataNode::new(DataSource::Buffer(buffer1.read(weight).unwrap()));

        let mut buffer2 = DataBuffer::with_capacity(metadata.clone(), 10);
        write_rows_to_buffer(&mut buffer2, &metadata, 0, vec![1], &mut seq);
        let node2 = DataNode::new(DataSource::Buffer(buffer2.read(weight).unwrap()));

        check_merger_read(
            vec![node1, node2],
            &[(0, vec![(0, 0)]), (0, vec![(1, 2)]), (0, vec![(1, 1)])],
        );
    }
}
