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

//! Primary key index of the merge tree.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, RwLock};

use datatypes::arrow::array::{Array, ArrayBuilder, BinaryArray, BinaryBuilder};
use datatypes::arrow::compute;
use snafu::ResultExt;

use crate::error::{ComputeArrowSnafu, Result};
use crate::memtable::merge_tree::{PkId, PkIndex, ShardId};

// TODO(yingwen): Consider using byte size to manage block.
/// Maximum keys in a block. Should be power of 2.
const MAX_KEYS_PER_BLOCK: u16 = 256;

/// Config for the index.
pub(crate) struct IndexConfig {
    /// Max keys in an index shard.
    pub(crate) max_keys_per_shard: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            // TODO(yingwen): Use 4096 or find a proper value.
            max_keys_per_shard: 8192,
        }
    }
}

/// Primary key index.
pub(crate) struct KeyIndex {
    config: IndexConfig,
    // TODO(yingwen): 1. Support multiple shard.
    shard: RwLock<MutableShard>,
}

impl KeyIndex {
    pub(crate) fn new(config: IndexConfig) -> KeyIndex {
        KeyIndex {
            config,
            shard: RwLock::new(MutableShard::new(0)),
        }
    }

    pub(crate) fn add_primary_key(&self, key: &[u8]) -> Result<PkId> {
        let mut shard = self.shard.write().unwrap();
        let pkid = shard.try_add_primary_key(&self.config, key)?;
        // TODO(yingwen): Switch shard if current shard is full.
        Ok(pkid.expect("shard is full"))
    }

    pub(crate) fn scan_index(&self) -> Result<BoxedIndexReader> {
        let shard = self.shard.read().unwrap();
        let reader = shard.scan_shard()?;

        Ok(Box::new(reader))
    }
}

// TODO(yingwen): Support partition index (partition by a column, e.g. table_id) to
// reduce null columns and eliminate lock contention. We only need to partition the
// write buffer but modify dicts with partition lock held.
/// Mutable shard for the index.
struct MutableShard {
    shard_id: ShardId,
    key_buffer: KeyBuffer,
    dict_blocks: Vec<DictBlockRef>,
    num_keys: usize,
}

impl MutableShard {
    fn new(shard_id: ShardId) -> MutableShard {
        MutableShard {
            shard_id,
            key_buffer: KeyBuffer::new(MAX_KEYS_PER_BLOCK.into()),
            dict_blocks: Vec::new(),
            num_keys: 0,
        }
    }

    fn try_add_primary_key(&mut self, config: &IndexConfig, key: &[u8]) -> Result<Option<PkId>> {
        // The shard is full.
        if self.num_keys >= config.max_keys_per_shard {
            return Ok(None);
        }

        if self.key_buffer.len() >= MAX_KEYS_PER_BLOCK.into() {
            // The write buffer is full.
            let dict_block = self.key_buffer.finish()?;
            self.dict_blocks.push(Arc::new(dict_block));
        }

        // Safety: we check the buffer length.
        let pk_index = self.key_buffer.push_key(key);
        self.num_keys += 1;

        Ok(Some(PkId {
            shard_id: self.shard_id,
            pk_index,
        }))
    }

    fn scan_shard(&self) -> Result<ReaderMerger> {
        let block = self.key_buffer.finish_cloned()?;
        let mut readers = Vec::with_capacity(self.dict_blocks.len() + 1);
        readers.push(DictBlockReader::new(Arc::new(block)));
        for block in &self.dict_blocks {
            readers.push(DictBlockReader::new(block.clone()));
        }

        Ok(ReaderMerger::from_readers(readers))
    }
}

// TODO(yingwen): Bench using custom container for binary and ids so we can
// sort the buffer in place and reuse memory.
/// Buffer to store unsorted primary keys.
///
/// Now it doesn't support searching index by key. The memtable should use another
/// cache to map primary key to its index.
struct KeyBuffer {
    // TODO(yingwen): Maybe use BTreeMap as key builder.
    // We use arrow's binary builder as out default binary builder
    // is LargeBinaryBuilder
    primary_key_builder: BinaryBuilder,
    next_pk_index: usize,
}

impl KeyBuffer {
    fn new(item_capacity: usize) -> KeyBuffer {
        KeyBuffer {
            primary_key_builder: BinaryBuilder::with_capacity(item_capacity, 0),
            next_pk_index: 0,
        }
    }

    /// Pushes a new key and returns its pk index.
    ///
    /// # Panics
    /// Panics if the [PkIndex] type cannot represent the index.
    fn push_key(&mut self, key: &[u8]) -> PkIndex {
        let pk_index = self.next_pk_index.try_into().unwrap();
        self.next_pk_index += 1;
        self.primary_key_builder.append_value(key);

        pk_index
    }

    fn len(&self) -> usize {
        self.primary_key_builder.len()
    }

    /// Gets the primary key by its index.
    ///
    /// # Panics
    /// Panics if the index is invalid.
    fn get_key(&self, index: PkIndex) -> &[u8] {
        let values = self.primary_key_builder.values_slice();
        let offsets = self.primary_key_builder.offsets_slice();
        // Casting index to usize is safe.
        let start = offsets[index as usize];
        let end = offsets[index as usize + 1];

        // We know there is no null in the builder so we don't check validity.
        // The builder offset should be positive.
        &values[start as usize..end as usize]
    }

    fn finish(&mut self) -> Result<DictBlock> {
        // TODO(yingwen): We can check whether keys are already sorted first. But
        // we might need some benchmarks.
        let primary_key = self.primary_key_builder.finish();
        // Also resets the pk index for the next block.
        self.next_pk_index = 0;

        DictBlock::try_from_unsorted(primary_key)
    }

    fn finish_cloned(&self) -> Result<DictBlock> {
        let primary_key = self.primary_key_builder.finish_cloned();

        DictBlock::try_from_unsorted(primary_key)
    }
}

struct DictBlock {
    /// Sorted primary key buffer.
    primary_key: BinaryArray,
    /// PkIndex sorted by primary keys.
    ordered_pk_index: Vec<PkIndex>,
    /// Sort weight of each PkIndex. It also maps the PkIndex to the position
    /// of the primary key in the sorted key buffer.
    index_weight: Vec<u16>,
}

type DictBlockRef = Arc<DictBlock>;

impl DictBlock {
    fn try_from_unsorted(primary_key: BinaryArray) -> Result<Self> {
        assert!(primary_key.len() <= PkIndex::MAX.into());

        // Sort by primary key.
        let indices =
            compute::sort_to_indices(&primary_key, None, None).context(ComputeArrowSnafu)?;
        let primary_key = compute::take(&primary_key, &indices, None).context(ComputeArrowSnafu)?;

        // Weight of each pk index. We have check the length of the primary key.
        let index_weight: Vec<_> = indices.values().iter().map(|idx| *idx as u16).collect();

        let mut ordered_pk_index = vec![0; primary_key.len()];
        for (pk_index, dest_pos) in index_weight.iter().enumerate() {
            debug_assert!(pk_index <= PkIndex::MAX.into());

            ordered_pk_index[*dest_pos as usize] = pk_index as PkIndex;
        }

        let dict = DictBlock {
            primary_key: primary_key
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .clone(),
            ordered_pk_index,
            index_weight,
        };
        Ok(dict)
    }

    fn len(&self) -> usize {
        self.primary_key.len()
    }

    /// Get key by [PkIndex].
    fn key_by_pk_index(&self, index: PkIndex) -> &[u8] {
        // Casting index to usize is safe.
        let pos = self.index_weight[index as usize];
        self.key_at(pos as usize)
    }

    /// Get key at position.
    fn key_at(&self, pos: usize) -> &[u8] {
        self.primary_key.value(pos)
    }

    /// Get [PkIndex] at position.
    fn pk_index_at(&self, pos: usize) -> PkIndex {
        self.ordered_pk_index[pos]
    }
}

/// Reader to scan index keys.
pub(crate) trait IndexReader: Send {
    /// Returns whether the reader is valid.
    fn is_valid(&self) -> bool;

    /// Returns current key.
    ///
    /// # Panics
    /// Panics if the reader is invalid.
    fn current_key(&self) -> &[u8];

    /// Returns current pk index.
    ///
    /// # Panics
    /// Panics if the reader is invalid.
    fn current_pk_index(&self) -> PkIndex;

    /// Advance the reader.
    ///
    /// # Panics
    /// Panics if the reader is invalid.
    fn next(&mut self);
}

pub(crate) type BoxedIndexReader = Box<dyn IndexReader>;

struct DictBlockReader {
    block: DictBlockRef,
    current: usize,
}

impl DictBlockReader {
    fn new(block: DictBlockRef) -> Self {
        Self { block, current: 0 }
    }
}

impl IndexReader for DictBlockReader {
    fn is_valid(&self) -> bool {
        self.current < self.block.len()
    }

    fn current_key(&self) -> &[u8] {
        self.block.key_at(self.current)
    }

    fn current_pk_index(&self) -> PkIndex {
        self.block.pk_index_at(self.current)
    }

    fn next(&mut self) {
        assert!(self.is_valid());
        self.current += 1;
    }
}

/// Wrapper for heap merge.
///
/// Reader inside the wrapper must be valid.
struct HeapWrapper(DictBlockReader);

impl PartialEq for HeapWrapper {
    fn eq(&self, other: &HeapWrapper) -> bool {
        self.0.current_key() == other.0.current_key()
    }
}

impl Eq for HeapWrapper {}

impl PartialOrd for HeapWrapper {
    fn partial_cmp(&self, other: &HeapWrapper) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapWrapper {
    fn cmp(&self, other: &HeapWrapper) -> Ordering {
        // The std binary heap is a max heap, but we want the nodes are ordered in
        // ascend order, so we compare the nodes in reverse order.
        other.0.current_key().cmp(self.0.current_key())
    }
}

struct ReaderMerger {
    heap: BinaryHeap<HeapWrapper>,
}

impl ReaderMerger {
    fn from_readers(readers: Vec<DictBlockReader>) -> ReaderMerger {
        let heap = readers
            .into_iter()
            .filter_map(|reader| {
                if reader.is_valid() {
                    Some(HeapWrapper(reader))
                } else {
                    None
                }
            })
            .collect();

        ReaderMerger { heap }
    }
}

impl IndexReader for ReaderMerger {
    fn is_valid(&self) -> bool {
        !self.heap.is_empty()
    }

    fn current_key(&self) -> &[u8] {
        self.heap.peek().unwrap().0.current_key()
    }

    fn current_pk_index(&self) -> PkIndex {
        self.heap.peek().unwrap().0.current_pk_index()
    }

    fn next(&mut self) {
        while let Some(mut top) = self.heap.pop() {
            top.0.next();
            if top.0.is_valid() {
                self.heap.push(top);
                break;
            }
            // Top is exhausted, try next node.
        }
    }
}
