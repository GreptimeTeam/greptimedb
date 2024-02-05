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
use std::collections::{BTreeMap, BinaryHeap};
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

    pub(crate) fn write_primary_key(&self, key: &[u8]) -> Result<PkId> {
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
    // TODO(yingwen): Reuse keys.
    pk_to_index: BTreeMap<Vec<u8>, PkIndex>,
    key_buffer: KeyBuffer,
    dict_blocks: Vec<DictBlock>,
    num_keys: usize,
}

impl MutableShard {
    fn new(shard_id: ShardId) -> MutableShard {
        MutableShard {
            shard_id,
            pk_to_index: BTreeMap::new(),
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

        // Already in the shard.
        if let Some(pk_index) = self.pk_to_index.get(key).copied() {
            return Ok(Some(PkId {
                shard_id: self.shard_id,
                pk_index,
            }));
        }
        // A new key.

        if self.key_buffer.len() >= MAX_KEYS_PER_BLOCK.into() {
            // The write buffer is full.
            let dict_block = self.key_buffer.finish();
            self.dict_blocks.push(dict_block);
        }

        // Safety: we check the buffer length.
        let pk_index = self.key_buffer.push_key(key);
        self.pk_to_index.insert(key.to_vec(), pk_index);
        self.num_keys += 1;

        Ok(Some(PkId {
            shard_id: self.shard_id,
            pk_index,
        }))
    }

    fn scan_shard(&self) -> Result<BlocksReader> {
        let sorted_pk_indices = self.pk_to_index.values().copied().collect();
        let block = self.key_buffer.finish_cloned();
        let mut blocks = Vec::with_capacity(self.dict_blocks.len() + 1);
        blocks.extend_from_slice(&self.dict_blocks);
        blocks.push(block);

        Ok(BlocksReader::new(blocks, sorted_pk_indices))
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

    fn finish(&mut self) -> DictBlock {
        let primary_key = self.primary_key_builder.finish();

        DictBlock::from_unsorted(primary_key)
    }

    fn finish_cloned(&self) -> DictBlock {
        let primary_key = self.primary_key_builder.finish_cloned();

        DictBlock::from_unsorted(primary_key)
    }
}

// The array should be cheap to clone.
#[derive(Clone)]
struct DictBlock {
    /// Primary key buffer (unsorted).
    primary_key: BinaryArray,
}

impl DictBlock {
    fn from_unsorted(primary_key: BinaryArray) -> Self {
        Self { primary_key }
    }

    fn len(&self) -> usize {
        self.primary_key.len()
    }

    /// Get key by [PkIndex].
    fn key_by_pk_index(&self, index: PkIndex) -> &[u8] {
        let pos = index % MAX_KEYS_PER_BLOCK;
        self.primary_key.value(pos as usize)
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

pub(crate) struct BlocksReader {
    blocks: Vec<DictBlock>,
    sorted_pk_indices: Vec<PkIndex>,
    /// Current offset in the `sorted_pk_indices`.
    offset: usize,
}

impl BlocksReader {
    fn new(blocks: Vec<DictBlock>, sorted_pk_indices: Vec<PkIndex>) -> BlocksReader {
        BlocksReader {
            blocks,
            sorted_pk_indices,
            offset: 0,
        }
    }

    fn key_by_pk_index(&self, pk_index: PkIndex) -> &[u8] {
        let block_idx = pk_index / MAX_KEYS_PER_BLOCK;
        self.blocks[block_idx as usize].key_by_pk_index(pk_index)
    }
}

impl IndexReader for BlocksReader {
    fn is_valid(&self) -> bool {
        self.offset < self.sorted_pk_indices.len()
    }

    fn current_key(&self) -> &[u8] {
        let pk_index = self.current_pk_index();
        self.key_by_pk_index(pk_index)
    }

    fn current_pk_index(&self) -> PkIndex {
        assert!(self.is_valid());
        self.sorted_pk_indices[self.offset]
    }

    fn next(&mut self) {
        assert!(self.is_valid());
        self.offset += 1;
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    fn prepare_input_keys(num_keys: usize) -> Vec<Vec<u8>> {
        let prefix = ["a", "b", "c", "d", "e", "f"];
        let mut rng = rand::thread_rng();
        let mut keys = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            let prefix_idx = rng.gen_range(0..prefix.len());
            // We don't need to decode the priamry key in index's test so we format the string
            // into the key.
            let key = format!("{}{}", prefix[prefix_idx], i);
            keys.push(key.into_bytes());
        }

        keys
    }

    #[test]
    fn test_write_scan_single_shard() {
        let num_keys = MAX_KEYS_PER_BLOCK * 2 + MAX_KEYS_PER_BLOCK / 2;
        let keys = prepare_input_keys(num_keys.into());

        let index = KeyIndex::new(IndexConfig {
            max_keys_per_shard: (MAX_KEYS_PER_BLOCK * 3).into(),
        });
        let mut last_pk_id = None;
        for key in &keys {
            let pk_id = index.write_primary_key(key).unwrap();
            last_pk_id = Some(pk_id);
        }
        assert_eq!(
            PkId {
                shard_id: 0,
                pk_index: num_keys - 1,
            },
            last_pk_id.unwrap()
        );

        let mut expect: Vec<_> = keys
            .into_iter()
            .enumerate()
            .map(|(i, key)| (key, i as PkIndex))
            .collect();
        expect.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut result = Vec::with_capacity(expect.len());
        let mut reader = index.scan_index().unwrap();
        while reader.is_valid() {
            result.push((reader.current_key().to_vec(), reader.current_pk_index()));
            reader.next();
        }
        assert_eq!(expect, result);
    }
}
