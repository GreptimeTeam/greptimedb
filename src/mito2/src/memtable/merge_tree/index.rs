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
use crate::memtable::merge_tree::mutable::WriteMetrics;
use crate::memtable::merge_tree::{PkId, PkIndex, ShardId};

// TODO(yingwen): Consider using byte size to manage block.
/// Maximum keys in a block. Should be power of 2.
const MAX_KEYS_PER_BLOCK: u16 = 256;

/// Config for the index.
#[derive(Debug, Clone)]
pub(crate) struct IndexConfig {
    /// Max keys in an index shard.
    pub(crate) max_keys_per_shard: usize,
}

/// Primary key index.
pub(crate) struct KeyIndex {
    config: IndexConfig,
    // TODO(yingwen): Support multiple shard.
    shard: RwLock<Shard>,
}

pub(crate) type KeyIndexRef = Arc<KeyIndex>;

impl KeyIndex {
    pub(crate) fn new(config: IndexConfig) -> KeyIndex {
        KeyIndex {
            config,
            shard: RwLock::new(Shard::new(0)),
        }
    }

    pub(crate) fn sorted_pk_indices(&self) -> Vec<u16> {
        let shard = self.shard.read().unwrap();
        shard.sorted_pk_indices()
    }

    pub(crate) fn write_primary_key(&self, key: &[u8], metrics: &mut WriteMetrics) -> Result<PkId> {
        let mut shard = self.shard.write().unwrap();
        let pk_id = shard.try_add_primary_key(&self.config, key, metrics)?;
        // TODO(yingwen): Switch shard if current shard is full.
        Ok(pk_id.expect("shard is full"))
    }

    pub(crate) fn scan_shard(&self, shard_id: ShardId) -> Result<ShardReader> {
        let shard = self.shard.read().unwrap();
        assert_eq!(shard.shard_id, shard_id);
        let reader = shard.scan_shard()?;

        Ok(reader)
    }

    /// Freezes the index.
    pub(crate) fn freeze(&self) -> Result<()> {
        let mut shard = self.shard.write().unwrap();
        shard.freeze()
    }

    /// Returns a new index for write.
    ///
    /// Callers must freeze the index first.
    pub(crate) fn fork(&self) -> KeyIndex {
        let current_shard = self.shard.read().unwrap();
        let shard = current_shard.fork();

        KeyIndex {
            config: self.config.clone(),
            shard: RwLock::new(shard),
        }
    }

    pub(crate) fn memory_size(&self) -> usize {
        self.shard.read().unwrap().memory_size()
    }
}

type PkIndexMap = BTreeMap<Vec<u8>, PkIndex>;

// TODO(yingwen): Support partition index (partition by a column, e.g. table_id) to
// reduce null columns and eliminate lock contention. We only need to partition the
// write buffer but modify dicts with partition lock held.
/// Mutable shard for the index.
struct Shard {
    shard_id: ShardId,
    // TODO(yingwen): Reuse keys.
    pk_to_index: PkIndexMap,
    key_buffer: KeyBuffer,
    dict_blocks: Vec<DictBlock>,
    num_keys: usize,
    /// Bytes allocated by keys in the [Shard::pk_to_index].
    key_bytes_in_index: usize,
    // TODO(yingwen): Serialize the shard instead of keeping the map.
    shared_index: Option<Arc<PkIndexMap>>,
}

impl Shard {
    fn new(shard_id: ShardId) -> Shard {
        Shard {
            shard_id,
            pk_to_index: BTreeMap::new(),
            key_buffer: KeyBuffer::new(MAX_KEYS_PER_BLOCK.into()),
            dict_blocks: Vec::new(),
            num_keys: 0,
            key_bytes_in_index: 0,
            shared_index: None,
        }
    }

    fn try_add_primary_key(
        &mut self,
        config: &IndexConfig,
        key: &[u8],
        metrics: &mut WriteMetrics,
    ) -> Result<Option<PkId>> {
        // The shard is full.
        if self.num_keys >= config.max_keys_per_shard {
            return Ok(None);
        }

        // The shard is immutable, find in the shared index.
        if let Some(shared_index) = &self.shared_index {
            let pk_id = shared_index.get(key).map(|pk_index| PkId {
                shard_id: self.shard_id,
                pk_index: *pk_index,
            });
            return Ok(pk_id);
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

        // Since we store the key two times so the bytes usage doubled.
        metrics.key_bytes += key.len() * 2;
        self.key_bytes_in_index += key.len();

        Ok(Some(PkId {
            shard_id: self.shard_id,
            pk_index,
        }))
    }

    fn scan_shard(&self) -> Result<ShardReader> {
        let pk_indices = self.sorted_pk_indices();
        let block = self.key_buffer.finish_cloned();
        let mut blocks = Vec::with_capacity(self.dict_blocks.len() + 1);
        blocks.extend_from_slice(&self.dict_blocks);
        blocks.push(block);

        Ok(ShardReader::new(blocks, pk_indices))
    }

    fn freeze(&mut self) -> Result<()> {
        if self.key_buffer.is_empty() {
            return Ok(());
        }

        let dict_block = self.key_buffer.finish();
        self.dict_blocks.push(dict_block);

        // Freezes the pk to index map.
        let pk_to_index = std::mem::take(&mut self.pk_to_index);
        self.shared_index = Some(Arc::new(pk_to_index));

        Ok(())
    }

    fn fork(&self) -> Shard {
        Shard {
            shard_id: self.shard_id,
            pk_to_index: BTreeMap::new(),
            key_buffer: KeyBuffer::new(MAX_KEYS_PER_BLOCK.into()),
            dict_blocks: self.dict_blocks.clone(),
            num_keys: self.num_keys,
            key_bytes_in_index: self.key_bytes_in_index,
            shared_index: self.shared_index.clone(),
        }
    }

    fn sorted_pk_indices(&self) -> Vec<PkIndex> {
        if let Some(shared_index) = &self.shared_index {
            return shared_index.values().copied().collect();
        }

        self.pk_to_index.values().copied().collect()
    }

    fn memory_size(&self) -> usize {
        self.key_bytes_in_index
            + self.key_buffer.buffer_memory_size()
            + self
                .dict_blocks
                .iter()
                .map(|block| block.buffer_memory_size())
                .sum::<usize>()
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

    fn is_empty(&self) -> bool {
        self.primary_key_builder.is_empty()
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
        // Reserve capacity for the new builder. `finish()` the builder will leave the builder
        // empty with capacity 0.
        // TODO(yingwen): Do we need to reserve capacity for data?
        self.primary_key_builder = BinaryBuilder::with_capacity(primary_key.len(), 0);

        DictBlock::from_unsorted(primary_key)
    }

    fn finish_cloned(&self) -> DictBlock {
        let primary_key = self.primary_key_builder.finish_cloned();

        DictBlock::from_unsorted(primary_key)
    }

    fn buffer_memory_size(&self) -> usize {
        self.primary_key_builder.values_slice().len()
            + std::mem::size_of_val(self.primary_key_builder.offsets_slice())
            + self
                .primary_key_builder
                .validity_slice()
                .map(|v| v.len())
                .unwrap_or(0)
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

    fn buffer_memory_size(&self) -> usize {
        self.primary_key.get_buffer_memory_size()
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

pub(crate) struct ShardReader {
    blocks: Vec<DictBlock>,
    sorted_pk_indices: Vec<PkIndex>,
    /// Current offset in the `sorted_pk_indices`.
    offset: usize,
}

impl ShardReader {
    fn new(blocks: Vec<DictBlock>, sorted_pk_indices: Vec<PkIndex>) -> ShardReader {
        ShardReader {
            blocks,
            sorted_pk_indices,
            offset: 0,
        }
    }

    fn key_by_pk_index(&self, pk_index: PkIndex) -> &[u8] {
        let block_idx = pk_index / MAX_KEYS_PER_BLOCK;
        self.blocks[block_idx as usize].key_by_pk_index(pk_index)
    }

    pub(crate) fn sorted_pk_index(&self) -> &[PkIndex] {
        &self.sorted_pk_indices
    }
}

pub(crate) fn compute_pk_weights(sorted_pk_indices: &[PkIndex], pk_weights: &mut Vec<u16>) {
    pk_weights.clear();
    pk_weights.resize(sorted_pk_indices.len(), 0);

    for (weight, pk_index) in sorted_pk_indices.iter().enumerate() {
        pk_weights[*pk_index as usize] = weight as u16;
    }
}

impl IndexReader for ShardReader {
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
        let mut metrics = WriteMetrics::default();
        for key in &keys {
            let pk_id = index.write_primary_key(key, &mut metrics).unwrap();
            last_pk_id = Some(pk_id);
        }
        assert_eq!(
            PkId {
                shard_id: 0,
                pk_index: num_keys - 1,
            },
            last_pk_id.unwrap()
        );
        let key_bytes: usize = keys.iter().map(|key| key.len() * 2).sum();
        assert_eq!(key_bytes, metrics.key_bytes);

        let mut expect: Vec<_> = keys
            .into_iter()
            .enumerate()
            .map(|(i, key)| (key, i as PkIndex))
            .collect();
        expect.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut result = Vec::with_capacity(expect.len());
        let mut reader = index.scan_shard(0).unwrap();
        while reader.is_valid() {
            result.push((reader.current_key().to_vec(), reader.current_pk_index()));
            reader.next();
        }
        assert_eq!(expect, result);
    }

    #[test]
    fn test_index_memory_size() {
        let index = KeyIndex::new(IndexConfig {
            max_keys_per_shard: (MAX_KEYS_PER_BLOCK * 3).into(),
        });
        let mut metrics = WriteMetrics::default();
        // 513 keys
        let num_keys = MAX_KEYS_PER_BLOCK * 2 + 1;
        // Writes 2 blocks
        for i in 0..num_keys {
            // Each key is 5 bytes.
            let key = format!("{i:05}");
            index
                .write_primary_key(key.as_bytes(), &mut metrics)
                .unwrap();
        }
        // num_keys * 5 * 2
        assert_eq!(5130, metrics.key_bytes);
        assert_eq!(8850, index.memory_size());
    }
}
