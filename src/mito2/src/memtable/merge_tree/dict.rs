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

//! Key dictionary of a shard.

use std::collections::BTreeMap;
use std::sync::Arc;

use datatypes::arrow::array::{Array, ArrayBuilder, BinaryArray, BinaryBuilder};

use crate::error::Result;
use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::PkIndex;

/// Maximum keys in a [DictBlock].
const MAX_KEYS_PER_BLOCK: u16 = 256;

type PkIndexMap = BTreeMap<Vec<u8>, PkIndex>;

/// Builder to build a key dictionary.
pub struct KeyDictBuilder {
    /// Max keys of the dictionary.
    capacity: usize,
    /// Number of keys in the builder.
    num_keys: usize,
    /// Maps primary key to pk index.
    pk_to_index: PkIndexMap,
    /// Buffer for active dict block.
    key_buffer: KeyBuffer,
    /// Dictionary blocks.
    dict_blocks: Vec<DictBlock>,
    /// Bytes allocated by keys in the [pk_to_index](Self::pk_to_index).
    key_bytes_in_index: usize,
}

impl KeyDictBuilder {
    /// Creates a new builder that can hold up to `capacity` keys.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            num_keys: 0,
            pk_to_index: BTreeMap::new(),
            key_buffer: KeyBuffer::new(MAX_KEYS_PER_BLOCK.into()),
            dict_blocks: Vec::new(),
            key_bytes_in_index: 0,
        }
    }

    /// Gets the pk index by the key.
    pub fn get_pk_index(&self, key: &[u8]) -> Option<PkIndex> {
        self.pk_to_index.get(key).copied()
    }

    /// Adds the key to the builder and returns its index if the builder is not full.
    ///
    /// Returns `None` if the builder is full.
    pub fn try_insert_key(
        &mut self,
        key: &[u8],
        metrics: &mut WriteMetrics,
    ) -> Result<Option<PkIndex>> {
        if let Some(pk_index) = self.pk_to_index.get(key).copied() {
            // Already in the builder.
            return Ok(Some(pk_index));
        }

        // A new key.
        if self.num_keys >= self.capacity {
            // The builder is full.
            return Ok(None);
        }

        if self.key_buffer.len() >= MAX_KEYS_PER_BLOCK.into() {
            // The write buffer is full. Freeze a dict block.
            let dict_block = self.key_buffer.finish();
            self.dict_blocks.push(dict_block);
        }

        // Safety: we have checked the buffer length.
        let pk_index = self.key_buffer.push_key(key);
        self.pk_to_index.insert(key.to_vec(), pk_index);
        self.num_keys += 1;

        // Since we store the key twice so the bytes usage doubled.
        metrics.key_bytes += key.len() * 2;
        self.key_bytes_in_index += key.len();

        Ok(Some(pk_index))
    }

    /// Memory size of the builder.
    pub fn memory_size(&self) -> usize {
        self.key_bytes_in_index
            + self.key_buffer.buffer_memory_size()
            + self
                .dict_blocks
                .iter()
                .map(|block| block.buffer_memory_size())
                .sum::<usize>()
    }

    /// Finishes the builder.
    pub fn finish(&mut self) -> Option<KeyDict> {
        if self.key_buffer.is_empty() {
            return None;
        }

        // Finishes current dict block.
        let dict_block = self.key_buffer.finish();
        self.dict_blocks.push(dict_block);
        // Takes the pk to index map.
        let mut pk_to_index = std::mem::take(&mut self.pk_to_index);
        // Computes key position and then alter pk index.
        let mut key_positions = vec![0; pk_to_index.len()];
        for (i, pk_index) in pk_to_index.values_mut().enumerate() {
            // The position of the i-th key is the old pk index.
            key_positions[i] = *pk_index;
            // Overwrites the pk index.
            *pk_index = i as PkIndex;
        }

        Some(KeyDict {
            pk_to_index,
            dict_blocks: std::mem::take(&mut self.dict_blocks),
            key_positions,
        })
    }

    /// Scans the builder.
    pub fn scan(&self) -> DictBuilderReader {
        let sorted_pk_indices = self.pk_to_index.values().copied().collect();
        let block = self.key_buffer.finish_cloned();
        let mut blocks = Vec::with_capacity(self.dict_blocks.len() + 1);
        blocks.extend_from_slice(&self.dict_blocks);
        blocks.push(block);

        DictBuilderReader::new(blocks, sorted_pk_indices)
    }
}

/// Reader to scan the [KeyDictBuilder].
#[derive(Default)]
pub struct DictBuilderReader {
    blocks: Vec<DictBlock>,
    sorted_pk_indices: Vec<PkIndex>,
    /// Current offset in the `sorted_pk_indices`.
    offset: usize,
}

impl DictBuilderReader {
    fn new(blocks: Vec<DictBlock>, sorted_pk_indices: Vec<PkIndex>) -> Self {
        Self {
            blocks,
            sorted_pk_indices,
            offset: 0,
        }
    }

    /// Returns true if the item in the reader is valid.
    pub fn is_valid(&self) -> bool {
        self.offset < self.sorted_pk_indices.len()
    }

    /// Returns current key.
    pub fn current_key(&self) -> &[u8] {
        let pk_index = self.current_pk_index();
        self.key_by_pk_index(pk_index)
    }

    /// Returns current [PkIndex] of the key.
    pub fn current_pk_index(&self) -> PkIndex {
        assert!(self.is_valid());
        self.sorted_pk_indices[self.offset]
    }

    /// Advances the reader.
    pub fn next(&mut self) {
        assert!(self.is_valid());
        self.offset += 1;
    }

    /// Returns pk indices sorted by keys.
    pub(crate) fn sorted_pk_index(&self) -> &[PkIndex] {
        &self.sorted_pk_indices
    }

    fn key_by_pk_index(&self, pk_index: PkIndex) -> &[u8] {
        let block_idx = pk_index / MAX_KEYS_PER_BLOCK;
        self.blocks[block_idx as usize].key_by_pk_index(pk_index)
    }
}

/// A key dictionary.
#[derive(Default)]
pub struct KeyDict {
    // TODO(yingwen): We can use key_positions to do a binary search.
    /// Key map to find a key in the dict.
    pk_to_index: PkIndexMap,
    /// Unsorted key blocks.
    dict_blocks: Vec<DictBlock>,
    /// Maps pk index to position of the key in [Self::dict_blocks].
    key_positions: Vec<PkIndex>,
}

pub type KeyDictRef = Arc<KeyDict>;

impl KeyDict {
    /// Gets the primary key by its index.
    ///
    /// # Panics
    /// Panics if the index is invalid.
    pub fn key_by_pk_index(&self, index: PkIndex) -> &[u8] {
        let position = self.key_positions[index as usize];
        let block_index = position / MAX_KEYS_PER_BLOCK;
        self.dict_blocks[block_index as usize].key_by_pk_index(position)
    }
}

/// Buffer to store unsorted primary keys.
struct KeyBuffer {
    // We use arrow's binary builder as out default binary builder
    // is LargeBinaryBuilder
    // TODO(yingwen): Change the type binary vector to Binary instead of LargeBinary.
    /// Builder for binary key array.
    key_builder: BinaryBuilder,
    next_pk_index: usize,
}

impl KeyBuffer {
    fn new(item_capacity: usize) -> Self {
        Self {
            key_builder: BinaryBuilder::with_capacity(item_capacity, 0),
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
        self.key_builder.append_value(key);

        pk_index
    }

    /// Returns number of items in the buffer.
    fn len(&self) -> usize {
        self.key_builder.len()
    }

    /// Returns whether the buffer is empty.
    fn is_empty(&self) -> bool {
        self.key_builder.is_empty()
    }

    /// Gets the primary key by its index.
    ///
    /// # Panics
    /// Panics if the index is invalid.
    fn get_key(&self, index: PkIndex) -> &[u8] {
        let values = self.key_builder.values_slice();
        let offsets = self.key_builder.offsets_slice();
        // Casting index to usize is safe.
        let start = offsets[index as usize];
        let end = offsets[index as usize + 1];

        // We ensure no null in the builder so we don't check validity.
        // The builder offset should be positive.
        &values[start as usize..end as usize]
    }

    /// Returns the buffer size of the builder.
    fn buffer_memory_size(&self) -> usize {
        self.key_builder.values_slice().len()
            + std::mem::size_of_val(self.key_builder.offsets_slice())
            + self
                .key_builder
                .validity_slice()
                .map(|v| v.len())
                .unwrap_or(0)
    }

    fn finish(&mut self) -> DictBlock {
        let primary_key = self.key_builder.finish();
        // Reserve capacity for the new builder. `finish()` the builder will leave the builder
        // empty with capacity 0.
        // TODO(yingwen): Do we need to reserve capacity for data?
        self.key_builder = BinaryBuilder::with_capacity(primary_key.len(), 0);

        DictBlock::new(primary_key)
    }

    fn finish_cloned(&self) -> DictBlock {
        let primary_key = self.key_builder.finish_cloned();

        DictBlock::new(primary_key)
    }
}

/// A block in the key dictionary.
///
/// The block is cheap to clone. Keys in the block are unsorted.
#[derive(Clone)]
struct DictBlock {
    /// Container of keys in the block.
    keys: BinaryArray,
}

impl DictBlock {
    fn new(keys: BinaryArray) -> Self {
        Self { keys }
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    fn key_by_pk_index(&self, index: PkIndex) -> &[u8] {
        let pos = index % MAX_KEYS_PER_BLOCK;
        self.keys.value(pos as usize)
    }

    fn buffer_memory_size(&self) -> usize {
        self.keys.get_buffer_memory_size()
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
    fn test_write_scan_builder() {
        let num_keys = MAX_KEYS_PER_BLOCK * 2 + MAX_KEYS_PER_BLOCK / 2;
        let keys = prepare_input_keys(num_keys.into());

        let mut builder = KeyDictBuilder::new((MAX_KEYS_PER_BLOCK * 3).into());
        let mut last_pk_index = None;
        let mut metrics = WriteMetrics::default();
        for key in &keys {
            let pk_index = builder.try_insert_key(key, &mut metrics).unwrap().unwrap();
            last_pk_index = Some(pk_index);
        }
        assert_eq!(num_keys - 1, last_pk_index.unwrap());
        let key_bytes: usize = keys.iter().map(|key| key.len() * 2).sum();
        assert_eq!(key_bytes, metrics.key_bytes);

        let mut expect: Vec<_> = keys
            .into_iter()
            .enumerate()
            .map(|(i, key)| (key, i as PkIndex))
            .collect();
        expect.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut result = Vec::with_capacity(expect.len());
        let mut reader = builder.scan();
        while reader.is_valid() {
            result.push((reader.current_key().to_vec(), reader.current_pk_index()));
            reader.next();
        }
        assert_eq!(expect, result);
    }

    #[test]
    fn test_builder_memory_size() {
        let mut builder = KeyDictBuilder::new((MAX_KEYS_PER_BLOCK * 3).into());
        let mut metrics = WriteMetrics::default();
        // 513 keys
        let num_keys = MAX_KEYS_PER_BLOCK * 2 + 1;
        // Writes 2 blocks
        for i in 0..num_keys {
            // Each key is 5 bytes.
            let key = format!("{i:05}");
            builder
                .try_insert_key(key.as_bytes(), &mut metrics)
                .unwrap();
        }
        // num_keys * 5 * 2
        assert_eq!(5130, metrics.key_bytes);
        assert_eq!(8850, builder.memory_size());
    }
}
