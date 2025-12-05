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

use crate::memtable::partition_tree::PkIndex;
use crate::memtable::stats::WriteMetrics;
use crate::metrics::MEMTABLE_DICT_BYTES;

/// Maximum keys in a [DictBlock].
const MAX_KEYS_PER_BLOCK: u16 = 256;

/// The key is mcmp-encoded primary keys, while the values are the pk index and
/// optionally sparsely encoded primary keys.
type PkIndexMap = BTreeMap<Vec<u8>, (PkIndex, Option<Vec<u8>>)>;

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
    /// Bytes allocated by keys in the index.
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
            dict_blocks: Vec::with_capacity(capacity / MAX_KEYS_PER_BLOCK as usize + 1),
            key_bytes_in_index: 0,
        }
    }

    /// Returns true if the builder is full.
    pub fn is_full(&self) -> bool {
        self.num_keys >= self.capacity
    }

    /// Adds the key to the builder and returns its index if the builder is not full.
    ///
    /// # Panics
    /// Panics if the builder is full.
    pub fn insert_key(
        &mut self,
        full_primary_key: &[u8],
        sparse_key: Option<&[u8]>,
        metrics: &mut WriteMetrics,
    ) -> PkIndex {
        assert!(!self.is_full());

        if let Some(pk_index) = self.pk_to_index.get(full_primary_key).map(|v| v.0) {
            // Already in the builder.
            return pk_index;
        }

        if self.key_buffer.len() >= MAX_KEYS_PER_BLOCK.into() {
            // The write buffer is full. Freeze a dict block.
            let dict_block = self.key_buffer.finish(false);
            self.dict_blocks.push(dict_block);
        }

        // Safety: we have checked the buffer length.
        let pk_index = self.key_buffer.push_key(full_primary_key);
        let (sparse_key, sparse_key_len) = if let Some(sparse_key) = sparse_key {
            (Some(sparse_key.to_vec()), sparse_key.len())
        } else {
            (None, 0)
        };
        self.pk_to_index
            .insert(full_primary_key.to_vec(), (pk_index, sparse_key));
        self.num_keys += 1;

        // Since we store the key twice so the bytes usage doubled.
        metrics.key_bytes += full_primary_key.len() * 2 + sparse_key_len;
        self.key_bytes_in_index += full_primary_key.len() + sparse_key_len;

        // Adds key size of index to the metrics.
        MEMTABLE_DICT_BYTES.add((full_primary_key.len() + sparse_key_len) as i64);

        pk_index
    }

    /// Memory size of the builder.
    #[cfg(test)]
    pub fn memory_size(&self) -> usize {
        self.key_bytes_in_index
            + self.key_buffer.buffer_memory_size()
            + self
                .dict_blocks
                .iter()
                .map(|block| block.buffer_memory_size())
                .sum::<usize>()
    }

    /// Finishes the builder. The key of the second BTreeMap is sparse-encoded bytes.
    pub fn finish(&mut self) -> Option<(KeyDict, BTreeMap<Vec<u8>, PkIndex>)> {
        if self.key_buffer.is_empty() {
            return None;
        }
        let mut pk_to_index_map = BTreeMap::new();

        // Finishes current dict block and resets the pk index.
        let dict_block = self.key_buffer.finish(true);
        self.dict_blocks.push(dict_block);
        // Computes key position and then alter pk index.
        let mut key_positions = vec![0; self.pk_to_index.len()];

        for (i, (full_pk, (pk_index, sparse_key))) in (std::mem::take(&mut self.pk_to_index))
            .into_iter()
            .enumerate()
        {
            // The position of the i-th key is the old pk index.
            key_positions[i] = pk_index;
            if let Some(sparse_key) = sparse_key {
                pk_to_index_map.insert(sparse_key, i as PkIndex);
            }
            pk_to_index_map.insert(full_pk, i as PkIndex);
        }

        self.num_keys = 0;
        let key_bytes_in_index = self.key_bytes_in_index;
        self.key_bytes_in_index = 0;

        Some((
            KeyDict {
                dict_blocks: std::mem::take(&mut self.dict_blocks),
                key_positions,
                key_bytes_in_index,
            },
            pk_to_index_map,
        ))
    }

    /// Reads the builder.
    pub fn read(&self) -> DictBuilderReader {
        let sorted_pk_indices = self.pk_to_index.values().map(|v| v.0).collect();
        let block = self.key_buffer.finish_cloned();
        let mut blocks = Vec::with_capacity(self.dict_blocks.len() + 1);
        blocks.extend_from_slice(&self.dict_blocks);
        blocks.push(block);

        DictBuilderReader::new(blocks, sorted_pk_indices)
    }
}

impl Drop for KeyDictBuilder {
    fn drop(&mut self) {
        MEMTABLE_DICT_BYTES.sub(self.key_bytes_in_index as i64);
    }
}

/// Reader to scan the [KeyDictBuilder].
#[derive(Default)]
pub struct DictBuilderReader {
    blocks: Vec<DictBlock>,
    sorted_pk_indices: Vec<PkIndex>,
}

impl DictBuilderReader {
    fn new(blocks: Vec<DictBlock>, sorted_pk_indices: Vec<PkIndex>) -> Self {
        Self {
            blocks,
            sorted_pk_indices,
        }
    }

    /// Returns the number of keys.
    #[cfg(test)]
    pub fn num_keys(&self) -> usize {
        self.sorted_pk_indices.len()
    }

    /// Gets the i-th pk index.
    #[cfg(test)]
    pub fn pk_index(&self, offset: usize) -> PkIndex {
        self.sorted_pk_indices[offset]
    }

    /// Gets the i-th key.
    #[cfg(test)]
    pub fn key(&self, offset: usize) -> &[u8] {
        let pk_index = self.pk_index(offset);
        self.key_by_pk_index(pk_index)
    }

    /// Gets the key by the pk index.
    pub fn key_by_pk_index(&self, pk_index: PkIndex) -> &[u8] {
        let block_idx = pk_index / MAX_KEYS_PER_BLOCK;
        self.blocks[block_idx as usize].key_by_pk_index(pk_index)
    }

    /// Returns pk weights to sort a data part and replaces pk indices.
    pub(crate) fn pk_weights_to_sort_data(&self, pk_weights: &mut Vec<u16>) {
        compute_pk_weights(&self.sorted_pk_indices, pk_weights)
    }
}

/// Returns pk weights to sort a data part and replaces pk indices.
fn compute_pk_weights(sorted_pk_indices: &[PkIndex], pk_weights: &mut Vec<u16>) {
    pk_weights.resize(sorted_pk_indices.len(), 0);
    for (weight, pk_index) in sorted_pk_indices.iter().enumerate() {
        pk_weights[*pk_index as usize] = weight as u16;
    }
}

/// A key dictionary.
#[derive(Default)]
pub struct KeyDict {
    // TODO(yingwen): We can use key_positions to do a binary search.
    /// Unsorted key blocks.
    dict_blocks: Vec<DictBlock>,
    /// Maps pk index to position of the key in [Self::dict_blocks].
    key_positions: Vec<PkIndex>,
    /// Bytes of keys in the index.
    key_bytes_in_index: usize,
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

    /// Returns pk weights to sort a data part and replaces pk indices.
    pub(crate) fn pk_weights_to_sort_data(&self) -> Vec<u16> {
        let mut pk_weights = Vec::with_capacity(self.key_positions.len());
        compute_pk_weights(&self.key_positions, &mut pk_weights);
        pk_weights
    }

    /// Returns the shared memory size.
    pub(crate) fn shared_memory_size(&self) -> usize {
        self.key_bytes_in_index
            + self
                .dict_blocks
                .iter()
                .map(|block| block.buffer_memory_size())
                .sum::<usize>()
    }
}

impl Drop for KeyDict {
    fn drop(&mut self) {
        MEMTABLE_DICT_BYTES.sub(self.key_bytes_in_index as i64);
    }
}

/// Buffer to store unsorted primary keys.
struct KeyBuffer {
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

    /// Returns the buffer size of the builder.
    #[cfg(test)]
    fn buffer_memory_size(&self) -> usize {
        self.key_builder.values_slice().len()
            + std::mem::size_of_val(self.key_builder.offsets_slice())
            + self
                .key_builder
                .validity_slice()
                .map(|v| v.len())
                .unwrap_or(0)
    }

    fn finish(&mut self, reset_index: bool) -> DictBlock {
        let primary_key = self.key_builder.finish();
        // Reserve capacity for the new builder. `finish()` the builder will leave the builder
        // empty with capacity 0.
        // TODO(yingwen): Do we need to reserve capacity for data?
        self.key_builder = BinaryBuilder::with_capacity(primary_key.len(), 0);
        if reset_index {
            self.next_pk_index = 0;
        }

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
        let buffer_size = keys.get_buffer_memory_size();
        MEMTABLE_DICT_BYTES.add(buffer_size as i64);

        Self { keys }
    }

    fn key_by_pk_index(&self, index: PkIndex) -> &[u8] {
        let pos = index % MAX_KEYS_PER_BLOCK;
        self.keys.value(pos as usize)
    }

    fn buffer_memory_size(&self) -> usize {
        self.keys.get_buffer_memory_size()
    }
}

impl Drop for DictBlock {
    fn drop(&mut self) {
        let buffer_size = self.keys.get_buffer_memory_size();
        MEMTABLE_DICT_BYTES.sub(buffer_size as i64);
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    fn prepare_input_keys(num_keys: usize) -> Vec<Vec<u8>> {
        let prefix = ["a", "b", "c", "d", "e", "f"];
        let mut rng = rand::rng();
        let mut keys = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            let prefix_idx = rng.random_range(0..prefix.len());
            // We don't need to decode the primary key in index's test so we format the string
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
            assert!(!builder.is_full());
            let pk_index = builder.insert_key(key, None, &mut metrics);
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
        let reader = builder.read();
        for i in 0..reader.num_keys() {
            result.push((reader.key(i).to_vec(), reader.pk_index(i)));
        }
        assert_eq!(expect, result);
    }

    #[test]
    fn test_dict_memory_size() {
        let mut builder = KeyDictBuilder::new((MAX_KEYS_PER_BLOCK * 3).into());
        let mut metrics = WriteMetrics::default();
        // 513 keys
        let num_keys = MAX_KEYS_PER_BLOCK * 2 + 1;
        // Writes 2 blocks
        for i in 0..num_keys {
            // Each key is 5 bytes.
            let key = format!("{i:05}");
            builder.insert_key(key.as_bytes(), None, &mut metrics);
        }
        let key_bytes = num_keys as usize * 5;
        assert_eq!(key_bytes * 2, metrics.key_bytes);
        assert_eq!(key_bytes, builder.key_bytes_in_index);
        assert_eq!(8730, builder.memory_size());

        let (dict, _) = builder.finish().unwrap();
        assert_eq!(0, builder.key_bytes_in_index);
        assert_eq!(key_bytes, dict.key_bytes_in_index);
        assert!(dict.shared_memory_size() > key_bytes);
    }

    #[test]
    fn test_builder_finish() {
        let mut builder = KeyDictBuilder::new((MAX_KEYS_PER_BLOCK * 2).into());
        let mut metrics = WriteMetrics::default();
        for i in 0..MAX_KEYS_PER_BLOCK * 2 {
            let key = format!("{i:010}");
            assert!(!builder.is_full());
            builder.insert_key(key.as_bytes(), None, &mut metrics);
        }
        assert!(builder.is_full());
        builder.finish();

        assert!(!builder.is_full());
        assert_eq!(0, builder.insert_key(b"a0", None, &mut metrics));
    }

    #[test]
    fn test_builder_finish_with_sparse_key() {
        let mut builder = KeyDictBuilder::new((MAX_KEYS_PER_BLOCK * 2).into());
        let mut metrics = WriteMetrics::default();
        let full_key = "42".to_string();
        let sparse_key = &[42u8];

        builder.insert_key(full_key.as_bytes(), Some(sparse_key), &mut metrics);
        let (dict, pk_to_pk_id) = builder.finish().unwrap();
        assert_eq!(dict.key_positions.len(), 1);
        assert_eq!(dict.dict_blocks.len(), 1);
        assert_eq!(
            pk_to_pk_id.get(sparse_key.as_slice()),
            pk_to_pk_id.get(full_key.as_bytes())
        );
    }
}
