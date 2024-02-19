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
    pub fn try_add_key(
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
}

/// Buffer to store unsorted primary keys.
pub struct KeyBuffer {
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

/// A key dictionary.
#[derive(Default)]
pub struct KeyDict {
    /// Key map to find a key in the dict.
    pk_to_index: PkIndexMap,
    /// Unsorted key blocks.
    dict_blocks: Vec<DictBlock>,
    /// Maps pk index to position of the key in [Self::dict_blocks].
    key_positions: Vec<PkIndex>,
}

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

pub type KeyDictRef = Arc<KeyDict>;

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
