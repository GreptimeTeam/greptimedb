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

/// Primary key index.
pub(crate) struct KeyIndex {}

impl KeyIndex {
    pub(crate) fn add_primary_key(&mut self, key: &[u8]) -> Result<PkId> {
        unimplemented!()
    }
}

// TODO(yingwen): Support partition index (partition by a column, e.g. table_id) to
// reduce null columns and eliminate lock contention. We only need to partition the
// write buffer but modify dicts with partition lock held.
/// Mutable shard for the index.
struct MutableShard {
    shard_id: ShardId,
    key_buffer: KeyBuffer,
    dict_blocks: Vec<DictBlock>,
    num_keys: usize,
}

impl MutableShard {
    fn try_add_primary_key(&mut self, config: &IndexConfig, key: &[u8]) -> Result<Option<PkId>> {
        // The shard is full.
        if self.num_keys >= config.max_keys_per_shard {
            return Ok(None);
        }

        if self.key_buffer.len() >= MAX_KEYS_PER_BLOCK.into() {
            // The write buffer is full.
            let dict_block = self.key_buffer.finish()?;
            self.dict_blocks.push(dict_block);
        }

        // Safety: we check the buffer length.
        let pk_index = self.key_buffer.push_key(key);

        Ok(Some(PkId {
            shard_id: self.shard_id,
            pk_index,
        }))
    }
}

// TODO(yingwen): Bench using custom container for binary and ids so we can
// sort the buffer in place and reuse memory.
/// Buffer to store unsorted primary keys.
///
/// Now it doesn't support searching index by key. The memtable should use another
/// cache to map primary key to its index.
struct KeyBuffer {
    // We use arrow's binary builder as out default binary builder
    // is LargeBinaryBuilder
    primary_key_builder: BinaryBuilder,
    next_pk_index: usize,
}

impl KeyBuffer {
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

    fn get_key(&self, index: PkIndex) -> &[u8] {
        // Casting index to usize is safe.
        let pos = self.index_weight[index as usize];
        self.primary_key.value(pos as usize)
    }
}
