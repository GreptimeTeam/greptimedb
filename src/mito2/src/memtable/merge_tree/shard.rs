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

//! Shard in a partition.

use std::collections::HashSet;

use common_recordbatch::filter::SimpleFilterEvaluator;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::data::{DataBuffer, DataParts, DATA_INIT_CAP};
use crate::memtable::merge_tree::dict::KeyDictRef;
use crate::memtable::merge_tree::{PkId, ShardId};

/// Shard stores data related to the same key dictionary.
pub struct Shard {
    pub(crate) shard_id: ShardId,
    /// Key dictionary of the shard. `None` if the schema of the tree doesn't have a primary key.
    key_dict: Option<KeyDictRef>,
    /// Data in the shard.
    data_parts: DataParts,
}

impl Shard {
    /// Returns a new shard.
    pub fn new(shard_id: ShardId, key_dict: Option<KeyDictRef>, data_parts: DataParts) -> Shard {
        Shard {
            shard_id,
            key_dict,
            data_parts,
        }
    }

    /// Returns the pk id of the key if it exists.
    pub fn find_key(&self, key: &[u8]) -> Option<PkId> {
        let key_dict = self.key_dict.as_ref()?;
        let pk_index = key_dict.get_pk_index(key)?;

        Some(PkId {
            shard_id: self.shard_id,
            pk_index,
        })
    }

    /// Writes a key value into the shard.
    pub fn write_key_value(&mut self, pk_id: PkId, key_value: KeyValue) {
        debug_assert_eq!(self.shard_id, pk_id.shard_id);

        self.data_parts.active.write_row(pk_id.pk_index, key_value);
    }

    /// Scans the shard.
    // TODO(yingwen): Push down projection to data parts.
    pub fn scan(&self) -> ShardReader {
        unimplemented!()
    }

    /// Returns the memory size of the shard part.
    pub fn shared_memory_size(&self) -> usize {
        self.key_dict
            .as_ref()
            .map(|dict| dict.shared_memory_size())
            .unwrap_or(0)
    }

    /// Forks a shard.
    pub fn fork(&self, metadata: RegionMetadataRef) -> Shard {
        Shard {
            shard_id: self.shard_id,
            key_dict: self.key_dict.clone(),
            data_parts: DataParts {
                active: DataBuffer::with_capacity(metadata, DATA_INIT_CAP),
                frozen: Vec::new(),
            },
        }
    }
}

/// Reader to read rows in a shard.
pub struct ShardReader {}
