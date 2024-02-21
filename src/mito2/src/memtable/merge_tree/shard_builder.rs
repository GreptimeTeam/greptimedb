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

//! Builder of a shard.

use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::data::DataBuffer;
use crate::memtable::merge_tree::dict::KeyDictBuilder;
use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::shard::Shard;
use crate::memtable::merge_tree::ShardId;

/// Builder to write keys and data to a shard that the key dictionary
/// is still active.
pub struct ShardBuilder {
    /// Builder for the key dictionary.
    dict_builder: KeyDictBuilder,
    /// Buffer to store data.
    data_buffer: DataBuffer,
    /// Max keys in an index shard.
    index_max_keys_per_shard: usize,
    /// Number of rows to freeze a data part.
    data_freeze_threshold: usize,
}

impl ShardBuilder {
    /// Write a key value with its encoded primary key.
    pub fn write_with_key(
        &mut self,
        _key: &[u8],
        _key_value: KeyValue,
        _metrics: &mut WriteMetrics,
    ) -> Result<()> {
        unimplemented!()
    }

    /// Returns true if the builder is empty.
    pub fn is_empty(&self) -> bool {
        unimplemented!()
    }

    /// Returns true if the builder need to freeze.
    pub fn should_freeze(&self) -> bool {
        unimplemented!()
    }

    /// Builds a new shard and resets the builder.
    pub fn finish(&mut self, _shard_id: ShardId) -> Result<Shard> {
        unimplemented!()
    }

    /// Builds a new shard.
    pub fn finish_cloned(&mut self, _shard_id: ShardId) -> Result<Shard> {
        unimplemented!()
    }
}
