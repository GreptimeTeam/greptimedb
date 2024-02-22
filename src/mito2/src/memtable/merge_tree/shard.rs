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
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::data::DataParts;
use crate::memtable::merge_tree::dict::KeyDictRef;
use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::{PkId, ShardId};

/// Shard stores data related to the same key dictionary.
pub struct Shard {
    shard_id: ShardId,
    /// Key dictionary of the shard. `None` if the schema of the tree doesn't have a primary key.
    key_dict: Option<KeyDictRef>,
    /// Data in the shard.
    data_parts: DataParts,
}

impl Shard {
    /// Returns a shard without dictionary.
    pub fn new_no_dict(_shard_id: ShardId) -> Shard {
        unimplemented!()
    }

    /// Returns the pk id of the key if it exists.
    pub fn find_key(&self, _key: &[u8]) -> Option<PkId> {
        unimplemented!()
    }

    /// Writes a key value into the shard.
    pub fn write_key_value(
        &mut self,
        _pk_id: PkId,
        _key_value: KeyValue,
        _metrics: &mut WriteMetrics,
    ) -> Result<()> {
        unimplemented!()
    }

    /// Scans the shard.
    pub fn scan(
        &self,
        _projection: &HashSet<ColumnId>,
        _filters: &[SimpleFilterEvaluator],
    ) -> ShardReader {
        unimplemented!()
    }
}

/// Reader to read rows in a shard.
pub struct ShardReader {}
