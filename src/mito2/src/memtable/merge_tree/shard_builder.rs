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

use std::collections::HashSet;
use std::sync::Arc;

use common_recordbatch::filter::SimpleFilterEvaluator;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::data::{DataBuffer, DataParts, DATA_INIT_CAP};
use crate::memtable::merge_tree::dict::KeyDictBuilder;
use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::shard::Shard;
use crate::memtable::merge_tree::{MergeTreeConfig, ShardId};

/// Builder to write keys and data to a shard that the key dictionary
/// is still active.
pub struct ShardBuilder {
    /// Builder for the key dictionary.
    dict_builder: KeyDictBuilder,
    /// Buffer to store data.
    data_buffer: DataBuffer,
    /// Number of rows to freeze a data part.
    data_freeze_threshold: usize,
}

impl ShardBuilder {
    /// Returns a new builder.
    pub fn new(metadata: RegionMetadataRef, config: &MergeTreeConfig) -> ShardBuilder {
        ShardBuilder {
            dict_builder: KeyDictBuilder::new(config.index_max_keys_per_shard),
            data_buffer: DataBuffer::with_capacity(metadata, DATA_INIT_CAP),
            data_freeze_threshold: config.data_freeze_threshold,
        }
    }

    /// Write a key value with its encoded primary key.
    pub fn write_with_key(&mut self, key: &[u8], key_value: KeyValue, metrics: &mut WriteMetrics) {
        // Safety: we check whether the builder need to freeze before.
        let pk_index = self.dict_builder.insert_key(key, metrics);
        self.data_buffer.write_row(pk_index, key_value);
    }

    /// Returns true if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.data_buffer.is_empty()
    }

    /// Returns true if the builder need to freeze.
    pub fn should_freeze(&self) -> bool {
        self.dict_builder.is_full() || self.data_buffer.num_rows() == self.data_freeze_threshold
    }

    /// Builds a new shard and resets the builder.
    pub fn finish(
        &mut self,
        shard_id: ShardId,
        metadata: RegionMetadataRef,
    ) -> Result<Option<Shard>> {
        if self.is_empty() {
            return Ok(None);
        }

        let key_dict = self.dict_builder.finish();
        let data_part = match &key_dict {
            Some(dict) => {
                let pk_weights = dict.pk_weights_to_sort_data();
                self.data_buffer.freeze(&pk_weights)?
            }
            None => {
                let pk_weights = [0];
                self.data_buffer.freeze(&pk_weights)?
            }
        };

        // build data parts.
        let data_parts = DataParts {
            active: DataBuffer::with_capacity(metadata, DATA_INIT_CAP),
            frozen: vec![data_part],
        };
        let key_dict = key_dict.map(Arc::new);

        Ok(Some(Shard::new(shard_id, key_dict, data_parts)))
    }

    /// Scans the shard builder.
    pub fn scan(
        &mut self,
        _projection: &HashSet<ColumnId>,
        _filters: &[SimpleFilterEvaluator],
    ) -> Result<ShardBuilderReader> {
        unimplemented!()
    }
}

/// Reader to scan a shard. builder.
pub struct ShardBuilderReader {}

// TODO(yingwen): Can we use generic for data reader?
