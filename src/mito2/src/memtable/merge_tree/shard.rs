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

use store_api::metadata::RegionMetadataRef;

use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::data::{DataBatch, DataParts, DataPartsReader, DATA_INIT_CAP};
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
    pub fn find_id_by_key(&self, key: &[u8]) -> Option<PkId> {
        let key_dict = self.key_dict.as_ref()?;
        let pk_index = key_dict.get_pk_index(key)?;

        Some(PkId {
            shard_id: self.shard_id,
            pk_index,
        })
    }

    /// Writes a key value into the shard.
    pub fn write_with_pk_id(&mut self, pk_id: PkId, key_value: KeyValue) {
        debug_assert_eq!(self.shard_id, pk_id.shard_id);

        self.data_parts.write_row(pk_id.pk_index, key_value);
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
            data_parts: DataParts::new(metadata, DATA_INIT_CAP),
        }
    }
}

/// Reader to read rows in a shard.
pub struct ShardReader {
    key_dict: Option<KeyDictRef>,
    parts_reader: DataPartsReader,
}

impl ShardReader {
    fn is_valid(&self) -> bool {
        self.parts_reader.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.parts_reader.next()
    }

    fn current_key(&self) -> Option<&[u8]> {
        let pk_index = self.parts_reader.current_data_batch().pk_index();
        self.key_dict
            .as_ref()
            .map(|dict| dict.key_by_pk_index(pk_index))
    }

    fn current_batch(&self) -> DataBatch {
        self.parts_reader.current_data_batch()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::memtable::merge_tree::dict::KeyDictBuilder;
    use crate::memtable::merge_tree::metrics::WriteMetrics;
    use crate::memtable::merge_tree::PkIndex;
    use crate::memtable::KeyValues;
    use crate::test_util::memtable_util::{
        build_key_values_with_ts_seq_values, encode_key, encode_key_by_kv, encode_keys,
        metadata_for_test,
    };

    fn input_with_key(metadata: &RegionMetadataRef) -> Vec<KeyValues> {
        vec![
            build_key_values_with_ts_seq_values(
                metadata,
                "shard".to_string(),
                2,
                [20, 21].into_iter(),
                [Some(0.0), Some(1.0)].into_iter(),
                0,
            ),
            build_key_values_with_ts_seq_values(
                metadata,
                "shard".to_string(),
                0,
                [0, 1].into_iter(),
                [Some(0.0), Some(1.0)].into_iter(),
                1,
            ),
            build_key_values_with_ts_seq_values(
                metadata,
                "shard".to_string(),
                1,
                [10, 11].into_iter(),
                [Some(0.0), Some(1.0)].into_iter(),
                2,
            ),
        ]
    }

    fn new_shard_with_dict(
        shard_id: ShardId,
        metadata: RegionMetadataRef,
        input: &[KeyValues],
    ) -> Shard {
        let mut dict_builder = KeyDictBuilder::new(1024);
        let mut metrics = WriteMetrics::default();
        let mut keys = Vec::with_capacity(input.len());
        for kvs in input {
            encode_keys(&metadata, kvs, &mut keys);
        }
        for key in &keys {
            dict_builder.insert_key(key, &mut metrics);
        }

        let dict = dict_builder.finish().unwrap();
        let data_parts = DataParts::new(metadata, DATA_INIT_CAP);

        Shard::new(shard_id, Some(Arc::new(dict)), data_parts)
    }

    #[test]
    fn test_shard_find_by_key() {
        let metadata = metadata_for_test();
        let input = input_with_key(&metadata);
        let shard = new_shard_with_dict(8, metadata, &input);
        for i in 0..input.len() {
            let key = encode_key("shard", i as u32);
            assert_eq!(
                PkId {
                    shard_id: 8,
                    pk_index: i as PkIndex,
                },
                shard.find_id_by_key(&key).unwrap()
            );
        }
        assert!(shard.find_id_by_key(&encode_key("shard", 100)).is_none());
    }

    #[test]
    fn test_write_shard() {
        let metadata = metadata_for_test();
        let input = input_with_key(&metadata);
        let mut shard = new_shard_with_dict(8, metadata, &input);
        for key_values in &input {
            for kv in key_values.iter() {
                let key = encode_key_by_kv(&kv);
                let pk_id = shard.find_id_by_key(&key).unwrap();
                shard.write_with_pk_id(pk_id, kv);
            }
        }
    }
}
