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

//! Mutable part of the merge tree.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use api::v1::OpType;
use bytes::Bytes;
use common_telemetry::warn;
use common_time::Timestamp;
use datatypes::data_type::DataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{
    BinaryVectorBuilder, MutableVector, UInt16VectorBuilder, UInt64VectorBuilder,
    UInt8VectorBuilder,
};
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;

use crate::error::{PrimaryKeyLengthMismatchSnafu, Result};
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::MergeTreeConfig;
use crate::memtable::KeyValues;
use crate::row_converter::{McmpRowCodec, RowCodec};

/// Initial capacity for builders.
const INITIAL_BUILDER_CAPACITY: usize = 8;

/// Mutable part that buffers input data.
pub(crate) struct MutablePart {
    active_dict: KeyDict,
    immutable_dicts: Vec<Arc<KeyDict>>,
    interned_blocks: HashMap<DictIdType, InternedBlock>,
    /// Stores rows without interning primary keys. If the region doesn't have primary key,
    /// it also stores rows in this block.
    plain_block: Option<PlainBlock>,
    /// Next id of the dictionary.
    next_dict_id: DictIdType,
    /// Number of keys in a dictionary.
    dict_key_num: KeyIdType,
    /// Maximum number of dictionaries in the part.
    max_dict_num: usize,
}

impl MutablePart {
    /// Creates a new mutable part.
    pub(crate) fn new(config: &MergeTreeConfig) -> MutablePart {
        let dict_key_num = config
            .enable_dict
            .then(|| {
                config
                    .dict_key_num
                    .try_into()
                    .inspect_err(|_| warn!("Sanitize dict key num to {}", KeyIdType::MAX))
                    .unwrap_or(KeyIdType::MAX)
            })
            .unwrap_or(0);

        MutablePart {
            active_dict: KeyDict::new(0, dict_key_num),
            immutable_dicts: Vec::new(),
            interned_blocks: HashMap::new(),
            plain_block: None,
            // 0 is allocated by the first active dict.
            next_dict_id: 1,
            dict_key_num,
            max_dict_num: config.max_dict_num,
        }
    }

    /// Write key-values into the part.
    pub(crate) fn write(
        &mut self,
        metadata: &RegionMetadataRef,
        row_codec: &McmpRowCodec,
        kvs: &KeyValues,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        let mut primary_key = Vec::new();

        for kv in kvs.iter() {
            ensure!(
                kv.num_primary_keys() == row_codec.num_fields(),
                PrimaryKeyLengthMismatchSnafu {
                    expect: row_codec.num_fields(),
                    actual: kv.num_primary_keys(),
                }
            );
            // Safety: timestamp of kv must be both present and a valid timestamp value.
            let ts = kv.timestamp().as_timestamp().unwrap().unwrap().value();
            metrics.min_ts = metrics.min_ts.min(ts);
            metrics.max_ts = metrics.max_ts.max(ts);
            metrics.value_bytes += kv.fields().map(|v| v.data_size()).sum::<usize>();

            if metadata.primary_key.is_empty() {
                // No primary key, write to the plain block.
                self.write_plain(metadata, &kv, None);
                continue;
            }

            // Encode primary key.
            primary_key.clear();
            row_codec.encode_to_vec(kv.primary_keys(), &mut primary_key)?;

            if self.is_dictionary_enabled() {
                self.write_intern(metadata, &kv, &mut primary_key, metrics);
            } else {
                self.write_plain(metadata, &kv, Some(&mut primary_key));
            }
        }

        metrics.value_bytes +=
            kvs.num_rows() * (std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>());

        Ok(())
    }

    /// Returns true if the dictionary is enabled.
    fn is_dictionary_enabled(&self) -> bool {
        self.dict_key_num == 0
    }

    /// Writes to the intern block and falls back to the plain block if it can't intern the key.
    ///
    /// The region must have a primary key.
    fn write_intern(
        &mut self,
        metadata: &RegionMetadataRef,
        key_value: &KeyValue,
        primary_key: &mut Vec<u8>,
        metrics: &mut WriteMetrics,
    ) {
        assert!(!metadata.primary_key.is_empty());

        let Some((dict_id, key_id)) = self.maybe_intern(metadata, primary_key, metrics) else {
            // Unable to intern the key, write to the plain block.
            return self.write_plain(metadata, key_value, Some(primary_key));
        };

        let block = self
            .interned_blocks
            .entry(dict_id)
            .or_insert_with(|| InternedBlock::new(dict_id, metadata, INITIAL_BUILDER_CAPACITY));
        block.key.push(Some(key_id));
        block.value.push(key_value);
    }

    /// Writes the `key_value` and the `primary_key` to the plain block.
    fn write_plain(
        &mut self,
        metadata: &RegionMetadataRef,
        key_value: &KeyValue,
        primary_key: Option<&[u8]>,
    ) {
        let block = self
            .plain_block
            .get_or_insert_with(|| PlainBlock::new(metadata, INITIAL_BUILDER_CAPACITY));

        if primary_key.is_some() {
            // None means no primary key so we only push the key when it is `Some`.
            block.key.push(primary_key);
        }

        block.value.push(key_value);
    }

    /// Tries to intern the primary key, returns the id of the dictionary and the key.
    fn maybe_intern(
        &mut self,
        metadata: &RegionMetadataRef,
        primary_key: &mut Vec<u8>,
        metrics: &mut WriteMetrics,
    ) -> Option<(DictIdType, KeyIdType)> {
        if let Some(key_id) = self.active_dict.get_key_id(&primary_key) {
            return Some((self.active_dict.id, key_id));
        }

        for dict in &self.immutable_dicts {
            if let Some(key_id) = dict.get_key_id(&primary_key) {
                return Some((dict.id, key_id));
            }
        }

        // A new key.
        if self.active_dict.is_full() {
            // Allocates a new dictionary.
            let key_dict = self.allocate_dict()?;
            let prev_dict = std::mem::replace(&mut self.active_dict, key_dict);
            self.immutable_dicts.push(Arc::new(prev_dict));
        }

        // Intern the primary key.
        let key = std::mem::take(primary_key);
        metrics.key_bytes += key.len();
        let key_id = self.active_dict.must_intern(key);
        return Some((self.active_dict.id, key_id));
    }

    /// Allocates a new dictionary if possible.
    fn allocate_dict(&mut self) -> Option<KeyDict> {
        if self.immutable_dicts.len() + 1 >= self.max_dict_num {
            return None;
        }

        let dict_id = self.next_dict_id;
        self.next_dict_id = self.next_dict_id.checked_add(1)?;

        Some(KeyDict::new(dict_id, self.dict_key_num))
    }
}

/// Metrics of writing the mutable part.
pub(crate) struct WriteMetrics {
    /// Size allocated by keys.
    pub(crate) key_bytes: usize,
    /// Size allocated by values.
    pub(crate) value_bytes: usize,
    /// Minimum timestamp.
    pub(crate) min_ts: i64,
    /// Maximum timestamp
    pub(crate) max_ts: i64,
}

impl Default for WriteMetrics {
    fn default() -> Self {
        Self {
            key_bytes: 0,
            value_bytes: 0,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
        }
    }
}

/// Id of the primary key.
type KeyIdType = u16;
/// Id of a [KeyDict].
type DictIdType = u32;

/// Sorted dictionary for primary keys.
struct KeyDict {
    /// Id of the dictionary.
    id: DictIdType,
    /// The dictionary.
    dict: BTreeMap<Bytes, KeyIdType>,
    /// Interned keys.
    keys: Vec<Bytes>,
    /// Capacity of the dictionary.
    dict_key_num: KeyIdType,
}

impl KeyDict {
    /// Creates a new dictionary.
    fn new(id: DictIdType, capacity: KeyIdType) -> Self {
        Self {
            id,
            dict: BTreeMap::new(),
            keys: Vec::new(),
            dict_key_num: capacity,
        }
    }

    /// Gets the id of the key if the dictionary contains the key.
    fn get_key_id(&self, key: &[u8]) -> Option<KeyIdType> {
        self.dict.get(key).copied()
    }

    /// Returns true if the dictionary is full.
    fn is_full(&self) -> bool {
        self.keys.len() > usize::from(self.dict_key_num)
    }

    /// Adds the key into the dictionary and returns the
    /// id of the key.
    ///
    /// # Panics
    /// Panics if the dictionary is full.
    fn must_intern(&mut self, key: Vec<u8>) -> KeyIdType {
        let key = Bytes::from(key);
        let key_id = self.keys.len().try_into().unwrap();
        let old = self.dict.insert(key.clone(), key_id);
        debug_assert!(old.is_none());
        self.keys.push(key);

        key_id
    }
}

/// A block that keys are interned in the [KeyDict].
struct InternedBlock {
    /// Id of the dictionary.
    id: DictIdType,
    /// Ids of keys.
    key: UInt16VectorBuilder,
    /// Values of rows.
    value: ValueBuilder,
}

impl InternedBlock {
    /// Returns a new block.
    fn new(id: DictIdType, metadata: &RegionMetadataRef, capacity: usize) -> InternedBlock {
        InternedBlock {
            id,
            key: UInt16VectorBuilder::with_capacity(capacity),
            value: ValueBuilder::new(metadata, capacity),
        }
    }

    /// Inserts the row without primary key.
    fn insert_no_key(&mut self, key_value: &KeyValue) {
        self.value.push(key_value);
    }

    /// Inserts the row by id.
    fn insert_by_id(&mut self, key_id: KeyIdType, key_value: &KeyValue) {
        self.key.push(Some(key_id));
        self.value.push(key_value);
    }
}

/// A block that stores primary key directly.
struct PlainBlock {
    /// Primary keys. Empty if no primary key.
    key: BinaryVectorBuilder,
    /// Values of rows.
    value: ValueBuilder,
}

impl PlainBlock {
    /// Returns a new block.
    fn new(metadata: &RegionMetadataRef, capacity: usize) -> PlainBlock {
        PlainBlock {
            key: BinaryVectorBuilder::with_capacity(capacity),
            value: ValueBuilder::new(metadata, capacity),
        }
    }
}

/// Mutable buffer for values.
struct ValueBuilder {
    timestamp: Box<dyn MutableVector>,
    sequence: UInt64VectorBuilder,
    op_type: UInt8VectorBuilder,
    fields: Vec<Box<dyn MutableVector>>,
}

impl ValueBuilder {
    /// Creates a new builder with specific capacity.
    fn new(metadata: &RegionMetadataRef, capacity: usize) -> Self {
        let timestamp = metadata
            .time_index_column()
            .column_schema
            .data_type
            .create_mutable_vector(capacity);
        let sequence = UInt64VectorBuilder::with_capacity(capacity);
        let op_type = UInt8VectorBuilder::with_capacity(capacity);
        let fields = metadata
            .field_columns()
            .map(|c| c.column_schema.data_type.create_mutable_vector(capacity))
            .collect();

        Self {
            timestamp,
            sequence,
            op_type,
            fields,
        }
    }

    /// Pushes the value of a row into the builder.
    ///
    /// # Panics
    /// Panics if fields have unexpected types.
    fn push(&mut self, key_value: &KeyValue) {
        self.timestamp.push_value_ref(key_value.timestamp());
        self.sequence.push(Some(key_value.sequence()));
        self.op_type.push(Some(key_value.op_type() as u8));
        for (idx, value) in key_value.fields().enumerate() {
            self.fields[idx].push_value_ref(value);
        }
    }
}
