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

use std::collections::BTreeMap;

use api::v1::OpType;
use bytes::Bytes;
use common_time::Timestamp;
use datatypes::data_type::DataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::value::ValueRef;
use datatypes::vectors::{
    MutableVector, UInt16VectorBuilder, UInt64VectorBuilder, UInt8VectorBuilder,
};
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::SequenceNumber;

use crate::error::{PrimaryKeyLengthMismatchSnafu, Result};
use crate::memtable::key_values::KeyValue;
use crate::memtable::KeyValues;
use crate::row_converter::{McmpRowCodec, RowCodec};

/// Initial values builder capacity.
const INITIAL_BUILDER_CAPACITY: usize = 8;

// TODO(yingwen): Block without dictionary.

/// Mutable part that buffers input data.
pub(crate) struct MutablePart {
    blocks: Vec<DictBlock>,
    dict_size: KeyIdType,
    metrics: Metrics,
}

impl MutablePart {
    /// Write key-values into the part.
    pub(crate) fn write(
        &mut self,
        metadata: &RegionMetadataRef,
        row_codec: &McmpRowCodec,
        kvs: &KeyValues,
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
            // Encode primary key.
            primary_key.clear();
            row_codec.encode_to_vec(kv.primary_keys(), &mut primary_key)?;

            if self.blocks.is_empty() {
                self.blocks.push(DictBlock::new(metadata, self.dict_size));
            }

            // Update metrics first.
            // Safety: timestamp of kv must be both present and a valid timestamp value.
            let ts = kv.timestamp().as_timestamp().unwrap().unwrap().value();
            self.metrics.min_ts = self.metrics.min_ts.min(ts);
            self.metrics.max_ts = self.metrics.max_ts.max(ts);
            self.metrics.value_bytes += kv.fields().map(|v| v.data_size()).sum::<usize>();

            if self.find_blocks_to_insert(&primary_key, &kv) {
                continue;
            }

            // A new key.
            // TODO(yingwen): Also consider size of the block.
            if self.blocks.last().unwrap().key_dict.is_full() {
                self.blocks.push(DictBlock::new(metadata, self.dict_size));
            }
            let last = self.blocks.last_mut().unwrap();

            self.metrics.key_bytes += primary_key.len();
            // Safety: The block is not full.
            last.insert_by_key(std::mem::take(&mut primary_key), &kv);
        }

        self.metrics.value_bytes +=
            kvs.num_rows() * (std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>());

        Ok(())
    }

    /// Finds a block that contains the key and insert the value, returns true the value
    /// is inserted.
    #[must_use]
    fn find_blocks_to_insert(&mut self, primary_key: &[u8], key_value: &KeyValue) -> bool {
        for block in &mut self.blocks {
            let Some(key_id) = block.key_dict.get_key_id(&primary_key) else {
                continue;
            };

            // The key belongs to this block
            block.insert_by_id(key_id, &key_value);
            return true;
        }

        false
    }
}

/// Metrics of the mutable part.
struct Metrics {
    /// Size allocated by keys.
    key_bytes: usize,
    /// Size allocated by values.
    value_bytes: usize,
    /// Minimum timestamp.
    min_ts: i64,
    /// Maximum timestamp
    max_ts: i64,
}

/// A block contains a key dict and values.
struct DictBlock {
    key_dict: KeyDict,
    values: ValuesBuilder,
}

impl DictBlock {
    /// Creates a new block.
    fn new(metadata: &RegionMetadataRef, dict_size: KeyIdType) -> DictBlock {
        DictBlock {
            key_dict: KeyDict::new(dict_size),
            values: ValuesBuilder::new(metadata, INITIAL_BUILDER_CAPACITY),
        }
    }

    /// Inserts the row by id.
    fn insert_by_id(&mut self, key_id: KeyIdType, key_value: &KeyValue) {
        self.values.push(
            key_id,
            key_value.timestamp(),
            key_value.sequence(),
            key_value.op_type(),
            key_value.fields(),
        );
    }

    /// Inserts the row by key.
    fn insert_by_key(&mut self, key: Vec<u8>, key_value: &KeyValue) {
        let key_id = self.key_dict.must_intern(key);
        self.insert_by_id(key_id, key_value);
    }
}

/// Id of the primary key.
type KeyIdType = u16;

/// Sorted dictionary for primary keys.
struct KeyDict {
    /// The dictionary.
    dict: BTreeMap<Bytes, KeyIdType>,
    /// Interned keys.
    keys: Vec<Bytes>,
    /// Capacity of the dictionary.
    dict_size: KeyIdType,
}

impl KeyDict {
    /// Creates a new dictionary.
    fn new(capacity: KeyIdType) -> Self {
        Self {
            dict: BTreeMap::new(),
            // We initialize the keys buffer with a small capacity to avoid
            // allocating too much memory.
            keys: Vec::with_capacity(INITIAL_BUILDER_CAPACITY),
            dict_size: capacity,
        }
    }

    /// Gets the id of the key if the dictionary contains the key.
    fn get_key_id(&self, key: &[u8]) -> Option<KeyIdType> {
        self.dict.get(key).copied()
    }

    /// Returns true if the dictionary is full.
    fn is_full(&self) -> bool {
        self.keys.len() > usize::from(self.dict_size)
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

/// Mutable buffer for values.
struct ValuesBuilder {
    key: UInt16VectorBuilder,
    timestamp: Box<dyn MutableVector>,
    sequence: UInt64VectorBuilder,
    op_type: UInt8VectorBuilder,
    fields: Vec<Box<dyn MutableVector>>,
}

impl ValuesBuilder {
    /// Creates a new builder with specific capacity.
    fn new(metadata: &RegionMetadataRef, capacity: usize) -> Self {
        let key = UInt16VectorBuilder::with_capacity(capacity);
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
            key,
            timestamp,
            sequence,
            op_type,
            fields,
        }
    }

    /// Pushes a row into the builder.
    ///
    /// # Panics
    /// Panics if fields have unexpected types.
    fn push<'a>(
        &mut self,
        key_id: KeyIdType,
        ts: ValueRef,
        sequence: SequenceNumber,
        op_type: OpType,
        fields: impl Iterator<Item = ValueRef<'a>>,
    ) {
        self.key.push(Some(key_id));
        self.timestamp.push_value_ref(ts);
        self.sequence.push(Some(sequence));
        self.op_type.push(Some(op_type as u8));
        for (idx, value) in fields.enumerate() {
            self.fields[idx].push_value_ref(value);
        }
    }
}
