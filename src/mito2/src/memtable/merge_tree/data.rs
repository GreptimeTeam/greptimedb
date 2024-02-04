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

//! The value part of key-value separated merge-tree structure.

use common_recordbatch::RecordBatch;
use datatypes::data_type::DataType;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder};
use datatypes::vectors::{
    MutableVector, UInt16VectorBuilder, UInt64VectorBuilder, UInt8VectorBuilder,
};
use store_api::metadata::RegionMetadataRef;

use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::{PkId, ShardId};
use crate::memtable::{BoxedBatchIterator, KeyValues};

/// Buffer for the value part (TSID, ts, field columns) in a shard.
pub struct DataBuffer {
    shard_id: ShardId,
    metadata: RegionMetadataRef,
    /// Data types for field columns.
    field_types: Vec<ConcreteDataType>,
    /// Builder for primary key index.
    pk_index_builder: UInt16VectorBuilder,
    ts_builder: Box<dyn MutableVector>,
    sequence_builder: UInt64VectorBuilder,
    op_type_builder: UInt8VectorBuilder,
    /// Builders for field columns.
    field_builders: Vec<Option<Box<dyn MutableVector>>>,
}

impl DataBuffer {
    pub fn with_capacity(metadata: RegionMetadataRef, init_capacity: usize) -> Self {
        let ts_builder = metadata
            .time_index_column()
            .column_schema
            .data_type
            .create_mutable_vector(init_capacity);

        let pk_id_builder = UInt16VectorBuilder::with_capacity(init_capacity);
        let sequence_builder = UInt64VectorBuilder::with_capacity(init_capacity);
        let op_type_builder = UInt8VectorBuilder::with_capacity(init_capacity);

        let field_types = metadata
            .field_columns()
            .map(|c| c.column_schema.data_type.clone())
            .collect::<Vec<_>>();
        let field_builders = (0..field_types.len()).map(|_| None).collect();

        Self {
            shard_id: 0,
            metadata,
            field_types,
            pk_index_builder: pk_id_builder,
            ts_builder,
            sequence_builder,
            op_type_builder,
            field_builders,
        }
    }

    /// Writes a row to data buffer.
    pub fn write_row(&mut self, pk_id: PkId, kv: KeyValue) {
        self.ts_builder.push_value_ref(kv.timestamp());
        self.pk_index_builder.push(Some(pk_id.pk_index));
        self.sequence_builder.push(Some(kv.sequence()));
        self.op_type_builder.push(Some(kv.op_type() as u8));

        debug_assert_eq!(self.field_builders.len(), kv.num_fields());

        for (idx, field) in kv.fields().enumerate() {
            self.field_builders[idx]
                .get_or_insert_with(|| {
                    let mut builder =
                        self.field_types[idx].create_mutable_vector(self.ts_builder.len());
                    builder.push_nulls(self.ts_builder.len() - 1);
                    builder
                })
                .push_value_ref(field);
        }
    }

    /// Freezes `DataBuffer` to bytes. Use `pk_lookup_table` to convert pk_id to encoded primary key bytes.
    pub fn freeze(self, _pk_wights: &[u16]) -> DataPart {
        todo!()
    }

    pub fn iter(&self) -> Result<impl Iterator<Item = RecordBatch>> {
        todo!();
        Ok(std::iter::empty())
    }
}

/// Format of immutable data part.
pub enum DataPart {
    Parquet(Vec<u8>),
}

impl DataPart {
    pub fn iter(&self) -> Result<BoxedBatchIterator> {
        todo!()
    }
}
