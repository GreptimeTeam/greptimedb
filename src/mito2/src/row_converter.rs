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

mod dense;
mod sparse;
use std::fmt::Debug;
use std::sync::Arc;

use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::value::{Value, ValueRef};
pub use dense::{DensePrimaryKeyCodec, SortField};
pub use sparse::{SparsePrimaryKeyCodec, SparseValues};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::memtable::key_values::KeyValue;

/// Row value encoder/decoder.
pub trait PrimaryKeyCodecExt {
    /// Encodes rows to bytes.
    /// # Note
    /// Ensure the length of row iterator matches the length of fields.
    fn encode<'a, I>(&self, row: I) -> Result<Vec<u8>>
    where
        I: Iterator<Item = ValueRef<'a>>,
    {
        let mut buffer = Vec::new();
        self.encode_to_vec(row, &mut buffer)?;
        Ok(buffer)
    }

    /// Encodes rows to specific vec.
    /// # Note
    /// Ensure the length of row iterator matches the length of fields.
    fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = ValueRef<'a>>;
}

pub trait PrimaryKeyFilter: Send + Sync {
    /// Returns true if the primary key matches the filter.
    fn matches(&mut self, pk: &[u8]) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeValues {
    Dense(Vec<(ColumnId, Value)>),
    Sparse(SparseValues),
}

impl CompositeValues {
    /// Extends the composite values with the given values.
    pub fn extend(&mut self, values: &[(ColumnId, Value)]) {
        match self {
            CompositeValues::Dense(dense_values) => {
                for (column_id, value) in values {
                    dense_values.push((*column_id, value.clone()));
                }
            }
            CompositeValues::Sparse(sprase_value) => {
                for (column_id, value) in values {
                    sprase_value.insert(*column_id, value.clone());
                }
            }
        }
    }
}

#[cfg(test)]
impl CompositeValues {
    pub fn into_sparse(self) -> SparseValues {
        match self {
            CompositeValues::Sparse(v) => v,
            _ => panic!("CompositeValues is not sparse"),
        }
    }

    pub fn into_dense(self) -> Vec<Value> {
        match self {
            CompositeValues::Dense(v) => v.into_iter().map(|(_, v)| v).collect(),
            _ => panic!("CompositeValues is not dense"),
        }
    }
}

pub trait PrimaryKeyCodec: Send + Sync + Debug {
    /// Encodes a key value to bytes.
    fn encode_key_value(&self, key_value: &KeyValue, buffer: &mut Vec<u8>) -> Result<()>;

    /// Encodes values to bytes.
    fn encode_values(&self, values: &[(ColumnId, Value)], buffer: &mut Vec<u8>) -> Result<()>;

    /// Returns the number of fields in the primary key.
    fn num_fields(&self) -> usize;

    /// Returns a primary key filter factory.
    fn primary_key_filter(
        &self,
        metadata: &RegionMetadataRef,
        filters: Arc<Vec<SimpleFilterEvaluator>>,
    ) -> Box<dyn PrimaryKeyFilter>;

    /// Returns the estimated size of the primary key.
    fn estimated_size(&self) -> Option<usize> {
        None
    }

    /// Returns the encoding type of the primary key.
    fn encoding(&self) -> PrimaryKeyEncoding;

    /// Decodes the primary key from the given bytes.
    ///
    /// Returns a [`CompositeValues`] that follows the primary key ordering.
    fn decode(&self, bytes: &[u8]) -> Result<CompositeValues>;

    /// Decode the leftmost value from bytes.
    fn decode_leftmost(&self, bytes: &[u8]) -> Result<Option<Value>>;
}

/// Builds a primary key codec from region metadata.
pub fn build_primary_key_codec(region_metadata: &RegionMetadataRef) -> Arc<dyn PrimaryKeyCodec> {
    let fields = region_metadata.primary_key_columns().map(|col| {
        (
            col.column_id,
            SortField::new(col.column_schema.data_type.clone()),
        )
    });
    build_primary_key_codec_with_fields(region_metadata.primary_key_encoding, fields)
}

/// Builds a primary key codec from region metadata.
pub fn build_primary_key_codec_with_fields(
    encoding: PrimaryKeyEncoding,
    fields: impl Iterator<Item = (ColumnId, SortField)>,
) -> Arc<dyn PrimaryKeyCodec> {
    match encoding {
        PrimaryKeyEncoding::Dense => Arc::new(DensePrimaryKeyCodec::with_fields(fields.collect())),
        PrimaryKeyEncoding::Sparse => {
            Arc::new(SparsePrimaryKeyCodec::with_fields(fields.collect()))
        }
    }
}
