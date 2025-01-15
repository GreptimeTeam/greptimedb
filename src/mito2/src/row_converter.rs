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
// TODO(weny): remove it.
#[allow(unused)]
mod sparse;

use std::sync::Arc;

use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::value::{Value, ValueRef};
pub use dense::{DensePrimaryKeyCodec, SortField};
pub use sparse::{SparsePrimaryKeyCodec, SparseValues};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;

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

    /// Decode row values from bytes.
    fn decode(&self, bytes: &[u8]) -> Result<Vec<Value>>;
}

pub trait PrimaryKeyFilter: Send + Sync {
    /// Returns true if the primary key matches the filter.
    fn matches(&mut self, pk: &[u8]) -> bool;
}

pub trait PrimaryKeyCodec: Send + Sync {
    /// Encodes a key value to bytes.
    fn encode_key_value(&self, key_value: &KeyValue, buffer: &mut Vec<u8>) -> Result<()>;

    /// Encodes values to bytes.
    fn encode_values(&self, values: &[Value], buffer: &mut Vec<u8>) -> Result<()>;

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
    /// Returns a [`Vec<Value>`] that follows the primary key ordering.
    fn decode_dense(&self, bytes: &[u8]) -> Result<Vec<Value>>;

    /// Decode the leftmost value from bytes.
    fn decode_leftmost(&self, bytes: &[u8]) -> Result<Option<Value>>;
}
