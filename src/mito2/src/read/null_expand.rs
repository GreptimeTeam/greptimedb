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

//! A reader transformer to expand possible [NullVector].

use std::collections::HashMap;

use async_trait::async_trait;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::vectors::VectorRef;
use store_api::metadata::RegionMetadataRef;

use crate::error::Result;
use crate::read::{Batch, BatchReader, BoxedBatchReader};

pub struct NullExpandReader {
    input: BoxedBatchReader,
    null_buf: HashMap<ConcreteDataType, VectorRef>,
    metadata: RegionMetadataRef,
}

impl NullExpandReader {
    pub fn new(input: BoxedBatchReader, metadata: RegionMetadataRef) -> Self {
        Self {
            input,
            null_buf: HashMap::new(),
            metadata,
        }
    }

    fn make_null_vec(data_type: &ConcreteDataType, length: usize) -> VectorRef {
        let mut builder = data_type.create_mutable_vector(length);
        builder.push_nulls(length);
        builder.to_vector()
    }
}

#[async_trait]
impl BatchReader for NullExpandReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let Some(mut batch) = self.input.next_batch().await? else {
            return Ok(None);
        };

        batch.fields.iter_mut().for_each(|field| {
            if matches!(field.data.data_type(), ConcreteDataType::Null(..)) {
                let length = field.data.len();
                let data_type = self
                    .metadata
                    .column_by_id(field.column_id)
                    .unwrap()
                    .column_schema
                    .data_type
                    .clone();
                if let Some(prev_null_vec) = self.null_buf.get_mut(&data_type) {
                    // field.data = null_vec.clone();
                    if prev_null_vec.len() >= length {
                        field.data = prev_null_vec.slice(0, length);
                    } else {
                        let new_null_vec = Self::make_null_vec(&data_type, length);
                        // self.null_buf.insert(data_type, null_vec.clone());
                        *prev_null_vec = new_null_vec.clone();
                        field.data = new_null_vec;
                    }
                } else {
                    // let mut builder = data_type.create_mutable_vector(length);
                    // builder.push_nulls(length);
                    // let null_vec = builder.to_vector();
                    let null_vec = Self::make_null_vec(&data_type, length);
                    self.null_buf.insert(data_type, null_vec.clone());
                    field.data = null_vec;
                }
            }
            // if let Some(null_buf) = self.null_buf.get(&field.data_type()) {
            //     field.set_null_bitmap(null_buf.clone());
            // }
            // if
        });

        Ok(Some(batch))
    }
}
