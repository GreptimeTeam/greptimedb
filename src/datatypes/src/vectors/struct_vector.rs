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

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StructArray};
use serde_json::Value as JsonValue;
use snafu::ResultExt;

use crate::error::{self, Result, UnsupportedOperationSnafu};
use crate::prelude::ConcreteDataType;
use crate::serialize::Serializable;
use crate::types::{StructField, StructFields};
use crate::value::{Value, ValueRef};
use crate::vectors::operations::VectorOp;
use crate::vectors::{self, Helper, Validity, Vector, VectorRef};

/// A simple wrapper around `StructArray` to represent a vector of structs in GreptimeDB.
#[derive(Debug, PartialEq)]
pub struct StructVector {
    array: StructArray,
    data_type: StructFields,
}

#[allow(unused)]
impl StructVector {
    pub fn new(array: StructArray) -> Result<Self> {
        let fields = array.fields();
        let data_type = StructFields::new(
            fields
                .iter()
                .map(|field| {
                    Ok(StructField::new(
                        field.name().to_string(),
                        ConcreteDataType::try_from(field.data_type())?,
                        field.is_nullable(),
                    ))
                })
                .collect::<Result<Vec<StructField>>>()?,
        );
        Ok(StructVector { array, data_type })
    }

    pub fn array(&self) -> &StructArray {
        &self.array
    }

    pub fn as_arrow(&self) -> &dyn Array {
        &self.array
    }
}

impl Vector for StructVector {
    fn data_type(&self) -> ConcreteDataType {
        todo!()
    }

    fn vector_type_name(&self) -> String {
        "StructVector".to_string()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    fn to_arrow_array(&self) -> ArrayRef {
        Arc::new(self.array.clone())
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn Array> {
        Box::new(self.array.clone())
    }
    fn validity(&self) -> Validity {
        vectors::impl_validity_for_vector!(self.array)
    }

    fn memory_size(&self) -> usize {
        self.array.get_buffer_memory_size()
    }

    fn null_count(&self) -> usize {
        self.array.null_count()
    }

    fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }

    fn slice(&self, offset: usize, length: usize) -> VectorRef {
        Arc::new(StructVector {
            array: self.array.slice(offset, length),
            data_type: self.data_type.clone(),
        })
    }

    fn get(&self, _: usize) -> Value {
        todo!("Support StructValue")
    }

    fn get_ref(&self, _: usize) -> ValueRef {
        todo!("Support StructValue")
    }
}

impl VectorOp for StructVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        let column_arrays = self
            .array
            .columns()
            .iter()
            .map(|col| {
                let vector = Helper::try_into_vector(col)
                    .expect("Failed to replicate struct vector columns");
                vector.replicate(offsets).to_arrow_array()
            })
            .collect::<Vec<_>>();
        let replicated_array = StructArray::new(
            self.array.fields().clone(),
            column_arrays,
            self.array.nulls().cloned(),
        );
        Arc::new(
            StructVector::new(replicated_array).expect("Failed to create replicated StructVector"),
        )
    }

    fn cast(&self, to_type: &ConcreteDataType) -> Result<VectorRef> {
        UnsupportedOperationSnafu {
            op: "cast",
            vector_type: self.vector_type_name(),
        }
        .fail()
    }

    fn filter(&self, filter: &vectors::BooleanVector) -> Result<VectorRef> {
        let column_arrays = self
            .array
            .columns()
            .iter()
            .map(|col| {
                Helper::try_into_vector(col)
                    .and_then(|v| v.filter(filter))
                    .map(|v| v.to_arrow_array())
            })
            .collect::<Result<Vec<_>>>()?;
        let replicated_array = StructArray::new(
            self.array.fields().clone(),
            column_arrays,
            self.array.nulls().cloned(),
        );
        Ok(Arc::new(StructVector::new(replicated_array)?))
    }

    fn take(&self, indices: &vectors::UInt32Vector) -> Result<VectorRef> {
        let column_arrays = self
            .array
            .columns()
            .iter()
            .map(|col| {
                Helper::try_into_vector(col)
                    .and_then(|v| v.take(indices))
                    .map(|v| v.to_arrow_array())
            })
            .collect::<Result<Vec<_>>>()?;
        let replicated_array = StructArray::new(
            self.array.fields().clone(),
            column_arrays,
            self.array.nulls().cloned(),
        );
        Ok(Arc::new(StructVector::new(replicated_array)?))
    }
}

impl Serializable for StructVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        let mut result = serde_json::Map::new();
        for (field, value) in self.array.fields().iter().zip(self.array.columns().iter()) {
            let value_vector = Helper::try_into_vector(value)?;
            let field_value = (0..value_vector.len())
                .map(|i| value_vector.get(i))
                .map(|v| serde_json::to_value(v).context(error::SerializeSnafu))
                .collect::<Result<Vec<_>>>()?;
            result.insert(field.name().clone(), JsonValue::Array(field_value));
        }
        let fields = JsonValue::Object(result);
        let data_type = serde_json::to_value(&self.data_type).context(error::SerializeSnafu)?;
        Ok(vec![JsonValue::Object(
            [
                ("fields".to_string(), fields),
                ("data_type".to_string(), data_type),
            ]
            .iter()
            .cloned()
            .collect(),
        )])
    }
}
