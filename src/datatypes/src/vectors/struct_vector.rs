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

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::compute::TakeOptions;
use arrow_array::{Array, ArrayRef, StructArray};
use snafu::ResultExt;

use crate::error::{ArrowComputeSnafu, Result, SerializeSnafu, UnsupportedOperationSnafu};
use crate::prelude::{ConcreteDataType, ScalarVector};
use crate::serialize::Serializable;
use crate::types::StructType;
use crate::value::{StructValue, StructValueRef, Value, ValueRef};
use crate::vectors::operations::VectorOp;
use crate::vectors::{self, Helper, Validity, Vector, VectorRef};

/// A simple wrapper around `StructArray` to represent a vector of structs in GreptimeDB.
#[derive(Debug, PartialEq)]
pub struct StructVector {
    array: StructArray,
    fields: StructType,
}

impl StructVector {
    pub fn new(array: StructArray) -> Result<Self> {
        let fields = array.fields();
        let struct_type = fields.try_into()?;
        Ok(StructVector {
            array,
            fields: struct_type,
        })
    }

    pub fn array(&self) -> &StructArray {
        &self.array
    }

    pub fn as_arrow(&self) -> &dyn Array {
        &self.array
    }

    pub fn struct_type(&self) -> StructType {
        self.fields.clone()
    }
}

impl Vector for StructVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::struct_datatype(self.fields.clone())
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
            fields: self.fields.clone(),
        })
    }

    fn get(&self, index: usize) -> Value {
        if !self.array.is_valid(index) {
            return Value::Null;
        }

        let array = &self.array.value(index);
        let vector = Helper::try_into_vector(array).unwrap_or_else(|_| {
            panic!(
                "arrow array with datatype {:?} cannot converted to our vector",
                array.data_type()
            )
        });
        let values = (0..vector.len())
            .map(|i| vector.get(i))
            .collect::<Vec<Value>>();
        Value::Struct(StructValue::new(values, self.fields.clone()))
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        ValueRef::Struct(StructValueRef::Indexed {
            vector: self,
            idx: index,
        })
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

    fn cast(&self, _to_type: &ConcreteDataType) -> Result<VectorRef> {
        UnsupportedOperationSnafu {
            op: "cast",
            vector_type: self.vector_type_name(),
        }
        .fail()
    }

    fn filter(&self, filter: &vectors::BooleanVector) -> Result<VectorRef> {
        let filtered =
            datafusion_common::arrow::compute::filter(&self.array, filter.as_boolean_array())
                .context(ArrowComputeSnafu)
                .and_then(Helper::try_into_vector)?;
        Ok(filtered)
    }

    fn take(&self, indices: &vectors::UInt32Vector) -> Result<VectorRef> {
        let take_result = datafusion_common::arrow::compute::take(
            &self.array,
            indices.as_arrow(),
            Some(TakeOptions { check_bounds: true }),
        )
        .context(ArrowComputeSnafu)
        .and_then(Helper::try_into_vector)?;
        Ok(take_result)
    }
}

impl Serializable for StructVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        let mut vectors = BTreeMap::new();
        for (field, value) in self.array.fields().iter().zip(self.array.columns().iter()) {
            let value_vector = Helper::try_into_vector(value)?;
            vectors.insert(field.name().clone(), value_vector);
        }

        let mut results = Vec::with_capacity(self.array().len());
        for idx in 0..self.array().len() {
            let mut result = serde_json::Map::new();
            for field in vectors.keys() {
                let field_value = vectors.get(field).unwrap().get(idx);
                result.insert(
                    field.to_string(),
                    field_value.try_into().context(SerializeSnafu)?,
                );
            }

            results.push(result.into());
        }

        Ok(results)
    }
}

impl ScalarVector for StructVector {
    type OwnedItem = StructValue;
    type RefItem<'a> = StructValueRef<'a>;
    type Iter<'a> = StructIter<'a>;
    type Builder = StructVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if self.array.is_valid(idx) {
            Some(StructValueRef::Indexed { vector: self, idx })
        } else {
            None
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        StructIter::new(self)
    }
}
