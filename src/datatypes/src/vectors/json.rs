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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayBuilder, ArrayRef};
use snafu::ResultExt;

use crate::arrow_array::{BinaryArray, MutableBinaryArray};
use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::value::{JsonbValue, JsonbValueRef, Value, ValueRef};
use crate::vectors::{self, MutableVector, Validity, Vector, VectorRef};

/// Vector of json values.
#[derive(Debug, PartialEq)]
pub struct JsonVector {
    array: BinaryArray,
}

impl JsonVector {
    pub fn as_arrow(&self) -> &BinaryArray {
        &self.array
    }
}

impl Vector for JsonVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::json_datatype()
    }

    fn vector_type_name(&self) -> String {
        "JsonVector".to_string()
    }

    fn as_any(&self) -> &dyn Any {
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

    // Should change?
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
        let array = self.array.slice(offset, length);
        Arc::new(Self { array })
    }

    fn get(&self, index: usize) -> Value {
        if !self.array.is_valid(index) {
            return Value::Null;
        }

        let value = self.array.value(index);
        Value::Json(JsonbValue::new(value.to_vec()))
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        if !self.array.is_valid(index) {
            return ValueRef::Null;
        }

        let value = self.array.value(index);
        ValueRef::Json(JsonbValueRef::new(value))
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn is_const(&self) -> bool {
        false
    }

    fn only_null(&self) -> bool {
        self.null_count() == self.len()
    }

    fn try_get(&self, index: usize) -> Result<Value> {
        snafu::ensure!(
            index < self.len(),
            error::BadArrayAccessSnafu {
                index,
                size: self.len()
            }
        );
        Ok(self.get(index))
    }
}

impl From<BinaryArray> for JsonVector {
    fn from(array: BinaryArray) -> Self {
        Self { array }
    }
}
pub struct JsonIter<'a> {
    vector: &'a JsonVector,
    idx: usize,
}

impl<'a> JsonIter<'a> {
    pub fn new(vector: &'a JsonVector) -> Self {
        Self { vector, idx: 0 }
    }
}

impl<'a> Iterator for JsonIter<'a> {
    type Item = Option<JsonbValueRef<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.vector.len() {
            let idx = self.idx;
            self.idx += 1;

            if self.vector.is_null(idx) {
                Some(None)
            } else {
                Some(Some(JsonbValueRef::new(self.vector.array.value(idx))))
            }
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.vector.len(), Some(self.vector.len()))
    }
}

impl ScalarVector for JsonVector {
    type OwnedItem = JsonbValue;
    type RefItem<'a> = JsonbValueRef<'a>;
    type Iter<'a> = JsonIter<'a>;
    type Builder = JsonVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if self.array.is_valid(idx) {
            Some(JsonbValueRef::new(self.array.value(idx)))
        } else {
            None
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        JsonIter::new(self)
    }
}

pub struct JsonVectorBuilder {
    mutable_array: MutableBinaryArray,
}

impl MutableVector for JsonVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::json_datatype()
    }

    fn len(&self) -> usize {
        self.mutable_array.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn to_vector(&mut self) -> VectorRef {
        Arc::new(self.finish())
    }

    fn to_vector_cloned(&self) -> VectorRef {
        Arc::new(self.finish_cloned())
    }

    fn try_push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        match value.as_json()? {
            Some(v) => self.mutable_array.append_value(v.value()),
            None => self.mutable_array.append_null(),
        }
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        vectors::impl_extend_for_builder!(self, vector, JsonVector, offset, length)
    }

    fn push_null(&mut self) {
        self.mutable_array.append_null()
    }
}

impl ScalarVectorBuilder for JsonVectorBuilder {
    type VectorType = JsonVector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutableBinaryArray::with_capacity(capacity, 0),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        match value {
            Some(v) => self.mutable_array.append_value(v.value()),
            None => self.mutable_array.append_null(),
        }
    }

    fn finish(&mut self) -> Self::VectorType {
        JsonVector {
            array: self.mutable_array.finish(),
        }
    }

    fn finish_cloned(&self) -> Self::VectorType {
        JsonVector {
            array: self.mutable_array.finish_cloned(),
        }
    }
}

impl Serializable for JsonVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        self.iter_data()
            .map(|v| match v {
                None => Ok(serde_json::Value::Null), // if binary vector not present, map to NULL
                Some(vec) => serde_json::to_value(jsonb::to_string(vec.value())),
            })
            .collect::<serde_json::Result<_>>()
            .context(error::SerializeSnafu)
    }
}

vectors::impl_try_from_arrow_array_for_vector!(BinaryArray, JsonVector);
