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

use arrow::array::Array;
use arrow::datatypes::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, Int32Array};
use serde_json::Value as JsonValue;
use snafu::ResultExt;

use super::operations::VectorOp;
use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::serialize::Serializable;
use crate::types::DictionaryType;
use crate::value::{Value, ValueRef};
use crate::vectors::{self, Helper, Validity, Vector, VectorRef};

/// Vector of dictionaries, basically backed by Arrow's `DictionaryArray`.
#[derive(Debug, PartialEq)]
pub struct DictionaryVector {
    array: DictionaryArray<Int32Type>,
    /// The datatype of the items in the dictionary.
    item_type: ConcreteDataType,
    /// The vector of items in the dictionary.
    item_vector: VectorRef,
}

impl DictionaryVector {
    /// Create a new instance of `DictionaryVector` from a dictionary array and item type
    pub fn new(array: DictionaryArray<Int32Type>, item_type: ConcreteDataType) -> Self {
        let item_vector = Helper::try_into_vector(array.values()).unwrap_or_else(|_| {
            panic!(
                "arrow array with datatype {:?} cannot be converted to our vector",
                array.values().data_type()
            )
        });
        Self {
            array,
            item_type,
            item_vector,
        }
    }

    /// Returns the underlying Arrow dictionary array
    pub fn array(&self) -> &DictionaryArray<Int32Type> {
        &self.array
    }

    /// Returns the keys array of this dictionary
    pub fn keys(&self) -> &arrow_array::PrimitiveArray<Int32Type> {
        self.array.keys()
    }

    /// Returns the values array of this dictionary
    pub fn values(&self) -> &ArrayRef {
        self.array.values()
    }

    pub fn as_arrow(&self) -> &dyn Array {
        &self.array
    }
}

impl Vector for DictionaryVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::Dictionary(DictionaryType::new(
            ConcreteDataType::int32_datatype(),
            self.item_type.clone(),
        ))
    }

    fn vector_type_name(&self) -> String {
        "DictionaryVector".to_string()
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
        Arc::new(Self {
            array: self.array.slice(offset, length),
            item_type: self.item_type.clone(),
            item_vector: self.item_vector.clone(),
        })
    }

    fn get(&self, index: usize) -> Value {
        if !self.array.is_valid(index) {
            return Value::Null;
        }

        let key = self.array.keys().value(index);
        self.item_vector.get(key as usize)
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        if !self.array.is_valid(index) {
            return ValueRef::Null;
        }

        let key = self.array.keys().value(index);
        self.item_vector.get_ref(key as usize)
    }
}

impl Serializable for DictionaryVector {
    fn serialize_to_json(&self) -> Result<Vec<JsonValue>> {
        // Convert the dictionary array to JSON, where each element is either null or
        // the value it refers to in the dictionary
        let mut result = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if self.is_null(i) {
                result.push(JsonValue::Null);
            } else {
                let key = self.array.keys().value(i);
                let value = self.item_vector.get(key as usize);
                let json_value = serde_json::to_value(value).context(error::SerializeSnafu)?;
                result.push(json_value);
            }
        }

        Ok(result)
    }
}

impl From<DictionaryArray<Int32Type>> for DictionaryVector {
    fn from(array: DictionaryArray<Int32Type>) -> Self {
        let item_type = ConcreteDataType::from_arrow_type(array.values().data_type());
        let item_vector = Helper::try_into_vector(array.values()).unwrap_or_else(|_| {
            panic!(
                "arrow array with datatype {:?} cannot be converted to our vector",
                array.values().data_type()
            )
        });
        Self {
            array,
            item_type,
            item_vector,
        }
    }
}

pub struct DictionaryIter<'a> {
    vector: &'a DictionaryVector,
    idx: usize,
}

impl<'a> DictionaryIter<'a> {
    pub fn new(vector: &'a DictionaryVector) -> DictionaryIter<'a> {
        DictionaryIter { vector, idx: 0 }
    }
}

impl<'a> Iterator for DictionaryIter<'a> {
    type Item = Option<ValueRef<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.vector.len() {
            return None;
        }

        let idx = self.idx;
        self.idx += 1;

        if self.vector.is_null(idx) {
            return Some(None);
        }

        Some(Some(self.vector.item_vector.get_ref(self.idx)))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.vector.len() - self.idx,
            Some(self.vector.len() - self.idx),
        )
    }
}

impl VectorOp for DictionaryVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        let keys = self.array.keys();
        let mut replicated_keys = Vec::with_capacity(offsets.len());

        for &offset in offsets {
            if offset < self.len() {
                if keys.is_valid(offset) {
                    replicated_keys.push(Some(keys.value(offset)));
                } else {
                    replicated_keys.push(None);
                }
            } else {
                replicated_keys.push(None);
            }
        }

        let new_keys = Int32Array::from(replicated_keys);
        let new_array = DictionaryArray::try_new(new_keys, self.values().clone())
            .expect("Failed to create replicated dictionary array");

        Arc::new(Self {
            array: new_array,
            item_type: self.item_type.clone(),
            item_vector: self.item_vector.clone(),
        })
    }

    fn find_unique(&self, selected: &mut common_base::BitVec, prev_vector: Option<&dyn Vector>) {
        if let Some(prev) = prev_vector {
            if let Some(prev_dict) = prev.as_any().downcast_ref::<DictionaryVector>() {
                // If previous vector is also a dictionary, we can compare dictionary keys
                for i in 0..self.len() {
                    if i < prev_dict.len()
                        && !self.is_null(i)
                        && !prev_dict.is_null(i)
                        && self.array.keys().value(i) == prev_dict.array.keys().value(i)
                    {
                        continue;
                    }
                    selected.set(i, true);
                }
            } else {
                // If previous vector is of different type, mark all as unique
                for i in 0..self.len() {
                    selected.set(i, true);
                }
            }
        } else {
            // No previous vector, mark all as unique
            for i in 0..self.len() {
                selected.set(i, true);
            }
        }
    }

    fn filter(&self, filter: &vectors::BooleanVector) -> Result<VectorRef> {
        let key_array: ArrayRef = Arc::new(self.array.keys().clone());
        let key_vector = Helper::try_into_vector(&key_array).unwrap_or_else(|_| {
            panic!(
                "arrow array with datatype {:?} cannot be converted to our vector",
                key_array.data_type()
            )
        });
        let filtered_key_vector = key_vector.filter(filter)?;
        let filtered_key_array = filtered_key_vector.to_arrow_array();
        let filtered_key_array = filtered_key_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let new_array = DictionaryArray::try_new(filtered_key_array.clone(), self.values().clone())
            .expect("Failed to create filtered dictionary array");

        Ok(Arc::new(Self {
            array: new_array,
            item_type: self.item_type.clone(),
            item_vector: self.item_vector.clone(),
        }))
    }

    fn cast(&self, to_type: &ConcreteDataType) -> Result<VectorRef> {
        let new_items = self.item_vector.cast(to_type)?;
        let new_array =
            DictionaryArray::try_new(self.array.keys().clone(), new_items.to_arrow_array())
                .expect("Failed to create casted dictionary array");
        Ok(Arc::new(Self {
            array: new_array,
            item_type: to_type.clone(),
            item_vector: self.item_vector.clone(),
        }))
    }

    fn take(&self, indices: &vectors::UInt32Vector) -> Result<VectorRef> {
        let key_array: ArrayRef = Arc::new(self.array.keys().clone());
        let key_vector = Helper::try_into_vector(&key_array).unwrap_or_else(|_| {
            panic!(
                "arrow array with datatype {:?} cannot be converted to our vector",
                key_array.data_type()
            )
        });
        let new_key_vector = key_vector.take(indices)?;
        let new_key_array = new_key_vector.to_arrow_array();
        let new_key_array = new_key_array.as_any().downcast_ref::<Int32Array>().unwrap();

        let new_array = DictionaryArray::try_new(new_key_array.clone(), self.values().clone())
            .expect("Failed to create filtered dictionary array");

        Ok(Arc::new(Self {
            array: new_array,
            item_type: self.item_type.clone(),
            item_vector: self.item_vector.clone(),
        }))
    }
}
