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
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, PrimitiveArray, PrimitiveBuilder};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType};
use serde_json::Value as JsonValue;
use snafu::ResultExt;

use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::serialize::Serializable;
use crate::types::DictionaryType;
use crate::value::{Value, ValueRef};
use crate::vectors::operations::VectorOp;
use crate::vectors::{self, Helper, Validity, Vector, VectorRef};

/// Vector of dictionaries, basically backed by Arrow's `DictionaryArray`.
pub struct DictionaryVector<K: ArrowDictionaryKeyType> {
    array: DictionaryArray<K>,
    /// The datatype of the keys in the dictionary.
    key_type: ConcreteDataType,
    /// The datatype of the items in the dictionary.
    item_type: ConcreteDataType,
    /// The vector of items in the dictionary.
    item_vector: VectorRef,
}

impl<K: ArrowDictionaryKeyType> fmt::Debug for DictionaryVector<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DictionaryVector")
            .field("array", &self.array)
            .field("key_type", &self.key_type)
            .field("item_type", &self.item_type)
            .finish()
    }
}

impl<K: ArrowDictionaryKeyType> PartialEq for DictionaryVector<K> {
    fn eq(&self, other: &DictionaryVector<K>) -> bool {
        self.array == other.array
            && self.key_type == other.key_type
            && self.item_type == other.item_type
    }
}

impl<K: ArrowDictionaryKeyType> DictionaryVector<K> {
    /// Create a new instance of `DictionaryVector` from a dictionary array and item type
    pub fn new(array: DictionaryArray<K>, item_type: ConcreteDataType) -> Result<Self> {
        let key_type = ConcreteDataType::try_from(&K::DATA_TYPE)?;
        let item_vector = Helper::try_into_vector(array.values())?;

        Ok(Self {
            array,
            key_type,
            item_type,
            item_vector,
        })
    }

    /// Returns the underlying Arrow dictionary array
    pub fn array(&self) -> &DictionaryArray<K> {
        &self.array
    }

    /// Returns the keys array of this dictionary
    pub fn keys(&self) -> &arrow_array::PrimitiveArray<K> {
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

impl<K: ArrowDictionaryKeyType> Vector for DictionaryVector<K> {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::Dictionary(DictionaryType::new(
            self.key_type.clone(),
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
            key_type: self.key_type.clone(),
            item_type: self.item_type.clone(),
            item_vector: self.item_vector.clone(),
        })
    }

    fn get(&self, index: usize) -> Value {
        if !self.array.is_valid(index) {
            return Value::Null;
        }

        let key = self.array.keys().value(index);
        self.item_vector.get(key.as_usize())
    }

    fn get_ref(&self, index: usize) -> ValueRef<'_> {
        if !self.array.is_valid(index) {
            return ValueRef::Null;
        }

        let key = self.array.keys().value(index);
        self.item_vector.get_ref(key.as_usize())
    }
}

impl<K: ArrowDictionaryKeyType> Serializable for DictionaryVector<K> {
    fn serialize_to_json(&self) -> Result<Vec<JsonValue>> {
        // Convert the dictionary array to JSON, where each element is either null or
        // the value it refers to in the dictionary
        let mut result = Vec::with_capacity(self.len());

        let keys = self.array.keys();
        let key_values = &keys.values()[..self.len()];
        for (i, &key) in key_values.iter().enumerate() {
            if self.is_null(i) {
                result.push(JsonValue::Null);
            } else {
                let value = self.item_vector.get(key.as_usize());
                let json_value = serde_json::to_value(value).context(error::SerializeSnafu)?;
                result.push(json_value);
            }
        }

        Ok(result)
    }
}

impl<K: ArrowDictionaryKeyType> TryFrom<DictionaryArray<K>> for DictionaryVector<K> {
    type Error = crate::error::Error;

    fn try_from(array: DictionaryArray<K>) -> Result<Self> {
        let key_type = ConcreteDataType::try_from(array.keys().data_type())?;
        let item_type = ConcreteDataType::try_from(array.values().data_type())?;
        let item_vector = Helper::try_into_vector(array.values())?;

        Ok(Self {
            array,
            key_type,
            item_type,
            item_vector,
        })
    }
}

pub struct DictionaryIter<'a, K: ArrowDictionaryKeyType> {
    vector: &'a DictionaryVector<K>,
    idx: usize,
}

impl<'a, K: ArrowDictionaryKeyType> DictionaryIter<'a, K> {
    pub fn new(vector: &'a DictionaryVector<K>) -> DictionaryIter<'a, K> {
        DictionaryIter { vector, idx: 0 }
    }
}

impl<'a, K: ArrowDictionaryKeyType> Iterator for DictionaryIter<'a, K> {
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

        Some(Some(self.vector.get_ref(idx)))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.vector.len() - self.idx,
            Some(self.vector.len() - self.idx),
        )
    }
}

impl<K: ArrowDictionaryKeyType> VectorOp for DictionaryVector<K> {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        let keys = self.array.keys();
        let mut replicated_keys = PrimitiveBuilder::new();

        let mut previous_offset = 0;
        let mut key_iter = keys.iter().chain(std::iter::repeat(None));
        for &offset in offsets {
            let key = key_iter.next().unwrap();

            // repeat this key (offset - previous_offset) times
            let repeat_count = offset - previous_offset;
            for _ in 0..repeat_count {
                replicated_keys.append_option(key);
            }

            previous_offset = offset;
        }

        let new_keys = replicated_keys.finish();
        let new_array = DictionaryArray::try_new(new_keys, self.values().clone())
            .expect("Failed to create replicated dictionary array");

        Arc::new(Self {
            array: new_array,
            key_type: self.key_type.clone(),
            item_type: self.item_type.clone(),
            item_vector: self.item_vector.clone(),
        })
    }

    fn filter(&self, filter: &vectors::BooleanVector) -> Result<VectorRef> {
        let key_array: ArrayRef = Arc::new(self.array.keys().clone());
        let key_vector = Helper::try_into_vector(&key_array)?;
        let filtered_key_vector = key_vector.filter(filter)?;
        let filtered_key_array = filtered_key_vector.to_arrow_array();
        let filtered_key_array = filtered_key_array
            .as_any()
            .downcast_ref::<PrimitiveArray<K>>()
            .unwrap();

        let new_array = DictionaryArray::try_new(filtered_key_array.clone(), self.values().clone())
            .expect("Failed to create filtered dictionary array");

        Ok(Arc::new(Self {
            array: new_array,
            key_type: self.key_type.clone(),
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
            key_type: self.key_type.clone(),
            item_type: to_type.clone(),
            item_vector: self.item_vector.clone(),
        }))
    }

    fn take(&self, indices: &vectors::UInt32Vector) -> Result<VectorRef> {
        let key_array: ArrayRef = Arc::new(self.array.keys().clone());
        let key_vector = Helper::try_into_vector(&key_array)?;
        let new_key_vector = key_vector.take(indices)?;
        let new_key_array = new_key_vector.to_arrow_array();
        let new_key_array = new_key_array
            .as_any()
            .downcast_ref::<PrimitiveArray<K>>()
            .unwrap();

        let new_array = DictionaryArray::try_new(new_key_array.clone(), self.values().clone())
            .expect("Failed to create filtered dictionary array");

        Ok(Arc::new(Self {
            array: new_array,
            key_type: self.key_type.clone(),
            item_type: self.item_type.clone(),
            item_vector: self.item_vector.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray, UInt32Array};
    use arrow::datatypes::{Int64Type, UInt32Type};

    use super::*;

    // Helper function to create a test dictionary vector with string values
    fn create_test_dictionary() -> DictionaryVector<Int64Type> {
        // Dictionary values: ["a", "b", "c", "d"]
        // Keys: [0, 1, 2, null, 1, 3]
        // Resulting in: ["a", "b", "c", null, "b", "d"]
        let values = StringArray::from(vec!["a", "b", "c", "d"]);
        let keys = Int64Array::from(vec![Some(0), Some(1), Some(2), None, Some(1), Some(3)]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));
        DictionaryVector::<Int64Type>::try_from(dict_array).unwrap()
    }

    #[test]
    fn test_dictionary_vector_basics() {
        let dict_vec = create_test_dictionary();

        // Test length and null count
        assert_eq!(dict_vec.len(), 6);
        assert_eq!(dict_vec.null_count(), 1);

        // Test data type
        let data_type = dict_vec.data_type();
        if let ConcreteDataType::Dictionary(dict_type) = data_type {
            assert_eq!(*dict_type.value_type(), ConcreteDataType::string_datatype());
        } else {
            panic!("Expected Dictionary data type");
        }

        // Test is_null
        assert!(!dict_vec.is_null(0));
        assert!(dict_vec.is_null(3));

        // Test get values
        assert_eq!(dict_vec.get(0), Value::String("a".to_string().into()));
        assert_eq!(dict_vec.get(1), Value::String("b".to_string().into()));
        assert_eq!(dict_vec.get(3), Value::Null);
        assert_eq!(dict_vec.get(4), Value::String("b".to_string().into()));
    }

    #[test]
    fn test_slice() {
        let dict_vec = create_test_dictionary();
        let sliced = dict_vec.slice(1, 3);

        assert_eq!(sliced.len(), 3);
        assert_eq!(sliced.get(0), Value::String("b".to_string().into()));
        assert_eq!(sliced.get(1), Value::String("c".to_string().into()));
        assert_eq!(sliced.get(2), Value::Null);
    }

    #[test]
    fn test_replicate() {
        let dict_vec = create_test_dictionary();

        // Replicate with offsets [0, 2, 5] - should get values at these indices
        let offsets = vec![0, 2, 5];
        let replicated = dict_vec.replicate(&offsets);
        assert_eq!(replicated.len(), 5);
        assert_eq!(replicated.get(0), Value::String("b".to_string().into()));
        assert_eq!(replicated.get(1), Value::String("b".to_string().into()));
        assert_eq!(replicated.get(2), Value::String("c".to_string().into()));
        assert_eq!(replicated.get(3), Value::String("c".to_string().into()));
        assert_eq!(replicated.get(4), Value::String("c".to_string().into()));
    }

    #[test]
    fn test_filter() {
        let dict_vec = create_test_dictionary();

        // Keep only indices 0, 2, 4
        let filter_values = vec![true, false, true, false, true, false];
        let filter = vectors::BooleanVector::from(filter_values);

        let filtered = dict_vec.filter(&filter).unwrap();
        assert_eq!(filtered.len(), 3);

        // Check the values
        assert_eq!(filtered.get(0), Value::String("a".to_string().into()));
        assert_eq!(filtered.get(1), Value::String("c".to_string().into()));
        assert_eq!(filtered.get(2), Value::String("b".to_string().into()));
    }

    #[test]
    fn test_cast() {
        let dict_vec = create_test_dictionary();

        // Cast to the same type should return an equivalent vector
        let casted = dict_vec.cast(&ConcreteDataType::string_datatype()).unwrap();

        // The returned vector should have string values
        assert_eq!(
            casted.data_type(),
            ConcreteDataType::Dictionary(DictionaryType::new(
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::string_datatype(),
            ))
        );
        assert_eq!(casted.len(), dict_vec.len());

        // Values should match the original dictionary lookups
        assert_eq!(casted.get(0), Value::String("a".to_string().into()));
        assert_eq!(casted.get(1), Value::String("b".to_string().into()));
        assert_eq!(casted.get(2), Value::String("c".to_string().into()));
        assert_eq!(casted.get(3), Value::Null);
        assert_eq!(casted.get(4), Value::String("b".to_string().into()));
        assert_eq!(casted.get(5), Value::String("d".to_string().into()));
    }

    #[test]
    fn test_take() {
        let dict_vec = create_test_dictionary();

        // Take indices 2, 0, 4
        let indices_vec = vec![Some(2u32), Some(0), Some(4)];
        let indices = vectors::UInt32Vector::from(indices_vec);

        let taken = dict_vec.take(&indices).unwrap();
        assert_eq!(taken.len(), 3);

        // Check the values
        assert_eq!(taken.get(0), Value::String("c".to_string().into()));
        assert_eq!(taken.get(1), Value::String("a".to_string().into()));
        assert_eq!(taken.get(2), Value::String("b".to_string().into()));
    }

    #[test]
    fn test_other_type() {
        let values = StringArray::from(vec!["a", "b", "c", "d"]);
        let keys = UInt32Array::from(vec![Some(0), Some(1), Some(2), None, Some(1), Some(3)]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));
        let dict_vec = DictionaryVector::<UInt32Type>::try_from(dict_array).unwrap();
        assert_eq!(
            ConcreteDataType::dictionary_datatype(
                ConcreteDataType::uint32_datatype(),
                ConcreteDataType::string_datatype()
            ),
            dict_vec.data_type()
        );
    }
}
