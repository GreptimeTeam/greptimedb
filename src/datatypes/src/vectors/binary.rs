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

use arrow::array::{Array, ArrayBuilder, ArrayIter, ArrayRef};
use snafu::ResultExt;

use crate::arrow_array::{BinaryArray, MutableBinaryArray};
use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::value::{Value, ValueRef};
use crate::vectors::{self, MutableVector, Validity, Vector, VectorRef};

/// Vector of binary strings.
#[derive(Debug, PartialEq)]
pub struct BinaryVector {
    array: BinaryArray,
}

impl BinaryVector {
    pub(crate) fn as_arrow(&self) -> &dyn Array {
        &self.array
    }
}

impl From<BinaryArray> for BinaryVector {
    fn from(array: BinaryArray) -> Self {
        Self { array }
    }
}

impl From<Vec<Option<Vec<u8>>>> for BinaryVector {
    fn from(data: Vec<Option<Vec<u8>>>) -> Self {
        Self {
            array: BinaryArray::from_iter(data),
        }
    }
}

impl Vector for BinaryVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::binary_datatype()
    }

    fn vector_type_name(&self) -> String {
        "BinaryVector".to_string()
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
        let array = self.array.slice(offset, length);
        Arc::new(Self { array })
    }

    fn get(&self, index: usize) -> Value {
        vectors::impl_get_for_vector!(self.array, index)
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        vectors::impl_get_ref_for_vector!(self.array, index)
    }
}

impl From<Vec<Vec<u8>>> for BinaryVector {
    fn from(data: Vec<Vec<u8>>) -> Self {
        Self {
            array: BinaryArray::from_iter_values(data),
        }
    }
}

impl ScalarVector for BinaryVector {
    type OwnedItem = Vec<u8>;
    type RefItem<'a> = &'a [u8];
    type Iter<'a> = ArrayIter<&'a BinaryArray>;
    type Builder = BinaryVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if self.array.is_valid(idx) {
            Some(self.array.value(idx))
        } else {
            None
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        self.array.iter()
    }
}

pub struct BinaryVectorBuilder {
    mutable_array: MutableBinaryArray,
}

impl MutableVector for BinaryVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::binary_datatype()
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
        match value.as_binary()? {
            Some(v) => self.mutable_array.append_value(v),
            None => self.mutable_array.append_null(),
        }
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        vectors::impl_extend_for_builder!(self, vector, BinaryVector, offset, length)
    }

    fn push_null(&mut self) {
        self.mutable_array.append_null()
    }
}

impl ScalarVectorBuilder for BinaryVectorBuilder {
    type VectorType = BinaryVector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutableBinaryArray::with_capacity(capacity, 0),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        match value {
            Some(v) => self.mutable_array.append_value(v),
            None => self.mutable_array.append_null(),
        }
    }

    fn finish(&mut self) -> Self::VectorType {
        BinaryVector {
            array: self.mutable_array.finish(),
        }
    }

    fn finish_cloned(&self) -> Self::VectorType {
        BinaryVector {
            array: self.mutable_array.finish_cloned(),
        }
    }
}

impl Serializable for BinaryVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        self.iter_data()
            .map(|v| match v {
                None => Ok(serde_json::Value::Null), // if binary vector not present, map to NULL
                Some(vec) => serde_json::to_value(vec),
            })
            .collect::<serde_json::Result<_>>()
            .context(error::SerializeSnafu)
    }
}

vectors::impl_try_from_arrow_array_for_vector!(BinaryArray, BinaryVector);

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType as ArrowDataType;
    use common_base::bytes::Bytes;
    use serde_json;

    use super::*;
    use crate::arrow_array::BinaryArray;
    use crate::data_type::DataType;
    use crate::serialize::Serializable;
    use crate::types::BinaryType;

    #[test]
    fn test_binary_vector_misc() {
        let v = BinaryVector::from(BinaryArray::from_iter_values([
            vec![1, 2, 3],
            vec![1, 2, 3],
        ]));

        assert_eq!(2, v.len());
        assert_eq!("BinaryVector", v.vector_type_name());
        assert!(!v.is_const());
        assert!(v.validity().is_all_valid());
        assert!(!v.only_null());
        assert_eq!(128, v.memory_size());

        for i in 0..2 {
            assert!(!v.is_null(i));
            assert_eq!(Value::Binary(Bytes::from(vec![1, 2, 3])), v.get(i));
            assert_eq!(ValueRef::Binary(&[1, 2, 3]), v.get_ref(i));
        }

        let arrow_arr = v.to_arrow_array();
        assert_eq!(2, arrow_arr.len());
        assert_eq!(&ArrowDataType::LargeBinary, arrow_arr.data_type());
    }

    #[test]
    fn test_serialize_binary_vector_to_json() {
        let vector = BinaryVector::from(BinaryArray::from_iter_values([
            vec![1, 2, 3],
            vec![1, 2, 3],
        ]));

        let json_value = vector.serialize_to_json().unwrap();
        assert_eq!(
            "[[1,2,3],[1,2,3]]",
            serde_json::to_string(&json_value).unwrap()
        );
    }

    #[test]
    fn test_serialize_binary_vector_with_null_to_json() {
        let mut builder = BinaryVectorBuilder::with_capacity(4);
        builder.push(Some(&[1, 2, 3]));
        builder.push(None);
        builder.push(Some(&[4, 5, 6]));
        let vector = builder.finish();

        let json_value = vector.serialize_to_json().unwrap();
        assert_eq!(
            "[[1,2,3],null,[4,5,6]]",
            serde_json::to_string(&json_value).unwrap()
        );
    }

    #[test]
    fn test_from_arrow_array() {
        let arrow_array = BinaryArray::from_iter_values([vec![1, 2, 3], vec![1, 2, 3]]);
        let original = BinaryArray::from(arrow_array.to_data());
        let vector = BinaryVector::from(arrow_array);
        assert_eq!(original, vector.array);
    }

    #[test]
    fn test_binary_vector_build_get() {
        let mut builder = BinaryVectorBuilder::with_capacity(4);
        builder.push(Some(b"hello"));
        builder.push(Some(b"happy"));
        builder.push(Some(b"world"));
        builder.push(None);

        let vector = builder.finish();
        assert_eq!(b"hello", vector.get_data(0).unwrap());
        assert_eq!(None, vector.get_data(3));

        assert_eq!(Value::Binary(b"hello".as_slice().into()), vector.get(0));
        assert_eq!(Value::Null, vector.get(3));

        let mut iter = vector.iter_data();
        assert_eq!(b"hello", iter.next().unwrap().unwrap());
        assert_eq!(b"happy", iter.next().unwrap().unwrap());
        assert_eq!(b"world", iter.next().unwrap().unwrap());
        assert_eq!(None, iter.next().unwrap());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_binary_vector_validity() {
        let mut builder = BinaryVectorBuilder::with_capacity(4);
        builder.push(Some(b"hello"));
        builder.push(Some(b"world"));
        let vector = builder.finish();
        assert_eq!(0, vector.null_count());
        assert!(vector.validity().is_all_valid());

        let mut builder = BinaryVectorBuilder::with_capacity(3);
        builder.push(Some(b"hello"));
        builder.push(None);
        builder.push(Some(b"world"));
        let vector = builder.finish();
        assert_eq!(1, vector.null_count());
        let validity = vector.validity();
        assert!(!validity.is_set(1));

        assert_eq!(1, validity.null_count());
        assert!(!validity.is_set(1));
    }

    #[test]
    fn test_binary_vector_builder() {
        let input = BinaryVector::from_slice(&[b"world", b"one", b"two"]);

        let mut builder = BinaryType.create_mutable_vector(3);
        builder.push_value_ref(ValueRef::Binary("hello".as_bytes()));
        assert!(builder.try_push_value_ref(ValueRef::Int32(123)).is_err());
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&crate::vectors::Int32Vector::from_slice([13]), 0, 1)
            .is_err());
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(BinaryVector::from_slice(&[b"hello", b"one", b"two"]));
        assert_eq!(expect, vector);
    }

    #[test]
    fn test_binary_vector_builder_finish_cloned() {
        let mut builder = BinaryVectorBuilder::with_capacity(1024);
        builder.push(Some(b"one"));
        builder.push(Some(b"two"));
        builder.push(Some(b"three"));
        let vector = builder.finish_cloned();
        assert_eq!(b"one", vector.get_data(0).unwrap());
        assert_eq!(vector.len(), 3);
        assert_eq!(builder.len(), 3);

        builder.push(Some(b"four"));
        let vector = builder.finish_cloned();
        assert_eq!(b"four", vector.get_data(3).unwrap());
        assert_eq!(builder.len(), 4);
    }
}
