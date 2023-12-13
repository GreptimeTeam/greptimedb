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
use std::borrow::Borrow;
use std::sync::Arc;

use arrow::array::{Array, ArrayBuilder, ArrayIter, ArrayRef, BooleanArray, BooleanBuilder};
use snafu::ResultExt;

use crate::data_type::ConcreteDataType;
use crate::error::Result;
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::value::{Value, ValueRef};
use crate::vectors::{self, MutableVector, Validity, Vector, VectorRef};

/// Vector of boolean.
#[derive(Debug, PartialEq)]
pub struct BooleanVector {
    array: BooleanArray,
}

impl BooleanVector {
    pub(crate) fn as_arrow(&self) -> &dyn Array {
        &self.array
    }

    /// Get the inner boolean array.
    pub fn as_boolean_array(&self) -> &BooleanArray {
        &self.array
    }

    pub(crate) fn false_count(&self) -> usize {
        self.array.false_count()
    }
}

impl From<Vec<bool>> for BooleanVector {
    fn from(data: Vec<bool>) -> Self {
        BooleanVector {
            array: BooleanArray::from(data),
        }
    }
}

impl From<BooleanArray> for BooleanVector {
    fn from(array: BooleanArray) -> Self {
        Self { array }
    }
}

impl From<Vec<Option<bool>>> for BooleanVector {
    fn from(data: Vec<Option<bool>>) -> Self {
        BooleanVector {
            array: BooleanArray::from(data),
        }
    }
}

impl<Ptr: Borrow<Option<bool>>> FromIterator<Ptr> for BooleanVector {
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        BooleanVector {
            array: BooleanArray::from_iter(iter),
        }
    }
}

impl Vector for BooleanVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::boolean_datatype()
    }

    fn vector_type_name(&self) -> String {
        "BooleanVector".to_string()
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
        Arc::new(Self::from(self.array.slice(offset, length)))
    }

    fn get(&self, index: usize) -> Value {
        vectors::impl_get_for_vector!(self.array, index)
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        vectors::impl_get_ref_for_vector!(self.array, index)
    }
}

impl ScalarVector for BooleanVector {
    type OwnedItem = bool;
    type RefItem<'a> = bool;
    type Iter<'a> = ArrayIter<&'a BooleanArray>;
    type Builder = BooleanVectorBuilder;

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

pub struct BooleanVectorBuilder {
    mutable_array: BooleanBuilder,
}

impl MutableVector for BooleanVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::boolean_datatype()
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
        match value.as_boolean()? {
            Some(v) => self.mutable_array.append_value(v),
            None => self.mutable_array.append_null(),
        }
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        vectors::impl_extend_for_builder!(self, vector, BooleanVector, offset, length)
    }

    fn push_null(&mut self) {
        self.mutable_array.append_null()
    }
}

impl ScalarVectorBuilder for BooleanVectorBuilder {
    type VectorType = BooleanVector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: BooleanBuilder::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        match value {
            Some(v) => self.mutable_array.append_value(v),
            None => self.mutable_array.append_null(),
        }
    }

    fn finish(&mut self) -> Self::VectorType {
        BooleanVector {
            array: self.mutable_array.finish(),
        }
    }

    fn finish_cloned(&self) -> Self::VectorType {
        BooleanVector {
            array: self.mutable_array.finish_cloned(),
        }
    }
}

impl Serializable for BooleanVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        self.iter_data()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<_>>()
            .context(crate::error::SerializeSnafu)
    }
}

vectors::impl_try_from_arrow_array_for_vector!(BooleanArray, BooleanVector);

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType as ArrowDataType;
    use serde_json;

    use super::*;
    use crate::data_type::DataType;
    use crate::serialize::Serializable;
    use crate::types::BooleanType;

    #[test]
    fn test_boolean_vector_misc() {
        let bools = vec![true, false, true, true, false, false, true, true, false];
        let v = BooleanVector::from(bools.clone());
        assert_eq!(9, v.len());
        assert_eq!("BooleanVector", v.vector_type_name());
        assert!(!v.is_const());
        assert!(v.validity().is_all_valid());
        assert!(!v.only_null());
        assert_eq!(2, v.memory_size());

        for (i, b) in bools.iter().enumerate() {
            assert!(!v.is_null(i));
            assert_eq!(Value::Boolean(*b), v.get(i));
            assert_eq!(ValueRef::Boolean(*b), v.get_ref(i));
        }

        let arrow_arr = v.to_arrow_array();
        assert_eq!(9, arrow_arr.len());
        assert_eq!(&ArrowDataType::Boolean, arrow_arr.data_type());
    }

    #[test]
    fn test_serialize_boolean_vector_to_json() {
        let vector = BooleanVector::from(vec![true, false, true, true, false, false]);

        let json_value = vector.serialize_to_json().unwrap();
        assert_eq!(
            "[true,false,true,true,false,false]",
            serde_json::to_string(&json_value).unwrap(),
        );
    }

    #[test]
    fn test_serialize_boolean_vector_with_null_to_json() {
        let vector = BooleanVector::from(vec![Some(true), None, Some(false)]);

        let json_value = vector.serialize_to_json().unwrap();
        assert_eq!(
            "[true,null,false]",
            serde_json::to_string(&json_value).unwrap(),
        );
    }

    #[test]
    fn test_boolean_vector_from_vec() {
        let input = vec![false, true, false, true];
        let vec = BooleanVector::from(input.clone());
        assert_eq!(4, vec.len());
        for (i, v) in input.into_iter().enumerate() {
            assert_eq!(Some(v), vec.get_data(i), "Failed at {i}")
        }
    }

    #[test]
    fn test_boolean_vector_from_iter() {
        let input = vec![Some(false), Some(true), Some(false), Some(true)];
        let vec = input.iter().collect::<BooleanVector>();
        assert_eq!(4, vec.len());
        for (i, v) in input.into_iter().enumerate() {
            assert_eq!(v, vec.get_data(i), "Failed at {i}")
        }
    }

    #[test]
    fn test_boolean_vector_from_vec_option() {
        let input = vec![Some(false), Some(true), None, Some(true)];
        let vec = BooleanVector::from(input.clone());
        assert_eq!(4, vec.len());
        for (i, v) in input.into_iter().enumerate() {
            assert_eq!(v, vec.get_data(i), "failed at {i}")
        }
    }

    #[test]
    fn test_boolean_vector_build_get() {
        let input = [Some(true), None, Some(false)];
        let mut builder = BooleanVectorBuilder::with_capacity(3);
        for v in input {
            builder.push(v);
        }
        let vector = builder.finish();
        assert_eq!(input.len(), vector.len());

        let res: Vec<_> = vector.iter_data().collect();
        assert_eq!(input, &res[..]);

        for (i, v) in input.into_iter().enumerate() {
            assert_eq!(v, vector.get_data(i));
            assert_eq!(Value::from(v), vector.get(i));
        }
    }

    #[test]
    fn test_boolean_vector_validity() {
        let vector = BooleanVector::from(vec![Some(true), None, Some(false)]);
        assert_eq!(1, vector.null_count());
        let validity = vector.validity();
        assert_eq!(1, validity.null_count());
        assert!(!validity.is_set(1));

        let vector = BooleanVector::from(vec![true, false, false]);
        assert_eq!(0, vector.null_count());
        assert!(vector.validity().is_all_valid());
    }

    #[test]
    fn test_boolean_vector_builder() {
        let input = BooleanVector::from_slice(&[true, false, true]);

        let mut builder = BooleanType.create_mutable_vector(3);
        builder.push_value_ref(ValueRef::Boolean(true));
        assert!(builder.try_push_value_ref(ValueRef::Int32(123)).is_err());
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&crate::vectors::Int32Vector::from_slice([13]), 0, 1)
            .is_err());
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(BooleanVector::from_slice(&[true, false, true]));
        assert_eq!(expect, vector);
    }

    #[test]
    fn test_boolean_vector_builder_finish_cloned() {
        let mut builder = BooleanVectorBuilder::with_capacity(1024);
        builder.push(Some(true));
        builder.push(Some(false));
        builder.push(Some(true));
        let vector = builder.finish_cloned();
        assert!(vector.get_data(0).unwrap());
        assert_eq!(vector.len(), 3);
        assert_eq!(builder.len(), 3);

        builder.push(Some(false));
        let vector = builder.finish_cloned();
        assert!(!vector.get_data(3).unwrap());
        assert_eq!(builder.len(), 4);
    }
}
