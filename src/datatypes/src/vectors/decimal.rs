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
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::ArrayData;
use arrow_array::builder::{ArrayBuilder, Decimal128Builder};
use arrow_array::iterator::ArrayIter;
use arrow_array::{Array, ArrayRef, Decimal128Array};
use common_decimal::Decimal128;
use snafu::{OptionExt, ResultExt};

use super::{MutableVector, Validity, Vector, VectorRef};
use crate::arrow::datatypes::DataType as ArrowDataType;
use crate::data_type::ConcreteDataType;
use crate::error::{self, CastTypeSnafu, InvalidArgumentsSnafu, Result};
use crate::prelude::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::value::{Value, ValueRef};
use crate::vectors;

/// Decimal128Vector is a vector keep i128 values with precision and scale.
#[derive(Debug, PartialEq)]
pub struct Decimal128Vector {
    array: Decimal128Array,
}

impl Decimal128Vector {
    pub fn new(array: Decimal128Array) -> Self {
        Self { array }
    }

    pub fn from_array_data(array: ArrayData) -> Self {
        Self {
            array: Decimal128Array::from(array),
        }
    }

    /// Construct Vector from i128 values
    pub fn from_values<I: IntoIterator<Item = i128>>(iter: I) -> Self {
        Self {
            array: Decimal128Array::from_iter_values(iter),
        }
    }

    pub fn from_slice<P: AsRef<[i128]>>(slice: P) -> Self {
        let iter = slice.as_ref().iter().copied();
        Self {
            array: Decimal128Array::from_iter_values(iter),
        }
    }

    pub fn from_wrapper_slice<P: AsRef<[Decimal128]>>(slice: P) -> Self {
        let iter = slice.as_ref().iter().copied().map(|v| v.val());
        Self {
            array: Decimal128Array::from_iter_values(iter),
        }
    }

    pub fn to_array_data(&self) -> ArrayData {
        self.array.to_data()
    }

    pub fn get_slice(&self, offset: usize, length: usize) -> Self {
        let data = self.array.to_data().slice(offset, length);
        Self::from_array_data(data)
    }

    /// Change the precision and scale of the Decimal128Vector,
    /// And check precision and scale if compatible.
    pub fn with_precision_and_scale(self, precision: u8, scale: i8) -> Result<Self> {
        let array = self
            .array
            .with_precision_and_scale(precision, scale)
            .context(InvalidArgumentsSnafu {})?;
        Ok(Self { array })
    }

    pub fn null_if_overflow_precision(&self, precision: u8) -> Self {
        Self {
            array: self.array.null_if_overflow_precision(precision),
        }
    }

    pub fn validate_decimal_precision(&self, precision: u8) -> Result<()> {
        self.array
            .validate_decimal_precision(precision)
            .context(InvalidArgumentsSnafu {})
    }

    /// Return decimal value as string
    pub fn value_as_string(&self, idx: usize) -> String {
        self.array.value_as_string(idx)
    }

    pub fn precision(&self) -> u8 {
        self.array.precision()
    }

    pub fn scale(&self) -> i8 {
        self.array.scale()
    }

    pub(crate) fn as_arrow(&self) -> &dyn Array {
        &self.array
    }
}

impl Vector for Decimal128Vector {
    fn data_type(&self) -> ConcreteDataType {
        if let ArrowDataType::Decimal128(p, s) = self.array.data_type() {
            ConcreteDataType::decimal128_datatype(*p, *s)
        } else {
            ConcreteDataType::decimal128_default_datatype()
        }
    }

    fn vector_type_name(&self) -> String {
        "Decimal128Vector".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    fn to_arrow_array(&self) -> ArrayRef {
        let data = self.array.to_data();
        Arc::new(Decimal128Array::from(data))
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn Array> {
        let data = self.array.to_data();
        Box::new(Decimal128Array::from(data))
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
        let data = self.array.to_data().slice(offset, length);
        Arc::new(Self::from_array_data(data))
    }

    fn get(&self, index: usize) -> Value {
        if !self.array.is_valid(index) {
            return Value::Null;
        }

        match self.array.data_type() {
            ArrowDataType::Decimal128(precision, scale) => {
                // Safety: The index have been checked by `is_valid()`.
                let value = unsafe { self.array.value_unchecked(index) };
                Value::Decimal128(Decimal128::new_unchecked(value, *precision, *scale))
            }
            _ => Value::Null,
        }
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        if !self.array.is_valid(index) {
            return ValueRef::Null;
        }

        match self.array.data_type() {
            ArrowDataType::Decimal128(precision, scale) => {
                // Safety: The index have been checked by `is_valid()`.
                let value = unsafe { self.array.value_unchecked(index) };
                ValueRef::Decimal128(Decimal128::new_unchecked(value, *precision, *scale))
            }
            _ => ValueRef::Null,
        }
    }
}

impl From<Decimal128Array> for Decimal128Vector {
    fn from(array: Decimal128Array) -> Self {
        Self { array }
    }
}

impl From<Vec<Option<i128>>> for Decimal128Vector {
    fn from(vec: Vec<Option<i128>>) -> Self {
        let array = Decimal128Array::from_iter(vec);
        Self { array }
    }
}

impl Serializable for Decimal128Vector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        self.iter_data()
            .map(|v| match v {
                None => Ok(serde_json::Value::Null), // if decimal vector not present, map to NULL
                Some(vec) => serde_json::to_value(vec),
            })
            .collect::<serde_json::Result<_>>()
            .context(error::SerializeSnafu)
    }
}

pub struct Decimal128Iter<'a> {
    data_type: &'a ArrowDataType,
    iter: ArrayIter<&'a Decimal128Array>,
}

impl<'a> Iterator for Decimal128Iter<'a> {
    type Item = Option<Decimal128>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.iter.next().and_then(|v| {
            v.and_then(|v| match self.data_type {
                ArrowDataType::Decimal128(precision, scale) => {
                    Some(Decimal128::new_unchecked(v, *precision, *scale))
                }
                _ => None,
            })
        }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl ScalarVector for Decimal128Vector {
    type OwnedItem = Decimal128;

    type RefItem<'a> = Decimal128;

    type Iter<'a> = Decimal128Iter<'a>;

    type Builder = Decimal128VectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if !self.array.is_valid(idx) {
            return None;
        }

        match self.array.data_type() {
            ArrowDataType::Decimal128(precision, scale) => {
                // Safety: The index have been checked by `is_valid()`.
                let value = unsafe { self.array.value_unchecked(idx) };
                // Safety: The precision and scale have been checked by ArrowDataType.
                Some(Decimal128::new_unchecked(value, *precision, *scale))
            }
            _ => None,
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        Self::Iter {
            data_type: self.array.data_type(),
            iter: self.array.iter(),
        }
    }
}

pub struct Decimal128VectorBuilder {
    mutable_array: Decimal128Builder,
}

impl MutableVector for Decimal128VectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        unimplemented!()
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

    fn try_push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        let decimal_val = value.as_decimal128()?.map(|v| v.val());
        self.mutable_array.append_option(decimal_val);
        Ok(())
    }

    fn push_null(&mut self) {
        self.mutable_array.append_null();
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        let decimal_vector =
            vector
                .as_any()
                .downcast_ref::<Decimal128Vector>()
                .context(CastTypeSnafu {
                    msg: format!(
                        "Failed to cast vector from {} to Decimal128Vector",
                        vector.vector_type_name(),
                    ),
                })?;
        let slice = decimal_vector.get_slice(offset, length);
        for i in slice.iter_data() {
            self.mutable_array.append_option(i.map(|v| v.val()));
        }
        Ok(())
    }
}

impl ScalarVectorBuilder for Decimal128VectorBuilder {
    type VectorType = Decimal128Vector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: Decimal128Builder::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        self.mutable_array.append_option(value.map(|v| v.val()));
    }

    fn finish(&mut self) -> Self::VectorType {
        Decimal128Vector {
            array: self.mutable_array.finish(),
        }
    }
}

vectors::impl_try_from_arrow_array_for_vector!(Decimal128Array, Decimal128Vector);

#[cfg(test)]
pub mod tests {
    use arrow_array::Decimal128Array;
    use common_decimal::Decimal128;

    use super::*;
    use crate::vectors::operations::VectorOp;
    use crate::vectors::Int8Vector;

    #[test]
    fn test_from_arrow_decimal128_array() {
        let decimal_array = Decimal128Array::from(vec![Some(123), Some(456)]);
        let decimal_vector = Decimal128Vector::from(decimal_array);
        let expect = Decimal128Vector::from_values(vec![123, 456]);

        assert_eq!(decimal_vector, expect);
    }

    #[test]
    fn test_from_slice() {
        let decimal_vector = Decimal128Vector::from_slice([123, 456]);
        let decimal_vector2 = Decimal128Vector::from_wrapper_slice([
            Decimal128::new_unchecked(123, 10, 2),
            Decimal128::new_unchecked(456, 10, 2),
        ]);
        let expect = Decimal128Vector::from_values(vec![123, 456]);

        assert_eq!(decimal_vector, expect);
        assert_eq!(decimal_vector2, expect);
    }

    #[test]
    fn test_decimal128_vector_basic() {
        let data = vec![100, 200, 300];
        // create a decimal vector
        let decimal_vector = Decimal128Vector::from_values(data.clone())
            .with_precision_and_scale(10, 2)
            .unwrap();

        assert_eq!(decimal_vector.len(), 3);
        // check the first value
        assert_eq!(
            decimal_vector.get(0),
            Value::Decimal128(Decimal128::new_unchecked(100, 10, 2))
        );

        // iterator for-loop
        for (i, _) in data.iter().enumerate() {
            assert_eq!(
                decimal_vector.get_data(i),
                Some(Decimal128::new_unchecked((i + 1) as i128 * 100, 10, 2))
            );
            assert_eq!(
                decimal_vector.get(i),
                Value::Decimal128(Decimal128::new_unchecked((i + 1) as i128 * 100, 10, 2))
            )
        }
    }

    #[test]
    fn test_decimal128_vector_builder() {
        let mut decimal_builder = Decimal128VectorBuilder::with_capacity(3);
        decimal_builder.push(Some(Decimal128::new_unchecked(100, 10, 2)));
        decimal_builder.push(Some(Decimal128::new_unchecked(200, 10, 2)));
        decimal_builder.push(Some(Decimal128::new_unchecked(300, 10, 2)));
        let decimal_vector = decimal_builder
            .finish()
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(decimal_vector.len(), 3);
        assert_eq!(
            decimal_vector.get(0),
            Value::Decimal128(Decimal128::new_unchecked(100, 10, 2))
        );
        assert_eq!(
            decimal_vector.get(1),
            Value::Decimal128(Decimal128::new_unchecked(200, 10, 2))
        );
        assert_eq!(
            decimal_vector.get(2),
            Value::Decimal128(Decimal128::new_unchecked(300, 10, 2))
        );

        // push value error
        let mut decimal_builder = Decimal128VectorBuilder::with_capacity(3);
        decimal_builder.push(Some(Decimal128::new_unchecked(123, 10, 2)));
        decimal_builder.push(Some(Decimal128::new_unchecked(1234, 10, 2)));
        decimal_builder.push(Some(Decimal128::new_unchecked(12345, 10, 2)));
        let decimal_vector = decimal_builder.finish().with_precision_and_scale(3, 2);
        assert!(decimal_vector.is_ok());
    }

    #[test]
    fn test_cast() {
        let vector = Int8Vector::from_values(vec![1, 2, 3, 4, 100]);
        let casted_vector = vector.cast(&ConcreteDataType::decimal128_datatype(3, 1));
        assert!(casted_vector.is_ok());
        let vector = casted_vector.unwrap();
        let array = vector.as_any().downcast_ref::<Decimal128Vector>().unwrap();
        // because 100 is out of Decimal(3, 1) range, so it will be null
        assert!(array.is_null(4));
    }
}
