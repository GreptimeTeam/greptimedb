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

use arrow_array::builder::{ArrayBuilder, Decimal128Builder};
use arrow_array::iterator::ArrayIter;
use arrow_array::{Array, ArrayRef, Decimal128Array};
use common_decimal::decimal128::{DECIMAL128_DEFAULT_SCALE, DECIMAL128_MAX_PRECISION};
use common_decimal::Decimal128;
use snafu::{OptionExt, ResultExt};

use super::{MutableVector, Validity, Vector, VectorRef};
use crate::arrow::datatypes::DataType as ArrowDataType;
use crate::data_type::ConcreteDataType;
use crate::error::{
    self, CastTypeSnafu, InvalidPrecisionOrScaleSnafu, Result, ValueExceedsPrecisionSnafu,
};
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
    /// New a Decimal128Vector from Arrow Decimal128Array
    pub fn new(array: Decimal128Array) -> Self {
        Self { array }
    }

    /// Construct Vector from i128 values
    pub fn from_values<I: IntoIterator<Item = i128>>(iter: I) -> Self {
        Self {
            array: Decimal128Array::from_iter_values(iter),
        }
    }

    /// Construct Vector from i128 values slice
    pub fn from_slice<P: AsRef<[i128]>>(slice: P) -> Self {
        let iter = slice.as_ref().iter().copied();
        Self {
            array: Decimal128Array::from_iter_values(iter),
        }
    }

    /// Construct Vector from Wrapper(Decimal128) values slice
    pub fn from_wrapper_slice<P: AsRef<[Decimal128]>>(slice: P) -> Self {
        let iter = slice.as_ref().iter().copied().map(|v| v.val());
        Self {
            array: Decimal128Array::from_iter_values(iter),
        }
    }

    /// Get decimal128 value from vector by offset and length.
    pub fn get_slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self { array }
    }

    /// Returns a Decimal vector with the same data as self, with the
    /// specified precision and scale(should in Decimal128 range),
    /// and return error if value is out of precision bound.
    ///
    ///
    /// For example:
    /// value = 12345, precision = 3, return error.
    pub fn with_precision_and_scale(self, precision: u8, scale: i8) -> Result<Self> {
        // validate if precision is too small
        self.validate_decimal_precision(precision)?;
        let array = self
            .array
            .with_precision_and_scale(precision, scale)
            .context(InvalidPrecisionOrScaleSnafu { precision, scale })?;
        Ok(Self { array })
    }

    /// Returns a Decimal vector with the same data as self, with the
    /// specified precision and scale(should in Decimal128 range),
    /// and return null if value is out of precision bound.
    ///
    /// For example:
    /// value = 12345, precision = 3, the value will be casted to null.
    pub fn with_precision_and_scale_to_null(self, precision: u8, scale: i8) -> Result<Self> {
        self.null_if_overflow_precision(precision)
            .with_precision_and_scale(precision, scale)
    }

    /// Return decimal value as string
    pub fn value_as_string(&self, idx: usize) -> String {
        self.array.value_as_string(idx)
    }

    /// Return decimal128 vector precision
    pub fn precision(&self) -> u8 {
        self.array.precision()
    }

    /// Return decimal128 vector scale
    pub fn scale(&self) -> i8 {
        self.array.scale()
    }

    /// Return decimal128 vector inner array
    pub(crate) fn as_arrow(&self) -> &dyn Array {
        &self.array
    }

    /// Validate decimal precision, if precision is invalid, return error.
    fn validate_decimal_precision(&self, precision: u8) -> Result<()> {
        self.array
            .validate_decimal_precision(precision)
            .context(ValueExceedsPrecisionSnafu { precision })
    }

    /// Values that exceed the precision bounds will be casted to Null.
    fn null_if_overflow_precision(&self, precision: u8) -> Self {
        Self {
            array: self.array.null_if_overflow_precision(precision),
        }
    }

    /// Get decimal128 Value from array by index.
    fn get_decimal128_value_from_array(&self, index: usize) -> Option<Decimal128> {
        if self.array.is_valid(index) {
            // Safety: The index have been checked by `is_valid()`.
            let value = unsafe { self.array.value_unchecked(index) };
            // Safety: The precision and scale have been checked by Vector.
            Some(Decimal128::new(value, self.precision(), self.scale()))
        } else {
            None
        }
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
        Arc::new(self.get_slice(offset, length))
    }

    fn get(&self, index: usize) -> Value {
        if let Some(decimal) = self.get_decimal128_value_from_array(index) {
            Value::Decimal128(decimal)
        } else {
            Value::Null
        }
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        if let Some(decimal) = self.get_decimal128_value_from_array(index) {
            ValueRef::Decimal128(decimal)
        } else {
            ValueRef::Null
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
                Some(d) => serde_json::to_value(d),
            })
            .collect::<serde_json::Result<_>>()
            .context(error::SerializeSnafu)
    }
}

pub struct Decimal128Iter<'a> {
    precision: u8,
    scale: i8,
    iter: ArrayIter<&'a Decimal128Array>,
}

impl<'a> Iterator for Decimal128Iter<'a> {
    type Item = Option<Decimal128>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|item| item.map(|v| Decimal128::new(v, self.precision, self.scale)))
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
        self.get_decimal128_value_from_array(idx)
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        Self::Iter {
            precision: self.precision(),
            scale: self.scale(),
            iter: self.array.iter(),
        }
    }
}

pub struct Decimal128VectorBuilder {
    precision: u8,
    scale: i8,
    mutable_array: Decimal128Builder,
}

impl MutableVector for Decimal128VectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::decimal128_datatype(self.precision, self.scale)
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
        self.mutable_array
            .extend(slice.iter_data().map(|v| v.map(|d| d.val())));
        Ok(())
    }
}

impl ScalarVectorBuilder for Decimal128VectorBuilder {
    type VectorType = Decimal128Vector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            precision: DECIMAL128_MAX_PRECISION,
            scale: DECIMAL128_DEFAULT_SCALE,
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

    fn finish_cloned(&self) -> Self::VectorType {
        Decimal128Vector {
            array: self.mutable_array.finish_cloned(),
        }
    }
}

impl Decimal128VectorBuilder {
    /// Change the precision and scale of the Decimal128VectorBuilder.
    pub fn with_precision_and_scale(self, precision: u8, scale: i8) -> Result<Self> {
        let mutable_array = self
            .mutable_array
            .with_precision_and_scale(precision, scale)
            .context(InvalidPrecisionOrScaleSnafu { precision, scale })?;
        Ok(Self {
            precision,
            scale,
            mutable_array,
        })
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

        let decimal_array = Decimal128Array::from(vec![Some(123), Some(456)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let decimal_vector = Decimal128Vector::from(decimal_array);
        let expect = Decimal128Vector::from_values(vec![123, 456])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(decimal_vector, expect);

        let decimal_array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(123), Some(456)])
                .with_precision_and_scale(3, 2)
                .unwrap(),
        );
        let decimal_vector = Decimal128Vector::try_from_arrow_array(decimal_array).unwrap();
        let expect = Decimal128Vector::from_values(vec![123, 456])
            .with_precision_and_scale(3, 2)
            .unwrap();
        assert_eq!(decimal_vector, expect);
    }

    #[test]
    fn test_from_slice() {
        let decimal_vector = Decimal128Vector::from_slice([123, 456]);
        let decimal_vector2 = Decimal128Vector::from_wrapper_slice([
            Decimal128::new(123, 10, 2),
            Decimal128::new(456, 10, 2),
        ]);
        let expect = Decimal128Vector::from_values(vec![123, 456]);

        assert_eq!(decimal_vector, expect);
        assert_eq!(decimal_vector2, expect);
    }

    #[test]
    fn test_decimal128_vector_slice() {
        let data = vec![100, 200, 300];
        // create a decimal vector
        let decimal_vector = Decimal128Vector::from_values(data.clone())
            .with_precision_and_scale(10, 2)
            .unwrap();
        let decimal_vector2 = decimal_vector.slice(1, 2);
        assert_eq!(decimal_vector2.len(), 2);
        assert_eq!(
            decimal_vector2.get(0),
            Value::Decimal128(Decimal128::new(200, 10, 2))
        );
        assert_eq!(
            decimal_vector2.get(1),
            Value::Decimal128(Decimal128::new(300, 10, 2))
        );
    }

    #[test]
    fn test_decimal128_vector_basic() {
        let data = vec![100, 200, 300];
        // create a decimal vector
        let decimal_vector = Decimal128Vector::from_values(data.clone())
            .with_precision_and_scale(10, 2)
            .unwrap();

        // can use value_of_string(idx) get a decimal string
        assert_eq!(decimal_vector.value_as_string(0), "1.00");

        // iterator for-loop
        for i in 0..data.len() {
            assert_eq!(
                decimal_vector.get_data(i),
                Some(Decimal128::new((i + 1) as i128 * 100, 10, 2))
            );
            assert_eq!(
                decimal_vector.get(i),
                Value::Decimal128(Decimal128::new((i + 1) as i128 * 100, 10, 2))
            );
            assert_eq!(
                decimal_vector.get_ref(i),
                ValueRef::Decimal128(Decimal128::new((i + 1) as i128 * 100, 10, 2))
            );
        }

        // directly convert vector with precision = 2 and scale = 1,
        // then all of value will be null because of precision.
        let decimal_vector = decimal_vector
            .with_precision_and_scale_to_null(2, 1)
            .unwrap();
        assert_eq!(decimal_vector.len(), 3);
        assert!(decimal_vector.is_null(0));
        assert!(decimal_vector.is_null(1));
        assert!(decimal_vector.is_null(2));
    }

    #[test]
    fn test_decimal128_vector_builder() {
        let mut decimal_builder = Decimal128VectorBuilder::with_capacity(3)
            .with_precision_and_scale(10, 2)
            .unwrap();
        decimal_builder.push(Some(Decimal128::new(100, 10, 2)));
        decimal_builder.push(Some(Decimal128::new(200, 10, 2)));
        decimal_builder.push(Some(Decimal128::new(300, 10, 2)));
        let decimal_vector = decimal_builder.finish();
        assert_eq!(decimal_vector.len(), 3);
        assert_eq!(decimal_vector.precision(), 10);
        assert_eq!(decimal_vector.scale(), 2);
        assert_eq!(
            decimal_vector.get(0),
            Value::Decimal128(Decimal128::new(100, 10, 2))
        );
        assert_eq!(
            decimal_vector.get(1),
            Value::Decimal128(Decimal128::new(200, 10, 2))
        );
        assert_eq!(
            decimal_vector.get(2),
            Value::Decimal128(Decimal128::new(300, 10, 2))
        );

        // push value error
        let mut decimal_builder = Decimal128VectorBuilder::with_capacity(3);
        decimal_builder.push(Some(Decimal128::new(123, 38, 10)));
        decimal_builder.push(Some(Decimal128::new(1234, 38, 10)));
        decimal_builder.push(Some(Decimal128::new(12345, 38, 10)));
        let decimal_vector = decimal_builder.finish();
        assert_eq!(decimal_vector.precision(), 38);
        assert_eq!(decimal_vector.scale(), 10);
        let result = decimal_vector.with_precision_and_scale(3, 2);
        assert_eq!(
            "Value exceeds the precision 3 bound",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_cast_to_decimal128() {
        let vector = Int8Vector::from_values(vec![1, 2, 3, 4, 100]);
        let casted_vector = vector.cast(&ConcreteDataType::decimal128_datatype(3, 1));
        assert!(casted_vector.is_ok());
        let vector = casted_vector.unwrap();
        let array = vector.as_any().downcast_ref::<Decimal128Vector>().unwrap();
        // because 100 is out of Decimal(3, 1) range, so it will be null
        assert!(array.is_null(4));
    }

    #[test]
    fn test_decimal28_vector_iter_data() {
        let vector = Decimal128Vector::from_values(vec![1, 2, 3, 4])
            .with_precision_and_scale(3, 1)
            .unwrap();
        let mut iter = vector.iter_data();
        assert_eq!(iter.next(), Some(Some(Decimal128::new(1, 3, 1))));
        assert_eq!(iter.next(), Some(Some(Decimal128::new(2, 3, 1))));
        assert_eq!(iter.next(), Some(Some(Decimal128::new(3, 3, 1))));
        assert_eq!(iter.next(), Some(Some(Decimal128::new(4, 3, 1))));
        assert_eq!(iter.next(), None);

        let values = vector
            .iter_data()
            .filter_map(|v| v.map(|x| x.val() * 2))
            .collect::<Vec<_>>();
        assert_eq!(values, vec![2, 4, 6, 8]);
    }

    #[test]
    fn test_decimal128_vector_builder_finish_cloned() {
        let mut builder = Decimal128VectorBuilder::with_capacity(1024);
        builder.push(Some(Decimal128::new(1, 3, 1)));
        builder.push(Some(Decimal128::new(1, 3, 1)));
        builder.push(Some(Decimal128::new(1, 3, 1)));
        builder.push(Some(Decimal128::new(1, 3, 1)));
        let vector = builder.finish_cloned();
        assert_eq!(vector.len(), 4);
        assert_eq!(builder.len(), 4);
    }
}
