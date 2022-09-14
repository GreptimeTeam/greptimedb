use std::any::Any;
use std::iter::FromIterator;
use std::slice::Iter;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MutableArray, MutablePrimitiveArray, PrimitiveArray};
use arrow::bitmap::utils::ZipValidity;
use serde_json::Value as JsonValue;
use snafu::{OptionExt, ResultExt};

use crate::data_type::ConcreteDataType;
use crate::error::ConversionSnafu;
use crate::error::{Result, SerializeSnafu};
use crate::scalars::{Scalar, ScalarRef};
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::{Primitive, PrimitiveElement};
use crate::value::{Value, ValueRef};
use crate::vectors::{self, MutableVector, Validity, Vector, VectorRef};

/// Vector for primitive data types.
#[derive(Debug, Clone, PartialEq)]
pub struct PrimitiveVector<T: Primitive> {
    pub(crate) array: PrimitiveArray<T>,
}

impl<T: Primitive> PrimitiveVector<T> {
    pub fn new(array: PrimitiveArray<T>) -> Self {
        Self { array }
    }

    pub fn try_from_arrow_array(array: impl AsRef<dyn Array>) -> Result<Self> {
        Ok(Self::new(
            array
                .as_ref()
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .with_context(|| ConversionSnafu {
                    from: format!("{:?}", array.as_ref().data_type()),
                })?
                .clone(),
        ))
    }

    pub fn from_slice<P: AsRef<[T]>>(slice: P) -> Self {
        Self {
            array: PrimitiveArray::from_slice(slice),
        }
    }

    pub fn from_vec(array: Vec<T>) -> Self {
        Self {
            array: PrimitiveArray::from_vec(array),
        }
    }

    pub fn from_values<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self {
            array: PrimitiveArray::from_values(iter),
        }
    }

    pub(crate) fn as_arrow(&self) -> &dyn Array {
        &self.array
    }
}

impl<T: PrimitiveElement> Vector for PrimitiveVector<T> {
    fn data_type(&self) -> ConcreteDataType {
        T::build_data_type()
    }

    fn vector_type_name(&self) -> String {
        format!("{}Vector", T::type_name())
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
        self.array.values().len() * std::mem::size_of::<T>()
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
        if self.array.is_valid(index) {
            // Safety: The index have been checked by `is_valid()`.
            unsafe { self.array.value_unchecked(index).into_value_ref() }
        } else {
            ValueRef::Null
        }
    }
}

impl<T: Primitive> From<PrimitiveArray<T>> for PrimitiveVector<T> {
    fn from(array: PrimitiveArray<T>) -> Self {
        Self { array }
    }
}

impl<T: Primitive> From<Vec<Option<T>>> for PrimitiveVector<T> {
    fn from(v: Vec<Option<T>>) -> Self {
        Self {
            array: PrimitiveArray::<T>::from(v),
        }
    }
}

impl<T: Primitive, Ptr: std::borrow::Borrow<Option<T>>> FromIterator<Ptr> for PrimitiveVector<T> {
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        Self {
            array: MutablePrimitiveArray::<T>::from_iter(iter).into(),
        }
    }
}

impl<T> ScalarVector for PrimitiveVector<T>
where
    T: PrimitiveElement,
{
    type OwnedItem = T;
    type RefItem<'a> = T;
    type Iter<'a> = PrimitiveIter<'a, T>;
    type Builder = PrimitiveVectorBuilder<T>;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if self.array.is_valid(idx) {
            Some(self.array.value(idx))
        } else {
            None
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        PrimitiveIter {
            iter: self.array.iter(),
        }
    }
}

pub type UInt8Vector = PrimitiveVector<u8>;
pub type UInt16Vector = PrimitiveVector<u16>;
pub type UInt32Vector = PrimitiveVector<u32>;
pub type UInt64Vector = PrimitiveVector<u64>;

pub type Int8Vector = PrimitiveVector<i8>;
pub type Int16Vector = PrimitiveVector<i16>;
pub type Int32Vector = PrimitiveVector<i32>;
pub type Int64Vector = PrimitiveVector<i64>;

pub type Float32Vector = PrimitiveVector<f32>;
pub type Float64Vector = PrimitiveVector<f64>;

pub struct PrimitiveIter<'a, T> {
    iter: ZipValidity<'a, &'a T, Iter<'a, T>>,
}

impl<'a, T: Copy> Iterator for PrimitiveIter<'a, T> {
    type Item = Option<T>;

    fn next(&mut self) -> Option<Option<T>> {
        self.iter.next().map(|v| v.copied())
    }
}

pub struct PrimitiveVectorBuilder<T: PrimitiveElement> {
    pub(crate) mutable_array: MutablePrimitiveArray<T>,
}

impl<T: PrimitiveElement> PrimitiveVectorBuilder<T> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutablePrimitiveArray::with_capacity(capacity),
        }
    }
}

pub type UInt8VectorBuilder = PrimitiveVectorBuilder<u8>;
pub type UInt16VectorBuilder = PrimitiveVectorBuilder<u16>;
pub type UInt32VectorBuilder = PrimitiveVectorBuilder<u32>;
pub type UInt64VectorBuilder = PrimitiveVectorBuilder<u64>;

pub type Int8VectorBuilder = PrimitiveVectorBuilder<i8>;
pub type Int16VectorBuilder = PrimitiveVectorBuilder<i16>;
pub type Int32VectorBuilder = PrimitiveVectorBuilder<i32>;
pub type Int64VectorBuilder = PrimitiveVectorBuilder<i64>;

pub type Float32VectorBuilder = PrimitiveVectorBuilder<f32>;
pub type Float64VectorBuilder = PrimitiveVectorBuilder<f64>;

impl<T: PrimitiveElement> MutableVector for PrimitiveVectorBuilder<T> {
    fn data_type(&self) -> ConcreteDataType {
        T::build_data_type()
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
        Arc::new(PrimitiveVector::<T> {
            array: std::mem::take(&mut self.mutable_array).into(),
        })
    }

    fn push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        let primitive = T::cast_value_ref(value)?;
        self.mutable_array.push(primitive);
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        let primitive = T::cast_vector(vector)?;
        // Slice the underlying array to avoid creating a new Arc.
        let slice = primitive.slice(offset, length);
        self.mutable_array.extend_trusted_len(slice.iter());
        Ok(())
    }
}

impl<T> ScalarVectorBuilder for PrimitiveVectorBuilder<T>
where
    T: Scalar<VectorType = PrimitiveVector<T>> + PrimitiveElement,
    for<'a> T: ScalarRef<'a, ScalarType = T, VectorType = PrimitiveVector<T>>,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    type VectorType = PrimitiveVector<T>;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutablePrimitiveArray::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        self.mutable_array.push(value);
    }

    fn finish(&mut self) -> Self::VectorType {
        PrimitiveVector {
            array: std::mem::take(&mut self.mutable_array).into(),
        }
    }
}

impl<T: PrimitiveElement> Serializable for PrimitiveVector<T> {
    fn serialize_to_json(&self) -> Result<Vec<JsonValue>> {
        self.array
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

pub(crate) fn replicate_primitive<T: PrimitiveElement>(
    vector: &PrimitiveVector<T>,
    offsets: &[usize],
) -> VectorRef {
    assert_eq!(offsets.len(), vector.len());

    if offsets.is_empty() {
        return vector.slice(0, 0);
    }

    let mut builder = PrimitiveVectorBuilder::<T>::with_capacity(*offsets.last().unwrap() as usize);

    let mut previous_offset = 0;

    for (i, offset) in offsets.iter().enumerate() {
        let data = unsafe { vector.array.value_unchecked(i) };
        builder.mutable_array.extend(
            std::iter::repeat(data)
                .take(*offset - previous_offset)
                .map(Option::Some),
        );
        previous_offset = *offset;
    }
    builder.to_vector()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType as ArrowDataType;
    use serde_json;

    use super::*;
    use crate::data_type::DataType;
    use crate::serialize::Serializable;
    use crate::types::Int64Type;

    fn check_vec(v: PrimitiveVector<i32>) {
        assert_eq!(4, v.len());
        assert_eq!("Int32Vector", v.vector_type_name());
        assert!(!v.is_const());
        assert_eq!(Validity::AllValid, v.validity());
        assert!(!v.only_null());

        for i in 0..4 {
            assert!(!v.is_null(i));
            assert_eq!(Value::Int32(i as i32 + 1), v.get(i));
            assert_eq!(ValueRef::Int32(i as i32 + 1), v.get_ref(i));
        }

        let json_value = v.serialize_to_json().unwrap();
        assert_eq!("[1,2,3,4]", serde_json::to_string(&json_value).unwrap(),);

        let arrow_arr = v.to_arrow_array();
        assert_eq!(4, arrow_arr.len());
        assert_eq!(&ArrowDataType::Int32, arrow_arr.data_type());
    }

    #[test]
    fn test_from_values() {
        let v = PrimitiveVector::<i32>::from_values(vec![1, 2, 3, 4]);
        check_vec(v);
    }

    #[test]
    fn test_from_vec() {
        let v = PrimitiveVector::<i32>::from_vec(vec![1, 2, 3, 4]);
        check_vec(v);
    }

    #[test]
    fn test_from_slice() {
        let v = PrimitiveVector::<i32>::from_slice(vec![1, 2, 3, 4]);
        check_vec(v);
    }

    #[test]
    fn test_serialize_primitive_vector_with_null_to_json() {
        let input = [Some(1i32), Some(2i32), None, Some(4i32), None];
        let mut builder = PrimitiveVectorBuilder::with_capacity(input.len());
        for v in input {
            builder.push(v);
        }
        let vector = builder.finish();

        let json_value = vector.serialize_to_json().unwrap();
        assert_eq!(
            "[1,2,null,4,null]",
            serde_json::to_string(&json_value).unwrap(),
        );
    }

    #[test]
    fn test_from_arrow_array() {
        let arrow_array = PrimitiveArray::from_slice(vec![1, 2, 3, 4]);
        let v = PrimitiveVector::from(arrow_array);
        check_vec(v);
    }

    #[test]
    fn test_primitive_vector_build_get() {
        let input = [Some(1i32), Some(2i32), None, Some(4i32), None];
        let mut builder = PrimitiveVectorBuilder::with_capacity(input.len());
        for v in input {
            builder.push(v);
        }
        let vector = builder.finish();
        assert_eq!(input.len(), vector.len());

        for (i, v) in input.into_iter().enumerate() {
            assert_eq!(v, vector.get_data(i));
            assert_eq!(Value::from(v), vector.get(i));
        }

        let res: Vec<_> = vector.iter_data().collect();
        assert_eq!(input, &res[..]);
    }

    #[test]
    fn test_primitive_vector_validity() {
        let input = [Some(1i32), Some(2i32), None, None];
        let mut builder = PrimitiveVectorBuilder::with_capacity(input.len());
        for v in input {
            builder.push(v);
        }
        let vector = builder.finish();
        assert_eq!(2, vector.null_count());
        let validity = vector.validity();
        let slots = validity.slots().unwrap();
        assert_eq!(2, slots.null_count());
        assert!(!slots.get_bit(2));
        assert!(!slots.get_bit(3));

        let vector = PrimitiveVector::<i32>::from_slice(vec![1, 2, 3, 4]);
        assert_eq!(0, vector.null_count());
        assert_eq!(Validity::AllValid, vector.validity());
    }

    #[test]
    fn test_memory_size() {
        let v = PrimitiveVector::<i32>::from_slice((0..5).collect::<Vec<i32>>());
        assert_eq!(20, v.memory_size());
        let v = PrimitiveVector::<i64>::from(vec![Some(0i64), Some(1i64), Some(2i64), None, None]);
        assert_eq!(40, v.memory_size());
    }

    #[test]
    fn test_primitive_vector_builder() {
        let mut builder = Int64Type::default().create_mutable_vector(3);
        builder.push_value_ref(ValueRef::Int64(123)).unwrap();
        assert!(builder.push_value_ref(ValueRef::Int32(123)).is_err());

        let input = Int64Vector::from_slice(&[7, 8, 9]);
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&Int32Vector::from_slice(&[13]), 0, 1)
            .is_err());
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(Int64Vector::from_slice(&[123, 8, 9]));
        assert_eq!(expect, vector);
    }
}
