use std::any::Any;
use std::iter::FromIterator;
use std::slice::Iter;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MutablePrimitiveArray, PrimitiveArray};
use arrow::bitmap::utils::ZipValidity;
use serde_json::Value as JsonValue;
use snafu::{OptionExt, ResultExt};

use crate::data_type::ConcreteDataType;
use crate::error::ConversionSnafu;
use crate::error::{Result, SerializeSnafu};
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::{DataTypeBuilder, Primitive};
use crate::vectors::{Validity, Vector};

/// Vector for primitive data types.
#[derive(Debug)]
pub struct PrimitiveVector<T: Primitive> {
    array: PrimitiveArray<T>,
}

impl<T: Primitive> PrimitiveVector<T> {
    pub fn new(array: PrimitiveArray<T>) -> Self {
        Self { array }
    }
    pub fn try_from_arrow_array(array: ArrayRef) -> Result<Self> {
        Ok(Self::new(
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .with_context(|| ConversionSnafu {
                    from: format!("{:?}", array.data_type()),
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
}

impl<T: Primitive + DataTypeBuilder> Vector for PrimitiveVector<T> {
    fn data_type(&self) -> ConcreteDataType {
        T::build_data_type()
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

    fn validity(&self) -> Validity {
        match self.array.validity() {
            Some(bitmap) => Validity::Slots(bitmap),
            None => Validity::AllValid,
        }
    }
}

impl<T: Primitive> From<PrimitiveArray<T>> for PrimitiveVector<T> {
    fn from(array: PrimitiveArray<T>) -> Self {
        Self { array }
    }
}

impl<T: Primitive, Ptr: std::borrow::Borrow<Option<T>>> FromIterator<Ptr> for PrimitiveVector<T> {
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        Self {
            array: MutablePrimitiveArray::<T>::from_iter(iter).into(),
        }
    }
}

impl<T: Primitive + DataTypeBuilder> ScalarVector for PrimitiveVector<T> {
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

pub struct PrimitiveVectorBuilder<T: Primitive> {
    mutable_array: MutablePrimitiveArray<T>,
}

impl<T: Primitive + DataTypeBuilder> ScalarVectorBuilder for PrimitiveVectorBuilder<T> {
    type VectorType = PrimitiveVector<T>;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutablePrimitiveArray::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        self.mutable_array.push(value);
    }

    fn finish(self) -> Self::VectorType {
        PrimitiveVector {
            array: self.mutable_array.into(),
        }
    }
}

impl<T: Primitive + DataTypeBuilder> Serializable for PrimitiveVector<T> {
    fn serialize_to_json(&self) -> Result<Vec<JsonValue>> {
        self.iter_data()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;
    use crate::serialize::Serializable;

    fn check_vec(v: PrimitiveVector<i32>) {
        let json_value = v.serialize_to_json().unwrap();
        assert_eq!("[1,2,3,4]", serde_json::to_string(&json_value).unwrap(),);
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
}
