use std::any::Any;
use std::slice::Iter;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MutablePrimitiveArray, PrimitiveArray};
use arrow::bitmap::utils::ZipValidity;
use serde_json::Value as JsonValue;
use snafu::{OptionExt, ResultExt};

use crate::data_type::DataTypeRef;
use crate::error;
use crate::error::ConversionSnafu;
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::types::{DataTypeBuilder, Primitive};
use crate::vectors::Vector;

/// Vector for primitive data types.
#[derive(Debug)]
pub struct PrimitiveVector<T: Primitive> {
    array: PrimitiveArray<T>,
}

impl<T: Primitive> PrimitiveVector<T> {
    pub fn new(array: PrimitiveArray<T>) -> Self {
        Self { array }
    }
}

impl<T: Primitive + DataTypeBuilder> Vector for PrimitiveVector<T> {
    fn data_type(&self) -> DataTypeRef {
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
}

impl<'a, T: Primitive> PrimitiveVector<T> {
    /// implement iter for PrimitiveVector
    #[inline]
    pub fn iter(&'a self) -> std::slice::Iter<'a, T> {
        self.array.values().iter()
    }

    /// Convert an Arrow array to PrimitiveVector.
    pub fn try_from_arrow_array(array: &dyn Array) -> Result<Self, error::Error> {
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
}

impl<T: Primitive + DataTypeBuilder> ScalarVector for PrimitiveVector<T> {
    type RefItem<'a> = T;
    type Iter<'a> = PrimitiveIter<'a, T>;
    type Builder = PrimitiveVectorBuilder<T>;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if idx < self.len() {
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

impl<T: Primitive> From<PrimitiveArray<T>> for PrimitiveVector<T> {
    fn from(arrow_array: PrimitiveArray<T>) -> Self {
        Self::new(
            arrow_array
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .with_context(|| ConversionSnafu {
                    from: format!("{:?}", arrow_array.data_type()),
                })
                .unwrap()
                .clone(),
        )
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

macro_rules! impl_serializable {
    ($ty: ident) => {
        impl crate::serialize::Serializable for $ty {
            fn serialize_to_json(&self) -> crate::error::Result<Vec<JsonValue>> {
                self.iter()
                    .map(|x| serde_json::to_value(x))
                    .collect::<serde_json::Result<_>>()
                    .context(crate::error::SerializeSnafu)
            }
        }
    };
}

impl_serializable! { UInt8Vector }
impl_serializable! { UInt16Vector }
impl_serializable! { UInt32Vector }
impl_serializable! { UInt64Vector }
impl_serializable! { Int8Vector }
impl_serializable! { Int16Vector }
impl_serializable! { Int32Vector }
impl_serializable! { Int64Vector }
impl_serializable! { Float32Vector }
impl_serializable! { Float64Vector }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::Serializable;

    #[test]
    pub fn test_from_arrow_array() {
        let arrow_array = PrimitiveArray::from_slice(vec![1, 2, 3, 4]);
        let vector = PrimitiveVector::from(arrow_array);
        println!("{:?}", vector);
        assert_eq!(
            vec![
                JsonValue::from(1),
                JsonValue::from(2),
                JsonValue::from(3),
                JsonValue::from(4)
            ],
            vector.serialize_to_json().unwrap()
        );
    }
}
