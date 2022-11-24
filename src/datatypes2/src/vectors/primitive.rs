use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayData, ArrayIter, ArrayRef, PrimitiveArray, PrimitiveBuilder,
};
use serde_json::Value as JsonValue;
use snafu::ResultExt;

use crate::data_type::ConcreteDataType;
use crate::error::{Result, SerializeSnafu};
use crate::scalars::{Scalar, ScalarRef, ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::{LogicalPrimitiveType, WrapperType};
use crate::value::{IntoValueRef, Value, ValueRef};
use crate::vectors::{self, MutableVector, Validity, Vector, VectorRef};

/// Vector for primitive data types.
#[derive(PartialEq)]
pub struct PrimitiveVector<T: LogicalPrimitiveType> {
    // TODO(yingwen): Maybe we don't need to pub this field.
    pub(crate) array: PrimitiveArray<T::ArrowPrimitive>,
}

impl<T: LogicalPrimitiveType> PrimitiveVector<T> {
    pub fn new(array: PrimitiveArray<T::ArrowPrimitive>) -> Self {
        Self { array }
    }

    pub fn try_from_arrow_array(array: impl AsRef<dyn Array>) -> Result<Self> {
        let data = array.as_ref().data().clone();
        // TODO(yingwen): Should we check the array type?
        let concrete_array = PrimitiveArray::<T::ArrowPrimitive>::from(data);
        Ok(Self::new(concrete_array))
    }

    pub fn from_slice<P: AsRef<[T::Native]>>(slice: P) -> Self {
        let iter = slice.as_ref().iter().copied();
        Self {
            array: PrimitiveArray::from_iter_values(iter),
        }
    }

    pub fn from_vec(array: Vec<T::Native>) -> Self {
        Self {
            array: PrimitiveArray::from_iter_values(array),
        }
    }

    pub fn from_values<I: IntoIterator<Item = T::Native>>(iter: I) -> Self {
        Self {
            array: PrimitiveArray::from_iter_values(iter),
        }
    }

    pub(crate) fn as_arrow(&self) -> &PrimitiveArray<T::ArrowPrimitive> {
        &self.array
    }

    fn to_array_data(&self) -> ArrayData {
        self.array.data().clone()
    }

    fn from_array_data(data: ArrayData) -> Self {
        Self {
            array: PrimitiveArray::from(data),
        }
    }

    // To distinguish with `Vector::slice()`.
    fn get_slice(&self, offset: usize, length: usize) -> Self {
        let data = self.array.data().slice(offset, length);
        Self::from_array_data(data)
    }
}

impl<T: LogicalPrimitiveType> Vector for PrimitiveVector<T> {
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
        let data = self.to_array_data();
        Arc::new(PrimitiveArray::<T::ArrowPrimitive>::from(data))
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn Array> {
        let data = self.to_array_data();
        Box::new(PrimitiveArray::<T::ArrowPrimitive>::from(data))
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
        let data = self.array.data().slice(offset, length);
        Arc::new(Self::from_array_data(data))
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

impl<T: LogicalPrimitiveType> fmt::Debug for PrimitiveVector<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PrimitiveVector")
            .field("array", &self.array)
            .finish()
    }
}

// impl<T: Primitive> From<PrimitiveArray<T>> for PrimitiveVector<T> {
//     fn from(array: PrimitiveArray<T>) -> Self {
//         Self { array }
//     }
// }

// impl<T: Primitive> From<Vec<Option<T>>> for PrimitiveVector<T> {
//     fn from(v: Vec<Option<T>>) -> Self {
//         Self {
//             array: PrimitiveArray::<T>::from(v),
//         }
//     }
// }

// impl<T: Primitive, Ptr: std::borrow::Borrow<Option<T>>> FromIterator<Ptr> for PrimitiveVector<T> {
//     fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
//         Self {
//             array: MutablePrimitiveArray::<T>::from_iter(iter).into(),
//         }
//     }
// }

pub struct PrimitiveIter<'a, T: LogicalPrimitiveType> {
    iter: ArrayIter<&'a PrimitiveArray<T::ArrowPrimitive>>,
}

impl<'a, T: LogicalPrimitiveType> Iterator for PrimitiveIter<'a, T> {
    type Item = Option<T::Wrapper>;

    fn next(&mut self) -> Option<Option<T::Wrapper>> {
        self.iter
            .next()
            .map(|item| item.map(|v| T::Wrapper::from_native(v)))
    }

    // TODO(yingwen): Other methods like size_hint.
}

impl<T: LogicalPrimitiveType> ScalarVector for PrimitiveVector<T> {
    type OwnedItem = T::Wrapper;
    type RefItem<'a> = T::Wrapper;
    type Iter<'a> = PrimitiveIter<'a, T>;
    type Builder = PrimitiveVectorBuilder<T>;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if self.array.is_valid(idx) {
            Some(T::Wrapper::from_native(self.array.value(idx)))
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

impl<T: LogicalPrimitiveType> Serializable for PrimitiveVector<T> {
    fn serialize_to_json(&self) -> Result<Vec<JsonValue>> {
        self.array
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

/// Builder to build a primitive vector.
pub struct PrimitiveVectorBuilder<T: LogicalPrimitiveType> {
    mutable_array: PrimitiveBuilder<T::ArrowPrimitive>,
}

impl<T: LogicalPrimitiveType> MutableVector for PrimitiveVectorBuilder<T> {
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
        Arc::new(self.finish())
    }

    fn push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        let primitive = T::cast_value_ref(value)?;
        match primitive {
            Some(v) => self.mutable_array.append_value(v.into_native()),
            None => self.mutable_array.append_null(),
        }
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        let primitive = T::cast_vector(vector)?;
        // Slice the underlying array to avoid creating a new Arc.
        let slice = primitive.get_slice(offset, length);
        for v in slice.iter_data() {
            self.push(v);
        }
        Ok(())
    }
}

impl<T> ScalarVectorBuilder for PrimitiveVectorBuilder<T>
where
    T: LogicalPrimitiveType,
    T::Wrapper: Scalar<VectorType = PrimitiveVector<T>>,
    for<'a> T::Wrapper: ScalarRef<'a, ScalarType = T::Wrapper, VectorType = PrimitiveVector<T>>,
    for<'a> T::Wrapper: Scalar<RefType<'a> = T::Wrapper>,
{
    type VectorType = PrimitiveVector<T>;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: PrimitiveBuilder::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        self.mutable_array
            .append_option(value.map(|v| v.into_native()));
    }

    fn finish(&mut self) -> Self::VectorType {
        PrimitiveVector {
            array: self.mutable_array.finish(),
        }
    }
}

// impl<T: PrimitiveElement> PrimitiveVectorBuilder<T> {
//     fn with_type_capacity(data_type: ConcreteDataType, capacity: usize) -> Self {
//         Self {
//             mutable_array: MutablePrimitiveArray::with_capacity_from(
//                 capacity,
//                 data_type.as_arrow_type(),
//             ),
//         }
//     }
// }

pub(crate) fn replicate_primitive<T: LogicalPrimitiveType>(
    vector: &PrimitiveVector<T>,
    offsets: &[usize],
) -> PrimitiveVector<T> {
    assert_eq!(offsets.len(), vector.len());

    if offsets.is_empty() {
        return vector.get_slice(0, 0);
    }

    let mut builder = PrimitiveVectorBuilder::<T>::with_capacity(*offsets.last().unwrap() as usize);

    let mut previous_offset = 0;

    for (offset, value) in offsets.iter().zip(vector.array.iter()) {
        let repeat_times = *offset - previous_offset;
        match value {
            Some(data) => {
                unsafe {
                    // Safety: std::iter::Repeat and std::iter::Take implement TrustedLen.
                    builder
                        .mutable_array
                        .append_trusted_len_iter(std::iter::repeat(data).take(repeat_times));
                }
            }
            None => {
                builder.mutable_array.append_nulls(repeat_times);
            }
        }
        previous_offset = *offset;
    }
    builder.finish()
}
