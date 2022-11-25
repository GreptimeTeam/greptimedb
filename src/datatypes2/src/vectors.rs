use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::bitmap::Bitmap;
use snafu::ensure;

use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::serialize::Serializable;
use crate::value::{Value, ValueRef};
use crate::vectors::operations::VectorOp;

pub mod binary;
pub mod boolean;
pub mod operations;
pub mod primitive;

pub use binary::*;
pub use boolean::*;
pub use primitive::*;

// TODO(yingwen): We need to reimplement Validity as arrow's Bitmap doesn't support null_count().
#[derive(Debug, PartialEq)]
pub enum Validity<'a> {
    /// Whether the array slot is valid or not (null).
    Slots(&'a Bitmap),
    /// All slots are valid.
    AllValid,
    /// All slots are null.
    AllNull,
}

impl<'a> Validity<'a> {
    pub fn slots(&self) -> Option<&Bitmap> {
        match self {
            Validity::Slots(bitmap) => Some(bitmap),
            _ => None,
        }
    }

    /// Returns whether `i-th` bit is set.
    pub fn is_set(&self, i: usize) -> bool {
        match self {
            Validity::Slots(bitmap) => bitmap.is_set(i),
            Validity::AllValid => true,
            Validity::AllNull => false,
        }
    }
}

/// Vector of data values.
pub trait Vector: Send + Sync + Serializable + Debug + VectorOp {
    /// Returns the data type of the vector.
    ///
    /// This may require heap allocation.
    fn data_type(&self) -> ConcreteDataType;

    fn vector_type_name(&self) -> String;

    /// Returns the vector as [Any](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns number of elements in the vector.
    fn len(&self) -> usize;

    /// Returns whether the vector is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert this vector to a new arrow [ArrayRef].
    fn to_arrow_array(&self) -> ArrayRef;

    /// Convert this vector to a new boxed arrow [Array].
    fn to_boxed_arrow_array(&self) -> Box<dyn Array>;

    /// Returns the validity of the Array.
    fn validity(&self) -> Validity;

    /// Returns the memory size of vector.
    fn memory_size(&self) -> usize;

    /// The number of null slots on this [`Vector`].
    /// # Implementation
    /// This is `O(1)`.
    fn null_count(&self) -> usize;

    /// Returns true when it's a ConstantColumn
    fn is_const(&self) -> bool {
        false
    }

    /// Returns whether row is null.
    fn is_null(&self, row: usize) -> bool;

    /// If the only value vector can contain is NULL.
    fn only_null(&self) -> bool {
        self.null_count() == self.len()
    }

    /// Slices the `Vector`, returning a new `VectorRef`.
    ///
    /// # Panics
    /// This function panics if `offset + length > self.len()`.
    fn slice(&self, offset: usize, length: usize) -> VectorRef;

    /// Returns the clone of value at `index`.
    ///
    /// # Panics
    /// Panic if `index` is out of bound.
    fn get(&self, index: usize) -> Value;

    /// Returns the clone of value at `index` or error if `index`
    /// is out of bound.
    fn try_get(&self, index: usize) -> Result<Value> {
        ensure!(
            index < self.len(),
            error::BadArrayAccessSnafu {
                index,
                size: self.len()
            }
        );
        Ok(self.get(index))
    }

    /// Returns the reference of value at `index`.
    ///
    /// # Panics
    /// Panic if `index` is out of bound.
    fn get_ref(&self, index: usize) -> ValueRef;
}

pub type VectorRef = Arc<dyn Vector>;

/// Mutable vector that could be used to build an immutable vector.
pub trait MutableVector: Send + Sync {
    /// Returns the data type of the vector.
    fn data_type(&self) -> ConcreteDataType;

    /// Returns the length of the vector.
    fn len(&self) -> usize;

    /// Returns whether the vector is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert to Any, to enable dynamic casting.
    fn as_any(&self) -> &dyn Any;

    /// Convert to mutable Any, to enable dynamic casting.
    fn as_mut_any(&mut self) -> &mut dyn Any;

    /// Convert `self` to an (immutable) [VectorRef] and reset `self`.
    fn to_vector(&mut self) -> VectorRef;

    /// Push value ref to this mutable vector.
    ///
    /// Returns error if data type unmatch.
    fn push_value_ref(&mut self, value: ValueRef) -> Result<()>;

    /// Extend this mutable vector by slice of `vector`.
    ///
    /// Returns error if data type unmatch.
    ///
    /// # Panics
    /// Panics if `offset + length > vector.len()`.
    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()>;
}

/// Helper to define `try_from_arrow_array(array: arrow::array::ArrayRef)` function.
macro_rules! impl_try_from_arrow_array_for_vector {
    ($Array: ident, $Vector: ident) => {
        impl $Vector {
            pub fn try_from_arrow_array(
                array: impl AsRef<dyn arrow::array::Array>,
            ) -> crate::error::Result<$Vector> {
                let data = array.as_ref().data().clone();
                // TODO(yingwen): Should we check the array type?
                let concrete_array = $Array::from(data);
                Ok($Vector::from(concrete_array))

                // The original implementation using arrow2 checks array type:
                // Ok($Vector::from(
                //     array
                //         .as_ref()
                //         .as_any()
                //         .downcast_ref::<$Array>()
                //         .with_context(|| crate::error::ConversionSnafu {
                //             from: std::format!("{:?}", array.as_ref().data_type()),
                //         })?
                //         .clone(),
                // ))
            }
        }
    };
}

macro_rules! impl_validity_for_vector {
    ($array: expr) => {
        match $array.data().null_bitmap() {
            Some(bitmap) => Validity::Slots(bitmap),
            None => Validity::AllValid,
        }
    };
}

macro_rules! impl_get_for_vector {
    ($array: expr, $index: ident) => {
        if $array.is_valid($index) {
            // Safety: The index have been checked by `is_valid()`.
            unsafe { $array.value_unchecked($index).into() }
        } else {
            Value::Null
        }
    };
}

macro_rules! impl_get_ref_for_vector {
    ($array: expr, $index: ident) => {
        if $array.is_valid($index) {
            // Safety: The index have been checked by `is_valid()`.
            unsafe { $array.value_unchecked($index).into() }
        } else {
            ValueRef::Null
        }
    };
}

macro_rules! impl_extend_for_builder {
    ($mutable_vector: expr, $vector: ident, $VectorType: ident, $offset: ident, $length: ident) => {{
        use snafu::OptionExt;

        let sliced_vector = $vector.slice($offset, $length);
        let concrete_vector = sliced_vector
            .as_any()
            .downcast_ref::<$VectorType>()
            .with_context(|| crate::error::CastTypeSnafu {
                msg: format!(
                    "Failed to cast vector from {} to {}",
                    $vector.vector_type_name(),
                    stringify!($VectorType)
                ),
            })?;
        for value in concrete_vector.iter_data() {
            $mutable_vector.push(value);
        }
        Ok(())
    }};
}

pub(crate) use {
    impl_extend_for_builder, impl_get_for_vector, impl_get_ref_for_vector,
    impl_try_from_arrow_array_for_vector, impl_validity_for_vector,
};

// TODO(yingwen): Make this compile.
// #[cfg(test)]
// pub mod tests {
//     use arrow::array::{Array, PrimitiveArray};
//     use serde_json;

//     use super::helper::Helper;
//     use super::*;
//     use crate::data_type::DataType;
//     use crate::types::PrimitiveElement;

//     #[test]
//     fn test_df_columns_to_vector() {
//         let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1, 2, 3]));
//         let vector = Helper::try_into_vector(df_column).unwrap();
//         assert_eq!(
//             i32::build_data_type().as_arrow_type(),
//             vector.data_type().as_arrow_type()
//         );
//     }

//     #[test]
//     fn test_serialize_i32_vector() {
//         let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::<i32>::from_slice(vec![1, 2, 3]));
//         let json_value = Helper::try_into_vector(df_column)
//             .unwrap()
//             .serialize_to_json()
//             .unwrap();
//         assert_eq!("[1,2,3]", serde_json::to_string(&json_value).unwrap());
//     }

//     #[test]
//     fn test_serialize_i8_vector() {
//         let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1u8, 2u8, 3u8]));
//         let json_value = Helper::try_into_vector(df_column)
//             .unwrap()
//             .serialize_to_json()
//             .unwrap();
//         assert_eq!("[1,2,3]", serde_json::to_string(&json_value).unwrap());
//     }
// }
