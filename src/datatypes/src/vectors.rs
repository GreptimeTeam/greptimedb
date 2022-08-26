pub mod binary;
pub mod boolean;
mod builder;
pub mod constant;
pub mod date;
pub mod datetime;
mod eq;
mod helper;
mod list;
pub mod mutable;
pub mod null;
pub mod primitive;
mod string;

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::bitmap::Bitmap;
pub use binary::*;
pub use boolean::*;
pub use builder::VectorBuilder;
pub use constant::*;
pub use date::*;
pub use datetime::*;
pub use helper::Helper;
pub use list::*;
pub use mutable::MutableVector;
pub use null::*;
pub use primitive::*;
use snafu::ensure;
pub use string::*;

use crate::data_type::ConcreteDataType;
use crate::error::{BadArrayAccessSnafu, Result};
use crate::serialize::Serializable;
use crate::value::Value;

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
}

/// Vector of data values.
pub trait Vector: Send + Sync + Serializable + Debug {
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

    /// Returns the validity of the Array.
    fn validity(&self) -> Validity;

    /// Returns the memory size of vector.
    fn memory_size(&self) -> usize;

    /// The number of null slots on this [`Vector`].
    /// # Implementation
    /// This is `O(1)`.
    fn null_count(&self) -> usize {
        match self.validity() {
            Validity::Slots(bitmap) => bitmap.null_count(),
            Validity::AllValid => 0,
            Validity::AllNull => self.len(),
        }
    }

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
            BadArrayAccessSnafu {
                index,
                size: self.len()
            }
        );
        Ok(self.get(index))
    }

    // Copies each element according offsets parameter.
    // (i-th element should be copied offsets[i] - offsets[i - 1] times.)
    fn replicate(&self, offsets: &[usize]) -> VectorRef;
}

pub type VectorRef = Arc<dyn Vector>;

/// Helper to define `try_from_arrow_array(array: arrow::array::ArrayRef)` function.
macro_rules! impl_try_from_arrow_array_for_vector {
    ($Array: ident, $Vector: ident) => {
        impl $Vector {
            pub fn try_from_arrow_array(
                array: impl AsRef<dyn arrow::array::Array>,
            ) -> crate::error::Result<$Vector> {
                Ok($Vector::from(
                    array
                        .as_ref()
                        .as_any()
                        .downcast_ref::<$Array>()
                        .with_context(|| crate::error::ConversionSnafu {
                            from: std::format!("{:?}", array.as_ref().data_type()),
                        })?
                        .clone(),
                ))
            }
        }
    };
}

macro_rules! impl_validity_for_vector {
    ($array: expr) => {
        match $array.validity() {
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

pub(crate) use {
    impl_get_for_vector, impl_try_from_arrow_array_for_vector, impl_validity_for_vector,
};

#[cfg(test)]
pub mod tests {
    use arrow::array::{Array, PrimitiveArray};
    use serde_json;

    use super::helper::Helper;
    use super::*;
    use crate::data_type::DataType;
    use crate::types::DataTypeBuilder;

    #[test]
    fn test_df_columns_to_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1, 2, 3]));
        let vector = Helper::try_into_vector(df_column).unwrap();
        assert_eq!(
            i32::build_data_type().as_arrow_type(),
            vector.data_type().as_arrow_type()
        );
    }

    #[test]
    fn test_serialize_i32_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::<i32>::from_slice(vec![1, 2, 3]));
        let json_value = Helper::try_into_vector(df_column)
            .unwrap()
            .serialize_to_json()
            .unwrap();
        assert_eq!("[1,2,3]", serde_json::to_string(&json_value).unwrap());
    }

    #[test]
    fn test_serialize_i8_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1u8, 2u8, 3u8]));
        let json_value = Helper::try_into_vector(df_column)
            .unwrap()
            .serialize_to_json()
            .unwrap();
        assert_eq!("[1,2,3]", serde_json::to_string(&json_value).unwrap());
    }
}
