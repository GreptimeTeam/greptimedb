pub mod binary;
pub mod boolean;
pub mod null;
pub mod primitive;
mod string;

use std::any::Any;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::bitmap::Bitmap;
use arrow::datatypes::DataType as ArrowDataType;
pub use binary::*;
pub use boolean::*;
pub use null::*;
pub use primitive::*;
pub use string::*;

use crate::data_type::ConcreteDataType;
use crate::error::Result;
use crate::serialize::Serializable;
pub use crate::vectors::{
    BinaryVector, BooleanVector, Float32Vector, Float64Vector, Int16Vector, Int32Vector,
    Int64Vector, Int8Vector, NullVector, StringVector, UInt16Vector, UInt32Vector, UInt64Vector,
    UInt8Vector,
};

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
pub trait Vector: Send + Sync + Serializable {
    /// Returns the data type of the vector.
    ///
    /// This may require heap allocation.
    fn data_type(&self) -> ConcreteDataType;

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
}

pub type VectorRef = Arc<dyn Vector>;

/// Try to cast an arrow array into vector
///
/// # Panics
/// Panic if given arrow data type is not supported.
pub fn try_into_vector(array: ArrayRef) -> Result<VectorRef> {
    Ok(match array.data_type() {
        ArrowDataType::Null => Arc::new(NullVector::try_from_arrow_array(array)?),
        ArrowDataType::Boolean => Arc::new(BooleanVector::try_from_arrow_array(array)?),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => {
            Arc::new(BinaryVector::try_from_arrow_array(array)?)
        }
        ArrowDataType::Int8 => Arc::new(Int8Vector::try_from_arrow_array(array)?),
        ArrowDataType::Int16 => Arc::new(Int16Vector::try_from_arrow_array(array)?),
        ArrowDataType::Int32 => Arc::new(Int32Vector::try_from_arrow_array(array)?),
        ArrowDataType::Int64 => Arc::new(Int64Vector::try_from_arrow_array(array)?),
        ArrowDataType::UInt8 => Arc::new(UInt8Vector::try_from_arrow_array(array)?),
        ArrowDataType::UInt16 => Arc::new(UInt16Vector::try_from_arrow_array(array)?),
        ArrowDataType::UInt32 => Arc::new(UInt32Vector::try_from_arrow_array(array)?),
        ArrowDataType::UInt64 => Arc::new(UInt64Vector::try_from_arrow_array(array)?),
        ArrowDataType::Float32 => Arc::new(Float32Vector::try_from_arrow_array(array)?),
        ArrowDataType::Float64 => Arc::new(Float64Vector::try_from_arrow_array(array)?),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
            Arc::new(StringVector::try_from_arrow_array(array)?)
        }
        _ => unimplemented!(),
    })
}

/// Helper to define `try_from_arrow_array(array: arrow::array::ArrayRef)` function.
macro_rules! impl_try_from_arrow_array_for_vector {
    ($Array: ident, $Vector: ident) => {
        impl $Vector {
            pub fn try_from_arrow_array(
                array: arrow::array::ArrayRef,
            ) -> crate::error::Result<$Vector> {
                Ok($Vector::from(
                    array
                        .as_any()
                        .downcast_ref::<$Array>()
                        .with_context(|| crate::error::ConversionSnafu {
                            from: std::format!("{:?}", array.data_type()),
                        })?
                        .clone(),
                ))
            }
        }
    };
}

pub(crate) use impl_try_from_arrow_array_for_vector;

#[cfg(test)]
pub mod tests {
    use arrow::array::{Array, PrimitiveArray};
    use serde_json;

    use super::*;
    use crate::data_type::DataType;
    use crate::types::DataTypeBuilder;

    #[test]
    fn test_df_columns_to_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1, 2, 3]));
        let vector = try_into_vector(df_column).unwrap();
        assert_eq!(
            i32::build_data_type().as_arrow_type(),
            vector.data_type().as_arrow_type()
        );
    }

    #[test]
    fn test_serialize_i32_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::<i32>::from_slice(vec![1, 2, 3]));
        let json_value = try_into_vector(df_column)
            .unwrap()
            .serialize_to_json()
            .unwrap();
        assert_eq!("[1,2,3]", serde_json::to_string(&json_value).unwrap());
    }

    #[test]
    fn test_serialize_i8_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1u8, 2u8, 3u8]));
        let json_value = try_into_vector(df_column)
            .unwrap()
            .serialize_to_json()
            .unwrap();
        assert_eq!("[1,2,3]", serde_json::to_string(&json_value).unwrap());
    }
}
