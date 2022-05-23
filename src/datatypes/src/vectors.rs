pub mod binary;
pub mod boolean;
pub mod null;
pub mod primitive;
mod string;

use std::any::Any;
use std::sync::Arc;

use arrow::array::ArrayRef;
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
mod tests {
    use arrow::array::{Array, PrimitiveArray};
    use serde::Serialize;

    use super::*;
    use crate::data_type::DataType;
    use crate::types::DataTypeBuilder;

    #[test]
    pub fn test_df_columns_to_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1, 2, 3]));
        let vector = try_into_vector(df_column).unwrap();
        assert_eq!(
            i32::build_data_type().as_arrow_type(),
            vector.data_type().as_arrow_type()
        );
    }

    #[test]
    pub fn test_serialize_i32_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::<i32>::from_slice(vec![1, 2, 3]));
        let json_value = try_into_vector(df_column)
            .unwrap()
            .serialize_to_json()
            .unwrap();
        let mut output = vec![];
        let mut serializer = serde_json::ser::Serializer::new(&mut output);
        json_value.serialize(&mut serializer).unwrap();
        assert_eq!(b"[1,2,3]", output.as_slice());
    }

    #[test]
    pub fn test_serialize_i8_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1u8, 2u8, 3u8]));
        let json_value = try_into_vector(df_column)
            .unwrap()
            .serialize_to_json()
            .unwrap();
        let mut output = vec![];
        let mut serializer = serde_json::ser::Serializer::new(&mut output);
        json_value.serialize(&mut serializer).unwrap();
        assert_eq!(b"[1,2,3]", output.as_slice());
    }
}
