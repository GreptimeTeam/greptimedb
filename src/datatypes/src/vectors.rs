pub mod binary;
pub mod boolean;
pub mod constant;
mod helper;
pub mod mutable;
pub mod null;
pub mod primitive;
mod string;

use std::any::Any;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::bitmap::Bitmap;
pub use binary::*;
pub use boolean::*;
pub use constant::*;
pub use helper::Helper;
pub use mutable::MutableVector;
pub use null::*;
pub use primitive::*;
pub use string::*;

use crate::data_type::ConcreteDataType;
use crate::error::{BadArrayAccessSnafu, Result};
use crate::serialize::Serializable;
use crate::value::Value;
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

    /// Returns true when it's a ConstantColumn
    fn is_const(&self) -> bool {
        false
    }

    /// Returns whether row is null.
    fn is_null(&self, _row: usize) -> bool {
        false
    }

    /// If the only value vector can contain is NULL.
    fn only_null(&self) -> bool {
        false
    }

    fn slice(&self, offset: usize, length: usize) -> VectorRef;

    /// # Safety
    /// Assumes that the `index` is smaller than size.
    fn get(&self, index: usize) -> Value;

    fn get_checked(&self, index: usize) -> Result<Value> {
        if index > self.len() {
            return BadArrayAccessSnafu {
                msg: format!("Index out of bounds: {}, col size: {}", index, self.len())
                    .to_string(),
            }
            .fail();
        }
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
