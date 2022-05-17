pub mod binary;
pub mod primitive;

use std::any::Any;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
pub use binary::*;
use paste::paste;
pub use primitive::*;
use serde_json::Value;

use crate::data_type::DataTypeRef;
use crate::serialize::Serializable;
use crate::vectors::{
    Float32Vector, Float64Vector, Int16Vector, Int32Vector, Int64Vector, Int8Vector, UInt16Vector,
    UInt32Vector, UInt64Vector, UInt8Vector,
};

/// Vector of data values.
pub trait Vector: Send + Sync {
    /// Returns the data type of the vector.
    ///
    /// This may require heap allocation.
    fn data_type(&self) -> DataTypeRef;

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

pub trait TryIntoVector {
    fn try_into_vector(self) -> crate::error::Result<VectorRef>;
}

macro_rules! impl_try_into_vector_for_arrow_array {
    ( $($ty: expr),+ ) => {
        paste! {
            impl<A> TryIntoVector for A
where
    A: AsRef<dyn Array>,
{
    fn try_into_vector(self) -> Result<VectorRef, crate::error::Error> {
            match self.as_ref().data_type() {
                $(
                    DataType::$ty => Ok(Arc::new(<[<$ty Vector>]>::try_from_arrow_array(self.as_ref())?)),
                )+
                _ => {
                    unimplemented!()
                }
            }
        }}
        }
    }
}

macro_rules! impl_arrow_array_serialize {
    ( $($ty: expr),+ ) => {
        impl<A> Serializable for A
where
    A: AsRef<dyn Array> + Send + Sync,
{
        fn serialize_to_json(&self) -> crate::error::Result<Vec<Value>> {
            paste! {
                match self.as_ref().data_type() {
                    $(
                        DataType::$ty => <[<$ty Vector>]>::try_from_arrow_array(self.as_ref())?.serialize_to_json(),
                    )+
                    _ => {
                        unimplemented!()
                    }
                }
            }
        }
    }
    };
}

// todo(hl): implement more type to vector conversion
impl_try_into_vector_for_arrow_array![
    Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64
];

// todo(hl): implement serializations for more types
impl_arrow_array_serialize![
    Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64
];

#[cfg(test)]
mod tests {
    use arrow::array::{Array, PrimitiveArray};
    use serde::Serialize;

    use super::*;
    use crate::types::DataTypeBuilder;

    #[test]
    pub fn test_df_columns_to_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1, 2, 3]));
        let vector = df_column.try_into_vector().unwrap();
        assert_eq!(
            i32::build_data_type().as_arrow_type(),
            vector.data_type().as_arrow_type()
        );
    }

    #[test]
    pub fn test_serialize_i32_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1, 2, 3]));
        let json_value = df_column.serialize_to_json().unwrap();
        let mut output = vec![];
        let mut serializer = serde_json::ser::Serializer::new(&mut output);
        json_value.serialize(&mut serializer).unwrap();
        assert_eq!(b"[1,2,3]", output.as_slice());
    }

    #[test]
    pub fn test_serialize_i8_vector() {
        let df_column: Arc<dyn Array> = Arc::new(PrimitiveArray::from_slice(vec![1u8, 2u8, 3u8]));
        let json_value = df_column.serialize_to_json().unwrap();
        let mut output = vec![];
        let mut serializer = serde_json::ser::Serializer::new(&mut output);
        json_value.serialize(&mut serializer).unwrap();
        assert_eq!(b"[1,2,3]", output.as_slice());
    }
}
