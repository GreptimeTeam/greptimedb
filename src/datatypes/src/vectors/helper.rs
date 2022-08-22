//! Vector helper functions, inspired by databend Series mod

use std::any::Any;
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType as ArrowDataType;
use datafusion_common::ScalarValue;
use snafu::OptionExt;

use crate::error::{ConversionSnafu, Result, UnknownVectorSnafu};
use crate::scalars::*;
use crate::vectors::date::DateVector;
use crate::vectors::*;

pub struct Helper;

impl Helper {
    /// Get a pointer to the underlying data of this vectors.
    /// Can be useful for fast comparisons.
    /// # Safety
    /// Assumes that the `vector` is  T.
    pub unsafe fn static_cast<T: Any>(vector: &VectorRef) -> &T {
        let object = vector.as_ref();
        debug_assert!(object.as_any().is::<T>());
        &*(object as *const dyn Vector as *const T)
    }

    pub fn check_get_scalar<T: Scalar>(vector: &VectorRef) -> Result<&<T as Scalar>::VectorType> {
        let arr = vector
            .as_any()
            .downcast_ref::<<T as Scalar>::VectorType>()
            .with_context(|| UnknownVectorSnafu {
                msg: format!(
                    "downcast vector error, vector type: {:?}, expected vector: {:?}",
                    vector.vector_type_name(),
                    std::any::type_name::<T>(),
                ),
            });
        arr
    }

    pub fn check_get<T: 'static + Vector>(vector: &VectorRef) -> Result<&T> {
        let arr = vector
            .as_any()
            .downcast_ref::<T>()
            .with_context(|| UnknownVectorSnafu {
                msg: format!(
                    "downcast vector error, vector type: {:?}, expected vector: {:?}",
                    vector.vector_type_name(),
                    std::any::type_name::<T>(),
                ),
            });
        arr
    }

    pub fn check_get_mutable_vector<T: 'static + MutableVector>(
        vector: &mut dyn MutableVector,
    ) -> Result<&mut T> {
        let ty = vector.data_type();
        let arr = vector
            .as_mut_any()
            .downcast_mut()
            .with_context(|| UnknownVectorSnafu {
                msg: format!(
                    "downcast vector error, vector type: {:?}, expected vector: {:?}",
                    ty,
                    std::any::type_name::<T>(),
                ),
            });
        arr
    }

    pub fn check_get_scalar_vector<T: Scalar>(
        vector: &VectorRef,
    ) -> Result<&<T as Scalar>::VectorType> {
        let arr = vector
            .as_any()
            .downcast_ref::<<T as Scalar>::VectorType>()
            .with_context(|| UnknownVectorSnafu {
                msg: format!(
                    "downcast vector error, vector type: {:?}, expected vector: {:?}",
                    vector.vector_type_name(),
                    std::any::type_name::<T>(),
                ),
            });
        arr
    }

    /// Try to cast an arrow scalar value into vector
    ///
    /// # Panics
    /// Panic if given scalar value is not supported.
    pub fn try_from_scalar_value(value: ScalarValue, length: usize) -> Result<VectorRef> {
        let vector = match value {
            ScalarValue::Boolean(v) => {
                ConstantVector::new(Arc::new(BooleanVector::from(vec![v])), length)
            }
            ScalarValue::Float32(v) => {
                ConstantVector::new(Arc::new(Float32Vector::from(vec![v])), length)
            }
            ScalarValue::Float64(v) => {
                ConstantVector::new(Arc::new(Float64Vector::from(vec![v])), length)
            }
            ScalarValue::Int8(v) => {
                ConstantVector::new(Arc::new(Int8Vector::from(vec![v])), length)
            }
            ScalarValue::Int16(v) => {
                ConstantVector::new(Arc::new(Int16Vector::from(vec![v])), length)
            }
            ScalarValue::Int32(v) => {
                ConstantVector::new(Arc::new(Int32Vector::from(vec![v])), length)
            }
            ScalarValue::Int64(v) => {
                ConstantVector::new(Arc::new(Int64Vector::from(vec![v])), length)
            }
            ScalarValue::UInt8(v) => {
                ConstantVector::new(Arc::new(UInt8Vector::from(vec![v])), length)
            }
            ScalarValue::UInt16(v) => {
                ConstantVector::new(Arc::new(UInt16Vector::from(vec![v])), length)
            }
            ScalarValue::UInt32(v) => {
                ConstantVector::new(Arc::new(UInt32Vector::from(vec![v])), length)
            }
            ScalarValue::UInt64(v) => {
                ConstantVector::new(Arc::new(UInt64Vector::from(vec![v])), length)
            }
            ScalarValue::Utf8(v) => {
                ConstantVector::new(Arc::new(StringVector::from(vec![v])), length)
            }
            ScalarValue::LargeUtf8(v) => {
                ConstantVector::new(Arc::new(StringVector::from(vec![v])), length)
            }
            ScalarValue::Binary(v) => {
                ConstantVector::new(Arc::new(BinaryVector::from(vec![v])), length)
            }
            ScalarValue::LargeBinary(v) => {
                ConstantVector::new(Arc::new(BinaryVector::from(vec![v])), length)
            }
            _ => {
                return ConversionSnafu {
                    from: format!("Unsupported scalar value: {}", value),
                }
                .fail()
            }
        };

        Ok(Arc::new(vector))
    }

    /// Try to cast an arrow array into vector
    ///
    /// # Panics
    /// Panic if given arrow data type is not supported.
    pub fn try_into_vector(array: impl AsRef<dyn Array>) -> Result<VectorRef> {
        Ok(match array.as_ref().data_type() {
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
            ArrowDataType::Date32 => Arc::new(DateVector::try_from_arrow_array(array)?),
            ArrowDataType::List(_) => Arc::new(ListVector::try_from_arrow_array(array)?),
            _ => unimplemented!("Arrow array datatype: {:?}", array.as_ref().data_type()),
        })
    }

    pub fn try_into_vectors(arrays: &[ArrayRef]) -> Result<Vec<VectorRef>> {
        arrays.iter().map(Self::try_into_vector).collect()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;

    use super::*;

    #[test]
    fn test_try_into_vectors() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from_vec(vec![1])),
            Arc::new(Int32Array::from_vec(vec![2])),
            Arc::new(Int32Array::from_vec(vec![3])),
        ];
        let vectors = Helper::try_into_vectors(&arrays);
        assert!(vectors.is_ok());
        let vectors = vectors.unwrap();
        vectors.iter().for_each(|v| assert_eq!(1, v.len()));
        assert_eq!(Value::Int32(1), vectors[0].get(0));
        assert_eq!(Value::Int32(2), vectors[1].get(0));
        assert_eq!(Value::Int32(3), vectors[2].get(0));
    }
}
