//! Vector helper functions, inspired by databend Series mod

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use datafusion_common::ScalarValue;

use crate::error::Result;
use crate::vectors::*;

pub struct Helper;

impl Helper {
    /// Get a pointer to the underlying data of this columns.
    /// Can be useful for fast comparisons.
    /// # Safety
    /// Assumes that the `column` is  T.
    pub unsafe fn static_cast<T: Any>(column: &VectorRef) -> &T {
        let object = column.as_ref();
        debug_assert!(object.as_any().is::<T>());
        &*(object as *const dyn Vector as *const T)
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
            _ => unimplemented!("FIXME: return error result"),
        };

        Ok(Arc::new(vector))
    }

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
}
