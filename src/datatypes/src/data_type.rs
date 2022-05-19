use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;

use crate::type_id::LogicalTypeId;
use crate::types::{
    BinaryType, BooleanType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    NullType, StringType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use crate::value::Value;

#[derive(Clone, Debug)]
#[enum_dispatch::enum_dispatch(DataType)]
pub enum ConcretDataType {
    Null(NullType),
    Boolean(BooleanType),

    // Numeric types:
    Int8(Int8Type),
    Int16(Int16Type),
    Int32(Int32Type),
    Int64(Int64Type),
    UInt8(UInt8Type),
    UInt16(UInt16Type),
    UInt32(UInt32Type),
    UInt64(UInt64Type),
    Float32(Float32Type),
    Float64(Float64Type),

    // String types
    Binary(BinaryType),
    String(StringType),
}

impl ConcretDataType {
    pub fn from_arrow_type(dt: &ArrowDataType) -> Self {
        match dt {
            ArrowDataType::Null => ConcretDataType::Null(NullType::default()),
            ArrowDataType::Boolean => ConcretDataType::Boolean(BooleanType::default()),
            ArrowDataType::Binary | ArrowDataType::LargeBinary => {
                ConcretDataType::Binary(BinaryType::default())
            }
            ArrowDataType::UInt8 => ConcretDataType::UInt8(UInt8Type::default()),
            ArrowDataType::UInt16 => ConcretDataType::UInt16(UInt16Type::default()),
            ArrowDataType::UInt32 => ConcretDataType::UInt32(UInt32Type::default()),
            ArrowDataType::UInt64 => ConcretDataType::UInt64(UInt64Type::default()),
            ArrowDataType::Int8 => ConcretDataType::Int8(Int8Type::default()),
            ArrowDataType::Int16 => ConcretDataType::Int16(Int16Type::default()),
            ArrowDataType::Int32 => ConcretDataType::Int32(Int32Type::default()),
            ArrowDataType::Int64 => ConcretDataType::Int64(Int64Type::default()),
            ArrowDataType::Float32 => ConcretDataType::Float32(Float32Type::default()),
            ArrowDataType::Float64 => ConcretDataType::Float64(Float64Type::default()),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
                ConcretDataType::String(StringType::default())
            }

            // this is safe, because we define the datatype firstly
            _ => {
                unimplemented!("arrow data_type: {:?}", dt)
            }
        }
    }
}

/// Data type abstraction.
pub trait DataType: std::fmt::Debug + Send + Sync {
    /// Name of this data type.
    fn name(&self) -> &str;

    /// Returns id of the Logical data type.
    fn logical_type_id(&self) -> LogicalTypeId;

    /// Returns the default value of this type.
    fn default_value(&self) -> Value;

    /// Convert this type as [arrow2::datatypes::DataType].
    fn as_arrow_type(&self) -> ArrowDataType;
}

pub type DataTypeRef = Arc<dyn DataType>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_arrow_type() {
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::Null),
            ConcretDataType::Null(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::Boolean),
            ConcretDataType::Boolean(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::Binary),
            ConcretDataType::Binary(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::LargeBinary),
            ConcretDataType::Binary(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::Int8),
            ConcretDataType::Int8(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::Int16),
            ConcretDataType::Int16(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::Int32),
            ConcretDataType::Int32(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::Int64),
            ConcretDataType::Int64(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::UInt8),
            ConcretDataType::UInt8(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::UInt16),
            ConcretDataType::UInt16(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::UInt32),
            ConcretDataType::UInt32(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::UInt64),
            ConcretDataType::UInt64(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::Float32),
            ConcretDataType::Float32(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::Float64),
            ConcretDataType::Float64(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::Utf8),
            ConcretDataType::String(_)
        ));
        assert!(matches!(
            ConcretDataType::from_arrow_type(&ArrowDataType::LargeUtf8),
            ConcretDataType::String(_)
        ));
    }
}
