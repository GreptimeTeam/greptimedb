use std::any::TypeId;
use std::marker::PhantomData;

use arrow::datatypes::DataType as ArrowDataType;
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::data_type::{ConcreteDataType, DataType};
use crate::type_id::LogicalTypeId;
use crate::types::primitive_traits::Primitive;
use crate::value::Value;

#[derive(Clone, Serialize, Deserialize)]
pub struct PrimitiveType<T: Primitive> {
    #[serde(skip)]
    _phantom: PhantomData<T>,
}

impl<T: Primitive, U: Primitive> PartialEq<PrimitiveType<U>> for PrimitiveType<T> {
    fn eq(&self, _other: &PrimitiveType<U>) -> bool {
        TypeId::of::<T>() == TypeId::of::<U>()
    }
}

impl<T: Primitive> Eq for PrimitiveType<T> {}

/// Create a new [ConcreteDataType] from a primitive type.
pub trait DataTypeBuilder {
    fn build_data_type() -> ConcreteDataType;
    fn type_name() -> String;
}

macro_rules! impl_build_data_type {
    ($Type:ident, $TypeId:ident) => {
        paste::paste! {
            impl DataTypeBuilder for $Type {
                fn build_data_type() -> ConcreteDataType {
                    ConcreteDataType::$TypeId(PrimitiveType::<$Type>::default())
                }
                fn type_name() -> String {
                    stringify!($TypeId).to_string()
                }
            }
        }
    };
}

macro_rules! impl_numeric {
    ($Type:ident, $TypeId:ident) => {
        impl DataType for PrimitiveType<$Type> {
            fn name(&self) -> &str {
                stringify!($TypeId)
            }

            fn logical_type_id(&self) -> LogicalTypeId {
                LogicalTypeId::$TypeId
            }

            fn default_value(&self) -> Value {
                $Type::default().into()
            }

            fn as_arrow_type(&self) -> ArrowDataType {
                ArrowDataType::$TypeId
            }
        }

        impl std::fmt::Debug for PrimitiveType<$Type> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}", self.name())
            }
        }

        impl Default for PrimitiveType<$Type> {
            fn default() -> Self {
                Self {
                    _phantom: PhantomData,
                }
            }
        }

        impl_build_data_type!($Type, $TypeId);

        paste! {
            pub type [<$TypeId Type>]=PrimitiveType<$Type>;
        }
    };
}

impl_numeric!(u8, UInt8);
impl_numeric!(u16, UInt16);
impl_numeric!(u32, UInt32);
impl_numeric!(u64, UInt64);
impl_numeric!(i8, Int8);
impl_numeric!(i16, Int16);
impl_numeric!(i32, Int32);
impl_numeric!(i64, Int64);
impl_numeric!(f32, Float32);
impl_numeric!(f64, Float64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq() {
        assert_eq!(UInt8Type::default(), UInt8Type::default());
        assert_eq!(UInt16Type::default(), UInt16Type::default());
        assert_eq!(UInt32Type::default(), UInt32Type::default());
        assert_eq!(UInt64Type::default(), UInt64Type::default());
        assert_eq!(Int8Type::default(), Int8Type::default());
        assert_eq!(Int16Type::default(), Int16Type::default());
        assert_eq!(Int32Type::default(), Int32Type::default());
        assert_eq!(Int64Type::default(), Int64Type::default());
        assert_eq!(Float32Type::default(), Float32Type::default());
        assert_eq!(Float64Type::default(), Float64Type::default());

        assert_ne!(Float32Type::default(), Float64Type::default());
        assert_ne!(Float32Type::default(), Int32Type::default());
    }
}
