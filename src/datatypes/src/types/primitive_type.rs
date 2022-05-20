use std::marker::PhantomData;

use arrow::datatypes::DataType as ArrowDataType;
use paste::paste;

use crate::data_type::{ConcreteDataType, DataType};
use crate::type_id::LogicalTypeId;
use crate::types::primitive_traits::Primitive;
use crate::value::Value;

#[derive(Clone)]
pub struct PrimitiveType<T: Primitive> {
    _phantom: PhantomData<T>,
}

/// Create a new [ConcreteDataType] from a primitive type.
pub trait DataTypeBuilder {
    fn build_data_type() -> ConcreteDataType;
}

macro_rules! impl_build_data_type {
    ($Type:ident, $TypeId:ident) => {
        paste::paste! {
            impl DataTypeBuilder for $Type {
                fn build_data_type() -> ConcreteDataType {
                    ConcreteDataType::$TypeId(PrimitiveType::<$Type>::default())
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
