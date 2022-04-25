use std::marker::PhantomData;
use std::sync::Arc;

use arrow2::datatypes::DataType as ArrowDataType;

use crate::data_type::{DataType, DataTypeRef};
use crate::type_id::LogicalTypeId;
use crate::types::primitive_traits::Primitive;
use crate::value::Value;

pub struct PrimitiveType<T: Primitive> {
    _phantom: PhantomData<T>,
}

impl<T: Primitive> PrimitiveType<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

/// Create a new [DataTypeRef] from a primitive type.
pub trait CreateDataType {
    fn create_data_type() -> DataTypeRef;
}

macro_rules! impl_create_data_type {
    ($Type:ident) => {
        paste::paste! {
            impl CreateDataType for $Type {
                fn create_data_type() -> DataTypeRef {
                    Arc::new(PrimitiveType::<$Type>::new())
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

        impl_create_data_type!($Type);
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
