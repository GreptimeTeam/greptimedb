// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::TypeId;
use std::marker::PhantomData;

use arrow::array::PrimitiveArray;
use arrow::datatypes::DataType as ArrowDataType;
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Result};
use crate::scalars::{Scalar, ScalarRef, ScalarVectorBuilder};
use crate::type_id::LogicalTypeId;
use crate::types::primitive_traits::Primitive;
use crate::value::{Value, ValueRef};
use crate::vectors::{MutableVector, PrimitiveVector, PrimitiveVectorBuilder, Vector};

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

/// A trait that provide helper methods for a primitive type to implementing the [PrimitiveVector].
pub trait PrimitiveElement
where
    for<'a> Self: Primitive
        + Scalar<VectorType = PrimitiveVector<Self>>
        + ScalarRef<'a, ScalarType = Self, VectorType = PrimitiveVector<Self>>
        + Scalar<RefType<'a> = Self>,
{
    /// Construct the data type struct.
    fn build_data_type() -> ConcreteDataType;

    /// Returns the name of the type id.
    fn type_name() -> String;

    /// Dynamic cast the vector to the concrete vector type.
    fn cast_vector(vector: &dyn Vector) -> Result<&PrimitiveArray<Self>>;

    /// Cast value ref to the primitive type.
    fn cast_value_ref(value: ValueRef) -> Result<Option<Self>>;
}

macro_rules! impl_primitive_element {
    ($Type:ident, $TypeId:ident) => {
        paste::paste! {
            impl PrimitiveElement for $Type {
                fn build_data_type() -> ConcreteDataType {
                    ConcreteDataType::$TypeId(PrimitiveType::<$Type>::default())
                }

                fn type_name() -> String {
                    stringify!($TypeId).to_string()
                }

                fn cast_vector(vector: &dyn Vector) -> Result<&PrimitiveArray<$Type>> {
                    let primitive_vector = vector
                        .as_any()
                        .downcast_ref::<PrimitiveVector<$Type>>()
                        .with_context(|| error::CastTypeSnafu {
                            msg: format!(
                                "Failed to cast {} to vector of primitive type {}",
                                vector.vector_type_name(),
                                stringify!($TypeId)
                            ),
                        })?;
                    Ok(&primitive_vector.array)
                }

                fn cast_value_ref(value: ValueRef) -> Result<Option<Self>> {
                    match value {
                        ValueRef::Null => Ok(None),
                        ValueRef::$TypeId(v) => Ok(Some(v.into())),
                        other => error::CastTypeSnafu {
                            msg: format!(
                                "Failed to cast value {:?} to primitive type {}",
                                other,
                                stringify!($TypeId),
                            ),
                        }.fail(),
                    }
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

            fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
                Box::new(PrimitiveVectorBuilder::<$Type>::with_capacity(capacity))
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

        impl_primitive_element!($Type, $TypeId);

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
