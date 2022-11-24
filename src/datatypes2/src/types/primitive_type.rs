use std::cmp::Ordering;

use arrow::datatypes::{ArrowNativeType, ArrowPrimitiveType, DataType as ArrowDataType};
use num::NumCast;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Result};
use crate::scalars::{Scalar, ScalarRef, ScalarVectorBuilder};
use crate::type_id::LogicalTypeId;
use crate::value::{IntoValueRef, Value, ValueRef};
use crate::vectors::{MutableVector, PrimitiveVector, PrimitiveVectorBuilder, Vector};

/// Data types that can be used as arrow's native type.
pub trait NativeType:
    ArrowNativeType + Into<Value> + IntoValueRef<'static> + Serialize + NumCast + Scalar
{
    /// Largest numeric type this primitive type can be cast to.
    type LargestType: NativeType;
}

macro_rules! impl_native_type {
    ($Type: ident, $LargestType: ident) => {
        impl NativeType for $Type {
            type LargestType = $LargestType;
        }
    };
}

impl_native_type!(u8, u64);
impl_native_type!(u16, u64);
impl_native_type!(u32, u64);
impl_native_type!(u64, u64);
impl_native_type!(i8, i64);
impl_native_type!(i16, i64);
impl_native_type!(i32, i64);
impl_native_type!(i64, i64);
impl_native_type!(f32, f64);
impl_native_type!(f64, f64);

pub trait WrapperType: Copy + Scalar + PartialEq {
    type LogicalType: LogicalPrimitiveType<Wrapper = Self, Native = Self::Native>;
    type Native;

    fn from_native(value: Self::Native) -> Self;

    fn into_native(self) -> Self::Native;
}

macro_rules! impl_wrapper {
    ($Type: ident, $LogicalType: ident) => {
        impl WrapperType for $Type {
            type LogicalType = $LogicalType;
            type Native = $Type;

            fn from_native(value: Self::Native) -> Self {
                value
            }

            fn into_native(self) -> Self::Native {
                self
            }
        }
    };
}

impl_wrapper!(u8, UInt8Type);
impl_wrapper!(u16, UInt16Type);
impl_wrapper!(u32, UInt32Type);
impl_wrapper!(u64, UInt64Type);
impl_wrapper!(i8, Int8Type);
impl_wrapper!(i16, Int16Type);
impl_wrapper!(i32, Int32Type);
impl_wrapper!(i64, Int64Type);
impl_wrapper!(f32, Float32Type);
impl_wrapper!(f64, Float64Type);

/// Trait bridging the logcial primitive type with [ArrowPrimitiveType].
pub trait LogicalPrimitiveType: 'static + Sized {
    /// Arrow primitive type of this logical type.
    type ArrowPrimitive: ArrowPrimitiveType<Native = Self::Native>;
    /// Native (physical) type of this logical type.
    type Native: NativeType;
    type Wrapper: WrapperType<LogicalType = Self, Native = Self::Native>
        + for<'a> Scalar<VectorType = PrimitiveVector<Self>, RefType<'a> = Self::Wrapper>
        + for<'a> ScalarRef<'a, ScalarType = Self::Wrapper, VectorType = PrimitiveVector<Self>>;

    /// Construct the data type struct.
    fn build_data_type() -> ConcreteDataType;

    /// Returns the name of the type.
    fn type_name() -> String;

    /// Dynamic cast the vector to the concrete vector type.
    fn cast_vector(vector: &dyn Vector) -> Result<&PrimitiveVector<Self>>;

    /// Cast value ref to the primitive type.
    fn cast_value_ref(value: ValueRef) -> Result<Option<Self::Wrapper>>;
}

macro_rules! define_logical_primitive_type {
    ($Native: ident, $TypeId: ident, $DataType: ident) => {
        #[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
        pub struct $DataType {}

        impl LogicalPrimitiveType for $DataType {
            type ArrowPrimitive = arrow::datatypes::$DataType;
            type Native = $Native;
            type Wrapper = $Native;

            fn build_data_type() -> ConcreteDataType {
                ConcreteDataType::$TypeId($DataType::default())
            }

            fn type_name() -> String {
                stringify!($TypeId).to_string()
            }

            fn cast_vector(vector: &dyn Vector) -> Result<&PrimitiveVector<$DataType>> {
                vector
                    .as_any()
                    .downcast_ref::<PrimitiveVector<$DataType>>()
                    .with_context(|| error::CastTypeSnafu {
                        msg: format!(
                            "Failed to cast {} to vector of primitive type {}",
                            vector.vector_type_name(),
                            stringify!($TypeId)
                        ),
                    })
            }

            fn cast_value_ref(value: ValueRef) -> Result<Option<$Native>> {
                match value {
                    ValueRef::Null => Ok(None),
                    ValueRef::$TypeId(v) => Ok(Some(v.into())),
                    other => error::CastTypeSnafu {
                        msg: format!(
                            "Failed to cast value {:?} to primitive type {}",
                            other,
                            stringify!($TypeId),
                        ),
                    }
                    .fail(),
                }
            }
        }

        impl DataType for $DataType {
            fn name(&self) -> &str {
                stringify!($TypeId)
            }

            fn logical_type_id(&self) -> LogicalTypeId {
                LogicalTypeId::$TypeId
            }

            fn default_value(&self) -> Value {
                $Native::default().into()
            }

            fn as_arrow_type(&self) -> ArrowDataType {
                ArrowDataType::$TypeId
            }

            fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
                Box::new(PrimitiveVectorBuilder::<$DataType>::with_capacity(capacity))
            }
        }
    };
}

define_logical_primitive_type!(u8, UInt8, UInt8Type);
define_logical_primitive_type!(u16, UInt16, UInt16Type);
define_logical_primitive_type!(u32, UInt32, UInt32Type);
define_logical_primitive_type!(u64, UInt64, UInt64Type);
define_logical_primitive_type!(i8, Int8, Int8Type);
define_logical_primitive_type!(i16, Int16, Int16Type);
define_logical_primitive_type!(i32, Int32, Int32Type);
define_logical_primitive_type!(i64, Int64, Int64Type);
define_logical_primitive_type!(f32, Float32, Float32Type);
define_logical_primitive_type!(f64, Float64, Float64Type);

// TODO(yingwen): Should we rename it to OrdNativeType?
/// A new type for [NativeType], complement the `Ord` feature for it. Wrapping non ordered
/// primitive types like `f32` and `f64` in `OrdPrimitive` can make them be used in places that
/// require `Ord`. For example, in `Median` or `Percentile` UDAFs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrdPrimitive<T: NativeType>(pub T);

impl<T: NativeType> OrdPrimitive<T> {
    pub fn as_primitive(&self) -> T {
        self.0
    }
}

impl<T: NativeType> Eq for OrdPrimitive<T> {}

impl<T: NativeType> PartialOrd for OrdPrimitive<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: NativeType> Ord for OrdPrimitive<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.into().cmp(&other.0.into())
    }
}

impl<T: NativeType> From<OrdPrimitive<T>> for Value {
    fn from(p: OrdPrimitive<T>) -> Self {
        p.0.into()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;

    use super::*;

    #[test]
    fn test_ord_primitive() {
        struct Foo<T>
        where
            T: NativeType,
        {
            heap: BinaryHeap<OrdPrimitive<T>>,
        }

        impl<T> Foo<T>
        where
            T: NativeType,
        {
            fn push(&mut self, value: T) {
                let value = OrdPrimitive::<T>(value);
                self.heap.push(value);
            }
        }

        macro_rules! test {
            ($Type:ident) => {
                let mut foo = Foo::<$Type> {
                    heap: BinaryHeap::new(),
                };
                foo.push($Type::default());
            };
        }

        test!(u8);
        test!(u16);
        test!(u32);
        test!(u64);
        test!(i8);
        test!(i16);
        test!(i32);
        test!(i64);
        test!(f32);
        test!(f64);
    }
}
