// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::fmt;

use arrow::datatypes::{ArrowNativeType, ArrowPrimitiveType, DataType as ArrowDataType};
use common_time::interval::IntervalUnit;
use common_time::{Date, DateTime};
use num::NumCast;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use super::boolean_type::bool_to_numeric;
use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Result};
use crate::scalars::{Scalar, ScalarRef, ScalarVectorBuilder};
use crate::type_id::LogicalTypeId;
use crate::types::{DateTimeType, DateType};
use crate::value::{Value, ValueRef};
use crate::vectors::{MutableVector, PrimitiveVector, PrimitiveVectorBuilder, Vector};

/// Data types that can be used as arrow's native type.
pub trait NativeType: ArrowNativeType + NumCast {}

macro_rules! impl_native_type {
    ($Type: ident) => {
        impl NativeType for $Type {}
    };
}

impl_native_type!(u8);
impl_native_type!(u16);
impl_native_type!(u32);
impl_native_type!(u64);
impl_native_type!(i8);
impl_native_type!(i16);
impl_native_type!(i32);
impl_native_type!(i64);
impl_native_type!(i128);
impl_native_type!(f32);
impl_native_type!(f64);

/// Represents the wrapper type that wraps a native type using the `newtype pattern`,
/// such as [Date](`common_time::Date`) is a wrapper type for the underlying native
/// type `i32`.
pub trait WrapperType:
    Copy
    + Send
    + Sync
    + fmt::Debug
    + for<'a> Scalar<RefType<'a> = Self>
    + PartialEq
    + Into<Value>
    + Into<ValueRef<'static>>
    + Serialize
    + Into<serde_json::Value>
{
    /// Logical primitive type that this wrapper type belongs to.
    type LogicalType: LogicalPrimitiveType<Wrapper = Self, Native = Self::Native>;
    /// The underlying native type.
    type Native: NativeType;

    /// Convert native type into this wrapper type.
    fn from_native(value: Self::Native) -> Self;

    /// Convert this wrapper type into native type.
    fn into_native(self) -> Self::Native;
}

/// Trait bridging the logical primitive type with [ArrowPrimitiveType].
pub trait LogicalPrimitiveType: 'static + Sized {
    /// Arrow primitive type of this logical type.
    type ArrowPrimitive: ArrowPrimitiveType<Native = Self::Native>;
    /// Native (physical) type of this logical type.
    type Native: NativeType;
    /// Wrapper type that the vector returns.
    type Wrapper: WrapperType<LogicalType = Self, Native = Self::Native>
        + for<'a> Scalar<VectorType = PrimitiveVector<Self>, RefType<'a> = Self::Wrapper>
        + for<'a> ScalarRef<'a, ScalarType = Self::Wrapper>;
    /// Largest type this primitive type can cast to.
    type LargestType: LogicalPrimitiveType;

    /// Construct the data type struct.
    fn build_data_type() -> ConcreteDataType;

    /// Return the name of the type.
    fn type_name() -> &'static str;

    /// Dynamic cast the vector to the concrete vector type.
    fn cast_vector(vector: &dyn Vector) -> Result<&PrimitiveVector<Self>>;

    /// Cast value ref to the primitive type.
    fn cast_value_ref(value: ValueRef) -> Result<Option<Self::Wrapper>>;
}

/// A new type for [WrapperType], complement the `Ord` feature for it. Wrapping non ordered
/// primitive types like `f32` and `f64` in `OrdPrimitive` can make them be used in places that
/// require `Ord`. For example, in `Median` or `Percentile` UDAFs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrdPrimitive<T: WrapperType>(pub T);

impl<T: WrapperType> OrdPrimitive<T> {
    pub fn as_primitive(&self) -> T::Native {
        self.0.into_native()
    }
}

impl<T: WrapperType> Eq for OrdPrimitive<T> {}

impl<T: WrapperType> PartialOrd for OrdPrimitive<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: WrapperType> Ord for OrdPrimitive<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        Into::<Value>::into(self.0).cmp(&Into::<Value>::into(other.0))
    }
}

impl<T: WrapperType> From<OrdPrimitive<T>> for Value {
    fn from(p: OrdPrimitive<T>) -> Self {
        p.0.into()
    }
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

impl WrapperType for Date {
    type LogicalType = DateType;
    type Native = i32;

    fn from_native(value: i32) -> Self {
        Date::new(value)
    }

    fn into_native(self) -> i32 {
        self.val()
    }
}

impl WrapperType for DateTime {
    type LogicalType = DateTimeType;
    type Native = i64;

    fn from_native(value: Self::Native) -> Self {
        DateTime::new(value)
    }

    fn into_native(self) -> Self::Native {
        self.val()
    }
}

macro_rules! define_logical_primitive_type {
    ($Native: ident, $TypeId: ident, $DataType: ident, $Largest: ident) => {
        // We need to define it as an empty struct `struct DataType {}` instead of a struct-unit
        // `struct DataType;` to ensure the serialized JSON string is compatible with previous
        // implementation.
        #[derive(
            Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        pub struct $DataType {}

        impl LogicalPrimitiveType for $DataType {
            type ArrowPrimitive = arrow::datatypes::$DataType;
            type Native = $Native;
            type Wrapper = $Native;
            type LargestType = $Largest;

            fn build_data_type() -> ConcreteDataType {
                ConcreteDataType::$TypeId($DataType::default())
            }

            fn type_name() -> &'static str {
                stringify!($TypeId)
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
    };
}

macro_rules! define_non_timestamp_primitive {
    ( $Native: ident, $TypeId: ident, $DataType: ident, $Largest: ident $(, $TargetType: ident)* ) => {
        define_logical_primitive_type!($Native, $TypeId, $DataType, $Largest);

        impl DataType for $DataType {
            fn name(&self) -> String {
                stringify!($TypeId).to_string()
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


            fn try_cast(&self, from: Value) -> Option<Value> {
                match from {
                    Value::Boolean(v) => bool_to_numeric(v).map(Value::$TypeId),
                    Value::String(v) => v.as_utf8().parse::<$Native>().map(|val| Value::from(val)).ok(),
                    $(
                        Value::$TargetType(v) => num::cast::cast(v).map(Value::$TypeId),
                    )*
                    _ => None,
                }
            }
        }
    };
}

define_non_timestamp_primitive!(
    u8, UInt8, UInt8Type, UInt64Type, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
    Float32, Float64
);
define_non_timestamp_primitive!(
    u16, UInt16, UInt16Type, UInt64Type, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
    Float32, Float64
);
define_non_timestamp_primitive!(
    u32, UInt32, UInt32Type, UInt64Type, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
    Float32, Float64
);
define_non_timestamp_primitive!(
    u64, UInt64, UInt64Type, UInt64Type, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
    Float32, Float64
);
define_non_timestamp_primitive!(
    i8, Int8, Int8Type, Int64Type, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
    Float32, Float64
);
define_non_timestamp_primitive!(
    i16, Int16, Int16Type, Int64Type, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
    Float32, Float64
);

define_non_timestamp_primitive!(
    f32,
    Float32,
    Float32Type,
    Float64Type,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64
);
define_non_timestamp_primitive!(
    f64,
    Float64,
    Float64Type,
    Float64Type,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64
);

// Timestamp primitive:
define_logical_primitive_type!(i64, Int64, Int64Type, Int64Type);

define_logical_primitive_type!(i32, Int32, Int32Type, Int64Type);

impl DataType for Int64Type {
    fn name(&self) -> String {
        "Int64".to_string()
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Int64
    }

    fn default_value(&self) -> Value {
        Value::Int64(0)
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Int64
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(PrimitiveVectorBuilder::<Int64Type>::with_capacity(capacity))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Boolean(v) => bool_to_numeric(v).map(Value::Int64),
            Value::Int8(v) => num::cast::cast(v).map(Value::Int64),
            Value::Int16(v) => num::cast::cast(v).map(Value::Int64),
            Value::Int32(v) => num::cast::cast(v).map(Value::Int64),
            Value::Int64(v) => Some(Value::Int64(v)),
            Value::UInt8(v) => num::cast::cast(v).map(Value::Int64),
            Value::UInt16(v) => num::cast::cast(v).map(Value::Int64),
            Value::UInt32(v) => num::cast::cast(v).map(Value::Int64),
            Value::Float32(v) => num::cast::cast(v).map(Value::Int64),
            Value::Float64(v) => num::cast::cast(v).map(Value::Int64),
            Value::String(v) => v.as_utf8().parse::<i64>().map(Value::Int64).ok(),
            Value::DateTime(v) => Some(Value::Int64(v.val())),
            Value::Timestamp(v) => Some(Value::Int64(v.value())),
            Value::Time(v) => Some(Value::Int64(v.value())),
            Value::Interval(v) => match v.unit() {
                IntervalUnit::DayTime => Some(Value::Int64(v.to_i64())),
                IntervalUnit::YearMonth => None,
                IntervalUnit::MonthDayNano => None,
            },
            _ => None,
        }
    }
}

impl DataType for Int32Type {
    fn name(&self) -> String {
        "Int32".to_string()
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Int32
    }

    fn default_value(&self) -> Value {
        Value::Int32(0)
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Int32
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(PrimitiveVectorBuilder::<Int32Type>::with_capacity(capacity))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Boolean(v) => bool_to_numeric(v).map(Value::Int32),
            Value::Int8(v) => num::cast::cast(v).map(Value::Int32),
            Value::Int16(v) => num::cast::cast(v).map(Value::Int32),
            Value::Int32(v) => Some(Value::Int32(v)),
            Value::Int64(v) => num::cast::cast(v).map(Value::Int64),
            Value::UInt8(v) => num::cast::cast(v).map(Value::Int32),
            Value::UInt16(v) => num::cast::cast(v).map(Value::Int32),
            Value::UInt32(v) => num::cast::cast(v).map(Value::UInt32),
            Value::UInt64(v) => num::cast::cast(v).map(Value::UInt64),
            Value::Float32(v) => num::cast::cast(v).map(Value::Int32),
            Value::Float64(v) => num::cast::cast(v).map(Value::Int32),
            Value::String(v) => v.as_utf8().parse::<i32>().map(Value::Int32).ok(),
            Value::Date(v) => Some(Value::Int32(v.val())),
            Value::Interval(v) => match v.unit() {
                IntervalUnit::YearMonth => Some(Value::Int32(v.to_i32())),
                IntervalUnit::DayTime => None,
                IntervalUnit::MonthDayNano => None,
            },
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;

    use ordered_float::OrderedFloat;

    use super::*;

    #[test]
    fn test_ord_primitive() {
        struct Foo<T>
        where
            T: WrapperType,
        {
            heap: BinaryHeap<OrdPrimitive<T>>,
        }

        impl<T> Foo<T>
        where
            T: WrapperType,
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
                assert_eq!($Type::default(), foo.heap.pop().unwrap().as_primitive());
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

    macro_rules! assert_primitive_cast {
        ($value: expr, $datatype:expr, $expected: expr) => {
            let val = $value;
            let b = $datatype.try_cast(val).unwrap();
            assert_eq!(b, $expected);
        };
    }

    #[test]
    fn test_primitive_cast() {
        // Integer cast
        assert_primitive_cast!(
            Value::UInt8(123),
            ConcreteDataType::uint16_datatype(),
            Value::UInt16(123)
        );

        assert_primitive_cast!(
            Value::UInt8(123),
            ConcreteDataType::uint32_datatype(),
            Value::UInt32(123)
        );
        assert_primitive_cast!(
            Value::UInt8(123),
            ConcreteDataType::uint64_datatype(),
            Value::UInt64(123)
        );
        assert_primitive_cast!(
            Value::UInt16(1234),
            ConcreteDataType::uint32_datatype(),
            Value::UInt32(1234)
        );
        assert_primitive_cast!(
            Value::UInt16(1234),
            ConcreteDataType::uint64_datatype(),
            Value::UInt64(1234)
        );
        assert_primitive_cast!(
            Value::UInt32(12345),
            ConcreteDataType::uint64_datatype(),
            Value::UInt64(12345)
        );

        assert_primitive_cast!(
            Value::UInt8(123),
            ConcreteDataType::uint16_datatype(),
            Value::UInt16(123)
        );

        assert_primitive_cast!(
            Value::Int8(123),
            ConcreteDataType::int32_datatype(),
            Value::Int32(123)
        );
        assert_primitive_cast!(
            Value::Int8(123),
            ConcreteDataType::int64_datatype(),
            Value::Int64(123)
        );
        assert_primitive_cast!(
            Value::Int16(1234),
            ConcreteDataType::int32_datatype(),
            Value::Int32(1234)
        );
        assert_primitive_cast!(
            Value::Int16(1234),
            ConcreteDataType::int64_datatype(),
            Value::Int64(1234)
        );
        assert_primitive_cast!(
            Value::Int32(12345),
            ConcreteDataType::int64_datatype(),
            Value::Int64(12345)
        );
    }

    #[test]
    fn test_float_cast() {
        // cast to Float32
        assert_primitive_cast!(
            Value::UInt8(12),
            ConcreteDataType::float32_datatype(),
            Value::Float32(OrderedFloat(12.0))
        );
        assert_primitive_cast!(
            Value::UInt16(12),
            ConcreteDataType::float32_datatype(),
            Value::Float32(OrderedFloat(12.0))
        );
        assert_primitive_cast!(
            Value::Int8(12),
            ConcreteDataType::float32_datatype(),
            Value::Float32(OrderedFloat(12.0))
        );
        assert_primitive_cast!(
            Value::Int16(12),
            ConcreteDataType::float32_datatype(),
            Value::Float32(OrderedFloat(12.0))
        );
        assert_primitive_cast!(
            Value::Int32(12),
            ConcreteDataType::float32_datatype(),
            Value::Float32(OrderedFloat(12.0))
        );

        // cast to Float64
        assert_primitive_cast!(
            Value::UInt8(12),
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(12.0))
        );
        assert_primitive_cast!(
            Value::UInt16(12),
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(12.0))
        );
        assert_primitive_cast!(
            Value::UInt32(12),
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(12.0))
        );
        assert_primitive_cast!(
            Value::Int8(12),
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(12.0))
        );
        assert_primitive_cast!(
            Value::Int16(12),
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(12.0))
        );
        assert_primitive_cast!(
            Value::Int32(12),
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(12.0))
        );
        assert_primitive_cast!(
            Value::Int64(12),
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(12.0))
        );
    }

    #[test]
    fn test_string_cast_to_primitive() {
        assert_primitive_cast!(
            Value::String("123".into()),
            ConcreteDataType::uint8_datatype(),
            Value::UInt8(123)
        );
        assert_primitive_cast!(
            Value::String("123".into()),
            ConcreteDataType::uint16_datatype(),
            Value::UInt16(123)
        );
        assert_primitive_cast!(
            Value::String("123".into()),
            ConcreteDataType::uint32_datatype(),
            Value::UInt32(123)
        );
        assert_primitive_cast!(
            Value::String("123".into()),
            ConcreteDataType::uint64_datatype(),
            Value::UInt64(123)
        );
        assert_primitive_cast!(
            Value::String("123".into()),
            ConcreteDataType::int8_datatype(),
            Value::Int8(123)
        );
        assert_primitive_cast!(
            Value::String("123".into()),
            ConcreteDataType::int16_datatype(),
            Value::Int16(123)
        );
        assert_primitive_cast!(
            Value::String("123".into()),
            ConcreteDataType::int32_datatype(),
            Value::Int32(123)
        );
        assert_primitive_cast!(
            Value::String("123".into()),
            ConcreteDataType::int64_datatype(),
            Value::Int64(123)
        );
        assert_primitive_cast!(
            Value::String("1.23".into()),
            ConcreteDataType::float32_datatype(),
            Value::Float32(OrderedFloat(1.23))
        );
        assert_primitive_cast!(
            Value::String("1.23".into()),
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(1.23))
        );
    }
}
