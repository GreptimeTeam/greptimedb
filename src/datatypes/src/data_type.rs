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

use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use common_time::timestamp::TimeUnit;
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::error::{self, Error, Result};
use crate::type_id::LogicalTypeId;
use crate::types::{
    BinaryType, BooleanType, DateTimeType, DateType, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, ListType, NullType, StringType, TimestampType, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use crate::value::Value;
use crate::vectors::MutableVector;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[enum_dispatch::enum_dispatch(DataType)]
pub enum ConcreteDataType {
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

    Date(DateType),
    DateTime(DateTimeType),
    Timestamp(TimestampType),

    List(ListType),
}

impl ConcreteDataType {
    pub fn is_float(&self) -> bool {
        matches!(
            self,
            ConcreteDataType::Float64(_) | ConcreteDataType::Float32(_)
        )
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, ConcreteDataType::Boolean(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(
            self,
            ConcreteDataType::String(_)
                | ConcreteDataType::Date(_)
                | ConcreteDataType::DateTime(_)
                | ConcreteDataType::Timestamp(_)
        )
    }

    pub fn is_signed(&self) -> bool {
        matches!(
            self,
            ConcreteDataType::Int8(_)
                | ConcreteDataType::Int16(_)
                | ConcreteDataType::Int32(_)
                | ConcreteDataType::Int64(_)
                | ConcreteDataType::Date(_)
                | ConcreteDataType::DateTime(_)
                | ConcreteDataType::Timestamp(_)
        )
    }

    pub fn is_unsigned(&self) -> bool {
        matches!(
            self,
            ConcreteDataType::UInt8(_)
                | ConcreteDataType::UInt16(_)
                | ConcreteDataType::UInt32(_)
                | ConcreteDataType::UInt64(_)
        )
    }

    pub fn is_timestamp(&self) -> bool {
        matches!(
            self,
            ConcreteDataType::Timestamp(_) | ConcreteDataType::Int64(_)
        )
    }

    pub fn numerics() -> Vec<ConcreteDataType> {
        vec![
            ConcreteDataType::int8_datatype(),
            ConcreteDataType::int16_datatype(),
            ConcreteDataType::int32_datatype(),
            ConcreteDataType::int64_datatype(),
            ConcreteDataType::uint8_datatype(),
            ConcreteDataType::uint16_datatype(),
            ConcreteDataType::uint32_datatype(),
            ConcreteDataType::uint64_datatype(),
            ConcreteDataType::float32_datatype(),
            ConcreteDataType::float64_datatype(),
        ]
    }

    /// Convert arrow data type to [ConcreteDataType].
    ///
    /// # Panics
    /// Panic if given arrow data type is not supported.
    pub fn from_arrow_type(dt: &ArrowDataType) -> Self {
        ConcreteDataType::try_from(dt).expect("Unimplemented type")
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ConcreteDataType::Null(NullType))
    }
}

impl TryFrom<&ArrowDataType> for ConcreteDataType {
    type Error = Error;

    fn try_from(dt: &ArrowDataType) -> Result<ConcreteDataType> {
        let concrete_type = match dt {
            ArrowDataType::Null => Self::null_datatype(),
            ArrowDataType::Boolean => Self::boolean_datatype(),
            ArrowDataType::UInt8 => Self::uint8_datatype(),
            ArrowDataType::UInt16 => Self::uint16_datatype(),
            ArrowDataType::UInt32 => Self::uint32_datatype(),
            ArrowDataType::UInt64 => Self::uint64_datatype(),
            ArrowDataType::Int8 => Self::int8_datatype(),
            ArrowDataType::Int16 => Self::int16_datatype(),
            ArrowDataType::Int32 => Self::int32_datatype(),
            ArrowDataType::Int64 => Self::int64_datatype(),
            ArrowDataType::Float32 => Self::float32_datatype(),
            ArrowDataType::Float64 => Self::float64_datatype(),
            ArrowDataType::Date32 => Self::date_datatype(),
            ArrowDataType::Date64 => Self::datetime_datatype(),
            ArrowDataType::Timestamp(u, _) => ConcreteDataType::from_arrow_time_unit(u),
            ArrowDataType::Binary | ArrowDataType::LargeBinary => Self::binary_datatype(),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Self::string_datatype(),
            ArrowDataType::List(field) => Self::List(ListType::new(
                ConcreteDataType::from_arrow_type(&field.data_type),
            )),
            _ => {
                return error::UnsupportedArrowTypeSnafu {
                    arrow_type: dt.clone(),
                }
                .fail()
            }
        };

        Ok(concrete_type)
    }
}

macro_rules! impl_new_concrete_type_functions {
    ($($Type: ident), +) => {
        paste! {
            impl ConcreteDataType {
                $(
                    pub fn [<$Type:lower _datatype>]() -> ConcreteDataType {
                        ConcreteDataType::$Type([<$Type Type>]::default())
                    }
                )+
            }
        }
    }
}

impl_new_concrete_type_functions!(
    Null, Boolean, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64,
    Binary, String, Date, DateTime
);

impl ConcreteDataType {
    pub fn list_datatype(inner_type: ConcreteDataType) -> ConcreteDataType {
        ConcreteDataType::List(ListType::new(inner_type))
    }

    pub fn timestamp_datatype(unit: TimeUnit) -> Self {
        ConcreteDataType::Timestamp(TimestampType::new(unit))
    }

    pub fn timestamp_millis_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::new(TimeUnit::Millisecond))
    }

    /// Converts from arrow timestamp unit to
    // TODO(hl): maybe impl From<ArrowTimestamp> for our timestamp ?
    pub fn from_arrow_time_unit(t: &arrow::datatypes::TimeUnit) -> Self {
        match t {
            arrow::datatypes::TimeUnit::Second => Self::timestamp_datatype(TimeUnit::Second),
            arrow::datatypes::TimeUnit::Millisecond => {
                Self::timestamp_datatype(TimeUnit::Millisecond)
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                Self::timestamp_datatype(TimeUnit::Microsecond)
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                Self::timestamp_datatype(TimeUnit::Nanosecond)
            }
        }
    }
}

/// Data type abstraction.
#[enum_dispatch::enum_dispatch]
pub trait DataType: std::fmt::Debug + Send + Sync {
    /// Name of this data type.
    fn name(&self) -> &str;

    /// Returns id of the Logical data type.
    fn logical_type_id(&self) -> LogicalTypeId;

    /// Returns the default value of this type.
    fn default_value(&self) -> Value;

    /// Convert this type as [arrow2::datatypes::DataType].
    fn as_arrow_type(&self) -> ArrowDataType;

    /// Create a mutable vector with given `capacity` of this type.
    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector>;
}

pub type DataTypeRef = Arc<dyn DataType>;

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

    use super::*;

    #[test]
    fn test_concrete_type_as_datatype_trait() {
        let concrete_type = ConcreteDataType::boolean_datatype();

        assert_eq!("Boolean", concrete_type.name());
        assert_eq!(Value::Boolean(false), concrete_type.default_value());
        assert_eq!(LogicalTypeId::Boolean, concrete_type.logical_type_id());
        assert_eq!(ArrowDataType::Boolean, concrete_type.as_arrow_type());
    }

    #[test]
    fn test_from_arrow_type() {
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Null),
            ConcreteDataType::Null(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Boolean),
            ConcreteDataType::Boolean(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Binary),
            ConcreteDataType::Binary(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::LargeBinary),
            ConcreteDataType::Binary(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int8),
            ConcreteDataType::Int8(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int16),
            ConcreteDataType::Int16(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int32),
            ConcreteDataType::Int32(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int64),
            ConcreteDataType::Int64(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt8),
            ConcreteDataType::UInt8(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt16),
            ConcreteDataType::UInt16(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt32),
            ConcreteDataType::UInt32(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt64),
            ConcreteDataType::UInt64(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Float32),
            ConcreteDataType::Float32(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Float64),
            ConcreteDataType::Float64(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Utf8),
            ConcreteDataType::String(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Utf8),
            ConcreteDataType::String(_)
        ));
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::List(Box::new(Field::new(
                "item",
                ArrowDataType::Int32,
                true,
            )))),
            ConcreteDataType::List(ListType::new(ConcreteDataType::int32_datatype()))
        );
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Date32),
            ConcreteDataType::Date(_)
        ));
    }

    #[test]
    fn test_from_arrow_timestamp() {
        assert_eq!(
            ConcreteDataType::timestamp_millis_datatype(),
            ConcreteDataType::from_arrow_time_unit(&arrow::datatypes::TimeUnit::Millisecond)
        );
        assert_eq!(
            ConcreteDataType::timestamp_datatype(TimeUnit::Microsecond),
            ConcreteDataType::from_arrow_time_unit(&arrow::datatypes::TimeUnit::Microsecond)
        );
        assert_eq!(
            ConcreteDataType::timestamp_datatype(TimeUnit::Nanosecond),
            ConcreteDataType::from_arrow_time_unit(&arrow::datatypes::TimeUnit::Nanosecond)
        );
        assert_eq!(
            ConcreteDataType::timestamp_datatype(TimeUnit::Second),
            ConcreteDataType::from_arrow_time_unit(&arrow::datatypes::TimeUnit::Second)
        );
    }

    #[test]
    fn test_is_timestamp() {
        assert!(ConcreteDataType::timestamp_millis_datatype().is_timestamp());
        assert!(ConcreteDataType::timestamp_datatype(TimeUnit::Second).is_timestamp());
        assert!(ConcreteDataType::timestamp_datatype(TimeUnit::Millisecond).is_timestamp());
        assert!(ConcreteDataType::timestamp_datatype(TimeUnit::Microsecond).is_timestamp());
        assert!(ConcreteDataType::timestamp_datatype(TimeUnit::Nanosecond).is_timestamp());
        assert!(ConcreteDataType::int64_datatype().is_timestamp());
    }

    #[test]
    fn test_is_null() {
        assert!(ConcreteDataType::null_datatype().is_null());
        assert!(!ConcreteDataType::int32_datatype().is_null());
    }
}
