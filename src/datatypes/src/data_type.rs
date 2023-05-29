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

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use common_time::timestamp::TimeUnit;
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::error::{self, Error, Result};
use crate::type_id::LogicalTypeId;
use crate::types::{
    BinaryType, BooleanType, DateTimeType, DateType, DictionaryType, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, ListType, NullType, StringType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, TimestampType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use crate::value::Value;
use crate::vectors::MutableVector;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

    // String types:
    Binary(BinaryType),
    String(StringType),

    // Date types:
    Date(DateType),
    DateTime(DateTimeType),
    Timestamp(TimestampType),

    // Compound types:
    List(ListType),
    Dictionary(DictionaryType),
}

impl fmt::Display for ConcreteDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConcreteDataType::Null(_) => write!(f, "Null"),
            ConcreteDataType::Boolean(_) => write!(f, "Boolean"),
            ConcreteDataType::Int8(_) => write!(f, "Int8"),
            ConcreteDataType::Int16(_) => write!(f, "Int16"),
            ConcreteDataType::Int32(_) => write!(f, "Int32"),
            ConcreteDataType::Int64(_) => write!(f, "Int64"),
            ConcreteDataType::UInt8(_) => write!(f, "UInt8"),
            ConcreteDataType::UInt16(_) => write!(f, "UInt16"),
            ConcreteDataType::UInt32(_) => write!(f, "UInt32"),
            ConcreteDataType::UInt64(_) => write!(f, "UInt64"),
            ConcreteDataType::Float32(_) => write!(f, "Float32"),
            ConcreteDataType::Float64(_) => write!(f, "Float64"),
            ConcreteDataType::Binary(_) => write!(f, "Binary"),
            ConcreteDataType::String(_) => write!(f, "String"),
            ConcreteDataType::Date(_) => write!(f, "Date"),
            ConcreteDataType::DateTime(_) => write!(f, "DateTime"),
            ConcreteDataType::Timestamp(_) => write!(f, "Timestamp"),
            ConcreteDataType::List(_) => write!(f, "List"),
            ConcreteDataType::Dictionary(_) => write!(f, "Dictionary"),
        }
    }
}

// TODO(yingwen): Refactor these `is_xxx()` methods, such as adding a `properties()` method
// returning all these properties to the `DataType` trait
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

    pub fn is_stringifiable(&self) -> bool {
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

    /// Try to cast the type as a [`ListType`].
    pub fn as_list(&self) -> Option<&ListType> {
        match self {
            ConcreteDataType::List(t) => Some(t),
            _ => None,
        }
    }

    /// Try to cast data type as a [`TimestampType`].
    pub fn as_timestamp(&self) -> Option<TimestampType> {
        match self {
            ConcreteDataType::Int64(_) => {
                Some(TimestampType::Millisecond(TimestampMillisecondType))
            }
            ConcreteDataType::Timestamp(t) => Some(*t),
            _ => None,
        }
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
                ConcreteDataType::from_arrow_type(field.data_type()),
            )),
            ArrowDataType::Dictionary(key_type, value_type) => {
                let key_type = ConcreteDataType::from_arrow_type(key_type);
                let value_type = ConcreteDataType::from_arrow_type(value_type);
                Self::Dictionary(DictionaryType::new(key_type, value_type))
            }
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
    Binary, Date, DateTime, String
);

impl ConcreteDataType {
    pub fn timestamp_second_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Second(TimestampSecondType::default()))
    }

    pub fn timestamp_millisecond_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Millisecond(
            TimestampMillisecondType::default(),
        ))
    }

    pub fn timestamp_microsecond_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Microsecond(
            TimestampMicrosecondType::default(),
        ))
    }

    pub fn timestamp_nanosecond_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Nanosecond(TimestampNanosecondType::default()))
    }

    pub fn timestamp_datatype(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::timestamp_second_datatype(),
            TimeUnit::Millisecond => Self::timestamp_millisecond_datatype(),
            TimeUnit::Microsecond => Self::timestamp_microsecond_datatype(),
            TimeUnit::Nanosecond => Self::timestamp_nanosecond_datatype(),
        }
    }

    /// Converts from arrow timestamp unit to
    pub fn from_arrow_time_unit(t: &ArrowTimeUnit) -> Self {
        match t {
            ArrowTimeUnit::Second => Self::timestamp_second_datatype(),
            ArrowTimeUnit::Millisecond => Self::timestamp_millisecond_datatype(),
            ArrowTimeUnit::Microsecond => Self::timestamp_microsecond_datatype(),
            ArrowTimeUnit::Nanosecond => Self::timestamp_nanosecond_datatype(),
        }
    }

    pub fn list_datatype(item_type: ConcreteDataType) -> ConcreteDataType {
        ConcreteDataType::List(ListType::new(item_type))
    }

    pub fn dictionary_datatype(
        key_type: ConcreteDataType,
        value_type: ConcreteDataType,
    ) -> ConcreteDataType {
        ConcreteDataType::Dictionary(DictionaryType::new(key_type, value_type))
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

    /// Convert this type as [arrow::datatypes::DataType].
    fn as_arrow_type(&self) -> ArrowDataType;

    /// Creates a mutable vector with given `capacity` of this type.
    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector>;

    /// Returns true if the data type is compatible with timestamp type so we can
    /// use it as a timestamp.
    fn is_timestamp_compatible(&self) -> bool;
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
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::List(Arc::new(Field::new(
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
            ConcreteDataType::timestamp_millisecond_datatype(),
            ConcreteDataType::from_arrow_time_unit(&ArrowTimeUnit::Millisecond)
        );
        assert_eq!(
            ConcreteDataType::timestamp_microsecond_datatype(),
            ConcreteDataType::from_arrow_time_unit(&ArrowTimeUnit::Microsecond)
        );
        assert_eq!(
            ConcreteDataType::timestamp_nanosecond_datatype(),
            ConcreteDataType::from_arrow_time_unit(&ArrowTimeUnit::Nanosecond)
        );
        assert_eq!(
            ConcreteDataType::timestamp_second_datatype(),
            ConcreteDataType::from_arrow_time_unit(&ArrowTimeUnit::Second)
        );
    }

    #[test]
    fn test_is_timestamp_compatible() {
        assert!(ConcreteDataType::timestamp_datatype(TimeUnit::Second).is_timestamp_compatible());
        assert!(
            ConcreteDataType::timestamp_datatype(TimeUnit::Millisecond).is_timestamp_compatible()
        );
        assert!(
            ConcreteDataType::timestamp_datatype(TimeUnit::Microsecond).is_timestamp_compatible()
        );
        assert!(
            ConcreteDataType::timestamp_datatype(TimeUnit::Nanosecond).is_timestamp_compatible()
        );
        assert!(ConcreteDataType::timestamp_second_datatype().is_timestamp_compatible());
        assert!(ConcreteDataType::timestamp_millisecond_datatype().is_timestamp_compatible());
        assert!(ConcreteDataType::timestamp_microsecond_datatype().is_timestamp_compatible());
        assert!(ConcreteDataType::timestamp_nanosecond_datatype().is_timestamp_compatible());
        assert!(ConcreteDataType::int64_datatype().is_timestamp_compatible());
        assert!(!ConcreteDataType::null_datatype().is_timestamp_compatible());
        assert!(!ConcreteDataType::binary_datatype().is_timestamp_compatible());
        assert!(!ConcreteDataType::boolean_datatype().is_timestamp_compatible());
        assert!(!ConcreteDataType::date_datatype().is_timestamp_compatible());
        assert!(!ConcreteDataType::datetime_datatype().is_timestamp_compatible());
        assert!(!ConcreteDataType::string_datatype().is_timestamp_compatible());
        assert!(!ConcreteDataType::int32_datatype().is_timestamp_compatible());
        assert!(!ConcreteDataType::uint64_datatype().is_timestamp_compatible());
    }

    #[test]
    fn test_is_null() {
        assert!(ConcreteDataType::null_datatype().is_null());
        assert!(!ConcreteDataType::int32_datatype().is_null());
    }

    #[test]
    fn test_is_float() {
        assert!(!ConcreteDataType::int32_datatype().is_float());
        assert!(ConcreteDataType::float32_datatype().is_float());
        assert!(ConcreteDataType::float64_datatype().is_float());
    }

    #[test]
    fn test_is_boolean() {
        assert!(!ConcreteDataType::int32_datatype().is_boolean());
        assert!(!ConcreteDataType::float32_datatype().is_boolean());
        assert!(ConcreteDataType::boolean_datatype().is_boolean());
    }

    #[test]
    fn test_is_stringifiable() {
        assert!(!ConcreteDataType::int32_datatype().is_stringifiable());
        assert!(!ConcreteDataType::float32_datatype().is_stringifiable());
        assert!(ConcreteDataType::string_datatype().is_stringifiable());
        assert!(ConcreteDataType::date_datatype().is_stringifiable());
        assert!(ConcreteDataType::datetime_datatype().is_stringifiable());
        assert!(ConcreteDataType::timestamp_second_datatype().is_stringifiable());
        assert!(ConcreteDataType::timestamp_millisecond_datatype().is_stringifiable());
        assert!(ConcreteDataType::timestamp_microsecond_datatype().is_stringifiable());
        assert!(ConcreteDataType::timestamp_nanosecond_datatype().is_stringifiable());
    }

    #[test]
    fn test_is_signed() {
        assert!(ConcreteDataType::int8_datatype().is_signed());
        assert!(ConcreteDataType::int16_datatype().is_signed());
        assert!(ConcreteDataType::int32_datatype().is_signed());
        assert!(ConcreteDataType::int64_datatype().is_signed());
        assert!(ConcreteDataType::date_datatype().is_signed());
        assert!(ConcreteDataType::datetime_datatype().is_signed());
        assert!(ConcreteDataType::timestamp_second_datatype().is_signed());
        assert!(ConcreteDataType::timestamp_millisecond_datatype().is_signed());
        assert!(ConcreteDataType::timestamp_microsecond_datatype().is_signed());
        assert!(ConcreteDataType::timestamp_nanosecond_datatype().is_signed());

        assert!(!ConcreteDataType::uint8_datatype().is_signed());
        assert!(!ConcreteDataType::uint16_datatype().is_signed());
        assert!(!ConcreteDataType::uint32_datatype().is_signed());
        assert!(!ConcreteDataType::uint64_datatype().is_signed());

        assert!(!ConcreteDataType::float32_datatype().is_signed());
        assert!(!ConcreteDataType::float64_datatype().is_signed());
    }

    #[test]
    fn test_is_unsigned() {
        assert!(!ConcreteDataType::int8_datatype().is_unsigned());
        assert!(!ConcreteDataType::int16_datatype().is_unsigned());
        assert!(!ConcreteDataType::int32_datatype().is_unsigned());
        assert!(!ConcreteDataType::int64_datatype().is_unsigned());
        assert!(!ConcreteDataType::date_datatype().is_unsigned());
        assert!(!ConcreteDataType::datetime_datatype().is_unsigned());
        assert!(!ConcreteDataType::timestamp_second_datatype().is_unsigned());
        assert!(!ConcreteDataType::timestamp_millisecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::timestamp_microsecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::timestamp_nanosecond_datatype().is_unsigned());

        assert!(ConcreteDataType::uint8_datatype().is_unsigned());
        assert!(ConcreteDataType::uint16_datatype().is_unsigned());
        assert!(ConcreteDataType::uint32_datatype().is_unsigned());
        assert!(ConcreteDataType::uint64_datatype().is_unsigned());

        assert!(!ConcreteDataType::float32_datatype().is_unsigned());
        assert!(!ConcreteDataType::float64_datatype().is_unsigned());
    }

    #[test]
    fn test_numerics() {
        let nums = ConcreteDataType::numerics();
        assert_eq!(10, nums.len());
    }

    #[test]
    fn test_as_list() {
        let list_type = ConcreteDataType::list_datatype(ConcreteDataType::int32_datatype());
        assert_eq!(
            ListType::new(ConcreteDataType::int32_datatype()),
            *list_type.as_list().unwrap()
        );
        assert!(ConcreteDataType::int32_datatype().as_list().is_none());
    }

    #[test]
    fn test_display_concrete_data_type() {
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Null).to_string(),
            "Null"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Boolean).to_string(),
            "Boolean"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Binary).to_string(),
            "Binary"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::LargeBinary).to_string(),
            "Binary"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int8).to_string(),
            "Int8"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int16).to_string(),
            "Int16"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int32).to_string(),
            "Int32"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int64).to_string(),
            "Int64"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt8).to_string(),
            "UInt8"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt16).to_string(),
            "UInt16"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt32).to_string(),
            "UInt32"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt64).to_string(),
            "UInt64"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Float32).to_string(),
            "Float32"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Float64).to_string(),
            "Float64"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Utf8).to_string(),
            "String"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::List(Arc::new(Field::new(
                "item",
                ArrowDataType::Int32,
                true,
            ))))
            .to_string(),
            "List"
        );
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Date32).to_string(),
            "Date"
        );
    }
}
