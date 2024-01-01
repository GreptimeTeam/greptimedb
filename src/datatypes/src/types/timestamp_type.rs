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

use std::str::FromStr;

use arrow::datatypes::{
    DataType as ArrowDataType, TimeUnit as ArrowTimeUnit,
    TimestampMicrosecondType as ArrowTimestampMicrosecondType,
    TimestampMillisecondType as ArrowTimestampMillisecondType,
    TimestampNanosecondType as ArrowTimestampNanosecondType,
    TimestampSecondType as ArrowTimestampSecondType,
};
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use enum_dispatch::enum_dispatch;
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::data_type::ConcreteDataType;
use crate::error;
use crate::error::InvalidTimestampPrecisionSnafu;
use crate::prelude::{
    DataType, LogicalTypeId, MutableVector, ScalarVectorBuilder, Value, ValueRef, Vector,
};
use crate::timestamp::{
    TimestampMicrosecond, TimestampMillisecond, TimestampNanosecond, TimestampSecond,
};
use crate::types::LogicalPrimitiveType;
use crate::vectors::{
    PrimitiveVector, TimestampMicrosecondVector, TimestampMicrosecondVectorBuilder,
    TimestampMillisecondVector, TimestampMillisecondVectorBuilder, TimestampNanosecondVector,
    TimestampNanosecondVectorBuilder, TimestampSecondVector, TimestampSecondVectorBuilder,
};

const SECOND_VARIATION: u64 = 0;
const MILLISECOND_VARIATION: u64 = 3;
const MICROSECOND_VARIATION: u64 = 6;
const NANOSECOND_VARIATION: u64 = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[enum_dispatch(DataType)]
pub enum TimestampType {
    Second(TimestampSecondType),
    Millisecond(TimestampMillisecondType),
    Microsecond(TimestampMicrosecondType),
    Nanosecond(TimestampNanosecondType),
}

impl TryFrom<u64> for TimestampType {
    type Error = error::Error;

    /// Convert fractional timestamp precision to timestamp types. Supported precisions are:
    /// - 0: second
    /// - 3: millisecond
    /// - 6: microsecond
    /// - 9: nanosecond
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            SECOND_VARIATION => Ok(TimestampType::Second(TimestampSecondType)),
            MILLISECOND_VARIATION => Ok(TimestampType::Millisecond(TimestampMillisecondType)),
            MICROSECOND_VARIATION => Ok(TimestampType::Microsecond(TimestampMicrosecondType)),
            NANOSECOND_VARIATION => Ok(TimestampType::Nanosecond(TimestampNanosecondType)),
            _ => InvalidTimestampPrecisionSnafu { precision: value }.fail(),
        }
    }
}

impl TimestampType {
    /// Returns the [`TimeUnit`] of this type.
    pub fn unit(&self) -> TimeUnit {
        match self {
            TimestampType::Second(_) => TimeUnit::Second,
            TimestampType::Millisecond(_) => TimeUnit::Millisecond,
            TimestampType::Microsecond(_) => TimeUnit::Microsecond,
            TimestampType::Nanosecond(_) => TimeUnit::Nanosecond,
        }
    }

    pub fn create_timestamp(&self, val: i64) -> Timestamp {
        Timestamp::new(val, self.unit())
    }

    pub fn precision(&self) -> u64 {
        match self {
            TimestampType::Second(_) => SECOND_VARIATION,
            TimestampType::Millisecond(_) => MILLISECOND_VARIATION,
            TimestampType::Microsecond(_) => MICROSECOND_VARIATION,
            TimestampType::Nanosecond(_) => NANOSECOND_VARIATION,
        }
    }
}

macro_rules! impl_data_type_for_timestamp {
    ($unit: ident) => {
        paste! {
            #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
            pub struct [<Timestamp $unit Type>];

            impl DataType for [<Timestamp $unit Type>] {
                fn name(&self) -> String {
                    stringify!([<Timestamp $unit>]).to_string()
                }

                fn logical_type_id(&self) -> LogicalTypeId {
                    LogicalTypeId::[<Timestamp $unit>]
                }

                fn default_value(&self) -> Value {
                    Value::Timestamp(Timestamp::new(0, TimeUnit::$unit))
                }

                fn as_arrow_type(&self) -> ArrowDataType {
                    ArrowDataType::Timestamp(ArrowTimeUnit::$unit, None)
                }

                fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
                    Box::new([<Timestamp $unit Vector Builder>]::with_capacity(capacity))
                }

                fn try_cast(&self, from: Value)-> Option<Value>{
                    match from {
                        Value::Timestamp(v) => v.convert_to(TimeUnit::$unit).map(Value::Timestamp),
                        Value::String(v) => Timestamp::from_str(v.as_utf8()).map(Value::Timestamp).ok(),
                        Value::Int64(v) => Some(Value::Timestamp(Timestamp::new(v, TimeUnit::$unit))),
                        Value::DateTime(v) => Timestamp::new_second(v.val()).convert_to(TimeUnit::$unit).map(Value::Timestamp),
                        Value::Date(v) => Timestamp::new_second(v.to_secs()).convert_to(TimeUnit::$unit).map(Value::Timestamp),
                        _ => None
                    }
                }
            }

            impl LogicalPrimitiveType for [<Timestamp $unit Type>] {
                type ArrowPrimitive = [<Arrow Timestamp $unit Type>];
                type Native = i64;
                type Wrapper = [<Timestamp $unit>];
                type LargestType = Self;

                fn build_data_type() -> ConcreteDataType {
                    ConcreteDataType::Timestamp(TimestampType::$unit(
                        [<Timestamp $unit Type>]::default(),
                    ))
                }

                fn type_name() -> &'static str {
                    stringify!([<Timestamp $unit Type>])
                }

                fn cast_vector(vector: &dyn Vector) -> crate::Result<&PrimitiveVector<Self>> {
                    vector
                        .as_any()
                        .downcast_ref::<[<Timestamp $unit Vector>]>()
                        .with_context(|| error::CastTypeSnafu {
                            msg: format!(
                                "Failed to cast {} to {}",
                                vector.vector_type_name(), stringify!([<Timestamp $unit Vector>])
                            ),
                        })
                }

                fn cast_value_ref(value: ValueRef) -> crate::Result<Option<Self::Wrapper>> {
                    match value {
                        ValueRef::Null => Ok(None),
                        ValueRef::Int64(v) =>{
                            Ok(Some([<Timestamp $unit>]::from(v)))
                        }
                        ValueRef::Timestamp(t) => match t.unit() {
                            TimeUnit::$unit => Ok(Some([<Timestamp $unit>](t))),
                            other => error::CastTypeSnafu {
                                msg: format!(
                                    "Failed to cast Timestamp value with different unit {:?} to {}",
                                    other, stringify!([<Timestamp $unit>])
                                ),
                            }
                            .fail(),
                        },
                        other => error::CastTypeSnafu {
                            msg: format!("Failed to cast value {:?} to {}", other, stringify!([<Timestamp $unit>])),
                        }
                        .fail(),
                    }
                }
            }
        }
    }
}

impl_data_type_for_timestamp!(Nanosecond);
impl_data_type_for_timestamp!(Second);
impl_data_type_for_timestamp!(Millisecond);
impl_data_type_for_timestamp!(Microsecond);

#[cfg(test)]
mod tests {
    use common_time::timezone::set_default_timezone;
    use common_time::{Date, DateTime};

    use super::*;

    #[test]
    fn test_timestamp_type_unit() {
        assert_eq!(
            TimeUnit::Second,
            TimestampType::Second(TimestampSecondType).unit()
        );
        assert_eq!(
            TimeUnit::Millisecond,
            TimestampType::Millisecond(TimestampMillisecondType).unit()
        );
        assert_eq!(
            TimeUnit::Microsecond,
            TimestampType::Microsecond(TimestampMicrosecondType).unit()
        );
        assert_eq!(
            TimeUnit::Nanosecond,
            TimestampType::Nanosecond(TimestampNanosecondType).unit()
        );
    }

    // $TZ doesn't take effort
    #[test]
    fn test_timestamp_cast() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        // String -> TimestampSecond
        let s = Value::String("2021-01-01 01:02:03".to_string().into());
        let ts = ConcreteDataType::timestamp_second_datatype()
            .try_cast(s)
            .unwrap();
        // 1609462923 is 2021-01-01T01:02:03Z
        assert_eq!(ts, Value::Timestamp(Timestamp::new_second(1609462923)));
        // String cast failed
        let s = Value::String("12345".to_string().into());
        let ts = ConcreteDataType::timestamp_second_datatype().try_cast(s);
        assert_eq!(ts, None);

        let n = Value::Int64(1694589525);
        // Int64 -> TimestampSecond
        let ts = ConcreteDataType::timestamp_second_datatype()
            .try_cast(n)
            .unwrap();
        assert_eq!(ts, Value::Timestamp(Timestamp::new_second(1694589525)));

        // Datetime -> TimestampSecond
        let dt = Value::DateTime(DateTime::from(1234567));
        let ts = ConcreteDataType::timestamp_second_datatype()
            .try_cast(dt)
            .unwrap();
        assert_eq!(ts, Value::Timestamp(Timestamp::new_second(1234567)));

        // Date -> TimestampMillisecond
        let d = Value::Date(Date::from_str("1970-01-01").unwrap());
        let ts = ConcreteDataType::timestamp_millisecond_datatype()
            .try_cast(d)
            .unwrap();
        assert_eq!(ts, Value::Timestamp(Timestamp::new_millisecond(0)));

        // TimestampSecond -> TimestampMicrosecond
        let second = Value::Timestamp(Timestamp::new_second(123));
        let microsecond = ConcreteDataType::timestamp_microsecond_datatype()
            .try_cast(second)
            .unwrap();
        assert_eq!(
            microsecond,
            Value::Timestamp(Timestamp::new_microsecond(123 * 1000000))
        )
    }
}
