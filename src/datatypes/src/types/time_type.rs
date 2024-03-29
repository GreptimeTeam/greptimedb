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

//! TimeType represents the elapsed time since midnight in the unit of `TimeUnit`.

use arrow::datatypes::{
    DataType as ArrowDataType, Time32MillisecondType as ArrowTimeMillisecondType,
    Time32SecondType as ArrowTimeSecondType, Time64MicrosecondType as ArrowTimeMicrosecondType,
    Time64NanosecondType as ArrowTimeNanosecondType, TimeUnit as ArrowTimeUnit,
};
use common_time::time::Time;
use common_time::timestamp::TimeUnit;
use enum_dispatch::enum_dispatch;
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error;
use crate::prelude::{
    ConcreteDataType, DataType, LogicalTypeId, MutableVector, ScalarVectorBuilder, Value, ValueRef,
    Vector,
};
use crate::time::{TimeMicrosecond, TimeMillisecond, TimeNanosecond, TimeSecond};
use crate::types::LogicalPrimitiveType;
use crate::vectors::{
    PrimitiveVector, TimeMicrosecondVector, TimeMicrosecondVectorBuilder, TimeMillisecondVector,
    TimeMillisecondVectorBuilder, TimeNanosecondVector, TimeNanosecondVectorBuilder,
    TimeSecondVector, TimeSecondVectorBuilder,
};

const SECOND_VARIATION: u64 = 0;
const MILLISECOND_VARIATION: u64 = 3;
const MICROSECOND_VARIATION: u64 = 6;
const NANOSECOND_VARIATION: u64 = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[enum_dispatch(DataType)]
pub enum TimeType {
    Second(TimeSecondType),
    Millisecond(TimeMillisecondType),
    Microsecond(TimeMicrosecondType),
    Nanosecond(TimeNanosecondType),
}

impl TimeType {
    /// Creates time type from `TimeUnit`.
    pub fn from_unit(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::Second(TimeSecondType),
            TimeUnit::Millisecond => Self::Millisecond(TimeMillisecondType),
            TimeUnit::Microsecond => Self::Microsecond(TimeMicrosecondType),
            TimeUnit::Nanosecond => Self::Nanosecond(TimeNanosecondType),
        }
    }

    /// Returns the time type's `TimeUnit`.
    pub fn unit(&self) -> TimeUnit {
        match self {
            TimeType::Second(_) => TimeUnit::Second,
            TimeType::Millisecond(_) => TimeUnit::Millisecond,
            TimeType::Microsecond(_) => TimeUnit::Microsecond,
            TimeType::Nanosecond(_) => TimeUnit::Nanosecond,
        }
    }

    /// Returns the time type's precision.
    pub fn precision(&self) -> u64 {
        match self {
            TimeType::Second(_) => SECOND_VARIATION,
            TimeType::Millisecond(_) => MILLISECOND_VARIATION,
            TimeType::Microsecond(_) => MICROSECOND_VARIATION,
            TimeType::Nanosecond(_) => NANOSECOND_VARIATION,
        }
    }
}

macro_rules! impl_data_type_for_time {
    ($unit: ident,$arrow_type: ident, $type: ty, $TargetType: ident) => {
        paste! {
            #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
            pub struct [<Time $unit Type>];

            impl DataType for [<Time $unit Type>] {
                fn name(&self) -> String {
                    stringify!([<Time $unit>]).to_string()
                }

                fn logical_type_id(&self) -> LogicalTypeId {
                    LogicalTypeId::[<Time $unit>]
                }

                fn default_value(&self) -> Value {
                    Value::Time(Time::new(0, TimeUnit::$unit))
                }

                fn as_arrow_type(&self) -> ArrowDataType {
                    ArrowDataType::$arrow_type(ArrowTimeUnit::$unit)
                }

                fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
                    Box::new([<Time $unit Vector Builder>]::with_capacity(capacity))
                }

                fn try_cast(&self, from: Value) -> Option<Value> {
                    match from {
                        Value::$TargetType(v) => Some(Value::Time(Time::new(v as i64, TimeUnit::$unit))),
                        Value::Time(v) => v.convert_to(TimeUnit::$unit).map(Value::Time),
                        _ => None,
                    }
                }
            }

            impl LogicalPrimitiveType for [<Time $unit Type>] {
                type ArrowPrimitive = [<Arrow Time $unit Type>];
                type Native = $type;
                type Wrapper = [<Time $unit>];
                type LargestType = Self;

                fn build_data_type() -> ConcreteDataType {
                    ConcreteDataType::Time(TimeType::$unit(
                        [<Time $unit Type>]::default(),
                    ))
                }

                fn type_name() -> &'static str {
                    stringify!([<Time $unit Type>])
                }

                fn cast_vector(vector: &dyn Vector) -> crate::Result<&PrimitiveVector<Self>> {
                    vector
                        .as_any()
                        .downcast_ref::<[<Time $unit Vector>]>()
                        .with_context(|| error::CastTypeSnafu {
                            msg: format!(
                                "Failed to cast {} to {}",
                                vector.vector_type_name(), stringify!([<Time $unit Vector>])
                            ),
                        })
                }

                fn cast_value_ref(value: ValueRef) -> crate::Result<Option<Self::Wrapper>> {
                    match value {
                        ValueRef::Null => Ok(None),
                        ValueRef::Int64(v) =>{
                            Ok(Some([<Time $unit>]::from(v)))
                        }
                        ValueRef::Time(t) => match t.unit() {
                            TimeUnit::$unit => Ok(Some([<Time $unit>](t))),
                            other => error::CastTypeSnafu {
                                msg: format!(
                                    "Failed to cast Time value with different unit {:?} to {}",
                                    other, stringify!([<Time $unit>])
                                ),
                            }
                            .fail(),
                        },
                        other => error::CastTypeSnafu {
                            msg: format!("Failed to cast value {:?} to {}", other, stringify!([<Time $unit>])),
                        }
                        .fail(),
                    }
                }
            }
        }
    }
}

impl_data_type_for_time!(Second, Time32, i32, Int32);
impl_data_type_for_time!(Millisecond, Time32, i32, Int32);
impl_data_type_for_time!(Nanosecond, Time64, i64, Int64);
impl_data_type_for_time!(Microsecond, Time64, i64, Int64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_type_unit() {
        assert_eq!(TimeUnit::Second, TimeType::Second(TimeSecondType).unit());
        assert_eq!(
            TimeUnit::Millisecond,
            TimeType::Millisecond(TimeMillisecondType).unit()
        );
        assert_eq!(
            TimeUnit::Microsecond,
            TimeType::Microsecond(TimeMicrosecondType).unit()
        );
        assert_eq!(
            TimeUnit::Nanosecond,
            TimeType::Nanosecond(TimeNanosecondType).unit()
        );
    }

    #[test]
    fn test_as_arrow_datatype() {
        assert_eq!(
            ArrowDataType::Time32(ArrowTimeUnit::Second),
            TimeSecondType.as_arrow_type()
        );
        assert_eq!(
            ArrowDataType::Time32(ArrowTimeUnit::Millisecond),
            TimeMillisecondType.as_arrow_type()
        );
        assert_eq!(
            ArrowDataType::Time64(ArrowTimeUnit::Microsecond),
            TimeMicrosecondType.as_arrow_type()
        );
        assert_eq!(
            ArrowDataType::Time64(ArrowTimeUnit::Nanosecond),
            TimeNanosecondType.as_arrow_type()
        );
    }

    #[test]
    fn test_time_cast() {
        // Int32 -> TimeSecondType
        let val = Value::Int32(1000);
        let time = ConcreteDataType::time_second_datatype()
            .try_cast(val)
            .unwrap();
        assert_eq!(time, Value::Time(Time::new_second(1000)));

        // Int32 -> TimeMillisecondType
        let val = Value::Int32(2000);
        let time = ConcreteDataType::time_millisecond_datatype()
            .try_cast(val)
            .unwrap();
        assert_eq!(time, Value::Time(Time::new_millisecond(2000)));

        // Int64 -> TimeMicrosecondType
        let val = Value::Int64(3000);
        let time = ConcreteDataType::time_microsecond_datatype()
            .try_cast(val)
            .unwrap();
        assert_eq!(time, Value::Time(Time::new_microsecond(3000)));

        // Int64 -> TimeNanosecondType
        let val = Value::Int64(4000);
        let time = ConcreteDataType::time_nanosecond_datatype()
            .try_cast(val)
            .unwrap();
        assert_eq!(time, Value::Time(Time::new_nanosecond(4000)));

        // Other situations will return None, such as Int64 -> TimeSecondType or
        // Int32 -> TimeMicrosecondType etc.
        let val = Value::Int64(123);
        let time = ConcreteDataType::time_second_datatype().try_cast(val);
        assert_eq!(time, None);

        let val = Value::Int32(123);
        let time = ConcreteDataType::time_microsecond_datatype().try_cast(val);
        assert_eq!(time, None);

        // TimeSecond -> TimeMicroSecond
        let second = Value::Time(Time::new_second(2023));
        let microsecond = ConcreteDataType::time_microsecond_datatype()
            .try_cast(second)
            .unwrap();
        assert_eq!(
            microsecond,
            Value::Time(Time::new_microsecond(2023 * 1000000))
        );

        // test overflow
        let second = Value::Time(Time::new_second(i64::MAX));
        let microsecond = ConcreteDataType::time_microsecond_datatype().try_cast(second);
        assert_eq!(microsecond, None);
    }
}
