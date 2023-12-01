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

use arrow::datatypes::{
    DataType as ArrowDataType, DurationMicrosecondType as ArrowDurationMicrosecondType,
    DurationMillisecondType as ArrowDurationMillisecondType,
    DurationNanosecondType as ArrowDurationNanosecondType,
    DurationSecondType as ArrowDurationSecondType, TimeUnit as ArrowTimeUnit,
};
use common_time::duration::Duration;
use common_time::timestamp::TimeUnit;
use enum_dispatch::enum_dispatch;
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use super::LogicalPrimitiveType;
use crate::data_type::DataType;
use crate::duration::{
    DurationMicrosecond, DurationMillisecond, DurationNanosecond, DurationSecond,
};
use crate::error;
use crate::prelude::{
    ConcreteDataType, LogicalTypeId, MutableVector, ScalarVectorBuilder, Value, ValueRef, Vector,
};
use crate::vectors::{
    DurationMicrosecondVector, DurationMicrosecondVectorBuilder, DurationMillisecondVector,
    DurationMillisecondVectorBuilder, DurationNanosecondVector, DurationNanosecondVectorBuilder,
    DurationSecondVector, DurationSecondVectorBuilder, PrimitiveVector,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[enum_dispatch(DataType)]
pub enum DurationType {
    Second(DurationSecondType),
    Millisecond(DurationMillisecondType),
    Microsecond(DurationMicrosecondType),
    Nanosecond(DurationNanosecondType),
}

impl DurationType {
    /// Creates time type from `TimeUnit`.
    pub fn from_unit(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::Second(DurationSecondType),
            TimeUnit::Millisecond => Self::Millisecond(DurationMillisecondType),
            TimeUnit::Microsecond => Self::Microsecond(DurationMicrosecondType),
            TimeUnit::Nanosecond => Self::Nanosecond(DurationNanosecondType),
        }
    }

    /// Returns the [`TimeUnit`] of this type.
    pub fn unit(&self) -> TimeUnit {
        match self {
            DurationType::Second(_) => TimeUnit::Second,
            DurationType::Millisecond(_) => TimeUnit::Millisecond,
            DurationType::Microsecond(_) => TimeUnit::Microsecond,
            DurationType::Nanosecond(_) => TimeUnit::Nanosecond,
        }
    }
}

macro_rules! impl_data_type_for_duration {
    ($unit: ident) => {
        paste! {
            #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
            pub struct [<Duration $unit Type>];

            impl DataType for [<Duration $unit Type>] {
                fn name(&self) -> String {
                    stringify!([<Duration $unit>]).to_string()
                }

                fn logical_type_id(&self) -> LogicalTypeId {
                    LogicalTypeId::[<Duration $unit>]
                }

                fn default_value(&self) -> Value {
                    Value::Duration(Duration::new(0, TimeUnit::$unit))
                }

                fn as_arrow_type(&self) -> ArrowDataType {
                    ArrowDataType::Duration(ArrowTimeUnit::$unit)
                }

                fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
                    Box::new([<Duration $unit Vector Builder>]::with_capacity(capacity))
                }


                fn try_cast(&self, _: Value) -> Option<Value> {
                    // TODO(QuenKar): Implement casting for duration types.
                    None
                }
            }

            impl LogicalPrimitiveType for [<Duration $unit Type>] {
                type ArrowPrimitive = [<Arrow Duration $unit Type>];
                type Native = i64;
                type Wrapper = [<Duration $unit>];
                type LargestType = Self;

                fn build_data_type() -> ConcreteDataType {
                    ConcreteDataType::Duration(DurationType::$unit(
                        [<Duration $unit Type>]::default(),
                    ))
                }

                fn type_name() -> &'static str {
                    stringify!([<Duration $unit Type>])
                }

                fn cast_vector(vector: &dyn Vector) -> crate::Result<&PrimitiveVector<Self>> {
                    vector
                        .as_any()
                        .downcast_ref::<[<Duration $unit Vector>]>()
                        .with_context(|| error::CastTypeSnafu {
                            msg: format!(
                                "Failed to cast {} to {}",
                                vector.vector_type_name(), stringify!([<Duration $unit Vector>])
                            ),
                        })
                }

                fn cast_value_ref(value: ValueRef) -> crate::Result<Option<Self::Wrapper>> {
                    match value {
                        ValueRef::Null => Ok(None),
                        ValueRef::Duration(t) => match t.unit() {
                            TimeUnit::$unit => Ok(Some([<Duration $unit>](t))),
                            other => error::CastTypeSnafu {
                                msg: format!(
                                    "Failed to cast Duration value with different unit {:?} to {}",
                                    other, stringify!([<Duration $unit>])
                                ),
                            }
                            .fail(),
                        },
                        other => error::CastTypeSnafu {
                            msg: format!("Failed to cast value {:?} to {}", other, stringify!([<Duration $unit>])),
                        }
                        .fail(),
                    }
                }
            }
        }
    }
}

impl_data_type_for_duration!(Second);
impl_data_type_for_duration!(Millisecond);
impl_data_type_for_duration!(Microsecond);
impl_data_type_for_duration!(Nanosecond);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duration_type_unit() {
        assert_eq!(
            TimeUnit::Second,
            DurationType::Second(DurationSecondType).unit()
        );

        assert_eq!(
            TimeUnit::Millisecond,
            DurationType::Millisecond(DurationMillisecondType).unit()
        );

        assert_eq!(
            TimeUnit::Microsecond,
            DurationType::Microsecond(DurationMicrosecondType).unit()
        );

        assert_eq!(
            TimeUnit::Nanosecond,
            DurationType::Nanosecond(DurationNanosecondType).unit()
        );
    }

    #[test]
    fn test_from_unit() {
        assert_eq!(
            DurationType::Second(DurationSecondType),
            DurationType::from_unit(TimeUnit::Second)
        );

        assert_eq!(
            DurationType::Millisecond(DurationMillisecondType),
            DurationType::from_unit(TimeUnit::Millisecond)
        );

        assert_eq!(
            DurationType::Microsecond(DurationMicrosecondType),
            DurationType::from_unit(TimeUnit::Microsecond)
        );

        assert_eq!(
            DurationType::Nanosecond(DurationNanosecondType),
            DurationType::from_unit(TimeUnit::Nanosecond)
        );
    }
}
