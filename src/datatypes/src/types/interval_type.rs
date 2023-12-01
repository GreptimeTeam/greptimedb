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
    DataType as ArrowDataType, IntervalDayTimeType as ArrowIntervalDayTimeType,
    IntervalMonthDayNanoType as ArrowIntervalMonthDayNanoType, IntervalUnit as ArrowIntervalUnit,
    IntervalYearMonthType as ArrowIntervalYearMonthType,
};
use common_time::interval::IntervalUnit;
use common_time::Interval;
use enum_dispatch::enum_dispatch;
use paste::paste;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::data_type::ConcreteDataType;
use crate::error;
use crate::interval::{IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use crate::prelude::{
    DataType, LogicalTypeId, MutableVector, ScalarVectorBuilder, Value, ValueRef, Vector,
};
use crate::types::LogicalPrimitiveType;
use crate::vectors::{
    IntervalDayTimeVector, IntervalDayTimeVectorBuilder, IntervalMonthDayNanoVector,
    IntervalMonthDayNanoVectorBuilder, IntervalYearMonthVector, IntervalYearMonthVectorBuilder,
    PrimitiveVector,
};

/// The "calendar" interval is a type of time interval that does not
/// have a precise duration without taking into account a specific
/// base timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[enum_dispatch(DataType)]
pub enum IntervalType {
    YearMonth(IntervalYearMonthType),
    DayTime(IntervalDayTimeType),
    MonthDayNano(IntervalMonthDayNanoType),
}

impl IntervalType {
    /// Returns the unit of the interval.
    pub fn unit(&self) -> IntervalUnit {
        match self {
            IntervalType::YearMonth(_) => IntervalUnit::YearMonth,
            IntervalType::DayTime(_) => IntervalUnit::DayTime,
            IntervalType::MonthDayNano(_) => IntervalUnit::MonthDayNano,
        }
    }
}

macro_rules! impl_data_type_for_interval {
    ($unit: ident, $type: ty) => {
        paste! {
            #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
            pub struct [<Interval $unit Type>];

            impl DataType for [<Interval $unit Type>] {
                fn name(&self) -> String {
                    stringify!([<Interval $unit>]).to_string()
                }

                fn logical_type_id(&self) -> LogicalTypeId {
                    LogicalTypeId::[<Interval $unit>]
                }

                fn default_value(&self) -> Value {
                    Value::Interval(Interval::from_i128(0))
                }

                fn as_arrow_type(&self) -> ArrowDataType {
                    ArrowDataType::Interval(ArrowIntervalUnit::$unit)
                }

                fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
                    Box::new([<Interval $unit Vector Builder>]::with_capacity(capacity))
                }


                fn try_cast(&self, _: Value) -> Option<Value> {
                    // TODO(QuenKar): Implement casting for interval types.
                    None
                }
            }

            impl LogicalPrimitiveType for [<Interval $unit Type>] {
                type ArrowPrimitive = [<Arrow Interval $unit Type>];
                type Native = $type;
                type Wrapper = [<Interval $unit>];
                type LargestType = Self;

                fn build_data_type() -> ConcreteDataType {
                    ConcreteDataType::Interval(IntervalType::$unit(
                        [<Interval $unit Type>]::default(),
                    ))
                }

                fn type_name() -> &'static str {
                    stringify!([<Interval $unit Type>])
                }

                fn cast_vector(vector: &dyn Vector) -> crate::Result<&PrimitiveVector<Self>> {
                    vector
                        .as_any()
                        .downcast_ref::<[<Interval $unit Vector>]>()
                        .with_context(|| error::CastTypeSnafu {
                            msg: format!(
                                "Failed to cast {} to {}",
                                vector.vector_type_name(), stringify!([<Interval $unit Vector>])
                            ),
                        })
                }

                fn cast_value_ref(value: ValueRef) -> crate::Result<Option<Self::Wrapper>> {
                    match value {
                        ValueRef::Null => Ok(None),
                        ValueRef::Interval(t) => Ok(Some([<Interval $unit>](t))),
                        other => error::CastTypeSnafu {
                            msg: format!("Failed to cast value {:?} to {}", other, stringify!([<Interval $unit>])),
                        }
                        .fail(),
                    }
                }
            }
        }
    }
}

impl_data_type_for_interval!(YearMonth, i32);
impl_data_type_for_interval!(DayTime, i64);
impl_data_type_for_interval!(MonthDayNano, i128);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_type_unit() {
        assert_eq!(
            IntervalUnit::DayTime,
            IntervalType::DayTime(IntervalDayTimeType).unit()
        );
        assert_eq!(
            IntervalUnit::MonthDayNano,
            IntervalType::MonthDayNano(IntervalMonthDayNanoType).unit()
        );
        assert_eq!(
            IntervalUnit::YearMonth,
            IntervalType::YearMonth(IntervalYearMonthType).unit()
        );
    }

    #[test]
    fn test_interval_as_arrow_type() {
        assert_eq!(
            ArrowDataType::Interval(ArrowIntervalUnit::DayTime),
            IntervalType::DayTime(IntervalDayTimeType).as_arrow_type()
        );
        assert_eq!(
            ArrowDataType::Interval(ArrowIntervalUnit::MonthDayNano),
            IntervalType::MonthDayNano(IntervalMonthDayNanoType).as_arrow_type()
        );
        assert_eq!(
            ArrowDataType::Interval(ArrowIntervalUnit::YearMonth),
            IntervalType::YearMonth(IntervalYearMonthType).as_arrow_type()
        );
    }
}
