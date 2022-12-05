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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[enum_dispatch(DataType)]
pub enum TimestampType {
    Second(TimestampSecondType),
    Millisecond(TimestampMillisecondType),
    Microsecond(TimestampMicrosecondType),
    Nanosecond(TimestampNanosecondType),
}

macro_rules! impl_data_type_for_timestamp {
    ($unit: ident) => {
        paste! {
            #[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
            pub struct [<Timestamp $unit Type>];

            impl DataType for [<Timestamp $unit Type>] {
                fn name(&self) -> &str {
                    stringify!([<Timestamp $unit Type>])
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

                fn is_timestamp_compatible(&self) -> bool {
                    true
                }
            }


            impl LogicalPrimitiveType for [<Timestamp $unit Type>] {
                type ArrowPrimitive = [<Arrow Timestamp $unit Type>];
                type Native = i64;
                type Wrapper = [<Timestamp $unit>];

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
