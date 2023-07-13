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
    DataType as ArrowDataType, IntervalMonthDayNanoType as ArrowIntervalMonthDayNanoType,
    IntervalUnit as ArrowIntervalUnit,
};
use common_time::Interval;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use super::LogicalPrimitiveType;
use crate::data_type::{ConcreteDataType, DataType};
use crate::error;
use crate::prelude::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{IntervalVector, IntervalVectorBuilder};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]

pub struct IntervalMonthDayNanoType;

impl DataType for IntervalMonthDayNanoType {
    fn name(&self) -> &str {
        "IntervalMonthDayNanoType"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::IntervalMonthDayNano
    }

    fn default_value(&self) -> crate::value::Value {
        Value::Interval(Default::default())
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Interval(ArrowIntervalUnit::MonthDayNano)
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn crate::vectors::MutableVector> {
        Box::new(IntervalVectorBuilder::with_capacity(capacity))
    }

    fn is_timestamp_compatible(&self) -> bool {
        false
    }
}

impl LogicalPrimitiveType for IntervalMonthDayNanoType {
    type ArrowPrimitive = ArrowIntervalMonthDayNanoType;

    type Native = i128;

    type Wrapper = Interval;

    type LargestType = Self;

    fn build_data_type() -> ConcreteDataType {
        ConcreteDataType::Interval(IntervalMonthDayNanoType::default())
    }

    fn type_name() -> &'static str {
        stringify!(IntervalMonthDayNanoType)
    }

    fn cast_vector(
        vector: &dyn crate::vectors::Vector,
    ) -> crate::Result<&crate::vectors::PrimitiveVector<Self>> {
        vector
            .as_any()
            .downcast_ref::<IntervalVector>()
            .with_context(|| error::CastTypeSnafu {
                msg: format!("Failed to cast vector to {}", Self::type_name()),
            })
    }

    fn cast_value_ref(value: crate::value::ValueRef) -> crate::Result<Option<Self::Wrapper>> {
        match value {
            crate::value::ValueRef::Null => Ok(None),
            crate::value::ValueRef::Interval(interval) => Ok(Some(interval)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value {:?} to {}", other, Self::type_name()),
            }
            .fail(),
        }
    }
}
