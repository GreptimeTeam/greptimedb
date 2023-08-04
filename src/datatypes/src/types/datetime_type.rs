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

use arrow::datatypes::{DataType as ArrowDataType, Date64Type};
use common_time::DateTime;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Result};
use crate::prelude::{LogicalTypeId, MutableVector, ScalarVectorBuilder, Value, ValueRef, Vector};
use crate::types::LogicalPrimitiveType;
use crate::vectors::{DateTimeVector, DateTimeVectorBuilder, PrimitiveVector};

/// Data type for [`DateTime`].
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DateTimeType;

impl DataType for DateTimeType {
    fn name(&self) -> &str {
        "DateTime"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::DateTime
    }

    fn default_value(&self) -> Value {
        Value::DateTime(DateTime::default())
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Date64
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(DateTimeVectorBuilder::with_capacity(capacity))
    }

    fn is_timestamp_compatible(&self) -> bool {
        false
    }
}

impl LogicalPrimitiveType for DateTimeType {
    type ArrowPrimitive = Date64Type;
    type Native = i64;
    type Wrapper = DateTime;
    type LargestType = Self;

    fn build_data_type() -> ConcreteDataType {
        ConcreteDataType::datetime_datatype()
    }

    fn type_name() -> &'static str {
        "DateTime"
    }

    fn cast_vector(vector: &dyn Vector) -> Result<&PrimitiveVector<Self>> {
        vector
            .as_any()
            .downcast_ref::<DateTimeVector>()
            .with_context(|| error::CastTypeSnafu {
                msg: format!(
                    "Failed to cast {} to DateTimeVector",
                    vector.vector_type_name()
                ),
            })
    }

    fn cast_value_ref(value: ValueRef) -> Result<Option<Self::Wrapper>> {
        match value {
            ValueRef::Null => Ok(None),
            ValueRef::DateTime(v) => Ok(Some(v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value {other:?} to DateTime"),
            }
            .fail(),
        }
    }
}
