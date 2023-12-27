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

use arrow::datatypes::{DataType as ArrowDataType, Date32Type};
use common_time::Date;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Result};
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::types::LogicalPrimitiveType;
use crate::value::{Value, ValueRef};
use crate::vectors::{DateVector, DateVectorBuilder, MutableVector, Vector};

/// Data type for Date (YYYY-MM-DD).
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DateType;

impl DataType for DateType {
    fn name(&self) -> String {
        "Date".to_string()
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Date
    }

    fn default_value(&self) -> Value {
        Value::Date(Default::default())
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Date32
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(DateVectorBuilder::with_capacity(capacity))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Int32(v) => Some(Value::Date(Date::from(v))),
            Value::String(v) => Date::from_str(v.as_utf8()).map(Value::Date).ok(),
            Value::Timestamp(v) => v.to_chrono_date().map(|date| Value::Date(date.into())),
            Value::DateTime(v) => Some(Value::DateTime(v)),
            _ => None,
        }
    }
}

impl LogicalPrimitiveType for DateType {
    type ArrowPrimitive = Date32Type;
    type Native = i32;
    type Wrapper = Date;
    type LargestType = Self;

    fn build_data_type() -> ConcreteDataType {
        ConcreteDataType::date_datatype()
    }

    fn type_name() -> &'static str {
        "Date"
    }

    fn cast_vector(vector: &dyn Vector) -> Result<&DateVector> {
        vector
            .as_any()
            .downcast_ref::<DateVector>()
            .with_context(|| error::CastTypeSnafu {
                msg: format!("Failed to cast {} to DateVector", vector.vector_type_name(),),
            })
    }

    fn cast_value_ref(value: ValueRef) -> Result<Option<Date>> {
        match value {
            ValueRef::Null => Ok(None),
            ValueRef::Date(v) => Ok(Some(v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value {other:?} to Date"),
            }
            .fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_base::bytes::StringBytes;
    use common_time::timezone::set_default_timezone;
    use common_time::Timestamp;

    use super::*;

    // $TZ doesn't take effort
    #[test]
    fn test_date_cast() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        // timestamp -> date
        let ts = Value::Timestamp(Timestamp::from_str("2000-01-01 08:00:01").unwrap());
        let date = ConcreteDataType::date_datatype().try_cast(ts).unwrap();
        assert_eq!(date, Value::Date(Date::from_str("2000-01-01").unwrap()));

        // this case bind with Zulu timezone.
        let ts = Value::Timestamp(Timestamp::from_str("2000-01-02 07:59:59").unwrap());
        let date = ConcreteDataType::date_datatype().try_cast(ts).unwrap();
        assert_eq!(date, Value::Date(Date::from_str("2000-01-02").unwrap()));

        // while this case is offsetted to Asia/Shanghai.
        let ts = Value::Timestamp(Timestamp::from_str("2000-01-02 07:59:59+08:00").unwrap());
        let date = ConcreteDataType::date_datatype().try_cast(ts).unwrap();
        assert_eq!(date, Value::Date(Date::from_str("2000-01-01").unwrap()));

        // Int32 -> date
        let val = Value::Int32(0);
        let date = ConcreteDataType::date_datatype().try_cast(val).unwrap();
        assert_eq!(date, Value::Date(Date::from_str("1970-01-01").unwrap()));

        let val = Value::Int32(19614);
        let date = ConcreteDataType::date_datatype().try_cast(val).unwrap();
        assert_eq!(date, Value::Date(Date::from_str("2023-09-14").unwrap()));

        // String -> date
        let s = Value::String(StringBytes::from("1970-02-12"));
        let date = ConcreteDataType::date_datatype().try_cast(s).unwrap();
        assert_eq!(date, Value::Date(Date::from_str("1970-02-12").unwrap()));
    }
}
