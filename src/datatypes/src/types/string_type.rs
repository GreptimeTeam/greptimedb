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

use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use common_base::bytes::StringBytes;
use serde::{Deserialize, Serialize};

use crate::data_type::{DataType, DataTypeRef};
use crate::prelude::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{MutableVector, StringVectorBuilder};

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StringType;

impl StringType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }
}

impl DataType for StringType {
    fn name(&self) -> String {
        "String".to_string()
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::String
    }

    fn default_value(&self) -> Value {
        StringBytes::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Utf8
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(StringVectorBuilder::with_capacity(capacity))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        if from.logical_type_id() == self.logical_type_id() {
            return Some(from);
        }

        match from {
            Value::Null => Some(Value::String(StringBytes::from("null".to_string()))),

            Value::Boolean(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::UInt8(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::UInt16(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::UInt32(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::UInt64(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::Int8(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::Int16(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::Int32(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::Int64(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::Float32(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::Float64(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::String(v) => Some(Value::String(v)),
            Value::Date(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::DateTime(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::Timestamp(v) => Some(Value::String(StringBytes::from(v.to_iso8601_string()))),
            Value::Time(v) => Some(Value::String(StringBytes::from(v.to_iso8601_string()))),
            Value::Interval(v) => Some(Value::String(StringBytes::from(v.to_iso8601_string()))),
            Value::Duration(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::Decimal128(v) => Some(Value::String(StringBytes::from(v.to_string()))),

            // StringBytes is only support for utf-8, Value::Binary is not allowed.
            Value::Binary(_) | Value::List(_) => None,
        }
    }
}
