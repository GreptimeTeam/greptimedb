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
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{MutableVector, StringVectorBuilder};

/// String size variant to distinguish between UTF8 and LargeUTF8
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub enum StringSizeType {
    /// Regular UTF8 strings (up to 2GB)
    #[default]
    Utf8,
    /// Large UTF8 strings (up to 2^63 bytes)
    LargeUtf8,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct StringType {
    #[serde(default)]
    size_type: StringSizeType,
}

/// Custom deserialization to support both old and new formats.
impl<'de> serde::Deserialize<'de> for StringType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Helper {
            #[serde(default)]
            size_type: StringSizeType,
        }

        let opt = Option::<Helper>::deserialize(deserializer)?;
        Ok(match opt {
            Some(helper) => Self {
                size_type: helper.size_type,
            },
            None => Self::default(),
        })
    }
}

impl Default for StringType {
    fn default() -> Self {
        Self {
            size_type: StringSizeType::Utf8,
        }
    }
}

impl StringType {
    /// Create a new StringType with default (Utf8) size
    pub fn new() -> Self {
        Self {
            size_type: StringSizeType::Utf8,
        }
    }

    /// Create a new StringType with specified size
    pub fn with_size(size_type: StringSizeType) -> Self {
        Self { size_type }
    }

    /// Create a StringType for regular UTF8 strings
    pub fn utf8() -> Self {
        Self::with_size(StringSizeType::Utf8)
    }

    /// Create a StringType for large UTF8 strings
    pub fn large_utf8() -> Self {
        Self::with_size(StringSizeType::LargeUtf8)
    }

    /// Get the size type
    pub fn size_type(&self) -> StringSizeType {
        self.size_type
    }

    /// Check if this is a large UTF8 string type
    pub fn is_large(&self) -> bool {
        matches!(self.size_type, StringSizeType::LargeUtf8)
    }

    pub fn arc() -> DataTypeRef {
        Arc::new(Self::new())
    }

    pub fn large_arc() -> DataTypeRef {
        Arc::new(Self::large_utf8())
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
        match self.size_type {
            StringSizeType::Utf8 => ArrowDataType::Utf8,
            StringSizeType::LargeUtf8 => ArrowDataType::LargeUtf8,
        }
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        match self.size_type {
            StringSizeType::Utf8 => Box::new(StringVectorBuilder::with_string_capacity(capacity)),
            StringSizeType::LargeUtf8 => {
                Box::new(StringVectorBuilder::with_large_capacity(capacity))
            }
        }
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
            Value::Timestamp(v) => Some(Value::String(StringBytes::from(v.to_iso8601_string()))),
            Value::Time(v) => Some(Value::String(StringBytes::from(v.to_iso8601_string()))),
            Value::IntervalYearMonth(v) => {
                Some(Value::String(StringBytes::from(v.to_iso8601_string())))
            }
            Value::IntervalDayTime(v) => {
                Some(Value::String(StringBytes::from(v.to_iso8601_string())))
            }
            Value::IntervalMonthDayNano(v) => {
                Some(Value::String(StringBytes::from(v.to_iso8601_string())))
            }
            Value::Duration(v) => Some(Value::String(StringBytes::from(v.to_string()))),
            Value::Decimal128(v) => Some(Value::String(StringBytes::from(v.to_string()))),

            Value::Json(v) => serde_json::to_string(v.as_ref()).ok().map(|s| s.into()),

            // StringBytes is only support for utf-8, Value::Binary and collections are not allowed.
            Value::Binary(_) | Value::List(_) | Value::Struct(_) => None,
        }
    }
}
