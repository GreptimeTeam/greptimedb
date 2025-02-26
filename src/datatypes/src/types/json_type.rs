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

use arrow::datatypes::DataType as ArrowDataType;
use common_base::bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::data_type::DataType;
use crate::error::{InvalidJsonSnafu, Result};
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{BinaryVectorBuilder, MutableVector};

pub const JSON_TYPE_NAME: &str = "Json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum JsonFormat {
    Jsonb,
}

impl Default for JsonFormat {
    fn default() -> Self {
        Self::Jsonb
    }
}

/// JsonType is a data type for JSON data. It is stored as binary data of jsonb format.
/// It utilizes current binary value and vector implementation.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct JsonType {
    pub format: JsonFormat,
}

impl JsonType {
    pub fn new(format: JsonFormat) -> Self {
        Self { format }
    }
}

impl DataType for JsonType {
    fn name(&self) -> String {
        JSON_TYPE_NAME.to_string()
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Json
    }

    fn default_value(&self) -> Value {
        Bytes::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Binary
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(BinaryVectorBuilder::with_capacity(capacity))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Binary(v) => Some(Value::Binary(v)),
            _ => None,
        }
    }
}

/// Converts a json type value to string
pub fn json_type_value_to_string(val: &[u8], format: &JsonFormat) -> Result<String> {
    match format {
        JsonFormat::Jsonb => Ok(jsonb::to_string(val)),
    }
}

/// Parses a string to a json type value
pub fn parse_string_to_json_type_value(s: &str, format: &JsonFormat) -> Result<Vec<u8>> {
    match format {
        JsonFormat::Jsonb => jsonb::parse_value(s.as_bytes())
            .map_err(|_| InvalidJsonSnafu { value: s }.build())
            .map(|json| json.to_vec()),
    }
}
