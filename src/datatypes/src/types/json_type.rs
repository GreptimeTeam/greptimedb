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

use arrow::datatypes::DataType as ArrowDataType;
use arrow_schema::Fields;
use common_base::bytes::Bytes;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::data_type::DataType;
use crate::error::{DeserializeSnafu, InvalidJsonSnafu, InvalidJsonbSnafu, Result};
use crate::prelude::ConcreteDataType;
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{BinaryVectorBuilder, MutableVector};

pub const JSON_TYPE_NAME: &str = "Json";

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum JsonFormat {
    #[default]
    Jsonb,
    Native(Box<ConcreteDataType>),
}

/// JsonType is a data type for JSON data. It is stored as binary data of jsonb format.
/// It utilizes current binary value and vector implementation.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
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
        match self.format {
            JsonFormat::Jsonb => ArrowDataType::Binary,
            JsonFormat::Native(_) => ArrowDataType::Struct(Fields::empty()),
        }
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
pub fn jsonb_to_string(val: &[u8]) -> Result<String> {
    match jsonb::from_slice(val) {
        Ok(jsonb_value) => {
            let serialized = jsonb_value.to_string();
            Ok(serialized)
        }
        Err(e) => InvalidJsonbSnafu { error: e }.fail(),
    }
}

/// Converts a json type value to serde_json::Value
pub fn jsonb_to_serde_json(val: &[u8]) -> Result<serde_json::Value> {
    let json_string = jsonb_to_string(val)?;
    serde_json::Value::from_str(json_string.as_str())
        .context(DeserializeSnafu { json: json_string })
}

/// Parses a string to a json type value
pub fn parse_string_to_jsonb(s: &str) -> Result<Vec<u8>> {
    jsonb::parse_value(s.as_bytes())
        .map_err(|_| InvalidJsonSnafu { value: s }.build())
        .map(|json| json.to_vec())
}
