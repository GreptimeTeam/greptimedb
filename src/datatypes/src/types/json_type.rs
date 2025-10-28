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

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use arrow_schema::Fields;
use common_base::bytes::Bytes;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::data_type::DataType;
use crate::error::{
    DeserializeSnafu, InvalidJsonSnafu, InvalidJsonbSnafu, MergeJsonDatatypeSnafu, Result,
};
use crate::prelude::ConcreteDataType;
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::types::{ListType, StructField, StructType};
use crate::value::Value;
use crate::vectors::json::builder::JsonVectorBuilder;
use crate::vectors::{BinaryVectorBuilder, MutableVector};

pub const JSON_TYPE_NAME: &str = "Json";
const JSON_PLAIN_FIELD_NAME: &str = "__plain__";

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

    pub(crate) fn empty() -> Self {
        Self {
            format: JsonFormat::Native(Box::new(ConcreteDataType::null_datatype())),
        }
    }

    /// Make json type a struct type, by:
    /// - if the json is an object, its entries are mapped to struct fields, obviously;
    /// - if not, the json is one of bool, number, string or array, make it a special field called
    ///   [JSON_PLAIN_FIELD_NAME] in a struct with only that field.
    pub(crate) fn as_struct_type(&self) -> StructType {
        match &self.format {
            JsonFormat::Jsonb => StructType::default(),
            JsonFormat::Native(inner) => match inner.as_ref() {
                ConcreteDataType::Struct(t) => t.clone(),
                x => StructType::new(Arc::new(vec![StructField::new(
                    JSON_PLAIN_FIELD_NAME.to_string(),
                    x.clone(),
                    true,
                )])),
            },
        }
    }

    /// Check if this json type is the special "plain" one.
    /// See [JsonType::as_struct_type].
    pub(crate) fn is_plain_json(&self) -> bool {
        let JsonFormat::Native(box ConcreteDataType::Struct(t)) = &self.format else {
            return true;
        };
        let fields = t.fields();
        fields.len() == 1 && fields[0].name() == JSON_PLAIN_FIELD_NAME
    }

    /// Try to merge this json type with others, error on datatype conflict.
    pub(crate) fn merge(&mut self, other: &JsonType) -> Result<()> {
        match (&self.format, &other.format) {
            (JsonFormat::Jsonb, JsonFormat::Jsonb) => Ok(()),
            (JsonFormat::Native(this), JsonFormat::Native(that)) => {
                let merged = merge(this.as_ref(), that.as_ref())?;
                self.format = JsonFormat::Native(Box::new(merged));
                Ok(())
            }
            _ => MergeJsonDatatypeSnafu {
                reason: "json format not match",
            }
            .fail(),
        }
    }
}

fn merge(this: &ConcreteDataType, that: &ConcreteDataType) -> Result<ConcreteDataType> {
    match (this, that) {
        (this, that) if this == that => Ok(this.clone()),
        (ConcreteDataType::List(this), ConcreteDataType::List(that)) => {
            merge_list(this, that).map(ConcreteDataType::List)
        }
        (ConcreteDataType::Struct(this), ConcreteDataType::Struct(that)) => {
            merge_struct(this, that).map(ConcreteDataType::Struct)
        }
        (ConcreteDataType::Null(_), x) | (x, ConcreteDataType::Null(_)) => Ok(x.clone()),
        _ => MergeJsonDatatypeSnafu {
            reason: format!("datatypes have conflict, this: {this}, that: {that}"),
        }
        .fail(),
    }
}

fn merge_list(this: &ListType, that: &ListType) -> Result<ListType> {
    let merged = merge(this.item_type(), that.item_type())?;
    Ok(ListType::new(Arc::new(merged)))
}

fn merge_struct(this: &StructType, that: &StructType) -> Result<StructType> {
    let this = Arc::unwrap_or_clone(this.fields());
    let that = Arc::unwrap_or_clone(that.fields());

    let mut this: BTreeMap<String, StructField> = this
        .into_iter()
        .map(|x| (x.name().to_string(), x))
        .collect();
    // merge "that" into "this" directly:
    for that_field in that {
        let field_name = that_field.name().to_string();
        if let Some(this_field) = this.get(&field_name) {
            let merged_field = StructField::new(
                field_name.clone(),
                merge(this_field.data_type(), that_field.data_type())?,
                true, // the value in json object must be always nullable
            );
            this.insert(field_name, merged_field);
        } else {
            this.insert(field_name, that_field);
        }
    }

    let fields = this.into_values().collect::<Vec<_>>();
    Ok(StructType::new(Arc::new(fields)))
}

impl DataType for JsonType {
    fn name(&self) -> String {
        match &self.format {
            JsonFormat::Jsonb => JSON_TYPE_NAME.to_string(),
            JsonFormat::Native(x) => format!("Json<{x}>"),
        }
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
        match self.format {
            JsonFormat::Jsonb => Box::new(BinaryVectorBuilder::with_capacity(capacity)),
            JsonFormat::Native(_) => Box::new(JsonVectorBuilder::with_capacity(capacity)),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::JsonStructureSettings;

    #[test]
    fn test_merge_json_type() -> Result<()> {
        fn test(
            json: &str,
            json_type: &mut JsonType,
            expected: std::result::Result<&str, &str>,
        ) -> Result<()> {
            let json: serde_json::Value = serde_json::from_str(json).unwrap();

            let settings = JsonStructureSettings::Structured(None);
            let value = settings.encode(json)?;
            let value_type = value.data_type();
            let Some(other) = value_type.as_json() else {
                unreachable!()
            };

            let result = json_type.merge(other);
            match (result, expected) {
                (Ok(()), Ok(expected)) => {
                    assert_eq!(json_type.name(), expected)
                }
                (Err(err), Err(expected)) => {
                    assert_eq!(err.to_string(), expected)
                }
                _ => unreachable!(),
            }
            Ok(())
        }

        let json_type = &mut JsonType::new(JsonFormat::Native(Box::new(
            ConcreteDataType::null_datatype(),
        )));

        // can merge with json object:
        let json = r#"{
            "hello": "world",
            "list": [1, 2, 3],
            "object": {"a": 1}
        }"#;
        let expected =
            r#"Json<Struct<"hello": String, "list": List<Int64>, "object": Struct<"a": Int64>>>"#;
        test(json, json_type, Ok(expected))?;

        // cannot merge with other non-object json values:
        let jsons = [r#""s""#, "1", "[1]"];
        let expects = [
            r#"Failed to merge JSON datatype: datatypes have conflict, this: Struct<"hello": String, "list": List<Int64>, "object": Struct<"a": Int64>>, that: String"#,
            r#"Failed to merge JSON datatype: datatypes have conflict, this: Struct<"hello": String, "list": List<Int64>, "object": Struct<"a": Int64>>, that: Int64"#,
            r#"Failed to merge JSON datatype: datatypes have conflict, this: Struct<"hello": String, "list": List<Int64>, "object": Struct<"a": Int64>>, that: List<Int64>"#,
        ];
        for (json, expect) in jsons.into_iter().zip(expects.into_iter()) {
            test(json, json_type, Err(expect))?;
        }

        // cannot merge with other json object with conflict field datatype:
        let json = r#"{
            "hello": 1,
            "float": 0.123,
            "no": 42
        }"#;
        let expected =
            r#"Failed to merge JSON datatype: datatypes have conflict, this: String, that: Int64"#;
        test(json, json_type, Err(expected))?;

        // can merge with another json object:
        let json = r#"{
            "hello": "greptime",
            "float": 0.123,
            "int": 42
        }"#;
        let expected = r#"Json<Struct<"float": Float64, "hello": String, "int": Int64, "list": List<Int64>, "object": Struct<"a": Int64>>>"#;
        test(json, json_type, Ok(expected))?;

        // can merge with some complex nested json object:
        let json = r#"{
            "list": [4],
            "object": {"foo": "bar", "l": ["x"], "o": {"key": "value"}},
            "float": 0.456,
            "int": 0
        }"#;
        let expected = r#"Json<Struct<"float": Float64, "hello": String, "int": Int64, "list": List<Int64>, "object": Struct<"a": Int64, "foo": String, "l": List<String>, "o": Struct<"key": String>>>>"#;
        test(json, json_type, Ok(expected))?;

        Ok(())
    }
}
