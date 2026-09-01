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

mod common;

use std::borrow::Cow;

use api::v1::ColumnDataType;
use api::v1::json_value::Value as JsonValue;
use api::v1::value::ValueData;
use arrow_schema::extension::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType,
};
use datatypes::extension::json::{Json2ExtensionType, JsonMetadata};

const INPUT_VALUE_OBJ: &str = r#"
[
  {
    "commit": "{\"commitTime\": \"1573840000.000\", \"commitAuthor\": \"test\"}"
  }
]
"#;

const INPUT_VALUE_ARR: &str = r#"
[
  {
    "commit": "[\"test1\", \"test2\"]"
  }
]
"#;

#[test]
fn test_json_parse_inplace() {
    let pipeline_yaml = r#"
---
processors:
  - json_parse:
      field: commit

transform:
  - field: commit
    type: json
"#;

    let output = common::parse_and_exec(INPUT_VALUE_OBJ, pipeline_yaml);

    // check schema
    assert_eq!(output.schema[0].column_name, "commit");
    let type_id: i32 = ColumnDataType::Json.into();
    assert_eq!(output.schema[0].datatype, type_id);

    // check value
    let ValueData::BinaryValue(json_value) = output.rows[0].values[0].value_data.clone().unwrap()
    else {
        panic!("expect binary value");
    };
    let v = jsonb::from_slice(&json_value).unwrap();

    let mut expected = jsonb::Object::new();
    expected.insert(
        "commitTime".to_string(),
        jsonb::Value::String(Cow::Borrowed("1573840000.000")),
    );
    expected.insert(
        "commitAuthor".to_string(),
        jsonb::Value::String(Cow::Borrowed("test")),
    );
    assert_eq!(v, jsonb::Value::Object(expected));
}

#[test]
fn test_json_parse_new_var() {
    let pipeline_yaml = r#"
---
processors:
  - json_parse:
      field: commit, commit_json

transform:
  - field: commit_json
    type: json
"#;

    let output = common::parse_and_exec(INPUT_VALUE_OBJ, pipeline_yaml);

    // check schema
    assert_eq!(output.schema[0].column_name, "commit_json");
    let type_id: i32 = ColumnDataType::Json.into();
    assert_eq!(output.schema[0].datatype, type_id);

    // check value
    let ValueData::BinaryValue(json_value) = output.rows[0].values[0].value_data.clone().unwrap()
    else {
        panic!("expect binary value");
    };
    let v = jsonb::from_slice(&json_value).unwrap();

    let mut expected = jsonb::Object::new();
    expected.insert(
        "commitTime".to_string(),
        jsonb::Value::String(Cow::Borrowed("1573840000.000")),
    );
    expected.insert(
        "commitAuthor".to_string(),
        jsonb::Value::String(Cow::Borrowed("test")),
    );
    assert_eq!(v, jsonb::Value::Object(expected));
}

#[test]
fn test_json2_parse() {
    let pipeline_yaml = r#"
---
processors:
  - json_parse:
      field: commit

transform:
  - field: commit
    type:
      json2:
        - path: "commitAuthor"
          type: string
          nullable: false
"#;

    let output = common::parse_and_exec(INPUT_VALUE_OBJ, pipeline_yaml);

    assert_eq!(output.schema[0].datatype, ColumnDataType::Json as i32);
    assert!(output.schema[0].datatype_extension.is_none());
    assert_eq!(
        output.schema[0]
            .options
            .as_ref()
            .and_then(|options| options.options.get(EXTENSION_TYPE_NAME_KEY))
            .map(String::as_str),
        Some(Json2ExtensionType::NAME)
    );
    let metadata = output.schema[0]
        .options
        .as_ref()
        .and_then(|options| options.options.get(EXTENSION_TYPE_METADATA_KEY))
        .unwrap();
    let metadata: JsonMetadata = serde_json::from_str(metadata).unwrap();
    assert_eq!(
        metadata.json_settings().type_hints()[0].path,
        ["commitAuthor"]
    );

    let ValueData::JsonValue(value) = output.rows[0].values[0].value_data.as_ref().unwrap() else {
        panic!("expect JSON2 value");
    };
    assert!(matches!(value.value, Some(JsonValue::Object(_))));
}

#[test]
fn test_json_parse_with_simple_extractor() {
    let pipeline_yaml = r#"
---
processors:
  - json_parse:
      field: commit, commit_json
  - simple_extract:
      field: commit_json, commit_author
      key: "commitAuthor"


transform:
  - field: commit_author
    type: string
"#;

    let output = common::parse_and_exec(INPUT_VALUE_OBJ, pipeline_yaml);

    // check schema
    assert_eq!(output.schema[0].column_name, "commit_author");
    let type_id: i32 = ColumnDataType::String.into();
    assert_eq!(output.schema[0].datatype, type_id);

    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(ValueData::StringValue("test".to_string()))
    );
}

#[test]
fn test_json_parse_array() {
    let pipeline_yaml = r#"
---
processors:
  - json_parse:
      field: commit

transform:
  - field: commit
    type: json
"#;

    let output = common::parse_and_exec(INPUT_VALUE_ARR, pipeline_yaml);

    // check schema
    assert_eq!(output.schema[0].column_name, "commit");
    let type_id: i32 = ColumnDataType::Json.into();
    assert_eq!(output.schema[0].datatype, type_id);

    // check value
    let ValueData::BinaryValue(json_value) = output.rows[0].values[0].value_data.clone().unwrap()
    else {
        panic!("expect binary value");
    };
    let v = jsonb::from_slice(&json_value).unwrap();

    let expected = jsonb::Value::Array(vec![
        jsonb::Value::String(Cow::Borrowed("test1")),
        jsonb::Value::String(Cow::Borrowed("test2")),
    ]);
    assert_eq!(v, expected);
}
