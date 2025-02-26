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

use greptime_proto::v1::value::ValueData::StringValue;
use greptime_proto::v1::{ColumnDataType, ColumnSchema, SemanticType};
use lazy_static::lazy_static;

mod common;

const PIPELINE_YAML: &str = r#"
---
processors:
  - join:
      field: join_test
      separator: "-"

transform:
  - field: join_test
    type: string
"#;

lazy_static! {
    pub static ref EXPECTED_SCHEMA: Vec<ColumnSchema> = vec![
        common::make_column_schema(
            "join_test".to_string(),
            ColumnDataType::String,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "greptime_timestamp".to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];
}

#[test]
fn test_simple_join() {
    let input_value_str = r#"
    [
      {
        "join_test": ["a", "b", "c"]
      }
    ]
"#;

    let output = common::parse_and_exec(input_value_str, PIPELINE_YAML);

    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("a-b-c".to_string()))
    );
}

#[test]
fn test_integer_join() {
    let input_value_str = r#"
    [
      {
        "join_test": [1, 2, 3]
      }
    ]
"#;
    let output = common::parse_and_exec(input_value_str, PIPELINE_YAML);

    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("1-2-3".to_string()))
    );
}

#[test]
fn test_boolean() {
    let input_value_str = r#"
    [
      {
        "join_test": [true, false, true]
      }
    ]
"#;
    let output = common::parse_and_exec(input_value_str, PIPELINE_YAML);

    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("true-false-true".to_string()))
    );
}

#[test]
fn test_float() {
    let input_value_str = r#"
    [
      {
        "join_test": [1.1, 1.2, 1.3]
      }
    ]
"#;
    let output = common::parse_and_exec(input_value_str, PIPELINE_YAML);

    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("1.1-1.2-1.3".to_string()))
    );
}

#[test]
fn test_mix_type() {
    let input_value_str = r#"
    [
      {
        "join_test": [1, true, "a", 1.1]
      }
    ]
"#;
    let output = common::parse_and_exec(input_value_str, PIPELINE_YAML);

    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("1-true-a-1.1".to_string()))
    );
}

#[test]
fn test_ignore_missing() {
    let empty_string = r#"{}"#;
    let pipeline_yaml = r#"
processors:
  - join:
      field: join_test
      separator: "-"
      ignore_missing: true

transform:
  - field: join_test
    type: string
"#;
    let output = common::parse_and_exec(empty_string, pipeline_yaml);

    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(output.rows[0].values[0].value_data, None);
}
