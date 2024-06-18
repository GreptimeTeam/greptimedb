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
        ColumnSchema {
            column_name: "join_test".to_string(),
            datatype: ColumnDataType::String.into(),
            semantic_type: SemanticType::Field.into(),
            datatype_extension: None,
        },
        ColumnSchema {
            column_name: "greptime_timestamp".to_string(),
            datatype: ColumnDataType::TimestampNanosecond.into(),
            semantic_type: SemanticType::Timestamp.into(),
            datatype_extension: None,
        },
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
