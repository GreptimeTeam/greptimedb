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

use api::v1::ColumnSchema;
use greptime_proto::v1::value::ValueData::StringValue;
use greptime_proto::v1::{ColumnDataType, SemanticType};
use lazy_static::lazy_static;

lazy_static! {
    static ref EXPECTED_SCHEMA: Vec<ColumnSchema> = vec![
        common::make_column_schema(
            "str_id".to_string(),
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
fn test_regex_pattern() {
    let input_value_str = r#"
    [
      {
        "str": "123 456"
      }
    ]
"#;

    let pipeline_yaml = r#"
processors:
  - regex:
      fields: 
        - str
      pattern: "(?<id>\\d+)"

transform:
  - field: str_id
    type: string
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    assert_eq!(output.schema, *EXPECTED_SCHEMA);

    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("123".to_string()))
    );
}

#[test]
fn test_regex_patterns() {
    let input_value_str = r#"
    [
      {
        "str": "123 456"
      }
    ]
"#;

    let pipeline_yaml = r#"
processors:
  - regex:
      fields: 
        - str
      patterns:
        - "(?<id>\\d+)"

transform:
  - field: str_id
    type: string
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    assert_eq!(output.schema, *EXPECTED_SCHEMA);

    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("123".to_string()))
    );
}

#[test]
fn test_ignore_missing() {
    let input_value_str = r#"{}"#;

    let pipeline_yaml = r#"
processors:
  - regex:
      fields: 
        - str
      pattern: "(?<id>\\d+)"
      ignore_missing: true

transform:
  - field: str_id
    type: string
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    assert_eq!(output.schema, *EXPECTED_SCHEMA);

    assert_eq!(output.rows[0].values[0].value_data, None);
}

#[test]
fn test_unuse_regex_group() {
    let input_value_str = r#"
  [
    {
      "str": "123 456"
    }
  ]
"#;

    let pipeline_yaml = r#"
processors:
- regex:
    fields:
      - str
    pattern: "(?<id1>\\d+) (?<id2>\\d+)"

transform:
- field: str_id1
  type: string
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    assert_eq!(
        output.schema,
        vec![
            common::make_column_schema(
                "str_id1".to_string(),
                ColumnDataType::String,
                SemanticType::Field,
            ),
            common::make_column_schema(
                "greptime_timestamp".to_string(),
                ColumnDataType::TimestampNanosecond,
                SemanticType::Timestamp,
            ),
        ]
    );

    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("123".to_string()))
    );
}
