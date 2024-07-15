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

use greptime_proto::v1::value::ValueData::StringValue;
use greptime_proto::v1::{ColumnDataType, SemanticType};

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

    let expected_schema = vec![
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

    assert_eq!(output.schema, expected_schema);

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

    let expected_schema = vec![
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

    assert_eq!(output.schema, expected_schema);

    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("123".to_string()))
    );
}
