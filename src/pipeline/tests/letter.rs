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
use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{ColumnDataType, SemanticType};
use lazy_static::lazy_static;

lazy_static! {
    static ref EXPECTED_SCHEMA: Vec<ColumnSchema> = vec![
        common::make_column_schema(
            "input_str".to_string(),
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
fn test_upper() {
    let test_input = r#"
{
    "input_str": "aaa"
}"#;

    let pipeline_yaml = r#"
processors:
  - letter:
      fields:
        - input_str
      method: upper

transform:
  - fields:
        - input_str
    type: string
"#;

    let output = common::parse_and_exec(test_input, pipeline_yaml);
    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(ValueData::StringValue("AAA".to_string()))
    );
}

#[test]
fn test_lower() {
    let test_input = r#"
{
    "input_str": "AAA"
}"#;

    let pipeline_yaml = r#"
processors:
  - letter:
      fields:
        - input_str
      method: lower

transform:
  - fields:
        - input_str
    type: string
"#;

    let output = common::parse_and_exec(test_input, pipeline_yaml);
    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(ValueData::StringValue("aaa".to_string()))
    );
}

#[test]
fn test_capital() {
    let test_input = r#"
{
    "upper": "AAA",
    "lower": "aaa"
}"#;

    let pipeline_yaml = r#"
processors:
  - letter:
      fields:
        - upper
        - lower
      method: capital

transform:
  - fields:
        - upper
        - lower
    type: string
"#;

    let expected_schema = vec![
        common::make_column_schema(
            "upper".to_string(),
            ColumnDataType::String,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "lower".to_string(),
            ColumnDataType::String,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "greptime_timestamp".to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    let output = common::parse_and_exec(test_input, pipeline_yaml);
    assert_eq!(output.schema, expected_schema);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(ValueData::StringValue("AAA".to_string()))
    );
    assert_eq!(
        output.rows[0].values[1].value_data,
        Some(ValueData::StringValue("Aaa".to_string()))
    );
}

#[test]
fn test_ignore_missing() {
    let test_input = r#"{}"#;

    let pipeline_yaml = r#"
processors:
  - letter:
      fields:
        - upper
        - lower
      method: capital
      ignore_missing: true

transform:
  - fields:
        - upper
        - lower
    type: string
"#;

    let expected_schema = vec![
        common::make_column_schema(
            "upper".to_string(),
            ColumnDataType::String,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "lower".to_string(),
            ColumnDataType::String,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "greptime_timestamp".to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    let output = common::parse_and_exec(test_input, pipeline_yaml);
    assert_eq!(output.schema, expected_schema);
    assert_eq!(output.rows[0].values[0].value_data, None);
    assert_eq!(output.rows[0].values[1].value_data, None);
}
