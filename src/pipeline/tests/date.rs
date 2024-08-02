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

const TEST_INPUT: &str = r#"
{
    "input_str": "2024-06-27T06:13:36.991Z"
}"#;

const TEST_VALUE: Option<ValueData> =
    Some(ValueData::TimestampNanosecondValue(1719468816991000000));

lazy_static! {
    static ref EXPECTED_SCHEMA: Vec<ColumnSchema> = vec![
        common::make_column_schema(
            "ts".to_string(),
            ColumnDataType::TimestampNanosecond,
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
fn test_parse_date() {
    let pipeline_yaml = r#"
processors:
  - date:
      fields:
        - input_str
      formats:
        - "%Y-%m-%dT%H:%M:%S%.3fZ"

transform:
  - fields:
        - input_str, ts
    type: time
"#;

    let output = common::parse_and_exec(TEST_INPUT, pipeline_yaml);
    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(output.rows[0].values[0].value_data, TEST_VALUE);
}

#[test]
fn test_multi_formats() {
    let pipeline_yaml = r#"
processors:
  - date:
      fields:
        - input_str
      formats:
        - "%Y-%m-%dT%H:%M:%S"
        - "%Y-%m-%dT%H:%M:%S%.3fZ"

transform:
  - fields:
        - input_str, ts
    type: time
"#;

    let output = common::parse_and_exec(TEST_INPUT, pipeline_yaml);
    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(output.rows[0].values[0].value_data, TEST_VALUE);
}

#[test]
fn test_ignore_missing() {
    let empty_input = r#"{}"#;

    let pipeline_yaml = r#"
processors:
  - date:
      fields:
        - input_str
      formats:
        - "%Y-%m-%dT%H:%M:%S"
        - "%Y-%m-%dT%H:%M:%S%.3fZ"
      ignore_missing: true

transform:
  - fields:
        - input_str, ts
    type: time
"#;

    let output = common::parse_and_exec(empty_input, pipeline_yaml);
    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(output.rows[0].values[0].value_data, None);
}

#[test]
fn test_timezone() {
    let pipeline_yaml = r#"
processors:
  - date:
      fields:
        - input_str
      formats:
        - "%Y-%m-%dT%H:%M:%S"
        - "%Y-%m-%dT%H:%M:%S%.3fZ"
      ignore_missing: true
      timezone: 'Asia/Shanghai'

transform:
  - fields:
        - input_str, ts
    type: time
"#;

    let output = common::parse_and_exec(TEST_INPUT, pipeline_yaml);
    assert_eq!(output.schema, *EXPECTED_SCHEMA);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(ValueData::TimestampNanosecondValue(1719440016991000000))
    );
}
