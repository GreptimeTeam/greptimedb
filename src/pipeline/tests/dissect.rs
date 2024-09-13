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

fn make_string_column_schema(name: String) -> greptime_proto::v1::ColumnSchema {
    common::make_column_schema(name, ColumnDataType::String, SemanticType::Field)
}

#[test]
fn test_dissect_pattern() {
    let input_value_str = r#"
    [
      {
        "str": "123 456"
      }
    ]
"#;

    let pipeline_yaml = r#"
processors:
  - dissect:
      field: str
      pattern: "%{a} %{b}"

transform:
  - fields:
        - a
        - b
    type: string
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    let expected_schema = vec![
        make_string_column_schema("a".to_string()),
        make_string_column_schema("b".to_string()),
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
    assert_eq!(
        output.rows[0].values[1].value_data,
        Some(StringValue("456".to_string()))
    );
}

#[test]
fn test_dissect_patterns() {
    let input_value_str = r#"
    [
      {
        "str": "123 456"
      }
    ]
"#;

    let pipeline_yaml = r#"
processors:
  - dissect:
      field: str
      patterns: 
        - "%{a} %{b}"

transform:
  - fields:
        - a
        - b
    type: string
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    let expected_schema = vec![
        make_string_column_schema("a".to_string()),
        make_string_column_schema("b".to_string()),
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
    assert_eq!(
        output.rows[0].values[1].value_data,
        Some(StringValue("456".to_string()))
    );
}

#[test]
fn test_ignore_missing() {
    let empty_str = r#"{}"#;

    let pipeline_yaml = r#"
processors:
  - dissect:
      field: str
      patterns: 
        - "%{a} %{b}"
      ignore_missing: true

transform:
  - fields:
        - a
        - b
    type: string
"#;

    let output = common::parse_and_exec(empty_str, pipeline_yaml);

    let expected_schema = vec![
        make_string_column_schema("a".to_string()),
        make_string_column_schema("b".to_string()),
        common::make_column_schema(
            "greptime_timestamp".to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    assert_eq!(output.schema, expected_schema);

    assert_eq!(output.rows[0].values[0].value_data, None);
    assert_eq!(output.rows[0].values[1].value_data, None);
}

#[test]
fn test_modifier() {
    let empty_str = r#"
{
    "str": "key1 key2 key3 key4 key5       key6"
}"#;

    let pipeline_yaml = r#"
processors:
  - dissect:
      field: str
      patterns: 
        - "%{key1} %{key2} %{+key3} %{+key3/2} %{key5->} %{?key6}"

transform:
  - fields:
        - key1
        - key2
        - key3
        - key5
    type: string
"#;

    let output = common::parse_and_exec(empty_str, pipeline_yaml);

    let expected_schema = vec![
        make_string_column_schema("key1".to_string()),
        make_string_column_schema("key2".to_string()),
        make_string_column_schema("key3".to_string()),
        make_string_column_schema("key5".to_string()),
        common::make_column_schema(
            "greptime_timestamp".to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    assert_eq!(output.schema, expected_schema);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("key1".to_string()))
    );
    assert_eq!(
        output.rows[0].values[1].value_data,
        Some(StringValue("key2".to_string()))
    );
    assert_eq!(
        output.rows[0].values[2].value_data,
        Some(StringValue("key3 key4".to_string()))
    );
    assert_eq!(
        output.rows[0].values[3].value_data,
        Some(StringValue("key5".to_string()))
    );
}

#[test]
fn test_append_separator() {
    let empty_str = r#"
{
    "str": "key1 key2"
}"#;

    let pipeline_yaml = r#"
processors:
  - dissect:
      field: str
      patterns: 
        - "%{+key1} %{+key1}"
      append_separator: "_"

transform:
  - fields:
        - key1
    type: string
"#;

    let output = common::parse_and_exec(empty_str, pipeline_yaml);

    let expected_schema = vec![
        make_string_column_schema("key1".to_string()),
        common::make_column_schema(
            "greptime_timestamp".to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    assert_eq!(output.schema, expected_schema);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(StringValue("key1_key2".to_string()))
    );
}

#[test]
fn test_parse_failure() {
    let input_str = r#"
{
    "str": "key1 key2"
}"#;

    let pipeline_yaml = r#"
processors:
  - dissect:
      field: str
      patterns:
        - "%{key1} %{key2} %{key3}"

transform:
  - fields:
      - key1
    type: string
"#;

    let input_value = serde_json::from_str::<serde_json::Value>(input_str).unwrap();

    let yaml_content = pipeline::Content::Yaml(pipeline_yaml.into());
    let pipeline: pipeline::Pipeline<pipeline::GreptimeTransformer> =
        pipeline::parse(&yaml_content).expect("failed to parse pipeline");
    let mut result = pipeline.init_intermediate_state();

    pipeline.prepare(input_value, &mut result).unwrap();
    let row = pipeline.exec_mut(&mut result);

    assert!(row.is_err());
    assert_eq!(row.err().unwrap(), "No matching pattern found");
}
