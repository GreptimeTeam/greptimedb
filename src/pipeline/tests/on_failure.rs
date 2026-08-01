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

use common_query::prelude::greptime_timestamp;
use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::value::ValueData::{I8Value, U8Value, U16Value};
use greptime_proto::v1::{ColumnDataType, SemanticType};

mod common;

fn assert_on_failure_value(
    input: &str,
    type_: &str,
    on_failure: &str,
    default: Option<&str>,
    expected: Option<ValueData>,
) {
    let input = format!(r#"[{{"value": {input}}}]"#);
    let default = default
        .map(|value| format!("    default: {value}\n"))
        .unwrap_or_default();
    let pipeline = format!(
        "---\ntransform:\n  - fields:\n      - value\n    type: {type_}\n{default}    on_failure: {on_failure}\n"
    );

    let output = common::parse_and_exec(&input, &pipeline);
    assert_eq!(output.rows[0].values[0].value_data, expected);
}

#[test]
fn test_on_failure_with_ignore() {
    let input_value_str = r#"
    [
      {
        "version": "-"
      }
    ]
"#;

    let pipeline_yaml = r#"
---
description: Pipeline for Testing on-failure

transform:
  - fields:
      - version
    type: uint8
    on_failure: ignore
"#;
    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    let expected_schema = vec![
        common::make_column_schema(
            "version".to_string(),
            ColumnDataType::Uint8,
            SemanticType::Field,
        ),
        common::make_column_schema(
            greptime_timestamp().to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    assert_eq!(output.schema, expected_schema);
    assert_eq!(output.rows[0].values[0].value_data, None);
}

#[test]
fn test_on_failure_with_default() {
    let input_value_str = r#"
    [
      {
        "version": "-"
      }
    ]
"#;

    let pipeline_yaml = r#"
---
description: Pipeline for Testing on-failure

transform:
  - fields:
      - version
    type: uint8
    default: 0
    on_failure: default
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    let expected_schema = vec![
        common::make_column_schema(
            "version".to_string(),
            ColumnDataType::Uint8,
            SemanticType::Field,
        ),
        common::make_column_schema(
            greptime_timestamp().to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    assert_eq!(output.schema, expected_schema);
    assert_eq!(output.rows[0].values[0].value_data, Some(U8Value(0)));
}

#[test]
fn test_default() {
    let input_value_str = r#"
    [{}]
"#;

    let pipeline_yaml = r#"
---
description: Pipeline for Testing on-failure

transform:
  - fields:
      - version
    type: uint8
    default: 0
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    let expected_schema = vec![
        common::make_column_schema(
            "version".to_string(),
            ColumnDataType::Uint8,
            SemanticType::Field,
        ),
        common::make_column_schema(
            greptime_timestamp().to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    assert_eq!(output.schema, expected_schema);
    assert_eq!(output.rows[0].values[0].value_data, Some(U8Value(0)));
}

#[test]
fn test_narrowing_overflow_honors_on_failure() {
    // input, target type, on_failure, explicit default, expected value
    let cases = [
        // Numeric narrowing overflows honor ignore and both default modes.
        ("-1", "uint8", "ignore", None, None),
        ("-1", "uint8", "default", None, Some(U8Value(0))),
        ("-1", "uint8", "default", Some("42"), Some(U8Value(42))),
        ("256", "int8", "ignore", None, None),
        ("256", "int8", "default", None, Some(I8Value(0))),
        ("256", "int8", "default", Some("-42"), Some(I8Value(-42))),
        // Numeric in-range boundaries remain exact.
        ("0", "uint8", "ignore", None, Some(U8Value(0))),
        ("255", "uint8", "ignore", None, Some(U8Value(255))),
        ("-128", "int8", "ignore", None, Some(I8Value(-128))),
        ("127", "int8", "ignore", None, Some(I8Value(127))),
        // Numeric-string overflows follow the same policy.
        (r#""-1""#, "uint8", "ignore", None, None),
        (r#""-1""#, "uint8", "default", None, Some(U8Value(0))),
        (r#""-1""#, "uint8", "default", Some("24"), Some(U8Value(24))),
        (r#""256""#, "int8", "ignore", None, None),
        (r#""256""#, "int8", "default", None, Some(I8Value(0))),
        (
            r#""256""#,
            "int8",
            "default",
            Some("-24"),
            Some(I8Value(-24)),
        ),
        // Numeric-string boundaries remain exact.
        (r#""0""#, "uint8", "ignore", None, Some(U8Value(0))),
        (r#""255""#, "uint8", "ignore", None, Some(U8Value(255))),
        (r#""-128""#, "int8", "ignore", None, Some(I8Value(-128))),
        (r#""127""#, "int8", "ignore", None, Some(I8Value(127))),
        // Keep malformed-string default behavior as a control.
        (
            r#""not-a-number""#,
            "int8",
            "default",
            Some("-7"),
            Some(I8Value(-7)),
        ),
    ];

    for (input, type_, on_failure, default, expected) in cases {
        assert_on_failure_value(input, type_, on_failure, default, expected);
    }
}

#[test]
fn test_multiple_on_failure() {
    let input_value_str = r#"
    [
      {
        "version": "-",
        "spec_version": "-"
      }
    ]
"#;

    let pipeline_yaml = r#"
---
description: Pipeline for Testing on-failure

transform:
  - fields:
      - version
    type: uint8
    default: 0
    on_failure: default
  - fields:
      - spec_version
    type: uint16
    default: 0
    on_failure: default
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    let expected_schema = vec![
        common::make_column_schema(
            "version".to_string(),
            ColumnDataType::Uint8,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "spec_version".to_string(),
            ColumnDataType::Uint16,
            SemanticType::Field,
        ),
        common::make_column_schema(
            greptime_timestamp().to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    assert_eq!(output.schema, expected_schema);
    assert_eq!(output.rows[0].values[0].value_data, Some(U8Value(0)));
    assert_eq!(output.rows[0].values[1].value_data, Some(U16Value(0)));
}
