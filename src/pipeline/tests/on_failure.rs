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

use greptime_proto::v1::value::ValueData::{U16Value, U8Value};
use greptime_proto::v1::{ColumnDataType, ColumnSchema, SemanticType};
use pipeline::{parse, Content, GreptimeTransformer, Pipeline, Value};

mod common;

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
        ColumnSchema {
            column_name: "version".to_string(),
            datatype: ColumnDataType::Uint8.into(),
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
        ColumnSchema {
            column_name: "version".to_string(),
            datatype: ColumnDataType::Uint8.into(),
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
        ColumnSchema {
            column_name: "version".to_string(),
            datatype: ColumnDataType::Uint8.into(),
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

    assert_eq!(output.schema, expected_schema);
    assert_eq!(output.rows[0].values[0].value_data, Some(U8Value(0)));
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
        ColumnSchema {
            column_name: "version".to_string(),
            datatype: ColumnDataType::Uint8.into(),
            semantic_type: SemanticType::Field.into(),
            datatype_extension: None,
        },
        ColumnSchema {
            column_name: "spec_version".to_string(),
            datatype: ColumnDataType::Uint16.into(),
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

    assert_eq!(output.schema, expected_schema);
    assert_eq!(output.rows[0].values[0].value_data, Some(U8Value(0)));
    assert_eq!(output.rows[0].values[1].value_data, Some(U16Value(0)));
}
