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

use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{ColumnDataType, SemanticType};

#[test]
fn test() {
    let test_input = r#"
{
    "encoding": "2024-06-27T06:13:36.991Z",
    "decoding": "2024-06-27T06%3A13%3A36.991Z"
}"#;

    let pipeline_yaml = r#"
processors:
  - urlencoding:
      field: encoding
      method: encode

  - urlencoding:
      field: decoding
      method: decode

transform:
  - fields:
        - encoding
        - decoding
    type: string
"#;

    let expected_schema = vec![
        common::make_column_schema(
            "encoding".to_string(),
            ColumnDataType::String,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "decoding".to_string(),
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
        Some(ValueData::StringValue(
            "2024-06-27T06%3A13%3A36.991Z".to_string()
        ))
    );
    assert_eq!(
        output.rows[0].values[1].value_data,
        Some(ValueData::StringValue(
            "2024-06-27T06:13:36.991Z".to_string()
        ))
    );
}

#[test]
fn test_ignore_missing() {
    let test_input = r#"{}"#;

    let pipeline_yaml = r#"
processors:
  - urlencoding:
      field: encoding
      method: encode
      ignore_missing: true

transform:
  - fields:
        - encoding
    type: string
"#;

    let expected_schema = vec![
        common::make_column_schema(
            "encoding".to_string(),
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
}
