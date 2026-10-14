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

use common_query::prelude::greptime_timestamp;
use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{ColumnDataType, SemanticType};

#[test]
fn test_logfmt() {
    let input_value_str = r#"
[
  {
    "message": "level=info msg=\"user logged in\" user=alice duration=12ms cached"
  }
]
"#;

    let pipeline_yaml = r#"
---
processors:
  - logfmt:
      field: message, parsed
  - simple_extract:
      field: parsed, level
      key: "level"
  - simple_extract:
      field: parsed, msg
      key: "msg"
  - simple_extract:
      field: parsed, user
      key: "user"
  - simple_extract:
      field: parsed, cached
      key: "cached"

transform:
  - fields:
      - level
      - msg
      - user
    type: string
  - field: cached
    type: boolean
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    let expected_schema = vec![
        common::make_column_schema(
            "level".to_string(),
            ColumnDataType::String,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "msg".to_string(),
            ColumnDataType::String,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "user".to_string(),
            ColumnDataType::String,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "cached".to_string(),
            ColumnDataType::Boolean,
            SemanticType::Field,
        ),
        common::make_column_schema(
            greptime_timestamp().to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];
    assert_eq!(output.schema, expected_schema);

    let values = &output.rows[0].values;
    assert_eq!(
        values[0].value_data,
        Some(ValueData::StringValue("info".to_string()))
    );
    assert_eq!(
        values[1].value_data,
        Some(ValueData::StringValue("user logged in".to_string()))
    );
    assert_eq!(
        values[2].value_data,
        Some(ValueData::StringValue("alice".to_string()))
    );
    assert_eq!(values[3].value_data, Some(ValueData::BoolValue(true)));
}

#[test]
fn test_logfmt_ignore_missing() {
    let pipeline_yaml = r#"
---
processors:
  - logfmt:
      field: message
      ignore_missing: true

transform:
  - field: message
    type: string
"#;

    let output = common::parse_and_exec(r#"[{"other": "x"}]"#, pipeline_yaml);
    assert_eq!(output.rows[0].values[0].value_data, None);
}
