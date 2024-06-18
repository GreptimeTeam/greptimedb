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

use greptime_proto::v1::value::ValueData::TimestampMillisecondValue;
use greptime_proto::v1::{ColumnDataType, ColumnSchema, SemanticType};

mod common;

#[test]
fn test_gsub() {
    let input_value_str = r#"
    [
      {
        "reqTimeSec": "1573840000.000"
      }
    ]
"#;

    let pipeline_yaml = r#"
---
description: Pipeline for Akamai DataStream2 Log

processors:
  - gsub:
      field: reqTimeSec
      pattern: "\\."
      replacement: ""
  - epoch:
      field: reqTimeSec
      resolution: millisecond
      ignore_missing: true

transform:
  - field: reqTimeSec
    type: epoch, millisecond
    index: timestamp
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    let expected_schema = vec![ColumnSchema {
        column_name: "reqTimeSec".to_string(),
        datatype: ColumnDataType::TimestampMillisecond.into(),
        semantic_type: SemanticType::Timestamp.into(),
        datatype_extension: None,
    }];

    assert_eq!(output.schema, expected_schema);
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(TimestampMillisecondValue(1573840000000))
    );
}
