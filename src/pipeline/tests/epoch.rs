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

#[test]
fn test_parse_epoch() {
    let test_input = r#"
    {
        "input_s": "1722580862",
        "input_sec": "1722580862",
        "input_second": "1722580862",
        "input_ms": "1722580887794",
        "input_millisecond": "1722580887794",
        "input_milli": "1722580887794",
        "input_default": "1722580887794",
        "input_us": "1722580905423969",
        "input_microsecond": "1722580905423969",
        "input_micro": "1722580905423969",
        "input_ns": "1722580929863842048",
        "input_nanosecond": "1722580929863842048",
        "input_nano": "1722580929863842048"
    }"#;

    let pipeline_yaml = r#"
processors:
  - epoch:
      field: input_s
      resolution: s
  - epoch:
      field: input_sec
      resolution: sec
  - epoch:
      field: input_second
      resolution: second
  - epoch:
      field: input_ms
      resolution: ms
  - epoch:
      field: input_millisecond
      resolution: millisecond
  - epoch:
      field: input_milli
      resolution: milli
  - epoch:
      field: input_default
  - epoch:
      field: input_us
      resolution: us
  - epoch:
      field: input_microsecond
      resolution: microsecond
  - epoch:
      field: input_micro
      resolution: micro
  - epoch:
      field: input_ns
      resolution: ns
  - epoch:
      field: input_nanosecond
      resolution: nanosecond
  - epoch:
      field: input_nano
      resolution: nano

transform:
  - field: input_s
    type: epoch, s
  - field: input_sec
    type: epoch, sec
  - field: input_second
    type: epoch, second

  - field: input_ms
    type: epoch, ms
  - field: input_millisecond
    type: epoch, millisecond
  - field: input_milli
    type: epoch, milli
  - field: input_default
    type: epoch, milli

  - field: input_us
    type: epoch, us
  - field: input_microsecond
    type: epoch, microsecond
  - field: input_micro
    type: epoch, micro

  - field: input_ns
    type: epoch, ns
  - field: input_nanosecond
    type: epoch, nanosecond
  - field: input_nano
    type: epoch, nano
"#;
    fn make_time_field(name: &str, datatype: ColumnDataType) -> ColumnSchema {
        common::make_column_schema(name.to_string(), datatype, SemanticType::Field)
    }

    let expected_schema = vec![
        make_time_field("input_s", ColumnDataType::TimestampSecond),
        make_time_field("input_sec", ColumnDataType::TimestampSecond),
        make_time_field("input_second", ColumnDataType::TimestampSecond),
        make_time_field("input_ms", ColumnDataType::TimestampMillisecond),
        make_time_field("input_millisecond", ColumnDataType::TimestampMillisecond),
        make_time_field("input_milli", ColumnDataType::TimestampMillisecond),
        make_time_field("input_default", ColumnDataType::TimestampMillisecond),
        make_time_field("input_us", ColumnDataType::TimestampMicrosecond),
        make_time_field("input_microsecond", ColumnDataType::TimestampMicrosecond),
        make_time_field("input_micro", ColumnDataType::TimestampMicrosecond),
        make_time_field("input_ns", ColumnDataType::TimestampNanosecond),
        make_time_field("input_nanosecond", ColumnDataType::TimestampNanosecond),
        make_time_field("input_nano", ColumnDataType::TimestampNanosecond),
        common::make_column_schema(
            "greptime_timestamp".to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    let output = common::parse_and_exec(test_input, pipeline_yaml);
    assert_eq!(output.schema, expected_schema);

    for i in 0..2 {
        assert_eq!(
            output.rows[0].values[i].value_data,
            Some(ValueData::TimestampSecondValue(1722580862))
        );
    }
    for i in 3..6 {
        assert_eq!(
            output.rows[0].values[i].value_data,
            Some(ValueData::TimestampMillisecondValue(1722580887794))
        );
    }
    for i in 7..9 {
        assert_eq!(
            output.rows[0].values[i].value_data,
            Some(ValueData::TimestampMicrosecondValue(1722580905423969))
        );
    }
    for i in 10..12 {
        assert_eq!(
            output.rows[0].values[i].value_data,
            Some(ValueData::TimestampNanosecondValue(1722580929863842048))
        );
    }
}

#[test]
fn test_ignore_missing() {
    let empty_input = r#"{}"#;

    let pipeline_yaml = r#"
processors:
  - epoch:
      field: input_s
      resolution: s
      ignore_missing: true

transform:
  - fields:
        - input_s, ts
    type: epoch, s
"#;

    let expected_schema = vec![
        common::make_column_schema(
            "ts".to_string(),
            ColumnDataType::TimestampSecond,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "greptime_timestamp".to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];

    let output = common::parse_and_exec(empty_input, pipeline_yaml);
    assert_eq!(output.schema, expected_schema);
    assert_eq!(output.rows[0].values[0].value_data, None);
}

#[test]
fn test_default_wrong_resolution() {
    let test_input = r#"
    {
        "input_s": "1722580862",
        "input_nano": "1722583122284583936"
    }"#;

    let pipeline_yaml = r#"
processors:
  - epoch:
      fields: 
        - input_s
        - input_nano

transform:
  - fields:
      - input_s
    type: epoch, s
  - fields:
      - input_nano
    type: epoch, nano
"#;

    let expected_schema = vec![
        common::make_column_schema(
            "input_s".to_string(),
            ColumnDataType::TimestampSecond,
            SemanticType::Field,
        ),
        common::make_column_schema(
            "input_nano".to_string(),
            ColumnDataType::TimestampNanosecond,
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
    // this is actually wrong
    // TODO(shuiyisong): add check for type when converting epoch
    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(ValueData::TimestampMillisecondValue(1722580862))
    );
    assert_eq!(
        output.rows[0].values[1].value_data,
        Some(ValueData::TimestampMillisecondValue(1722583122284583936))
    );
}
