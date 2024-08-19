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
fn test_timestamp_parse_date() {
    let pipeline_yaml = r#"
processors:
  - timestamp:
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
fn test_timestamp_multi_formats() {
    let pipeline_yaml = r#"
processors:
  - timestamp:
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
fn test_timestamp_ignore_missing() {
    {
        let empty_input = r#"{}"#;

        let pipeline_yaml = r#"
processors:
  - timestamp:
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
    {
        let empty_input = r#"{}"#;

        let pipeline_yaml = r#"
    processors:
      - timestamp:
          field: input_s
          resolution: s
          ignore_missing: true
    
    transform:
      - fields:
            - input_s, ts
        type: timestamp, s
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
}

#[test]
fn test_timestamp_timezone() {
    let pipeline_yaml = r#"
processors:
  - timestamp:
      fields:
        - input_str
      formats:
        - ["%Y-%m-%dT%H:%M:%S", "Asia/Shanghai"]
        - ["%Y-%m-%dT%H:%M:%S%.3fZ", "Asia/Shanghai"]
      ignore_missing: true

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

#[test]
fn test_timestamp_parse_epoch() {
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
  - timestamp:
      field: input_s
      resolution: s
  - timestamp:
      field: input_sec
      resolution: sec
  - timestamp:
      field: input_second
      resolution: second
  - timestamp:
      field: input_ms
      resolution: ms
  - timestamp:
      field: input_millisecond
      resolution: millisecond
  - timestamp:
      field: input_milli
      resolution: milli
  - timestamp:
      field: input_default
  - timestamp:
      field: input_us
      resolution: us
  - timestamp:
      field: input_microsecond
      resolution: microsecond
  - timestamp:
      field: input_micro
      resolution: micro
  - timestamp:
      field: input_ns
      resolution: ns
  - timestamp:
      field: input_nanosecond
      resolution: nanosecond
  - timestamp:
      field: input_nano
      resolution: nano

transform:
  - field: input_s
    type: timestamp, s
  - field: input_sec
    type: timestamp, sec
  - field: input_second
    type: timestamp, second

  - field: input_ms
    type: timestamp, ms
  - field: input_millisecond
    type: timestamp, millisecond
  - field: input_milli
    type: timestamp, milli
  - field: input_default
    type: timestamp, milli

  - field: input_us
    type: timestamp, us
  - field: input_microsecond
    type: timestamp, microsecond
  - field: input_micro
    type: timestamp, micro

  - field: input_ns
    type: timestamp, ns
  - field: input_nanosecond
    type: timestamp, nanosecond
  - field: input_nano
    type: timestamp, nano
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
fn test_timestamp_default_wrong_resolution() {
    let test_input = r#"
    {
        "input_s": "1722580862",
        "input_nano": "1722583122284583936"
    }"#;

    let pipeline_yaml = r#"
processors:
  - timestamp:
      fields: 
        - input_s
        - input_nano

transform:
  - fields:
      - input_s
    type: timestamp, s
  - fields:
      - input_nano
    type: timestamp, nano
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
