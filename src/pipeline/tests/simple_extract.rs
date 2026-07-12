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

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, SemanticType};
use common_query::prelude::greptime_timestamp;
use lazy_static::lazy_static;

lazy_static! {
    static ref EXPECTED_SCHEMA: Vec<ColumnSchema> = vec![
        common::make_column_schema(
            "commit_author".to_string(),
            ColumnDataType::String,
            SemanticType::Field,
        ),
        common::make_column_schema(
            greptime_timestamp().to_string(),
            ColumnDataType::TimestampNanosecond,
            SemanticType::Timestamp,
        ),
    ];
}

#[test]
fn test_simple_extract() {
    let input_value_str = r#"
    [
      {
        "commit": {
            "commitTime": "1573840000.000",
            "commitAuthor": "test"
        }
      }
    ]
"#;

    let pipeline_yaml = r#"
---
processors:
  - simple_extract:
      field: commit, commit_author
      key: "commitAuthor"

transform:
  - field: commit_author
    type: string
"#;

    let output = common::parse_and_exec(input_value_str, pipeline_yaml);

    assert_eq!(output.schema, *EXPECTED_SCHEMA);

    assert_eq!(
        output.rows[0].values[0].value_data,
        Some(ValueData::StringValue("test".to_string()))
    );
}
