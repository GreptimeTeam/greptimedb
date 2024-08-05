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

use greptime_proto::v1::{ColumnDataType, ColumnSchema, Rows, SemanticType};
use pipeline::{parse, Content, GreptimeTransformer, Pipeline, Value};

/// test util function to parse and execute pipeline
pub fn parse_and_exec(input_str: &str, pipeline_yaml: &str) -> Rows {
    let input_value: Value = serde_json::from_str::<serde_json::Value>(input_str)
        .expect("failed to parse into json")
        .try_into()
        .expect("failed to convert into value");

    let yaml_content = Content::Yaml(pipeline_yaml.into());
    let pipeline: Pipeline<GreptimeTransformer> =
        parse(&yaml_content).expect("failed to parse pipeline");

    pipeline.exec(input_value).expect("failed to exec pipeline")
}

/// test util function to create column schema
pub fn make_column_schema(
    column_name: String,
    datatype: ColumnDataType,
    semantic_type: SemanticType,
) -> ColumnSchema {
    ColumnSchema {
        column_name,
        datatype: datatype.into(),
        semantic_type: semantic_type.into(),
        ..Default::default()
    }
}
