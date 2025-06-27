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
use pipeline::{
    parse, serde_value_to_vrl_value, setup_pipeline, Content, Pipeline, PipelineContext,
};

/// test util function to parse and execute pipeline
pub fn parse_and_exec(input_str: &str, pipeline_yaml: &str) -> Rows {
    let input_value = serde_json::from_str::<serde_json::Value>(input_str).unwrap();

    let yaml_content = Content::Yaml(pipeline_yaml);
    let pipeline: Pipeline = parse(&yaml_content).expect("failed to parse pipeline");

    let (pipeline, mut schema_info, pipeline_def, pipeline_param) = setup_pipeline!(pipeline);
    let pipeline_ctx = PipelineContext::new(
        &pipeline_def,
        &pipeline_param,
        session::context::Channel::Unknown,
    );

    let mut rows = Vec::new();

    match input_value {
        serde_json::Value::Array(array) => {
            for value in array {
                let intermediate_status = serde_value_to_vrl_value(value).unwrap();
                let row = pipeline
                    .exec_mut(intermediate_status, &pipeline_ctx, &mut schema_info)
                    .expect("failed to exec pipeline")
                    .into_transformed()
                    .expect("expect transformed result ");
                rows.push(row.0);
            }
        }
        serde_json::Value::Object(_) => {
            let intermediate_status = serde_value_to_vrl_value(input_value).unwrap();
            let row = pipeline
                .exec_mut(intermediate_status, &pipeline_ctx, &mut schema_info)
                .expect("failed to exec pipeline")
                .into_transformed()
                .expect("expect transformed result ");
            rows.push(row.0);
        }
        _ => {
            panic!("invalid input value");
        }
    }

    Rows {
        schema: schema_info.schema.clone(),
        rows,
    }
}

/// test util function to create column schema
#[allow(dead_code)]
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
