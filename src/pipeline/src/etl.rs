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

#![allow(dead_code)]

pub mod error;
pub mod field;
pub mod processor;
pub mod transform;
pub mod value;

use ahash::HashSet;
use common_telemetry::debug;
use error::{IntermediateKeyIndexSnafu, PrepareValueMustBeObjectSnafu, YamlLoadSnafu};
use itertools::Itertools;
use processor::{Processor, ProcessorBuilder, Processors};
use snafu::{OptionExt, ResultExt};
use transform::{TransformBuilders, Transformer, Transforms};
use value::Value;
use yaml_rust::YamlLoader;

use crate::etl::error::Result;

const DESCRIPTION: &str = "description";
const PROCESSORS: &str = "processors";
const TRANSFORM: &str = "transform";
const TRANSFORMS: &str = "transforms";

pub enum Content {
    Json(String),
    Yaml(String),
}

pub fn parse<T>(input: &Content) -> Result<Pipeline<T>>
where
    T: Transformer,
{
    match input {
        Content::Yaml(str) => {
            let docs = YamlLoader::load_from_str(str).context(YamlLoadSnafu)?;

            let doc = &docs[0];

            let description = doc[DESCRIPTION].as_str().map(|s| s.to_string());

            let processor_builder_list = if let Some(v) = doc[PROCESSORS].as_vec() {
                v.try_into()?
            } else {
                processor::ProcessorBuilderList::default()
            };

            let transform_builders =
                if let Some(v) = doc[TRANSFORMS].as_vec().or(doc[TRANSFORM].as_vec()) {
                    v.try_into()?
                } else {
                    TransformBuilders::default()
                };

            let processors_required_keys = &processor_builder_list.input_keys;
            let processors_output_keys = &processor_builder_list.output_keys;
            let processors_required_original_keys = &processor_builder_list.original_input_keys;

            debug!(
                "processors_required_original_keys: {:?}",
                processors_required_original_keys
            );
            debug!("processors_required_keys: {:?}", processors_required_keys);
            debug!("processors_output_keys: {:?}", processors_output_keys);

            let transforms_required_keys = &transform_builders.required_keys;
            let mut tr_keys = Vec::with_capacity(50);
            for key in transforms_required_keys.iter() {
                if !processors_output_keys.contains(key)
                    && !processors_required_original_keys.contains(key)
                {
                    tr_keys.push(key.clone());
                }
            }

            let mut required_keys = processors_required_original_keys.clone();

            required_keys.append(&mut tr_keys);
            required_keys.sort();

            debug!("required_keys: {:?}", required_keys);

            // intermediate keys are the keys that all processor and transformer required
            let ordered_intermediate_keys: Vec<String> = [
                processors_required_keys,
                transforms_required_keys,
                processors_output_keys,
            ]
            .iter()
            .flat_map(|l| l.iter())
            .collect::<HashSet<&String>>()
            .into_iter()
            .sorted()
            .cloned()
            .collect_vec();

            let mut final_intermediate_keys = Vec::with_capacity(ordered_intermediate_keys.len());
            let mut intermediate_keys_exclude_original =
                Vec::with_capacity(ordered_intermediate_keys.len());

            for key_name in ordered_intermediate_keys.iter() {
                if required_keys.contains(key_name) {
                    final_intermediate_keys.push(key_name.clone());
                } else {
                    intermediate_keys_exclude_original.push(key_name.clone());
                }
            }

            final_intermediate_keys.extend(intermediate_keys_exclude_original);

            let output_keys = transform_builders.output_keys.clone();

            let processors_kind_list = processor_builder_list
                .processor_builders
                .into_iter()
                .map(|builder| builder.build(&final_intermediate_keys))
                .collect::<Result<Vec<_>>>()?;
            let processors = Processors {
                processors: processors_kind_list,
                required_keys: processors_required_keys.clone(),
                output_keys: processors_output_keys.clone(),
                required_original_keys: processors_required_original_keys.clone(),
            };

            let transfor_list = transform_builders
                .builders
                .into_iter()
                .map(|builder| builder.build(&final_intermediate_keys, &output_keys))
                .collect::<Result<Vec<_>>>()?;

            let transformers = Transforms {
                transforms: transfor_list,
                required_keys: transforms_required_keys.clone(),
                output_keys: output_keys.clone(),
            };

            let transformer = T::new(transformers)?;

            Ok(Pipeline {
                description,
                processors,
                transformer,
                required_keys,
                output_keys,
                intermediate_keys: final_intermediate_keys,
            })
        }
        Content::Json(_) => unimplemented!(),
    }
}

#[derive(Debug)]
pub struct Pipeline<T>
where
    T: Transformer,
{
    description: Option<String>,
    processors: processor::Processors,
    transformer: T,
    /// required keys for the preprocessing from map data from user
    /// include all processor required and transformer required keys
    required_keys: Vec<String>,
    /// all output keys from the transformer
    output_keys: Vec<String>,
    /// intermediate keys from the processors
    intermediate_keys: Vec<String>,
    // pub on_failure: processor::Processors,
}

impl<T> std::fmt::Display for Pipeline<T>
where
    T: Transformer,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(description) = &self.description {
            writeln!(f, "description: {description}")?;
        }

        let processors = self.processors.iter().map(|p| p.kind()).join(",");
        writeln!(f, "processors: {processors}")?;

        writeln!(f, "transformer: {}", self.transformer)
    }
}

impl<T> Pipeline<T>
where
    T: Transformer,
{
    pub fn exec_mut(&self, val: &mut Vec<Value>) -> Result<T::VecOutput> {
        for processor in self.processors.iter() {
            processor.exec_mut(val)?;
        }

        self.transformer.transform_mut(val)
    }

    pub fn prepare_pipeline_value(&self, val: Value, result: &mut [Value]) -> Result<()> {
        match val {
            Value::Map(map) => {
                let mut search_from = 0;
                // because of the key in the json map is ordered
                for (payload_key, payload_value) in map.values.into_iter() {
                    if search_from >= self.required_keys.len() {
                        break;
                    }

                    // because of map key is ordered, required_keys is ordered too
                    if let Some(pos) = self.required_keys[search_from..]
                        .iter()
                        .position(|k| k == &payload_key)
                    {
                        result[search_from + pos] = payload_value;
                        // next search from is always after the current key
                        search_from += pos;
                    }
                }
            }
            Value::String(_) => {
                result[0] = val;
            }
            _ => {
                return PrepareValueMustBeObjectSnafu.fail();
            }
        }
        Ok(())
    }

    pub fn prepare(&self, val: serde_json::Value, result: &mut [Value]) -> Result<()> {
        match val {
            serde_json::Value::Object(map) => {
                let mut search_from = 0;
                // because of the key in the json map is ordered
                for (payload_key, payload_value) in map.into_iter() {
                    if search_from >= self.required_keys.len() {
                        break;
                    }

                    // because of map key is ordered, required_keys is ordered too
                    if let Some(pos) = self.required_keys[search_from..]
                        .iter()
                        .position(|k| k == &payload_key)
                    {
                        result[search_from + pos] = payload_value.try_into()?;
                        // next search from is always after the current key
                        search_from += pos;
                    }
                }
            }
            serde_json::Value::String(_) => {
                result[0] = val.try_into()?;
            }
            _ => {
                return PrepareValueMustBeObjectSnafu.fail();
            }
        }
        Ok(())
    }

    pub fn init_intermediate_state(&self) -> Vec<Value> {
        vec![Value::Null; self.intermediate_keys.len()]
    }

    pub fn reset_intermediate_state(&self, result: &mut [Value]) {
        for i in result {
            *i = Value::Null;
        }
    }

    pub fn processors(&self) -> &processor::Processors {
        &self.processors
    }

    pub fn transformer(&self) -> &T {
        &self.transformer
    }

    /// Required fields in user-supplied data
    pub fn required_keys(&self) -> &Vec<String> {
        &self.required_keys
    }

    /// All output keys from the pipeline
    pub fn output_keys(&self) -> &Vec<String> {
        &self.output_keys
    }

    /// intermediate keys from the processors
    pub fn intermediate_keys(&self) -> &Vec<String> {
        &self.intermediate_keys
    }

    pub fn schemas(&self) -> &Vec<greptime_proto::v1::ColumnSchema> {
        self.transformer.schemas()
    }
}

pub(crate) fn find_key_index(intermediate_keys: &[String], key: &str, kind: &str) -> Result<usize> {
    intermediate_keys
        .iter()
        .position(|k| k == key)
        .context(IntermediateKeyIndexSnafu { kind, key })
}

pub enum PipelineWay {
    Identity,
    Custom(std::sync::Arc<Pipeline<crate::GreptimeTransformer>>),
}

#[cfg(test)]
mod tests {

    use api::v1::Rows;
    use greptime_proto::v1::value::ValueData;
    use greptime_proto::v1::{self, ColumnDataType, SemanticType};

    use crate::etl::transform::GreptimeTransformer;
    use crate::etl::{parse, Content, Pipeline};
    use crate::Value;

    #[test]
    fn test_pipeline_prepare() {
        let input_value_str = r#"
                {
                    "my_field": "1,2",
                    "foo": "bar"
                }
            "#;
        let input_value: serde_json::Value = serde_json::from_str(input_value_str).unwrap();

        let pipeline_yaml = r#"description: 'Pipeline for Apache Tomcat'
processors:
  - csv:
      field: my_field
      target_fields: field1, field2
transform:
  - field: field1
    type: uint32
  - field: field2
    type: uint32
"#;
        let pipeline: Pipeline<GreptimeTransformer> =
            parse(&Content::Yaml(pipeline_yaml.into())).unwrap();
        let mut payload = pipeline.init_intermediate_state();
        pipeline.prepare(input_value, &mut payload).unwrap();
        assert_eq!(&["my_field"].to_vec(), pipeline.required_keys());
        assert_eq!(
            payload,
            vec![Value::String("1,2".to_string()), Value::Null, Value::Null]
        );
        let result = pipeline.exec_mut(&mut payload).unwrap();

        assert_eq!(result.values[0].value_data, Some(ValueData::U32Value(1)));
        assert_eq!(result.values[1].value_data, Some(ValueData::U32Value(2)));
        match &result.values[2].value_data {
            Some(ValueData::TimestampNanosecondValue(v)) => {
                assert_ne!(*v, 0);
            }
            _ => panic!("expect null value"),
        }
    }

    #[test]
    fn test_dissect_pipeline() {
        let message = r#"129.37.245.88 - meln1ks [01/Aug/2024:14:22:47 +0800] "PATCH /observability/metrics/production HTTP/1.0" 501 33085"#.to_string();
        let pipeline_str = r#"processors:
  - dissect:
      fields:
        - message
      patterns:
        - "%{ip} %{?ignored} %{username} [%{ts}] \"%{method} %{path} %{proto}\" %{status} %{bytes}"
  - timestamp:
      fields:
        - ts
      formats:
        - "%d/%b/%Y:%H:%M:%S %z"

transform:
  - fields:
      - ip
      - username
      - method
      - path
      - proto
    type: string
  - fields:
      - status
    type: uint16
  - fields:
      - bytes
    type: uint32
  - field: ts
    type: timestamp, ns
    index: time"#;
        let pipeline: Pipeline<GreptimeTransformer> =
            parse(&Content::Yaml(pipeline_str.into())).unwrap();
        let mut payload = pipeline.init_intermediate_state();
        pipeline
            .prepare(serde_json::Value::String(message), &mut payload)
            .unwrap();
        let result = pipeline.exec_mut(&mut payload).unwrap();
        let sechema = pipeline.schemas();

        assert_eq!(sechema.len(), result.values.len());
        let test = vec![
            (
                ColumnDataType::String as i32,
                Some(ValueData::StringValue("129.37.245.88".into())),
            ),
            (
                ColumnDataType::String as i32,
                Some(ValueData::StringValue("meln1ks".into())),
            ),
            (
                ColumnDataType::String as i32,
                Some(ValueData::StringValue("PATCH".into())),
            ),
            (
                ColumnDataType::String as i32,
                Some(ValueData::StringValue(
                    "/observability/metrics/production".into(),
                )),
            ),
            (
                ColumnDataType::String as i32,
                Some(ValueData::StringValue("HTTP/1.0".into())),
            ),
            (
                ColumnDataType::Uint16 as i32,
                Some(ValueData::U16Value(501)),
            ),
            (
                ColumnDataType::Uint32 as i32,
                Some(ValueData::U32Value(33085)),
            ),
            (
                ColumnDataType::TimestampNanosecond as i32,
                Some(ValueData::TimestampNanosecondValue(1722493367000000000)),
            ),
        ];
        for i in 0..sechema.len() {
            let schema = &sechema[i];
            let value = &result.values[i];
            assert_eq!(schema.datatype, test[i].0);
            assert_eq!(value.value_data, test[i].1);
        }
    }

    #[test]
    fn test_csv_pipeline() {
        let input_value_str = r#"
                {
                    "my_field": "1,2",
                    "foo": "bar"
                }
            "#;
        let input_value: serde_json::Value = serde_json::from_str(input_value_str).unwrap();

        let pipeline_yaml = r#"
description: Pipeline for Apache Tomcat
processors:
  - csv:
      field: my_field
      target_fields: field1, field2
transform:
  - field: field1
    type: uint32
  - field: field2
    type: uint32
"#;

        let pipeline: Pipeline<GreptimeTransformer> =
            parse(&Content::Yaml(pipeline_yaml.into())).unwrap();
        let mut payload = pipeline.init_intermediate_state();
        pipeline.prepare(input_value, &mut payload).unwrap();
        assert_eq!(&["my_field"].to_vec(), pipeline.required_keys());
        assert_eq!(
            payload,
            vec![Value::String("1,2".to_string()), Value::Null, Value::Null]
        );
        let result = pipeline.exec_mut(&mut payload).unwrap();
        assert_eq!(result.values[0].value_data, Some(ValueData::U32Value(1)));
        assert_eq!(result.values[1].value_data, Some(ValueData::U32Value(2)));
        match &result.values[2].value_data {
            Some(ValueData::TimestampNanosecondValue(v)) => {
                assert_ne!(*v, 0);
            }
            _ => panic!("expect null value"),
        }
    }

    #[test]
    fn test_date_pipeline() {
        let input_value_str = r#"
            {
                "my_field": "1,2",
                "foo": "bar",
                "test_time": "2014-5-17T04:34:56+00:00"
            }
        "#;
        let input_value: serde_json::Value = serde_json::from_str(input_value_str).unwrap();

        let pipeline_yaml = r#"
---
description: Pipeline for Apache Tomcat

processors:
  - timestamp:
      field: test_time

transform:
  - field: test_time
    type: timestamp, ns
    index: time
"#;

        let pipeline: Pipeline<GreptimeTransformer> =
            parse(&Content::Yaml(pipeline_yaml.into())).unwrap();
        let schema = pipeline.schemas().clone();
        let mut result = pipeline.init_intermediate_state();
        pipeline.prepare(input_value, &mut result).unwrap();
        let row = pipeline.exec_mut(&mut result).unwrap();
        let output = Rows {
            schema,
            rows: vec![row],
        };
        let schemas = output.schema;

        assert_eq!(schemas.len(), 1);
        let schema = schemas[0].clone();
        assert_eq!("test_time", schema.column_name);
        assert_eq!(ColumnDataType::TimestampNanosecond as i32, schema.datatype);
        assert_eq!(SemanticType::Timestamp as i32, schema.semantic_type);

        let row = output.rows[0].clone();
        assert_eq!(1, row.values.len());
        let value_data = row.values[0].clone().value_data;
        assert_eq!(
            Some(v1::value::ValueData::TimestampNanosecondValue(
                1400301296000000000
            )),
            value_data
        );
    }
}
