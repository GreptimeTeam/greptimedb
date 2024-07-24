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

pub mod field;
pub mod processor;
pub mod transform;
pub mod value;

use ahash::HashSet;
use common_telemetry::debug;
use itertools::{merge, Itertools};
use processor::Processor;
use transform::{Transformer, Transforms};
use value::{Map, Value};
use yaml_rust::YamlLoader;

const DESCRIPTION: &str = "description";
const PROCESSORS: &str = "processors";
const TRANSFORM: &str = "transform";

pub enum Content {
    Json(String),
    Yaml(String),
}

fn set_processor_keys_index(
    processors: &mut processor::Processors,
    final_intermediate_keys: &Vec<String>,
) -> Result<(), String> {
    for processor in processors.iter_mut() {
        for field in processor.fields_mut().iter_mut() {
            let index = final_intermediate_keys
                .iter()
                .position(|r| *r == field.input_field.name)
                .ok_or(format!(
                    "input field {} is not found in required keys: {final_intermediate_keys:?}",
                    field.input_field.name
                ))?;
            field.set_input_index(index);
            for (k, v) in field.output_fields.iter_mut() {
                let index = final_intermediate_keys
                    .iter()
                    .position(|r| *r == *k)
                    .ok_or(format!(
                    "output field {k} is not found in required keys: {final_intermediate_keys:?}"
                ))?;
                *v = index;
            }
        }
    }
    Ok(())
}

fn set_transform_keys_index(
    transforms: &mut Transforms,
    final_intermediate_keys: &[String],
    output_keys: &[String],
) -> Result<(), String> {
    for transform in transforms.iter_mut() {
        for field in transform.fields.iter_mut() {
            let index = final_intermediate_keys
                .iter()
                .position(|r| *r == field.input_field.name)
                .ok_or(format!(
                    "input field {} is not found in required keys: {final_intermediate_keys:?}",
                    field.input_field.name
                ))?;
            field.set_input_index(index);
            for (k, v) in field.output_fields.iter_mut() {
                let index = output_keys.iter().position(|r| *r == *k).ok_or(format!(
                    "output field {k} is not found in required keys: {final_intermediate_keys:?}"
                ))?;
                *v = index;
            }
        }
    }
    Ok(())
}

pub fn parse<T>(input: &Content) -> Result<Pipeline<T>, String>
where
    T: Transformer,
{
    match input {
        Content::Yaml(str) => {
            let docs = YamlLoader::load_from_str(str).map_err(|e| e.to_string())?;

            let doc = &docs[0];

            let description = doc[DESCRIPTION].as_str().map(|s| s.to_string());

            let mut processors = if let Some(v) = doc[PROCESSORS].as_vec() {
                v.try_into()?
            } else {
                processor::Processors::default()
            };

            let mut transforms = if let Some(v) = doc[TRANSFORM].as_vec() {
                v.try_into()?
            } else {
                Transforms::default()
            };

            let processors_output_keys = processors.output_keys();
            let processors_required_keys = processors.required_keys();
            let processors_required_original_keys = processors.required_original_keys();

            debug!(
                "processors_required_original_keys: {:?}",
                processors_required_original_keys
            );
            debug!("processors_required_keys: {:?}", processors_required_keys);
            debug!("processors_output_keys: {:?}", processors_output_keys);

            let transforms_required_keys = transforms.required_keys();
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
            let ordered_intermediate_keys: Vec<String> =
                merge(processors_required_keys, transforms_required_keys)
                    .cloned()
                    .collect::<HashSet<String>>()
                    .into_iter()
                    .sorted()
                    .collect();

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

            let output_keys = transforms.output_keys().clone();
            set_processor_keys_index(&mut processors, &final_intermediate_keys)?;
            set_transform_keys_index(&mut transforms, &final_intermediate_keys, &output_keys)?;

            Ok(Pipeline {
                description,
                processors,
                transformer: T::new(transforms)?,
                required_keys,
                output_keys,
                intermediate_keys: final_intermediate_keys,
            })
        }
        Content::Json(_) => unimplemented!(),
    }
}

#[derive(Debug, Clone)]
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
    fn exec_map<'a>(&self, map: &'a mut Map) -> Result<&'a mut Map, String> {
        let mut v = map;
        for processor in self.processors.iter() {
            v = processor.exec_map(v)?;
        }
        Ok(v)
    }

    pub fn exec(&self, mut val: value::Value) -> Result<T::Output, String> {
        let result = match val {
            value::Value::Map(ref mut map) => {
                self.exec_map(map)?;
                val
            }
            value::Value::Array(arr) => arr
                .values
                .into_iter()
                .map(|mut v| match v {
                    value::Value::Map(ref mut map) => {
                        self.exec_map(map)?;
                        Ok(v)
                    }
                    _ => Err(format!("expected a map, but got {}", v)),
                })
                .collect::<Result<Vec<value::Value>, String>>()
                .map(|values| value::Value::Array(value::Array { values }))?,
            _ => return Err(format!("expected a map or array, but got {}", val)),
        };

        self.transformer.transform(result)
    }

    pub fn exec_mut(&self, val: &mut Vec<value::Value>) -> Result<T::VecOutput, String> {
        for processor in self.processors.iter() {
            processor.exec_mut(val)?;
        }

        self.transformer.transform_mut(val)
    }

    pub fn preprepase(&self, val: serde_json::Value, result: &mut [Value]) -> Result<(), String> {
        match val {
            serde_json::Value::Object(map) => {
                let mut index = 0;

                // because of the key in the required_keys is ordered
                for (payload_key, payload_value) in map.into_iter() {
                    // find the key in the required_keys
                    let mut current_index = index;
                    while current_index < self.required_keys.len() {
                        if self.required_keys[current_index] == payload_key {
                            result[current_index] = payload_value.try_into()?;
                            index += 1;
                            break;
                            // can not find the key in the required_keys
                        } else {
                            // find the key in the required_keys
                            current_index += 1;
                        }
                    }
                }
            }
            serde_json::Value::String(_) => {
                result[0] = val.try_into()?;
            }
            _ => {
                return Err("expect object".to_string());
            }
        }
        Ok(())
    }

    pub fn init_intermediate_state(&self) -> Vec<Value> {
        vec![Value::Null; self.intermediate_keys.len()]
    }

    pub fn reset_intermediate_state(&self, result: &mut [Value]) {
        for i in 0..result.len() {
            result[i] = Value::Null;
        }
    }

    pub fn processors(&self) -> &processor::Processors {
        &self.processors
    }

    pub fn transformer(&self) -> &T {
        &self.transformer
    }

    pub fn required_keys(&self) -> &Vec<String> {
        &self.required_keys
    }

    pub fn output_keys(&self) -> &Vec<String> {
        &self.output_keys
    }

    pub fn intermediate_keys(&self) -> &Vec<String> {
        &self.intermediate_keys
    }

    pub fn schemas(&self) -> &Vec<greptime_proto::v1::ColumnSchema> {
        self.transformer.schemas()
    }
}

#[cfg(test)]
mod tests {

    use greptime_proto::v1::{self, ColumnDataType, SemanticType};

    use crate::etl::transform::GreptimeTransformer;
    use crate::etl::{parse, Content, Pipeline};

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
---
description: Pipeline for Apache Tomcat

processors:
  - csv:
      field: my_field, field1, field2

transform:
  - field: field1
    type: uint32
  - field: field2
    type: uint32
"#;

        let pipeline: Pipeline<GreptimeTransformer> =
            parse(&Content::Yaml(pipeline_yaml.into())).unwrap();
        let output = pipeline.exec(input_value.try_into().unwrap());
        assert!(output.is_ok());
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
  - date:
      field: test_time

transform:
  - field: test_time
    type: time
    index: timestamp
"#;

        let pipeline: Pipeline<GreptimeTransformer> =
            parse(&Content::Yaml(pipeline_yaml.into())).unwrap();
        let output = pipeline.exec(input_value.try_into().unwrap()).unwrap();
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
