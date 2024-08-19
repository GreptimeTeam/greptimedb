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

use ahash::{HashMap, HashSet};
use common_telemetry::{debug, warn};
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

/// set the index for the processor keys
/// the index is the position of the key in the final intermediate keys
fn set_processor_keys_index(
    processors: &mut processor::Processors,
    final_intermediate_keys: &Vec<String>,
) -> Result<(), String> {
    let final_intermediate_key_index = final_intermediate_keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k.as_str(), i))
        .collect::<HashMap<_, _>>();
    for processor in processors.iter_mut() {
        for field in processor.fields_mut().iter_mut() {
            let index = final_intermediate_key_index.get(field.input_field.name.as_str()).ok_or(format!(
                    "input field {} is not found in intermediate keys: {final_intermediate_keys:?} when set processor keys index",
                    field.input_field.name
                ))?;
            field.set_input_index(*index);
            for (k, v) in field.output_fields_index_mapping.iter_mut() {
                let index = final_intermediate_key_index.get(k.as_str());
                match index {
                    Some(index) => {
                        *v = *index;
                    }
                    None => {
                        warn!(
                            "output field {k} is not found in intermediate keys: {final_intermediate_keys:?} when set processor keys index"
                        );
                    }
                }
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
    let final_intermediate_key_index = final_intermediate_keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k.as_str(), i))
        .collect::<HashMap<_, _>>();
    let output_key_index = output_keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k.as_str(), i))
        .collect::<HashMap<_, _>>();
    for transform in transforms.iter_mut() {
        for field in transform.fields.iter_mut() {
            let index = final_intermediate_key_index.get(field.input_field.name.as_str()).ok_or(format!(
                    "input field {} is not found in intermediate keys: {final_intermediate_keys:?} when set transform keys index",
                    field.input_field.name
                ))?;
            field.set_input_index(*index);
            for (k, v) in field.output_fields_index_mapping.iter_mut() {
                let index = output_key_index.get(k.as_str()).ok_or(format!(
                    "output field {k} is not found in output keys: {final_intermediate_keys:?} when set transform keys index"
                ))?;
                *v = *index;
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

            let transforms = if let Some(v) = doc[TRANSFORM].as_vec() {
                v.try_into()?
            } else {
                Transforms::default()
            };

            let mut transformer = T::new(transforms)?;
            let transforms = transformer.transforms_mut();

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
            set_transform_keys_index(transforms, &final_intermediate_keys, &output_keys)?;

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
    fn exec_map(&self, map: &mut Map) -> Result<(), String> {
        let v = map;
        for processor in self.processors.iter() {
            processor.exec_map(v)?;
        }
        Ok(())
    }

    pub fn exec(&self, mut val: Value) -> Result<T::Output, String> {
        let result = match val {
            Value::Map(ref mut map) => {
                self.exec_map(map)?;
                val
            }
            Value::Array(arr) => arr
                .values
                .into_iter()
                .map(|mut v| match v {
                    Value::Map(ref mut map) => {
                        self.exec_map(map)?;
                        Ok(v)
                    }
                    _ => Err(format!("expected a map, but got {}", v)),
                })
                .collect::<Result<Vec<Value>, String>>()
                .map(|values| Value::Array(value::Array { values }))?,
            _ => return Err(format!("expected a map or array, but got {}", val)),
        };

        self.transformer.transform(result)
    }

    pub fn exec_mut(&self, val: &mut Vec<Value>) -> Result<T::VecOutput, String> {
        for processor in self.processors.iter() {
            processor.exec_mut(val)?;
        }

        self.transformer.transform_mut(val)
    }

    pub fn prepare(&self, val: serde_json::Value, result: &mut [Value]) -> Result<(), String> {
        match val {
            serde_json::Value::Object(map) => {
                let mut search_from = 0;
                // because of the key in the json map is ordered
                for (payload_key, payload_value) in map.into_iter() {
                    if search_from >= self.required_keys.len() - 1 {
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
                return Err("expect object".to_string());
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

#[cfg(test)]
mod tests {

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

        let pipeline_yaml = r#"
---
description: Pipeline for Apache Tomcat

processors:
  - csv:
      field: my_field, my_field,field1, field2

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
        assert_eq!(
            &["greptime_timestamp", "my_field"].to_vec(),
            pipeline.required_keys()
        );
        assert_eq!(
            payload,
            vec![
                Value::Null,
                Value::String("1,2".to_string()),
                Value::Null,
                Value::Null
            ]
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
---
description: Pipeline for Apache Tomcat

processors:
  - csv:
      field: my_field,my_field, field1, field2

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
  - timestamp:
      field: test_time

transform:
  - field: test_time
    type: timestamp, ns
    index: time
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
