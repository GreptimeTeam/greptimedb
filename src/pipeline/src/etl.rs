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

use itertools::Itertools;
use transform::{Transformer, Transforms};
use value::Map;
use yaml_rust::YamlLoader;

const DESCRIPTION: &str = "description";
const PROCESSORS: &str = "processors";
const TRANSFORM: &str = "transform";

pub enum Content {
    Json(String),
    Yaml(String),
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

            let processors = if let Some(v) = doc[PROCESSORS].as_vec() {
                v.try_into()?
            } else {
                processor::Processors::default()
            };

            let transforms = if let Some(v) = doc[TRANSFORM].as_vec() {
                v.try_into()?
            } else {
                Transforms::default()
            };

            Ok(Pipeline {
                description,
                processors,
                transformer: T::new(transforms)?,
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
