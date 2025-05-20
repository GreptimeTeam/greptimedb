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

use ahash::{HashMap, HashMapExt};
use api::v1::Row;
use common_time::timestamp::TimeUnit;
use processor::{Processor, Processors};
use snafu::{ensure, OptionExt, ResultExt};
use transform::Transforms;
use value::Value;
use yaml_rust::YamlLoader;

use crate::dispatcher::{Dispatcher, Rule};
use crate::error::{
    InputValueMustBeObjectSnafu, IntermediateKeyIndexSnafu, Result,
    TransformNoTimestampProcessorSnafu, YamlLoadSnafu, YamlParseSnafu,
};
use crate::etl::processor::ProcessorKind;
use crate::tablesuffix::TableSuffixTemplate;
use crate::GreptimeTransformer;

const DESCRIPTION: &str = "description";
const PROCESSORS: &str = "processors";
const TRANSFORM: &str = "transform";
const TRANSFORMS: &str = "transforms";
const DISPATCHER: &str = "dispatcher";
const TABLESUFFIX: &str = "table_suffix";

pub type PipelineMap = std::collections::BTreeMap<String, Value>;

pub enum Content<'a> {
    Json(&'a str),
    Yaml(&'a str),
}

pub fn parse(input: &Content) -> Result<Pipeline> {
    match input {
        Content::Yaml(str) => {
            let docs = YamlLoader::load_from_str(str).context(YamlLoadSnafu)?;

            ensure!(docs.len() == 1, YamlParseSnafu);

            let doc = &docs[0];

            let description = doc[DESCRIPTION].as_str().map(|s| s.to_string());

            let processors = if let Some(v) = doc[PROCESSORS].as_vec() {
                v.try_into()?
            } else {
                Processors::default()
            };

            let transformers = if let Some(v) = doc[TRANSFORMS].as_vec().or(doc[TRANSFORM].as_vec())
            {
                v.try_into()?
            } else {
                Transforms::default()
            };

            let transformer = if transformers.is_empty() {
                // use auto transform
                // check processors have at least one timestamp-related processor
                let cnt = processors
                    .iter()
                    .filter(|p| {
                        matches!(
                            p,
                            ProcessorKind::Date(_)
                                | ProcessorKind::Timestamp(_)
                                | ProcessorKind::Epoch(_)
                        )
                    })
                    .count();
                ensure!(cnt > 0, TransformNoTimestampProcessorSnafu);
                None
            } else {
                Some(GreptimeTransformer::new(transformers)?)
            };

            let dispatcher = if !doc[DISPATCHER].is_badvalue() {
                Some(Dispatcher::try_from(&doc[DISPATCHER])?)
            } else {
                None
            };

            let tablesuffix = if !doc[TABLESUFFIX].is_badvalue() {
                Some(TableSuffixTemplate::try_from(&doc[TABLESUFFIX])?)
            } else {
                None
            };

            Ok(Pipeline {
                description,
                processors,
                transformer,
                dispatcher,
                tablesuffix,
            })
        }
        Content::Json(_) => unimplemented!(),
    }
}

#[derive(Debug)]
pub struct Pipeline {
    description: Option<String>,
    processors: processor::Processors,
    dispatcher: Option<Dispatcher>,
    transformer: Option<GreptimeTransformer>,
    tablesuffix: Option<TableSuffixTemplate>,
}

/// Where the pipeline executed is dispatched to, with context information
#[derive(Debug, Hash, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct DispatchedTo {
    pub table_suffix: String,
    pub pipeline: Option<String>,
}

impl From<&Rule> for DispatchedTo {
    fn from(value: &Rule) -> Self {
        DispatchedTo {
            table_suffix: value.table_suffix.clone(),
            pipeline: value.pipeline.clone(),
        }
    }
}

impl DispatchedTo {
    /// Generate destination table name from input
    pub fn dispatched_to_table_name(&self, original: &str) -> String {
        format!("{}_{}", &original, self.table_suffix)
    }
}

/// The result of pipeline execution
#[derive(Debug)]
pub enum PipelineExecOutput {
    Transformed((Row, Option<String>)),
    // table_suffix, ts_key -> unit
    AutoTransform(Option<String>, HashMap<String, TimeUnit>),
    DispatchedTo(DispatchedTo),
}

impl PipelineExecOutput {
    pub fn into_transformed(self) -> Option<(Row, Option<String>)> {
        if let Self::Transformed(o) = self {
            Some(o)
        } else {
            None
        }
    }

    pub fn into_dispatched(self) -> Option<DispatchedTo> {
        if let Self::DispatchedTo(d) = self {
            Some(d)
        } else {
            None
        }
    }
}

pub fn json_to_map(val: serde_json::Value) -> Result<PipelineMap> {
    match val {
        serde_json::Value::Object(map) => {
            let mut intermediate_state = PipelineMap::new();
            for (k, v) in map {
                intermediate_state.insert(k, Value::try_from(v)?);
            }
            Ok(intermediate_state)
        }
        _ => InputValueMustBeObjectSnafu.fail(),
    }
}

pub fn json_array_to_map(val: Vec<serde_json::Value>) -> Result<Vec<PipelineMap>> {
    val.into_iter().map(json_to_map).collect()
}

pub fn simd_json_to_map(val: simd_json::OwnedValue) -> Result<PipelineMap> {
    match val {
        simd_json::OwnedValue::Object(map) => {
            let mut intermediate_state = PipelineMap::new();
            for (k, v) in map.into_iter() {
                intermediate_state.insert(k, Value::try_from(v)?);
            }
            Ok(intermediate_state)
        }
        _ => InputValueMustBeObjectSnafu.fail(),
    }
}

pub fn simd_json_array_to_map(val: Vec<simd_json::OwnedValue>) -> Result<Vec<PipelineMap>> {
    val.into_iter().map(simd_json_to_map).collect()
}

impl Pipeline {
    pub fn exec_mut(&self, val: &mut PipelineMap) -> Result<PipelineExecOutput> {
        // process
        for processor in self.processors.iter() {
            processor.exec_mut(val)?;
        }

        // dispatch, fast return if matched
        if let Some(rule) = self.dispatcher.as_ref().and_then(|d| d.exec(val)) {
            return Ok(PipelineExecOutput::DispatchedTo(rule.into()));
        }

        if let Some(transformer) = self.transformer() {
            let row = transformer.transform_mut(val)?;
            let table_suffix = self.tablesuffix.as_ref().and_then(|t| t.apply(val));
            Ok(PipelineExecOutput::Transformed((row, table_suffix)))
        } else {
            let table_suffix = self.tablesuffix.as_ref().and_then(|t| t.apply(val));
            let mut ts_unit_map = HashMap::with_capacity(4);
            // get all ts values
            for (k, v) in val {
                if let Value::Timestamp(ts) = v {
                    if !ts_unit_map.contains_key(k) {
                        ts_unit_map.insert(k.clone(), ts.get_unit());
                    }
                }
            }
            Ok(PipelineExecOutput::AutoTransform(table_suffix, ts_unit_map))
        }
    }

    pub fn processors(&self) -> &processor::Processors {
        &self.processors
    }

    pub fn transformer(&self) -> Option<&GreptimeTransformer> {
        self.transformer.as_ref()
    }

    pub fn schemas(&self) -> Option<&Vec<greptime_proto::v1::ColumnSchema>> {
        self.transformer.as_ref().map(|t| t.schemas())
    }
}

pub(crate) fn find_key_index(intermediate_keys: &[String], key: &str, kind: &str) -> Result<usize> {
    intermediate_keys
        .iter()
        .position(|k| k == key)
        .context(IntermediateKeyIndexSnafu { kind, key })
}

#[cfg(test)]
mod tests {
    use api::v1::Rows;
    use greptime_proto::v1::value::ValueData;
    use greptime_proto::v1::{self, ColumnDataType, SemanticType};

    use super::*;

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
        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let mut payload = json_to_map(input_value).unwrap();
        let result = pipeline
            .exec_mut(&mut payload)
            .unwrap()
            .into_transformed()
            .unwrap();

        assert_eq!(result.0.values[0].value_data, Some(ValueData::U32Value(1)));
        assert_eq!(result.0.values[1].value_data, Some(ValueData::U32Value(2)));
        match &result.0.values[2].value_data {
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
        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_str)).unwrap();
        let mut payload = PipelineMap::new();
        payload.insert("message".to_string(), Value::String(message));
        let result = pipeline
            .exec_mut(&mut payload)
            .unwrap()
            .into_transformed()
            .unwrap();
        let sechema = pipeline.schemas().unwrap();

        assert_eq!(sechema.len(), result.0.values.len());
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
            let value = &result.0.values[i];
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

        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let mut payload = json_to_map(input_value).unwrap();
        let result = pipeline
            .exec_mut(&mut payload)
            .unwrap()
            .into_transformed()
            .unwrap();
        assert_eq!(result.0.values[0].value_data, Some(ValueData::U32Value(1)));
        assert_eq!(result.0.values[1].value_data, Some(ValueData::U32Value(2)));
        match &result.0.values[2].value_data {
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

        let pipeline_yaml = r#"---
description: Pipeline for Apache Tomcat

processors:
    - timestamp:
        field: test_time

transform:
    - field: test_time
      type: timestamp, ns
      index: time
    "#;

        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let schema = pipeline.schemas().unwrap().clone();
        let mut result = json_to_map(input_value).unwrap();

        let row = pipeline
            .exec_mut(&mut result)
            .unwrap()
            .into_transformed()
            .unwrap();
        let output = Rows {
            schema,
            rows: vec![row.0],
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

    #[test]
    fn test_dispatcher() {
        let pipeline_yaml = r#"
---
description: Pipeline for Apache Tomcat

processors:

dispatcher:
  field: typename
  rules:
    - value: http
      table_suffix: http_events
    - value: database
      table_suffix: db_events
      pipeline: database_pipeline

transform:
  - field: typename
    type: string

"#;
        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let dispatcher = pipeline.dispatcher.expect("expect dispatcher");
        assert_eq!(dispatcher.field, "typename");

        assert_eq!(dispatcher.rules.len(), 2);

        assert_eq!(
            dispatcher.rules[0],
            crate::dispatcher::Rule {
                value: Value::String("http".to_string()),
                table_suffix: "http_events".to_string(),
                pipeline: None
            }
        );

        assert_eq!(
            dispatcher.rules[1],
            crate::dispatcher::Rule {
                value: Value::String("database".to_string()),
                table_suffix: "db_events".to_string(),
                pipeline: Some("database_pipeline".to_string()),
            }
        );

        let bad_yaml1 = r#"
---
description: Pipeline for Apache Tomcat

processors:

dispatcher:
  _field: typename
  rules:
    - value: http
      table_suffix: http_events
    - value: database
      table_suffix: db_events
      pipeline: database_pipeline

transform:
  - field: typename
    type: string

"#;
        let bad_yaml2 = r#"
---
description: Pipeline for Apache Tomcat

processors:

dispatcher:
  field: typename
  rules:
    - value: http
      _table_suffix: http_events
    - value: database
      _table_suffix: db_events
      pipeline: database_pipeline

transform:
  - field: typename
    type: string

"#;
        let bad_yaml3 = r#"
---
description: Pipeline for Apache Tomcat

processors:

dispatcher:
  field: typename
  rules:
    - _value: http
      table_suffix: http_events
    - _value: database
      table_suffix: db_events
      pipeline: database_pipeline

transform:
  - field: typename
    type: string

"#;

        let r: Result<Pipeline> = parse(&Content::Yaml(bad_yaml1));
        assert!(r.is_err());
        let r: Result<Pipeline> = parse(&Content::Yaml(bad_yaml2));
        assert!(r.is_err());
        let r: Result<Pipeline> = parse(&Content::Yaml(bad_yaml3));
        assert!(r.is_err());
    }
}
