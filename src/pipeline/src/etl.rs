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
pub mod ctx_req;
pub mod field;
pub mod processor;
pub mod transform;
pub mod value;

use std::collections::BTreeMap;

use api::v1::Row;
use common_time::timestamp::TimeUnit;
use itertools::Itertools;
use processor::{Processor, Processors};
use snafu::{ensure, OptionExt, ResultExt};
use transform::Transforms;
use value::Value;
use yaml_rust::{Yaml, YamlLoader};

use crate::dispatcher::{Dispatcher, Rule};
use crate::error::{
    AutoTransformOneTimestampSnafu, Error, InputValueMustBeObjectSnafu, IntermediateKeyIndexSnafu,
    InvalidVersionNumberSnafu, Result, YamlLoadSnafu, YamlParseSnafu,
};
use crate::etl::processor::ProcessorKind;
use crate::etl::transform::transformer::greptime::values_to_row;
use crate::tablesuffix::TableSuffixTemplate;
use crate::{ContextOpt, GreptimeTransformer, IdentityTimeIndex, PipelineContext, SchemaInfo};

const DESCRIPTION: &str = "description";
const DOC_VERSION: &str = "version";
const PROCESSORS: &str = "processors";
const TRANSFORM: &str = "transform";
const TRANSFORMS: &str = "transforms";
const DISPATCHER: &str = "dispatcher";
const TABLESUFFIX: &str = "table_suffix";

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

            let doc_version = (&doc[DOC_VERSION]).try_into()?;

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
                    .filter_map(|p| match p {
                        ProcessorKind::Date(d) if !d.ignore_missing() => Some(
                            d.fields
                                .iter()
                                .map(|f| (f.target_or_input_field(), TimeUnit::Nanosecond))
                                .collect_vec(),
                        ),
                        ProcessorKind::Epoch(e) if !e.ignore_missing() => Some(
                            e.fields
                                .iter()
                                .map(|f| (f.target_or_input_field(), (&e.resolution).into()))
                                .collect_vec(),
                        ),
                        _ => None,
                    })
                    .flatten()
                    .collect_vec();
                ensure!(cnt.len() == 1, AutoTransformOneTimestampSnafu);

                let (ts_name, timeunit) = cnt.first().unwrap();
                TransformerMode::AutoTransform(ts_name.to_string(), *timeunit)
            } else {
                TransformerMode::GreptimeTransformer(GreptimeTransformer::new(
                    transformers,
                    &doc_version,
                )?)
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
                doc_version,
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

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub enum PipelineDocVersion {
    /// 1. All fields meant to be preserved have to explicitly set in the transform section.
    /// 2. Or no transform is set, then the auto-transform will be used.
    #[default]
    V1,

    /// A combination of transform and auto-transform.
    /// First it goes through the transform section,
    /// then use auto-transform to set the rest fields.
    ///
    /// This is useful if you only want to set the index field,
    /// and let the normal fields be auto-inferred.
    V2,
}

impl TryFrom<&Yaml> for PipelineDocVersion {
    type Error = Error;

    fn try_from(value: &Yaml) -> Result<Self> {
        if value.is_badvalue() || value.is_null() {
            return Ok(PipelineDocVersion::V1);
        }

        let version = match value {
            Yaml::String(s) => s
                .parse::<i64>()
                .map_err(|_| InvalidVersionNumberSnafu { version: s.clone() }.build())?,
            Yaml::Integer(i) => *i,
            _ => {
                return InvalidVersionNumberSnafu {
                    version: value.as_str().unwrap_or_default().to_string(),
                }
                .fail();
            }
        };

        match version {
            1 => Ok(PipelineDocVersion::V1),
            2 => Ok(PipelineDocVersion::V2),
            _ => InvalidVersionNumberSnafu {
                version: version.to_string(),
            }
            .fail(),
        }
    }
}

#[derive(Debug)]
pub struct Pipeline {
    doc_version: PipelineDocVersion,
    description: Option<String>,
    processors: processor::Processors,
    dispatcher: Option<Dispatcher>,
    transformer: TransformerMode,
    tablesuffix: Option<TableSuffixTemplate>,
}

#[derive(Debug, Clone)]
pub enum TransformerMode {
    GreptimeTransformer(GreptimeTransformer),
    AutoTransform(String, TimeUnit),
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
    Transformed(TransformedOutput),
    DispatchedTo(DispatchedTo, Value),
}

#[derive(Debug)]
pub struct TransformedOutput {
    pub opt: ContextOpt,
    pub row: Row,
    pub table_suffix: Option<String>,
}

impl PipelineExecOutput {
    // Note: This is a test only function, do not use it in production.
    pub fn into_transformed(self) -> Option<(Row, Option<String>)> {
        if let Self::Transformed(TransformedOutput {
            row, table_suffix, ..
        }) = self
        {
            Some((row, table_suffix))
        } else {
            None
        }
    }

    // Note: This is a test only function, do not use it in production.
    pub fn into_dispatched(self) -> Option<DispatchedTo> {
        if let Self::DispatchedTo(d, _) = self {
            Some(d)
        } else {
            None
        }
    }
}

pub fn json_to_map(val: serde_json::Value) -> Result<Value> {
    match val {
        serde_json::Value::Object(map) => {
            let mut intermediate_state = BTreeMap::new();
            for (k, v) in map {
                intermediate_state.insert(k, Value::try_from(v)?);
            }
            Ok(Value::Map(intermediate_state.into()))
        }
        _ => InputValueMustBeObjectSnafu.fail(),
    }
}

pub fn json_array_to_map(val: Vec<serde_json::Value>) -> Result<Vec<Value>> {
    val.into_iter().map(json_to_map).collect()
}

pub fn simd_json_to_map(val: simd_json::OwnedValue) -> Result<Value> {
    match val {
        simd_json::OwnedValue::Object(map) => {
            let mut intermediate_state = BTreeMap::new();
            for (k, v) in map.into_iter() {
                intermediate_state.insert(k, Value::try_from(v)?);
            }
            Ok(Value::Map(intermediate_state.into()))
        }
        _ => InputValueMustBeObjectSnafu.fail(),
    }
}

pub fn simd_json_array_to_map(val: Vec<simd_json::OwnedValue>) -> Result<Vec<Value>> {
    val.into_iter().map(simd_json_to_map).collect()
}

impl Pipeline {
    fn is_v1(&self) -> bool {
        self.doc_version == PipelineDocVersion::V1
    }

    pub fn exec_mut(
        &self,
        mut val: Value,
        pipeline_ctx: &PipelineContext<'_>,
        schema_info: &mut SchemaInfo,
    ) -> Result<PipelineExecOutput> {
        // process
        for processor in self.processors.iter() {
            val = processor.exec_mut(val)?;
        }

        // dispatch, fast return if matched
        if let Some(rule) = self.dispatcher.as_ref().and_then(|d| d.exec(&val)) {
            return Ok(PipelineExecOutput::DispatchedTo(rule.into(), val));
        }

        // extract the options first
        // this might be a breaking change, for table_suffix is now right after the processors
        let mut opt = ContextOpt::from_pipeline_map_to_opt(&mut val)?;
        let table_suffix = opt.resolve_table_suffix(self.tablesuffix.as_ref(), &val);

        let row = match self.transformer() {
            TransformerMode::GreptimeTransformer(greptime_transformer) => {
                let values = greptime_transformer.transform_mut(&mut val, self.is_v1())?;
                if self.is_v1() {
                    // v1 dont combine with auto-transform
                    // so return immediately
                    return Ok(PipelineExecOutput::Transformed(TransformedOutput {
                        opt,
                        row: Row { values },
                        table_suffix,
                    }));
                }
                // continue v2 process, check ts column and set the rest fields with auto-transform
                // if transformer presents, then ts has been set
                values_to_row(schema_info, val, pipeline_ctx, Some(values))?
            }
            TransformerMode::AutoTransform(ts_name, time_unit) => {
                // infer ts from the context
                // we've check that only one timestamp should exist

                // Create pipeline context with the found timestamp
                let def = crate::PipelineDefinition::GreptimeIdentityPipeline(Some(
                    IdentityTimeIndex::Epoch(ts_name.to_string(), *time_unit, false),
                ));
                let n_ctx =
                    PipelineContext::new(&def, pipeline_ctx.pipeline_param, pipeline_ctx.channel);
                values_to_row(schema_info, val, &n_ctx, None)?
            }
        };

        Ok(PipelineExecOutput::Transformed(TransformedOutput {
            opt,
            row,
            table_suffix,
        }))
    }

    pub fn processors(&self) -> &processor::Processors {
        &self.processors
    }

    pub fn transformer(&self) -> &TransformerMode {
        &self.transformer
    }

    // the method is for test purpose
    pub fn schemas(&self) -> Option<&Vec<greptime_proto::v1::ColumnSchema>> {
        match &self.transformer {
            TransformerMode::GreptimeTransformer(t) => Some(t.schemas()),
            TransformerMode::AutoTransform(_, _) => None,
        }
    }
}

pub(crate) fn find_key_index(intermediate_keys: &[String], key: &str, kind: &str) -> Result<usize> {
    intermediate_keys
        .iter()
        .position(|k| k == key)
        .context(IntermediateKeyIndexSnafu { kind, key })
}

/// This macro is test only, do not use it in production.
/// The schema_info cannot be used in auto-transform ts-infer mode for lacking the ts schema.
///
/// Usage:
/// ```rust
/// let (pipeline, schema_info, pipeline_def, pipeline_param) = setup_pipeline!(pipeline);
/// let pipeline_ctx = PipelineContext::new(&pipeline_def, &pipeline_param, Channel::Unknown);
/// ```
#[macro_export]
macro_rules! setup_pipeline {
    ($pipeline:expr) => {{
        use std::sync::Arc;

        use $crate::{GreptimePipelineParams, Pipeline, PipelineDefinition, SchemaInfo};

        let pipeline: Arc<Pipeline> = Arc::new($pipeline);
        let schema = pipeline.schemas().unwrap();
        let schema_info = SchemaInfo::from_schema_list(schema.clone());

        let pipeline_def = PipelineDefinition::Resolved(pipeline.clone());
        let pipeline_param = GreptimePipelineParams::default();

        (pipeline, schema_info, pipeline_def, pipeline_param)
    }};
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::Rows;
    use greptime_proto::v1::value::ValueData;
    use greptime_proto::v1::{self, ColumnDataType, SemanticType};

    use super::*;

    #[test]
    fn test_pipeline_prepare() {
        let input_value_str = r#"
                    {
                        "my_field": "1,2",
                        "foo": "bar",
                        "ts": "1"
                    }
                "#;
        let input_value: serde_json::Value = serde_json::from_str(input_value_str).unwrap();

        let pipeline_yaml = r#"description: 'Pipeline for Apache Tomcat'
processors:
    - csv:
        field: my_field
        target_fields: field1, field2
    - epoch:
        field: ts
        resolution: ns
transform:
    - field: field1
      type: uint32
    - field: field2
      type: uint32
    - field: ts
      type: timestamp, ns
      index: time
    "#;

        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let (pipeline, mut schema_info, pipeline_def, pipeline_param) = setup_pipeline!(pipeline);
        let pipeline_ctx = PipelineContext::new(
            &pipeline_def,
            &pipeline_param,
            session::context::Channel::Unknown,
        );

        let payload = json_to_map(input_value).unwrap();
        let result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();

        assert_eq!(result.0.values[0].value_data, Some(ValueData::U32Value(1)));
        assert_eq!(result.0.values[1].value_data, Some(ValueData::U32Value(2)));
        match &result.0.values[2].value_data {
            Some(ValueData::TimestampNanosecondValue(v)) => {
                assert_ne!(v, &0);
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
    - date:
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
        let pipeline = Arc::new(pipeline);
        let schema = pipeline.schemas().unwrap();
        let mut schema_info = SchemaInfo::from_schema_list(schema.clone());

        let pipeline_def = crate::PipelineDefinition::Resolved(pipeline.clone());
        let pipeline_param = crate::GreptimePipelineParams::default();
        let pipeline_ctx = PipelineContext::new(
            &pipeline_def,
            &pipeline_param,
            session::context::Channel::Unknown,
        );
        let mut payload = BTreeMap::new();
        payload.insert("message".to_string(), Value::String(message));
        let payload = Value::Map(payload.into());

        let result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();

        // println!("[DEBUG]schema_info: {:?}", schema_info.schema);
        // println!("[DEBUG]re: {:?}", result.0.values);

        assert_eq!(schema_info.schema.len(), result.0.values.len());
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
        // manually set schema
        let schema = pipeline.schemas().unwrap();
        for i in 0..schema.len() {
            let schema = &schema[i];
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
                        "foo": "bar",
                        "ts": "1"
                    }
                "#;
        let input_value: serde_json::Value = serde_json::from_str(input_value_str).unwrap();

        let pipeline_yaml = r#"
    description: Pipeline for Apache Tomcat
    processors:
      - csv:
          field: my_field
          target_fields: field1, field2
      - epoch:
          field: ts
          resolution: ns
    transform:
      - field: field1
        type: uint32
      - field: field2
        type: uint32
      - field: ts
        type: timestamp, ns
        index: time
    "#;

        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let (pipeline, mut schema_info, pipeline_def, pipeline_param) = setup_pipeline!(pipeline);
        let pipeline_ctx = PipelineContext::new(
            &pipeline_def,
            &pipeline_param,
            session::context::Channel::Unknown,
        );

        let payload = json_to_map(input_value).unwrap();
        let result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();
        assert_eq!(result.0.values[0].value_data, Some(ValueData::U32Value(1)));
        assert_eq!(result.0.values[1].value_data, Some(ValueData::U32Value(2)));
        match &result.0.values[2].value_data {
            Some(ValueData::TimestampNanosecondValue(v)) => {
                assert_ne!(v, &0);
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
    - date:
        field: test_time

transform:
    - field: test_time
      type: timestamp, ns
      index: time
    "#;

        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let pipeline = Arc::new(pipeline);
        let schema = pipeline.schemas().unwrap();
        let mut schema_info = SchemaInfo::from_schema_list(schema.clone());

        let pipeline_def = crate::PipelineDefinition::Resolved(pipeline.clone());
        let pipeline_param = crate::GreptimePipelineParams::default();
        let pipeline_ctx = PipelineContext::new(
            &pipeline_def,
            &pipeline_param,
            session::context::Channel::Unknown,
        );
        let schema = pipeline.schemas().unwrap().clone();
        let result = json_to_map(input_value).unwrap();

        let row = pipeline
            .exec_mut(result, &pipeline_ctx, &mut schema_info)
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
  - epoch:
      field: ts
      resolution: ns

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
  - field: ts
    type: timestamp, ns
    index: time
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
  - epoch:
      field: ts
      resolution: ns

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
  - field: ts
    type: timestamp, ns
    index: time
"#;
        let bad_yaml2 = r#"
---
description: Pipeline for Apache Tomcat

processors:
  - epoch:
      field: ts
      resolution: ns
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
  - field: ts
    type: timestamp, ns
    index: time
"#;
        let bad_yaml3 = r#"
---
description: Pipeline for Apache Tomcat

processors:
  - epoch:
      field: ts
      resolution: ns
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
  - field: ts
    type: timestamp, ns
    index: time
"#;

        let r: Result<Pipeline> = parse(&Content::Yaml(bad_yaml1));
        assert!(r.is_err());
        let r: Result<Pipeline> = parse(&Content::Yaml(bad_yaml2));
        assert!(r.is_err());
        let r: Result<Pipeline> = parse(&Content::Yaml(bad_yaml3));
        assert!(r.is_err());
    }
}
