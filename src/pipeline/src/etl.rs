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

use api::v1::Row;
use common_time::timestamp::TimeUnit;
use itertools::Itertools;
use processor::{Processor, Processors};
use snafu::{OptionExt, ResultExt, ensure};
use transform::Transforms;
use vrl::core::Value as VrlValue;
use yaml_rust::{Yaml, YamlLoader};

use crate::dispatcher::{Dispatcher, Rule};
use crate::error::{
    AutoTransformOneTimestampSnafu, Error, IntermediateKeyIndexSnafu, InvalidVersionNumberSnafu,
    Result, YamlLoadSnafu, YamlParseSnafu,
};
use crate::etl::processor::ProcessorKind;
use crate::etl::transform::transformer::greptime::values_to_rows;
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
        [original, &self.table_suffix].concat()
    }
}

/// The result of pipeline execution
#[derive(Debug)]
pub enum PipelineExecOutput {
    Transformed(TransformedOutput),
    DispatchedTo(DispatchedTo, VrlValue),
    Filtered,
}

/// Output from a successful pipeline transformation.
///
/// Each row can have its own table_suffix for routing to different tables
/// when using one-to-many expansion with per-row table suffix hints.
#[derive(Debug)]
pub struct TransformedOutput {
    pub opt: ContextOpt,
    /// Each row paired with its optional table suffix
    pub rows: Vec<(Row, Option<String>)>,
}

impl PipelineExecOutput {
    // Note: This is a test only function, do not use it in production.
    pub fn into_transformed(self) -> Option<Vec<(Row, Option<String>)>> {
        if let Self::Transformed(TransformedOutput { rows, .. }) = self {
            Some(rows)
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

impl Pipeline {
    fn is_v1(&self) -> bool {
        self.doc_version == PipelineDocVersion::V1
    }

    pub fn exec_mut(
        &self,
        mut val: VrlValue,
        pipeline_ctx: &PipelineContext<'_>,
        schema_info: &mut SchemaInfo,
    ) -> Result<PipelineExecOutput> {
        // process
        for processor in self.processors.iter() {
            val = processor.exec_mut(val)?;
            if val.is_null() {
                // line is filtered
                return Ok(PipelineExecOutput::Filtered);
            }
        }

        // dispatch, fast return if matched
        if let Some(rule) = self.dispatcher.as_ref().and_then(|d| d.exec(&val)) {
            return Ok(PipelineExecOutput::DispatchedTo(rule.into(), val));
        }

        // For array inputs (one-to-many), extract options from each element
        // For single object inputs, extract once
        let (opt, rows) = if val.is_array() {
            // Array input: extract per-row options in transform_array_elements
            let rows = match self.transformer() {
                TransformerMode::GreptimeTransformer(greptime_transformer) => {
                    transform_array_elements(
                        val.as_array_mut().unwrap(),
                        greptime_transformer,
                        self.is_v1(),
                        schema_info,
                        pipeline_ctx,
                        self.tablesuffix.as_ref(),
                    )?
                }
                TransformerMode::AutoTransform(ts_name, time_unit) => {
                    let def = crate::PipelineDefinition::GreptimeIdentityPipeline(Some(
                        IdentityTimeIndex::Epoch(ts_name.clone(), *time_unit, false),
                    ));
                    let n_ctx = PipelineContext::new(
                        &def,
                        pipeline_ctx.pipeline_param,
                        pipeline_ctx.channel,
                    );
                    values_to_rows(
                        schema_info,
                        val,
                        &n_ctx,
                        None,
                        true,
                        self.tablesuffix.as_ref(),
                    )?
                }
            };
            (ContextOpt::default(), rows)
        } else {
            // Single object input
            let mut opt = ContextOpt::from_pipeline_map_to_opt(&mut val)?;
            let table_suffix = opt.resolve_table_suffix(self.tablesuffix.as_ref(), &val);

            let rows = match self.transformer() {
                TransformerMode::GreptimeTransformer(greptime_transformer) => {
                    let values = greptime_transformer.transform_mut(&mut val, self.is_v1())?;
                    if self.is_v1() {
                        // v1 dont combine with auto-transform
                        return Ok(PipelineExecOutput::Transformed(TransformedOutput {
                            opt,
                            rows: vec![(Row { values }, table_suffix)],
                        }));
                    }
                    // continue v2 process, and set the rest fields with auto-transform
                    values_to_rows(
                        schema_info,
                        val,
                        pipeline_ctx,
                        Some(values),
                        false,
                        self.tablesuffix.as_ref(),
                    )?
                }
                TransformerMode::AutoTransform(ts_name, time_unit) => {
                    let def = crate::PipelineDefinition::GreptimeIdentityPipeline(Some(
                        IdentityTimeIndex::Epoch(ts_name.clone(), *time_unit, false),
                    ));
                    let n_ctx = PipelineContext::new(
                        &def,
                        pipeline_ctx.pipeline_param,
                        pipeline_ctx.channel,
                    );
                    values_to_rows(
                        schema_info,
                        val,
                        &n_ctx,
                        None,
                        true,
                        self.tablesuffix.as_ref(),
                    )?
                }
            };
            (opt, rows)
        };

        Ok(PipelineExecOutput::Transformed(TransformedOutput {
            opt,
            rows,
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

    pub fn is_variant_table_name(&self) -> bool {
        // even if the pipeline doesn't have dispatcher or table_suffix,
        // it can still be a variant because of VRL processor and hint
        self.dispatcher.is_some() || self.tablesuffix.is_some()
    }
}

/// Transforms an array of VRL values into rows with per-row table suffixes.
fn transform_array_elements(
    arr: &mut [VrlValue],
    transformer: &GreptimeTransformer,
    is_v1: bool,
    schema_info: &mut SchemaInfo,
    pipeline_ctx: &PipelineContext<'_>,
    tablesuffix_template: Option<&TableSuffixTemplate>,
) -> Result<Vec<(Row, Option<String>)>> {
    use crate::error::{ArrayElementMustBeObjectSnafu, TransformArrayElementSnafu};

    let mut rows = Vec::with_capacity(arr.len());

    for (index, element) in arr.iter_mut().enumerate() {
        if !element.is_object() {
            return ArrayElementMustBeObjectSnafu {
                index,
                actual_type: element.kind_str().to_string(),
            }
            .fail();
        }

        // Extract ContextOpt and table_suffix for this element
        let mut opt = ContextOpt::from_pipeline_map_to_opt(element)
            .map_err(Box::new)
            .context(TransformArrayElementSnafu { index })?;
        let table_suffix = opt.resolve_table_suffix(tablesuffix_template, element);

        let values = transformer
            .transform_mut(element, is_v1)
            .map_err(Box::new)
            .context(TransformArrayElementSnafu { index })?;

        if is_v1 {
            // v1 mode: just use transformer output directly
            rows.push((Row { values }, table_suffix));
        } else {
            // v2 mode: combine with auto-transform for remaining fields
            // Note: table_suffix already extracted, pass None to avoid double extraction
            let element_rows = values_to_rows(
                schema_info,
                element.clone(),
                pipeline_ctx,
                Some(values),
                false,
                None, // table_suffix already extracted above
            )
            .map_err(Box::new)
            .context(TransformArrayElementSnafu { index })?;
            // Apply the already-extracted table_suffix to all rows from this element
            rows.extend(
                element_rows
                    .into_iter()
                    .map(|(row, _)| (row, table_suffix.clone())),
            );
        }
    }

    Ok(rows)
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
/// ```ignore
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
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use api::v1::Rows;
    use greptime_proto::v1::value::ValueData;
    use greptime_proto::v1::{self, ColumnDataType, SemanticType};
    use vrl::prelude::Bytes;
    use vrl::value::KeyString;

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

        let payload = input_value.into();
        let mut result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();

        let (row, _table_suffix) = result.swap_remove(0);
        assert_eq!(row.values[0].value_data, Some(ValueData::U32Value(1)));
        assert_eq!(row.values[1].value_data, Some(ValueData::U32Value(2)));
        match &row.values[2].value_data {
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
        let payload = VrlValue::Object(BTreeMap::from([(
            KeyString::from("message"),
            VrlValue::Bytes(Bytes::from(message)),
        )]));

        let result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();

        assert_eq!(schema_info.schema.len(), result[0].0.values.len());
        let test = [
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
            let value = &result[0].0.values[i];
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

        let payload = input_value.into();
        let result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();
        assert_eq!(
            result[0].0.values[0].value_data,
            Some(ValueData::U32Value(1))
        );
        assert_eq!(
            result[0].0.values[1].value_data,
            Some(ValueData::U32Value(2))
        );
        match &result[0].0.values[2].value_data {
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
        let result = input_value.into();

        let rows_with_suffix = pipeline
            .exec_mut(result, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();
        let output = Rows {
            schema,
            rows: rows_with_suffix.into_iter().map(|(r, _)| r).collect(),
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
                value: VrlValue::Bytes(Bytes::from("http")),
                table_suffix: "http_events".to_string(),
                pipeline: None
            }
        );

        assert_eq!(
            dispatcher.rules[1],
            crate::dispatcher::Rule {
                value: VrlValue::Bytes(Bytes::from("database")),
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

    /// Test one-to-many VRL pipeline expansion.
    /// A VRL processor can return an array, which results in multiple output rows.
    #[test]
    fn test_one_to_many_vrl_expansion() {
        let pipeline_yaml = r#"
processors:
  - epoch:
      field: timestamp
      resolution: ms
  - vrl:
      source: |
        events = del(.events)
        base_host = del(.host)
        base_ts = del(.timestamp)
        map_values(array!(events)) -> |event| {
            {
                "host": base_host,
                "event_type": event.type,
                "event_value": event.value,
                "timestamp": base_ts
            }
        }

transform:
  - field: host
    type: string
  - field: event_type
    type: string
  - field: event_value
    type: int32
  - field: timestamp
    type: timestamp, ms
    index: time
"#;

        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let (pipeline, mut schema_info, pipeline_def, pipeline_param) = setup_pipeline!(pipeline);
        let pipeline_ctx = PipelineContext::new(
            &pipeline_def,
            &pipeline_param,
            session::context::Channel::Unknown,
        );

        // Input with 3 events
        let input_value: serde_json::Value = serde_json::from_str(
            r#"{
                "host": "server1",
                "timestamp": 1716668197217,
                "events": [
                    {"type": "cpu", "value": 80},
                    {"type": "memory", "value": 60},
                    {"type": "disk", "value": 45}
                ]
            }"#,
        )
        .unwrap();

        let payload = input_value.into();
        let result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();

        // Should produce 3 rows from 1 input
        assert_eq!(result.len(), 3);

        // Verify each row has correct structure
        for (row, _table_suffix) in &result {
            assert_eq!(row.values.len(), 4); // host, event_type, event_value, timestamp
            // First value should be "server1"
            assert_eq!(
                row.values[0].value_data,
                Some(ValueData::StringValue("server1".to_string()))
            );
            // Last value should be the timestamp
            assert_eq!(
                row.values[3].value_data,
                Some(ValueData::TimestampMillisecondValue(1716668197217))
            );
        }

        // Verify event types
        let event_types: Vec<_> = result
            .iter()
            .map(|(r, _)| match &r.values[1].value_data {
                Some(ValueData::StringValue(s)) => s.clone(),
                _ => panic!("expected string"),
            })
            .collect();
        assert!(event_types.contains(&"cpu".to_string()));
        assert!(event_types.contains(&"memory".to_string()));
        assert!(event_types.contains(&"disk".to_string()));
    }

    /// Test that single object output still works (backward compatibility)
    #[test]
    fn test_single_object_output_unchanged() {
        let pipeline_yaml = r#"
processors:
  - epoch:
      field: ts
      resolution: ms
  - vrl:
      source: |
        .processed = true
        .

transform:
  - field: name
    type: string
  - field: processed
    type: boolean
  - field: ts
    type: timestamp, ms
    index: time
"#;

        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let (pipeline, mut schema_info, pipeline_def, pipeline_param) = setup_pipeline!(pipeline);
        let pipeline_ctx = PipelineContext::new(
            &pipeline_def,
            &pipeline_param,
            session::context::Channel::Unknown,
        );

        let input_value: serde_json::Value = serde_json::from_str(
            r#"{
                "name": "test",
                "ts": 1716668197217
            }"#,
        )
        .unwrap();

        let payload = input_value.into();
        let result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();

        // Should produce exactly 1 row
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].0.values[0].value_data,
            Some(ValueData::StringValue("test".to_string()))
        );
        assert_eq!(
            result[0].0.values[1].value_data,
            Some(ValueData::BoolValue(true))
        );
    }

    /// Test that empty array produces zero rows
    #[test]
    fn test_empty_array_produces_zero_rows() {
        let pipeline_yaml = r#"
processors:
  - vrl:
      source: |
        .events

transform:
  - field: value
    type: int32
  - field: greptime_timestamp
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

        let input_value: serde_json::Value = serde_json::from_str(r#"{"events": []}"#).unwrap();

        let payload = input_value.into();
        let result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();

        // Empty array should produce zero rows
        assert_eq!(result.len(), 0);
    }

    /// Test that array elements must be objects
    #[test]
    fn test_array_element_must_be_object() {
        let pipeline_yaml = r#"
processors:
  - vrl:
      source: |
        .items

transform:
  - field: value
    type: int32
  - field: greptime_timestamp
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

        // Array with non-object elements should fail
        let input_value: serde_json::Value =
            serde_json::from_str(r#"{"items": [1, 2, 3]}"#).unwrap();

        let payload = input_value.into();
        let result = pipeline.exec_mut(payload, &pipeline_ctx, &mut schema_info);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("must be an object"),
            "Expected error about non-object element, got: {}",
            err_msg
        );
    }

    /// Test one-to-many with table suffix from VRL hint
    #[test]
    fn test_one_to_many_with_table_suffix_hint() {
        let pipeline_yaml = r#"
processors:
  - epoch:
      field: ts
      resolution: ms
  - vrl:
      source: |
        .greptime_table_suffix = "_" + string!(.category)
        .

transform:
  - field: name
    type: string
  - field: category
    type: string
  - field: ts
    type: timestamp, ms
    index: time
"#;

        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let (pipeline, mut schema_info, pipeline_def, pipeline_param) = setup_pipeline!(pipeline);
        let pipeline_ctx = PipelineContext::new(
            &pipeline_def,
            &pipeline_param,
            session::context::Channel::Unknown,
        );

        let input_value: serde_json::Value = serde_json::from_str(
            r#"{
                "name": "test",
                "category": "metrics",
                "ts": 1716668197217
            }"#,
        )
        .unwrap();

        let payload = input_value.into();
        let result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();

        // Should have table suffix extracted per row
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, Some("_metrics".to_string()));
    }

    /// Test one-to-many with per-row table suffix
    #[test]
    fn test_one_to_many_per_row_table_suffix() {
        let pipeline_yaml = r#"
processors:
  - epoch:
      field: timestamp
      resolution: ms
  - vrl:
      source: |
        events = del(.events)
        base_ts = del(.timestamp)
        
        map_values(array!(events)) -> |event| {
            suffix = "_" + string!(event.category)
            {
                "name": event.name,
                "value": event.value,
                "timestamp": base_ts,
                "greptime_table_suffix": suffix
            }
        }

transform:
  - field: name
    type: string
  - field: value
    type: int32
  - field: timestamp
    type: timestamp, ms
    index: time
"#;

        let pipeline: Pipeline = parse(&Content::Yaml(pipeline_yaml)).unwrap();
        let (pipeline, mut schema_info, pipeline_def, pipeline_param) = setup_pipeline!(pipeline);
        let pipeline_ctx = PipelineContext::new(
            &pipeline_def,
            &pipeline_param,
            session::context::Channel::Unknown,
        );

        // Input with events that should go to different tables
        let input_value: serde_json::Value = serde_json::from_str(
            r#"{
                "timestamp": 1716668197217,
                "events": [
                    {"name": "cpu_usage", "value": 80, "category": "cpu"},
                    {"name": "mem_usage", "value": 60, "category": "memory"},
                    {"name": "cpu_temp", "value": 45, "category": "cpu"}
                ]
            }"#,
        )
        .unwrap();

        let payload = input_value.into();
        let result = pipeline
            .exec_mut(payload, &pipeline_ctx, &mut schema_info)
            .unwrap()
            .into_transformed()
            .unwrap();

        // Should produce 3 rows
        assert_eq!(result.len(), 3);

        // Collect table suffixes
        let table_suffixes: Vec<_> = result.iter().map(|(_, suffix)| suffix.clone()).collect();

        // Should have different table suffixes per row
        assert!(table_suffixes.contains(&Some("_cpu".to_string())));
        assert!(table_suffixes.contains(&Some("_memory".to_string())));

        // Count rows per table suffix
        let cpu_count = table_suffixes
            .iter()
            .filter(|s| *s == &Some("_cpu".to_string()))
            .count();
        let memory_count = table_suffixes
            .iter()
            .filter(|s| *s == &Some("_memory".to_string()))
            .count();
        assert_eq!(cpu_count, 2);
        assert_eq!(memory_count, 1);
    }
}
