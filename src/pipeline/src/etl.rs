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
/// A single input can produce multiple output rows when the VRL processor
/// returns an array. Each element in the array becomes a separate row,
/// enabling one-to-many log expansion.
#[derive(Debug)]
pub struct TransformedOutput {
    /// Context options extracted from the input (e.g., table options, hints)
    pub opt: ContextOpt,
    /// The transformed rows. Usually contains one row, but can contain multiple
    /// when the pipeline expands a single input into multiple outputs.
    pub rows: Vec<Row>,
    /// Optional table suffix for routing to different tables
    pub table_suffix: Option<String>,
}

impl PipelineExecOutput {
    // Note: This is a test only function, do not use it in production.
    pub fn into_transformed(self) -> Option<(Vec<Row>, Option<String>)> {
        if let Self::Transformed(TransformedOutput {
            rows, table_suffix, ..
        }) = self
        {
            Some((rows, table_suffix))
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

        // extract the options first (for single object inputs)
        // this might be a breaking change, for table_suffix is now right after the processors
        // Note: for array inputs (one-to-many), options are extracted from each element in
        // transform_array_elements or values_to_rows
        let mut opt = if val.is_array() {
            ContextOpt::default()
        } else {
            ContextOpt::from_pipeline_map_to_opt(&mut val)?
        };
        let table_suffix = opt.resolve_table_suffix(self.tablesuffix.as_ref(), &val);

        let rows = match self.transformer() {
            TransformerMode::GreptimeTransformer(greptime_transformer) => {
                // Handle one-to-many: if val is an array, transform each element
                if let Some(arr) = val.as_array_mut() {
                    transform_array_elements(
                        arr,
                        greptime_transformer,
                        self.is_v1(),
                        schema_info,
                        pipeline_ctx,
                    )?
                } else {
                    // Single object transformation
                    let values = greptime_transformer.transform_mut(&mut val, self.is_v1())?;
                    if self.is_v1() {
                        // v1 dont combine with auto-transform
                        // so return immediately
                        return Ok(PipelineExecOutput::Transformed(TransformedOutput {
                            opt,
                            rows: vec![Row { values }],
                            table_suffix,
                        }));
                    }
                    // continue v2 process, and set the rest fields with auto-transform
                    // if transformer presents, then ts has been set
                    values_to_rows(schema_info, val, pipeline_ctx, Some(values), false)?
                }
            }
            TransformerMode::AutoTransform(ts_name, time_unit) => {
                // infer ts from the context
                // we've check that only one timestamp should exist

                // Create pipeline context with the found timestamp
                let def = crate::PipelineDefinition::GreptimeIdentityPipeline(Some(
                    IdentityTimeIndex::Epoch(ts_name.clone(), *time_unit, false),
                ));
                let n_ctx =
                    PipelineContext::new(&def, pipeline_ctx.pipeline_param, pipeline_ctx.channel);
                values_to_rows(schema_info, val, &n_ctx, None, true)?
            }
        };

        Ok(PipelineExecOutput::Transformed(TransformedOutput {
            opt,
            rows,
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

    pub fn is_variant_table_name(&self) -> bool {
        // even if the pipeline doesn't have dispatcher or table_suffix,
        // it can still be a variant because of VRL processor and hint
        self.dispatcher.is_some() || self.tablesuffix.is_some()
    }
}

/// Transforms an array of VRL values into rows.
///
/// This is used for one-to-many pipeline expansion where a VRL processor
/// returns an array. Each element in the array is transformed separately.
fn transform_array_elements(
    arr: &mut [VrlValue],
    transformer: &GreptimeTransformer,
    is_v1: bool,
    schema_info: &mut SchemaInfo,
    pipeline_ctx: &PipelineContext<'_>,
) -> Result<Vec<Row>> {
    use crate::error::{ArrayElementMustBeObjectSnafu, TransformArrayElementSnafu};

    let mut rows = Vec::with_capacity(arr.len());

    for (index, element) in arr.iter_mut().enumerate() {
        // Validate that each element is an object
        if !element.is_object() {
            return ArrayElementMustBeObjectSnafu {
                index,
                actual_type: element.kind_str().to_string(),
            }
            .fail();
        }

        let values = transformer
            .transform_mut(element, is_v1)
            .map_err(Box::new)
            .context(TransformArrayElementSnafu { index })?;

        if is_v1 {
            // v1 mode: just use transformer output directly
            rows.push(Row { values });
        } else {
            // v2 mode: combine with auto-transform for remaining fields
            let element_rows = values_to_rows(
                schema_info,
                element.clone(),
                pipeline_ctx,
                Some(values),
                false,
            )
            .map_err(Box::new)
            .context(TransformArrayElementSnafu { index })?;
            rows.extend(element_rows);
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
