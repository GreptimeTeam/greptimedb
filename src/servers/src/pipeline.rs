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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use session::context::QueryContextRef;
use snafu::ResultExt;

use api::v1::{Row, RowInsertRequest, Rows};
use pipeline::error::PipelineTransformSnafu;
use pipeline::{
    DispatchedTo, GreptimeTransformer, Pipeline, PipelineExecInput, PipelineExecOutput,
    PipelineVersion,
};

use crate::error::{CatalogSnafu, PipelineSnafu, Result};
use crate::metrics::{
    METRIC_FAILURE_VALUE, METRIC_HTTP_LOGS_TRANSFORM_ELAPSED, METRIC_SUCCESS_VALUE,
};
use crate::query_handler::PipelineHandlerRef;

pub(crate) const GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME: &str = "greptime_identity";

#[inline]
pub(crate) fn pipeline_exec_with_intermediate_state(
    pipeline: &Arc<Pipeline<GreptimeTransformer>>,
    intermediate_state: &mut Vec<pipeline::Value>,
    transformed: &mut Vec<Row>,
    dispatched: &mut BTreeMap<DispatchedTo, Vec<Vec<pipeline::Value>>>,
    db: &str,
    transform_timer: &Instant,
    is_top_level: bool,
) -> Result<()> {
    let r = pipeline
        .exec_mut(intermediate_state)
        .inspect_err(|_| {
            if is_top_level {
                METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
                    .with_label_values(&[db, METRIC_FAILURE_VALUE])
                    .observe(transform_timer.elapsed().as_secs_f64());
            }
        })
        .context(PipelineTransformSnafu)
        .context(PipelineSnafu)?;

    match r {
        PipelineExecOutput::Transformed(row) => {
            transformed.push(row);
        }
        PipelineExecOutput::DispatchedTo(dispatched_to) => {
            if let Some(values) = dispatched.get_mut(&dispatched_to) {
                values.push(intermediate_state.clone());
            } else {
                dispatched.insert(dispatched_to, vec![intermediate_state.clone()]);
            }
        }
    }

    Ok(())
}

/// Enum for holding information of a pipeline, which is either pipeline itself,
/// or information that be used to retrieve a pipeline from `PipelineHandler`
pub(crate) enum PipelineDefinition<'a> {
    Resolved(Arc<Pipeline<GreptimeTransformer>>),
    ByNameAndValue((&'a str, PipelineVersion)),
    GreptimeIdentityPipeline,
}

impl<'a> PipelineDefinition<'a> {
    pub fn from_name(name: &'a str, version: PipelineVersion) -> Self {
        if name == GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME {
            Self::GreptimeIdentityPipeline
        } else {
            Self::ByNameAndValue((name, version))
        }
    }

    /// Never call this on `GreptimeIdentityPipeline` because it's a real pipeline
    pub async fn get_pipeline(
        self,
        handler: &PipelineHandlerRef,
        query_ctx: &QueryContextRef,
    ) -> Result<Arc<Pipeline<GreptimeTransformer>>> {
        match self {
            Self::Resolved(pipeline) => Ok(pipeline),
            Self::ByNameAndValue((name, version)) => {
                handler.get_pipeline(name, version, query_ctx.clone()).await
            }
            _ => {
                unreachable!("Never call get_pipeline on identity.")
            }
        }
    }
}

pub(crate) async fn run_pipeline<'a>(
    state: &PipelineHandlerRef,
    pipeline_definition: PipelineDefinition<'a>,
    values: PipelineExecInput,
    table_name: String,
    query_ctx: &QueryContextRef,
    db: &str,
    is_top_level: bool,
) -> Result<Vec<RowInsertRequest>> {
    if matches!(
        pipeline_definition,
        PipelineDefinition::GreptimeIdentityPipeline
    ) {
        let table = state
            .get_table(&table_name, &query_ctx)
            .await
            .context(CatalogSnafu)?;
        pipeline::identity_pipeline(values, table)
            .map(|rows| {
                vec![RowInsertRequest {
                    rows: Some(rows),
                    table_name,
                }]
            })
            .context(PipelineTransformSnafu)
            .context(PipelineSnafu)
    } else {
        let pipeline = pipeline_definition.get_pipeline(state, query_ctx).await?;

        let transform_timer = std::time::Instant::now();
        let mut intermediate_state = pipeline.init_intermediate_state();

        let mut transformed = Vec::with_capacity(values.len());
        let mut dispatched: BTreeMap<DispatchedTo, Vec<Vec<pipeline::Value>>> = BTreeMap::new();

        match values {
            PipelineExecInput::Original(array) => {
                for v in array {
                    pipeline
                        .prepare(v, &mut intermediate_state)
                        .inspect_err(|_| {
                            if is_top_level {
                                METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
                                    .with_label_values(&[db, METRIC_FAILURE_VALUE])
                                    .observe(transform_timer.elapsed().as_secs_f64());
                            }
                        })
                        .context(PipelineTransformSnafu)
                        .context(PipelineSnafu)?;

                    pipeline_exec_with_intermediate_state(
                        &pipeline,
                        &mut intermediate_state,
                        &mut transformed,
                        &mut dispatched,
                        db,
                        &transform_timer,
                        is_top_level,
                    )?;

                    pipeline.reset_intermediate_state(&mut intermediate_state);
                }
            }
            PipelineExecInput::Intermediate { array, .. } => {
                for mut intermediate_state in array {
                    pipeline_exec_with_intermediate_state(
                        &pipeline,
                        &mut intermediate_state,
                        &mut transformed,
                        &mut dispatched,
                        db,
                        &transform_timer,
                        is_top_level,
                    )?;
                }
            }
        }

        let mut results = Vec::new();
        // if current pipeline generates some transformed results, build it as
        // `RowInsertRequest` and append to results. If the pipeline doesn't
        // have dispatch, this will be only output of the pipeline.
        if !transformed.is_empty() {
            results.push(RowInsertRequest {
                rows: Some(Rows {
                    rows: transformed,
                    schema: pipeline.schemas().clone(),
                }),
                table_name: table_name.clone(),
            })
        }

        // if current pipeline contains dispatcher and has several rules, we may
        // already accumulated several dispatched rules and rows.
        for (dispatched_to, values) in dispatched {
            // we generate the new table name according to `table_part` and
            // current custom table name.
            let table_name = format!("{}_{}", &table_name, dispatched_to.table_part);
            let next_pipeline_name = dispatched_to
                .pipeline
                .as_deref()
                .unwrap_or(GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME);

            // run pipeline recursively. Note that the values we are going to
            // process is now intermediate version. It's in form of
            // `Vec<Vec<pipeline::Value>>`.
            let requests = Box::pin(run_pipeline(
                state,
                PipelineDefinition::from_name(next_pipeline_name, None),
                PipelineExecInput::Intermediate {
                    array: values,
                    // FIXME(sunng87): this intermediate_keys is incorrect. what
                    // we will need is the keys that generated after processors
                    keys: pipeline.intermediate_keys().clone(),
                },
                table_name,
                query_ctx,
                db,
                false,
            ))
            .await?;

            results.extend(requests);
        }

        if is_top_level {
            METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
                .with_label_values(&[db, METRIC_SUCCESS_VALUE])
                .observe(transform_timer.elapsed().as_secs_f64());
        }

        Ok(results)
    }
}
