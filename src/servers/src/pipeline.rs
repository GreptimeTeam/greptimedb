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

use api::v1::{RowInsertRequest, Rows};
use pipeline::{
    DispatchedTo, GreptimePipelineParams, GreptimeTransformer, IdentityTimeIndex, Pipeline,
    PipelineDefinition, PipelineExecOutput, PipelineMap, GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME,
};
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::error::{CatalogSnafu, PipelineSnafu, Result};
use crate::metrics::{
    METRIC_FAILURE_VALUE, METRIC_HTTP_LOGS_TRANSFORM_ELAPSED, METRIC_SUCCESS_VALUE,
};
use crate::query_handler::PipelineHandlerRef;

/// Never call this on `GreptimeIdentityPipeline` because it's a real pipeline
pub async fn get_pipeline(
    pipeline_def: PipelineDefinition,
    handler: &PipelineHandlerRef,
    query_ctx: &QueryContextRef,
) -> Result<Arc<Pipeline<GreptimeTransformer>>> {
    match pipeline_def {
        PipelineDefinition::Resolved(pipeline) => Ok(pipeline),
        PipelineDefinition::ByNameAndValue((name, version)) => {
            handler
                .get_pipeline(&name, version, query_ctx.clone())
                .await
        }
        _ => {
            unreachable!("Never call get_pipeline on identity.")
        }
    }
}

pub(crate) async fn run_pipeline(
    handler: &PipelineHandlerRef,
    pipeline_definition: PipelineDefinition,
    pipeline_parameters: &GreptimePipelineParams,
    data_array: Vec<PipelineMap>,
    table_name: String,
    query_ctx: &QueryContextRef,
    is_top_level: bool,
) -> Result<Vec<RowInsertRequest>> {
    match pipeline_definition {
        PipelineDefinition::GreptimeIdentityPipeline(custom_ts) => {
            run_identity_pipeline(
                handler,
                custom_ts,
                pipeline_parameters,
                data_array,
                table_name,
                query_ctx,
            )
            .await
        }
        _ => {
            run_custom_pipeline(
                handler,
                pipeline_definition,
                pipeline_parameters,
                data_array,
                table_name,
                query_ctx,
                is_top_level,
            )
            .await
        }
    }
}

async fn run_identity_pipeline(
    handler: &PipelineHandlerRef,
    custom_ts: Option<IdentityTimeIndex>,
    pipeline_parameters: &GreptimePipelineParams,
    data_array: Vec<PipelineMap>,
    table_name: String,
    query_ctx: &QueryContextRef,
) -> Result<Vec<RowInsertRequest>> {
    let table = handler
        .get_table(&table_name, query_ctx)
        .await
        .context(CatalogSnafu)?;
    pipeline::identity_pipeline(data_array, table, pipeline_parameters, custom_ts)
        .map(|rows| {
            vec![RowInsertRequest {
                rows: Some(rows),
                table_name,
            }]
        })
        .context(PipelineSnafu)
}

async fn run_custom_pipeline(
    handler: &PipelineHandlerRef,
    pipeline_definition: PipelineDefinition,
    pipeline_parameters: &GreptimePipelineParams,
    data_array: Vec<PipelineMap>,
    table_name: String,
    query_ctx: &QueryContextRef,
    is_top_level: bool,
) -> Result<Vec<RowInsertRequest>> {
    let db = query_ctx.get_db_string();
    let pipeline = get_pipeline(pipeline_definition, handler, query_ctx).await?;

    let transform_timer = std::time::Instant::now();

    let mut transformed = Vec::with_capacity(data_array.len());
    let mut dispatched: BTreeMap<DispatchedTo, Vec<PipelineMap>> = BTreeMap::new();

    for mut values in data_array {
        let r = pipeline
            .exec_mut(&mut values)
            .inspect_err(|_| {
                METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
                    .with_label_values(&[db.as_str(), METRIC_FAILURE_VALUE])
                    .observe(transform_timer.elapsed().as_secs_f64());
            })
            .context(PipelineSnafu)?;

        match r {
            PipelineExecOutput::Transformed(row) => {
                transformed.push(row);
            }
            PipelineExecOutput::DispatchedTo(dispatched_to) => {
                if let Some(coll) = dispatched.get_mut(&dispatched_to) {
                    coll.push(values);
                } else {
                    dispatched.insert(dispatched_to, vec![values]);
                }
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
    for (dispatched_to, coll) in dispatched {
        // we generate the new table name according to `table_part` and
        // current custom table name.
        let table_name = dispatched_to.dispatched_to_table_name(&table_name);
        let next_pipeline_name = dispatched_to
            .pipeline
            .as_deref()
            .unwrap_or(GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME);

        // run pipeline recursively.
        let requests = Box::pin(run_pipeline(
            handler,
            PipelineDefinition::from_name(next_pipeline_name, None, None).context(PipelineSnafu)?,
            pipeline_parameters,
            coll,
            table_name,
            query_ctx,
            false,
        ))
        .await?;

        results.extend(requests);
    }

    if is_top_level {
        METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
            .with_label_values(&[db.as_str(), METRIC_SUCCESS_VALUE])
            .observe(transform_timer.elapsed().as_secs_f64());
    }

    Ok(results)
}
