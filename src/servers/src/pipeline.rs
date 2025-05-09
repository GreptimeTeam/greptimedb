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

use ahash::{HashMap, HashMapExt};
use api::v1::{RowInsertRequest, Rows};
use itertools::Itertools;
use pipeline::error::AutoTransformOneTimestampSnafu;
use pipeline::{
    DispatchedTo, IdentityTimeIndex, Pipeline, PipelineContext, PipelineDefinition,
    PipelineExecOutput, PipelineMap, GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME,
};
use session::context::{Channel, QueryContextRef};
use snafu::{OptionExt, ResultExt};

use crate::error::{CatalogSnafu, PipelineSnafu, Result};
use crate::http::event::PipelineIngestRequest;
use crate::metrics::{
    METRIC_FAILURE_VALUE, METRIC_HTTP_LOGS_TRANSFORM_ELAPSED, METRIC_SUCCESS_VALUE,
};
use crate::query_handler::PipelineHandlerRef;

macro_rules! push_to_map {
    ($map:expr, $key:expr, $value:expr, $capacity:expr) => {
        $map.entry($key)
            .or_insert_with(|| Vec::with_capacity($capacity))
            .push($value);
    };
}

/// Never call this on `GreptimeIdentityPipeline` because it's a real pipeline
pub async fn get_pipeline(
    pipeline_def: &PipelineDefinition,
    handler: &PipelineHandlerRef,
    query_ctx: &QueryContextRef,
) -> Result<Arc<Pipeline>> {
    match pipeline_def {
        PipelineDefinition::Resolved(pipeline) => Ok(pipeline.clone()),
        PipelineDefinition::ByNameAndValue((name, version)) => {
            handler
                .get_pipeline(name, *version, query_ctx.clone())
                .await
        }
        _ => {
            unreachable!("Never call get_pipeline on identity.")
        }
    }
}

pub(crate) async fn run_pipeline(
    handler: &PipelineHandlerRef,
    pipeline_ctx: &PipelineContext<'_>,
    pipeline_req: PipelineIngestRequest,
    query_ctx: &QueryContextRef,
    is_top_level: bool,
) -> Result<Vec<RowInsertRequest>> {
    if pipeline_ctx.pipeline_definition.is_identity() {
        run_identity_pipeline(handler, pipeline_ctx, pipeline_req, query_ctx).await
    } else {
        run_custom_pipeline(handler, pipeline_ctx, pipeline_req, query_ctx, is_top_level).await
    }
}

async fn run_identity_pipeline(
    handler: &PipelineHandlerRef,
    pipeline_ctx: &PipelineContext<'_>,
    pipeline_req: PipelineIngestRequest,
    query_ctx: &QueryContextRef,
) -> Result<Vec<RowInsertRequest>> {
    let PipelineIngestRequest {
        table: table_name,
        values: data_array,
    } = pipeline_req;
    let table = if pipeline_ctx.channel == Channel::Prometheus {
        None
    } else {
        handler
            .get_table(&table_name, query_ctx)
            .await
            .context(CatalogSnafu)?
    };
    pipeline::identity_pipeline(data_array, table, pipeline_ctx)
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
    pipeline_ctx: &PipelineContext<'_>,
    pipeline_req: PipelineIngestRequest,
    query_ctx: &QueryContextRef,
    is_top_level: bool,
) -> Result<Vec<RowInsertRequest>> {
    let db = query_ctx.get_db_string();
    let pipeline = get_pipeline(pipeline_ctx.pipeline_definition, handler, query_ctx).await?;

    let transform_timer = std::time::Instant::now();

    let PipelineIngestRequest {
        table: table_name,
        values: pipeline_maps,
    } = pipeline_req;
    let arr_len = pipeline_maps.len();
    let mut transformed_map = HashMap::new();
    let mut dispatched: BTreeMap<DispatchedTo, Vec<PipelineMap>> = BTreeMap::new();
    let mut auto_map = HashMap::new();
    let mut auto_map_ts_keys = HashMap::new();

    for mut pipeline_map in pipeline_maps {
        let r = pipeline
            .exec_mut(&mut pipeline_map)
            .inspect_err(|_| {
                METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
                    .with_label_values(&[db.as_str(), METRIC_FAILURE_VALUE])
                    .observe(transform_timer.elapsed().as_secs_f64());
            })
            .context(PipelineSnafu)?;

        match r {
            PipelineExecOutput::Transformed((row, table_suffix)) => {
                let act_table_name = table_suffix_to_table_name(&table_name, table_suffix);
                push_to_map!(transformed_map, act_table_name, row, arr_len);
            }
            PipelineExecOutput::AutoTransform(table_suffix, ts_keys) => {
                let act_table_name = table_suffix_to_table_name(&table_name, table_suffix);
                push_to_map!(auto_map, act_table_name.clone(), pipeline_map, arr_len);
                auto_map_ts_keys
                    .entry(act_table_name)
                    .or_insert_with(HashMap::new)
                    .extend(ts_keys);
            }
            PipelineExecOutput::DispatchedTo(dispatched_to) => {
                push_to_map!(dispatched, dispatched_to, pipeline_map, arr_len);
            }
        }
    }

    let mut results = Vec::new();

    if let Some(s) = pipeline.schemas() {
        // transformed

        // if current pipeline generates some transformed results, build it as
        // `RowInsertRequest` and append to results. If the pipeline doesn't
        // have dispatch, this will be only output of the pipeline.
        for (table_name, rows) in transformed_map {
            results.push(RowInsertRequest {
                rows: Some(Rows {
                    rows,
                    schema: s.clone(),
                }),
                table_name,
            });
        }
    } else {
        // auto map
        for (table_name, pipeline_maps) in auto_map {
            if pipeline_maps.is_empty() {
                continue;
            }

            let ts_unit_map = auto_map_ts_keys
                .remove(&table_name)
                .context(AutoTransformOneTimestampSnafu)
                .context(PipelineSnafu)?;
            // only one timestamp key is allowed
            // which will be converted to ts index
            let (ts_key, unit) = ts_unit_map
                .into_iter()
                .exactly_one()
                .map_err(|_| AutoTransformOneTimestampSnafu.build())
                .context(PipelineSnafu)?;

            let ident_ts_index = IdentityTimeIndex::Epoch(ts_key.to_string(), unit, false);
            let new_def = PipelineDefinition::GreptimeIdentityPipeline(Some(ident_ts_index));
            let next_pipeline_ctx =
                PipelineContext::new(&new_def, pipeline_ctx.pipeline_param, pipeline_ctx.channel);

            let reqs = run_identity_pipeline(
                handler,
                &next_pipeline_ctx,
                PipelineIngestRequest {
                    table: table_name,
                    values: pipeline_maps,
                },
                query_ctx,
            )
            .await?;

            results.extend(reqs);
        }
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
        let next_pipeline_def =
            PipelineDefinition::from_name(next_pipeline_name, None, None).context(PipelineSnafu)?;
        let next_pipeline_ctx = PipelineContext::new(
            &next_pipeline_def,
            pipeline_ctx.pipeline_param,
            pipeline_ctx.channel,
        );
        let requests = Box::pin(run_pipeline(
            handler,
            &next_pipeline_ctx,
            PipelineIngestRequest {
                table: table_name,
                values: coll,
            },
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

#[inline]
fn table_suffix_to_table_name(table_name: &String, table_suffix: Option<String>) -> String {
    match table_suffix {
        Some(suffix) => format!("{}{}", table_name, suffix),
        None => table_name.clone(),
    }
}
