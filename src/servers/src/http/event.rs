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

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use api::v1::RowInsertRequests;
use async_trait::async_trait;
use axum::extract::{FromRequest, Multipart, Path, Query, Request, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use axum_extra::TypedHeader;
use common_error::ext::ErrorExt;
use common_query::{Output, OutputData};
use common_telemetry::{error, warn};
use datatypes::value::column_data_to_json;
use headers::ContentType;
use lazy_static::lazy_static;
use pipeline::error::PipelineTransformSnafu;
use pipeline::util::to_pipeline_version;
use pipeline::{GreptimePipelineParams, GreptimeTransformer, PipelineDefinition, PipelineVersion};
use serde::{Deserialize, Serialize};
use serde_json::{json, Deserializer, Map, Value};
use session::context::{Channel, QueryContext, QueryContextRef};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    status_code_to_http_status, Error, InvalidParameterSnafu, ParseJsonSnafu, PipelineSnafu,
    Result, UnsupportedContentTypeSnafu,
};
use crate::http::header::constants::GREPTIME_PIPELINE_PARAMS_HEADER;
use crate::http::header::CONTENT_TYPE_PROTOBUF_STR;
use crate::http::result::greptime_manage_resp::GreptimedbManageResponse;
use crate::http::result::greptime_result_v1::GreptimedbV1Response;
use crate::http::HttpResponse;
use crate::interceptor::{LogIngestInterceptor, LogIngestInterceptorRef};
use crate::metrics::{
    METRIC_FAILURE_VALUE, METRIC_HTTP_LOGS_INGESTION_COUNTER, METRIC_HTTP_LOGS_INGESTION_ELAPSED,
    METRIC_SUCCESS_VALUE,
};
use crate::pipeline::run_pipeline;
use crate::query_handler::PipelineHandlerRef;

const GREPTIME_INTERNAL_PIPELINE_NAME_PREFIX: &str = "greptime_";

lazy_static! {
    pub static ref JSON_CONTENT_TYPE: ContentType = ContentType::json();
    pub static ref TEXT_CONTENT_TYPE: ContentType = ContentType::text();
    pub static ref TEXT_UTF8_CONTENT_TYPE: ContentType = ContentType::text_utf8();
    pub static ref PB_CONTENT_TYPE: ContentType =
        ContentType::from_str(CONTENT_TYPE_PROTOBUF_STR).unwrap();
}

/// LogIngesterQueryParams is used for query params of log ingester API.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LogIngesterQueryParams {
    /// The database where log data will be written to.
    pub db: Option<String>,
    /// The table where log data will be written to.
    pub table: Option<String>,
    /// The pipeline that will be used for log ingestion.
    pub pipeline_name: Option<String>,
    /// The version of the pipeline to be used for log ingestion.
    pub version: Option<String>,
    /// Whether to ignore errors during log ingestion.
    pub ignore_errors: Option<bool>,
    /// The source of the log data.
    pub source: Option<String>,
    /// The JSON field name of the log message. If not provided, it will take the whole log as the message.
    /// The field must be at the top level of the JSON structure.
    pub msg_field: Option<String>,
}

/// LogIngestRequest is the internal request for log ingestion. The raw log input can be transformed into multiple LogIngestRequests.
/// Multiple LogIngestRequests will be ingested into the same database with the same pipeline.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LogIngestRequest {
    /// The table where the log data will be written to.
    pub table: String,
    /// The log data to be ingested.
    pub values: Vec<Value>,
}

pub struct PipelineContent(String);

impl<S> FromRequest<S> for PipelineContent
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let content_type_header = req.headers().get(CONTENT_TYPE);
        let content_type = content_type_header.and_then(|value| value.to_str().ok());
        if let Some(content_type) = content_type {
            if content_type.ends_with("yaml") {
                let payload = String::from_request(req, state)
                    .await
                    .map_err(IntoResponse::into_response)?;
                return Ok(Self(payload));
            }

            if content_type.starts_with("multipart/form-data") {
                let mut payload: Multipart = Multipart::from_request(req, state)
                    .await
                    .map_err(IntoResponse::into_response)?;
                let file = payload
                    .next_field()
                    .await
                    .map_err(IntoResponse::into_response)?;
                let payload = file
                    .ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response())?
                    .text()
                    .await
                    .map_err(IntoResponse::into_response)?;
                return Ok(Self(payload));
            }
        }

        Err(StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response())
    }
}

#[axum_macros::debug_handler]
pub async fn add_pipeline(
    State(state): State<LogState>,
    Path(pipeline_name): Path<String>,
    Extension(mut query_ctx): Extension<QueryContext>,
    PipelineContent(payload): PipelineContent,
) -> Result<GreptimedbManageResponse> {
    let start = Instant::now();
    let handler = state.log_handler;
    ensure!(
        !pipeline_name.is_empty(),
        InvalidParameterSnafu {
            reason: "pipeline_name is required in path",
        }
    );
    ensure!(
        !pipeline_name.starts_with(GREPTIME_INTERNAL_PIPELINE_NAME_PREFIX),
        InvalidParameterSnafu {
            reason: "pipeline_name cannot start with greptime_",
        }
    );
    ensure!(
        !payload.is_empty(),
        InvalidParameterSnafu {
            reason: "pipeline is required in body",
        }
    );

    query_ctx.set_channel(Channel::Http);
    let query_ctx = Arc::new(query_ctx);

    let content_type = "yaml";
    let result = handler
        .insert_pipeline(&pipeline_name, content_type, &payload, query_ctx)
        .await;

    result
        .map(|pipeline| {
            GreptimedbManageResponse::from_pipeline(
                pipeline_name,
                pipeline.0.to_timezone_aware_string(None),
                start.elapsed().as_millis() as u64,
            )
        })
        .map_err(|e| {
            error!(e; "failed to insert pipeline");
            e
        })
}

#[axum_macros::debug_handler]
pub async fn delete_pipeline(
    State(state): State<LogState>,
    Extension(mut query_ctx): Extension<QueryContext>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Path(pipeline_name): Path<String>,
) -> Result<GreptimedbManageResponse> {
    let start = Instant::now();
    let handler = state.log_handler;
    ensure!(
        !pipeline_name.is_empty(),
        InvalidParameterSnafu {
            reason: "pipeline_name is required",
        }
    );

    let version_str = query_params.version.context(InvalidParameterSnafu {
        reason: "version is required",
    })?;

    let version = to_pipeline_version(Some(&version_str)).context(PipelineSnafu)?;

    query_ctx.set_channel(Channel::Http);
    let query_ctx = Arc::new(query_ctx);

    handler
        .delete_pipeline(&pipeline_name, version, query_ctx)
        .await
        .map(|v| {
            if v.is_some() {
                GreptimedbManageResponse::from_pipeline(
                    pipeline_name,
                    version_str,
                    start.elapsed().as_millis() as u64,
                )
            } else {
                GreptimedbManageResponse::from_pipelines(vec![], start.elapsed().as_millis() as u64)
            }
        })
        .map_err(|e| {
            error!(e; "failed to delete pipeline");
            e
        })
}

/// Transform NDJSON array into a single array
/// always return an array
fn transform_ndjson_array_factory(
    values: impl IntoIterator<Item = Result<Value, serde_json::Error>>,
    ignore_error: bool,
) -> Result<Vec<Value>> {
    values
        .into_iter()
        .try_fold(Vec::with_capacity(100), |mut acc_array, item| match item {
            Ok(item_value) => {
                match item_value {
                    Value::Array(item_array) => {
                        acc_array.extend(item_array);
                    }
                    Value::Object(_) => {
                        acc_array.push(item_value);
                    }
                    _ => {
                        if !ignore_error {
                            warn!("invalid item in array: {:?}", item_value);
                            return InvalidParameterSnafu {
                                reason: format!("invalid item:{} in array", item_value),
                            }
                            .fail();
                        }
                    }
                }
                Ok(acc_array)
            }
            Err(_) if !ignore_error => item.map(|x| vec![x]).context(ParseJsonSnafu),
            Err(_) => {
                warn!("invalid item in array: {:?}", item);
                Ok(acc_array)
            }
        })
}

/// Dryrun pipeline with given data
async fn dryrun_pipeline_inner(
    value: Vec<Value>,
    pipeline: Arc<pipeline::Pipeline<GreptimeTransformer>>,
    pipeline_handler: PipelineHandlerRef,
    query_ctx: &QueryContextRef,
) -> Result<Response> {
    let params = GreptimePipelineParams::default();

    let results = run_pipeline(
        &pipeline_handler,
        PipelineDefinition::Resolved(pipeline),
        &params,
        pipeline::json_array_to_intermediate_state(value)
            .context(PipelineTransformSnafu)
            .context(PipelineSnafu)?,
        "dry_run".to_owned(),
        query_ctx,
        true,
    )
    .await?;

    let colume_type_key = "colume_type";
    let data_type_key = "data_type";
    let name_key = "name";

    let results = results
        .into_iter()
        .filter_map(|row| {
            if let Some(rows) = row.rows {
                let table_name = row.table_name;
                let schema = rows.schema;

                let schema = schema
                    .iter()
                    .map(|cs| {
                        let mut map = Map::new();
                        map.insert(name_key.to_string(), Value::String(cs.column_name.clone()));
                        map.insert(
                            data_type_key.to_string(),
                            Value::String(cs.datatype().as_str_name().to_string()),
                        );
                        map.insert(
                            colume_type_key.to_string(),
                            Value::String(cs.semantic_type().as_str_name().to_string()),
                        );
                        map.insert(
                            "fulltext".to_string(),
                            Value::Bool(
                                cs.options
                                    .clone()
                                    .is_some_and(|x| x.options.contains_key("fulltext")),
                            ),
                        );
                        Value::Object(map)
                    })
                    .collect::<Vec<_>>();

                let rows = rows
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.values
                            .into_iter()
                            .enumerate()
                            .map(|(idx, v)| {
                                v.value_data
                                    .map(|d| {
                                        let mut map = Map::new();
                                        map.insert("value".to_string(), column_data_to_json(d));
                                        map.insert(
                                            "key".to_string(),
                                            schema[idx][name_key].clone(),
                                        );
                                        map.insert(
                                            "semantic_type".to_string(),
                                            schema[idx][colume_type_key].clone(),
                                        );
                                        map.insert(
                                            "data_type".to_string(),
                                            schema[idx][data_type_key].clone(),
                                        );
                                        Value::Object(map)
                                    })
                                    .unwrap_or(Value::Null)
                            })
                            .collect()
                    })
                    .collect();

                let mut result = Map::new();
                result.insert("schema".to_string(), Value::Array(schema));
                result.insert("rows".to_string(), Value::Array(rows));
                result.insert("table_name".to_string(), Value::String(table_name));
                let result = Value::Object(result);
                Some(result)
            } else {
                None
            }
        })
        .collect();
    Ok(Json(Value::Array(results)).into_response())
}

/// Dryrun pipeline with given data
/// pipeline_name and pipeline_version to specify pipeline stored in db
/// pipeline to specify pipeline raw content
/// data to specify data
/// data maght be list of string or list of object
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PipelineDryrunParams {
    pub pipeline_name: Option<String>,
    pub pipeline_version: Option<String>,
    pub pipeline: Option<String>,
    pub data: Vec<Value>,
}

/// Check if the payload is valid json
/// Check if the payload contains pipeline or pipeline_name and data
/// Return Some if valid, None if invalid
fn check_pipeline_dryrun_params_valid(payload: &str) -> Option<PipelineDryrunParams> {
    match serde_json::from_str::<PipelineDryrunParams>(payload) {
        // payload with pipeline or pipeline_name and data is array
        Ok(params) if params.pipeline.is_some() || params.pipeline_name.is_some() => Some(params),
        // because of the pipeline_name or pipeline is required
        Ok(_) => None,
        // invalid json
        Err(_) => None,
    }
}

/// Check if the pipeline_name exists
fn check_pipeline_name_exists(pipeline_name: Option<String>) -> Result<String> {
    pipeline_name.context(InvalidParameterSnafu {
        reason: "pipeline_name is required",
    })
}

/// Check if the data length less than 10
fn check_data_valid(data_len: usize) -> Result<()> {
    ensure!(
        data_len <= 10,
        InvalidParameterSnafu {
            reason: "data is required",
        }
    );
    Ok(())
}

fn add_step_info_for_pipeline_dryrun_error(step_msg: &str, e: Error) -> Response {
    let body = Json(json!({
        "error": format!("{}: {}", step_msg,e.output_msg()),
    }));

    (status_code_to_http_status(&e.status_code()), body).into_response()
}

#[axum_macros::debug_handler]
pub async fn pipeline_dryrun(
    State(log_state): State<LogState>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TypedHeader(content_type): TypedHeader<ContentType>,
    payload: String,
) -> Result<Response> {
    let handler = log_state.log_handler;

    query_ctx.set_channel(Channel::Http);
    let query_ctx = Arc::new(query_ctx);

    match check_pipeline_dryrun_params_valid(&payload) {
        Some(params) => {
            let data = params.data;

            check_data_valid(data.len())?;

            match params.pipeline {
                None => {
                    let version = to_pipeline_version(params.pipeline_version.as_deref())
                        .context(PipelineSnafu)?;
                    let pipeline_name = check_pipeline_name_exists(params.pipeline_name)?;
                    let pipeline = handler
                        .get_pipeline(&pipeline_name, version, query_ctx.clone())
                        .await?;
                    dryrun_pipeline_inner(data, pipeline, handler, &query_ctx).await
                }
                Some(pipeline) => {
                    let pipeline = handler.build_pipeline(&pipeline);
                    match pipeline {
                        Ok(pipeline) => {
                            match dryrun_pipeline_inner(
                                data,
                                Arc::new(pipeline),
                                handler,
                                &query_ctx,
                            )
                            .await
                            {
                                Ok(response) => Ok(response),
                                Err(e) => Ok(add_step_info_for_pipeline_dryrun_error(
                                    "Failed to exec pipeline",
                                    e,
                                )),
                            }
                        }
                        Err(e) => Ok(add_step_info_for_pipeline_dryrun_error(
                            "Failed to build pipeline",
                            e,
                        )),
                    }
                }
            }
        }
        None => {
            // This path is for back compatibility with the previous dry run code
            // where the payload is just data (JSON or plain text) and the pipeline name
            // is specified using query param.
            let pipeline_name = check_pipeline_name_exists(query_params.pipeline_name)?;

            let version =
                to_pipeline_version(query_params.version.as_deref()).context(PipelineSnafu)?;

            let ignore_errors = query_params.ignore_errors.unwrap_or(false);

            let value =
                extract_pipeline_value_by_content_type(content_type, payload, ignore_errors)?;

            check_data_valid(value.len())?;

            let pipeline = handler
                .get_pipeline(&pipeline_name, version, query_ctx.clone())
                .await?;

            dryrun_pipeline_inner(value, pipeline, handler, &query_ctx).await
        }
    }
}

#[axum_macros::debug_handler]
pub async fn log_ingester(
    State(log_state): State<LogState>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TypedHeader(content_type): TypedHeader<ContentType>,
    headers: HeaderMap,
    payload: String,
) -> Result<HttpResponse> {
    // validate source and payload
    let source = query_params.source.as_deref();
    let response = match &log_state.log_validator {
        Some(validator) => validator.validate(source, &payload).await,
        None => None,
    };
    if let Some(response) = response {
        return response;
    }

    let handler = log_state.log_handler;

    let pipeline_name = query_params.pipeline_name.context(InvalidParameterSnafu {
        reason: "pipeline_name is required",
    })?;
    let table_name = query_params.table.context(InvalidParameterSnafu {
        reason: "table is required",
    })?;

    let version = to_pipeline_version(query_params.version.as_deref()).context(PipelineSnafu)?;

    let ignore_errors = query_params.ignore_errors.unwrap_or(false);

    let value = extract_pipeline_value_by_content_type(content_type, payload, ignore_errors)?;

    query_ctx.set_channel(Channel::Http);
    let query_ctx = Arc::new(query_ctx);

    let value = log_state
        .ingest_interceptor
        .as_ref()
        .pre_pipeline(value, query_ctx.clone())?;

    ingest_logs_inner(
        handler,
        pipeline_name,
        version,
        vec![LogIngestRequest {
            table: table_name,
            values: value,
        }],
        query_ctx,
        headers,
    )
    .await
}

fn extract_pipeline_value_by_content_type(
    content_type: ContentType,
    payload: String,
    ignore_errors: bool,
) -> Result<Vec<Value>> {
    Ok(match content_type {
        ct if ct == *JSON_CONTENT_TYPE => transform_ndjson_array_factory(
            Deserializer::from_str(&payload).into_iter(),
            ignore_errors,
        )?,
        ct if ct == *TEXT_CONTENT_TYPE || ct == *TEXT_UTF8_CONTENT_TYPE => payload
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| json!({"message": line}))
            .collect(),
        _ => UnsupportedContentTypeSnafu { content_type }.fail()?,
    })
}

pub(crate) async fn ingest_logs_inner(
    state: PipelineHandlerRef,
    pipeline_name: String,
    version: PipelineVersion,
    log_ingest_requests: Vec<LogIngestRequest>,
    query_ctx: QueryContextRef,
    headers: HeaderMap,
) -> Result<HttpResponse> {
    let db = query_ctx.get_db_string();
    let exec_timer = std::time::Instant::now();

    let mut insert_requests = Vec::with_capacity(log_ingest_requests.len());

    let pipeline_params = GreptimePipelineParams::from_params(
        headers
            .get(GREPTIME_PIPELINE_PARAMS_HEADER)
            .and_then(|v| v.to_str().ok()),
    );

    for request in log_ingest_requests {
        let requests = run_pipeline(
            &state,
            PipelineDefinition::from_name(&pipeline_name, version),
            &pipeline_params,
            pipeline::json_array_to_intermediate_state(request.values)
                .context(PipelineTransformSnafu)
                .context(PipelineSnafu)?,
            request.table,
            &query_ctx,
            true,
        )
        .await?;

        insert_requests.extend(requests);
    }

    let output = state
        .insert(
            RowInsertRequests {
                inserts: insert_requests,
            },
            query_ctx,
        )
        .await;

    if let Ok(Output {
        data: OutputData::AffectedRows(rows),
        meta: _,
    }) = &output
    {
        METRIC_HTTP_LOGS_INGESTION_COUNTER
            .with_label_values(&[db.as_str()])
            .inc_by(*rows as u64);
        METRIC_HTTP_LOGS_INGESTION_ELAPSED
            .with_label_values(&[db.as_str(), METRIC_SUCCESS_VALUE])
            .observe(exec_timer.elapsed().as_secs_f64());
    } else {
        METRIC_HTTP_LOGS_INGESTION_ELAPSED
            .with_label_values(&[db.as_str(), METRIC_FAILURE_VALUE])
            .observe(exec_timer.elapsed().as_secs_f64());
    }

    let response = GreptimedbV1Response::from_output(vec![output])
        .await
        .with_execution_time(exec_timer.elapsed().as_millis() as u64);
    Ok(response)
}

#[async_trait]
pub trait LogValidator: Send + Sync {
    /// validate payload by source before processing
    /// Return a `Some` result to indicate validation failure.
    async fn validate(&self, source: Option<&str>, payload: &str) -> Option<Result<HttpResponse>>;
}

pub type LogValidatorRef = Arc<dyn LogValidator + 'static>;

/// axum state struct to hold log handler and validator
#[derive(Clone)]
pub struct LogState {
    pub log_handler: PipelineHandlerRef,
    pub log_validator: Option<LogValidatorRef>,
    pub ingest_interceptor: Option<LogIngestInterceptorRef<Error>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_ndjson() {
        let s = "{\"a\": 1}\n{\"b\": 2}";
        let a = Value::Array(
            transform_ndjson_array_factory(Deserializer::from_str(s).into_iter(), false).unwrap(),
        )
        .to_string();
        assert_eq!(a, "[{\"a\":1},{\"b\":2}]");

        let s = "{\"a\": 1}";
        let a = Value::Array(
            transform_ndjson_array_factory(Deserializer::from_str(s).into_iter(), false).unwrap(),
        )
        .to_string();
        assert_eq!(a, "[{\"a\":1}]");

        let s = "[{\"a\": 1}]";
        let a = Value::Array(
            transform_ndjson_array_factory(Deserializer::from_str(s).into_iter(), false).unwrap(),
        )
        .to_string();
        assert_eq!(a, "[{\"a\":1}]");

        let s = "[{\"a\": 1}, {\"b\": 2}]";
        let a = Value::Array(
            transform_ndjson_array_factory(Deserializer::from_str(s).into_iter(), false).unwrap(),
        )
        .to_string();
        assert_eq!(a, "[{\"a\":1},{\"b\":2}]");
    }
}
