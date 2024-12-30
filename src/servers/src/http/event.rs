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

use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use api::v1::{RowInsertRequest, RowInsertRequests, Rows};
use axum::body::HttpBody;
use axum::extract::{FromRequest, Multipart, Path, Query, State};
use axum::headers::ContentType;
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{async_trait, BoxError, Extension, Json, TypedHeader};
use common_error::ext::ErrorExt;
use common_query::{Output, OutputData};
use common_telemetry::{error, warn};
use datatypes::value::column_data_to_json;
use lazy_static::lazy_static;
use pipeline::error::PipelineTransformSnafu;
use pipeline::util::to_pipeline_version;
use pipeline::{GreptimeTransformer, PipelineVersion};
use serde::{Deserialize, Serialize};
use serde_json::{json, Deserializer, Map, Value};
use session::context::{Channel, QueryContext, QueryContextRef};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    status_code_to_http_status, CatalogSnafu, Error, InvalidParameterSnafu, ParseJsonSnafu,
    PipelineSnafu, Result, UnsupportedContentTypeSnafu,
};
use crate::http::header::CONTENT_TYPE_PROTOBUF_STR;
use crate::http::result::greptime_manage_resp::GreptimedbManageResponse;
use crate::http::result::greptime_result_v1::GreptimedbV1Response;
use crate::http::HttpResponse;
use crate::interceptor::{LogIngestInterceptor, LogIngestInterceptorRef};
use crate::metrics::{
    METRIC_FAILURE_VALUE, METRIC_HTTP_LOGS_INGESTION_COUNTER, METRIC_HTTP_LOGS_INGESTION_ELAPSED,
    METRIC_HTTP_LOGS_TRANSFORM_ELAPSED, METRIC_SUCCESS_VALUE,
};
use crate::query_handler::PipelineHandlerRef;

pub const GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME: &str = "greptime_identity";
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

pub struct PipelineContent(String);

#[async_trait]
impl<S, B> FromRequest<S, B> for PipelineContent
where
    B: HttpBody + Send + 'static,
    B::Data: Send,
    bytes::Bytes: std::convert::From<<B as HttpBody>::Data>,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request<B>, state: &S) -> StdResult<Self, Self::Rejection> {
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

    let version = to_pipeline_version(Some(version_str.clone())).context(PipelineSnafu)?;

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
    values: impl IntoIterator<Item = StdResult<Value, serde_json::Error>>,
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
fn dryrun_pipeline_inner(
    value: Vec<Value>,
    pipeline: &pipeline::Pipeline<GreptimeTransformer>,
) -> Result<Response> {
    let mut intermediate_state = pipeline.init_intermediate_state();

    let mut results = Vec::with_capacity(value.len());
    for v in value {
        pipeline
            .prepare(v, &mut intermediate_state)
            .context(PipelineTransformSnafu)
            .context(PipelineSnafu)?;
        let r = pipeline
            .exec_mut(&mut intermediate_state)
            .context(PipelineTransformSnafu)
            .context(PipelineSnafu)?;
        results.push(r);
        pipeline.reset_intermediate_state(&mut intermediate_state);
    }

    let colume_type_key = "colume_type";
    let data_type_key = "data_type";
    let name_key = "name";

    let schema = pipeline
        .schemas()
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
    let rows = results
        .into_iter()
        .map(|row| {
            let row = row
                .values
                .into_iter()
                .enumerate()
                .map(|(idx, v)| {
                    v.value_data
                        .map(|d| {
                            let mut map = Map::new();
                            map.insert("value".to_string(), column_data_to_json(d));
                            map.insert("key".to_string(), schema[idx][name_key].clone());
                            map.insert(
                                "semantic_type".to_string(),
                                schema[idx][colume_type_key].clone(),
                            );
                            map.insert("data_type".to_string(), schema[idx][data_type_key].clone());
                            Value::Object(map)
                        })
                        .unwrap_or(Value::Null)
                })
                .collect();
            Value::Array(row)
        })
        .collect::<Vec<_>>();
    let mut result = Map::new();
    result.insert("schema".to_string(), Value::Array(schema));
    result.insert("rows".to_string(), Value::Array(rows));
    let result = Value::Object(result);
    Ok(Json(result).into_response())
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

    match check_pipeline_dryrun_params_valid(&payload) {
        Some(params) => {
            let data = params.data;

            check_data_valid(data.len())?;

            match params.pipeline {
                None => {
                    let version =
                        to_pipeline_version(params.pipeline_version).context(PipelineSnafu)?;
                    let pipeline_name = check_pipeline_name_exists(params.pipeline_name)?;
                    let pipeline = handler
                        .get_pipeline(&pipeline_name, version, Arc::new(query_ctx))
                        .await?;
                    dryrun_pipeline_inner(data, &pipeline)
                }
                Some(pipeline) => {
                    let pipeline = handler.build_pipeline(&pipeline);
                    match pipeline {
                        Ok(pipeline) => match dryrun_pipeline_inner(data, &pipeline) {
                            Ok(response) => Ok(response),
                            Err(e) => Ok(add_step_info_for_pipeline_dryrun_error(
                                "Failed to exec pipeline",
                                e,
                            )),
                        },
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

            let version = to_pipeline_version(query_params.version).context(PipelineSnafu)?;

            let ignore_errors = query_params.ignore_errors.unwrap_or(false);

            let value =
                extract_pipeline_value_by_content_type(content_type, payload, ignore_errors)?;

            check_data_valid(value.len())?;

            query_ctx.set_channel(Channel::Http);
            let query_ctx = Arc::new(query_ctx);

            let pipeline = handler
                .get_pipeline(&pipeline_name, version, query_ctx.clone())
                .await?;

            dryrun_pipeline_inner(value, &pipeline)
        }
    }
}

#[axum_macros::debug_handler]
pub async fn log_ingester(
    State(log_state): State<LogState>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TypedHeader(content_type): TypedHeader<ContentType>,
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

    let version = to_pipeline_version(query_params.version).context(PipelineSnafu)?;

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
        table_name,
        value,
        query_ctx,
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
            .map(|line| Value::String(line.to_string()))
            .collect(),
        _ => UnsupportedContentTypeSnafu { content_type }.fail()?,
    })
}

pub(crate) async fn ingest_logs_inner(
    state: PipelineHandlerRef,
    pipeline_name: String,
    version: PipelineVersion,
    table_name: String,
    pipeline_data: Vec<Value>,
    query_ctx: QueryContextRef,
) -> Result<HttpResponse> {
    let db = query_ctx.get_db_string();
    let exec_timer = std::time::Instant::now();

    let mut results = Vec::with_capacity(pipeline_data.len());
    let transformed_data: Rows;
    if pipeline_name == GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME {
        let table = state
            .get_table(&table_name, &query_ctx)
            .await
            .context(CatalogSnafu)?;
        let rows = pipeline::identity_pipeline(pipeline_data, table)
            .context(PipelineTransformSnafu)
            .context(PipelineSnafu)?;

        transformed_data = rows
    } else {
        let pipeline = state
            .get_pipeline(&pipeline_name, version, query_ctx.clone())
            .await?;

        let transform_timer = std::time::Instant::now();
        let mut intermediate_state = pipeline.init_intermediate_state();

        for v in pipeline_data {
            pipeline
                .prepare(v, &mut intermediate_state)
                .inspect_err(|_| {
                    METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
                        .with_label_values(&[db.as_str(), METRIC_FAILURE_VALUE])
                        .observe(transform_timer.elapsed().as_secs_f64());
                })
                .context(PipelineTransformSnafu)
                .context(PipelineSnafu)?;
            let r = pipeline
                .exec_mut(&mut intermediate_state)
                .inspect_err(|_| {
                    METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
                        .with_label_values(&[db.as_str(), METRIC_FAILURE_VALUE])
                        .observe(transform_timer.elapsed().as_secs_f64());
                })
                .context(PipelineTransformSnafu)
                .context(PipelineSnafu)?;
            results.push(r);
            pipeline.reset_intermediate_state(&mut intermediate_state);
        }

        METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
            .with_label_values(&[db.as_str(), METRIC_SUCCESS_VALUE])
            .observe(transform_timer.elapsed().as_secs_f64());

        transformed_data = Rows {
            rows: results,
            schema: pipeline.schemas().clone(),
        };
    }

    let insert_request = RowInsertRequest {
        rows: Some(transformed_data),
        table_name: table_name.clone(),
    };
    let insert_requests = RowInsertRequests {
        inserts: vec![insert_request],
    };
    let output = state.insert(insert_requests, query_ctx).await;

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
