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
use std::sync::Arc;

use api::v1::{RowInsertRequest, RowInsertRequests, Rows};
use axum::body::HttpBody;
use axum::extract::{FromRequest, Multipart, Path, Query, State};
use axum::headers::ContentType;
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{async_trait, BoxError, Extension, TypedHeader};
use common_telemetry::{error, warn};
use http::{HeaderMap, HeaderValue};
use mime_guess::mime;
use pipeline::error::{CastTypeSnafu, PipelineTransformSnafu};
use pipeline::util::to_pipeline_version;
use pipeline::{PipelineVersion, Value as PipelineValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Deserializer, Value};
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    InvalidParameterSnafu, ParseJsonSnafu, PipelineSnafu, Result, UnsupportedContentTypeSnafu,
};
use crate::http::greptime_result_v1::GreptimedbV1Response;
use crate::http::HttpResponse;
use crate::query_handler::LogHandlerRef;

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct LogIngesterQueryParams {
    pub table: Option<String>,
    pub db: Option<String>,
    pub pipeline_name: Option<String>,
    pub ignore_errors: Option<bool>,

    pub version: Option<String>,
    pub source: Option<String>,
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
    Extension(query_ctx): Extension<QueryContextRef>,
    PipelineContent(payload): PipelineContent,
) -> Result<impl IntoResponse> {
    let handler = state.log_handler;
    if pipeline_name.is_empty() {
        return Err(InvalidParameterSnafu {
            reason: "pipeline_name is required in path",
        }
        .build());
    }

    if payload.is_empty() {
        return Err(InvalidParameterSnafu {
            reason: "pipeline is required in body",
        }
        .build());
    }

    let content_type = "yaml";
    let result = handler
        .insert_pipeline(&pipeline_name, content_type, &payload, query_ctx)
        .await;

    result
        .map(|pipeline| {
            let json_header =
                HeaderValue::from_str(mime_guess::mime::APPLICATION_JSON.as_ref()).unwrap();
            let mut headers = HeaderMap::new();
            headers.append(CONTENT_TYPE, json_header);

            let version = pipeline.0.to_timezone_aware_string(None);
            (
                headers,
                json!({"version": version, "name": pipeline_name}).to_string(),
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
    Extension(query_ctx): Extension<QueryContextRef>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Path(pipeline_name): Path<String>,
) -> Result<String> {
    let handler = state.log_handler;
    ensure!(
        !pipeline_name.is_empty(),
        InvalidParameterSnafu {
            reason: "pipeline_name is required",
        }
    );

    let version = query_params.version.context(InvalidParameterSnafu {
        reason: "version is required",
    })?;

    let version = to_pipeline_version(Some(version)).context(PipelineSnafu)?;

    handler
        .delete_pipeline(&pipeline_name, version, query_ctx)
        .await
        .map(|_| "ok".to_string())
        .map_err(|e| {
            error!(e; "failed to delete pipeline");
            e
        })
}

/// Transform NDJSON array into a single array
fn transform_ndjson_array_factory(
    values: impl IntoIterator<Item = StdResult<Value, serde_json::Error>>,
    ignore_error: bool,
) -> Result<Value> {
    values.into_iter().try_fold(
        Value::Array(Vec::with_capacity(100)),
        |acc, item| match acc {
            Value::Array(mut acc_array) => {
                if let Ok(item_value) = item {
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
                    Ok(Value::Array(acc_array))
                } else if !ignore_error {
                    item.context(ParseJsonSnafu)
                } else {
                    warn!("invalid item in array: {:?}", item);
                    Ok(Value::Array(acc_array))
                }
            }
            _ => unreachable!("invalid acc: {:?}", acc),
        },
    )
}

#[axum_macros::debug_handler]
pub async fn log_ingester(
    State(log_state): State<LogState>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Extension(query_ctx): Extension<QueryContextRef>,
    TypedHeader(content_type): TypedHeader<ContentType>,
    payload: String,
) -> Result<HttpResponse> {
    if let Some(log_validator) = log_state.log_validator {
        if let Some(response) = log_validator.validate(query_params.source.clone(), &payload) {
            return response;
        }
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

    let m: mime::Mime = content_type.clone().into();
    let value = match m.subtype() {
        mime::JSON => transform_ndjson_array_factory(
            Deserializer::from_str(&payload).into_iter(),
            ignore_errors,
        )?,
        // add more content type support
        _ => UnsupportedContentTypeSnafu { content_type }.fail()?,
    };

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

async fn ingest_logs_inner(
    state: LogHandlerRef,
    pipeline_name: String,
    version: PipelineVersion,
    table_name: String,
    payload: Value,
    query_ctx: QueryContextRef,
) -> Result<HttpResponse> {
    let start = std::time::Instant::now();
    let pipeline_data = PipelineValue::try_from(payload)
        .map_err(|reason| CastTypeSnafu { msg: reason }.build())
        .context(PipelineSnafu)?;

    let pipeline = state
        .get_pipeline(&pipeline_name, version, query_ctx.clone())
        .await?;
    let transformed_data: Rows = pipeline
        .exec(pipeline_data)
        .map_err(|reason| PipelineTransformSnafu { reason }.build())
        .context(PipelineSnafu)?;

    let insert_request = RowInsertRequest {
        rows: Some(transformed_data),
        table_name: table_name.clone(),
    };
    let insert_requests = RowInsertRequests {
        inserts: vec![insert_request],
    };
    let output = state.insert_logs(insert_requests, query_ctx).await;

    let response = GreptimedbV1Response::from_output(vec![output])
        .await
        .with_execution_time(start.elapsed().as_millis() as u64);
    Ok(response)
}

pub trait LogValidator {
    /// validate payload by source before processing
    fn validate(&self, source: Option<String>, payload: &str) -> Option<Result<HttpResponse>>;
}

pub type LogValidatorRef = Arc<dyn LogValidator + Send + Sync>;

/// axum state struct to hold log handler and validator
#[derive(Clone)]
pub struct LogState {
    pub log_handler: LogHandlerRef,
    pub log_validator: Option<LogValidatorRef>,
}
