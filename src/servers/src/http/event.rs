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

use std::collections::HashMap;
use std::result::Result as StdResult;

use api::v1::{RowInsertRequest, RowInsertRequests, Rows};
use axum::body::HttpBody;
use axum::extract::{FromRequest, Multipart, Path, Query, State};
use axum::headers::ContentType;
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{async_trait, BoxError, Extension, TypedHeader};
use common_telemetry::{error, warn};
use mime_guess::mime;
use pipeline::error::{CastTypeSnafu, ExecPipelineSnafu};
use pipeline::Value as PipelineValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Value};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

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
    State(handler): State<LogHandlerRef>,
    Path(pipeline_name): Path<String>,
    Extension(query_ctx): Extension<QueryContextRef>,
    PipelineContent(payload): PipelineContent,
) -> Result<String> {
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

    result.map(|_| "ok".to_string()).map_err(|e| {
        error!(e; "failed to insert pipeline");
        e
    })
}

/// Transform NDJSON array into a single array
fn transform_ndjson_array_factory(
    ignore_error: bool,
) -> impl FnMut(Result<Value>, std::result::Result<Value, serde_json::Error>) -> Result<Value> {
    move |acc, item| {
        acc.and_then(|acc| match acc {
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
                                return Err(InvalidParameterSnafu {
                                    reason: format!("invalid item:{} in array", item_value),
                                }
                                .build());
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
        })
    }
}

#[axum_macros::debug_handler]
pub async fn log_ingester(
    State(handler): State<LogHandlerRef>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Extension(query_ctx): Extension<QueryContextRef>,
    TypedHeader(content_type): TypedHeader<ContentType>,
    payload: String,
) -> Result<HttpResponse> {
    let pipeline_name = query_params.pipeline_name.context(InvalidParameterSnafu {
        reason: "pipeline_name is required",
    })?;
    let table_name = query_params.table.context(InvalidParameterSnafu {
        reason: "table is required",
    })?;

    let ignore_errors = query_params.ignore_errors.unwrap_or(false);

    let m: mime::Mime = content_type.clone().into();
    let value = match m.subtype() {
        mime::JSON => Deserializer::from_str(&payload).into_iter::<Value>().fold(
            Ok(Value::Array(Vec::with_capacity(100))),
            transform_ndjson_array_factory(ignore_errors),
        )?,
        // add more content type support
        _ => UnsupportedContentTypeSnafu { content_type }.fail()?,
    };

    log_ingester_inner(handler, pipeline_name, table_name, value, query_ctx).await
}

async fn log_ingester_inner(
    state: LogHandlerRef,
    pipeline_name: String,
    table_name: String,
    payload: Value,
    query_ctx: QueryContextRef,
) -> Result<HttpResponse> {
    let pipeline_data = PipelineValue::try_from(payload)
        .map_err(|reason| CastTypeSnafu { msg: reason }.build())
        .context(PipelineSnafu)?;

    let pipeline = state
        .get_pipeline(&pipeline_name, query_ctx.clone())
        .await?;
    let transformed_data: Rows = pipeline
        .exec(pipeline_data)
        .map_err(|reason| ExecPipelineSnafu { reason }.build())
        .context(PipelineSnafu)?;

    let insert_request = RowInsertRequest {
        rows: Some(transformed_data),
        table_name: table_name.clone(),
    };
    let insert_requests = RowInsertRequests {
        inserts: vec![insert_request],
    };
    state.insert_log(insert_requests, query_ctx).await.map(|_| {
        HttpResponse::GreptimedbV1(GreptimedbV1Response {
            output: vec![],
            execution_time_ms: 0,
            resp_metrics: HashMap::new(),
        })
    })
}
