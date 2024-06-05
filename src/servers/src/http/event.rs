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

use api::v1::{RowInsertRequest, RowInsertRequests, Rows};
use axum::extract::{Json, Query, State};
use axum::headers::ContentType;
use axum::Extension;
use common_telemetry::{error, info};
use headers::HeaderMapExt;
use http::HeaderMap;
use pipeline::Value as PipelineValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    InsertLogSnafu, InvalidParameterSnafu, ParseJsonSnafu, Result, UnsupportedContentTypeSnafu,
};
use crate::http::greptime_result_v1::GreptimedbV1Response;
use crate::http::HttpResponse;
use crate::query_handler::LogHandlerRef;

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct LogIngesterQueryParams {
    pub table_name: Option<String>,
    pub db: Option<String>,
    pub pipeline_name: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn add_pipeline(
    State(handler): State<LogHandlerRef>,
    Extension(query_ctx): Extension<QueryContextRef>,
    Json(payload): Json<Value>,
) -> Result<String> {
    let name = payload["name"].as_str().context(InvalidParameterSnafu {
        reason: "name is required in payload",
    })?;
    let pipeline = payload["pipeline"]
        .as_str()
        .context(InvalidParameterSnafu {
            reason: "pipeline is required in payload",
        })?;

    let content_type = "yaml";
    let result = handler
        .insert_pipeline(name, content_type, pipeline, query_ctx)
        .await;

    result.map(|_| "ok".to_string()).map_err(|e| {
        error!(e; "failed to insert pipeline");
        e
    })
}

#[axum_macros::debug_handler]
pub async fn log_ingester(
    State(state): State<LogHandlerRef>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Extension(query_ctx): Extension<QueryContextRef>,
    // TypedHeader(content_type): TypedHeader<ContentType>,
    headers: HeaderMap,
    payload: String,
) -> Result<HttpResponse> {
    // TODO(shuiyisong): remove debug log
    info!("[log_header]: {:?}", headers);
    info!("[log_payload]: {:?}", payload);

    let content_type = headers
        .typed_get::<ContentType>()
        .unwrap_or(ContentType::text());

    let value;
    // TODO (qtang): we should decide json or jsonl
    if content_type == ContentType::json() {
        value = serde_json::from_str(&payload).context(ParseJsonSnafu)?;
    // TODO (qtang): we should decide which content type to support
    // form_url_cncoded type is only placeholder
    } else if content_type == ContentType::form_url_encoded() {
        value = parse_space_separated_log(payload)?;
    } else {
        return UnsupportedContentTypeSnafu { content_type }.fail();
    }
    log_ingester_inner(state, query_params, query_ctx, value)
        .await
        .or_else(|e| InsertLogSnafu { msg: e }.fail())
}

fn parse_space_separated_log(payload: String) -> Result<Value> {
    // ToStructuredLogSnafu
    let _log = payload.split_whitespace().collect::<Vec<&str>>();
    // TODO (qtang): implement this
    todo!()
}

async fn log_ingester_inner(
    state: LogHandlerRef,
    query_params: LogIngesterQueryParams,
    query_ctx: QueryContextRef,
    payload: Value,
) -> std::result::Result<HttpResponse, String> {
    let pipeline_id = query_params
        .pipeline_name
        .ok_or("pipeline_name is required".to_string())?;

    let pipeline_data = PipelineValue::try_from(payload)?;

    let pipeline = state
        .get_pipeline(&pipeline_id, query_ctx.clone())
        .await
        .map_err(|e| e.to_string())?;
    let transformed_data: Rows = pipeline.exec(pipeline_data)?;

    let table_name = query_params
        .table_name
        .ok_or("table_name is required".to_string())?;

    let insert_request = RowInsertRequest {
        rows: Some(transformed_data),
        table_name: table_name.clone(),
    };
    let insert_requests = RowInsertRequests {
        inserts: vec![insert_request],
    };
    state
        .insert_log(insert_requests, query_ctx)
        .await
        .map(|_| {
            HttpResponse::GreptimedbV1(GreptimedbV1Response {
                output: vec![],
                execution_time_ms: 0,
                resp_metrics: HashMap::new(),
            })
        })
        .map_err(|e| e.to_string())
}
