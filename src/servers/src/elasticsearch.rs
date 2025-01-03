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

use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Query, State};
use axum::headers::ContentType;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::{Extension, TypedHeader};
use common_telemetry::{debug, error, warn};
use serde_json::{json, Deserializer, Value};
use session::context::{Channel, QueryContext};

use crate::http::event::{
    ingest_logs_inner, LogIngesterQueryParams, LogState, GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME,
};

const ELASTICSEARCH_VERSION: &str = "8.16.0";

// Return fake response for Elasticsearch ping request.
#[axum_macros::debug_handler]
pub async fn handle_get_version() -> impl IntoResponse {
    let body = serde_json::json!({
        "version": {
            "number": ELASTICSEARCH_VERSION
        }
    });
    (StatusCode::OK, elasticsearch_headers(), axum::Json(body))
}

// Return fake response for Elasticsearch license request.
// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/get-license.html.
#[axum_macros::debug_handler]
pub async fn handle_get_license() -> impl IntoResponse {
    let body = serde_json::json!({
        "license": {
            "uid": "cbff45e7-c553-41f7-ae4f-9205eabd80xx",
            "type": "oss",
            "status": "active",
            "expiry_date_in_millis": 4891198687000 as i64,
        }
    });
    (StatusCode::OK, elasticsearch_headers(), axum::Json(body))
}

// Process _bulk API requests.
// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk-api-request.
#[axum_macros::debug_handler]
pub async fn handle_bulk_api(
    State(log_state): State<LogState>,
    Query(params): Query<LogIngesterQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TypedHeader(_content_type): TypedHeader<ContentType>,
    payload: String,
) -> impl IntoResponse {
    let start = Instant::now();
    debug!(
        "Received bulk request, params: {:?}, payload: {:?}",
        params, payload
    );

    let table = if let Some(table) = params.table {
        table
    } else {
        error!("table is required");
        return (
            StatusCode::BAD_REQUEST,
            elasticsearch_headers(),
            axum::Json(write_bulk_response(
                start.elapsed().as_millis() as i64,
                0,
                true,
            )),
        );
    };

    // If pipeline_name is not provided, use the internal pipeline.
    let pipeline = if let Some(pipeline) = params.pipeline_name {
        pipeline
    } else {
        GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME.to_string()
    };

    // Read the ndjson payload and convert it to a vector of Value.
    let values: Vec<Value> = Deserializer::from_str(&payload)
        .into_iter::<Value>()
        .filter_map(|result| result.ok())
        .collect();

    if values.is_empty() {
        warn!("Received invalid bulk request: {:?}", payload);
        return (
            StatusCode::BAD_REQUEST,
            elasticsearch_headers(),
            axum::Json(write_bulk_response(
                start.elapsed().as_millis() as i64,
                0,
                true,
            )),
        );
    }

    // Read the first object to get the command, it should be "create" or "index".
    let command = values[0].get("create").or_else(|| values[0].get("index"));
    if command.is_none() {
        error!(
            "Received invalid bulk request, expected 'create' or 'index' but got {:?}",
            values[0]
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            elasticsearch_headers(),
            axum::Json(write_bulk_response(
                start.elapsed().as_millis() as i64,
                0,
                true,
            )),
        );
    }

    // TODO(zyy17): Support to specify other field to get the log data.
    // collect `message` and parse to log_values.
    let log_values = {
        let mut log_values: Vec<Value> = Vec::new();
        for value in &values[1..] {
            let message = value.get("message");
            if let Some(Value::String(message)) = message {
                let value = match serde_json::from_str::<Value>(message) {
                    Ok(value) => value,
                    Err(_) => Value::String(message.to_string()),
                };
                log_values.push(value);
            }
        }
        log_values
    };

    debug!("Received log data: {:?}", log_values);

    // The schema is already set in the query_ctx in auth process.
    query_ctx.set_channel(Channel::Http);

    if let Err(e) = ingest_logs_inner(
        log_state.log_handler,
        pipeline,
        None,
        table,
        log_values,
        Arc::new(query_ctx),
    )
    .await
    {
        warn!("Failed to ingest logs: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            elasticsearch_headers(),
            axum::Json(write_bulk_response(
                start.elapsed().as_millis() as i64,
                0,
                true,
            )),
        );
    }

    (
        StatusCode::OK,
        elasticsearch_headers(),
        axum::Json(write_bulk_response(
            start.elapsed().as_millis() as i64,
            values.len() - 1,
            false,
        )),
    )
}

// It will generate the following response when write _bulk request to GreptimeDB successfully:
// {
//     "took": 1000,
//     "errors": false,
//     "items": [
//         { "create": { "status": 201 } },
//         { "create": { "status": 201 } },
//         ...
//     ]
// }
fn write_bulk_response(took_ms: i64, n: usize, is_error: bool) -> Value {
    let items: Vec<Value> = (0..n)
        .map(|_| {
            json!({
                "create": {
                    "status": StatusCode::CREATED.as_u16()
                }
            })
        })
        .collect();

    json!({
        "took": took_ms,
        "errors": is_error,
        "items": items
    })
}

pub fn elasticsearch_headers() -> HeaderMap {
    HeaderMap::from_iter([
        (
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        ),
        // Logstash needs this header to identify the product.
        (
            HeaderName::from_static("x-elastic-product"),
            HeaderValue::from_static("Elasticsearch"),
        ),
    ])
}
