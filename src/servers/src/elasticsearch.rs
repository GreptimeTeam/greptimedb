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
use common_error::ext::ErrorExt;
use common_telemetry::{debug, warn};
use serde_json::{json, Deserializer, Value};
use session::context::{Channel, QueryContext};
use snafu::{ensure, ResultExt};

use crate::error::{
    status_code_to_http_status, InvalidElasticsearchInputSnafu, ParseJsonSnafu,
    Result as ServersResult,
};
use crate::http::event::{
    ingest_logs_inner, LogIngesterQueryParams, LogState, GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME,
};

// The fake version of Elasticsearch and used for `_version` API.
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
            "expiry_date_in_millis": 4891198687000_i64,
        }
    });
    (StatusCode::OK, elasticsearch_headers(), axum::Json(body))
}

// Process `_bulk` API requests. Only support to create logs.
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

    // The `schema` is already set in the query_ctx in auth process.
    query_ctx.set_channel(Channel::Elasticsearch);

    // Record the ingestion time histogram.
    let _timer = crate::metrics::METRIC_ELASTICSEARCH_LOGS_INGESTION_ELAPSED
        .with_label_values(&[params.db.unwrap_or("public".to_string()).as_str()])
        .start_timer();

    let table = if let Some(table) = params.table {
        table
    } else {
        return (
            StatusCode::BAD_REQUEST,
            elasticsearch_headers(),
            axum::Json(write_bulk_response(
                start.elapsed().as_millis() as i64,
                0,
                StatusCode::BAD_REQUEST.as_u16() as u32,
                "require parameter 'table'",
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
    let log_values = match convert_es_input_to_log_values(&payload, &params.msg_field) {
        Ok(log_values) => log_values,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                elasticsearch_headers(),
                axum::Json(write_bulk_response(
                    start.elapsed().as_millis() as i64,
                    0,
                    StatusCode::BAD_REQUEST.as_u16() as u32,
                    e.to_string().as_str(),
                )),
            );
        }
    };
    let log_num = log_values.len();

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
            status_code_to_http_status(&e.status_code()),
            elasticsearch_headers(),
            axum::Json(write_bulk_response(
                start.elapsed().as_millis() as i64,
                0,
                e.status_code() as u32,
                e.to_string().as_str(),
            )),
        );
    }

    (
        StatusCode::OK,
        elasticsearch_headers(),
        axum::Json(write_bulk_response(
            start.elapsed().as_millis() as i64,
            log_num,
            StatusCode::CREATED.as_u16() as u32,
            "",
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
// If the status code is not 201, it will generate the following response:
// {
//     "took": 1000,
//     "errors": true,
//     "items": [
//         { "create": { "status": 400, "error": { "type": "illegal_argument_exception", "reason": "<error_reason>" } } }
//     ]
// }
fn write_bulk_response(took_ms: i64, n: usize, status_code: u32, error_reason: &str) -> Value {
    if error_reason.is_empty() {
        let items: Vec<Value> = (0..n)
            .map(|_| {
                json!({
                    "create": {
                        "status": status_code
                    }
                })
            })
            .collect();
        json!({
            "took": took_ms,
            "errors": false,
            "items": items,
        })
    } else {
        json!({
            "took": took_ms,
            "errors": true,
            "items": [
                { "create": { "status": status_code, "error": { "type": "illegal_argument_exception", "reason": error_reason } } }
            ]
        })
    }
}

/// Returns the headers for every response of Elasticsearch API.
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

// The input will be Elasticsearch bulk request in NDJSON format.
// For example, the input will be like this:
// { "index" : { "_index" : "test", "_id" : "1" } }
// { "field1" : "value1" }
// { "index" : { "_index" : "test", "_id" : "2" } }
// { "field2" : "value2" }
fn convert_es_input_to_log_values(
    input: &str,
    msg_field: &Option<String>,
) -> ServersResult<Vec<Value>> {
    // Read the ndjson payload and convert it to `Vec<Value>`. Return error if the input is not a valid JSON.
    let values: Vec<Value> = Deserializer::from_str(input)
        .into_iter::<Value>()
        .collect::<Result<_, _>>()
        .context(ParseJsonSnafu)?;

    // Check if the input is empty.
    ensure!(
        !values.is_empty(),
        InvalidElasticsearchInputSnafu {
            reason: "empty bulk request".to_string(),
        }
    );

    let mut log_values: Vec<Value> = Vec::new();

    // For Elasticsearch post `_bulk` API, each chunk contains two objects:
    // 1. The first object is the command, it should be `create` or `index`. `create` is used for insert, `index` is used for upsert.
    // 2. The second object is the document data.
    let mut is_document = false;
    for v in values {
        if !is_document {
            // Read the first object to get the command, it should be `create` or `index`.
            ensure!(
                v.get("create").is_some() || v.get("index").is_some(),
                InvalidElasticsearchInputSnafu {
                    reason: format!(
                        "invalid bulk request, expected 'create' or 'index' but got {:?}",
                        v
                    ),
                }
            );
            is_document = true;
            continue;
        }

        // It means the second object is the document data.
        if is_document {
            // If the msg_field is provided, fetch the value of the field from the document data.
            if let Some(msg_field) = msg_field {
                if let Some(Value::String(message)) = v.get(msg_field) {
                    let value = match serde_json::from_str::<Value>(message) {
                        Ok(value) => value,
                        // If the message is not a valid JSON, just use the original message as the log value.
                        Err(_) => Value::String(message.to_string()),
                    };
                    log_values.push(value);
                }
            } else {
                log_values.push(v);
            }
            is_document = false;
        }
    }

    debug!("Received log data: {:?}", log_values);

    Ok(log_values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_es_input_to_log_values() {
        let test_cases = vec![
            // Normal case.
            (
                r#"
                {"create":{"_index":"test","_id":"1"}}
                {"foo1":"foo1_value", "bar1":"bar1_value"}
                {"create":{"_index":"test","_id":"2"}}
                {"foo2":"foo2_value","bar2":"bar2_value"}
                "#,
                None,
                Ok(vec![
                    json!({"foo1": "foo1_value", "bar1": "bar1_value"}),
                    json!({"foo2": "foo2_value", "bar2": "bar2_value"}),
                ]),
            ),
            // Specify the `data` field as the message field and the value is a JSON string.
            (
                r#"
                {"create":{"_index":"test","_id":"1"}}
                {"data":"{\"foo1\":\"foo1_value\", \"bar1\":\"bar1_value\"}", "not_data":"not_data_value"}
                {"create":{"_index":"test","_id":"2"}}
                {"data":"{\"foo2\":\"foo2_value\", \"bar2\":\"bar2_value\"}", "not_data":"not_data_value"}
                "#,
                Some("data".to_string()),
                Ok(vec![
                    json!({"foo1": "foo1_value", "bar1": "bar1_value"}),
                    json!({"foo2": "foo2_value", "bar2": "bar2_value"}),
                ]),
            ),
            // Simulate the log data from Logstash.
            (
                r#"
                {"create":{"_id":null,"_index":"logs-generic-default","routing":null}}
                {"message":"172.16.0.1 - - [25/May/2024:20:19:37 +0000] \"GET /contact HTTP/1.1\" 404 162 \"-\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1\"","@timestamp":"2025-01-04T04:32:13.868962186Z","event":{"original":"172.16.0.1 - - [25/May/2024:20:19:37 +0000] \"GET /contact HTTP/1.1\" 404 162 \"-\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1\""},"host":{"name":"orbstack"},"log":{"file":{"path":"/var/log/nginx/access.log"}},"@version":"1","data_stream":{"type":"logs","dataset":"generic","namespace":"default"}}
                {"create":{"_id":null,"_index":"logs-generic-default","routing":null}}
                {"message":"10.0.0.1 - - [25/May/2024:20:18:37 +0000] \"GET /images/logo.png HTTP/1.1\" 304 0 \"-\" \"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0\"","@timestamp":"2025-01-04T04:32:13.868723810Z","event":{"original":"10.0.0.1 - - [25/May/2024:20:18:37 +0000] \"GET /images/logo.png HTTP/1.1\" 304 0 \"-\" \"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0\""},"host":{"name":"orbstack"},"log":{"file":{"path":"/var/log/nginx/access.log"}},"@version":"1","data_stream":{"type":"logs","dataset":"generic","namespace":"default"}}
                "#,
                Some("message".to_string()),
                Ok(vec![
                    json!("172.16.0.1 - - [25/May/2024:20:19:37 +0000] \"GET /contact HTTP/1.1\" 404 162 \"-\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1\""),
                    json!("10.0.0.1 - - [25/May/2024:20:18:37 +0000] \"GET /images/logo.png HTTP/1.1\" 304 0 \"-\" \"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0\""),
                ]),
            ),
            // With invalid bulk request.
            (
                r#"
                { "not_create_or_index" : { "_index" : "test", "_id" : "1" } }
                { "foo1" : "foo1_value", "bar1" : "bar1_value" }
                "#,
                None,
                Err(InvalidElasticsearchInputSnafu {
                    reason: "it's a invalid bulk request".to_string(),
                }),
            ),
        ];

        for (input, msg_field, expected) in test_cases {
            let log_values = convert_es_input_to_log_values(input, &msg_field);
            if expected.is_ok() {
                assert_eq!(log_values.unwrap(), expected.unwrap());
            } else {
                assert!(log_values.is_err());
            }
        }
    }
}
