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

use axum::Extension;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum_extra::TypedHeader;
use common_error::ext::ErrorExt;
use common_telemetry::{debug, error};
use headers::ContentType;
use once_cell::sync::Lazy;
use pipeline::util::to_pipeline_version;
use pipeline::{
    GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME, GreptimePipelineParams, PipelineDefinition,
};
use serde_json::{Deserializer, Value, json};
use session::context::{Channel, QueryContext};
use snafu::{ResultExt, ensure};
use table::requests::{
    SEMANTIC_SIGNAL_TYPE, SEMANTIC_SOURCE, SIGNAL_TYPE_LOG, SOURCE_ELASTICSEARCH,
};
use vrl::value::Value as VrlValue;

use crate::error::{
    InvalidElasticsearchInputSnafu, ParseJsonSnafu, Result as ServersResult,
    status_code_to_http_status,
};
use crate::http::HttpResponse;
use crate::http::event::{
    LogIngesterQueryParams, LogState, PipelineIngestRequest,
    extract_pipeline_params_map_from_headers, ingest_logs_inner,
};
use crate::http::extractor::string_value_from_header;
use crate::http::header::constants::{
    GREPTIME_PIPELINE_NAME_HEADER_NAME, GREPTIME_PIPELINE_VERSION_HEADER_NAME,
};
use crate::metrics::{
    METRIC_ELASTICSEARCH_LOGS_DOCS_COUNT, METRIC_ELASTICSEARCH_LOGS_INGESTION_ELAPSED,
};

// The headers for every response of Elasticsearch API.
static ELASTICSEARCH_HEADERS: Lazy<HeaderMap> = Lazy::new(|| {
    HeaderMap::from_iter([
        (
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        ),
        (
            HeaderName::from_static("x-elastic-product"),
            HeaderValue::from_static("Elasticsearch"),
        ),
    ])
});

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

/// Process `_bulk` API requests. Only support to create logs.
/// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk-api-request.
#[axum_macros::debug_handler]
pub async fn handle_bulk_api(
    State(log_state): State<LogState>,
    Query(params): Query<LogIngesterQueryParams>,
    Extension(query_ctx): Extension<QueryContext>,
    TypedHeader(_content_type): TypedHeader<ContentType>,
    headers: HeaderMap,
    payload: String,
) -> impl IntoResponse {
    do_handle_bulk_api(log_state, None, params, query_ctx, headers, payload).await
}

/// Process `/${index}/_bulk` API requests. Only support to create logs.
/// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk-api-request.
#[axum_macros::debug_handler]
pub async fn handle_bulk_api_with_index(
    State(log_state): State<LogState>,
    Path(index): Path<String>,
    Query(params): Query<LogIngesterQueryParams>,
    Extension(query_ctx): Extension<QueryContext>,
    TypedHeader(_content_type): TypedHeader<ContentType>,
    headers: HeaderMap,
    payload: String,
) -> impl IntoResponse {
    do_handle_bulk_api(log_state, Some(index), params, query_ctx, headers, payload).await
}

async fn do_handle_bulk_api(
    log_state: LogState,
    index: Option<String>,
    params: LogIngesterQueryParams,
    mut query_ctx: QueryContext,
    headers: HeaderMap,
    payload: String,
) -> impl IntoResponse {
    let start = Instant::now();
    debug!(
        "Received bulk request, params: {:?}, payload: {:?}",
        params, payload
    );

    // The `schema` is already set in the query_ctx in auth process.
    query_ctx.set_channel(Channel::Elasticsearch);
    query_ctx.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_LOG);
    query_ctx.set_extension(SEMANTIC_SOURCE, SOURCE_ELASTICSEARCH);

    let db = query_ctx.current_schema();

    // Record the ingestion time histogram.
    let _timer = METRIC_ELASTICSEARCH_LOGS_INGESTION_ELAPSED
        .with_label_values(&[&db])
        .start_timer();

    // If pipeline_name is not provided, use the internal pipeline.
    let pipeline_name = params.pipeline_name.as_deref().unwrap_or_else(|| {
        headers
            .get(GREPTIME_PIPELINE_NAME_HEADER_NAME)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME)
    });

    // The pipeline version can be selected with the `version` query parameter or the
    // `x-greptime-pipeline-version` header; the query parameter takes precedence, consistent
    // with how `pipeline_name` is resolved above. Without either, the latest version is used.
    // Read the header through the shared extractor so a present but non-UTF-8 value is
    // rejected with 400 instead of silently falling back to the latest pipeline.
    let header_version = if params.version.is_none() {
        match string_value_from_header(&headers, &[GREPTIME_PIPELINE_VERSION_HEADER_NAME]) {
            Ok(version) => version,
            Err((status, reason)) => {
                return (
                    status,
                    elasticsearch_headers(),
                    axum::Json(write_bulk_response(
                        start.elapsed().as_millis() as i64,
                        0,
                        status.as_u16() as u32,
                        reason.as_str(),
                    )),
                );
            }
        }
    } else {
        None
    };
    let pipeline_version = params.version.as_deref().or(header_version.as_deref());

    // Read the ndjson payload and convert it to a vector of Value.
    let requests = match parse_bulk_request(&payload, &index, &params.msg_field) {
        Ok(requests) => requests,
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
    let log_num = requests.len();

    let pipeline_result = to_pipeline_version(pipeline_version)
        .and_then(|version| PipelineDefinition::from_name(pipeline_name, version, None));
    let pipeline = match pipeline_result {
        Ok(pipeline) => pipeline,
        Err(e) => {
            error!(e; "Failed to ingest logs");
            // Elasticsearch clients inspect `items[].create.status`, so report the
            // HTTP status rather than GreptimeDB's internal status code.
            let http_status = status_code_to_http_status(&e.status_code());
            return (
                http_status,
                elasticsearch_headers(),
                axum::Json(write_bulk_response(
                    start.elapsed().as_millis() as i64,
                    0,
                    http_status.as_u16() as u32,
                    e.to_string().as_str(),
                )),
            );
        }
    };
    let pipeline_params =
        GreptimePipelineParams::from_map(extract_pipeline_params_map_from_headers(&headers));
    let ingest_result = ingest_logs_inner(
        log_state.log_handler,
        pipeline,
        requests,
        Arc::new(query_ctx),
        pipeline_params,
    )
    .await;
    let failure = match ingest_result {
        Ok(response) => ingest_failure(&response),
        Err(e) => {
            error!(e; "Failed to ingest logs");
            Some((status_code_to_http_status(&e.status_code()), e.to_string()))
        }
    };
    if let Some((http_status, reason)) = failure {
        return (
            http_status,
            elasticsearch_headers(),
            axum::Json(write_bulk_response(
                start.elapsed().as_millis() as i64,
                0,
                http_status.as_u16() as u32,
                reason.as_str(),
            )),
        );
    }

    // Record the number of documents ingested.
    METRIC_ELASTICSEARCH_LOGS_DOCS_COUNT
        .with_label_values(&[&db])
        .inc_by(log_num as u64);

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

/// Returns the HTTP status and reason if the ingestion response reports a failure.
///
/// `ingest_logs_inner` returns `Ok` even when inserting the transformed rows fails,
/// carrying the insert error in an [`HttpResponse::Error`]. Without this check those
/// documents would be acknowledged with `201` although nothing was written.
fn ingest_failure(response: &HttpResponse) -> Option<(StatusCode, String)> {
    match response {
        HttpResponse::Error(err) => {
            let status = common_error::status_code::StatusCode::from_u32(err.code())
                .unwrap_or(common_error::status_code::StatusCode::Unknown);
            Some((status_code_to_http_status(&status), err.error().to_string()))
        }
        _ => None,
    }
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
        let mut response = json!({
            "took": took_ms,
            "errors": false,
        });
        response["items"] = Value::Array(items);
        response
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
    ELASTICSEARCH_HEADERS.clone()
}

// Parse the Elasticsearch bulk request and convert it to multiple LogIngestRequests.
// The input will be Elasticsearch bulk request in NDJSON format.
// For example, the input will be like this:
// { "index" : { "_index" : "test", "_id" : "1" } }
// { "field1" : "value1" }
// { "index" : { "_index" : "test", "_id" : "2" } }
// { "field2" : "value2" }
fn parse_bulk_request(
    input: &str,
    index_from_url: &Option<String>,
    msg_field: &Option<String>,
) -> ServersResult<Vec<PipelineIngestRequest>> {
    // Read the ndjson payload and convert it to `Vec<Value>`. Return error if the input is not a valid JSON.
    let values: Vec<VrlValue> = Deserializer::from_str(input)
        .into_iter::<VrlValue>()
        .collect::<Result<_, _>>()
        .context(ParseJsonSnafu)?;

    // Check if the input is empty.
    ensure!(
        !values.is_empty(),
        InvalidElasticsearchInputSnafu {
            reason: "empty bulk request".to_string(),
        }
    );

    let mut requests: Vec<PipelineIngestRequest> = Vec::with_capacity(values.len() / 2);
    let mut values = values.into_iter();

    // Read the ndjson payload and convert it to a (index, value) vector.
    // For Elasticsearch post `_bulk` API, each chunk contains two objects:
    //   1. The first object is the command, it should be `create` or `index`.
    //   2. The second object is the document data.
    while let Some(cmd) = values.next() {
        // NOTE: Although the native Elasticsearch API supports upsert in `index` command, we don't support change any data in `index` command and it's same as `create` command.
        let mut cmd = cmd.into_object();
        let index = if let Some(cmd) = cmd.as_mut().and_then(|c| c.remove("create")) {
            get_index_from_cmd(cmd)?
        } else if let Some(cmd) = cmd.as_mut().and_then(|c| c.remove("index")) {
            get_index_from_cmd(cmd)?
        } else {
            return InvalidElasticsearchInputSnafu {
                reason: format!(
                    "invalid bulk request, expected 'create' or 'index' but got {:?}",
                    cmd
                ),
            }
            .fail();
        };

        // Read the second object to get the document data. Stop the loop if there is no document.
        if let Some(document) = values.next() {
            // If the msg_field is provided, fetch the value of the field from the document data.
            let log_value = if let Some(msg_field) = msg_field {
                get_log_value_from_msg_field(document, msg_field)
            } else {
                document
            };

            ensure!(
                index.is_some() || index_from_url.is_some(),
                InvalidElasticsearchInputSnafu {
                    reason: "missing index in bulk request".to_string(),
                }
            );

            requests.push(PipelineIngestRequest {
                table: index.unwrap_or_else(|| index_from_url.as_ref().unwrap().clone()),
                values: vec![log_value],
            });
        }
    }

    debug!(
        "Received {} log ingest requests: {:?}",
        requests.len(),
        requests
    );

    Ok(requests)
}

// Get the index from the command. We will take index as the table name in GreptimeDB.
fn get_index_from_cmd(v: VrlValue) -> ServersResult<Option<String>> {
    let Some(index) = v.into_object().and_then(|mut m| m.remove("_index")) else {
        return Ok(None);
    };

    if let VrlValue::Bytes(index) = index {
        Ok(Some(String::from_utf8_lossy(&index).to_string()))
    } else {
        // If the `_index` exists, it should be a string.
        InvalidElasticsearchInputSnafu {
            reason: "index is not a string in bulk request",
        }
        .fail()
    }
}

// If the msg_field is provided, fetch the value of the field from the document data.
// For example, if the `msg_field` is `message`, and the document data is `{"message":"hello"}`, the log value will be Value::String("hello").
fn get_log_value_from_msg_field(v: VrlValue, msg_field: &str) -> VrlValue {
    let VrlValue::Object(mut m) = v else {
        return v;
    };

    if let Some(message) = m.remove(msg_field) {
        match message {
            VrlValue::Bytes(bytes) => {
                match serde_json::from_slice::<VrlValue>(&bytes) {
                    Ok(v) => v,
                    // If the message is not a valid JSON, return a map with the original message key and value.
                    Err(_) => {
                        let map = BTreeMap::from([(
                            msg_field.to_string().into(),
                            VrlValue::Bytes(bytes),
                        )]);
                        VrlValue::Object(map)
                    }
                }
            }
            // If the message is not a string, just use the original message as the log value.
            _ => message,
        }
    } else {
        // If the msg_field is not found, just use the original message as the log value.
        VrlValue::Object(m)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::RowInsertRequests;
    use async_trait::async_trait;
    use common_query::Output;
    use datatypes::timestamp::TimestampNanosecond;
    use pipeline::{Pipeline, PipelineInfo, PipelineOptions, PipelineVersion};
    use session::context::QueryContextRef;
    use snafu::Location;

    use super::*;
    use crate::error::Error as ServersError;
    use crate::http::result::error_result::ErrorResponse;
    use crate::http::result::greptime_result_v1::GreptimedbV1Response;
    use crate::query_handler::{PipelineHandler, PipelineHandlerRef};

    /// A pipeline handler whose inserts always report the given outcome.
    struct MockPipelineHandler {
        options: PipelineOptions,
        /// `None` reports a successful insert of every batch; `Some(reason)`
        /// reports the failure of every batch with that reason.
        insert_failure: Option<String>,
    }

    impl MockPipelineHandler {
        fn new(insert_failure: Option<&str>) -> Self {
            Self {
                options: PipelineOptions::default(),
                insert_failure: insert_failure.map(|reason| reason.to_string()),
            }
        }
    }

    #[async_trait]
    impl PipelineHandler for MockPipelineHandler {
        async fn insert(
            &self,
            _input: RowInsertRequests,
            _ctx: QueryContextRef,
        ) -> ServersResult<Output> {
            unimplemented!()
        }

        async fn insert_all(
            &self,
            inputs: Vec<(QueryContextRef, RowInsertRequests)>,
        ) -> ServersResult<Vec<ServersResult<Output>>> {
            let outputs = inputs
                .iter()
                .map(|_| match &self.insert_failure {
                    // The insert of a batch failed, e.g. because a value does not
                    // match the type of an existing column (case 1 in #10471).
                    Some(reason) => Err(ServersError::InvalidParameter {
                        reason: reason.clone(),
                        location: Location::default(),
                    }),
                    None => Ok(Output::new_with_affected_rows(1)),
                })
                .collect();
            Ok(outputs)
        }

        fn check_pipeline_query_permission(
            &self,
            _query_ctx: &QueryContextRef,
        ) -> ServersResult<()> {
            Ok(())
        }

        async fn get_pipeline(
            &self,
            _name: &str,
            _version: PipelineVersion,
            _query_ctx: QueryContextRef,
        ) -> ServersResult<Arc<Pipeline>> {
            unimplemented!()
        }

        async fn insert_pipeline(
            &self,
            _name: &str,
            _content_type: &str,
            _pipeline: &str,
            _query_ctx: QueryContextRef,
        ) -> ServersResult<PipelineInfo> {
            unimplemented!()
        }

        async fn delete_pipeline(
            &self,
            _name: &str,
            _version: PipelineVersion,
            _query_ctx: QueryContextRef,
        ) -> ServersResult<Option<()>> {
            unimplemented!()
        }

        async fn get_table(
            &self,
            _table: &str,
            _query_ctx: &QueryContext,
        ) -> std::result::Result<Option<Arc<table::Table>>, catalog::error::Error> {
            // No existing table, so the identity pipeline infers the schema from
            // the documents themselves.
            Ok(None)
        }

        fn pipeline_options(&self) -> &PipelineOptions {
            &self.options
        }

        fn build_pipeline(&self, _pipeline: &str) -> ServersResult<Pipeline> {
            unimplemented!()
        }

        async fn get_pipeline_str(
            &self,
            _name: &str,
            _version: PipelineVersion,
            _query_ctx: QueryContextRef,
        ) -> ServersResult<(String, TimestampNanosecond)> {
            unimplemented!()
        }
    }

    async fn bulk_request(log_handler: PipelineHandlerRef, payload: &str) -> (StatusCode, Value) {
        let log_state = LogState {
            log_handler,
            log_validator: None,
            ingest_interceptor: None,
        };
        let response = do_handle_bulk_api(
            log_state,
            None,
            LogIngesterQueryParams::default(),
            QueryContext::with("greptime", "public"),
            HeaderMap::new(),
            payload.to_string(),
        )
        .await
        .into_response();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("Failed to read the bulk response body");
        let body = serde_json::from_slice(&body).expect("Failed to parse the bulk response body");
        (status, body)
    }

    /// A failed insert must not be acknowledged with `201` for every document:
    /// Elasticsearch clients read `errors` and `items[].create.status` and would
    /// treat the dropped documents as written. See #10471.
    #[tokio::test]
    async fn test_bulk_insert_failure_is_reported() {
        let payload = "{\"create\":{\"_index\":\"test\",\"_id\":\"1\"}}\n{\"age\":\"adult\"}";
        let (status, body) = bulk_request(
            Arc::new(MockPipelineHandler::new(Some(
                "column age expect type Int64, given: STRING",
            ))),
            payload,
        )
        .await;

        assert_eq!(StatusCode::BAD_REQUEST, status);
        assert_eq!(Some(true), body["errors"].as_bool());
        let item = &body["items"][0]["create"];
        assert_eq!(Some(400), item["status"].as_u64());
        let reason = item["error"]["reason"].as_str().unwrap();
        assert!(
            reason.contains("column age expect type Int64, given: STRING"),
            "unexpected reason: {reason}"
        );
    }

    /// The successful path is unchanged: one `201` item per document.
    #[tokio::test]
    async fn test_bulk_insert_success_is_unchanged() {
        let payload = "{\"create\":{\"_index\":\"test\",\"_id\":\"1\"}}\n{\"age\":30}\n{\"create\":{\"_index\":\"test\",\"_id\":\"2\"}}\n{\"age\":31}";
        let (status, body) = bulk_request(Arc::new(MockPipelineHandler::new(None)), payload).await;

        assert_eq!(StatusCode::OK, status);
        assert_eq!(Some(false), body["errors"].as_bool());
        let items = body["items"].as_array().unwrap();
        assert_eq!(2, items.len());
        for item in items {
            assert_eq!(Some(201), item["create"]["status"].as_u64());
        }
    }

    #[test]
    fn test_ingest_failure() {
        // A failed insert is returned as an `Ok` error response and must be reported.
        let response = HttpResponse::Error(ErrorResponse::from_error_message(
            common_error::status_code::StatusCode::InvalidArguments,
            "column age expect type Int64, given: STRING".to_string(),
        ));
        let (status, reason) = ingest_failure(&response).unwrap();
        assert_eq!(StatusCode::BAD_REQUEST, status);
        assert_eq!("column age expect type Int64, given: STRING", reason);

        let response = HttpResponse::Error(ErrorResponse::from_error_message(
            common_error::status_code::StatusCode::Internal,
            "region unavailable".to_string(),
        ));
        let (status, _) = ingest_failure(&response).unwrap();
        assert_eq!(StatusCode::INTERNAL_SERVER_ERROR, status);

        let response = HttpResponse::GreptimedbV1(GreptimedbV1Response {
            output: vec![],
            execution_time_ms: 0,
            resp_metrics: Default::default(),
        });
        assert!(ingest_failure(&response).is_none());
    }

    #[test]
    fn test_parse_bulk_request() {
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
                None,
                Ok(vec![
                    PipelineIngestRequest {
                        table: "test".to_string(),
                        values: vec![
                            json!({"foo1": "foo1_value", "bar1": "bar1_value"}).into(),
                        ],
                    },
                    PipelineIngestRequest {
                        table: "test".to_string(),
                        values: vec![
                            json!({"foo2": "foo2_value", "bar2": "bar2_value"}).into(),
                        ],
                    },
                ]),
            ),
            // Case with index.
            (
                r#"
                {"create":{"_index":"test","_id":"1"}}
                {"foo1":"foo1_value", "bar1":"bar1_value"}
                {"create":{"_index":"logs","_id":"2"}}
                {"foo2":"foo2_value","bar2":"bar2_value"}
                "#,
                Some("logs".to_string()),
                None,
                Ok(vec![
                    PipelineIngestRequest {
                        table: "test".to_string(),
                        values: vec![
                            json!({"foo1": "foo1_value", "bar1": "bar1_value"}).into(),
                        ],
                    },
                    PipelineIngestRequest {
                        table: "logs".to_string(),
                        values: vec![
                            json!({"foo2": "foo2_value", "bar2": "bar2_value"}).into(),
                        ],
                    },
                ]),
            ),
            // Case with index.
            (
                r#"
                {"create":{"_index":"test","_id":"1"}}
                {"foo1":"foo1_value", "bar1":"bar1_value"}
                {"create":{"_index":"logs","_id":"2"}}
                {"foo2":"foo2_value","bar2":"bar2_value"}
                "#,
                Some("logs".to_string()),
                None,
                Ok(vec![
                    PipelineIngestRequest {
                        table: "test".to_string(),
                        values: vec![
                            json!({"foo1": "foo1_value", "bar1": "bar1_value"}).into(),
                        ],
                    },
                    PipelineIngestRequest {
                        table: "logs".to_string(),
                        values: vec![
                            json!({"foo2": "foo2_value", "bar2": "bar2_value"}).into(),
                        ],
                    },
                ]),
            ),
            // Case with incomplete bulk request.
            (
                r#"
                {"create":{"_index":"test","_id":"1"}}
                {"foo1":"foo1_value", "bar1":"bar1_value"}
                {"create":{"_index":"logs","_id":"2"}}
                "#,
                Some("logs".to_string()),
                None,
                Ok(vec![
                    PipelineIngestRequest {
                        table: "test".to_string(),
                        values: vec![
                            json!({"foo1": "foo1_value", "bar1": "bar1_value"}).into(),
                        ],
                    },
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
                None,
                Some("data".to_string()),
                Ok(vec![
                    PipelineIngestRequest {
                        table: "test".to_string(),
                        values: vec![
                            json!({"foo1": "foo1_value", "bar1": "bar1_value"}).into(),
                        ],
                    },
                    PipelineIngestRequest {
                        table: "test".to_string(),
                        values: vec![
                            json!({"foo2": "foo2_value", "bar2": "bar2_value"}).into(),
                        ],
                    },
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
                None,
                Some("message".to_string()),
                Ok(vec![
                    PipelineIngestRequest {
                        table: "logs-generic-default".to_string(),
                        values: vec![
                            json!({"message": "172.16.0.1 - - [25/May/2024:20:19:37 +0000] \"GET /contact HTTP/1.1\" 404 162 \"-\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1\""}).into(),
                        ],
                    },
                    PipelineIngestRequest {
                        table: "logs-generic-default".to_string(),
                        values: vec![
                            json!({"message": "10.0.0.1 - - [25/May/2024:20:18:37 +0000] \"GET /images/logo.png HTTP/1.1\" 304 0 \"-\" \"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0\""}).into(),
                        ],
                    },
                ]),
            ),
            // With invalid bulk request.
            (
                r#"
                { "not_create_or_index" : { "_index" : "test", "_id" : "1" } }
                { "foo1" : "foo1_value", "bar1" : "bar1_value" }
                "#,
                None,
                None,
                Err(InvalidElasticsearchInputSnafu {
                    reason: "it's a invalid bulk request".to_string(),
                }),
            ),
        ];

        for (input, index, msg_field, expected) in test_cases {
            let requests = parse_bulk_request(input, &index, &msg_field);
            if let Ok(expected) = expected {
                assert_eq!(requests.unwrap(), expected);
            } else {
                assert!(requests.is_err());
            }
        }
    }
}
