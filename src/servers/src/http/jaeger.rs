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
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode as HttpStatusCode;
use axum::response::IntoResponse;
use axum::Extension;
use common_catalog::consts::PARENT_SPAN_ID_COLUMN;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::{Output, OutputData};
use common_recordbatch::util;
use common_telemetry::{debug, error, tracing, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use session::context::{Channel, QueryContext};
use snafu::{OptionExt, ResultExt};

use crate::error::{
    status_code_to_http_status, CollectRecordbatchSnafu, Error, InvalidJaegerQuerySnafu, Result,
};
use crate::http::extractor::TraceTableName;
use crate::http::HttpRecordsOutput;
use crate::metrics::METRIC_JAEGER_QUERY_ELAPSED;
use crate::otlp::trace::{
    DURATION_NANO_COLUMN, KEY_OTEL_SCOPE_NAME, KEY_OTEL_SCOPE_VERSION, KEY_OTEL_STATUS_CODE,
    KEY_SERVICE_NAME, KEY_SPAN_KIND, RESOURCE_ATTRIBUTES_COLUMN, SCOPE_NAME_COLUMN,
    SCOPE_VERSION_COLUMN, SERVICE_NAME_COLUMN, SPAN_ATTRIBUTES_COLUMN, SPAN_EVENTS_COLUMN,
    SPAN_ID_COLUMN, SPAN_KIND_COLUMN, SPAN_KIND_PREFIX, SPAN_NAME_COLUMN, SPAN_STATUS_CODE,
    SPAN_STATUS_PREFIX, SPAN_STATUS_UNSET, TIMESTAMP_COLUMN, TRACE_ID_COLUMN, TRACE_TABLE_NAME,
};
use crate::query_handler::JaegerQueryHandlerRef;

pub const JAEGER_QUERY_TABLE_NAME_KEY: &str = "jaeger_query_table_name";

const REF_TYPE_CHILD_OF: &str = "CHILD_OF";
const SPAN_KIND_TIME_FMTS: [&str; 2] = ["%Y-%m-%d %H:%M:%S%.6f%z", "%Y-%m-%d %H:%M:%S%.9f%z"];

/// JaegerAPIResponse is the response of Jaeger HTTP API.
/// The original version is `structuredResponse` which is defined in https://github.com/jaegertracing/jaeger/blob/main/cmd/query/app/http_handler.go.
#[derive(Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct JaegerAPIResponse {
    pub data: Option<JaegerData>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
    pub errors: Vec<JaegerAPIError>,
}

/// JaegerData is the query result of Jaeger HTTP API.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum JaegerData {
    ServiceNames(Vec<String>),
    OperationsNames(Vec<String>),
    Operations(Vec<Operation>),
    Traces(Vec<Trace>),
}

/// JaegerAPIError is the error of Jaeger HTTP API.
#[derive(Default, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JaegerAPIError {
    pub code: i32,
    pub msg: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

/// Operation is an operation in a service.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_kind: Option<String>,
}

/// Trace is a collection of spans.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Trace {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    pub spans: Vec<Span>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub processes: HashMap<String, Process>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Span is a single operation within a trace.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    #[serde(rename = "traceID")]
    pub trace_id: String,

    #[serde(rename = "spanID")]
    pub span_id: String,

    #[serde(rename = "parentSpanID")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub parent_span_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub flags: Option<u32>,

    pub operation_name: String,
    pub references: Vec<Reference>,
    pub start_time: u64, // microseconds since unix epoch
    pub duration: u64,   // microseconds
    pub tags: Vec<KeyValue>,
    pub logs: Vec<Log>,

    #[serde(rename = "processID")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub process_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub process: Option<Process>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Reference is a reference from one span to another.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Reference {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    #[serde(rename = "spanID")]
    pub span_id: String,
    pub ref_type: String,
}

/// Process is the process emitting a set of spans.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Process {
    pub service_name: String,
    pub tags: Vec<KeyValue>,
}

/// Log is a log emitted in a span.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub timestamp: u64,
    pub fields: Vec<KeyValue>,
}

/// KeyValue is a key-value pair with typed value.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KeyValue {
    pub key: String,
    #[serde(rename = "type")]
    pub value_type: ValueType,
    pub value: Value,
}

/// Value is the value of a key-value pair in Jaeger Span attributes.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
#[serde(rename_all = "camelCase")]
pub enum Value {
    String(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Binary(Vec<u8>),
}

/// ValueType is the type of a value stored in KeyValue struct.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ValueType {
    String,
    Int64,
    Float64,
    Boolean,
    Binary,
}

/// JaegerQueryParams is the query parameters of Jaeger HTTP API.
#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JaegerQueryParams {
    /// Service name of the trace.
    #[serde(rename = "service")]
    pub service_name: Option<String>,

    /// Operation name of the trace.
    #[serde(rename = "operation")]
    pub operation_name: Option<String>,

    /// Limit the return data.
    pub limit: Option<usize>,

    /// Start time of the trace in microseconds since unix epoch.
    pub start: Option<i64>,

    /// End time of the trace in microseconds since unix epoch.
    pub end: Option<i64>,

    /// Max duration string value of the trace. Units can be `ns`, `us` (or `µs`), `ms`, `s`, `m`, `h`.
    pub max_duration: Option<String>,

    /// Min duration string value of the trace. Units can be `ns`, `us` (or `µs`), `ms`, `s`, `m`, `h`.
    pub min_duration: Option<String>,

    /// Tags of the trace in JSON format. It will be URL encoded in the raw query.
    /// The decoded format is like: tags="{\"http.status_code\":\"200\",\"latency\":\"11.234\",\"error\":\"false\",\"http.method\":\"GET\",\"http.path\":\"/api/v1/users\"}".
    /// The key and value of the map are both strings. The key and value is the attribute name and value of the span. The value will be converted to the corresponding type when querying.
    pub tags: Option<String>,

    /// The span kind of the trace.
    pub span_kind: Option<String>,
}

fn update_query_context(query_ctx: &mut QueryContext, table_name: Option<String>) {
    // db should be already handled by middlewares
    query_ctx.set_channel(Channel::Jaeger);
    if let Some(table) = table_name {
        query_ctx.set_extension(JAEGER_QUERY_TABLE_NAME_KEY, table);
    }
}

impl QueryTraceParams {
    fn from_jaeger_query_params(query_params: JaegerQueryParams) -> Result<Self> {
        let mut internal_query_params: QueryTraceParams = QueryTraceParams {
            service_name: query_params.service_name.context(InvalidJaegerQuerySnafu {
                reason: "service_name is required".to_string(),
            })?,
            operation_name: query_params.operation_name,
            // Convert start time from microseconds to nanoseconds.
            start_time: query_params.start.map(|start| start * 1000),
            end_time: query_params.end.map(|end| end * 1000),
            ..Default::default()
        };

        if let Some(max_duration) = query_params.max_duration {
            let duration = humantime::parse_duration(&max_duration).map_err(|e| {
                InvalidJaegerQuerySnafu {
                    reason: format!("parse maxDuration '{}' failed: {}", max_duration, e),
                }
                .build()
            })?;
            internal_query_params.max_duration = Some(duration.as_nanos() as u64);
        }

        if let Some(min_duration) = query_params.min_duration {
            let duration = humantime::parse_duration(&min_duration).map_err(|e| {
                InvalidJaegerQuerySnafu {
                    reason: format!("parse minDuration '{}' failed: {}", min_duration, e),
                }
                .build()
            })?;
            internal_query_params.min_duration = Some(duration.as_nanos() as u64);
        }

        if let Some(tags) = query_params.tags {
            // Serialize the tags to a JSON map.
            let mut tags_map: HashMap<String, JsonValue> =
                serde_json::from_str(&tags).map_err(|e| {
                    InvalidJaegerQuerySnafu {
                        reason: format!("parse tags '{}' failed: {}", tags, e),
                    }
                    .build()
                })?;
            for (_, v) in tags_map.iter_mut() {
                if let Some(number) = convert_string_to_number(v) {
                    *v = number;
                }
                if let Some(boolean) = convert_string_to_boolean(v) {
                    *v = boolean;
                }
            }
            internal_query_params.tags = Some(tags_map);
        }

        internal_query_params.limit = query_params.limit;

        Ok(internal_query_params)
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct QueryTraceParams {
    pub service_name: String,
    pub operation_name: Option<String>,

    // The limit of the number of traces to return.
    pub limit: Option<usize>,

    // Select the traces with the given tags(span attributes).
    pub tags: Option<HashMap<String, JsonValue>>,

    // The unit of the following time related parameters is nanoseconds.
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub min_duration: Option<u64>,
    pub max_duration: Option<u64>,
}

/// Handle the GET `/api/services` request.
#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "jaeger", request_type = "get_services"))]
pub async fn handle_get_services(
    State(handler): State<JaegerQueryHandlerRef>,
    Query(query_params): Query<JaegerQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TraceTableName(table_name): TraceTableName,
) -> impl IntoResponse {
    debug!(
        "Received Jaeger '/api/services' request, query_params: {:?}, query_ctx: {:?}",
        query_params, query_ctx
    );

    update_query_context(&mut query_ctx, table_name);
    let query_ctx = Arc::new(query_ctx);
    let db = query_ctx.get_db_string();

    // Record the query time histogram.
    let _timer = METRIC_JAEGER_QUERY_ELAPSED
        .with_label_values(&[&db, "/api/services"])
        .start_timer();

    match handler.get_services(query_ctx).await {
        Ok(output) => match covert_to_records(output).await {
            Ok(Some(records)) => match services_from_records(records) {
                Ok(services) => {
                    let services_num = services.len();
                    (
                        HttpStatusCode::OK,
                        axum::Json(JaegerAPIResponse {
                            data: Some(JaegerData::ServiceNames(services)),
                            total: services_num,
                            ..Default::default()
                        }),
                    )
                }
                Err(err) => {
                    error!("Failed to get services: {:?}", err);
                    error_response(err)
                }
            },
            Ok(None) => (HttpStatusCode::OK, axum::Json(JaegerAPIResponse::default())),
            Err(err) => {
                error!("Failed to get services: {:?}", err);
                error_response(err)
            }
        },
        Err(err) => handle_query_error(err, "Failed to get services", &db),
    }
}

/// Handle the GET `/api/traces/{trace_id}` request.
#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "jaeger", request_type = "get_trace"))]
pub async fn handle_get_trace(
    State(handler): State<JaegerQueryHandlerRef>,
    Path(trace_id): Path<String>,
    Query(query_params): Query<JaegerQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TraceTableName(table_name): TraceTableName,
) -> impl IntoResponse {
    debug!(
        "Received Jaeger '/api/traces/{}' request, query_params: {:?}, query_ctx: {:?}",
        trace_id, query_params, query_ctx
    );

    update_query_context(&mut query_ctx, table_name);
    let query_ctx = Arc::new(query_ctx);
    let db = query_ctx.get_db_string();

    // Record the query time histogram.
    let _timer = METRIC_JAEGER_QUERY_ELAPSED
        .with_label_values(&[&db, "/api/traces"])
        .start_timer();

    let output = match handler.get_trace(query_ctx, &trace_id).await {
        Ok(output) => output,
        Err(err) => {
            return handle_query_error(
                err,
                &format!("Failed to get trace for '{}'", trace_id),
                &db,
            );
        }
    };

    match covert_to_records(output).await {
        Ok(Some(records)) => match traces_from_records(records) {
            Ok(traces) => (
                HttpStatusCode::OK,
                axum::Json(JaegerAPIResponse {
                    data: Some(JaegerData::Traces(traces)),
                    ..Default::default()
                }),
            ),
            Err(err) => {
                error!("Failed to get trace '{}': {:?}", trace_id, err);
                error_response(err)
            }
        },
        Ok(None) => (HttpStatusCode::OK, axum::Json(JaegerAPIResponse::default())),
        Err(err) => {
            error!("Failed to get trace '{}': {:?}", trace_id, err);
            error_response(err)
        }
    }
}

/// Handle the GET `/api/traces` request.
#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "jaeger", request_type = "find_traces"))]
pub async fn handle_find_traces(
    State(handler): State<JaegerQueryHandlerRef>,
    Query(query_params): Query<JaegerQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TraceTableName(table_name): TraceTableName,
) -> impl IntoResponse {
    debug!(
        "Received Jaeger '/api/traces' request, query_params: {:?}, query_ctx: {:?}",
        query_params, query_ctx
    );

    update_query_context(&mut query_ctx, table_name);
    let query_ctx = Arc::new(query_ctx);
    let db = query_ctx.get_db_string();

    // Record the query time histogram.
    let _timer = METRIC_JAEGER_QUERY_ELAPSED
        .with_label_values(&[&db, "/api/traces"])
        .start_timer();

    match QueryTraceParams::from_jaeger_query_params(query_params) {
        Ok(query_params) => {
            let output = handler.find_traces(query_ctx, query_params).await;
            match output {
                Ok(output) => match covert_to_records(output).await {
                    Ok(Some(records)) => match traces_from_records(records) {
                        Ok(traces) => (
                            HttpStatusCode::OK,
                            axum::Json(JaegerAPIResponse {
                                data: Some(JaegerData::Traces(traces)),
                                ..Default::default()
                            }),
                        ),
                        Err(err) => {
                            error!("Failed to find traces: {:?}", err);
                            error_response(err)
                        }
                    },
                    Ok(None) => (HttpStatusCode::OK, axum::Json(JaegerAPIResponse::default())),
                    Err(err) => error_response(err),
                },
                Err(err) => handle_query_error(err, "Failed to find traces", &db),
            }
        }
        Err(e) => error_response(e),
    }
}

/// Handle the GET `/api/operations` request.
#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "jaeger", request_type = "get_operations"))]
pub async fn handle_get_operations(
    State(handler): State<JaegerQueryHandlerRef>,
    Query(query_params): Query<JaegerQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TraceTableName(table_name): TraceTableName,
) -> impl IntoResponse {
    debug!(
        "Received Jaeger '/api/operations' request, query_params: {:?}, query_ctx: {:?}",
        query_params, query_ctx
    );
    if let Some(service_name) = &query_params.service_name {
        update_query_context(&mut query_ctx, table_name);
        let query_ctx = Arc::new(query_ctx);
        let db = query_ctx.get_db_string();

        // Record the query time histogram.
        let _timer = METRIC_JAEGER_QUERY_ELAPSED
            .with_label_values(&[&db, "/api/operations"])
            .start_timer();

        match handler
            .get_operations(query_ctx, service_name, query_params.span_kind.as_deref())
            .await
        {
            Ok(output) => match covert_to_records(output).await {
                Ok(Some(records)) => match operations_from_records(records, true) {
                    Ok(operations) => {
                        let total = operations.len();
                        (
                            HttpStatusCode::OK,
                            axum::Json(JaegerAPIResponse {
                                data: Some(JaegerData::Operations(operations)),
                                total,
                                ..Default::default()
                            }),
                        )
                    }
                    Err(err) => {
                        error!("Failed to get operations: {:?}", err);
                        error_response(err)
                    }
                },
                Ok(None) => (HttpStatusCode::OK, axum::Json(JaegerAPIResponse::default())),
                Err(err) => error_response(err),
            },
            Err(err) => handle_query_error(
                err,
                &format!("Failed to get operations for service '{}'", service_name),
                &db,
            ),
        }
    } else {
        (
            HttpStatusCode::BAD_REQUEST,
            axum::Json(JaegerAPIResponse {
                errors: vec![JaegerAPIError {
                    code: 400,
                    msg: "parameter 'service' is required".to_string(),
                    trace_id: None,
                }],
                ..Default::default()
            }),
        )
    }
}

/// Handle the GET `/api/services/{service_name}/operations` request.
#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "jaeger", request_type = "get_operations_by_service")
)]
pub async fn handle_get_operations_by_service(
    State(handler): State<JaegerQueryHandlerRef>,
    Path(service_name): Path<String>,
    Query(query_params): Query<JaegerQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TraceTableName(table_name): TraceTableName,
) -> impl IntoResponse {
    debug!(
        "Received Jaeger '/api/services/{}/operations' request, query_params: {:?}, query_ctx: {:?}",
        service_name, query_params, query_ctx
    );

    update_query_context(&mut query_ctx, table_name);
    let query_ctx = Arc::new(query_ctx);
    let db = query_ctx.get_db_string();

    // Record the query time histogram.
    let _timer = METRIC_JAEGER_QUERY_ELAPSED
        .with_label_values(&[&db, "/api/services"])
        .start_timer();

    match handler.get_operations(query_ctx, &service_name, None).await {
        Ok(output) => match covert_to_records(output).await {
            Ok(Some(records)) => match operations_from_records(records, false) {
                Ok(operations) => {
                    let operations: Vec<String> =
                        operations.into_iter().map(|op| op.name).collect();
                    let total = operations.len();
                    (
                        HttpStatusCode::OK,
                        axum::Json(JaegerAPIResponse {
                            data: Some(JaegerData::OperationsNames(operations)),
                            total,
                            ..Default::default()
                        }),
                    )
                }
                Err(err) => {
                    error!(
                        "Failed to get operations for service '{}': {:?}",
                        service_name, err
                    );
                    error_response(err)
                }
            },
            Ok(None) => (HttpStatusCode::OK, axum::Json(JaegerAPIResponse::default())),
            Err(err) => error_response(err),
        },
        Err(err) => handle_query_error(
            err,
            &format!("Failed to get operations for service '{}'", service_name),
            &db,
        ),
    }
}

async fn covert_to_records(output: Output) -> Result<Option<HttpRecordsOutput>> {
    match output.data {
        OutputData::Stream(stream) => {
            let records = HttpRecordsOutput::try_new(
                stream.schema().clone(),
                util::collect(stream)
                    .await
                    .context(CollectRecordbatchSnafu)?,
            )?;
            debug!("The query records: {:?}", records);
            Ok(Some(records))
        }
        // It's unlikely to happen. However, if the output is not a stream, return None.
        _ => Ok(None),
    }
}

fn handle_query_error(
    err: Error,
    prompt: &str,
    db: &str,
) -> (HttpStatusCode, axum::Json<JaegerAPIResponse>) {
    // To compatible with the Jaeger API, if the trace table is not found, return an empty response instead of an error.
    if err.status_code() == StatusCode::TableNotFound {
        warn!(
            "No trace table '{}' found in database '{}'",
            TRACE_TABLE_NAME, db
        );
        (HttpStatusCode::OK, axum::Json(JaegerAPIResponse::default()))
    } else {
        error!("{}: {:?}", prompt, err);
        error_response(err)
    }
}

fn error_response(err: Error) -> (HttpStatusCode, axum::Json<JaegerAPIResponse>) {
    (
        status_code_to_http_status(&err.status_code()),
        axum::Json(JaegerAPIResponse {
            errors: vec![JaegerAPIError {
                code: err.status_code() as i32,
                msg: err.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
    )
}

fn traces_from_records(records: HttpRecordsOutput) -> Result<Vec<Trace>> {
    // maintain the mapping: trace_id -> (process_id -> service_name).
    let mut trace_id_to_processes: HashMap<String, HashMap<String, String>> = HashMap::new();
    // maintain the mapping: trace_id -> spans.
    let mut trace_id_to_spans: HashMap<String, Vec<Span>> = HashMap::new();
    // maintain the mapping: service.name -> resource.attributes.
    let mut service_to_resource_attributes: HashMap<String, Vec<KeyValue>> = HashMap::new();

    let is_span_attributes_flatten = !records
        .schema
        .column_schemas
        .iter()
        .any(|c| c.name == SPAN_ATTRIBUTES_COLUMN);

    for row in records.rows.into_iter() {
        let mut span = Span::default();
        let mut service_name = None;
        let mut resource_tags = vec![];

        for (idx, cell) in row.into_iter().enumerate() {
            // safe to use index here
            let column_name = &records.schema.column_schemas[idx].name;

            match column_name.as_str() {
                TRACE_ID_COLUMN => {
                    if let JsonValue::String(trace_id) = cell {
                        span.trace_id = trace_id.clone();
                        trace_id_to_processes.entry(trace_id).or_default();
                    }
                }
                TIMESTAMP_COLUMN => {
                    span.start_time = cell.as_u64().context(InvalidJaegerQuerySnafu {
                        reason: "Failed to convert timestamp to u64".to_string(),
                    })? / 1000;
                }
                DURATION_NANO_COLUMN => {
                    span.duration = cell.as_u64().context(InvalidJaegerQuerySnafu {
                        reason: "Failed to convert duration to u64".to_string(),
                    })? / 1000;
                }
                SERVICE_NAME_COLUMN => {
                    if let JsonValue::String(name) = cell {
                        service_name = Some(name);
                    }
                }
                SPAN_NAME_COLUMN => {
                    if let JsonValue::String(span_name) = cell {
                        span.operation_name = span_name;
                    }
                }
                SPAN_ID_COLUMN => {
                    if let JsonValue::String(span_id) = cell {
                        span.span_id = span_id;
                    }
                }
                SPAN_ATTRIBUTES_COLUMN => {
                    // for v0 data model, span_attributes are nested as a json
                    // data structure
                    if let JsonValue::Object(span_attrs) = cell {
                        span.tags.extend(object_to_tags(span_attrs));
                    }
                }
                RESOURCE_ATTRIBUTES_COLUMN => {
                    // for v0 data model, resource_attributes are nested as a json
                    // data structure

                    if let JsonValue::Object(mut resource_attrs) = cell {
                        resource_attrs.remove(KEY_SERVICE_NAME);
                        resource_tags = object_to_tags(resource_attrs);
                    }
                }
                PARENT_SPAN_ID_COLUMN => {
                    if let JsonValue::String(parent_span_id) = cell {
                        if !parent_span_id.is_empty() {
                            span.references.push(Reference {
                                trace_id: span.trace_id.clone(),
                                span_id: parent_span_id,
                                ref_type: REF_TYPE_CHILD_OF.to_string(),
                            });
                        }
                    }
                }
                SPAN_EVENTS_COLUMN => {
                    if let JsonValue::Array(events) = cell {
                        for event in events {
                            if let JsonValue::Object(mut obj) = event {
                                let Some(action) = obj.get("name").and_then(|v| v.as_str()) else {
                                    continue;
                                };

                                let Some(t) =
                                    obj.get("time").and_then(|t| t.as_str()).and_then(|s| {
                                        SPAN_KIND_TIME_FMTS
                                            .iter()
                                            .find_map(|fmt| {
                                                chrono::DateTime::parse_from_str(s, fmt).ok()
                                            })
                                            .map(|dt| dt.timestamp_micros() as u64)
                                    })
                                else {
                                    continue;
                                };

                                let mut fields = vec![KeyValue {
                                    key: "event".to_string(),
                                    value_type: ValueType::String,
                                    value: Value::String(action.to_string()),
                                }];

                                // Add event attributes as fields
                                if let Some(JsonValue::Object(attrs)) = obj.remove("attributes") {
                                    fields.extend(object_to_tags(attrs));
                                }

                                span.logs.push(Log {
                                    timestamp: t,
                                    fields,
                                });
                            }
                        }
                    }
                }
                SCOPE_NAME_COLUMN => {
                    if let JsonValue::String(scope_name) = cell {
                        if !scope_name.is_empty() {
                            span.tags.push(KeyValue {
                                key: KEY_OTEL_SCOPE_NAME.to_string(),
                                value_type: ValueType::String,
                                value: Value::String(scope_name),
                            });
                        }
                    }
                }
                SCOPE_VERSION_COLUMN => {
                    if let JsonValue::String(scope_version) = cell {
                        if !scope_version.is_empty() {
                            span.tags.push(KeyValue {
                                key: KEY_OTEL_SCOPE_VERSION.to_string(),
                                value_type: ValueType::String,
                                value: Value::String(scope_version),
                            });
                        }
                    }
                }
                SPAN_KIND_COLUMN => {
                    if let JsonValue::String(span_kind) = cell {
                        if !span_kind.is_empty() {
                            span.tags.push(KeyValue {
                                key: KEY_SPAN_KIND.to_string(),
                                value_type: ValueType::String,
                                value: Value::String(normalize_span_kind(&span_kind)),
                            });
                        }
                    }
                }
                SPAN_STATUS_CODE => {
                    if let JsonValue::String(span_status) = cell {
                        if span_status != SPAN_STATUS_UNSET && !span_status.is_empty() {
                            span.tags.push(KeyValue {
                                key: KEY_OTEL_STATUS_CODE.to_string(),
                                value_type: ValueType::String,
                                value: Value::String(normalize_status_code(&span_status)),
                            });
                        }
                    }
                }

                _ => {
                    // this this v1 data model
                    if is_span_attributes_flatten {
                        const SPAN_ATTR_PREFIX: &str = "span_attributes.";
                        const RESOURCE_ATTR_PREFIX: &str = "resource_attributes.";
                        // a span attributes column
                        if column_name.starts_with(SPAN_ATTR_PREFIX) {
                            if let Some(keyvalue) = to_keyvalue(
                                column_name
                                    .strip_prefix(SPAN_ATTR_PREFIX)
                                    .unwrap_or_default()
                                    .to_string(),
                                cell,
                            ) {
                                span.tags.push(keyvalue);
                            }
                        } else if column_name.starts_with(RESOURCE_ATTR_PREFIX) {
                            if let Some(keyvalue) = to_keyvalue(
                                column_name
                                    .strip_prefix(RESOURCE_ATTR_PREFIX)
                                    .unwrap_or_default()
                                    .to_string(),
                                cell,
                            ) {
                                resource_tags.push(keyvalue);
                            }
                        }
                    }
                }
            }
        }

        if let Some(service_name) = service_name {
            if !service_to_resource_attributes.contains_key(&service_name) {
                service_to_resource_attributes.insert(service_name.clone(), resource_tags);
            }

            if let Some(process) = trace_id_to_processes.get_mut(&span.trace_id) {
                if let Some(process_id) = process.get(&service_name) {
                    span.process_id = process_id.clone();
                } else {
                    // Allocate a new process id.
                    let process_id = format!("p{}", process.len() + 1);
                    process.insert(service_name, process_id.clone());
                    span.process_id = process_id;
                }
            }
        }

        // ensure span tags order
        span.tags.sort_by(|a, b| a.key.cmp(&b.key));

        if let Some(spans) = trace_id_to_spans.get_mut(&span.trace_id) {
            spans.push(span);
        } else {
            trace_id_to_spans.insert(span.trace_id.clone(), vec![span]);
        }
    }

    let mut traces = Vec::new();
    for (trace_id, spans) in trace_id_to_spans {
        let mut trace = Trace {
            trace_id,
            spans,
            ..Default::default()
        };

        if let Some(processes) = trace_id_to_processes.remove(&trace.trace_id) {
            let mut process_id_to_process = HashMap::new();
            for (service_name, process_id) in processes.into_iter() {
                let tags = service_to_resource_attributes
                    .remove(&service_name)
                    .unwrap_or_default();
                process_id_to_process.insert(process_id, Process { service_name, tags });
            }
            trace.processes = process_id_to_process;
        }
        traces.push(trace);
    }

    Ok(traces)
}

fn to_keyvalue(key: String, value: JsonValue) -> Option<KeyValue> {
    match value {
        JsonValue::String(value) => Some(KeyValue {
            key,
            value_type: ValueType::String,
            value: Value::String(value.to_string()),
        }),
        JsonValue::Number(value) => Some(KeyValue {
            key,
            value_type: ValueType::Int64,
            value: Value::Int64(value.as_i64().unwrap_or(0)),
        }),
        JsonValue::Bool(value) => Some(KeyValue {
            key,
            value_type: ValueType::Boolean,
            value: Value::Boolean(value),
        }),
        JsonValue::Array(value) => Some(KeyValue {
            key,
            value_type: ValueType::String,
            value: Value::String(serde_json::to_string(&value).unwrap()),
        }),
        JsonValue::Object(value) => Some(KeyValue {
            key,
            value_type: ValueType::String,
            value: Value::String(serde_json::to_string(&value).unwrap()),
        }),
        JsonValue::Null => None,
    }
}

fn object_to_tags(object: serde_json::map::Map<String, JsonValue>) -> Vec<KeyValue> {
    object
        .into_iter()
        .filter_map(|(key, value)| to_keyvalue(key, value))
        .collect()
}

fn services_from_records(records: HttpRecordsOutput) -> Result<Vec<String>> {
    let expected_schema = vec![(SERVICE_NAME_COLUMN, "String")];
    check_schema(&records, &expected_schema)?;

    let mut services = Vec::with_capacity(records.total_rows);
    for row in records.rows.into_iter() {
        for value in row.into_iter() {
            if let JsonValue::String(service_name) = value {
                services.push(service_name);
            }
        }
    }
    Ok(services)
}

// Construct Jaeger operations from records.
fn operations_from_records(
    records: HttpRecordsOutput,
    contain_span_kind: bool,
) -> Result<Vec<Operation>> {
    let expected_schema = vec![
        (SPAN_NAME_COLUMN, "String"),
        (SPAN_KIND_COLUMN, "String"),
        (SERVICE_NAME_COLUMN, "String"),
    ];
    check_schema(&records, &expected_schema)?;

    let mut operations = Vec::with_capacity(records.total_rows);
    for row in records.rows.into_iter() {
        let mut row_iter = row.into_iter();
        if let Some(JsonValue::String(operation)) = row_iter.next() {
            let mut operation = Operation {
                name: operation,
                span_kind: None,
            };
            if contain_span_kind {
                if let Some(JsonValue::String(span_kind)) = row_iter.next() {
                    operation.span_kind = Some(normalize_span_kind(&span_kind));
                }
            } else {
                // skip span kind.
                row_iter.next();
            }
            operations.push(operation);
        }
    }

    Ok(operations)
}

// Check whether the schema of the records is correct.
fn check_schema(records: &HttpRecordsOutput, expected_schema: &[(&str, &str)]) -> Result<()> {
    for (i, column) in records.schema.column_schemas.iter().enumerate() {
        if column.name != expected_schema[i].0 || column.data_type != expected_schema[i].1 {
            InvalidJaegerQuerySnafu {
                reason: "query result schema is not correct".to_string(),
            }
            .fail()?
        }
    }
    Ok(())
}

// By default, the span kind is stored as `SPAN_KIND_<kind>` in GreptimeDB.
// However, in Jaeger API, the span kind is returned as `<kind>` which is the lowercase of the span kind and without the `SPAN_KIND_` prefix.
fn normalize_span_kind(span_kind: &str) -> String {
    // If the span_kind starts with `SPAN_KIND_` prefix, remove it and convert to lowercase.
    if let Some(stripped) = span_kind.strip_prefix(SPAN_KIND_PREFIX) {
        stripped.to_lowercase()
    } else {
        // It's unlikely to happen. However, we still convert it to lowercase for consistency.
        span_kind.to_lowercase()
    }
}

// By default, the status code is stored as `STATUS_CODE_<code>` in GreptimeDB.
// However, in Jaeger API, the status code is returned as `<code>` without the `STATUS_CODE_` prefix.
fn normalize_status_code(status_code: &str) -> String {
    // If the span_kind starts with `SPAN_KIND_` prefix, remove it and convert to lowercase.
    if let Some(stripped) = status_code.strip_prefix(SPAN_STATUS_PREFIX) {
        stripped.to_string()
    } else {
        // It's unlikely to happen
        status_code.to_string()
    }
}

fn convert_string_to_number(input: &serde_json::Value) -> Option<serde_json::Value> {
    if let Some(data) = input.as_str() {
        if let Ok(number) = data.parse::<i64>() {
            return Some(serde_json::Value::Number(serde_json::Number::from(number)));
        }
        if let Ok(number) = data.parse::<f64>() {
            if let Some(number) = serde_json::Number::from_f64(number) {
                return Some(serde_json::Value::Number(number));
            }
        }
    }

    None
}

fn convert_string_to_boolean(input: &serde_json::Value) -> Option<serde_json::Value> {
    if let Some(data) = input.as_str() {
        if data == "true" {
            return Some(serde_json::Value::Bool(true));
        }
        if data == "false" {
            return Some(serde_json::Value::Bool(false));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Number, Value as JsonValue};

    use super::*;
    use crate::http::{ColumnSchema, HttpRecordsOutput, OutputSchema};

    #[test]
    fn test_services_from_records() {
        // The tests is the tuple of `(test_records, expected)`.
        let tests = vec![(
            HttpRecordsOutput {
                schema: OutputSchema {
                    column_schemas: vec![ColumnSchema {
                        name: "service_name".to_string(),
                        data_type: "String".to_string(),
                    }],
                },
                rows: vec![
                    vec![JsonValue::String("test-service-0".to_string())],
                    vec![JsonValue::String("test-service-1".to_string())],
                ],
                total_rows: 2,
                metrics: HashMap::new(),
            },
            vec!["test-service-0".to_string(), "test-service-1".to_string()],
        )];

        for (records, expected) in tests {
            let services = services_from_records(records).unwrap();
            assert_eq!(services, expected);
        }
    }

    #[test]
    fn test_operations_from_records() {
        // The tests is the tuple of `(test_records, contain_span_kind, expected)`.
        let tests = vec![
            (
                HttpRecordsOutput {
                    schema: OutputSchema {
                        column_schemas: vec![
                            ColumnSchema {
                                name: "span_name".to_string(),
                                data_type: "String".to_string(),
                            },
                            ColumnSchema {
                                name: "span_kind".to_string(),
                                data_type: "String".to_string(),
                            },
                        ],
                    },
                    rows: vec![
                        vec![
                            JsonValue::String("access-mysql".to_string()),
                            JsonValue::String("SPAN_KIND_SERVER".to_string()),
                        ],
                        vec![
                            JsonValue::String("access-redis".to_string()),
                            JsonValue::String("SPAN_KIND_CLIENT".to_string()),
                        ],
                    ],
                    total_rows: 2,
                    metrics: HashMap::new(),
                },
                false,
                vec![
                    Operation {
                        name: "access-mysql".to_string(),
                        span_kind: None,
                    },
                    Operation {
                        name: "access-redis".to_string(),
                        span_kind: None,
                    },
                ],
            ),
            (
                HttpRecordsOutput {
                    schema: OutputSchema {
                        column_schemas: vec![
                            ColumnSchema {
                                name: "span_name".to_string(),
                                data_type: "String".to_string(),
                            },
                            ColumnSchema {
                                name: "span_kind".to_string(),
                                data_type: "String".to_string(),
                            },
                        ],
                    },
                    rows: vec![
                        vec![
                            JsonValue::String("access-mysql".to_string()),
                            JsonValue::String("SPAN_KIND_SERVER".to_string()),
                        ],
                        vec![
                            JsonValue::String("access-redis".to_string()),
                            JsonValue::String("SPAN_KIND_CLIENT".to_string()),
                        ],
                    ],
                    total_rows: 2,
                    metrics: HashMap::new(),
                },
                true,
                vec![
                    Operation {
                        name: "access-mysql".to_string(),
                        span_kind: Some("server".to_string()),
                    },
                    Operation {
                        name: "access-redis".to_string(),
                        span_kind: Some("client".to_string()),
                    },
                ],
            ),
        ];

        for (records, contain_span_kind, expected) in tests {
            let operations = operations_from_records(records, contain_span_kind).unwrap();
            assert_eq!(operations, expected);
        }
    }

    #[test]
    fn test_traces_from_records() {
        // The tests is the tuple of `(test_records, expected)`.
        let tests = vec![(
            HttpRecordsOutput {
                schema: OutputSchema {
                    column_schemas: vec![
                        ColumnSchema {
                            name: "trace_id".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "timestamp".to_string(),
                            data_type: "TimestampNanosecond".to_string(),
                        },
                        ColumnSchema {
                            name: "duration_nano".to_string(),
                            data_type: "UInt64".to_string(),
                        },
                        ColumnSchema {
                            name: "service_name".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_name".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_id".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_attributes".to_string(),
                            data_type: "Json".to_string(),
                        },
                    ],
                },
                rows: vec![
                    vec![
                        JsonValue::String("5611dce1bc9ebed65352d99a027b08ea".to_string()),
                        JsonValue::Number(Number::from_u128(1738726754492422000).unwrap()),
                        JsonValue::Number(Number::from_u128(100000000).unwrap()),
                        JsonValue::String("test-service-0".to_string()),
                        JsonValue::String("access-mysql".to_string()),
                        JsonValue::String("008421dbbd33a3e9".to_string()),
                        JsonValue::Object(
                            json!({
                                "operation.type": "access-mysql",
                            })
                            .as_object()
                            .unwrap()
                            .clone(),
                        ),
                    ],
                    vec![
                        JsonValue::String("5611dce1bc9ebed65352d99a027b08ea".to_string()),
                        JsonValue::Number(Number::from_u128(1738726754642422000).unwrap()),
                        JsonValue::Number(Number::from_u128(100000000).unwrap()),
                        JsonValue::String("test-service-0".to_string()),
                        JsonValue::String("access-redis".to_string()),
                        JsonValue::String("ffa03416a7b9ea48".to_string()),
                        JsonValue::Object(
                            json!({
                                "operation.type": "access-redis",
                            })
                            .as_object()
                            .unwrap()
                            .clone(),
                        ),
                    ],
                ],
                total_rows: 2,
                metrics: HashMap::new(),
            },
            vec![Trace {
                trace_id: "5611dce1bc9ebed65352d99a027b08ea".to_string(),
                spans: vec![
                    Span {
                        trace_id: "5611dce1bc9ebed65352d99a027b08ea".to_string(),
                        span_id: "008421dbbd33a3e9".to_string(),
                        operation_name: "access-mysql".to_string(),
                        start_time: 1738726754492422,
                        duration: 100000,
                        tags: vec![KeyValue {
                            key: "operation.type".to_string(),
                            value_type: ValueType::String,
                            value: Value::String("access-mysql".to_string()),
                        }],
                        process_id: "p1".to_string(),
                        ..Default::default()
                    },
                    Span {
                        trace_id: "5611dce1bc9ebed65352d99a027b08ea".to_string(),
                        span_id: "ffa03416a7b9ea48".to_string(),
                        operation_name: "access-redis".to_string(),
                        start_time: 1738726754642422,
                        duration: 100000,
                        tags: vec![KeyValue {
                            key: "operation.type".to_string(),
                            value_type: ValueType::String,
                            value: Value::String("access-redis".to_string()),
                        }],
                        process_id: "p1".to_string(),
                        ..Default::default()
                    },
                ],
                processes: HashMap::from([(
                    "p1".to_string(),
                    Process {
                        service_name: "test-service-0".to_string(),
                        tags: vec![],
                    },
                )]),
                ..Default::default()
            }],
        )];

        for (records, expected) in tests {
            let traces = traces_from_records(records).unwrap();
            assert_eq!(traces, expected);
        }
    }

    #[test]
    fn test_traces_from_v1_records() {
        // The tests is the tuple of `(test_records, expected)`.
        let tests = vec![(
            HttpRecordsOutput {
                schema: OutputSchema {
                    column_schemas: vec![
                        ColumnSchema {
                            name: "trace_id".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "timestamp".to_string(),
                            data_type: "TimestampNanosecond".to_string(),
                        },
                        ColumnSchema {
                            name: "duration_nano".to_string(),
                            data_type: "UInt64".to_string(),
                        },
                        ColumnSchema {
                            name: "service_name".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_name".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_id".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_attributes.http.request.method".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_attributes.http.request.url".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_attributes.http.status_code".to_string(),
                            data_type: "UInt64".to_string(),
                        },
                    ],
                },
                rows: vec![
                    vec![
                        JsonValue::String("5611dce1bc9ebed65352d99a027b08ea".to_string()),
                        JsonValue::Number(Number::from_u128(1738726754492422000).unwrap()),
                        JsonValue::Number(Number::from_u128(100000000).unwrap()),
                        JsonValue::String("test-service-0".to_string()),
                        JsonValue::String("access-mysql".to_string()),
                        JsonValue::String("008421dbbd33a3e9".to_string()),
                        JsonValue::String("GET".to_string()),
                        JsonValue::String("/data".to_string()),
                        JsonValue::Number(Number::from_u128(200).unwrap()),
                    ],
                    vec![
                        JsonValue::String("5611dce1bc9ebed65352d99a027b08ea".to_string()),
                        JsonValue::Number(Number::from_u128(1738726754642422000).unwrap()),
                        JsonValue::Number(Number::from_u128(100000000).unwrap()),
                        JsonValue::String("test-service-0".to_string()),
                        JsonValue::String("access-redis".to_string()),
                        JsonValue::String("ffa03416a7b9ea48".to_string()),
                        JsonValue::String("POST".to_string()),
                        JsonValue::String("/create".to_string()),
                        JsonValue::Number(Number::from_u128(400).unwrap()),
                    ],
                ],
                total_rows: 2,
                metrics: HashMap::new(),
            },
            vec![Trace {
                trace_id: "5611dce1bc9ebed65352d99a027b08ea".to_string(),
                spans: vec![
                    Span {
                        trace_id: "5611dce1bc9ebed65352d99a027b08ea".to_string(),
                        span_id: "008421dbbd33a3e9".to_string(),
                        operation_name: "access-mysql".to_string(),
                        start_time: 1738726754492422,
                        duration: 100000,
                        tags: vec![
                            KeyValue {
                                key: "http.request.method".to_string(),
                                value_type: ValueType::String,
                                value: Value::String("GET".to_string()),
                            },
                            KeyValue {
                                key: "http.request.url".to_string(),
                                value_type: ValueType::String,
                                value: Value::String("/data".to_string()),
                            },
                            KeyValue {
                                key: "http.status_code".to_string(),
                                value_type: ValueType::Int64,
                                value: Value::Int64(200),
                            },
                        ],
                        process_id: "p1".to_string(),
                        ..Default::default()
                    },
                    Span {
                        trace_id: "5611dce1bc9ebed65352d99a027b08ea".to_string(),
                        span_id: "ffa03416a7b9ea48".to_string(),
                        operation_name: "access-redis".to_string(),
                        start_time: 1738726754642422,
                        duration: 100000,
                        tags: vec![
                            KeyValue {
                                key: "http.request.method".to_string(),
                                value_type: ValueType::String,
                                value: Value::String("POST".to_string()),
                            },
                            KeyValue {
                                key: "http.request.url".to_string(),
                                value_type: ValueType::String,
                                value: Value::String("/create".to_string()),
                            },
                            KeyValue {
                                key: "http.status_code".to_string(),
                                value_type: ValueType::Int64,
                                value: Value::Int64(400),
                            },
                        ],
                        process_id: "p1".to_string(),
                        ..Default::default()
                    },
                ],
                processes: HashMap::from([(
                    "p1".to_string(),
                    Process {
                        service_name: "test-service-0".to_string(),
                        tags: vec![],
                    },
                )]),
                ..Default::default()
            }],
        )];

        for (records, expected) in tests {
            let traces = traces_from_records(records).unwrap();
            assert_eq!(traces, expected);
        }
    }

    #[test]
    fn test_from_jaeger_query_params() {
        // The tests is the tuple of `(test_query_params, expected)`.
        let tests = vec![
            (
                JaegerQueryParams {
                    service_name: Some("test-service-0".to_string()),
                    ..Default::default()
                },
                QueryTraceParams {
                    service_name: "test-service-0".to_string(),
                    ..Default::default()
                },
            ),
            (
                JaegerQueryParams {
                    service_name: Some("test-service-0".to_string()),
                    operation_name: Some("access-mysql".to_string()),
                    start: Some(1738726754492422),
                    end: Some(1738726754642422),
                    max_duration: Some("100ms".to_string()),
                    min_duration: Some("50ms".to_string()),
                    limit: Some(10),
                    tags: Some("{\"http.status_code\":\"200\",\"latency\":\"11.234\",\"error\":\"false\",\"http.method\":\"GET\",\"http.path\":\"/api/v1/users\"}".to_string()),
                    ..Default::default()
                },
                QueryTraceParams {
                    service_name: "test-service-0".to_string(),
                    operation_name: Some("access-mysql".to_string()),
                    start_time: Some(1738726754492422000),
                    end_time: Some(1738726754642422000),
                    min_duration: Some(50000000),
                    max_duration: Some(100000000),
                    limit: Some(10),
                    tags: Some(HashMap::from([
                        ("http.status_code".to_string(), JsonValue::Number(Number::from(200))),
                        ("latency".to_string(), JsonValue::Number(Number::from_f64(11.234).unwrap())),
                        ("error".to_string(), JsonValue::Bool(false)),
                        ("http.method".to_string(), JsonValue::String("GET".to_string())),
                        ("http.path".to_string(), JsonValue::String("/api/v1/users".to_string())),
                    ])),
                },
            ),
        ];

        for (query_params, expected) in tests {
            let query_params = QueryTraceParams::from_jaeger_query_params(query_params).unwrap();
            assert_eq!(query_params, expected);
        }
    }

    #[test]
    fn test_check_schema() {
        // The tests is the tuple of `(test_records, expected_schema, is_ok)`.
        let tests = vec![(
            HttpRecordsOutput {
                schema: OutputSchema {
                    column_schemas: vec![
                        ColumnSchema {
                            name: "trace_id".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "timestamp".to_string(),
                            data_type: "TimestampNanosecond".to_string(),
                        },
                        ColumnSchema {
                            name: "duration_nano".to_string(),
                            data_type: "UInt64".to_string(),
                        },
                        ColumnSchema {
                            name: "service_name".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_name".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_id".to_string(),
                            data_type: "String".to_string(),
                        },
                        ColumnSchema {
                            name: "span_attributes".to_string(),
                            data_type: "Json".to_string(),
                        },
                    ],
                },
                rows: vec![],
                total_rows: 0,
                metrics: HashMap::new(),
            },
            vec![
                (TRACE_ID_COLUMN, "String"),
                (TIMESTAMP_COLUMN, "TimestampNanosecond"),
                (DURATION_NANO_COLUMN, "UInt64"),
                (SERVICE_NAME_COLUMN, "String"),
                (SPAN_NAME_COLUMN, "String"),
                (SPAN_ID_COLUMN, "String"),
                (SPAN_ATTRIBUTES_COLUMN, "Json"),
            ],
            true,
        )];

        for (records, expected_schema, is_ok) in tests {
            let result = check_schema(&records, &expected_schema);
            assert_eq!(result.is_ok(), is_ok);
        }
    }

    #[test]
    fn test_normalize_span_kind() {
        let tests = vec![
            ("SPAN_KIND_SERVER".to_string(), "server".to_string()),
            ("SPAN_KIND_CLIENT".to_string(), "client".to_string()),
        ];

        for (input, expected) in tests {
            let result = normalize_span_kind(&input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_convert_string_to_number() {
        let tests = vec![
            (
                JsonValue::String("123".to_string()),
                Some(JsonValue::Number(Number::from(123))),
            ),
            (
                JsonValue::String("123.456".to_string()),
                Some(JsonValue::Number(Number::from_f64(123.456).unwrap())),
            ),
        ];

        for (input, expected) in tests {
            let result = convert_string_to_number(&input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_convert_string_to_boolean() {
        let tests = vec![
            (
                JsonValue::String("true".to_string()),
                Some(JsonValue::Bool(true)),
            ),
            (
                JsonValue::String("false".to_string()),
                Some(JsonValue::Bool(false)),
            ),
        ];

        for (input, expected) in tests {
            let result = convert_string_to_boolean(&input);
            assert_eq!(result, expected);
        }
    }
}
