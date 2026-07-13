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

//! Splunk HTTP Event Collector (HEC) compatible ingestion endpoint.
//!
//! Clients point their base endpoint at `/v1/splunk`, so the full paths are e.g.
//! `/v1/splunk/services/collector/event` and `/v1/splunk/services/collector/health`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use api::v1::SemanticType;
use axum::Extension;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use common_base::regex_pattern::NAME_PATTERN_REG;
use common_error::ext::ErrorExt;
use common_query::prelude::greptime_timestamp;
use common_telemetry::{debug, error};
use operator::insert::SPLUNK_PK_METADATA_ORDER_KEY;
use pipeline::util::to_pipeline_version;
use pipeline::{
    ContextReq, GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME, GreptimePipelineParams, PipelineContext,
    PipelineDefinition,
};
use serde_json::{Deserializer, json};
use session::context::{Channel, QueryContext, QueryContextRef};
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{Result, status_code_to_http_status};
use crate::http::HttpResponse;
use crate::http::event::{
    LogIngesterQueryParams, LogState, PipelineIngestRequest, execute_log_context_req,
    extract_pipeline_params_map_from_headers, transform_ndjson_array_factory,
};
use crate::http::header::constants::GREPTIME_PIPELINE_NAME_HEADER_NAME;
use crate::metrics::{METRIC_HTTP_LOGS_INGESTION_COUNTER, METRIC_HTTP_LOGS_INGESTION_ELAPSED};
use crate::pipeline::run_pipeline;
use crate::query_handler::PipelineHandlerRef;

/// Default table used when neither the event's `index` nor a `?table=` query
/// param is provided.
const DEFAULT_SPLUNK_TABLE: &str = "splunk_logs";
/// HEC response code for a healthy collector. Splunk returns
/// `{"text":"HEC is healthy","code":17}`.
const HEC_HEALTHY_CODE: u32 = 17;

/// Query parameters for `/services/collector/raw`.
/// `channel` is accepted but ignored until indexer acknowledgment lands. `table`,
/// `pipeline_name`, and `version` are Greptime extensions matching `/event`.
#[derive(Debug, Default, serde::Deserialize)]
pub struct SplunkRawQueryParams {
    pub channel: Option<String>,
    pub host: Option<String>,
    pub source: Option<String>,
    pub sourcetype: Option<String>,
    pub index: Option<String>,
    pub time: Option<String>,
    pub table: Option<String>,
    pub pipeline_name: Option<String>,
    pub version: Option<String>,
}

/// Splits a raw HEC body into events: one event per line (`\n`, tolerating `\r\n`),
/// skipping blank/whitespace-only lines.
fn parse_raw_lines(body: &str) -> Vec<&str> {
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .collect()
}

/// Column holding the verbatim raw line on `/raw` (Splunk's `_raw`). Named `message`
/// to avoid clashing with `/event`'s `event` column (whose shape
/// varies by client: string vs identity-flattened object).
const RAW_MESSAGE_COLUMN: &str = "message";

/// Collects request-level `/raw` metadata (`host`/`source`/`sourcetype`) present in
/// the query params. The keys double as the tag-column names; values apply to every
/// event in the request (HEC `/raw` metadata is request-level, unlike `/event`).
fn raw_metadata(params: &SplunkRawQueryParams) -> Vec<(&'static str, &str)> {
    [
        ("host", &params.host),
        ("source", &params.source),
        ("sourcetype", &params.sourcetype),
    ]
    .into_iter()
    .filter_map(|(key, value)| value.as_deref().map(|v| (key, v)))
    .collect()
}

/// Maps one raw line to a per-event map: `{ greptime_timestamp: ts, message: <line>,
/// <metadata columns> }`. The line is stored as it is.
fn raw_line_to_map(line: &str, ts: DateTime<Utc>, metadata: &[(&'static str, &str)]) -> VrlValue {
    let mut map: BTreeMap<KeyString, VrlValue> = BTreeMap::new();
    map.insert(
        KeyString::from(greptime_timestamp()),
        VrlValue::Timestamp(ts),
    );
    map.insert(
        KeyString::from(RAW_MESSAGE_COLUMN),
        VrlValue::Bytes(Bytes::from(line.to_string())),
    );
    for (key, value) in metadata {
        map.insert(
            KeyString::from(*key),
            VrlValue::Bytes(Bytes::from(value.to_string())),
        );
    }
    VrlValue::Object(map)
}

/// HEC response body `{"text", "code"}`; clients branch on `code`.
fn hec_response(status: StatusCode, code: u32, text: &str) -> axum::response::Response {
    (status, axum::Json(json!({ "text": text, "code": code }))).into_response()
}

/// Parses a HEC body into a flat list of events. Handles both batch forms: objects
/// concatenated with any/no separator, and a top-level array (flattened).
fn parse_hec_events(body: &[u8]) -> Result<Vec<VrlValue>> {
    let values = Deserializer::from_slice(body).into_iter::<VrlValue>();
    // ignore_error = false: reject the whole batch on a malformed value.
    transform_ndjson_array_factory(values, false)
}

/// HEC `time`: epoch seconds (optionally fractional); values past ~1e12 are read as
/// milliseconds. `None` if absent/unparseable (caller falls back to ingest time).
fn parse_hec_time(value: &VrlValue) -> Option<DateTime<Utc>> {
    let n: f64 = match value {
        VrlValue::Integer(i) => *i as f64,
        VrlValue::Float(f) => f.into_inner(),
        VrlValue::Bytes(b) => std::str::from_utf8(b).ok()?.trim().parse().ok()?,
        VrlValue::Timestamp(dt) => return Some(*dt),
        _ => return None,
    };
    if !n.is_finite() {
        return None;
    }
    const MILLIS_THRESHOLD: f64 = 1e12;
    // Safe (`Option`-returning) constructors: out-of-range input yields `None`, not a panic.
    if n >= MILLIS_THRESHOLD {
        DateTime::from_timestamp_millis(n as i64)
    } else {
        let secs = n.floor() as i64;
        let nsecs = ((n - n.floor()) * 1e9) as u32;
        DateTime::from_timestamp(secs, nsecs)
    }
}

/// `event` missing -> 12, `event` blank -> 13.
/// present, non-null but unparsable `time` -> 6.
fn validate_event(event: &VrlValue) -> Option<(u32, &'static str)> {
    let VrlValue::Object(obj) = event else {
        return None;
    };
    match obj.get("event") {
        None => return Some((12, "Event field is required")),
        Some(value) if is_blank_event(value) => return Some((13, "Event field cannot be blank")),
        _ => {}
    }
    if let Some(time) = obj.get("time")
        && !matches!(time, VrlValue::Null)
        && parse_hec_time(time).is_none()
    {
        return Some((6, "invalid data format"));
    }
    None
}

/// A HEC `event` value is blank if it's `null` or an empty/whitespace-only string.
fn is_blank_event(value: &VrlValue) -> bool {
    match value {
        VrlValue::Null => true,
        VrlValue::Bytes(b) => std::str::from_utf8(b).is_ok_and(|s| s.trim().is_empty()),
        _ => false,
    }
}

/// Maps one HEC event to `(table, per-event map, tag names)`: `time`->timestamp,
/// `index`->table, host/source/sourcetype/`fields`->tags, `event`+rest->data.
/// `None` if the event isn't a JSON object.
fn hec_event_to_map(
    event: VrlValue,
    query_table: Option<&str>,
) -> Option<(String, VrlValue, Vec<String>)> {
    let mut obj = match event {
        VrlValue::Object(obj) => obj,
        other => {
            debug!("skipping non-object splunk HEC event: {other:?}");
            return None;
        }
    };

    // Timestamp: HEC `time` is honored first, else ingest time.
    let ts = obj
        .remove("time")
        .as_ref()
        .and_then(parse_hec_time)
        .unwrap_or_else(Utc::now);

    // Table routing: `index` (consumed) -> `?table=` -> default.
    let index = match obj.remove("index") {
        Some(VrlValue::Bytes(b)) => Some(String::from_utf8_lossy(&b).into_owned()),
        _ => None,
    };
    let table = index
        .as_deref()
        .and_then(sanitize_index)
        .or_else(|| query_table.map(str::to_string))
        .unwrap_or_else(|| DEFAULT_SPLUNK_TABLE.to_string());

    let mut map: BTreeMap<KeyString, VrlValue> = BTreeMap::new();
    map.insert(
        KeyString::from(greptime_timestamp()),
        VrlValue::Timestamp(ts),
    );

    let mut tag_names: Vec<String> = Vec::new();

    // `fields` is flat: spread its keys to top-level columns, all tags.
    if let Some(VrlValue::Object(fields)) = obj.remove("fields") {
        for (k, v) in fields {
            tag_names.push(k.as_str().to_string());
            map.insert(k, v);
        }
    }

    // host / source / sourcetype are tags.
    for key in ["host", "source", "sourcetype"] {
        if let Some(v) = obj.remove(key) {
            tag_names.push(key.to_string());
            map.insert(KeyString::from(key), v);
        }
    }

    // `event` and any remaining keys are data columns.
    for (k, v) in obj {
        map.insert(k, v);
    }

    Some((table, VrlValue::Object(map), tag_names))
}

/// Retags `Field` columns to `Tag` per table (identity makes everything a Field) so the
/// insert path adds them to the primary key. Tags are scoped by table name so a batch
/// targeting multiple tables can't cross-promote a same-named field. Identity-only:
/// rebuilds under the default opt.
fn apply_tag_columns(
    ctx_req: ContextReq,
    tag_columns: &HashMap<String, HashSet<String>>,
) -> ContextReq {
    let mut reqs = ctx_req.all_req().collect::<Vec<_>>();
    for req in &mut reqs {
        let Some(rows) = req.rows.as_mut() else {
            continue;
        };
        let Some(tags) = tag_columns.get(&req.table_name) else {
            continue;
        };
        for col in &mut rows.schema {
            if tags.contains(&col.column_name) {
                col.semantic_type = SemanticType::Tag as i32;
            }
        }
    }
    ContextReq::default_opt_with_reqs(reqs)
}

/// Coerces a Splunk `index` into a valid table name (`NAME_PATTERN`); `None` if empty.
fn sanitize_index(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if NAME_PATTERN_REG.is_match(trimmed) {
        return Some(trimmed.to_string());
    }
    let mut out = String::with_capacity(trimmed.len());
    for c in trimmed.chars() {
        // body-allowed set
        if c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '-' | '.' | '@' | '#') {
            out.push(c);
        } else {
            out.push('_'); // spaces, slashes, unicode, etc. → '_'
        }
    }

    let first_ok = out
        .chars()
        .next()
        .map(|c| c.is_ascii_alphabetic() || matches!(c, '_' | ':' | '-'))
        .unwrap_or(false);

    if !first_ok {
        out.insert(0, '_');
    }

    NAME_PATTERN_REG.is_match(&out).then_some(out)
}

pub(crate) fn is_splunk_request<B>(req: &axum::extract::Request<B>) -> bool {
    // Match only `/v1/splunk/<subpath>`
    req.uri().path().starts_with("/v1/splunk/")
}
/// Like `ingest_logs_inner`, but retags metadata columns (identity default) before insert.
async fn ingest_events(
    handler: PipelineHandlerRef,
    pipeline: PipelineDefinition,
    requests: Vec<PipelineIngestRequest>,
    query_ctx: QueryContextRef,
    pipeline_params: GreptimePipelineParams,
    tag_columns: HashMap<String, HashSet<String>>,
    apply_tags: bool,
) -> Result<HttpResponse> {
    let exec_timer = Instant::now();
    let pipeline_ctx = PipelineContext::new(&pipeline, &pipeline_params, query_ctx.channel());

    let mut ctx_req = ContextReq::default();
    for req in requests {
        ctx_req.merge(run_pipeline(&handler, &pipeline_ctx, req, &query_ctx, true).await?);
    }

    let ctx_req = if apply_tags {
        apply_tag_columns(ctx_req, &tag_columns)
    } else {
        ctx_req
    };

    execute_log_context_req(
        handler,
        ctx_req,
        query_ctx,
        exec_timer,
        &METRIC_HTTP_LOGS_INGESTION_COUNTER,
        &METRIC_HTTP_LOGS_INGESTION_ELAPSED,
    )
    .await
}

/// `GET /services/collector/health` (+ `/1.0`). Public (see `PUBLIC_API_PREFIX`),
/// since clients probe it before sending. `ack`/`token` query params are ignored.
#[axum_macros::debug_handler]
pub async fn handle_health() -> impl IntoResponse {
    hec_response(StatusCode::OK, HEC_HEALTHY_CODE, "HEC is healthy")
}

/// `POST /services/collector/event` (+ `/services/collector`, `/event/1.0` aliases).
/// Parses HEC events, runs them through the pipeline (identity default, overridable),
/// and inserts with metadata columns as tags.
#[axum_macros::debug_handler]
pub async fn handle_event(
    State(log_state): State<LogState>,
    Query(params): Query<LogIngesterQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    headers: HeaderMap,
    payload: Bytes,
) -> impl IntoResponse {
    query_ctx.set_channel(Channel::Splunk);
    let events = match parse_hec_events(&payload) {
        Ok(events) => events,
        // HEC code 6 == "invalid data format".
        Err(_) => return hec_response(StatusCode::BAD_REQUEST, 6, "invalid data format"),
    };
    if events.is_empty() {
        // HEC code 5 == "No data".
        return hec_response(StatusCode::BAD_REQUEST, 5, "No data");
    }

    // Map each event -> (table, per-event map, tag names); group by table.
    let query_table = params.table.as_deref();
    let mut by_table: HashMap<String, Vec<VrlValue>> = HashMap::new();
    let mut tag_columns: HashMap<String, HashSet<String>> = HashMap::new();
    for event in events {
        // Reject the batch on an invalid event: missing/blank `event` (12/13) or an
        // unparsable `time` (6).
        if let Some((code, text)) = validate_event(&event) {
            return hec_response(StatusCode::BAD_REQUEST, code, text);
        }
        if let Some((table, map, tags)) = hec_event_to_map(event, query_table) {
            tag_columns.entry(table.clone()).or_default().extend(tags);
            by_table.entry(table).or_default().push(map);
        }
    }
    let requests: Vec<PipelineIngestRequest> = by_table
        .into_iter()
        .map(|(table, values)| PipelineIngestRequest { table, values })
        .collect();

    // Events parsed but none were JSON objects, so nothing is ingestable. HEC code 6 == "invalid data format".
    if requests.is_empty() {
        return hec_response(StatusCode::BAD_REQUEST, 6, "invalid data format");
    }

    // Bad table name (e.g. invalid `?table=`) -> HEC code 7 ("incorrect index").
    if let Some(bad) = requests
        .iter()
        .find(|r| !NAME_PATTERN_REG.is_match(&r.table))
    {
        let msg = format!("incorrect index: {}", bad.table);
        return hec_response(StatusCode::BAD_REQUEST, 7, &msg);
    }

    resolve_pipeline_and_ingest(
        log_state,
        query_ctx,
        &headers,
        params.pipeline_name.clone(),
        params.version.clone(),
        requests,
        tag_columns,
    )
    .await
}

/// `POST /services/collector/raw` (+ `/raw/1.0` alias). Body is raw text, one event
/// per line, stored as it is in the [`RAW_MESSAGE_COLUMN`]; metadata comes from query
/// params and applies to every event. `channel` (param or `x-splunk-request-channel`
/// header) is accepted but ignored until indexer acknowledgment lands;
#[axum_macros::debug_handler]
pub async fn handle_raw(
    State(log_state): State<LogState>,
    Query(params): Query<SplunkRawQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    headers: HeaderMap,
    payload: Bytes,
) -> impl IntoResponse {
    query_ctx.set_channel(Channel::Splunk);

    let body = String::from_utf8_lossy(&payload);
    let lines = parse_raw_lines(&body);
    if lines.is_empty() {
        // HEC code 5 == "No data".
        return hec_response(StatusCode::BAD_REQUEST, 5, "No data");
    }

    // Request-level default timestamp: `?time=` (epoch) or ingest time. Splunk would
    // additionally extract per-event timestamps from line content; this doesn't support that yet.
    let ts = match &params.time {
        Some(t) => match parse_hec_time(&VrlValue::Bytes(Bytes::from(t.clone()))) {
            Some(ts) => ts,
            // HEC code 6 == "invalid data format".
            None => return hec_response(StatusCode::BAD_REQUEST, 6, "invalid data format"),
        },
        None => Utc::now(),
    };

    // Table routing: `?index=` (sanitized) -> `?table=` -> default.
    let table = params
        .index
        .as_deref()
        .and_then(sanitize_index)
        .or_else(|| params.table.clone())
        .unwrap_or_else(|| DEFAULT_SPLUNK_TABLE.to_string());
    // Bad table name (e.g. invalid `?table=`) -> HEC code 7 ("incorrect index").
    if !NAME_PATTERN_REG.is_match(&table) {
        let msg = format!("incorrect index: {table}");
        return hec_response(StatusCode::BAD_REQUEST, 7, &msg);
    }

    let metadata = raw_metadata(&params);
    let values = lines
        .iter()
        .map(|line| raw_line_to_map(line, ts, &metadata))
        .collect();
    let tag_columns = HashMap::from([(
        table.clone(),
        metadata.iter().map(|(key, _)| key.to_string()).collect(),
    )]);
    let requests = vec![PipelineIngestRequest { table, values }];

    resolve_pipeline_and_ingest(
        log_state,
        query_ctx,
        &headers,
        params.pipeline_name.clone(),
        params.version.clone(),
        requests,
        tag_columns,
    )
    .await
}

/// Shared tail of `/event` and `/raw`: resolves the pipeline (identity default;
/// overridable via param/header, with an optional `?version=` pin), enables tag
/// promotion + metadata-first primary-key ordering for the identity path only, runs
/// the ingest, and maps the outcome to a HEC response.
#[allow(clippy::too_many_arguments)]
async fn resolve_pipeline_and_ingest(
    log_state: LogState,
    mut query_ctx: QueryContext,
    headers: &HeaderMap,
    pipeline_name: Option<String>,
    version: Option<String>,
    requests: Vec<PipelineIngestRequest>,
    tag_columns: HashMap<String, HashSet<String>>,
) -> axum::response::Response {
    // Pipeline: identity by default; override via `pipeline_name` param or header.
    let pipeline_name = pipeline_name.unwrap_or_else(|| {
        headers
            .get(GREPTIME_PIPELINE_NAME_HEADER_NAME)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME)
            .to_string()
    });
    let version = match to_pipeline_version(version.as_deref()) {
        Ok(version) => version,
        // HEC code 6 == "invalid data format" (bad `?version=`).
        Err(_) => return hec_response(StatusCode::BAD_REQUEST, 6, "invalid pipeline version"),
    };
    // Only post-process tags for the identity default; respect a user pipeline's schema.
    let apply_tags = pipeline_name == GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME;
    if apply_tags {
        // Ask the insert path to lead the primary key with our metadata tags. Scoped to the
        // identity path so a user pipeline's own key order is left untouched.
        query_ctx.set_extension(SPLUNK_PK_METADATA_ORDER_KEY, "true");
    }
    // custom_time_index so timestamp doesn't get overridden by identity pipeline.
    let custom_time_index = Some((format!("{};epoch;ns", greptime_timestamp()), false));
    let pipeline = match PipelineDefinition::from_name(&pipeline_name, version, custom_time_index) {
        Ok(pipeline) => pipeline,
        Err(_) => return hec_response(StatusCode::INTERNAL_SERVER_ERROR, 8, "pipeline error"),
    };
    let pipeline_params =
        GreptimePipelineParams::from_map(extract_pipeline_params_map_from_headers(headers));

    match ingest_events(
        log_state.log_handler,
        pipeline,
        requests,
        Arc::new(query_ctx),
        pipeline_params,
        tag_columns,
        apply_tags,
    )
    .await
    {
        // HEC code 0 == "Success".
        Ok(_) => hec_response(StatusCode::OK, 0, "Success"),
        Err(e) => {
            error!(e; "failed to ingest splunk hec events");
            // client errors -> HEC code 6, else 8.
            let status = status_code_to_http_status(&e.status_code());
            let code = if status.is_client_error() { 6 } else { 8 };
            let msg = e.to_string();
            hec_response(status, code, &msg)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events_for(body: &[u8]) -> Vec<VrlValue> {
        parse_hec_events(body).unwrap()
    }

    #[test]
    fn parses_single_object() {
        let events = events_for(br#"{"event":"hello","time":1}"#);
        assert_eq!(events, vec![json!({"event":"hello","time":1}).into()]);
    }

    #[test]
    fn parses_concatenated_objects_without_separator() {
        let events = events_for(br#"{"event":"a"}{"event":"b"}"#);
        assert_eq!(
            events,
            vec![json!({"event":"a"}).into(), json!({"event":"b"}).into()]
        );
    }

    #[test]
    fn parses_newline_and_whitespace_separated_objects() {
        // newline, leading spaces, and a tab between objects — none are required.
        let events = events_for(b"{\"event\":\"a\"}\n  {\"event\":\"b\"}\t{\"event\":\"c\"}");
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn parses_top_level_array_into_flat_events() {
        let events = events_for(br#"[{"event":"a"},{"event":"b"}]"#);
        assert_eq!(
            events,
            vec![json!({"event":"a"}).into(), json!({"event":"b"}).into()]
        );
    }

    #[test]
    fn flattens_mixed_array_and_trailing_object() {
        // a top-level array immediately followed by a bare object.
        let events = events_for(br#"[{"event":"a"},{"event":"b"}]{"event":"c"}"#);
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn empty_or_whitespace_body_yields_no_events() {
        assert!(events_for(b"").is_empty());
        assert!(events_for(b"   \n  ").is_empty());
    }

    #[test]
    fn malformed_json_is_rejected() {
        assert!(parse_hec_events(br#"{"event":"a"}{bad}"#).is_err());
    }

    // ---- parse_raw_lines ----

    #[test]
    fn parses_raw_lines() {
        // single line, no trailing newline.
        assert_eq!(parse_raw_lines("Hello World"), vec!["Hello World"]);
        // \n and \r\n separated; trailing newline produces no extra event.
        assert_eq!(parse_raw_lines("a\nb\r\nc\n"), vec!["a", "b", "c"]);
        // blank / whitespace-only lines are skipped.
        assert_eq!(parse_raw_lines("a\n\n   \n\t\nb"), vec!["a", "b"]);
        // content is verbatim: leading indentation and inner whitespace preserved.
        assert_eq!(
            parse_raw_lines("line one\n  indented stack frame"),
            vec!["line one", "  indented stack frame"]
        );
        // a JSON-looking line stays an opaque string (no parsing on /raw).
        assert_eq!(parse_raw_lines("{\"a\":1}"), vec!["{\"a\":1}"]);
        // empty / whitespace-only body yields no events (-> HEC code 5 upstream).
        assert!(parse_raw_lines("").is_empty());
        assert!(parse_raw_lines(" \n \r\n ").is_empty());
    }

    // ---- raw_metadata / raw_line_to_map ----

    #[test]
    fn maps_raw_line_with_request_metadata() {
        let params = SplunkRawQueryParams {
            host: Some("web-01".to_string()),
            sourcetype: Some("access_log".to_string()),
            ..Default::default()
        };
        let meta = raw_metadata(&params);
        // present params only; keys are the tag-column names.
        assert_eq!(meta, vec![("host", "web-01"), ("sourcetype", "access_log")]);

        let ts = DateTime::from_timestamp(1447828325, 0).unwrap();
        let VrlValue::Object(m) = raw_line_to_map("GET /api 200", ts, &meta) else {
            panic!("expected object");
        };
        assert_eq!(
            m.get(RAW_MESSAGE_COLUMN),
            Some(&VrlValue::from(json!("GET /api 200")))
        );
        assert_eq!(m.get("host"), Some(&VrlValue::from(json!("web-01"))));
        assert_eq!(
            m.get("sourcetype"),
            Some(&VrlValue::from(json!("access_log")))
        );
        // absent metadata (`source`) makes no column.
        assert!(!m.contains_key("source"));
        assert!(matches!(
            m.get(greptime_timestamp()),
            Some(VrlValue::Timestamp(dt)) if dt.timestamp() == 1447828325
        ));

        // no query params at all -> just timestamp + message.
        let default_params = SplunkRawQueryParams::default();
        let empty = raw_metadata(&default_params);
        assert!(empty.is_empty());
        let VrlValue::Object(m) = raw_line_to_map("x", ts, &empty) else {
            panic!("expected object");
        };
        assert_eq!(m.len(), 2);
    }

    // ---- parse_hec_time ----

    #[test]
    fn parse_time_integer_seconds() {
        let v: VrlValue = json!(1426279439).into();
        assert_eq!(parse_hec_time(&v).unwrap().timestamp(), 1426279439);
    }

    #[test]
    fn parse_time_fractional_seconds_keeps_millis() {
        let v: VrlValue = json!(1426279439.5).into();
        // the .5s must survive (not be truncated like EpochProcessor would).
        assert_eq!(
            parse_hec_time(&v).unwrap().timestamp_millis(),
            1426279439500
        );
    }

    #[test]
    fn parse_time_integer_millis() {
        let v: VrlValue = json!(1447828325000_i64).into();
        // past the millis threshold -> read as ms, same instant as 1447828325s.
        assert_eq!(parse_hec_time(&v).unwrap().timestamp(), 1447828325);
    }

    #[test]
    fn parse_time_string_number() {
        let v: VrlValue = json!("1426279439").into();
        assert_eq!(parse_hec_time(&v).unwrap().timestamp(), 1426279439);
    }

    #[test]
    fn parse_time_passthrough_timestamp() {
        let dt = DateTime::from_timestamp_nanos(123_456_789);
        assert_eq!(parse_hec_time(&VrlValue::Timestamp(dt)), Some(dt));
    }

    #[test]
    fn parse_time_missing_or_invalid_is_none() {
        assert!(parse_hec_time(&VrlValue::Null).is_none());
        let not_num: VrlValue = json!("not a number").into();
        assert!(parse_hec_time(&not_num).is_none());
        let obj: VrlValue = json!({ "x": 1 }).into();
        assert!(parse_hec_time(&obj).is_none());
    }

    // ---- sanitize_index ----

    #[test]
    fn sanitize_keeps_valid_names() {
        assert_eq!(sanitize_index("main").as_deref(), Some("main"));
        assert_eq!(
            sanitize_index("web-prod.2024").as_deref(),
            Some("web-prod.2024")
        );
        assert_eq!(
            sanitize_index("cpu:metrics").as_deref(),
            Some("cpu:metrics")
        );
    }

    #[test]
    fn sanitize_replaces_invalid_chars() {
        assert_eq!(
            sanitize_index("my index/v2").as_deref(),
            Some("my_index_v2")
        );
    }

    #[test]
    fn sanitize_fixes_leading_digit() {
        assert_eq!(sanitize_index("123logs").as_deref(), Some("_123logs"));
    }

    #[test]
    fn sanitize_empty_is_none() {
        assert!(sanitize_index("").is_none());
        assert!(sanitize_index("   ").is_none());
    }

    #[test]
    fn sanitize_output_is_always_a_valid_table_name() {
        // Invariant: a non-empty input never yields a name the create path would reject.
        for raw in [
            "main",
            "web-prod.2024",
            "my index/v2",
            "123",
            "@#@#",
            "...",
            "日本語 logs",
            "a/b\\c",
        ] {
            if let Some(name) = sanitize_index(raw) {
                assert!(
                    NAME_PATTERN_REG.is_match(&name),
                    "sanitized {raw:?} -> {name:?} is not a valid table name"
                );
            }
        }
    }

    // ---- hec_event_to_map ----

    #[test]
    fn map_extracts_metadata_and_routes_by_index() {
        let event: VrlValue = json!({
            "time": 1426279439,
            "host": "web-01",
            "source": "nginx",
            "sourcetype": "access",
            "index": "web_logs",
            "event": "GET /api 200",
            "fields": { "region": "us-east" }
        })
        .into();

        let (table, map, tags) = hec_event_to_map(event, None).unwrap();

        // `index` -> table name.
        assert_eq!(table, "web_logs");

        // tags = host/source/sourcetype + each `fields` key.
        let tagset: HashSet<&str> = tags.iter().map(String::as_str).collect();
        assert_eq!(
            tagset,
            HashSet::from(["host", "source", "sourcetype", "region"])
        );

        let VrlValue::Object(m) = map else {
            panic!("expected object");
        };
        // metadata + fields became columns with their values.
        assert_eq!(m.get("host"), Some(&VrlValue::from(json!("web-01"))));
        assert_eq!(m.get("region"), Some(&VrlValue::from(json!("us-east"))));
        assert_eq!(m.get("event"), Some(&VrlValue::from(json!("GET /api 200"))));
        // `time` became the timestamp column, not a `time` column.
        assert!(!m.contains_key("time"));
        assert!(matches!(
            m.get(greptime_timestamp()),
            Some(VrlValue::Timestamp(dt)) if dt.timestamp() == 1426279439
        ));
        // `index` and `fields` are consumed, not columns.
        assert!(!m.contains_key("index"));
        assert!(!m.contains_key("fields"));
    }

    #[test]
    fn map_falls_back_to_query_table_then_default() {
        let ev1: VrlValue = json!({ "event": "x" }).into();
        let (t1, _, _) = hec_event_to_map(ev1, Some("from_query")).unwrap();
        assert_eq!(t1, "from_query");

        let ev2: VrlValue = json!({ "event": "x" }).into();
        let (t2, _, _) = hec_event_to_map(ev2, None).unwrap();
        assert_eq!(t2, "splunk_logs");
    }

    #[test]
    fn map_sanitizes_index_for_table() {
        let ev: VrlValue = json!({ "index": "web/prod", "event": "x" }).into();
        let (table, _, _) = hec_event_to_map(ev, None).unwrap();
        assert_eq!(table, "web_prod");
    }

    #[test]
    fn map_rejects_non_object_event() {
        let ev: VrlValue = json!("just a string").into();
        assert!(hec_event_to_map(ev, None).is_none());
    }

    // ---- validate_event ----

    #[test]
    fn validates_event() {
        let check = |v: serde_json::Value| validate_event(&v.into());

        // missing `event` -> code 12.
        assert_eq!(
            check(json!({ "host": "h" })),
            Some((12, "Event field is required"))
        );
        // present but blank (empty / whitespace) or null -> code 13.
        assert_eq!(
            check(json!({ "event": "" })),
            Some((13, "Event field cannot be blank"))
        );
        assert_eq!(
            check(json!({ "event": "   " })),
            Some((13, "Event field cannot be blank"))
        );
        assert_eq!(
            check(json!({ "event": null })),
            Some((13, "Event field cannot be blank"))
        );
        // valid: non-empty string, object, or other non-blank value.
        assert_eq!(check(json!({ "event": "hello" })), None);
        assert_eq!(check(json!({ "event": { "a": 1 } })), None);
        assert_eq!(check(json!({ "event": 0 })), None);
        // non-object events aren't validated here (handled by `hec_event_to_map`).
        assert_eq!(check(json!("just a string")), None);

        // present but unparsable `time` -> code 6 (number string / numeric are fine).
        let bad_time = Some((6, "invalid data format"));
        assert_eq!(
            check(json!({ "event": "x", "time": "not-a-time" })),
            bad_time
        );
        assert_eq!(check(json!({ "event": "x", "time": { "a": 1 } })), bad_time);
        assert_eq!(check(json!({ "event": "x", "time": 1700000000 })), None);
        assert_eq!(check(json!({ "event": "x", "time": "1700000000" })), None);
        // absent or null `time` falls back to ingest time, so it's allowed.
        assert_eq!(check(json!({ "event": "x" })), None);
        assert_eq!(check(json!({ "event": "x", "time": null })), None);
    }

    #[test]
    fn parses_minimal_events_in_both_batch_forms() {
        // Splunk docs "Example 3": minimal events (`event` + `time` only), sent both as
        // concatenated objects (whitespace-separated) and as a JSON array.
        let concatenated = r#"{
  "event": "event 1",
  "time": 1447828325
}

{
  "event": "event 2",
  "time": 1447828326
}"#;
        let array = r#"[
  { "event": "event 1", "time": 1447828325 },
  { "event": "event 2", "time": 1447828326 }
]"#;

        for body in [concatenated, array] {
            let events = parse_hec_events(body.as_bytes()).unwrap();
            assert_eq!(events.len(), 2);

            let (table, map, tags) =
                hec_event_to_map(events.into_iter().next().unwrap(), None).unwrap();
            assert_eq!(table, "splunk_logs"); // no `index` -> default table
            assert!(tags.is_empty()); // no host/source/sourcetype/fields
            let VrlValue::Object(m) = map else {
                panic!("expected object");
            };
            assert_eq!(m.get("event"), Some(&VrlValue::from(json!("event 1"))));
            assert!(matches!(
                m.get(greptime_timestamp()),
                Some(VrlValue::Timestamp(dt)) if dt.timestamp() == 1447828325
            ));
            assert!(!m.contains_key("host"));
        }
    }

    #[test]
    fn map_keeps_event_object_for_pipeline_flattening() {
        let ev: VrlValue = json!({ "event": { "a": 1 } }).into();
        let (_, map, _) = hec_event_to_map(ev, None).unwrap();
        let VrlValue::Object(m) = map else {
            panic!("expected object");
        };
        assert!(matches!(m.get("event"), Some(VrlValue::Object(_))));
    }

    #[test]
    fn map_uses_ingest_time_when_time_absent() {
        let ev: VrlValue = json!({ "event": "x" }).into();
        let (_, map, _) = hec_event_to_map(ev, None).unwrap();
        let VrlValue::Object(m) = map else {
            panic!("expected object");
        };
        assert!(matches!(
            m.get(greptime_timestamp()),
            Some(VrlValue::Timestamp(_))
        ));
    }

    // ---- real client payload ----

    #[test]
    fn parses_real_client_payloads() {
        // Shapes captured from real `splunk_hec` clients (values trimmed, structure
        // verbatim). The two clients deliberately disagree on batch separator,
        // `event` type, and `fields` keys — the parser must handle all of it.

        // --- Vector splunk_hec sink: NO separator; `event` is an object. ---
        let vector = concat!(
            r#"{"event":{"message":"GET /api 200","status":"200"},"fields":{"region":"us-east"},"#,
            r#""time":1781713834.069,"host":"web-01","index":"main","source":"vector-src","sourcetype":"vector_demo"}"#,
            r#"{"event":{"message":"POST /login 401","status":"401"},"fields":{"region":"us-west"},"#,
            r#""time":1781713834.119,"host":"web-02","index":"main","source":"vector-src","sourcetype":"vector_demo"}"#,
        );
        let events = parse_hec_events(vector.as_bytes()).unwrap();
        assert_eq!(events.len(), 2); // concatenated, no separator
        let (table, map, tags) =
            hec_event_to_map(events.into_iter().next().unwrap(), None).unwrap();
        assert_eq!(table, "main");
        let tagset: HashSet<&str> = tags.iter().map(String::as_str).collect();
        assert_eq!(
            tagset,
            HashSet::from(["host", "source", "sourcetype", "region"])
        );
        let VrlValue::Object(m) = map else {
            panic!("expected object");
        };
        assert!(matches!(
            m.get(greptime_timestamp()),
            Some(VrlValue::Timestamp(dt)) if dt.timestamp() == 1781713834
        ));
        assert!(matches!(m.get("event"), Some(VrlValue::Object(_)))); // event is an object

        // --- OTel Collector splunk_hec exporter: NEWLINE-separated; `event` is a
        //     string; a `fields` key contains dots. ---
        let otel = concat!(
            r#"{"event":"{\"level\":\"info\",\"msg\":\"login ok\"}","fields":{"log.file.name":"app.log"},"#,
            r#""host":"unknown","source":"otel-src","sourcetype":"otel_st","index":"main","time":1781714234.6849608}"#,
            "\n",
            r#"{"event":"{\"level\":\"error\",\"msg\":\"disk full\"}","fields":{"log.file.name":"app.log"},"#,
            r#""host":"unknown","source":"otel-src","sourcetype":"otel_st","index":"main","time":1781714234.6849632}"#,
        );
        let events = parse_hec_events(otel.as_bytes()).unwrap();
        assert_eq!(events.len(), 2); // newline-separated
        let (table, map, tags) =
            hec_event_to_map(events.into_iter().next().unwrap(), None).unwrap();
        assert_eq!(table, "main");
        let tagset: HashSet<&str> = tags.iter().map(String::as_str).collect();
        // a dotted `fields` key still becomes a tag column.
        assert_eq!(
            tagset,
            HashSet::from(["host", "source", "sourcetype", "log.file.name"])
        );
        let VrlValue::Object(m) = map else {
            panic!("expected object");
        };
        assert!(matches!(
            m.get(greptime_timestamp()),
            Some(VrlValue::Timestamp(dt)) if dt.timestamp() == 1781714234
        ));
        assert!(matches!(m.get("event"), Some(VrlValue::Bytes(_)))); // event is a string
    }

    #[test]
    fn tag_promotion_is_scoped_per_table() {
        use api::v1::{ColumnDataType, ColumnSchema, RowInsertRequest, Rows};

        fn field_col(name: &str) -> ColumnSchema {
            ColumnSchema {
                column_name: name.to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                datatype_extension: None,
                options: None,
            }
        }
        fn req(table: &str, cols: &[&str]) -> RowInsertRequest {
            RowInsertRequest {
                table_name: table.to_string(),
                rows: Some(Rows {
                    schema: cols.iter().map(|c| field_col(c)).collect(),
                    rows: vec![],
                }),
            }
        }

        // One batch -> two tables, both with a `region` column. `region` is a tag in
        // table "a" only; table "b"'s same-named field must NOT be promoted.
        let ctx_req =
            ContextReq::default_opt_with_reqs(vec![req("a", &["region"]), req("b", &["region"])]);
        let mut tags: HashMap<String, HashSet<String>> = HashMap::new();
        tags.insert("a".to_string(), HashSet::from(["region".to_string()]));
        tags.insert("b".to_string(), HashSet::new());

        let out = apply_tag_columns(ctx_req, &tags);

        for r in out.ref_all_req() {
            let region = r
                .rows
                .as_ref()
                .unwrap()
                .schema
                .iter()
                .find(|c| c.column_name == "region")
                .unwrap();
            let expected = match r.table_name.as_str() {
                "a" => SemanticType::Tag as i32,
                "b" => SemanticType::Field as i32, // would have been Tag before the per-table fix
                other => panic!("unexpected table {other}"),
            };
            assert_eq!(region.semantic_type, expected, "table {}", r.table_name);
        }
    }
}
