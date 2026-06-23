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
use common_telemetry::error;
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
pub(crate) const SPLUNK_API_PATH_NAME: &str = "/v1/splunk";
/// HEC response code for a healthy collector. Splunk returns
/// `{"text":"HEC is healthy","code":17}`.
const HEC_HEALTHY_CODE: u32 = 17;

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

/// Maps one HEC event to `(table, per-event map, tag names)`: `time`->timestamp,
/// `index`->table, host/source/sourcetype/`fields`->tags, `event`+rest->data.
/// `None` if the event isn't a JSON object.
fn hec_event_to_map(
    event: VrlValue,
    query_table: Option<&str>,
) -> Option<(String, VrlValue, Vec<String>)> {
    let VrlValue::Object(mut obj) = event else {
        return None;
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
    let path = req.uri().path();
    path.starts_with(SPLUNK_API_PATH_NAME)
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
        if let Some((table, map, tags)) = hec_event_to_map(event, query_table)
        {
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

    // Pipeline: identity by default; override via `pipeline` param or header.
    let pipeline_name = params.pipeline_name.clone().unwrap_or_else(|| {
        headers
            .get(GREPTIME_PIPELINE_NAME_HEADER_NAME)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME)
            .to_string()
    });
    // Only post-process tags for the identity default; respect a user pipeline's schema.
    let apply_tags = pipeline_name == GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME;
    let pipeline = match PipelineDefinition::from_name(&pipeline_name, None, None) {
        Ok(pipeline) => pipeline,
        Err(_) => return hec_response(StatusCode::INTERNAL_SERVER_ERROR, 8, "pipeline error"),
    };
    let pipeline_params =
        GreptimePipelineParams::from_map(extract_pipeline_params_map_from_headers(&headers));

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
