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

use std::time::Instant;

use reqwest::Client;
use serde_json::{Map, Value, json};

use crate::query_regression_runner::Result;
use crate::query_regression_runner::model::ResponseFormat;

pub(super) fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(super) fn extract_rows(body: &Value) -> Vec<Value> {
    fn visit(body: &Value, rows: &mut Vec<Value>) {
        match body {
            Value::Object(object) => {
                for key in ["data", "rows", "records", "output"] {
                    let Some(value) = object.get(key) else {
                        continue;
                    };
                    if matches!(key, "data" | "rows") && value.is_array() {
                        rows.extend(value.as_array().unwrap().iter().cloned());
                    } else {
                        visit(value, rows);
                    }
                }
            }
            Value::Array(values) => {
                if !values.is_empty()
                    && values
                        .iter()
                        .all(|value| !value.is_object() && !value.is_array())
                {
                    rows.push(body.clone());
                } else {
                    for value in values {
                        visit(value, rows);
                    }
                }
            }
            _ => {}
        }
    }

    let mut rows = Vec::new();
    visit(body, &mut rows);
    rows
}

pub(super) fn row_value<'a>(row: &'a Value, index: usize, name: &str) -> Option<&'a Value> {
    match row {
        Value::Object(values) => [
            name.to_string(),
            name.to_ascii_uppercase(),
            name.to_ascii_lowercase(),
        ]
        .into_iter()
        .find_map(|key| values.get(&key)),
        Value::Array(values) => values.get(index),
        _ => Some(row),
    }
}

pub(super) fn row_u64(row: &Value, index: usize, name: &str) -> Result<u64> {
    let value =
        row_value(row, index, name).ok_or_else(|| format!("missing {name} in row {row}"))?;
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .ok_or_else(|| format!("invalid {name} in row {row}").into())
}

pub(super) fn value_text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Null => "None".to_string(),
        _ => value.to_string(),
    }
}

pub(super) fn extract_count_value(result: &Value) -> Option<u64> {
    let row = result
        .get("response")?
        .get("data")?
        .as_array()?
        .first()?
        .as_object()?;
    row.iter()
        .find(|(key, _)| {
            let key = key.to_ascii_lowercase();
            key == "count(*)" || key.starts_with("count(")
        })
        .and_then(|(_, value)| value_u64(Some(value)))
}

pub(super) fn value_u64(value: Option<&Value>) -> Option<u64> {
    value?.as_u64().or_else(|| value?.as_str()?.parse().ok())
}

pub(super) fn value_f64(value: Option<&Value>) -> Option<f64> {
    value?.as_f64().or_else(|| value?.as_str()?.parse().ok())
}

/// Posts one statement batch with the historical flat `format=json` body; its
/// parsers (setup, visibility, discovery) read the flat `response.data` shape.
pub(super) async fn http_post_sql(client: &Client, port: u16, sql: &str, db: &str) -> Value {
    http_post_sql_with_format(client, port, sql, db, ResponseFormat::Json).await
}

/// Posts one statement batch with an explicit `/v1/sql` response format. The
/// SQL text is sent unchanged.
pub(super) async fn http_post_sql_with_format(
    client: &Client,
    port: u16,
    sql: &str,
    db: &str,
    response_format: ResponseFormat,
) -> Value {
    let mut sample = post_form(
        client,
        format!("http://127.0.0.1:{port}/v1/sql"),
        &[
            ("sql", sql),
            ("db", db),
            ("format", response_format.as_str()),
        ],
        response_format,
    )
    .await;
    sample
        .as_object_mut()
        .expect("HTTP samples are objects")
        .insert("sql".to_string(), Value::String(sql.to_string()));
    sample
}

/// Posts a Prometheus HTTP API range query (`/v1/prometheus/api/v1/query_range`)
/// and measures the full request-to-body latency. This exercises the Prometheus
/// JSON response building path (`PrometheusJsonResponse::record_batches_to_data`)
/// that `/v1/sql` (including `TQL ANALYZE`) does not go through.
pub(super) async fn http_post_prom_range_query(
    client: &Client,
    port: u16,
    query: &str,
    start: Option<&str>,
    end: Option<&str>,
    step: Option<&str>,
    db: &str,
) -> Value {
    let mut sample = post_form(
        client,
        prom_range_query_url(port, db),
        &[
            ("query", query),
            ("start", start.unwrap_or_default()),
            ("end", end.unwrap_or_default()),
            ("step", step.unwrap_or_default()),
        ],
        ResponseFormat::Json,
    )
    .await;
    sample
        .as_object_mut()
        .expect("HTTP samples are objects")
        .insert("query".to_string(), Value::String(query.to_string()));
    sample
}

fn prom_range_query_url(port: u16, db: &str) -> String {
    let mut url = reqwest::Url::parse(&format!(
        "http://127.0.0.1:{port}/v1/prometheus/api/v1/query_range"
    ))
    .expect("fixed Prometheus range query URL must be valid");
    url.query_pairs_mut().append_pair("db", db);
    url.into()
}

async fn post_form(
    client: &Client,
    url: String,
    form: &[(&str, &str)],
    response_format: ResponseFormat,
) -> Value {
    let started = Instant::now();
    let request = client.post(url).form(form);
    match request.send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            match response.text().await {
                Ok(raw) => {
                    // Latency still ends after the body has been read and parsed.
                    let (body, malformed) = match serde_json::from_str(&raw) {
                        Ok(body) => (body, false),
                        Err(_) => (json!({"raw": raw}), true),
                    };
                    let (ok, failure) = if status >= 400 {
                        (false, Some(format!("HTTP {status}")))
                    } else {
                        match response_format {
                            ResponseFormat::Json => (!response_has_error(&body), None),
                            ResponseFormat::GreptimedbV1 => {
                                match native_response_error(&body, malformed) {
                                    Some(reason) => (false, Some(reason)),
                                    None => (true, None),
                                }
                            }
                        }
                    };
                    let mut sample = json!({
                        "ok": ok,
                        "status": status,
                        "latency_ms": started.elapsed().as_secs_f64() * 1000.0,
                        "response": body,
                    });
                    if let Some(error) = failure {
                        sample
                            .as_object_mut()
                            .expect("HTTP samples are objects")
                            .insert("error".to_string(), Value::String(error));
                    }
                    sample
                }
                Err(error) => json!({
                    "ok": false,
                    "status": status,
                    "latency_ms": started.elapsed().as_secs_f64() * 1000.0,
                    "error": error.to_string(),
                }),
            }
        }
        Err(error) => json!({
            "ok": false,
            "status": Value::Null,
            "latency_ms": started.elapsed().as_secs_f64() * 1000.0,
            "error": error.to_string(),
        }),
    }
}

/// Explains why a native `greptimedb_v1` body is not fully successful, or
/// `None` when it is. The server serializes a successful batch as
/// `{output: [...], execution_time_ms}`, where every `output` entry is an
/// externally tagged `GreptimeQueryOutput` (`{"affectedrows": N}` or
/// `{"records": {...}}`), and reports failures as the root `ErrorResponse`
/// (`{code, error, execution_time_ms}`). Only the root object and the direct
/// `output` entries are inspected: record rows and their values are never
/// visited, so error-like row values cannot fail a successful response.
/// Malformed JSON, non-envelope bodies, and unknown output shapes fail.
fn native_response_error(body: &Value, malformed: bool) -> Option<String> {
    if malformed {
        return Some("greptimedb_v1 response body is not valid JSON".to_string());
    }
    let Some(object) = body.as_object() else {
        return Some("greptimedb_v1 response body is not a JSON object".to_string());
    };
    if let Some(reason) = root_error(object) {
        return Some(reason);
    }
    match object.get("output") {
        Some(Value::Array(outputs)) => outputs.iter().enumerate().find_map(|(index, output)| {
            output_error(output).map(|reason| format!("greptimedb_v1 output[{index}]: {reason}"))
        }),
        Some(_) => Some("greptimedb_v1 `output` is not an array".to_string()),
        None => Some("greptimedb_v1 response has neither `output` nor a root error".to_string()),
    }
}

/// Status fields of the root envelope, matching the server's `ErrorResponse`.
fn root_error(envelope: &Map<String, Value>) -> Option<String> {
    if envelope.get("error").is_some_and(is_truthy) {
        return Some("`error` is set".to_string());
    }
    envelope
        .get("code")
        .filter(|value| !is_success_code(value))
        .map(|_| "`code` is not a success code".to_string())
}

/// Checks one direct `output` entry against the serialized, externally tagged
/// `GreptimeQueryOutput` shape: exactly one recognized variant whose payload
/// has that variant's type. Nothing below the payload is inspected, so row
/// values under `records` may contain arbitrary `error`/`code` fields.
fn output_error(output: &Value) -> Option<String> {
    let Some(object) = output.as_object() else {
        return Some("is not a statement output object".to_string());
    };
    let Some((variant, payload)) = object.iter().next().filter(|_| object.len() == 1) else {
        return Some(format!(
            "must contain exactly one output variant, found {} fields",
            object.len()
        ));
    };
    match (variant.as_str(), payload) {
        ("affectedrows", payload) if payload.as_u64().is_some() => None,
        ("affectedrows", _) => Some("`affectedrows` is not an unsigned integer".to_string()),
        ("records", Value::Object(records)) if records.get("rows").is_some_and(Value::is_array) => {
            None
        }
        ("records", _) => Some("`records` is not a records object with a `rows` array".to_string()),
        (variant, _) => Some(format!("`{variant}` is not a recognized output variant")),
    }
}

fn response_has_error(body: &Value) -> bool {
    let Some(body) = body.as_object() else {
        return false;
    };
    ["error", "err_msg", "error_msg"]
        .into_iter()
        .any(|key| body.get(key).is_some_and(is_truthy))
        || body
            .get("error_code")
            .is_some_and(|value| !is_success_code(value))
        || (!body.contains_key("output")
            && body
                .get("code")
                .is_some_and(|value| !is_success_code(value)))
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_none_or(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

fn is_success_code(value: &Value) -> bool {
    let code = match value {
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Null => "None".to_string(),
        _ => value.to_string(),
    };
    matches!(code.to_lowercase().as_str(), "" | "0" | "success")
}

pub(super) fn sql_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prom_range_query_url_places_database_in_query_parameters() {
        let url = reqwest::Url::parse(&prom_range_query_url(4000, "catalog-schema name"))
            .expect("generated URL must be valid");
        assert_eq!(url.path(), "/v1/prometheus/api/v1/query_range");
        assert_eq!(
            url.query_pairs().collect::<Vec<_>>(),
            vec![("db".into(), "catalog-schema name".into())]
        );
    }

    #[test]
    fn top_level_errors_do_not_inspect_rows() {
        assert!(response_has_error(&json!({"error_code": 7})));
        assert!(response_has_error(&json!({"code": "bad"})));
        assert!(!response_has_error(
            &json!({"output": [{"code": 7, "error": "row value"}]})
        ));
        assert!(!response_has_error(
            &json!({"error_code": "success", "output": []})
        ));
    }

    #[test]
    fn extracts_remote_write_count_from_data_map() {
        assert_eq!(
            extract_count_value(&json!({"response": {"data": [{"COUNT(*)": "12"}]}})),
            Some(12)
        );
        assert_eq!(
            extract_count_value(&json!({"response": {"data": [{"count(value)": 7}]}})),
            Some(7)
        );
        assert_eq!(
            extract_count_value(&json!({"response": {"data": [[]]}})),
            None
        );
    }

    #[test]
    fn native_validation_accepts_success_and_rejects_errors() {
        // Successful affectedrows and records outputs; error-like row values
        // below `records` are data, not failures.
        for body in [
            json!({"output": [{"affectedrows": 32768}, {"affectedrows": 0}], "execution_time_ms": 12}),
            json!({"output": [{"records": {"schema": {"column_schemas": []}, "rows": [], "total_rows": 0}}], "execution_time_ms": 3}),
            json!({"output": [{"records": {"schema": {"column_schemas": []}, "rows": [[{"error": "row value", "code": 7}]], "total_rows": 1}}], "execution_time_ms": 3}),
            json!({"output": [{"affectedrows": 1}, {"records": {"rows": [["status", "error"]]}}]}),
        ] {
            assert_eq!(native_response_error(&body, false), None, "{body}");
        }

        // Root errors fail even when `output` is present.
        for body in [
            json!({"code": 1004, "error": "cannot output multi-statements result in json format", "output": [{"affectedrows": 1}]}),
            json!({"code": 3000, "output": [{"affectedrows": 1}]}),
            json!({"error": "flush failed", "output": [{"affectedrows": 1}]}),
        ] {
            assert!(native_response_error(&body, false).is_some(), "{body}");
        }
        // A success code is tolerated.
        assert_eq!(
            native_response_error(&json!({"code": 0, "output": [{"affectedrows": 1}]}), false),
            None
        );

        // Output entries must be exactly one recognized variant whose payload
        // matches the serialized `GreptimeQueryOutput` shape.
        for (body, needle) in [
            (json!({"output": [{}]}), "exactly one output variant"),
            (
                json!({"output": [{"foo": 1}]}),
                "`foo` is not a recognized output variant",
            ),
            (
                json!({"output": [{"affectedrows": 1, "records": {"rows": []}}]}),
                "exactly one output variant",
            ),
            (
                json!({"output": [{"affectedrows": "1"}]}),
                "`affectedrows` is not an unsigned integer",
            ),
            (
                json!({"output": [{"records": 3}]}),
                "`records` is not a records object with a `rows` array",
            ),
            (
                json!({"output": [{"records": {}}]}),
                "`records` is not a records object with a `rows` array",
            ),
            (
                json!({"output": ["boom"]}),
                "is not a statement output object",
            ),
        ] {
            let reason = native_response_error(&body, false).unwrap();
            assert!(
                reason.starts_with("greptimedb_v1 output[0]: ") && reason.contains(needle),
                "{reason}"
            );
        }

        // Malformed and non-envelope 200 bodies fail.
        for (body, malformed) in [
            (json!({"raw": "<html>not json</html>"}), true),
            (json!({"raw": "<html>not json</html>"}), false),
            (json!({"foo": 1}), false),
            (json!("boom"), false),
            (json!({"output": "boom"}), false),
        ] {
            assert!(native_response_error(&body, malformed).is_some(), "{body}");
        }
    }
}
