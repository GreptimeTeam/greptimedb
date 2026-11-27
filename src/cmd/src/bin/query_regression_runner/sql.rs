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

use query::promql::planner::NATIVE_PROMQL_EXTENSION_KEY;
use reqwest::{Client, RequestBuilder};
use serde_json::{Value, json};

use crate::query_regression_runner::Result;
use crate::query_regression_runner::model::Query;

/// Header that carries per-request query hints.
///
/// The name is a literal because it lives in the `session` crate
/// (`session::hints::HINTS_KEY`), which this runner does not depend on.
const HINTS_HEADER: &str = "x-greptime-hints";

/// The hints value that asks the frontend to plan a PromQL statement on the
/// native fused-series route, built from the option key the planner reads.
pub(super) fn native_promql_hint() -> String {
    format!("{NATIVE_PROMQL_EXTENSION_KEY}=true")
}

/// The value of the hints header one query needs, or [`None`] when the query
/// declares no requirement.
///
/// A query without [`Query::require_native`] keeps sending exactly the request it
/// sent before this header existed.
pub(super) fn required_hints(query: &Query) -> Option<String> {
    query.require_native.then(native_promql_hint)
}

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

/// The plan trees of a `TQL ANALYZE ... FORMAT JSON` response body.
///
/// `format=json` answers `data` as one object per row keyed by column name, and
/// `FORMAT JSON` puts a plan tree into a row's `plan` cell: a node names itself in
/// `name`, carries the rest of its plan line in `param`, and hangs its sub-nodes
/// from `children`. The analyze result also carries a plain-text `Total rows: N`
/// cell, so only the cells that are JSON objects or arrays are kept; a text cell
/// never contributes evidence.
///
/// [`None`] means the body held no plan tree at all, which proves no node.
pub(super) fn plan_trees_from_response(body: &Value) -> Option<Vec<Value>> {
    let trees = body
        .get("data")?
        .as_array()?
        .iter()
        .filter_map(|row| row.get("plan")?.as_str())
        .filter_map(|cell| serde_json::from_str::<Value>(cell).ok())
        .filter(|tree| tree.is_object() || tree.is_array())
        .collect::<Vec<_>>();
    (!trees.is_empty()).then_some(trees)
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

pub(super) async fn http_post_sql(client: &Client, port: u16, sql: &str, db: &str) -> Value {
    http_post_sql_with_hints(client, port, sql, db, None).await
}

/// Posts one SQL statement, carrying `hints` in the request hints header when the
/// caller states any.
pub(super) async fn http_post_sql_with_hints(
    client: &Client,
    port: u16,
    sql: &str,
    db: &str,
    hints: Option<&str>,
) -> Value {
    let mut sample = post_form(
        client,
        format!("http://127.0.0.1:{port}/v1/sql"),
        &[("sql", sql), ("db", db), ("format", "json")],
        hints,
    )
    .await;
    sample
        .as_object_mut()
        .expect("HTTP samples are objects")
        .insert("sql".to_string(), Value::String(sql.to_string()));
    sample
}

/// Posts the Prometheus HTTP API range query (`/v1/prometheus/api/v1/query_range`)
/// one configured query declares, and measures the full request-to-body latency.
/// This exercises the Prometheus JSON response building path
/// (`PrometheusJsonResponse::record_batches_to_data`) that `/v1/sql` (including
/// `TQL ANALYZE`) does not go through.
///
/// `hints` carries the request hints header when the caller states any.
pub(super) async fn http_post_prom_range_query(
    client: &Client,
    port: u16,
    query: &Query,
    db: &str,
    hints: Option<&str>,
) -> Value {
    let mut sample = post_form(
        client,
        prom_range_query_url(port, db),
        &[
            ("query", &query.query),
            ("start", query.start.as_deref().unwrap_or_default()),
            ("end", query.end.as_deref().unwrap_or_default()),
            ("step", query.step.as_deref().unwrap_or_default()),
        ],
        hints,
    )
    .await;
    sample
        .as_object_mut()
        .expect("HTTP samples are objects")
        .insert("query".to_string(), Value::String(query.query.clone()));
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
    hints: Option<&str>,
) -> Value {
    let started = Instant::now();
    let request = build_form_request(client, url, form, hints);
    match request.send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            match response.text().await {
                Ok(raw) => {
                    let body = serde_json::from_str(&raw).unwrap_or_else(|_| json!({"raw": raw}));
                    let ok = status < 400 && !response_has_error(&body);
                    let mut sample = json!({
                        "ok": ok,
                        "status": status,
                        "latency_ms": started.elapsed().as_secs_f64() * 1000.0,
                        "response": body,
                    });
                    if status >= 400 {
                        sample
                            .as_object_mut()
                            .expect("HTTP samples are objects")
                            .insert("error".to_string(), Value::String(format!("HTTP {status}")));
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

/// Builds one form-encoded POST, attaching the hints header only when the query
/// states one, so an unflagged query's request stays what it was.
fn build_form_request(
    client: &Client,
    url: String,
    form: &[(&str, &str)],
    hints: Option<&str>,
) -> RequestBuilder {
    let request = client.post(url).form(form);
    match hints {
        Some(hints) => request.header(HINTS_HEADER, hints),
        None => request,
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

    fn query(require_native: bool) -> Query {
        Query {
            name: Some("native_range".to_string()),
            kind: Some("sql".to_string()),
            query: "TQL ANALYZE (0, 10, '5s') some_metric".to_string(),
            start: None,
            end: None,
            step: None,
            warmup: 0,
            iterations: 1,
            thresholds: serde_json::Map::new(),
            require_native,
        }
    }

    fn request_headers(query: &Query) -> reqwest::header::HeaderMap {
        let client = Client::new();
        build_form_request(
            &client,
            "http://127.0.0.1:4000/v1/sql".to_string(),
            &[("sql", "SELECT 1"), ("format", "json")],
            required_hints(query).as_deref(),
        )
        .build()
        .expect("request must build without a network")
        .headers()
        .clone()
    }

    #[test]
    fn native_route_query_states_the_hint_and_an_unflagged_query_does_not() {
        let native = request_headers(&query(true));
        assert_eq!(
            native
                .get(HINTS_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("query.experimental_native_promql=true")
        );

        let plain = request_headers(&query(false));
        assert!(plain.get(HINTS_HEADER).is_none());
        // The unflagged request keeps the headers it sent before the hint existed.
        let baseline = Client::new()
            .post("http://127.0.0.1:4000/v1/sql")
            .form(&[("sql", "SELECT 1"), ("format", "json")])
            .build()
            .expect("request must build without a network")
            .headers()
            .clone();
        assert_eq!(plain, baseline);
    }

    #[test]
    fn plan_trees_read_only_the_plan_column_of_the_json_data_shape() {
        // `format=json` answers `data` as one object per row keyed by column name,
        // and `FORMAT JSON` puts one plan tree into the `plan` cell.
        let analyze = json!({
            "data": [
                {
                    "stage": 0,
                    "node": 0,
                    "plan": json!({
                        "name": "CooperativeExec",
                        "param": "",
                        "children": [{"name": "FusedSeriesExec", "param": "metric=m", "children": []}],
                    })
                    .to_string(),
                },
                {
                    "stage": 1,
                    "node": 0,
                    "plan": json!({"name": "SeriesScan", "param": "region=0", "children": []})
                        .to_string(),
                },
            ],
            "execution_time_ms": 3,
        });
        let trees = plan_trees_from_response(&analyze).unwrap();
        assert_eq!(trees.len(), 2);
        assert_eq!(trees[0]["name"], "CooperativeExec");
        assert_eq!(trees[0]["children"][0]["name"], "FusedSeriesExec");
        assert_eq!(trees[1]["name"], "SeriesScan");

        // The analyze result always ends with a plain-text `Total rows` cell, which
        // is ignored rather than treated as a malformed tree.
        let with_total = json!({
            "data": [
                {"plan": json!({"name": "FusedSeriesExec", "param": "metric=m", "children": []})
                    .to_string()},
                {"plan": "Total rows: 580"},
            ],
        });
        assert_eq!(plan_trees_from_response(&with_total).unwrap().len(), 1);

        // A `plan` column of text cells, an empty cell, and a result without a
        // `plan` column all hold no tree, even when a cell spells the marker.
        assert!(
            plan_trees_from_response(&json!({"data": [{"plan": "CooperativeExec"}]})).is_none()
        );
        assert!(plan_trees_from_response(&json!({"data": [{"plan": "Total rows: 1"}]})).is_none());
        assert!(
            plan_trees_from_response(&json!({"data": [{"plan": "\"FusedSeries\""}]})).is_none()
        );
        assert!(plan_trees_from_response(&json!({"data": [{"stage": 0, "plan": ""}]})).is_none());
        assert!(plan_trees_from_response(&json!({"data": [{"note": "FusedSeries"}]})).is_none());
        assert!(plan_trees_from_response(&json!({"data": []})).is_none());
        assert!(
            plan_trees_from_response(&json!({"output": [{"rows": [["FusedSeries"]]}]})).is_none()
        );
    }

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
}
