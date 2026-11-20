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
use std::path::Path;
use std::time::Duration;
use std::{fs, io};

use query::promql::planner::NATIVE_PROMQL_EXTENSION_KEY;
use reqwest::Client;
use serde_json::{Map, Value, json};
use sql::dialect::GreptimeDbDialect;
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::statement::Statement;
use sql::statements::tql::Tql;
use sqlparser::ast::AnalyzeFormat;

use crate::query_regression_runner::model::{Measurement, Query, QueryResult, Scenario, Table};
use crate::query_regression_runner::plan::{load_plan, normalize_scenario};
use crate::query_regression_runner::sql::{
    http_post_prom_range_query, http_post_sql, http_post_sql_with_hints, plan_trees_from_response,
    required_hints, sql_ident,
};
use crate::query_regression_runner::{MeasureArgs, Result};

/// The plan node names that mark the native fused-series route: the logical
/// extension node and its physical form, the shape `src/query/src/planner.rs`
/// asserts on.
const NATIVE_PROMQL_OPERATORS: [&str; 2] = ["FusedSeries", "FusedSeriesExec"];

pub(super) async fn run_measure(args: MeasureArgs) -> Result<()> {
    if !args.http_timeout.is_finite() || args.http_timeout < 0.0 {
        return Err("--http-timeout must be a non-negative finite number".into());
    }

    let case_path = args.case.canonicalize()?;
    let plan = load_plan(&args.fixture_generator, &case_path)?;
    let case_text = fs::read_to_string(&case_path)?;
    let raw_case: toml::Value = toml::from_str(&case_text)?;
    let case_metadata = raw_case
        .get("case")
        .cloned()
        .unwrap_or(toml::Value::Table(toml::map::Map::new()));
    let case_metadata = serde_json::to_value(case_metadata)?;

    let scenario_value = plan
        .get("scenario")
        .cloned()
        .ok_or("fixture plan has no scenario")?;
    let scenario: Scenario = serde_json::from_value(scenario_value.clone())?;
    let (tables, configured_queries) = normalize_scenario(scenario)?;
    // A `require_native` query that cannot show its own plan is a case defect, not
    // a target defect: reject it before either target is contacted.
    validate_native_queries(&case_path, &configured_queries)?;

    let client = Client::builder()
        .timeout(Duration::from_secs_f64(args.http_timeout))
        .build()?;
    let base = run_target(
        "base",
        &case_path,
        args.base_http_port,
        &tables,
        &configured_queries,
        &client,
    )
    .await;
    let candidate = run_target(
        "candidate",
        &case_path,
        args.candidate_http_port,
        &tables,
        &configured_queries,
        &client,
    )
    .await;
    let thresholds = enforce_thresholds(&configured_queries, &base, &candidate)?;
    let status = if base.status == "failed"
        || candidate.status == "failed"
        || thresholds
            .iter()
            .any(|threshold| threshold["status"] == "failed")
    {
        "failed"
    } else {
        "ok"
    };
    let report = json!({
        "case_path": case_path,
        "case": case_metadata,
        "scenario": scenario_value,
        "queries": configured_queries,
        "query_mode": "endpoint",
        "http_timeout": args.http_timeout,
        "targets": [target_report("base", args.base_http_port, base), target_report("candidate", args.candidate_http_port, candidate)],
        "thresholds": thresholds,
        "status": status,
    });
    let text = format!("{}\n", serde_json::to_string_pretty(&report)?);
    if let Some(path) = args.output {
        fs::write(path, &text)?;
    }
    print!("{text}");
    if status == "failed" {
        std::process::exit(1);
    }
    Ok(())
}

fn target_report(name: &str, http_port: u16, result: QueryResult) -> Value {
    let status = if result.status == "ok" {
        "measured"
    } else {
        "failed"
    };
    json!({
        "name": name,
        "http_port": http_port,
        "validation": result.validation,
        "validation_errors": result.validation_errors,
        "measurements": result.measurements,
        "status": status,
    })
}

/// Routes a configured query to the right endpoint: `prom_http` queries hit
/// the Prometheus HTTP range API (which exercises the Prometheus JSON response
/// builder), everything else goes through `/v1/sql` (including `TQL ANALYZE`).
async fn post_query(client: &Client, port: u16, query: &Query, db: &str) -> Value {
    post_query_with_hints(client, port, query, db, required_hints(query).as_deref()).await
}

/// Posts one configured query, stating `hints` on the wire when the caller has
/// any. Every request the runner makes for a query goes through here, so a
/// `require_native` query states its hint on both targets.
async fn post_query_with_hints(
    client: &Client,
    port: u16,
    query: &Query,
    db: &str,
    hints: Option<&str>,
) -> Value {
    if query.kind.as_deref() == Some("prom_http") {
        http_post_prom_range_query(client, port, query, db, hints).await
    } else {
        http_post_sql_with_hints(client, port, &query.query, db, hints).await
    }
}

async fn run_target(
    target: &str,
    case_path: &Path,
    port: u16,
    tables: &[Table],
    configured_queries: &[Query],
    client: &Client,
) -> QueryResult {
    let mut queries = configured_queries.to_vec();
    if queries.is_empty() {
        queries.push(Query {
            name: Some("count_all".to_string()),
            kind: Some("sql".to_string()),
            query: format!("SELECT count(*) FROM {}", sql_ident(&tables[0].name)),
            start: None,
            end: None,
            step: None,
            warmup: 0,
            iterations: 1,
            thresholds: Map::new(),
            require_native: false,
        });
    }

    let db = &tables[0].database;
    let mut validation = Vec::new();
    let mut validation_errors = Vec::new();
    for table in tables {
        let sql = format!("SHOW CREATE TABLE {}", sql_ident(&table.name));
        let sample = http_post_sql(client, port, &sql, &table.database).await;
        if !sample["ok"].as_bool().unwrap_or(false) {
            validation_errors.push(json!({
                "sql": sql,
                "error": sample.get("error"),
                "response": sample.get("response"),
            }));
        } else {
            for error in validate_show_create(&sample, table) {
                validation_errors.push(json!({
                    "sql": sql,
                    "error": error,
                    "response": sample.get("response"),
                }));
            }
        }
        validation.push(sample);
    }
    let first = post_query(client, port, &queries[0], db).await;
    if !first["ok"].as_bool().unwrap_or(false) {
        validation_errors.push(json!({
            "sql": queries[0].query,
            "error": first.get("error"),
            "response": first.get("response"),
        }));
    }
    validation.push(first);

    let mut measurements = Vec::with_capacity(queries.len());
    for query in &queries {
        if query.require_native {
            // Asserted once per target per query, before warmup: a case routed to
            // the legacy planner fails in seconds and reports no latency.
            let outcome = probe_native_route(target, client, port, query, db).await;
            if !record_native_route_check(case_path, target, query, outcome, &mut validation_errors)
            {
                continue;
            }
        }
        for _ in 0..query.warmup {
            let warmup = post_query(client, port, query, db).await;
            if !warmup["ok"].as_bool().unwrap_or(false) {
                validation_errors.push(json!({
                    "sql": query.query,
                    "phase": "warmup",
                    "error": warmup.get("error"),
                    "response": warmup.get("response"),
                }));
            }
        }
        let mut samples = Vec::with_capacity(query.iterations);
        let mut good_latencies = Vec::with_capacity(query.iterations);
        for _ in 0..query.iterations {
            let mut sample = post_query(client, port, query, db).await;
            let execution_time = sample
                .get("response")
                .and_then(|response| {
                    extract_execution_time_for_kind(query.kind.as_deref(), response)
                })
                .cloned()
                .unwrap_or(Value::Null);
            sample
                .as_object_mut()
                .expect("HTTP samples are objects")
                .insert("execution_time_ms".to_string(), execution_time);
            if sample["ok"].as_bool().unwrap_or(false) {
                good_latencies.push(sample["latency_ms"].as_f64().unwrap_or_default());
            }
            samples.push(sample);
        }
        let median = (!good_latencies.is_empty()).then(|| median(&good_latencies));
        let p95 = (!good_latencies.is_empty()).then(|| percentile(&good_latencies, 95.0));
        let status = if good_latencies.len() == samples.len() {
            "ok"
        } else {
            "failed"
        };
        measurements.push(Measurement {
            name: query.name.clone(),
            kind: query.kind.clone(),
            iterations: samples.len(),
            samples,
            latency_ms_median: median,
            latency_ms_p95: p95,
            status: status.to_string(),
        });
    }
    let failed = !validation_errors.is_empty() || measurements.iter().any(|m| m.status == "failed");
    QueryResult {
        validation,
        validation_errors,
        measurements,
        status: if failed { "failed" } else { "ok" }.to_string(),
    }
}

/// What one failed native-route assertion observed on one target.
struct NativeRouteFailure {
    /// The plan tree that response carried, or the whole body when it carried none.
    observed: String,
    /// What went wrong, before the case, query, and target identification is added.
    reason: String,
}

/// Whether a statement is a `TQL ANALYZE ... FORMAT JSON` form, whose own response
/// carries the plan tree of the statement.
///
/// The statement is parsed rather than pattern-matched as text: only the parser
/// knows whether those keywords belong to the statement or to a comment, and only
/// `FORMAT JSON` makes the response a plan tree instead of plan text lines.
fn is_tql_plan_statement(statement: &str) -> bool {
    let Ok(mut statements) = ParserContext::create_with_dialect(
        statement,
        &GreptimeDbDialect {},
        ParseOptions::default(),
    ) else {
        return false;
    };
    statements.len() == 1
        && matches!(
            statements.pop(),
            Some(Statement::Tql(Tql::Analyze(analyze)))
                if analyze.format == Some(AnalyzeFormat::JSON)
        )
}

/// Whether a JSON plan tree names the native fused-series operator.
///
/// Only a node's own `name` counts. `param` holds the rest of that plan line,
/// where a label value or a filter can spell anything, so it is never read.
fn plan_tree_names_native(tree: &Value) -> bool {
    match tree {
        Value::Object(node) => {
            node.get("name")
                .and_then(Value::as_str)
                .is_some_and(is_native_operator_name)
                || node
                    .get("children")
                    .and_then(Value::as_array)
                    .is_some_and(|children| children.iter().any(plan_tree_names_native))
        }
        Value::Array(nodes) => nodes.iter().any(plan_tree_names_native),
        _ => false,
    }
}

/// Whether a plan node name is the native fused-series operator.
///
/// A name may carry the rest of its plan line (`FusedSeriesExec metrics=[]`), so
/// the operator has to match as a whole word.
fn is_native_operator_name(name: &str) -> bool {
    NATIVE_PROMQL_OPERATORS.iter().any(|operator| {
        name.strip_prefix(operator).is_some_and(|rest| {
            rest.chars()
                .next()
                .is_none_or(|character| !character.is_alphanumeric() && character != '_')
        })
    })
}

/// The name of a query in messages: its declared name, or its statement.
fn query_label(query: &Query) -> &str {
    query.name.as_deref().unwrap_or(&query.query)
}

/// Rejects `require_native` queries that cannot prove their plan, before either
/// target is contacted.
///
/// Only `TQL ANALYZE ... FORMAT JSON` answers the plan tree of the statement
/// itself, and a statement that is not the timed query cannot prove anything about
/// it.
fn validate_native_queries(case_path: &Path, queries: &[Query]) -> Result<()> {
    if let Some(query) = queries.iter().find(|query| {
        query.require_native
            && (query.kind.as_deref() == Some("prom_http") || !is_tql_plan_statement(&query.query))
    }) {
        return Err(format!(
            "case {}: query `{}` declares require_native but is not a `TQL ANALYZE ... FORMAT \
             JSON` request to `/v1/sql`; only that statement answers the plan tree of the query \
             being timed, so nothing else can prove the native route",
            case_path.display(),
            query_label(query),
        )
        .into());
    }
    Ok(())
}

/// Proves one `require_native` query runs on the native fused-series route on one
/// target.
///
/// The assertion runs the same statement one extra time before any timing sample,
/// with the same hint, and reads the plan tree out of that untimed response.
async fn probe_native_route(
    target: &str,
    client: &Client,
    port: u16,
    query: &Query,
    db: &str,
) -> std::result::Result<(), NativeRouteFailure> {
    let hints = required_hints(query);
    let sample = post_query_with_hints(client, port, query, db, hints.as_deref()).await;
    if !sample["ok"].as_bool().unwrap_or(false) {
        return Err(NativeRouteFailure {
            observed: sample
                .get("response")
                .map(response_text)
                .unwrap_or_default(),
            reason: request_failure_reason(target, &sample),
        });
    }
    native_route_verdict(&sample["response"]).map_err(|reason| NativeRouteFailure {
        observed: sample
            .get("response")
            .map(response_text)
            .unwrap_or_default(),
        reason,
    })
}

/// Whether one analyze response proves the native route, or why it does not.
fn native_route_verdict(body: &Value) -> std::result::Result<(), String> {
    const NOT_A_TREE: &str = "its response was not a JSON plan tree, so it names no plan node; \
                              the statement must be `TQL ANALYZE ... FORMAT JSON`";
    let Some(trees) = plan_trees_from_response(body) else {
        return Err(NOT_A_TREE.to_string());
    };
    if trees.iter().any(plan_tree_names_native) {
        Ok(())
    } else {
        Err(format!(
            "its plan tree names no {} node, so it took the legacy path",
            native_operator_list()
        ))
    }
}

/// Records one query's native-route outcome on one target.
///
/// Returns whether the target may time the query: a failed assertion records a
/// `validation_errors` entry and skips the query, so the target reports no
/// benchmark measurement for a query that ran on the legacy route.
fn record_native_route_check(
    case_path: &Path,
    target: &str,
    query: &Query,
    outcome: std::result::Result<(), NativeRouteFailure>,
    validation_errors: &mut Vec<Value>,
) -> bool {
    let Err(failure) = outcome else {
        return true;
    };
    let message = format!(
        "case {}: query `{}` on the {target} target must be planned on the native PromQL route, \
         but {}; expected a plan node named {}",
        case_path.display(),
        query_label(query),
        failure.reason,
        native_operator_list(),
    );
    validation_errors.push(json!({
        "sql": query.query,
        "error": message,
        "response": plan_excerpt(&failure.observed),
    }));
    false
}

/// The accepted plan node names, for a failure message.
fn native_operator_list() -> String {
    NATIVE_PROMQL_OPERATORS.join(" or ")
}

/// Explains a failed hint-bearing request, including the base-binary hazard: an
/// unknown `query.*` option is a hard error, so a target whose binary predates the
/// option fails here instead of silently planning the legacy route.
fn request_failure_reason(target: &str, sample: &Value) -> String {
    let status = match sample.get("status").and_then(Value::as_u64) {
        Some(status) => format!("HTTP {status}"),
        None => sample
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("no response")
            .to_string(),
    };
    format!(
        "the request failed ({status}); the {target} binary may predate \
         `{NATIVE_PROMQL_EXTENSION_KEY}`, and require_native needs a base that supports it"
    )
}

/// The beginning of a plan or response body, short enough for a report entry.
fn plan_excerpt(text: &str) -> String {
    const MAX_CHARS: usize = 1200;
    match text.char_indices().nth(MAX_CHARS) {
        Some((index, _)) => format!("{}…", &text[..index]),
        None => text.to_string(),
    }
}

fn validate_show_create(result: &Value, table: &Table) -> Vec<&'static str> {
    let text = result
        .get("response")
        .map(response_text)
        .unwrap_or_default()
        .to_lowercase();
    let mut errors = Vec::new();
    if !text.contains(&table.name.to_lowercase()) {
        errors.push("SHOW CREATE output does not contain table name");
    }
    if table.validate_show_create_engine && (!text.contains("engine") || !text.contains("mito")) {
        errors.push("SHOW CREATE output does not mention ENGINE=mito");
    }
    if table.append_mode.is_some() && !text.contains("append_mode") {
        errors.push("SHOW CREATE output does not mention append_mode");
    }
    if table.sst_format.is_some() && !text.contains("sst_format") {
        errors.push("SHOW CREATE output does not mention sst_format");
    }
    errors
}

fn response_text(body: &Value) -> String {
    body.as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| serde_json::to_string(body).unwrap_or_default())
}

fn extract_execution_time_for_kind<'a>(kind: Option<&str>, body: &'a Value) -> Option<&'a Value> {
    if kind == Some("prom_http") {
        None
    } else {
        extract_execution_time(body)
    }
}

fn extract_execution_time(body: &Value) -> Option<&Value> {
    match body {
        Value::Object(map) => {
            for key in ["execution_time_ms", "execution_time", "elapsed"] {
                if let Some(value) = map.get(key) {
                    return Some(value);
                }
            }
            map.values().find_map(extract_execution_time)
        }
        Value::Array(values) => values.iter().find_map(extract_execution_time),
        _ => None,
    }
}

pub(super) fn median(values: &[f64]) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let middle = ordered.len() / 2;
    if ordered.len().is_multiple_of(2) {
        (ordered[middle - 1] + ordered[middle]) / 2.0
    } else {
        ordered[middle]
    }
}

fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let index = round_ties_even((pct / 100.0) * (ordered.len() - 1) as f64)
        .clamp(0, ordered.len() as isize - 1) as usize;
    ordered[index]
}

pub(super) fn round_ties_even(value: f64) -> isize {
    let floor = value.floor();
    let fraction = value - floor;
    if fraction < 0.5 {
        floor as isize
    } else if fraction > 0.5 {
        floor as isize + 1
    } else if (floor as isize) % 2 == 0 {
        floor as isize
    } else {
        floor as isize + 1
    }
}

fn enforce_thresholds(
    queries: &[Query],
    base: &QueryResult,
    candidate: &QueryResult,
) -> Result<Vec<Value>> {
    let base_by_name: HashMap<_, _> = base
        .measurements
        .iter()
        .map(|measurement| (measurement.name.as_deref(), measurement))
        .collect();
    let mut results = Vec::new();
    for candidate_measurement in &candidate.measurements {
        let query = queries
            .iter()
            .find(|query| query.name == candidate_measurement.name);
        let thresholds = query.map_or_else(Map::new, |query| query.thresholds.clone());
        let base_measurement = base_by_name.get(&candidate_measurement.name.as_deref());
        if let Some(limit) = thresholds
            .get("max_candidate_latency_regression_pct")
            .filter(|value| !value.is_null())
            .map(value_as_f64)
            .transpose()?
        {
            let result = match base_measurement {
                None => {
                    json!({"query": candidate_measurement.name, "threshold": "max_candidate_latency_regression_pct", "status": "failed", "reason": "missing base measurement"})
                }
                Some(base) if matches!(base.latency_ms_median, None | Some(0.0)) => {
                    json!({"query": candidate_measurement.name, "threshold": "max_candidate_latency_regression_pct", "status": "failed", "reason": "base median latency is missing or zero", "base_latency_ms_median": base.latency_ms_median})
                }
                Some(_) if candidate_measurement.latency_ms_median.is_none() => {
                    json!({"query": candidate_measurement.name, "threshold": "max_candidate_latency_regression_pct", "status": "failed", "reason": "missing candidate measurement"})
                }
                Some(base) => {
                    let actual = (candidate_measurement.latency_ms_median.unwrap()
                        - base.latency_ms_median.unwrap())
                        / base.latency_ms_median.unwrap()
                        * 100.0;
                    json!({"query": candidate_measurement.name, "threshold": "max_candidate_latency_regression_pct", "status": if actual <= limit { "passed" } else { "failed" }, "actual_pct": actual, "limit_pct": limit})
                }
            };
            results.push(result);
        }
        for key in thresholds
            .keys()
            .filter(|key| *key != "max_candidate_latency_regression_pct")
        {
            results.push(json!({"query": candidate_measurement.name, "threshold": key, "status": "failed", "reason": "unsupported threshold"}));
        }
    }
    Ok(results)
}

fn value_as_f64(value: &Value) -> Result<f64> {
    value.as_f64().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "threshold must be numeric").into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_and_p95_match_python() {
        assert_eq!(median(&[1.0, 8.0, 3.0, 4.0]), 3.5);
        assert_eq!(
            percentile(
                &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                95.0
            ),
            10.0
        );
        assert_eq!(percentile(&[], 95.0), 0.0);
    }

    #[test]
    fn finds_nested_execution_time_in_priority_order() {
        let body = json!({"output": [{"elapsed": 3}], "execution_time": 2});
        assert_eq!(extract_execution_time(&body), Some(&json!(2)));
        assert_eq!(
            extract_execution_time(&json!({"output": [{"elapsed": 3}]})),
            Some(&json!(3))
        );
    }

    #[test]
    fn prom_http_does_not_extract_execution_time_from_response() {
        let response = json!({
            "data": {
                "result": [{"metric": {"job": "api", "elapsed": "label"}}],
                "elapsed": 42
            }
        });
        assert_eq!(
            extract_execution_time_for_kind(Some("prom_http"), &response),
            None
        );
        assert_eq!(
            extract_execution_time_for_kind(Some("sql"), &response),
            Some(&json!(42))
        );
    }

    #[test]
    fn threshold_rejects_unknown_keys_and_zero_base() {
        let query = Query {
            name: Some("q".to_string()),
            kind: None,
            query: "SELECT 1".to_string(),
            start: None,
            end: None,
            step: None,
            warmup: 0,
            iterations: 1,
            thresholds: Map::from_iter([
                ("max_candidate_latency_regression_pct".to_string(), json!(0)),
                ("other".to_string(), json!(1)),
            ]),
            require_native: false,
        };
        let measurement = |median| Measurement {
            name: Some("q".to_string()),
            kind: None,
            iterations: 1,
            samples: vec![],
            latency_ms_median: median,
            latency_ms_p95: median,
            status: "ok".to_string(),
        };
        let base = QueryResult {
            validation: vec![],
            validation_errors: vec![],
            measurements: vec![measurement(Some(0.0))],
            status: "ok".to_string(),
        };
        let candidate = QueryResult {
            validation: vec![],
            validation_errors: vec![],
            measurements: vec![measurement(Some(1.0))],
            status: "ok".to_string(),
        };
        let results = enforce_thresholds(&[query], &base, &candidate).unwrap();
        assert_eq!(
            results[0]["reason"],
            "base median latency is missing or zero"
        );
        assert_eq!(results[1]["reason"], "unsupported threshold");
    }

    fn native_query(kind: Option<&str>, query: &str) -> Query {
        let range_parameter =
            |value: &'static str| (kind == Some("prom_http")).then(|| value.to_string());
        Query {
            name: Some("native_range".to_string()),
            kind: kind.map(ToString::to_string),
            query: query.to_string(),
            start: range_parameter("1704067200"),
            end: range_parameter("1704070800"),
            step: range_parameter("15s"),
            warmup: 0,
            iterations: 1,
            thresholds: Map::new(),
            require_native: true,
        }
    }

    /// An `ANALYZE ... FORMAT JSON` response body in the shape `format=json`
    /// answers: one object per row keyed by column name.
    fn analyze_body(cells: &[&str]) -> Value {
        json!({
            "data": cells
                .iter()
                .enumerate()
                .map(|(node, cell)| json!({"stage": 0, "node": node, "plan": cell}))
                .collect::<Vec<_>>(),
            "execution_time_ms": 3,
        })
    }

    /// One plan tree node, as `ANALYZE ... FORMAT JSON` prints it.
    fn plan_node(name: &str, param: &str, children: Vec<Value>) -> Value {
        json!({
            "name": name,
            "param": param,
            "output_rows": 4,
            "elapsed_compute": 7,
            "metrics": {},
            "children": children,
        })
    }

    /// The evidence pipeline [`probe_native_route`] applies to one response body.
    fn plans_native(body: &Value) -> bool {
        plan_trees_from_response(body).is_some_and(|trees| trees.iter().any(plan_tree_names_native))
    }

    #[test]
    fn native_plan_classification_reads_the_tree_node_names() {
        // The physical operator as its own node, and the logical extension node as
        // a node an operator runs inside itself. The analyze result always ends
        // with a plain-text `Total rows` cell, which is not evidence either way.
        let fused = plan_node(
            "FusedSeriesExec",
            "metric=some_metric",
            vec![plan_node("SeriesScan", "region=0", vec![])],
        )
        .to_string();
        let repartition = plan_node("SeriesRepartitionExec", "", vec![]).to_string();
        assert!(plans_native(&analyze_body(&[
            &fused,
            &repartition,
            "Total rows: 580"
        ])));

        let logical = plan_node(
            "CooperativeExec",
            "",
            vec![plan_node("FusedSeries", "metric=some_metric", vec![])],
        )
        .to_string();
        assert!(plans_native(&analyze_body(&[&logical, "Total rows: 4"])));
        assert!(
            native_route_verdict(&analyze_body(&[&fused, &repartition, "Total rows: 580"])).is_ok()
        );

        // A name may carry the rest of its plan line.
        let named_with_metrics =
            plan_node("FusedSeriesExec metrics=[]", "metric=m", vec![]).to_string();
        assert!(plans_native(&analyze_body(&[
            &named_with_metrics,
            "Total rows: 4"
        ])));

        // A legacy tree names no fused node at any depth.
        let legacy = plan_node(
            "PromInstantManipulateExec",
            "range=[0..10000]",
            vec![plan_node(
                "PromSeriesDivideExec",
                "tags=[\"host\"]",
                vec![plan_node("MergeScanExec", "peers=[]", vec![])],
            )],
        )
        .to_string();
        let body = analyze_body(&[&legacy, "Total rows: 580"]);
        assert!(!plans_native(&body));
        let reason = native_route_verdict(&body).unwrap_err();
        assert!(
            reason.contains("names no FusedSeries or FusedSeriesExec node"),
            "{reason}"
        );
    }

    #[test]
    fn native_plan_classification_never_reads_the_param_of_a_node() {
        // The marker in a node's `param` is the rest of a plan line: a filter or a
        // label value can spell it, so it is not evidence of the operator.
        let filtered =
            plan_node("TableScan", "m, filter=FusedSeries IS NOT NULL", vec![]).to_string();
        assert!(!plans_native(&analyze_body(&[&filtered, "Total rows: 4"])));

        let filtered_exec = plan_node(
            "Filter",
            "FusedSeriesExec = 1",
            vec![plan_node("SeriesScan", "metric=FusedSeries_x", vec![])],
        )
        .to_string();
        assert!(!plans_native(&analyze_body(&[
            &filtered_exec,
            "Total rows: 4"
        ])));

        // A name that only starts with the operator text is a different node.
        let similar = plan_node("FusedSeriesExtraExec", "metric=some_metric", vec![]).to_string();
        assert!(!plans_native(&analyze_body(&[&similar, "Total rows: 4"])));
    }

    #[test]
    fn native_plan_classification_fails_closed_without_a_plan_tree() {
        // A text plan cell is not a tree, so it proves no node; the same holds for
        // a body without a readable `plan` column.
        let text_only = json!({"data": [{"plan": "Total rows: 1"}]});
        assert!(plan_trees_from_response(&text_only).is_none());
        assert!(!plans_native(&text_only));
        let reason = native_route_verdict(&text_only).unwrap_err();
        assert!(reason.contains("was not a JSON plan tree"), "{reason}");
        assert!(reason.contains("TQL ANALYZE ... FORMAT JSON"), "{reason}");
        assert!(!plans_native(
            &json!({"data": [{"plan": "CooperativeExec\n  FusedSeriesExec"}]})
        ));
        assert!(!plans_native(&json!({"data": [{"note": "FusedSeries"}]})));
        assert!(!plans_native(&json!({"data": []})));
        assert!(!plans_native(
            &json!({"output": [{"rows": [["FusedSeries"]]}]})
        ));
    }

    #[test]
    fn require_native_query_must_be_a_tql_analyze_format_json_statement() {
        let case_path = Path::new("tests/perf/query_cases/prom_native/case.toml");
        for (kind, statement) in [
            (Some("prom_http"), "rate(some_metric[5m])"),
            (Some("sql"), "TQL EVAL (0, 10, '5s') some_metric"),
            (Some("sql"), "SELECT count(*) FROM t"),
            // The concrete false pass: a commented-out keyword in front of a
            // statement that echoes the marker.
            (
                Some("sql"),
                "/* TQL ANALYZE FORMAT JSON */ SELECT 'FusedSeries' AS plan",
            ),
            // The statement parses, but this kind sends it to the Prometheus
            // range endpoint, which answers no plan tree.
            (
                Some("prom_http"),
                "TQL ANALYZE FORMAT JSON (0, 10, '5s') some_metric",
            ),
        ] {
            let error = validate_native_queries(case_path, &[native_query(kind, statement)])
                .expect_err("require_native outside the JSON analyze form must be rejected")
                .to_string();
            assert!(error.contains("prom_native/case.toml"), "{error}");
            assert!(error.contains("native_range"), "{error}");
            assert!(error.contains("TQL ANALYZE ... FORMAT JSON"), "{error}");
        }

        for statement in [
            "TQL ANALYZE VERBOSE FORMAT JSON (0, 10, '5s') some_metric",
            "TQL ANALYZE FORMAT JSON (0, 10, '5s') some_metric",
            "tql analyze format json (0, 10, '5s') some_metric",
        ] {
            assert!(
                validate_native_queries(case_path, &[native_query(Some("sql"), statement)]).is_ok(),
                "{statement}"
            );
        }
    }

    #[test]
    fn tql_plan_statement_requires_the_json_format() {
        for statement in [
            "TQL ANALYZE VERBOSE FORMAT JSON (0, 10, '5s') some_metric",
            "TQL ANALYZE FORMAT JSON (0, 10, '5s') some_metric",
        ] {
            assert!(is_tql_plan_statement(statement), "{statement:?}");
        }
        for statement in [
            // The text form answers plan lines, which a label value can imitate.
            "TQL ANALYZE VERBOSE (0, 10, '5s') some_metric",
            "TQL ANALYZE (0, 10, '5s') some_metric",
            "TQL ANALYZE FORMAT TEXT (0, 10, '5s') some_metric",
            "TQL EXPLAIN VERBOSE FORMAT JSON (0, 10, '5s') some_metric",
            "TQL EVAL (0, 10, '5s') some_metric",
            "SELECT 'TQL ANALYZE FORMAT JSON'",
            "ANALYZE TABLE t",
            "TQL",
        ] {
            assert!(!is_tql_plan_statement(statement), "{statement:?}");
        }
    }

    #[test]
    fn failed_native_route_check_records_a_validation_error_and_skips_the_query() {
        let case_path = Path::new("tests/perf/query_cases/prom_native/case.toml");
        let query = native_query(
            Some("sql"),
            "TQL ANALYZE FORMAT JSON (0, 10, '5s') rate(some_metric[5m])",
        );
        let mut validation_errors = Vec::new();
        let failure = NativeRouteFailure {
            observed: "CooperativeExec\n  MergeScanExec: peers=[]".to_string(),
            reason: "its plan tree names no FusedSeries or FusedSeriesExec node, so it took \
                     the legacy path"
                .to_string(),
        };

        let measure = record_native_route_check(
            case_path,
            "candidate",
            &query,
            Err(failure),
            &mut validation_errors,
        );

        // The query is skipped, so no `Measurement` is pushed for it.
        assert!(!measure);
        assert_eq!(validation_errors.len(), 1);
        let entry = &validation_errors[0];
        assert_eq!(
            entry["sql"],
            "TQL ANALYZE FORMAT JSON (0, 10, '5s') rate(some_metric[5m])"
        );
        assert_eq!(
            entry["response"],
            "CooperativeExec\n  MergeScanExec: peers=[]"
        );
        let message = entry["error"].as_str().unwrap();
        assert!(message.contains("prom_native/case.toml"), "{message}");
        assert!(message.contains("native_range"), "{message}");
        assert!(message.contains("candidate"), "{message}");
        assert!(
            message.contains("FusedSeries or FusedSeriesExec"),
            "{message}"
        );
        // The observed plan stays in `response`, like the other entries in this file.
        assert!(
            entry["response"]
                .as_str()
                .unwrap()
                .contains("MergeScanExec"),
            "{entry}"
        );

        // A passing check keeps the query and records nothing.
        let mut validation_errors = Vec::new();
        assert!(record_native_route_check(
            case_path,
            "base",
            &query,
            Ok(()),
            &mut validation_errors
        ));
        assert!(validation_errors.is_empty());
    }

    #[test]
    fn failed_hint_request_names_the_base_binary_hazard() {
        let rejected = json!({
            "ok": false,
            "status": 400,
            "latency_ms": 1.0,
            "error": "HTTP 400",
            "response": {"error": "Unknown or unsupported query option: query.experimental_native_promql"},
        });
        let reason = request_failure_reason("base", &rejected);
        assert!(reason.contains("HTTP 400"), "{reason}");
        assert!(reason.contains("may predate"), "{reason}");
        assert!(
            reason.contains("query.experimental_native_promql"),
            "{reason}"
        );
        assert!(
            reason.contains("require_native needs a base that supports it"),
            "{reason}"
        );

        let unreachable = json!({"ok": false, "status": null, "error": "connection refused"});
        assert!(request_failure_reason("candidate", &unreachable).contains("connection refused"));
    }

    #[test]
    fn plan_excerpt_caps_the_character_count() {
        let plan = "n".repeat(1_300);
        let excerpt = plan_excerpt(&plan);
        assert_eq!(excerpt.chars().count(), 1_201);
        assert!(excerpt.ends_with('…'));
        assert_eq!(plan_excerpt("short"), "short");
    }
}
