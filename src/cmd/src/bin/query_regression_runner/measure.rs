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
use std::time::Duration;
use std::{fs, io};

use reqwest::Client;
use serde_json::{Map, Value, json};

use crate::query_regression_runner::model::{Measurement, Query, QueryResult, Scenario, Table};
use crate::query_regression_runner::plan::{load_plan, normalize_scenario};
use crate::query_regression_runner::sql::{
    extract_rows, http_post_multi_statement_sql, http_post_prom_range_query, http_post_sql,
    row_u64, sql_ident, sql_string,
};
use crate::query_regression_runner::{MeasureArgs, Result};

/// How long the measurement of a direct-SST case waits for the region statistics of its tables
/// to settle before it measures anything.
///
/// Datanodes report the role and the size of their regions on every heartbeat (`interval` of the
/// heartbeat options, three seconds by default) and a region is reported as a leader with a
/// non-zero size only after the metasrv granted its lease and the datanode applied the reported
/// role. A rewrite that prices tables by those statistics, like the nested broadcast join, sees
/// nothing before that, so a case would measure the un-rewritten plan without noticing.
const REGION_STATS_TIMEOUT: Duration = Duration::from_secs(60);
/// Poll interval of [`wait_for_region_statistics`].
const REGION_STATS_POLL_INTERVAL: Duration = Duration::from_secs(1);

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

    let client = Client::builder()
        .timeout(Duration::from_secs_f64(args.http_timeout))
        .build()?;
    let direct_fixture = matches!(scenario_kind(&plan), Some("direct_readable_sst"));
    let base = run_target(
        args.base_http_port,
        &tables,
        &configured_queries,
        &client,
        false,
        direct_fixture,
    )
    .await;
    let candidate_target = run_target(
        args.candidate_http_port,
        &tables,
        &configured_queries,
        &client,
        true,
        direct_fixture,
    )
    .await;
    let thresholds = enforce_thresholds(&configured_queries, &base, &candidate_target)?;
    let status = if base.status == "failed"
        || candidate_target.status == "failed"
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
        "targets": [target_report("base", args.base_http_port, base), target_report("candidate", args.candidate_http_port, candidate_target)],
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
async fn post_query(client: &Client, port: u16, query: &Query, db: &str, candidate: bool) -> Value {
    if query.kind.as_deref() == Some("prom_http") {
        http_post_prom_range_query(
            client,
            port,
            &query.query,
            query.start.as_deref(),
            query.end.as_deref(),
            query.step.as_deref(),
            db,
        )
        .await
    } else {
        let sql = query.request_sql(candidate);
        if sql == query.query {
            http_post_sql(client, port, &sql, db).await
        } else {
            // Statements of one request share the session context of that request, which is how
            // a session setting such as `SET experimental_dist_join = true` reaches the query of
            // the same request.
            http_post_multi_statement_sql(client, port, &sql, db).await
        }
    }
}

/// The scenario kind of the normalized plan, if it carries one.
fn scenario_kind(plan: &Value) -> Option<&str> {
    plan.get("scenario")?.get("kind")?.as_str()
}

/// The region statistics of `table`: `(regions, ready_regions)`, where a ready region has an
/// ordinary leader report with a non-zero disk size.
///
/// This is the shape a rewrite that prices tables by region statistics needs, e.g. the cost
/// heuristic of the nested broadcast join: a region that has not reported, reports zero bytes, or
/// is not served by a leader makes the statistics of its table unusable.
async fn region_statistics(client: &Client, port: u16, db: &str, table: &str) -> Value {
    let sql = format!(
        "SELECT count(*) AS regions, \
                sum(CASE WHEN region_role = 'Leader' AND disk_size > 0 THEN 1 ELSE 0 END) \
                    AS ready_regions \
         FROM information_schema.region_statistics \
         WHERE table_id = (SELECT table_id FROM information_schema.tables \
                           WHERE table_schema = {} AND table_name = {})",
        sql_string(db),
        sql_string(table)
    );
    http_post_sql(client, port, &sql, db).await
}

/// Waits until every table of a direct-SST case has one leader report with a non-zero size per
/// region, and returns the samples of the last round plus an error sample when the statistics did
/// not settle in time.
///
/// The measurement of a case that depends on those statistics would otherwise start before the
/// datanode reported them and silently measure the plan the case is not about.
async fn wait_for_region_statistics(
    client: &Client,
    port: u16,
    tables: &[Table],
) -> (Vec<Value>, Option<Value>) {
    let deadline = tokio::time::Instant::now() + REGION_STATS_TIMEOUT;
    loop {
        let mut samples = Vec::with_capacity(tables.len());
        let mut pending = Vec::new();
        for table in tables {
            let sample = region_statistics(client, port, &table.database, &table.name).await;
            match sample.get("response").and_then(parse_region_statistics) {
                Some((regions, ready)) if regions > 0 && ready == regions => {}
                Some((regions, ready)) => pending.push(json!({
                    "table": table.name,
                    "regions": regions,
                    "ready_regions": ready,
                })),
                None => {
                    return (
                        samples,
                        Some(json!({
                            "sql": "information_schema.region_statistics",
                            "phase": "region_statistics",
                            "table": table.name,
                            "error": "region statistics query failed",
                            "response": sample.get("response"),
                        })),
                    );
                }
            }
            samples.push(with_table(sample, table));
        }
        if pending.is_empty() {
            return (samples, None);
        }
        if tokio::time::Instant::now() >= deadline {
            return (
                samples,
                Some(json!({
                    "sql": "information_schema.region_statistics",
                    "phase": "region_statistics",
                    "error": format!(
                        "region statistics did not settle within {}s",
                        REGION_STATS_TIMEOUT.as_secs()
                    ),
                    "pending": pending,
                })),
            );
        }
        tokio::time::sleep(REGION_STATS_POLL_INTERVAL).await;
    }
}

fn with_table(mut sample: Value, table: &Table) -> Value {
    if let Some(object) = sample.as_object_mut() {
        object.insert("table".to_string(), Value::String(table.name.clone()));
    }
    sample
}

/// The `(regions, ready_regions)` row of a region statistics query.
fn parse_region_statistics(body: &Value) -> Option<(u64, u64)> {
    let rows = extract_rows(body);
    let row = rows.first()?;
    Some((
        row_u64(row, 0, "regions").ok()?,
        row_u64(row, 1, "ready_regions").ok()?,
    ))
}

async fn run_target(
    port: u16,
    tables: &[Table],
    configured_queries: &[Query],
    client: &Client,
    candidate: bool,
    wait_for_region_stats: bool,
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
            candidate_session_sql: Vec::new(),
            warmup: 0,
            iterations: 1,
            thresholds: Map::new(),
        });
    }

    let db = &tables[0].database;
    let mut validation = Vec::new();
    let mut validation_errors = Vec::new();
    if wait_for_region_stats {
        let (stats, error) = wait_for_region_statistics(client, port, tables).await;
        if let Some(error) = error {
            validation_errors.push(error);
        }
        validation.extend(stats);
    }
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
    let first = post_query(client, port, &queries[0], db, candidate).await;
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
        for _ in 0..query.warmup {
            let warmup = post_query(client, port, query, db, candidate).await;
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
            let mut sample = post_query(client, port, query, db, candidate).await;
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
    fn parses_region_statistics_counts() {
        let body = json!({
            "output": [{
                "records": {
                    "schema": {"column_schemas": [
                        {"name": "regions", "data_type": "UInt64"},
                        {"name": "ready_regions", "data_type": "UInt64"}
                    ]},
                    "rows": [[1, 1]],
                    "total_rows": 1
                }
            }]
        });
        assert_eq!(Some((1, 1)), parse_region_statistics(&body));
        assert_eq!(
            Some((2, 0)),
            parse_region_statistics(&json!({"data": [[2, 0]]}))
        );
        // A round without a row describes no regions and never settles.
        assert_eq!(
            None,
            parse_region_statistics(&json!({"output": [{"records": {"rows": []}}]}))
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
            candidate_session_sql: Vec::new(),
            warmup: 0,
            iterations: 1,
            thresholds: Map::from_iter([
                ("max_candidate_latency_regression_pct".to_string(), json!(0)),
                ("other".to_string(), json!(1)),
            ]),
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
}
