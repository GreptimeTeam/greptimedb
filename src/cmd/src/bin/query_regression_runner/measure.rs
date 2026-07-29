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
use crate::query_regression_runner::sql::{http_post_sql, sql_ident};
use crate::query_regression_runner::{MeasureArgs, Result};

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
    let base = run_target(
        "base",
        args.base_http_port,
        &tables,
        &configured_queries,
        &client,
    )
    .await;
    let candidate = run_target(
        "candidate",
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

async fn run_target(
    _name: &str,
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
            warmup: 0,
            iterations: 1,
            thresholds: Map::new(),
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
    let first = http_post_sql(client, port, &queries[0].query, db).await;
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
            let warmup = http_post_sql(client, port, &query.query, db).await;
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
            let mut sample = http_post_sql(client, port, &query.query, db).await;
            let execution_time = sample
                .get("response")
                .and_then(extract_execution_time)
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
    fn threshold_rejects_unknown_keys_and_zero_base() {
        let query = Query {
            name: Some("q".to_string()),
            kind: None,
            query: "SELECT 1".to_string(),
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
