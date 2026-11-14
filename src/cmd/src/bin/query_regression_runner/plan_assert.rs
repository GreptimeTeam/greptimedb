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

//! Plan-text and scan-metric assertions for `EXPLAIN ANALYZE VERBOSE` queries.
//!
//! Latency thresholds alone cannot tell an optimization that fired from one
//! that silently stopped applying: a candidate without dynamic-membership
//! pushdown still returns the same rows, just after reading much more data.
//! Cases therefore declare, per query:
//!
//! * `[scenario.queries.plan_checks]` — `require` / `forbid` regular
//!   expressions over the analyze plan rows.
//! * `[scenario.queries.thresholds]` — absolute upper bounds on scan metrics
//!   such as `page_size_to_fetch_store` and `rows_before_filter`.
//!
//! All assertions are candidate-only. The base target is the control: it may
//! legitimately lack the optimization, so gating on it would make every
//! feature-introducing PR fail. Base values are still reported on each
//! assertion result as diagnostics.
//!
//! Metric assertions evaluate the maximum observed value across the cold
//! pre-warmup execution and every measured iteration. Mito page reads are
//! served from the page cache once a scan has run, so an assertion that only
//! looked at warm iterations (`page_size_to_fetch_store` would then be absent)
//! would pass no matter which plan shape the candidate used.

use std::collections::HashMap;

use regex::Regex;
use serde_json::{Value, json};

use crate::query_regression_runner::Result;
use crate::query_regression_runner::measure::value_as_f64;
use crate::query_regression_runner::model::{Measurement, PlanChecks, Query, QueryResult};
use crate::query_regression_runner::sql::{extract_rows, row_value};

/// Query threshold key -> scan metric name reported inside the plan text.
///
/// The metric value is the total of every matching `"<name>": <value>` entry in
/// the plan rows, aggregated over all region-scan partitions and regions.
pub(super) const SCAN_METRIC_THRESHOLDS: &[(&str, &str)] = &[
    (
        "max_candidate_page_size_to_fetch_store_bytes",
        "page_size_to_fetch_store",
    ),
    ("max_candidate_num_sst_rows", "num_sst_rows"),
    ("max_candidate_rows_before_filter", "rows_before_filter"),
];

/// A validated plan assertion: one regular expression and the rule it encodes.
#[derive(Debug)]
pub(super) struct PlanPattern {
    pub(super) threshold: &'static str,
    pub(super) pattern: String,
    regex: Regex,
}

/// Compiles the query's plan assertions, failing on invalid regular expressions
/// instead of silently ignoring a broken gate.
pub(super) fn compile_plan_checks(query: &Query) -> Result<Vec<PlanPattern>> {
    let name = query.name.as_deref().unwrap_or("<unnamed query>");
    let PlanChecks { require, forbid } = &query.plan_checks;
    let mut patterns = Vec::with_capacity(require.len() + forbid.len());
    for (threshold, configured) in [("plan_require", require), ("plan_forbid", forbid)] {
        for pattern in configured {
            let regex = Regex::new(pattern).map_err(|error| {
                format!("query {name}: invalid {threshold} pattern {pattern:?}: {error}")
            })?;
            patterns.push(PlanPattern {
                threshold,
                pattern: pattern.clone(),
                regex,
            });
        }
    }
    Ok(patterns)
}

/// Returns the plan rows of an `EXPLAIN ANALYZE VERBOSE` response joined with
/// newlines, or `None` when the response carries no plan text at all.
///
/// `/v1/sql?format=json` returns one object per analyze row with `stage`,
/// `node`, and `plan` fields; other shapes return the same rows as arrays. When
/// no row can be recognized, the whole response is serialized as a fallback so
/// nested plan text is still matched by the caller's patterns.
pub(super) fn plan_text(sample: &Value) -> Option<String> {
    let response = sample.get("response").unwrap_or(sample);
    let mut rows = Vec::new();
    for row in extract_rows(response) {
        match &row {
            Value::Object(_) => {
                if let Some(plan) = row_value(&row, 2, "plan") {
                    push_text(plan, &mut rows);
                }
            }
            Value::Array(values) => match row_value(&row, 2, "plan") {
                Some(plan) => push_text(plan, &mut rows),
                None => {
                    for value in values {
                        push_text(value, &mut rows);
                    }
                }
            },
            value => push_text(value, &mut rows),
        }
    }
    if rows.is_empty() {
        // An empty response carries no plan; do not turn it into a matchable "{}".
        let fallback = match response {
            Value::String(text) => text.clone(),
            Value::Null => String::new(),
            other if other.as_object().is_some_and(|object| object.is_empty()) => String::new(),
            other if other.as_array().is_some_and(|array| array.is_empty()) => String::new(),
            other => serde_json::to_string(other)
                .unwrap_or_default()
                .replace("\\\"", "\""),
        };
        if !fallback.is_empty() {
            rows.push(fallback);
        }
    }
    let text = rows.join("\n");
    (!text.is_empty()).then_some(text)
}

fn push_text(value: &Value, rows: &mut Vec<String>) {
    match value {
        Value::String(text) if !text.is_empty() => rows.push(text.clone()),
        Value::Array(values) => {
            for value in values {
                push_text(value, rows);
            }
        }
        _ => {}
    }
}

/// Totals one scan metric across the plan text.
///
/// Region scan metrics are JSON fragments embedded in the plan rows, for
/// example `"page_size_to_fetch_store":1234` inside `fetch_metrics`. Every
/// occurrence is summed, so the value covers all partitions and regions of the
/// query. Returns `None` when the metric does not appear, which is different
/// from an explicit zero.
pub(super) fn scan_metric_total(plan: &str, name: &str) -> Option<u64> {
    let pattern = format!(r#""{}"\s*:\s*(\d+)"#, regex::escape(name));
    let regex = Regex::new(&pattern).expect("generated metric regex is valid");
    let mut total = None;
    for captures in regex.captures_iter(plan) {
        let value = captures
            .get(1)
            .and_then(|value| value.as_str().parse::<u64>().ok());
        if let Some(value) = value {
            *total.get_or_insert(0) += value;
        }
    }
    total
}

/// Extracts the plan text used for pattern assertions: the last sample that has
/// one, falling back to the cold pre-warmup execution.
pub(super) fn assertion_plan_text(
    measurement: &Measurement,
    first_execution: Option<&Value>,
) -> Option<String> {
    measurement
        .samples
        .iter()
        .rev()
        .find_map(plan_text)
        .or_else(|| first_execution.and_then(plan_text))
}

/// Every plan text a metric assertion must consider: the cold execution plus
/// all measured iterations.
pub(super) fn metric_plan_texts(
    measurement: &Measurement,
    first_execution: Option<&Value>,
) -> Vec<String> {
    first_execution
        .and_then(plan_text)
        .into_iter()
        .chain(measurement.samples.iter().filter_map(plan_text))
        .collect()
}

/// Returns the cold pre-warmup execution for `name`.
///
/// Only the first configured query is executed before warmup, so the cold
/// sample belongs to that query and to no other.
fn first_execution_for<'a>(
    result: &'a QueryResult,
    name: Option<&str>,
    first_query_name: Option<&str>,
) -> Option<&'a Value> {
    (name == first_query_name)
        .then_some(result.first_execution.as_ref())
        .flatten()
}

/// Evaluates the candidate's plan and scan-metric assertions.
///
/// The returned entries use the same `status` vocabulary as the latency
/// thresholds, so `run_measure` fails the case on any `failed` entry.
pub(super) fn enforce_plan_assertions(
    queries: &[Query],
    base: &QueryResult,
    candidate: &QueryResult,
) -> Result<Vec<Value>> {
    let base_by_name: HashMap<_, _> = base
        .measurements
        .iter()
        .map(|measurement| (measurement.name.as_deref(), measurement))
        .collect();
    let first_query_name = queries.first().and_then(|query| query.name.as_deref());
    let base_plan_text = |measurement_name: Option<&str>| -> Option<String> {
        let measurement = base_by_name.get(&measurement_name)?;
        assertion_plan_text(
            measurement,
            first_execution_for(base, measurement_name, first_query_name),
        )
    };

    let mut results = Vec::new();
    for measurement in &candidate.measurements {
        let Some(query) = queries.iter().find(|query| query.name == measurement.name) else {
            continue;
        };
        let first_execution =
            first_execution_for(candidate, measurement.name.as_deref(), first_query_name);
        let plan = assertion_plan_text(measurement, first_execution);
        for pattern in compile_plan_checks(query)? {
            let matches = plan
                .as_deref()
                .map_or(0, |plan| pattern.regex.find_iter(plan).count() as u64);
            // A missing plan cannot verify any assertion, including a forbid
            // rule: report a failure instead of letting `0 matches` pass.
            let passed = plan.is_some()
                && if pattern.threshold == "plan_require" {
                    matches >= 1
                } else {
                    matches == 0
                };
            let mut entry = json!({
                "query": measurement.name,
                "target": "candidate",
                "threshold": pattern.threshold,
                "pattern": pattern.pattern,
                "status": if passed { "passed" } else { "failed" },
                "actual": matches,
                "limit": u64::from(pattern.threshold == "plan_require"),
            });
            if plan.is_none() {
                entry["reason"] = json!("analyze response contains no plan text");
            } else if !passed && pattern.threshold == "plan_forbid" {
                entry["reason"] = json!("forbidden plan pattern matched");
            } else if !passed {
                entry["reason"] = json!("required plan pattern not found");
            }
            if let Some(base_matches) = base_plan_text(measurement.name.as_deref())
                .map(|plan| pattern.regex.find_iter(&plan).count() as u64)
            {
                entry["base_actual"] = json!(base_matches);
            }
            results.push(entry);
        }
        for (threshold, metric) in SCAN_METRIC_THRESHOLDS {
            let Some(limit) = query
                .thresholds
                .get(*threshold)
                .filter(|value| !value.is_null())
                .map(value_as_f64)
                .transpose()?
            else {
                continue;
            };
            let actual = metric_plan_texts(measurement, first_execution)
                .iter()
                .filter_map(|plan| scan_metric_total(plan, metric))
                .max();
            let base_actual =
                base_by_name
                    .get(&measurement.name.as_deref())
                    .and_then(|base_measurement| {
                        metric_plan_texts(
                            base_measurement,
                            first_execution_for(
                                base,
                                measurement.name.as_deref(),
                                first_query_name,
                            ),
                        )
                        .iter()
                        .filter_map(|plan| scan_metric_total(plan, metric))
                        .max()
                    });
            let mut entry = json!({
                "query": measurement.name,
                "target": "candidate",
                "threshold": threshold,
                "metric": metric,
                "limit": limit,
            });
            match actual {
                None => {
                    entry["status"] = json!("failed");
                    entry["actual"] = Value::Null;
                    entry["reason"] = json!("scan metric not found in plan text");
                }
                Some(actual) => {
                    entry["status"] = json!(if (actual as f64) <= limit {
                        "passed"
                    } else {
                        "failed"
                    });
                    entry["actual"] = json!(actual);
                }
            }
            if let Some(base_actual) = base_actual {
                entry["base_actual"] = json!(base_actual);
            }
            results.push(entry);
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const MEMBERSHIP_PLAN: &str = "\
        FilterExec: DynamicFilter [ trace_id@2 IN (SET) ([trace0, trace1]) ], projection=[ts@0]\n\
        RegionScanExec: {\"region_id\":4398046511104} metrics=[output_rows: 1926, ]\n\
    \"metrics_per_partition\": [{\"num_rows\":1926,\"rows_before_filter\":20000,\"num_sst_rows\":20000, \
    \"fetch_metrics\":{\"pages_to_fetch_store\":3,\"page_size_to_fetch_store\":1048576}}]";

    const NO_MEMBERSHIP_PLAN: &str = "\
        FilterExec: trace_id@2 = trace_id@5\n\
        RegionScanExec: {\"region_id\":4398046511104} metrics=[output_rows: 1926, ]\n\
    \"metrics_per_partition\": [{\"num_rows\":1926,\"rows_before_filter\":1000000,\"num_sst_rows\":1000000, \
    \"fetch_metrics\":{\"pages_to_fetch_store\":403,\"page_size_to_fetch_store\":125829120}}]";

    fn query(name: &str, require: &[&str], forbid: &[&str], thresholds: Value) -> Query {
        serde_json::from_value(json!({
            "name": name,
            "kind": "sql",
            "query": "EXPLAIN ANALYZE VERBOSE SELECT 1",
            "warmup": 0,
            "iterations": 1,
            "thresholds": thresholds,
            "plan_checks": { "require": require, "forbid": forbid },
        }))
        .unwrap()
    }

    fn analyze_row(plan: &str) -> Value {
        json!({
            "ok": true,
            "status": 200,
            "latency_ms": 1.0,
            "response": {"data": [{"stage": 0, "node": 0, "plan": plan}]},
        })
    }

    fn measurement(name: &str, samples: Vec<Value>) -> Measurement {
        Measurement {
            name: Some(name.to_string()),
            kind: None,
            iterations: samples.len(),
            samples,
            latency_ms_median: Some(1.0),
            latency_ms_p95: Some(1.0),
            status: "ok".to_string(),
        }
    }

    fn result(measurements: Vec<Measurement>, first_execution: Option<Value>) -> QueryResult {
        QueryResult {
            validation: vec![],
            validation_errors: vec![],
            measurements,
            first_execution,
            status: "ok".to_string(),
        }
    }

    fn membership_checks() -> Query {
        query(
            "join",
            &["DynamicFilter", r"trace_id@\d+\s+IN\s+\("],
            &[
                r"DynamicFilter \[ empty \]",
                r"DynamicFilter \[ true \]",
                r"DynamicFilter \[[^\n]*\bAND true\b",
            ],
            json!({
                "max_candidate_page_size_to_fetch_store_bytes": 4194304,
                "max_candidate_num_sst_rows": 60000,
                "max_candidate_rows_before_filter": 200000,
            }),
        )
    }

    fn find<'a>(results: &'a [Value], threshold: &str, subject: &str) -> &'a Value {
        results
            .iter()
            .find(|entry| {
                entry["threshold"] == threshold
                    && (entry["pattern"] == subject || entry["metric"] == subject)
            })
            .unwrap_or_else(|| panic!("no {threshold} entry for {subject} in {results:#?}"))
    }

    #[test]
    fn extracts_plan_rows_from_object_and_array_responses() {
        assert_eq!(
            plan_text(&analyze_row("ProjectionExec: expr=[count(*)]")).as_deref(),
            Some("ProjectionExec: expr=[count(*)]")
        );
        let array = json!({"response": {"output": [{"records": {"rows": [["0", "0", "a"], [null, null, "Total rows: 1"]]}}]}});
        assert_eq!(plan_text(&array).as_deref(), Some("a\nTotal rows: 1"));
    }

    #[test]
    fn falls_back_to_serialized_response_when_no_plan_row_is_recognizable() {
        let nested = json!({"response": {"output": [{"records": "DynamicFilter [ trace_id@2 IN (SET) ([trace0]) ]"}]}});
        let text = plan_text(&nested).unwrap();
        assert!(text.contains("DynamicFilter [ trace_id@2 IN (SET)"));
        // Serialized responses escape their quotes; the fallback unescapes them
        // so metric extraction and plan patterns see the original text.
        let metrics = json!({"response": {"output": [{"records": {
            "metrics": "{\"rows_before_filter\":2048,\"fetch_metrics\":{\"page_size_to_fetch_store\":7}}"
        }}]}});
        let text = plan_text(&metrics).unwrap();
        assert!(!text.contains("\\\""));
        assert_eq!(scan_metric_total(&text, "rows_before_filter"), Some(2048));
        assert_eq!(
            scan_metric_total(&text, "page_size_to_fetch_store"),
            Some(7)
        );
        assert_eq!(plan_text(&json!({"response": {}})), None);
        assert_eq!(plan_text(&json!({"response": []})), None);
    }

    #[test]
    fn totals_scan_metrics_across_plan_rows() {
        assert_eq!(
            scan_metric_total(MEMBERSHIP_PLAN, "page_size_to_fetch_store"),
            Some(1048576)
        );
        assert_eq!(
            scan_metric_total(NO_MEMBERSHIP_PLAN, "rows_before_filter"),
            Some(1_000_000)
        );
        // Repeated entries (one per partition or region) are summed.
        assert_eq!(
            scan_metric_total(
                MEMBERSHIP_PLAN.to_string().repeat(2).as_str(),
                "rows_before_filter"
            ),
            Some(40_000)
        );
        assert_eq!(scan_metric_total(MEMBERSHIP_PLAN, "missing_metric"), None);
        // Neighbouring metric names are reported separately and never conflated.
        assert_eq!(scan_metric_total(MEMBERSHIP_PLAN, "page_size_needed"), None);
        assert_eq!(
            scan_metric_total(MEMBERSHIP_PLAN, "pages_to_fetch_store"),
            Some(3)
        );
    }

    #[test]
    fn valid_membership_plan_passes_every_assertion() {
        let base = result(
            vec![measurement("join", vec![analyze_row(NO_MEMBERSHIP_PLAN)])],
            None,
        );
        let candidate = result(
            vec![measurement("join", vec![analyze_row(MEMBERSHIP_PLAN)])],
            None,
        );
        let results = enforce_plan_assertions(&[membership_checks()], &base, &candidate).unwrap();
        assert_eq!(results.len(), 8, "{results:#?}");
        for entry in &results {
            assert_eq!(entry["status"], "passed", "{entry}");
            assert_eq!(entry["target"], "candidate");
        }
        assert_eq!(find(&results, "plan_require", "DynamicFilter")["actual"], 1);
        assert_eq!(find(&results, "plan_require", "DynamicFilter")["limit"], 1);
        assert_eq!(
            find(&results, "plan_forbid", r"DynamicFilter \[ empty \]")["limit"],
            0
        );
        let page_bytes = find(
            &results,
            "max_candidate_page_size_to_fetch_store_bytes",
            "page_size_to_fetch_store",
        );
        assert_eq!(page_bytes["actual"], 1048576);
        assert_eq!(page_bytes["limit"], 4194304.0);
        let sst_rows = find(&results, "max_candidate_num_sst_rows", "num_sst_rows");
        assert_eq!(sst_rows["actual"], 20000);
        assert_eq!(sst_rows["limit"], 60000.0);
        // Base diagnostics are reported but never gate.
        assert_eq!(
            find(&results, "plan_require", "DynamicFilter")["base_actual"],
            0
        );
        assert_eq!(page_bytes["base_actual"], 125829120);
    }

    #[test]
    fn missing_membership_fails_require_and_store_assertions() {
        let candidate = result(
            vec![measurement("join", vec![analyze_row(NO_MEMBERSHIP_PLAN)])],
            None,
        );
        let results =
            enforce_plan_assertions(&[membership_checks()], &candidate, &candidate).unwrap();
        let require = find(&results, "plan_require", "DynamicFilter");
        assert_eq!(require["status"], "failed");
        assert_eq!(require["actual"], 0);
        assert_eq!(require["reason"], "required plan pattern not found");
        let membership = find(&results, "plan_require", r"trace_id@\d+\s+IN\s+\(");
        assert_eq!(membership["status"], "failed");

        let page_bytes = find(
            &results,
            "max_candidate_page_size_to_fetch_store_bytes",
            "page_size_to_fetch_store",
        );
        assert_eq!(page_bytes["status"], "failed");
        assert_eq!(page_bytes["actual"], 125829120);
        let rows = find(
            &results,
            "max_candidate_rows_before_filter",
            "rows_before_filter",
        );
        assert_eq!(rows["status"], "failed");
        assert_eq!(rows["actual"], 1_000_000);
        let sst_rows = find(&results, "max_candidate_num_sst_rows", "num_sst_rows");
        assert_eq!(sst_rows["status"], "failed");
        assert_eq!(sst_rows["actual"], 1_000_000);
    }

    #[test]
    fn and_true_and_empty_fallbacks_fail_forbid_patterns() {
        for plan in [
            "FilterExec: DynamicFilter [ true ]",
            "FilterExec: DynamicFilter [ empty ]",
            "FilterExec: DynamicFilter [ trace_id@2 >= trace0 AND trace_id@2 <= trace641 AND true ]",
            // Real degraded shape observed from a build whose 642-key membership
            // list exceeded the remote payload path: bounds survive, the list
            // becomes `true` inside each CASE branch.
            "FilterExec: DynamicFilter [ CASE hash_repartition % 3 WHEN 0 THEN trace_id@0 >= trace101 AND trace_id@0 <= trace98 AND true WHEN 1 THEN trace_id@0 >= trace0 AND trace_id@0 <= trace96 AND true ELSE false END ]",
        ] {
            let candidate = result(vec![measurement("join", vec![analyze_row(plan)])], None);
            let results =
                enforce_plan_assertions(&[membership_checks()], &candidate, &candidate).unwrap();
            let failed_forbid: Vec<_> = results
                .iter()
                .filter(|entry| entry["threshold"] == "plan_forbid" && entry["status"] == "failed")
                .collect();
            assert_eq!(failed_forbid.len(), 1, "{plan}: {results:#?}");
            assert_eq!(failed_forbid[0]["actual"], 1, "{plan}");
            assert_eq!(failed_forbid[0]["reason"], "forbidden plan pattern matched");
            // Bounds-only and unresolved shapes have no `IN (`, so the require fails too.
            assert_eq!(
                find(&results, "plan_require", r"trace_id@\d+\s+IN\s+\(")["status"],
                "failed",
                "{plan}"
            );
        }
    }

    #[test]
    fn missing_scan_metric_fails_instead_of_passing_trivially() {
        let plan = "FilterExec: DynamicFilter [ trace_id@2 IN (SET) ([trace0]) ]";
        let candidate = result(vec![measurement("join", vec![analyze_row(plan)])], None);
        let results =
            enforce_plan_assertions(&[membership_checks()], &candidate, &candidate).unwrap();
        assert_eq!(
            find(&results, "plan_require", "DynamicFilter")["status"],
            "passed"
        );
        for (threshold, metric) in SCAN_METRIC_THRESHOLDS {
            let entry = find(&results, threshold, metric);
            assert_eq!(entry["status"], "failed");
            assert_eq!(entry["reason"], "scan metric not found in plan text");
            assert_eq!(entry["actual"], Value::Null);
        }
    }

    #[test]
    fn missing_plan_text_fails_every_plan_assertion() {
        let sample = json!({"ok": true, "status": 200, "response": {}});
        let candidate = result(vec![measurement("join", vec![sample])], None);
        let results =
            enforce_plan_assertions(&[membership_checks()], &candidate, &candidate).unwrap();
        let plan_entries: Vec<_> = results
            .iter()
            .filter(|entry| {
                entry["threshold"] == "plan_require" || entry["threshold"] == "plan_forbid"
            })
            .collect();
        assert_eq!(plan_entries.len(), 5);
        for entry in plan_entries {
            assert_eq!(entry["status"], "failed");
            assert_eq!(entry["reason"], "analyze response contains no plan text");
        }
    }

    #[test]
    fn cold_execution_keeps_store_bytes_meaningful_when_warm_scans_are_cached() {
        let cached = analyze_row("RegionScanExec: metrics=[output_rows: 1926]");
        let candidate = result(
            vec![measurement("join", vec![cached.clone(), cached])],
            Some(analyze_row(MEMBERSHIP_PLAN)),
        );
        let results =
            enforce_plan_assertions(&[membership_checks()], &candidate, &candidate).unwrap();
        let page_bytes = find(
            &results,
            "max_candidate_page_size_to_fetch_store_bytes",
            "page_size_to_fetch_store",
        );
        assert_eq!(page_bytes["actual"], 1048576);
        assert_eq!(page_bytes["status"], "passed");
    }

    #[test]
    fn only_the_candidate_is_gated() {
        let base = result(
            vec![measurement("join", vec![analyze_row(MEMBERSHIP_PLAN)])],
            None,
        );
        let candidate = result(
            vec![measurement("join", vec![analyze_row(NO_MEMBERSHIP_PLAN)])],
            None,
        );
        let results = enforce_plan_assertions(&[membership_checks()], &base, &candidate).unwrap();
        assert!(results.iter().all(|entry| entry["target"] == "candidate"));
        assert!(results.iter().any(|entry| entry["status"] == "failed"));
    }

    #[test]
    fn unknown_plan_check_keys_and_invalid_patterns_are_rejected() {
        let unknown: std::result::Result<Query, serde_json::Error> =
            serde_json::from_value(json!({
                "name": "join",
                "query": "EXPLAIN ANALYZE VERBOSE SELECT 1",
                "plan_checks": { "requires": ["DynamicFilter"] },
            }));
        assert!(unknown.is_err());

        let invalid = query("join", &["DynamicFilter ["], &[], json!({}));
        let error = compile_plan_checks(&invalid).unwrap_err().to_string();
        assert!(error.contains("invalid plan_require pattern"), "{error}");
    }
}
