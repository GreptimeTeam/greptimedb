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
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::measure::round_ties_even;
use super::model::{OtlpLoad, OtlpThresholds};
use super::plan::normalized_otlp_load;
use super::sql::{extract_count_value, http_post_sql, sql_ident, sql_string, value_f64, value_u64};
use super::{FinalizeOtlpArgs, Result, RunOtlpTargetArgs};

const OTLP_ROWS: &str = "greptime_frontend_otlp_traces_rows";
const OTLP_FAILURES: &str = "greptime_frontend_otlp_traces_failure_count";
const OTLP_ELAPSED_SUM: &str = "greptime_servers_http_otlp_traces_elapsed_sum";
const OTLP_ELAPSED_COUNT: &str = "greptime_servers_http_otlp_traces_elapsed_count";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OtlpSnapshot {
    captured_monotonic_seconds: f64,
    values: HashMap<String, f64>,
}

pub(super) async fn run_otlp_target(args: RunOtlpTargetArgs) -> Result<()> {
    if !args.http_timeout.is_finite() || args.http_timeout < 0.0 {
        return Err("--http-timeout must be a non-negative finite number".into());
    }
    let (_, load) = normalized_otlp_load(&args.fixture_generator, &args.case)?;
    let client = Client::builder()
        .timeout(Duration::from_secs_f64(args.http_timeout))
        .build()?;
    let result = match run_otlp_target_inner(&args, &load, &client).await {
        Ok(result) => result,
        Err(error) => {
            json!({ "name": args.target_name, "status": "failed", "error": error.to_string() })
        }
    };
    let text = format!("{}\n", serde_json::to_string_pretty(&result)?);
    fs::write(&args.output, &text)?;
    print!("{text}");
    if result["status"] == "failed" {
        std::process::exit(1);
    }
    Ok(())
}

async fn run_otlp_target_inner(
    args: &RunOtlpTargetArgs,
    load: &OtlpLoad,
    client: &Client,
) -> Result<Value> {
    let create_database = http_post_sql(
        client,
        args.http_port,
        &format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            sql_ident(&load.database)
        ),
        "public",
    )
    .await;
    if !create_database["ok"].as_bool().unwrap_or(false) {
        return Ok(
            json!({ "name": args.target_name, "create_database": create_database, "status": "failed" }),
        );
    }
    let otelgen = run_otelgen_load(
        &args.otelgen_bin,
        args.http_port,
        &args.work_dir,
        load,
        client,
    )
    .await?;
    let metrics = summarize_otlp_metrics(&otelgen)?;
    let flush = http_post_sql(
        client,
        args.http_port,
        &format!("ADMIN FLUSH_TABLE({})", sql_string(&load.table)),
        &load.database,
    )
    .await;
    let visibility = poll_otlp_visibility(
        client,
        args.http_port,
        &load.table,
        &load.database,
        metrics["accepted_spans"].as_u64().unwrap_or_default(),
        load.visibility_timeout_seconds,
    )
    .await?;
    let checks_ok = otelgen["status"] == "ok"
        && metrics["missing_metrics"]
            .as_array()
            .is_some_and(Vec::is_empty)
        && metrics["accepted_spans"].as_u64().unwrap_or_default() > 0
        && metrics["http_requests"].as_u64().unwrap_or_default() > 0
        && flush["ok"].as_bool().unwrap_or(false)
        && visibility["ok"].as_bool().unwrap_or(false)
        && visibility["row_count_ok"].as_bool().unwrap_or(false);
    Ok(json!({
        "name": args.target_name,
        "otelgen": otelgen,
        "metrics": metrics,
        "create_database": create_database,
        "flush": flush,
        "visibility": visibility,
        "status": if checks_ok { "measured" } else { "failed" },
    }))
}

fn otelgen_command(otelgen_bin: &Path, http_port: u16, load: &OtlpLoad) -> Vec<String> {
    vec![
        otelgen_bin.to_string_lossy().to_string(),
        "--protocol".to_string(),
        "http".to_string(),
        "--otel-exporter-otlp-endpoint".to_string(),
        format!("127.0.0.1:{http_port}"),
        "--otel-exporter-otlp-url-path".to_string(),
        "/v1/otlp/v1/traces".to_string(),
        "--header".to_string(),
        format!("x-greptime-pipeline-name={}", load.pipeline),
        "--header".to_string(),
        format!("x-greptime-db-name={}", load.database),
        "--header".to_string(),
        format!("x-greptime-trace-table-name={}", load.table),
        "--log-level".to_string(),
        "error".to_string(),
        "--insecure".to_string(),
        "--duration".to_string(),
        load.duration_seconds.to_string(),
        "--rate".to_string(),
        load.rate.to_string(),
        "traces".to_string(),
        "multi".to_string(),
        "--workers".to_string(),
        load.workers.to_string(),
        "--scenarios".to_string(),
        load.workload.clone(),
        "--exporter-shards".to_string(),
        load.exporter_shards.to_string(),
    ]
}

async fn run_otelgen_load(
    otelgen_bin: &Path,
    http_port: u16,
    work_dir: &Path,
    load: &OtlpLoad,
    client: &Client,
) -> Result<Value> {
    let command = otelgen_command(otelgen_bin, http_port, load);
    let clock = Instant::now();
    let initial = fetch_otlp_metrics(client, http_port, &clock).await?;
    let log_dir = work_dir.join("otelgen");
    fs::create_dir_all(&log_dir)?;
    let stdout_path = log_dir.join("stdout.log");
    let stderr_path = log_dir.join("stderr.log");
    let mut child = Command::new(otelgen_bin)
        .args(&command[1..])
        .stdout(Stdio::from(fs::File::create(&stdout_path)?))
        .stderr(Stdio::from(fs::File::create(&stderr_path)?))
        .spawn()?;
    let started = Instant::now();
    if load.warmup_seconds > 0 {
        let _ = wait_for_child(&mut child, Duration::from_secs(load.warmup_seconds)).await?;
    }
    let warmed = fetch_otlp_metrics(client, http_port, &clock).await?;
    let mut timed_out = false;
    if child.try_wait()?.is_none() {
        let remaining = load
            .duration_seconds
            .saturating_sub(load.warmup_seconds)
            .saturating_add(60)
            .max(60);
        if !wait_for_child(&mut child, Duration::from_secs(remaining)).await? {
            timed_out = true;
            child.kill()?;
            let _ = child.wait()?;
        }
    }
    let final_snapshot = fetch_otlp_metrics(client, http_port, &clock).await?;
    let returncode = match child.try_wait()? {
        Some(status) => status.code(),
        None => {
            child.kill()?;
            child.wait()?.code()
        }
    };
    let elapsed_seconds = started.elapsed().as_secs_f64();
    Ok(json!({
        "status": if returncode == Some(0) && !timed_out && elapsed_seconds + 1.0 >= load.duration_seconds as f64 { "ok" } else { "failed" },
        "cmd": command,
        "returncode": returncode,
        "timed_out": timed_out,
        "elapsed_seconds": elapsed_seconds,
        "stdout_path": stdout_path,
        "stderr_path": stderr_path,
        "snapshots": { "initial": initial, "warmup": warmed, "final": final_snapshot },
    }))
}

async fn wait_for_child(child: &mut std::process::Child, timeout: Duration) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait()?.is_some() {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn fetch_otlp_metrics(
    client: &Client,
    http_port: u16,
    clock: &Instant,
) -> Result<OtlpSnapshot> {
    let text = client
        .get(format!("http://127.0.0.1:{http_port}/metrics"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Ok(OtlpSnapshot {
        captured_monotonic_seconds: clock.elapsed().as_secs_f64(),
        values: parse_prometheus_metrics(&text)?,
    })
}

fn parse_prometheus_metrics(text: &str) -> Result<HashMap<String, f64>> {
    let sample = Regex::new(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{[^}]*\})?\s+(\S+)")?;
    let mut values = HashMap::new();
    for line in text.lines() {
        let Some(captures) = sample.captures(line.trim()) else {
            continue;
        };
        let (Some(name), Some(value)) = (captures.get(1), captures.get(2)) else {
            continue;
        };
        let name = name.as_str();
        if !matches!(
            name,
            OTLP_ROWS | OTLP_FAILURES | OTLP_ELAPSED_SUM | OTLP_ELAPSED_COUNT
        ) {
            continue;
        }
        let value: f64 = value.as_str().parse()?;
        if !value.is_finite() {
            return Err(format!("non-finite Prometheus sample for {name}").into());
        }
        *values.entry(name.to_string()).or_default() += value;
    }
    Ok(values)
}

fn metric_delta(after: &OtlpSnapshot, before: &OtlpSnapshot, name: &str) -> Result<f64> {
    let delta = after.values.get(name).copied().unwrap_or_default()
        - before.values.get(name).copied().unwrap_or_default();
    if delta < 0.0 {
        return Err(format!("metric {name} decreased by {}", -delta).into());
    }
    Ok(delta)
}

fn summarize_otlp_metrics(run: &Value) -> Result<Value> {
    let snapshots = run
        .get("snapshots")
        .ok_or("otelgen result has no snapshots")?;
    let initial: OtlpSnapshot = serde_json::from_value(snapshots["initial"].clone())?;
    let warmed: OtlpSnapshot = serde_json::from_value(snapshots["warmup"].clone())?;
    let final_snapshot: OtlpSnapshot = serde_json::from_value(snapshots["final"].clone())?;
    let missing_metrics = [OTLP_ROWS, OTLP_ELAPSED_SUM, OTLP_ELAPSED_COUNT]
        .into_iter()
        .filter(|name| !final_snapshot.values.contains_key(*name))
        .collect::<Vec<_>>();
    let accepted_spans =
        round_ties_even(metric_delta(&final_snapshot, &initial, OTLP_ROWS)?) as u64;
    let measurement_accepted_spans =
        round_ties_even(metric_delta(&final_snapshot, &warmed, OTLP_ROWS)?) as u64;
    let http_requests =
        round_ties_even(metric_delta(&final_snapshot, &warmed, OTLP_ELAPSED_COUNT)?) as u64;
    let latency_seconds = metric_delta(&final_snapshot, &warmed, OTLP_ELAPSED_SUM)?;
    let measurement_seconds =
        final_snapshot.captured_monotonic_seconds - warmed.captured_monotonic_seconds;
    Ok(json!({
        "accepted_spans": accepted_spans,
        "measurement_accepted_spans": measurement_accepted_spans,
        "accepted_spans_per_second": if measurement_seconds > 0.0 { Some(measurement_accepted_spans as f64 / measurement_seconds) } else { None },
        "http_requests": http_requests,
        "mean_http_latency_ms": if http_requests > 0 { Some(latency_seconds / http_requests as f64 * 1000.0) } else { None },
        "failure_count": round_ties_even(metric_delta(&final_snapshot, &initial, OTLP_FAILURES)?) as u64,
        "measurement_seconds": measurement_seconds,
        "missing_metrics": missing_metrics,
    }))
}

async fn poll_otlp_visibility(
    client: &Client,
    port: u16,
    table_name: &str,
    database: &str,
    expected_rows: u64,
    timeout_seconds: u64,
) -> Result<Value> {
    let sql = format!("SELECT count(*) FROM {}", sql_ident(table_name));
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let mut attempts = 0;
    loop {
        attempts += 1;
        let mut result = http_post_sql(client, port, &sql, database).await;
        let observed_rows = extract_count_value(&result);
        let row_count_ok =
            result["ok"].as_bool().unwrap_or(false) && observed_rows == Some(expected_rows);
        result
            .as_object_mut()
            .ok_or("count result must be an object")?
            .extend([
                ("expected_rows".to_string(), json!(expected_rows)),
                ("observed_rows".to_string(), json!(observed_rows)),
                ("attempts".to_string(), json!(attempts)),
                ("row_count_ok".to_string(), json!(row_count_ok)),
            ]);
        if row_count_ok {
            return Ok(result);
        }
        if Instant::now() >= deadline {
            result["ok"] = json!(false);
            result["row_count_ok"] = json!(false);
            result["error"] = json!(format!(
                "expected {expected_rows} rows but observed {observed_rows:?} after {attempts} attempts"
            ));
            return Ok(result);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub(super) async fn run_finalize_otlp(args: FinalizeOtlpArgs) -> Result<()> {
    let (case_path, load) = normalized_otlp_load(&args.fixture_generator, &args.case)?;
    let raw_case: toml::Value = toml::from_str(&fs::read_to_string(&case_path)?)?;
    let case_metadata = serde_json::to_value(
        raw_case
            .get("case")
            .cloned()
            .unwrap_or(toml::Value::Table(toml::map::Map::new())),
    )?;
    let base: Value = serde_json::from_slice(&fs::read(&args.base_result)?)?;
    let candidate: Value = serde_json::from_slice(&fs::read(&args.candidate_result)?)?;
    let thresholds = enforce_otlp_thresholds(
        &load.thresholds,
        base.get("metrics").unwrap_or(&Value::Null),
        candidate.get("metrics").unwrap_or(&Value::Null),
    );
    let failed = thresholds
        .iter()
        .any(|threshold| threshold["status"] == "failed")
        || [base.get("status"), candidate.get("status")]
            .into_iter()
            .any(|status| status.and_then(Value::as_str) == Some("failed"));
    let report = json!({
        "case_path": case_path,
        "case": case_metadata,
        "scenario": { "kind": "otlp_trace_load", "load": load },
        "targets": [base, candidate],
        "thresholds": thresholds,
        "status": if failed { "failed" } else { "ok" },
    });
    let text = format!("{}\n", serde_json::to_string_pretty(&report)?);
    fs::write(&args.output, &text)?;
    print!("{text}");
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

fn enforce_otlp_thresholds(
    thresholds: &OtlpThresholds,
    base: &Value,
    candidate: &Value,
) -> Vec<Value> {
    let mut results = Vec::new();
    for (target, metrics) in [("base", base), ("candidate", candidate)] {
        let failures = value_u64(metrics.get("failure_count"));
        results.push(json!({
            "target": target,
            "threshold": "max_failure_count",
            "status": if failures.is_some_and(|failures| failures <= thresholds.max_failure_count) { "passed" } else { "failed" },
            "actual": failures,
            "limit": thresholds.max_failure_count,
        }));
    }
    for (name, base_value, candidate_value, limit, inverse) in [
        (
            "max_candidate_throughput_regression_pct",
            value_f64(base.get("accepted_spans_per_second")),
            value_f64(candidate.get("accepted_spans_per_second")),
            thresholds.max_candidate_throughput_regression_pct,
            true,
        ),
        (
            "max_candidate_mean_latency_regression_pct",
            value_f64(base.get("mean_http_latency_ms")),
            value_f64(candidate.get("mean_http_latency_ms")),
            thresholds.max_candidate_mean_latency_regression_pct,
            false,
        ),
    ] {
        let (Some(base_value), Some(candidate_value)) =
            (base_value.filter(|value| *value != 0.0), candidate_value)
        else {
            results.push(json!({
                "threshold": name,
                "status": "failed",
                "reason": if inverse { "missing or zero throughput" } else { "missing or zero mean latency" },
                "base": base_value,
                "candidate": candidate_value,
            }));
            continue;
        };
        let actual = if inverse {
            (base_value - candidate_value) / base_value * 100.0
        } else {
            (candidate_value - base_value) / base_value * 100.0
        };
        results.push(json!({
            "threshold": name,
            "status": if actual <= limit { "passed" } else { "failed" },
            "actual_pct": actual,
            "limit_pct": limit,
            "base": base_value,
            "candidate": candidate_value,
        }));
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn otlp_load_for_test() -> OtlpLoad {
        serde_json::from_value(json!({
            "database": "public",
            "table": "opentelemetry_traces",
            "pipeline": "greptime_trace_v1",
            "duration_seconds": 120,
            "warmup_seconds": 60,
            "rate": 50000,
            "workers": 4,
            "exporter_shards": 4,
            "workload": "microservices",
            "visibility_timeout_seconds": 30,
            "thresholds": {
                "max_candidate_throughput_regression_pct": 20.0,
                "max_candidate_mean_latency_regression_pct": 20.0,
                "max_failure_count": 0
            }
        }))
        .unwrap()
    }

    #[test]
    fn parses_labeled_otlp_metrics_and_rejects_non_finite_samples() {
        let metrics = parse_prometheus_metrics(
            "greptime_frontend_otlp_traces_rows 10\n\
             greptime_frontend_otlp_traces_failure_count{kind=\"a\"} 1\n\
             greptime_frontend_otlp_traces_failure_count{kind=\"b\"} 2\n",
        )
        .unwrap();
        assert_eq!(metrics[OTLP_ROWS], 10.0);
        assert_eq!(metrics[OTLP_FAILURES], 3.0);
        assert!(parse_prometheus_metrics("greptime_frontend_otlp_traces_rows NaN").is_err());
        let before = OtlpSnapshot {
            captured_monotonic_seconds: 0.0,
            values: HashMap::from([(OTLP_ROWS.to_string(), 2.0)]),
        };
        let after = OtlpSnapshot {
            captured_monotonic_seconds: 1.0,
            values: HashMap::from([(OTLP_ROWS.to_string(), 1.0)]),
        };
        assert!(metric_delta(&after, &before, OTLP_ROWS).is_err());
    }

    #[test]
    fn summarizes_otlp_deltas_and_builds_exact_command() {
        let snapshot = |captured: f64, values: Vec<(String, f64)>| OtlpSnapshot {
            captured_monotonic_seconds: captured,
            values: values.into_iter().collect(),
        };
        let initial = snapshot(0.0, vec![(OTLP_ROWS.to_string(), 10.0)]);
        let warmed = snapshot(
            5.0,
            vec![
                (OTLP_ROWS.to_string(), 110.0),
                (OTLP_ELAPSED_SUM.to_string(), 1.0),
                (OTLP_ELAPSED_COUNT.to_string(), 10.0),
            ],
        );
        let final_snapshot = snapshot(
            15.0,
            vec![
                (OTLP_ROWS.to_string(), 310.0),
                (OTLP_FAILURES.to_string(), 3.0),
                (OTLP_ELAPSED_SUM.to_string(), 3.0),
                (OTLP_ELAPSED_COUNT.to_string(), 30.0),
            ],
        );
        let metrics = summarize_otlp_metrics(&json!({
            "snapshots": { "initial": initial, "warmup": warmed, "final": final_snapshot }
        }))
        .unwrap();
        assert_eq!(metrics["accepted_spans"], 300);
        assert_eq!(metrics["measurement_accepted_spans"], 200);
        assert_eq!(metrics["accepted_spans_per_second"], 20.0);
        assert_eq!(metrics["http_requests"], 20);
        assert_eq!(metrics["mean_http_latency_ms"], 100.0);
        assert_eq!(metrics["failure_count"], 3);
        let missing = summarize_otlp_metrics(&json!({
            "snapshots": {
                "initial": snapshot(0.0, vec![(OTLP_ROWS.to_string(), 0.0)]),
                "warmup": snapshot(1.0, vec![(OTLP_ROWS.to_string(), 1.0)]),
                "final": snapshot(2.0, vec![(OTLP_ROWS.to_string(), 2.0)])
            }
        }))
        .unwrap();
        assert_eq!(
            missing["missing_metrics"],
            json!([OTLP_ELAPSED_SUM, OTLP_ELAPSED_COUNT])
        );
        let command = otelgen_command(Path::new("/bin/otelgen"), 4000, &otlp_load_for_test());
        assert_eq!(
            command,
            vec![
                "/bin/otelgen",
                "--protocol",
                "http",
                "--otel-exporter-otlp-endpoint",
                "127.0.0.1:4000",
                "--otel-exporter-otlp-url-path",
                "/v1/otlp/v1/traces",
                "--header",
                "x-greptime-pipeline-name=greptime_trace_v1",
                "--header",
                "x-greptime-db-name=public",
                "--header",
                "x-greptime-trace-table-name=opentelemetry_traces",
                "--log-level",
                "error",
                "--insecure",
                "--duration",
                "120",
                "--rate",
                "50000",
                "traces",
                "multi",
                "--workers",
                "4",
                "--scenarios",
                "microservices",
                "--exporter-shards",
                "4",
            ]
        );
    }

    #[test]
    fn otlp_thresholds_handle_zero_metrics_and_pass_fail_cases() {
        let load = otlp_load_for_test();
        let base = json!({ "failure_count": 0, "accepted_spans_per_second": 20.0, "mean_http_latency_ms": 100.0 });
        let candidate = json!({ "failure_count": 0, "accepted_spans_per_second": 18.0, "mean_http_latency_ms": 110.0 });
        let results = enforce_otlp_thresholds(&load.thresholds, &base, &candidate);
        assert!(results.iter().all(|result| result["status"] == "passed"));
        let failed = enforce_otlp_thresholds(
            &load.thresholds,
            &json!({ "failure_count": 0, "accepted_spans_per_second": 0.0, "mean_http_latency_ms": 100.0 }),
            &json!({ "failure_count": 3, "accepted_spans_per_second": 20.0, "mean_http_latency_ms": 130.0 }),
        );
        assert!(
            failed
                .iter()
                .any(|result| result["threshold"] == "max_failure_count"
                    && result["target"] == "candidate"
                    && result["status"] == "failed")
        );
        assert!(failed.iter().any(|result| result["threshold"]
            == "max_candidate_throughput_regression_pct"
            && result["reason"] == "missing or zero throughput"));
        assert!(failed.iter().any(|result| result["threshold"]
            == "max_candidate_mean_latency_regression_pct"
            && result["status"] == "failed"));
    }
}
