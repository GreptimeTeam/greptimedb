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
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use regex::Regex;
use reqwest::Client;
use serde_json::{Value, json};

use crate::query_regression_runner::measure::round_ties_even;
use crate::query_regression_runner::model::{MixMeasure, RemoteWrite, Scenario, WriteMeasure};
use crate::query_regression_runner::plan::load_plan;
use crate::query_regression_runner::remote::ingest::{
    SampleChunk, frontend_prom_config, remote_write_command, sample_chunks, scheduled_flushes,
};
use crate::query_regression_runner::sql::{
    http_post_sql, sql_ident, sql_string, value_f64, value_u64,
};
use crate::query_regression_runner::{Result, RunWriteThroughputArgs};

const SCHEDULER_ENV_PREFIX: &str = "GREPTIMEDB_DATANODE__RUNTIME__EXPERIMENTAL_WORKLOAD_SCHEDULER";
const SCHEDULER_POLL_PATTERN: &str = r#"^greptime_workload_scheduler_polls\{[^}]*workload="(query|write)"[^}]*\}\s+(\d+)(?:\s+.*)?$"#;
const HEALTH_TIMEOUT_SECONDS: u64 = 60;

pub(super) async fn run_write_throughput(args: RunWriteThroughputArgs) -> Result<()> {
    if !args.http_timeout.is_finite() || args.http_timeout < 0.0 {
        return Err("--http-timeout must be a non-negative finite number".into());
    }
    let case_path = args.case.canonicalize()?;
    let plan = load_plan(&args.fixture_generator, &case_path)?;
    let scenario_value = plan
        .get("scenario")
        .cloned()
        .ok_or("fixture plan has no scenario")?;
    let scenario: Scenario = serde_json::from_value(scenario_value.clone())?;
    let (remote, write_measure, scheduler) = match scenario {
        Scenario::WriteThroughput {
            remote_write,
            write_measure,
            scheduler,
        } => (remote_write, write_measure, scheduler),
        _ => {
            return Err("write-throughput command requires scenario kind write_throughput".into());
        }
    };
    let case_metadata = {
        let raw_case: toml::Value = toml::from_str(&fs::read_to_string(&case_path)?)?;
        serde_json::to_value(
            raw_case
                .get("case")
                .cloned()
                .unwrap_or(toml::Value::Table(toml::map::Map::new())),
        )?
    };
    let client = Client::builder()
        .timeout(Duration::from_secs_f64(args.http_timeout))
        .build()?;
    let work_root = args
        .work_dir
        .canonicalize()
        .unwrap_or(args.work_dir.clone());
    fs::create_dir_all(&work_root)?;
    let ports = allocate_ports(16)?;
    let mut report = json!({
        "case_path": case_path,
        "case": case_metadata,
        "scenario": scenario_value,
        "dry_run": args.dry_run,
        "query_mode": "distributed",
        "http_timeout": args.http_timeout,
        "targets": [],
        "thresholds": [],
        "status": if args.dry_run { "planned" } else { "running" },
    });

    let mut measurements = Vec::new();
    let mut query_measurements = Vec::new();
    for target_name in ["base", "candidate"] {
        let binary = if target_name == "base" {
            args.base_bin.clone()
        } else {
            args.candidate_bin.clone()
        };
        let target_ports = if target_name == "base" {
            &ports[..8]
        } else {
            &ports[8..]
        };
        let mut target = TargetCluster::new(binary, work_root.join(target_name), target_ports)?;
        let config_path = target.work_dir.join("frontend-prom-store.toml");
        fs::write(&config_path, frontend_prom_config(&remote.prom_store)?)?;
        if !args.dry_run {
            target
                .start_all(
                    &client,
                    &config_path,
                    scheduler_env(false, scheduler.as_ref()),
                )
                .await?;
        }
        let create_database = if args.dry_run {
            json!({"status": "dry-run", "database": remote.database.clone()})
        } else {
            http_post_sql(
                &client,
                target.http_port,
                &format!(
                    "CREATE DATABASE IF NOT EXISTS {}",
                    sql_ident(&remote.database)
                ),
                "public",
            )
            .await
        };
        let mix = write_measure.mix.as_ref();
        let mut scheduler_polls_before = None;
        let mut scheduler_polls_after = None;
        if mix.is_some() && !args.dry_run {
            scheduler_polls_before =
                Some(scrape_scheduler_polls(&client, target.datanode_http_port).await);
        }
        let (rw, flushes, query_measurement) = if let Some(mix) = mix {
            let (rw, flushes, attempts) = run_mixed_ingestion_and_queries(
                &args.fixture_generator,
                target.http_port,
                &remote,
                &client,
                mix,
                &write_measure,
                args.dry_run,
            )
            .await?;
            let query_measurement = if args.dry_run {
                planned_mix_query_measurement(mix, &write_measure)
            } else {
                mix_query_measurement(&attempts)
            };
            if !args.dry_run {
                scheduler_polls_after =
                    Some(scrape_scheduler_polls(&client, target.datanode_http_port).await);
            }
            (rw, flushes, Some(query_measurement))
        } else {
            let (rw, flushes) = run_write_throughput_ingestion(
                &args.fixture_generator,
                target.http_port,
                &remote,
                &client,
                args.dry_run,
            )
            .await?;
            (rw, flushes, None)
        };
        let measurement = if args.dry_run {
            planned_write_throughput_measurement(&remote, &write_measure)
        } else {
            write_throughput_measurement(&rw, &write_measure)
        };
        let scheduler_report = scheduler_report_entry(target_name, scheduler.as_ref());
        let mut target_report = json!({
            "name": target_name,
            "binary": target.binary,
            "work_dir": target.work_dir,
            "components": target.component_report(),
            "frontend_config": config_path,
            "create_database": create_database,
            "remote_write": rw,
            "flushes": flushes,
            "flush": Value::Null,
            "scheduler": scheduler_report,
            "write_measurement": measurement.clone(),
        });
        if let Some(flush) = target_report["flushes"]
            .as_array()
            .and_then(|flushes| flushes.last())
        {
            target_report["flush"] = flush.clone();
        }
        if let Some(query_measurement) = &query_measurement {
            target_report["query_measurement"] = query_measurement.clone();
        }
        if let Some(mix) = mix {
            target_report["mix"] = serde_json::to_value(mix)?;
            target_report["scheduler_poll_deltas"] =
                match (&scheduler_polls_after, &scheduler_polls_before) {
                    (Some(after), Some(before)) => scheduler_poll_deltas(after, before),
                    _ => json!({"status": "planned"}),
                };
        }
        let flushes_ok = target_report["flushes"].as_array().is_some_and(|flushes| {
            flushes
                .iter()
                .all(|flush| flush["ok"].as_bool().unwrap_or(false))
        });
        let measurement_ok = measurement["mean_rps"]
            .as_f64()
            .is_some_and(|mean_rps| mean_rps != 0.0);
        let query_ok = query_measurement
            .as_ref()
            .is_none_or(|query| query["samples"].as_u64().unwrap_or(0) > 0);
        let checks_ok = args.dry_run
            || (create_database["ok"].as_bool().unwrap_or(false)
                && flushes_ok
                && measurement_ok
                && query_ok);
        if !checks_ok && !args.dry_run {
            let mut validation = json!({
                "phase": "write_throughput",
                "create_database_ok": create_database["ok"].as_bool().unwrap_or(false),
                "flushes_ok": flushes_ok,
                "measurement_ok": measurement_ok,
                "mean_rps": measurement["mean_rps"],
                "failure_rate": measurement["failure_rate"],
            });
            if let Some(query_measurement) = &query_measurement {
                validation["query_ok"] = json!(query_ok);
                validation["query_samples"] = query_measurement["samples"].clone();
            }
            target_report
                .as_object_mut()
                .expect("target report is an object")
                .entry("validation_errors".to_string())
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .expect("validation_errors is an array")
                .push(validation);
        }
        target_report["status"] = json!(if args.dry_run {
            "planned"
        } else if checks_ok {
            "measured"
        } else {
            "failed"
        });
        let per_target_output = work_root.join(target_name).join("report.json");
        fs::write(
            &per_target_output,
            format!("{}\n", serde_json::to_string_pretty(&target_report)?),
        )?;
        report
            .get_mut("targets")
            .and_then(Value::as_array_mut)
            .expect("targets is an array")
            .push(target_report);
        measurements.push(measurement);
        if let Some(query_measurement) = query_measurement {
            query_measurements.push(query_measurement);
        }
        target.stop_all();
    }

    let thresholds = if args.dry_run {
        let mut planned = planned_write_throughput_thresholds(&write_measure);
        if let Some(mix) = &write_measure.mix {
            planned.extend(planned_mix_query_thresholds(mix));
        }
        planned
    } else {
        let mut enforced =
            enforce_write_throughput_thresholds(&write_measure, &measurements[0], &measurements[1]);
        if let Some(mix) = &write_measure.mix {
            enforced.extend(enforce_mix_query_thresholds(
                mix,
                &query_measurements[0],
                &query_measurements[1],
            ));
        }
        enforced
    };
    report["thresholds"] = json!(thresholds);
    let failed = report["thresholds"]
        .as_array()
        .is_some_and(|thresholds| thresholds.iter().any(|t| t["status"] == "failed"))
        || report["targets"]
            .as_array()
            .is_some_and(|targets| targets.iter().any(|t| t["status"] == "failed"));
    report["status"] = json!(if args.dry_run {
        "planned"
    } else if failed {
        "failed"
    } else {
        "ok"
    });
    let text = format!("{}\n", serde_json::to_string_pretty(&report)?);
    if let Some(path) = args.output {
        fs::write(path, &text)?;
    }
    print!("{text}");
    if !args.dry_run && failed {
        std::process::exit(1);
    }
    Ok(())
}

fn allocate_ports(n: usize) -> Result<Vec<u16>> {
    let listeners = (0..n)
        .map(|_| std::net::TcpListener::bind(("127.0.0.1", 0)))
        .collect::<std::io::Result<Vec<_>>>()?;
    let ports = listeners
        .iter()
        .map(|listener| listener.local_addr().map(|addr| addr.port()))
        .collect::<std::io::Result<Vec<_>>>()?;
    drop(listeners);
    Ok(ports)
}

/// Derive the datanode workload-scheduler environment variables for a target.
///
/// Returns `None` when the case has no `[scenario.scheduler]` section: both
/// base and candidate then run with the datanode default (scheduler disabled)
/// and the spawned datanode inherits the runner's environment unchanged. When
/// the section is present the base target only pins `ENABLE=false` while the
/// candidate pins `ENABLE=true` and forwards the scheduler weights/max-polls
/// so both targets are configured identically except for the enable flag.
/// Metasrv and frontend never receive these.
fn scheduler_env(
    enable: bool,
    scheduler: Option<&crate::query_regression_runner::model::WorkloadSchedulerConfig>,
) -> Option<HashMap<String, String>> {
    let scheduler = scheduler?;
    let mut env = HashMap::new();
    env.insert(
        format!("{SCHEDULER_ENV_PREFIX}__ENABLE"),
        if enable { "true" } else { "false" }.to_string(),
    );
    if enable {
        env.insert(
            format!("{SCHEDULER_ENV_PREFIX}__MAX_CONCURRENT_POLLS"),
            scheduler.max_concurrent_polls.to_string(),
        );
        env.insert(
            format!("{SCHEDULER_ENV_PREFIX}__QUERY_WEIGHT"),
            scheduler.query_weight.to_string(),
        );
        env.insert(
            format!("{SCHEDULER_ENV_PREFIX}__WRITE_WEIGHT"),
            scheduler.write_weight.to_string(),
        );
    }
    Some(env)
}

fn scheduler_report_entry(
    target_name: &str,
    scheduler: Option<&crate::query_regression_runner::model::WorkloadSchedulerConfig>,
) -> Value {
    let enabled = target_name == "candidate" && scheduler.is_some();
    json!({
        "enabled": enabled,
        "max_concurrent_polls": scheduler.map(|s| s.max_concurrent_polls).unwrap_or(0),
        "query_weight": scheduler.map(|s| s.query_weight).unwrap_or(2),
        "write_weight": scheduler.map(|s| s.write_weight).unwrap_or(8),
    })
}

/// A local metasrv + one datanode + frontend cluster for one write_throughput
/// target. The datanode process receives the workload-scheduler environment
/// variables; metasrv and frontend never do.
struct TargetCluster {
    binary: PathBuf,
    work_dir: PathBuf,
    http_port: u16,
    grpc_port: u16,
    mysql_port: u16,
    postgres_port: u16,
    metasrv_rpc_port: u16,
    metasrv_http_port: u16,
    datanode_rpc_port: u16,
    datanode_http_port: u16,
    datanode_data_dir: PathBuf,
    procs: HashMap<String, Child>,
}

impl TargetCluster {
    fn new(binary: PathBuf, work_dir: PathBuf, ports: &[u16]) -> Result<Self> {
        fs::create_dir_all(&work_dir)?;
        Ok(Self {
            datanode_data_dir: work_dir.join("datanode-0").join("data"),
            binary,
            work_dir,
            metasrv_rpc_port: ports[0],
            metasrv_http_port: ports[1],
            datanode_rpc_port: ports[2],
            datanode_http_port: ports[3],
            http_port: ports[4],
            grpc_port: ports[5],
            mysql_port: ports[6],
            postgres_port: ports[7],
            procs: HashMap::new(),
        })
    }

    fn component_report(&self) -> Value {
        json!({
            "metasrv": {
                "grpc": format!("127.0.0.1:{}", self.metasrv_rpc_port),
                "http": format!("127.0.0.1:{}", self.metasrv_http_port),
                "logs": self.work_dir.join("logs").join("metasrv"),
            },
            "datanode_0": {
                "node_id": 0,
                "grpc": format!("127.0.0.1:{}", self.datanode_rpc_port),
                "http": format!("127.0.0.1:{}", self.datanode_http_port),
                "data_home": self.datanode_data_dir,
                "logs": self.work_dir.join("logs").join("datanode-0"),
            },
            "frontend": {
                "http": format!("127.0.0.1:{}", self.http_port),
                "grpc": format!("127.0.0.1:{}", self.grpc_port),
                "mysql": format!("127.0.0.1:{}", self.mysql_port),
                "postgres": format!("127.0.0.1:{}", self.postgres_port),
                "logs": self.work_dir.join("logs").join("frontend"),
            },
        })
    }

    fn spawn(
        &mut self,
        name: &str,
        args: Vec<String>,
        env: Option<HashMap<String, String>>,
    ) -> Result<()> {
        let log_dir = self.work_dir.join("logs").join(name);
        fs::create_dir_all(&log_dir)?;
        let stdout = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_dir.join("stdout.log"))?;
        let stderr = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_dir.join("stderr.log"))?;
        let mut command = Command::new(&self.binary);
        command
            .args(&args)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        if let Some(env) = env {
            command.envs(env);
        }
        self.procs.insert(name.to_string(), command.spawn()?);
        Ok(())
    }

    fn ensure_metasrv_alive(&mut self) -> Result<()> {
        if let Some(proc) = self.procs.get_mut("metasrv") {
            if proc.try_wait()?.is_none() {
                return Ok(());
            }
        }
        Err("metasrv exited; memory-store metadata is no longer valid".into())
    }

    async fn start_metasrv(&mut self, client: &Client) -> Result<()> {
        let log_dir = self.work_dir.join("logs").join("metasrv");
        self.spawn(
            "metasrv",
            vec![
                "metasrv".to_string(),
                "start".to_string(),
                "--grpc-bind-addr".to_string(),
                format!("127.0.0.1:{}", self.metasrv_rpc_port),
                "--grpc-server-addr".to_string(),
                format!("127.0.0.1:{}", self.metasrv_rpc_port),
                "--http-addr".to_string(),
                format!("127.0.0.1:{}", self.metasrv_http_port),
                "--backend".to_string(),
                "memory-store".to_string(),
                "--enable-region-failover".to_string(),
                "false".to_string(),
                "--log-dir".to_string(),
                log_dir.to_string_lossy().to_string(),
            ],
            None,
        )?;
        wait_health(client, self.metasrv_http_port).await
    }

    async fn start_datanode(
        &mut self,
        client: &Client,
        scheduler_env: Option<HashMap<String, String>>,
    ) -> Result<()> {
        self.ensure_metasrv_alive()?;
        let log_dir = self.work_dir.join("logs").join("datanode-0");
        fs::create_dir_all(&self.datanode_data_dir)?;
        self.spawn(
            "datanode",
            vec![
                "datanode".to_string(),
                "start".to_string(),
                "--grpc-bind-addr".to_string(),
                format!("127.0.0.1:{}", self.datanode_rpc_port),
                "--grpc-server-addr".to_string(),
                format!("127.0.0.1:{}", self.datanode_rpc_port),
                "--http-addr".to_string(),
                format!("127.0.0.1:{}", self.datanode_http_port),
                "--data-home".to_string(),
                self.datanode_data_dir.to_string_lossy().to_string(),
                "--log-dir".to_string(),
                log_dir.to_string_lossy().to_string(),
                "--node-id".to_string(),
                "0".to_string(),
                "--metasrv-addrs".to_string(),
                format!("127.0.0.1:{}", self.metasrv_rpc_port),
            ],
            scheduler_env,
        )?;
        wait_health(client, self.datanode_http_port).await
    }

    async fn start_frontend(&mut self, client: &Client, config_file: Option<&Path>) -> Result<()> {
        self.ensure_metasrv_alive()?;
        let log_dir = self.work_dir.join("logs").join("frontend");
        let mut args = vec!["frontend".to_string(), "start".to_string()];
        if let Some(config_file) = config_file {
            args.extend([
                "--config-file".to_string(),
                config_file.to_string_lossy().to_string(),
            ]);
        }
        args.extend([
            "--metasrv-addrs".to_string(),
            format!("127.0.0.1:{}", self.metasrv_rpc_port),
            "--http-addr".to_string(),
            format!("127.0.0.1:{}", self.http_port),
            "--grpc-bind-addr".to_string(),
            format!("127.0.0.1:{}", self.grpc_port),
            "--grpc-server-addr".to_string(),
            format!("127.0.0.1:{}", self.grpc_port),
            "--mysql-addr".to_string(),
            format!("127.0.0.1:{}", self.mysql_port),
            "--postgres-addr".to_string(),
            format!("127.0.0.1:{}", self.postgres_port),
            "--log-dir".to_string(),
            log_dir.to_string_lossy().to_string(),
        ]);
        self.spawn("frontend", args, None)?;
        wait_health(client, self.http_port).await
    }

    async fn start_all(
        &mut self,
        client: &Client,
        config_file: &Path,
        scheduler_env: Option<HashMap<String, String>>,
    ) -> Result<()> {
        self.start_metasrv(client).await?;
        self.start_datanode(client, scheduler_env).await?;
        self.start_frontend(client, Some(config_file)).await
    }

    fn stop_all(&mut self) {
        for name in ["frontend", "datanode", "metasrv"] {
            let mut proc = self.procs.remove(name);
            if let Some(proc) = proc.as_mut() {
                let _ = proc.kill();
                let _ = proc.wait();
            }
        }
    }
}

impl Drop for TargetCluster {
    fn drop(&mut self) {
        // Ensure no orphaned metasrv/datanode/frontend processes survive an
        // early return (e.g. a failed ingestion).
        self.stop_all();
    }
}

async fn wait_health(client: &Client, port: u16) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(HEALTH_TIMEOUT_SECONDS);
    loop {
        let healthy = client
            .get(format!("http://127.0.0.1:{port}/health"))
            .send()
            .await
            .is_ok_and(|response| response.status().as_u16() < 500);
        if healthy {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("health check timed out on port {port}").into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn summarize_write_chunks(chunks: &[Value], remote: &RemoteWrite, dry_run: bool) -> Value {
    if dry_run {
        let total_samples = remote.samples_per_series;
        let rows = remote.series_count * total_samples;
        let batches = chunks.len() as u64
            * remote
                .series_count
                .div_ceil(remote.chunk_series_count.max(1));
        return json!({
            "rows": rows,
            "samples_written": rows,
            "batches": batches,
            "elapsed_seconds": 0.0,
        });
    }
    let mut rows = 0;
    let mut samples_written = 0;
    let mut batches = 0;
    let mut elapsed_seconds = 0.0;
    for chunk in chunks {
        let chunk_summary = chunk.get("summary").unwrap_or(&Value::Null);
        let chunk_rows = value_u64(chunk_summary.get("rows")).unwrap_or(0);
        rows += chunk_rows;
        samples_written += value_u64(chunk_summary.get("samples_written")).unwrap_or(chunk_rows);
        batches += value_u64(chunk_summary.get("batches")).unwrap_or(0);
        elapsed_seconds += value_f64(chunk_summary.get("elapsed_seconds"))
            .or_else(|| value_f64(chunk.get("elapsed_seconds")))
            .unwrap_or(0.0);
    }
    json!({
        "rows": rows,
        "samples_written": samples_written,
        "batches": batches,
        "elapsed_seconds": elapsed_seconds,
    })
}

/// Run one `query_perf_fixture prom-remote-write` invocation, recording failure
/// in the result instead of aborting: the write_throughput scenario measures a
/// failure rate, so a failed chunk stays in the chunk list with status
/// "failed" (and no summary).
fn run_remote_write_chunk(
    generator: &Path,
    port: u16,
    remote: &RemoteWrite,
    chunk: Option<&SampleChunk>,
    dry_run: bool,
) -> Result<Value> {
    let command = remote_write_command(generator, port, remote, chunk);
    if dry_run {
        return Ok(json!({ "status": "dry-run", "command": command }));
    }
    let started = Instant::now();
    let output = Command::new(generator).args(&command[1..]).output()?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let mut result = json!({
        "status": if output.status.success() { "ok" } else { "failed" },
        "command": command,
        "returncode": output.status.code(),
        "elapsed_seconds": elapsed_seconds,
        "stdout": stdout,
        "stderr": stderr,
    });
    if output.status.success() {
        match serde_json::from_str::<Value>(&result["stdout"].as_str().unwrap_or_default()) {
            Ok(summary) => result["summary"] = summary,
            Err(_) => result["summary_parse_error"] = result["stdout"].clone(),
        }
    }
    Ok(result)
}

async fn flush_remote_physical_table(
    client: &Client,
    port: u16,
    remote: &RemoteWrite,
    reason: &str,
    chunk_index: Option<u64>,
    dry_run: bool,
) -> Result<Value> {
    if dry_run {
        return Ok(json!({
            "status": "dry-run",
            "physical_table": remote.physical_table,
            "reason": reason,
            "chunk_index": chunk_index,
        }));
    }
    let mut result = http_post_sql(
        client,
        port,
        &format!("ADMIN FLUSH_TABLE({})", sql_string(&remote.physical_table)),
        &remote.database,
    )
    .await;
    result
        .as_object_mut()
        .expect("flush result is an object")
        .extend([
            ("physical_table".to_string(), json!(remote.physical_table)),
            ("reason".to_string(), json!(reason)),
            ("chunk_index".to_string(), json!(chunk_index)),
        ]);
    Ok(result)
}

/// Sample-chunked remote-write ingestion for the write_throughput scenario.
///
/// Mirrors `remote::ingest` but records failed chunks instead of raising, so
/// the measurement can compute a failure rate and per-window stats from the
/// per-chunk results. Each chunk result keeps its rows/elapsed so the
/// measurement functions can bucket RPS over the measurement window.
async fn run_write_throughput_ingestion(
    generator: &Path,
    port: u16,
    remote: &RemoteWrite,
    client: &Client,
    dry_run: bool,
) -> Result<(Value, Vec<Value>)> {
    let Some(chunks) = sample_chunks(remote)? else {
        let rw = run_remote_write_chunk(generator, port, remote, None, dry_run)?;
        let flush =
            flush_remote_physical_table(client, port, remote, "final", None, dry_run).await?;
        return Ok((rw, vec![flush]));
    };
    let mut results = Vec::with_capacity(chunks.len());
    let mut flushes = Vec::new();
    let scheduled = scheduled_flushes(&chunks, remote.flush_every_sample_chunks);
    for chunk in &chunks {
        let mut result = run_remote_write_chunk(generator, port, remote, Some(chunk), dry_run)?;
        result
            .as_object_mut()
            .expect("chunk result is an object")
            .extend([
                ("sample_offset".to_string(), json!(chunk.offset)),
                (
                    "samples_per_series".to_string(),
                    json!(chunk.samples_per_series),
                ),
                ("chunk_index".to_string(), json!(chunk.index)),
            ]);
        results.push(result);
        if let Some((_, reason)) = scheduled.iter().find(|(index, _)| *index == chunk.index) {
            flushes.push(
                flush_remote_physical_table(
                    client,
                    port,
                    remote,
                    reason,
                    Some(chunk.index),
                    dry_run,
                )
                .await?,
            );
        }
    }
    let aggregate = summarize_write_chunks(&results, remote, dry_run);
    let status = if dry_run {
        "dry-run"
    } else if results
        .iter()
        .all(|chunk| chunk["status"].as_str() == Some("ok"))
    {
        "ok"
    } else {
        "failed"
    };
    Ok((
        json!({
            "status": status,
            "mode": "sample-chunked",
            "sample_chunk_size": remote.sample_chunk_size,
            "flush_every_sample_chunks": remote.flush_every_sample_chunks,
            "chunks": results,
            "aggregate": aggregate,
        }),
        flushes,
    ))
}

fn write_chunk_rows(chunk: &Value) -> u64 {
    let summary = chunk.get("summary").unwrap_or(&Value::Null);
    value_u64(summary.get("rows"))
        .or_else(|| value_u64(chunk.get("rows")))
        .unwrap_or(0)
}

fn write_chunk_elapsed_seconds(chunk: &Value) -> f64 {
    let summary = chunk.get("summary").unwrap_or(&Value::Null);
    value_f64(summary.get("elapsed_seconds"))
        .or_else(|| value_f64(chunk.get("elapsed_seconds")))
        .unwrap_or(0.0)
}

/// Bucket chunk rows into per-window write stats.
///
/// Chunks are written sequentially; each chunk spreads its rows uniformly over
/// its own elapsed time. The measurement spans the first `duration_seconds` of
/// the combined timeline and is split into consecutive `window_seconds`
/// buckets (`duration_seconds` must be a multiple of `window_seconds`).
fn write_throughput_windows(
    chunks: &[Value],
    duration_seconds: u64,
    window_seconds: u64,
) -> Vec<Value> {
    let window_count = (duration_seconds / window_seconds).max(1);
    let mut rows_in_window = vec![0.0f64; window_count as usize];
    let mut position = 0.0f64;
    for chunk in chunks {
        let elapsed = write_chunk_elapsed_seconds(chunk);
        let rows = write_chunk_rows(chunk) as f64;
        if elapsed <= 0.0 || rows <= 0.0 {
            continue;
        }
        let start = position;
        let end = position + elapsed;
        for index in 0..window_count {
            let window_start = index as f64 * window_seconds as f64;
            let window_end = window_start + window_seconds as f64;
            let overlap = end.min(window_end) - start.max(window_start);
            if overlap > 0.0 {
                rows_in_window[index as usize] += rows * (overlap / elapsed);
            }
        }
        position = end;
        if position >= duration_seconds as f64 {
            break;
        }
    }
    (0..window_count)
        .map(|index| {
            json!({
                "index": index,
                "start_offset": index * window_seconds,
                "rows": rows_in_window[index as usize],
                "rps": rows_in_window[index as usize] / window_seconds as f64,
            })
        })
        .collect()
}

fn percentile(values: &[f64], pct: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let index = round_ties_even((pct / 100.0) * (ordered.len() - 1) as f64)
        .clamp(0, ordered.len() as isize - 1) as usize;
    Some(ordered[index])
}

/// Compute write throughput/latency/failure stats from ingestion results.
///
/// Rows and elapsed come from each chunk's generator summary; mean RPS is
/// total rows over total write elapsed. Per-window RPS buckets the chunk rows
/// over the first `duration_seconds`. p50/p99 write-request latency is
/// computed from per-chunk generator durations (each chunk is a sequential set
/// of remote-write requests, so this is a consistent base-vs-candidate proxy).
fn write_throughput_measurement(rw: &Value, write_measure: &WriteMeasure) -> Value {
    let chunks = rw
        .get("chunks")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| vec![rw.clone()]);
    let rows: u64 = chunks.iter().map(write_chunk_rows).sum();
    let elapsed_seconds: f64 = chunks.iter().map(write_chunk_elapsed_seconds).sum();
    let total_chunks = chunks.len();
    let failed = chunks
        .iter()
        .filter(|chunk| chunk["status"].as_str() != Some("ok"))
        .count();
    let latencies_ms = chunks
        .iter()
        .filter(|chunk| chunk["status"].as_str() == Some("ok"))
        .map(write_chunk_elapsed_seconds)
        .filter(|elapsed| *elapsed > 0.0)
        .map(|elapsed| elapsed * 1000.0)
        .collect::<Vec<_>>();
    json!({
        "rows": rows,
        "elapsed_seconds": elapsed_seconds,
        "mean_rps": if elapsed_seconds > 0.0 {
            Some(rows as f64 / elapsed_seconds)
        } else {
            None
        },
        "windows": write_throughput_windows(&chunks, write_measure.duration_seconds, write_measure.window_seconds),
        "p50_latency_ms": percentile(&latencies_ms, 50.0),
        "p99_latency_ms": percentile(&latencies_ms, 99.0),
        "failed_chunks": failed,
        "total_chunks": total_chunks,
        "failure_rate": if total_chunks > 0 {
            Some(failed as f64 / total_chunks as f64)
        } else {
            None
        },
        "target_rps": write_measure.target_rps,
    })
}

/// Dry-run measurement: no subprocess ran, so report planned values.
fn planned_write_throughput_measurement(
    remote: &RemoteWrite,
    write_measure: &WriteMeasure,
) -> Value {
    let rows = remote.series_count * remote.samples_per_series;
    let window_count = (write_measure.duration_seconds / write_measure.window_seconds).max(1);
    json!({
        "status": "planned",
        "planned_rows": rows,
        "rows": rows,
        "elapsed_seconds": Value::Null,
        "mean_rps": Value::Null,
        "windows": (0..window_count)
            .map(|index| json!({
                "index": index,
                "start_offset": index * write_measure.window_seconds,
                "rows": 0.0,
                "rps": 0.0,
            }))
            .collect::<Vec<_>>(),
        "p50_latency_ms": Value::Null,
        "p99_latency_ms": Value::Null,
        "failed_chunks": 0,
        "total_chunks": 0,
        "failure_rate": Value::Null,
        "target_rps": write_measure.target_rps,
    })
}

fn planned_write_throughput_thresholds(write_measure: &WriteMeasure) -> Vec<Value> {
    let thresholds = &write_measure.thresholds;
    let mut planned = vec![
        json!({
            "threshold": "max_failure_rate",
            "status": "planned",
            "limit": thresholds.max_failure_rate,
        }),
        json!({
            "threshold": "max_mean_rps_regression_pct",
            "status": "planned",
            "limit_pct": thresholds.max_mean_rps_regression_pct,
        }),
        json!({
            "threshold": "max_p99_latency_regression_pct",
            "status": "planned",
            "limit_pct": thresholds.max_p99_latency_regression_pct,
        }),
    ];
    if let Some(min_rps) = thresholds.min_rps_absolute {
        planned.push(json!({
            "threshold": "min_rps_absolute",
            "status": "planned",
            "limit": min_rps,
        }));
    }
    planned
}

/// Enforce write_throughput gates.
///
/// Per-target: `max_failure_rate` (failed chunk fraction) and optional
/// `min_rps_absolute` (mean RPS floor). Base-vs-candidate: mean RPS regression
/// pct `(base - candidate) / base * 100` and p99 latency regression pct
/// `(candidate - base) / base * 100`; positive actual means a candidate
/// regression, negative is an improvement.
fn enforce_write_throughput_thresholds(
    write_measure: &WriteMeasure,
    base: &Value,
    candidate: &Value,
) -> Vec<Value> {
    let thresholds = &write_measure.thresholds;
    let mut results = Vec::new();
    for (target, measurement) in [("base", base), ("candidate", candidate)] {
        let failure_rate = value_f64(measurement.get("failure_rate"));
        results.push(json!({
            "target": target,
            "threshold": "max_failure_rate",
            "status": if failure_rate.is_some_and(|rate| rate <= thresholds.max_failure_rate) {
                "passed"
            } else {
                "failed"
            },
            "actual": failure_rate,
            "limit": thresholds.max_failure_rate,
        }));
        if let Some(min_rps) = thresholds.min_rps_absolute {
            let mean_rps = value_f64(measurement.get("mean_rps"));
            results.push(json!({
                "target": target,
                "threshold": "min_rps_absolute",
                "status": if mean_rps.is_some_and(|rps| rps >= min_rps) {
                    "passed"
                } else {
                    "failed"
                },
                "actual": mean_rps,
                "limit": min_rps,
            }));
        }
    }

    let base_rps = value_f64(base.get("mean_rps"));
    let candidate_rps = value_f64(candidate.get("mean_rps"));
    let rps_limit = thresholds.max_mean_rps_regression_pct;
    match (base_rps.filter(|value| *value != 0.0), candidate_rps) {
        (Some(base_rps), Some(candidate_rps)) => {
            let actual = (base_rps - candidate_rps) / base_rps * 100.0;
            results.push(json!({
                "threshold": "max_mean_rps_regression_pct",
                "status": if actual <= rps_limit { "passed" } else { "failed" },
                "actual_pct": actual,
                "limit_pct": rps_limit,
                "base": base_rps,
                "candidate": candidate_rps,
            }));
        }
        _ => results.push(json!({
            "threshold": "max_mean_rps_regression_pct",
            "status": "failed",
            "reason": "missing or zero mean RPS",
            "base": base_rps,
            "candidate": candidate_rps,
        })),
    }

    let base_p99 = value_f64(base.get("p99_latency_ms"));
    let candidate_p99 = value_f64(candidate.get("p99_latency_ms"));
    let p99_limit = thresholds.max_p99_latency_regression_pct;
    match (base_p99.filter(|value| *value != 0.0), candidate_p99) {
        (Some(base_p99), Some(candidate_p99)) => {
            let actual = (candidate_p99 - base_p99) / base_p99 * 100.0;
            results.push(json!({
                "threshold": "max_p99_latency_regression_pct",
                "status": if actual <= p99_limit { "passed" } else { "failed" },
                "actual_pct": actual,
                "limit_pct": p99_limit,
                "base": base_p99,
                "candidate": candidate_p99,
            }));
        }
        _ => results.push(json!({
            "threshold": "max_p99_latency_regression_pct",
            "status": "failed",
            "reason": "missing or zero p99 latency",
            "base": base_p99,
            "candidate": candidate_p99,
        })),
    }
    results
}

/// Resolve the mixed-scenario query SQL.
///
/// Defaults to a count(*) over the remote-write physical table, which scans
/// every ingested row and therefore contends with the write path on the
/// datanode runtime.
fn mix_query_sql(mix: &MixMeasure, remote: &RemoteWrite) -> String {
    if let Some(sql) = &mix.query_sql {
        return sql.clone();
    }
    format!("SELECT count(*) FROM {}", sql_ident(&remote.physical_table))
}

/// Per-thread attempt count for a query loop: queries at t=0, interval,
/// 2*interval, ... while t < duration.
fn expected_mix_query_attempts(duration_seconds: u64, interval_ms: u64, parallelism: u64) -> u64 {
    let per_thread = (duration_seconds as f64 * 1000.0 / (interval_ms.max(1) as f64)).ceil() as u64;
    per_thread * parallelism.max(1)
}

/// Run `query_parallelism` query-loop tasks for `duration_seconds`.
///
/// Each task issues the mix query via HTTP SQL every `query_interval_ms`
/// (per-task wall clock; if a query itself takes longer than the interval the
/// task does not sleep). Returns the collected attempt results in completion
/// order; every attempt carries `ok`, `status`, `latency_ms`, and `sql` from
/// `http_post_sql`.
async fn run_mix_query_loop(
    client: &Client,
    port: u16,
    sql: &str,
    db: &str,
    interval_ms: u64,
    parallelism: u64,
    duration_seconds: u64,
) -> Vec<Value> {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let deadline = Instant::now() + Duration::from_secs(duration_seconds.max(0));
    let mut handles = Vec::new();
    for _ in 0..parallelism.max(1) {
        let client = client.clone();
        let attempts = Arc::clone(&attempts);
        let sql = sql.to_string();
        let db = db.to_string();
        handles.push(tokio::spawn(async move {
            let interval = Duration::from_millis(interval_ms.max(1));
            loop {
                let started = Instant::now();
                if started >= deadline {
                    break;
                }
                let result = http_post_sql(&client, port, &sql, &db).await;
                attempts.lock().expect("attempt lock poisoned").push(result);
                let remaining = interval.saturating_sub(started.elapsed());
                if remaining > Duration::ZERO {
                    tokio::time::sleep(remaining).await;
                }
            }
        }));
    }
    for handle in handles {
        let _ = handle.await;
    }
    Arc::try_unwrap(attempts)
        .expect("all query tasks joined")
        .into_inner()
        .expect("attempt lock poisoned")
}

/// Aggregate query-loop attempts into {samples, failures, failure_rate, p50_ms,
/// p99_ms, mean_ms, latency_samples}.
///
/// Latency percentiles/mean cover every attempt that recorded a latency
/// (including failed ones, so timeouts show up in p99).
fn mix_query_measurement(attempts: &[Value]) -> Value {
    let total = attempts.len();
    let failures = attempts
        .iter()
        .filter(|attempt| attempt["ok"].as_bool() != Some(true))
        .count();
    let latencies = attempts
        .iter()
        .filter_map(|attempt| value_f64(attempt.get("latency_ms")))
        .collect::<Vec<_>>();
    json!({
        "samples": total,
        "failures": failures,
        "failure_rate": if total > 0 {
            Some(failures as f64 / total as f64)
        } else {
            None
        },
        "p50_ms": percentile(&latencies, 50.0),
        "p99_ms": percentile(&latencies, 99.0),
        "mean_ms": if latencies.is_empty() {
            None
        } else {
            Some(latencies.iter().sum::<f64>() / latencies.len() as f64)
        },
        "latency_samples": latencies.len(),
    })
}

/// Dry-run query measurement: no query loop ran, so report planned counts.
fn planned_mix_query_measurement(mix: &MixMeasure, write_measure: &WriteMeasure) -> Value {
    json!({
        "status": "planned",
        "planned_attempts": expected_mix_query_attempts(
            write_measure.duration_seconds,
            mix.query_interval_ms,
            mix.query_parallelism,
        ),
        "query_interval_ms": mix.query_interval_ms,
        "query_parallelism": mix.query_parallelism,
        "samples": 0,
        "failures": 0,
        "failure_rate": Value::Null,
        "p50_ms": Value::Null,
        "p99_ms": Value::Null,
        "mean_ms": Value::Null,
        "latency_samples": 0,
    })
}

fn planned_mix_query_thresholds(mix: &MixMeasure) -> Vec<Value> {
    vec![
        json!({
            "threshold": "max_query_failure_rate",
            "status": "planned",
            "limit": mix.thresholds.max_query_failure_rate,
        }),
        json!({
            "threshold": "max_query_p99_regression_pct",
            "status": "planned",
            "limit_pct": mix.thresholds.max_query_p99_regression_pct,
        }),
    ]
}

/// Enforce the query-side gates of the mixed read/write scenario.
///
/// Per-target: `max_query_failure_rate` (failed query attempts / total).
/// Base-vs-candidate: `max_query_p99_regression_pct`
/// `(candidate_p99_ms - base_p99_ms) / base_p99_ms * 100`; positive actual
/// means a candidate regression, negative is an improvement.
fn enforce_mix_query_thresholds(mix: &MixMeasure, base: &Value, candidate: &Value) -> Vec<Value> {
    let thresholds = &mix.thresholds;
    let mut results = Vec::new();
    let failure_limit = thresholds.max_query_failure_rate;
    for (target, measurement) in [("base", base), ("candidate", candidate)] {
        let failure_rate = value_f64(measurement.get("failure_rate"));
        results.push(json!({
            "target": target,
            "threshold": "max_query_failure_rate",
            "status": if failure_rate.is_some_and(|rate| rate <= failure_limit) {
                "passed"
            } else {
                "failed"
            },
            "actual": failure_rate,
            "limit": failure_limit,
        }));
    }
    let base_p99 = value_f64(base.get("p99_ms"));
    let candidate_p99 = value_f64(candidate.get("p99_ms"));
    let p99_limit = thresholds.max_query_p99_regression_pct;
    match (base_p99.filter(|value| *value != 0.0), candidate_p99) {
        (Some(base_p99), Some(candidate_p99)) => {
            let actual = (candidate_p99 - base_p99) / base_p99 * 100.0;
            results.push(json!({
                "threshold": "max_query_p99_regression_pct",
                "status": if actual <= p99_limit { "passed" } else { "failed" },
                "actual_pct": actual,
                "limit_pct": p99_limit,
                "base": base_p99,
                "candidate": candidate_p99,
            }));
        }
        _ => results.push(json!({
            "threshold": "max_query_p99_regression_pct",
            "status": "failed",
            "reason": "missing or zero query p99 latency",
            "base": base_p99,
            "candidate": candidate_p99,
        })),
    }
    results
}

/// Run remote-write ingestion in a background task while a query loop hammers
/// the same frontend concurrently for `duration_seconds`, then join.
///
/// Each chunk is its own `query_perf_fixture prom-remote-write` subprocess and
/// the query loop issues HTTP /v1/sql requests from this process, so both hit
/// the same frontend/datanode and genuinely contend on the datanode runtime.
/// Returns (rw, flushes, query_attempts); in dry-run mode no task and no query
/// loop is started and attempts is empty.
async fn run_mixed_ingestion_and_queries(
    generator: &Path,
    port: u16,
    remote: &RemoteWrite,
    client: &Client,
    mix: &MixMeasure,
    write_measure: &WriteMeasure,
    dry_run: bool,
) -> Result<(Value, Vec<Value>, Vec<Value>)> {
    if dry_run {
        let (rw, flushes) =
            run_write_throughput_ingestion(generator, port, remote, client, true).await?;
        return Ok((rw, flushes, vec![]));
    }
    let sql = mix_query_sql(mix, remote);
    let database = remote.database.clone();
    let generator = generator.to_path_buf();
    let remote = remote.clone();
    let ingestion_client = client.clone();
    let ingestion = tokio::spawn(async move {
        run_write_throughput_ingestion(&generator, port, &remote, &ingestion_client, false)
            .await
            .map_err(|error| error.to_string())
    });
    let attempts = run_mix_query_loop(
        client,
        port,
        &sql,
        &database,
        mix.query_interval_ms,
        mix.query_parallelism,
        write_measure.duration_seconds,
    )
    .await;
    let (rw, flushes) = ingestion
        .await
        .map_err(|error| format!("write ingestion task failed: {error}"))?
        .map_err(|error| format!("write ingestion failed: {error}"))?;
    Ok((rw, flushes, attempts))
}

fn parse_scheduler_poll_metrics(text: &str) -> HashMap<String, u64> {
    let pattern = Regex::new(SCHEDULER_POLL_PATTERN).expect("scheduler poll regex is valid");
    let mut values = HashMap::new();
    for line in text.lines() {
        if let Some(captures) = pattern.captures(line.trim())
            && let (Some(workload), Some(count)) = (captures.get(1), captures.get(2))
            && let Ok(count) = count.as_str().parse::<u64>()
        {
            values.insert(workload.as_str().to_string(), count);
        }
    }
    values
}

/// Best-effort datanode scheduler-poll snapshot (never a gate).
///
/// Returns {"captured_monotonic_seconds", "values": {workload: polls}} on
/// success or {"error", "values": {}} when the datanode /metrics endpoint is
/// unreachable or the scheduler metric is absent.
async fn scrape_scheduler_polls(client: &Client, port: u16) -> Value {
    match client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()
        .await
    {
        Ok(response) => match response.text().await {
            Ok(text) => json!({
                "captured_monotonic_seconds": Instant::now().elapsed().as_secs_f64(),
                "values": parse_scheduler_poll_metrics(&text),
            }),
            Err(error) => json!({
                "error": error.to_string(),
                "values": {},
            }),
        },
        Err(error) => json!({
            "error": error.to_string(),
            "values": {},
        }),
    }
}

/// Delta of cumulative scheduler polls between two snapshots, per workload.
///
/// A workload whose counter is missing in either snapshot, or that decreased
/// (counter reset/restart), is reported as null because its delta is unknown.
fn scheduler_poll_deltas(after: &Value, before: &Value) -> Value {
    let mut result = serde_json::Map::new();
    for workload in ["query", "write"] {
        let before_value = before["values"].get(workload).and_then(Value::as_u64);
        let after_value = after["values"].get(workload).and_then(Value::as_u64);
        let delta = match (before_value, after_value) {
            (Some(before_value), Some(after_value)) => {
                let delta = after_value as i128 - before_value as i128;
                (delta >= 0).then_some(delta)
            }
            _ => None,
        };
        result.insert(workload.to_string(), json!(delta));
    }
    Value::Object(result)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::query_regression_runner::model::{
        MixMeasureThresholds, PromStore, RemoteValue, WorkloadSchedulerConfig,
        WriteThroughputThresholds,
    };

    fn make_remote() -> RemoteWrite {
        RemoteWrite {
            database: "public".to_string(),
            metric: "write_throughput_scheduler".to_string(),
            physical_table: "greptime_physical_table".to_string(),
            series_count: 2048,
            samples_per_series: 3600,
            start_unix_millis: 1_704_067_200_000,
            step_millis: 1000,
            chunk_series_count: 256,
            timeout_seconds: 120,
            sample_chunk_size: None,
            flush_every_sample_chunks: 1,
            visibility_timeout_seconds: 30,
            prom_store: PromStore {
                pending_rows_flush_interval: "1s".to_string(),
                max_batch_rows: 1_000_000,
                max_concurrent_flushes: 256,
                worker_channel_capacity: 65_526,
                max_inflight_requests: 3000,
            },
            value: RemoteValue {
                pattern: "linear".to_string(),
                base: 0.0,
                step: 0.125,
                cardinality: 97,
                seed: 0,
                run_length: 8,
                stall_every: 100,
                stall_length: 16,
                mixed_every: 5,
            },
            storage: None,
            read_bench: None,
        }
    }

    fn make_write_measure() -> WriteMeasure {
        WriteMeasure {
            duration_seconds: 60,
            window_seconds: 5,
            target_rps: 0.0,
            thresholds: WriteThroughputThresholds {
                max_failure_rate: 0.05,
                max_mean_rps_regression_pct: 10.0,
                max_p99_latency_regression_pct: 10.0,
                min_rps_absolute: Some(50_000.0),
            },
            mix: None,
        }
    }

    fn make_mix() -> MixMeasure {
        MixMeasure {
            query_sql: None,
            query_interval_ms: 100,
            query_parallelism: 2,
            thresholds: MixMeasureThresholds {
                max_query_failure_rate: 0.05,
                max_query_p99_regression_pct: 10.0,
            },
        }
    }

    fn ok_chunk(rows: u64, elapsed_seconds: f64) -> Value {
        json!({ "status": "ok", "summary": { "rows": rows, "elapsed_seconds": elapsed_seconds } })
    }

    #[test]
    fn windows_bucket_rows_within_a_window() {
        let chunks = vec![ok_chunk(100, 1.0), ok_chunk(100, 1.0)];
        let windows = write_throughput_windows(&chunks, 10, 5);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0]["start_offset"], 0);
        assert_eq!(windows[0]["rows"], 200.0);
        assert_eq!(windows[0]["rps"], 40.0);
        assert_eq!(windows[1]["start_offset"], 5);
        assert_eq!(windows[1]["rows"], 0.0);
        assert_eq!(windows[1]["rps"], 0.0);
    }

    #[test]
    fn windows_split_rows_across_window_boundaries() {
        let chunks = vec![ok_chunk(100, 10.0)];
        let windows = write_throughput_windows(&chunks, 10, 5);
        assert_eq!(
            windows
                .iter()
                .map(|w| w["rows"].as_f64().unwrap())
                .collect::<Vec<_>>(),
            vec![50.0, 50.0]
        );
        assert_eq!(
            windows
                .iter()
                .map(|w| w["rps"].as_f64().unwrap())
                .collect::<Vec<_>>(),
            vec![10.0, 10.0]
        );
    }

    #[test]
    fn windows_attribute_partial_overlaps_proportionally() {
        let chunks = vec![ok_chunk(100, 2.0), ok_chunk(100, 2.0), ok_chunk(100, 2.0)];
        let windows = write_throughput_windows(&chunks, 6, 3);
        assert_eq!(windows.len(), 2);
        assert_eq!(
            windows
                .iter()
                .map(|w| w["rows"].as_f64().unwrap())
                .collect::<Vec<_>>(),
            vec![150.0, 150.0]
        );
        assert_eq!(
            windows
                .iter()
                .map(|w| w["rps"].as_f64().unwrap())
                .collect::<Vec<_>>(),
            vec![50.0, 50.0]
        );
    }

    #[test]
    fn windows_truncate_at_duration() {
        let chunks = vec![ok_chunk(100, 10.0)];
        let windows = write_throughput_windows(&chunks, 5, 5);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0]["rows"], 50.0);
    }

    #[test]
    fn measurement_computes_rps_latency_and_failure_rate() {
        let chunks = vec![
            ok_chunk(1000, 0.05),
            ok_chunk(1000, 0.10),
            ok_chunk(1000, 0.15),
        ];
        let rw = json!({ "chunks": chunks });
        let mut write_measure = make_write_measure();
        write_measure.duration_seconds = 10;
        write_measure.window_seconds = 5;
        let measurement = write_throughput_measurement(&rw, &write_measure);
        assert_eq!(measurement["rows"], 3000);
        assert!(
            (measurement["elapsed_seconds"].as_f64().unwrap() - 0.30).abs() < 1e-9,
            "elapsed_seconds = {:?}",
            measurement["elapsed_seconds"]
        );
        assert!(
            (measurement["mean_rps"].as_f64().unwrap() - 10_000.0).abs() < 1e-6,
            "mean_rps = {:?}",
            measurement["mean_rps"]
        );
        assert_eq!(measurement["p50_latency_ms"], 100.0);
        assert_eq!(measurement["p99_latency_ms"], 150.0);
        assert_eq!(measurement["failed_chunks"], 0);
        assert_eq!(measurement["total_chunks"], 3);
        assert_eq!(measurement["failure_rate"], 0.0);
    }

    #[test]
    fn measurement_counts_failed_chunks_and_excludes_their_latency() {
        let chunks = vec![
            ok_chunk(1000, 1.0),
            json!({ "status": "failed", "returncode": 1, "elapsed_seconds": 0.5 }),
        ];
        let measurement =
            write_throughput_measurement(&json!({ "chunks": chunks }), &make_write_measure());
        assert_eq!(measurement["rows"], 1000);
        assert_eq!(measurement["failed_chunks"], 1);
        assert_eq!(measurement["failure_rate"], 0.5);
        assert_eq!(measurement["p50_latency_ms"], 1000.0);
        assert_eq!(measurement["p99_latency_ms"], 1000.0);
    }

    #[test]
    fn measurement_treats_single_invocation_result_as_one_chunk() {
        let rw = json!({ "status": "ok", "returncode": 0, "summary": { "rows": 5000, "elapsed_seconds": 2.0 } });
        let mut write_measure = make_write_measure();
        write_measure.duration_seconds = 10;
        write_measure.window_seconds = 5;
        let measurement = write_throughput_measurement(&rw, &write_measure);
        assert_eq!(measurement["rows"], 5000);
        assert_eq!(measurement["mean_rps"], 2500.0);
        assert_eq!(measurement["failed_chunks"], 0);
        assert_eq!(measurement["total_chunks"], 1);
        assert_eq!(measurement["failure_rate"], 0.0);
        assert_eq!(measurement["p50_latency_ms"], 2000.0);
        assert_eq!(measurement["windows"][0]["rows"], 5000.0);
    }

    #[test]
    fn planned_measurement_reports_expected_rows_and_windows() {
        let measurement =
            planned_write_throughput_measurement(&make_remote(), &make_write_measure());
        assert_eq!(measurement["status"], "planned");
        assert_eq!(measurement["planned_rows"], 2048 * 3600);
        assert!(measurement["mean_rps"].is_null());
        assert_eq!(measurement["windows"].as_array().unwrap().len(), 12);
        assert_eq!(measurement["windows"][1]["start_offset"], 5);
    }

    #[test]
    fn enforcement_passes_when_within_limits() {
        let base = json!({ "mean_rps": 100_000, "p99_latency_ms": 50.0, "failure_rate": 0.0 });
        let candidate = json!({ "mean_rps": 90_000, "p99_latency_ms": 55.0, "failure_rate": 0.02 });
        let results = enforce_write_throughput_thresholds(&make_write_measure(), &base, &candidate);
        assert_eq!(results.len(), 6);
        let by_key = results
            .iter()
            .map(|r| {
                (
                    r["target"].as_str().unwrap_or(""),
                    r["threshold"].as_str().unwrap_or(""),
                    r,
                )
            })
            .collect::<Vec<_>>();
        for (target, threshold, result) in &by_key {
            if *threshold != "max_mean_rps_regression_pct"
                && *threshold != "max_p99_latency_regression_pct"
            {
                assert_eq!(result["status"], "passed", "{target} {threshold}");
            }
        }
        let rps = by_key
            .iter()
            .find(|(_, threshold, _)| *threshold == "max_mean_rps_regression_pct")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(rps["status"], "passed");
        assert_eq!(rps["actual_pct"], 10.0);
        assert_eq!(rps["limit_pct"], 10.0);
        let p99 = by_key
            .iter()
            .find(|(_, threshold, _)| *threshold == "max_p99_latency_regression_pct")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(p99["status"], "passed");
        assert_eq!(p99["actual_pct"], 10.0);
    }

    #[test]
    fn enforcement_fails_when_regression_exceeds_limits() {
        let base = json!({ "mean_rps": 100_000, "p99_latency_ms": 50.0, "failure_rate": 0.0 });
        let candidate = json!({ "mean_rps": 80_000, "p99_latency_ms": 60.0, "failure_rate": 0.10 });
        let results = enforce_write_throughput_thresholds(&make_write_measure(), &base, &candidate);
        let by_key = results
            .iter()
            .map(|r| {
                (
                    r["target"].as_str().unwrap_or(""),
                    r["threshold"].as_str().unwrap_or(""),
                    r,
                )
            })
            .collect::<Vec<_>>();
        let candidate_failure = by_key
            .iter()
            .find(|(target, threshold, _)| {
                *target == "candidate" && *threshold == "max_failure_rate"
            })
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(candidate_failure["status"], "failed");
        let rps = by_key
            .iter()
            .find(|(_, threshold, _)| *threshold == "max_mean_rps_regression_pct")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(rps["status"], "failed");
        assert_eq!(rps["actual_pct"], 20.0);
        let p99 = by_key
            .iter()
            .find(|(_, threshold, _)| *threshold == "max_p99_latency_regression_pct")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(p99["status"], "failed");
        assert_eq!(p99["actual_pct"], 20.0);
    }

    #[test]
    fn enforcement_fails_on_missing_base_measurement() {
        let base = json!({ "mean_rps": Value::Null, "p99_latency_ms": Value::Null, "failure_rate": Value::Null });
        let candidate = json!({ "mean_rps": 90_000, "p99_latency_ms": 55.0, "failure_rate": 0.0 });
        let results = enforce_write_throughput_thresholds(&make_write_measure(), &base, &candidate);
        let by_key = results
            .iter()
            .map(|r| {
                (
                    r["target"].as_str().unwrap_or(""),
                    r["threshold"].as_str().unwrap_or(""),
                    r,
                )
            })
            .collect::<Vec<_>>();
        let rps = by_key
            .iter()
            .find(|(_, threshold, _)| *threshold == "max_mean_rps_regression_pct")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(rps["status"], "failed");
        assert_eq!(rps["reason"], "missing or zero mean RPS");
        let p99 = by_key
            .iter()
            .find(|(_, threshold, _)| *threshold == "max_p99_latency_regression_pct")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(p99["status"], "failed");
        let base_failure = by_key
            .iter()
            .find(|(target, threshold, _)| *target == "base" && *threshold == "max_failure_rate")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(base_failure["status"], "failed");
    }

    #[test]
    fn planned_thresholds_include_optional_min_rps() {
        let planned = planned_write_throughput_thresholds(&make_write_measure());
        assert_eq!(
            planned
                .iter()
                .map(|p| p["threshold"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "max_failure_rate",
                "max_mean_rps_regression_pct",
                "max_p99_latency_regression_pct",
                "min_rps_absolute"
            ]
        );
        assert!(planned.iter().all(|p| p["status"] == "planned"));

        let mut without_min = make_write_measure();
        without_min.thresholds.min_rps_absolute = None;
        let planned = planned_write_throughput_thresholds(&without_min);
        assert_eq!(
            planned
                .iter()
                .map(|p| p["threshold"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "max_failure_rate",
                "max_mean_rps_regression_pct",
                "max_p99_latency_regression_pct"
            ]
        );
    }

    #[test]
    fn scheduler_env_absent_section_disables_both() {
        assert!(scheduler_env(false, None).is_none());
        assert!(scheduler_env(true, None).is_none());
    }

    #[test]
    fn scheduler_env_derivation_base_disables_candidate_enables() {
        let scheduler = WorkloadSchedulerConfig {
            enable: false,
            max_concurrent_polls: 16,
            query_weight: 2,
            write_weight: 8,
        };
        let base_env = scheduler_env(false, Some(&scheduler)).unwrap();
        assert_eq!(
            base_env,
            HashMap::from([(
                format!("{SCHEDULER_ENV_PREFIX}__ENABLE"),
                "false".to_string()
            )])
        );
        let candidate_env = scheduler_env(true, Some(&scheduler)).unwrap();
        assert_eq!(
            candidate_env,
            HashMap::from([
                (
                    format!("{SCHEDULER_ENV_PREFIX}__ENABLE"),
                    "true".to_string()
                ),
                (
                    format!("{SCHEDULER_ENV_PREFIX}__MAX_CONCURRENT_POLLS"),
                    "16".to_string()
                ),
                (
                    format!("{SCHEDULER_ENV_PREFIX}__QUERY_WEIGHT"),
                    "2".to_string()
                ),
                (
                    format!("{SCHEDULER_ENV_PREFIX}__WRITE_WEIGHT"),
                    "8".to_string()
                ),
            ])
        );
    }

    #[test]
    fn scheduler_report_entry_matches_python() {
        let scheduler = WorkloadSchedulerConfig {
            enable: false,
            max_concurrent_polls: 16,
            query_weight: 2,
            write_weight: 8,
        };
        assert_eq!(
            scheduler_report_entry("base", Some(&scheduler)),
            json!({ "enabled": false, "max_concurrent_polls": 16, "query_weight": 2, "write_weight": 8 })
        );
        assert_eq!(
            scheduler_report_entry("candidate", Some(&scheduler)),
            json!({ "enabled": true, "max_concurrent_polls": 16, "query_weight": 2, "write_weight": 8 })
        );
        assert_eq!(
            scheduler_report_entry("base", None),
            json!({ "enabled": false, "max_concurrent_polls": 0, "query_weight": 2, "write_weight": 8 })
        );
        assert_eq!(
            scheduler_report_entry("candidate", None),
            json!({ "enabled": false, "max_concurrent_polls": 0, "query_weight": 2, "write_weight": 8 })
        );
    }

    #[test]
    fn expected_attempts_math() {
        assert_eq!(expected_mix_query_attempts(60, 100, 2), 1200);
        assert_eq!(expected_mix_query_attempts(10, 3000, 1), 4);
        assert_eq!(expected_mix_query_attempts(60, 1000, 4), 240);
        assert_eq!(expected_mix_query_attempts(60, 100, 0), 600);
    }

    #[test]
    fn mix_query_sql_default_and_override() {
        let mix = make_mix();
        assert_eq!(
            mix_query_sql(&mix, &make_remote()),
            "SELECT count(*) FROM \"greptime_physical_table\""
        );
        let mut overridden = make_mix();
        overridden.query_sql =
            Some("SELECT max(greptime_value) FROM greptime_physical_table".to_string());
        assert_eq!(
            mix_query_sql(&overridden, &make_remote()),
            "SELECT max(greptime_value) FROM greptime_physical_table"
        );
    }

    #[test]
    fn measurement_aggregates_latency_and_failures() {
        let attempts = vec![
            json!({ "ok": true, "latency_ms": 10.0 }),
            json!({ "ok": true, "latency_ms": 20.0 }),
            json!({ "ok": true, "latency_ms": 30.0 }),
            json!({ "ok": false, "latency_ms": 40.0 }),
            json!({ "ok": false, "latency_ms": 50.0 }),
        ];
        let measurement = mix_query_measurement(&attempts);
        assert_eq!(measurement["samples"], 5);
        assert_eq!(measurement["failures"], 2);
        assert_eq!(measurement["failure_rate"], 0.4);
        assert_eq!(measurement["latency_samples"], 5);
        assert_eq!(measurement["p50_ms"], 30.0);
        assert_eq!(measurement["p99_ms"], 50.0);
        assert_eq!(measurement["mean_ms"], 30.0);
    }

    #[test]
    fn measurement_empty_attempts() {
        let measurement = mix_query_measurement(&[]);
        assert_eq!(measurement["samples"], 0);
        assert_eq!(measurement["failures"], 0);
        assert!(measurement["failure_rate"].is_null());
        assert!(measurement["p50_ms"].is_null());
        assert!(measurement["p99_ms"].is_null());
        assert!(measurement["mean_ms"].is_null());
    }

    #[test]
    fn planned_measurement_reports_expected_attempts() {
        let measurement = planned_mix_query_measurement(&make_mix(), &make_write_measure());
        assert_eq!(measurement["status"], "planned");
        assert_eq!(measurement["planned_attempts"], 1200);
        assert_eq!(measurement["query_interval_ms"], 100);
        assert_eq!(measurement["query_parallelism"], 2);
        assert!(measurement["p99_ms"].is_null());
    }

    #[test]
    fn combined_write_and_query_gates() {
        let write_base =
            json!({ "mean_rps": 100_000, "p99_latency_ms": 50.0, "failure_rate": 0.0 });
        let write_candidate =
            json!({ "mean_rps": 90_000, "p99_latency_ms": 55.0, "failure_rate": 0.02 });
        let query_base = json!({ "p99_ms": 200.0, "failure_rate": 0.0 });
        let query_candidate = json!({ "p99_ms": 220.0, "failure_rate": 0.01 });
        let mut combined = enforce_write_throughput_thresholds(
            &make_write_measure(),
            &write_base,
            &write_candidate,
        );
        combined.extend(enforce_mix_query_thresholds(
            &make_mix(),
            &query_base,
            &query_candidate,
        ));
        assert_eq!(combined.len(), 9);
        let by_key = combined
            .iter()
            .map(|r| {
                (
                    r["target"].as_str().unwrap_or(""),
                    r["threshold"].as_str().unwrap_or(""),
                    r,
                )
            })
            .collect::<Vec<_>>();
        let rps = by_key
            .iter()
            .find(|(_, threshold, _)| *threshold == "max_mean_rps_regression_pct")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(rps["status"], "passed");
        let q99 = by_key
            .iter()
            .find(|(_, threshold, _)| *threshold == "max_query_p99_regression_pct")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(q99["status"], "passed");
        assert_eq!(q99["actual_pct"], 10.0);
        assert_eq!(q99["limit_pct"], 10.0);
    }

    #[test]
    fn query_gates_fail_on_regression_and_failures() {
        let query_base = json!({ "p99_ms": 200.0, "failure_rate": 0.0 });
        let query_candidate = json!({ "p99_ms": 260.0, "failure_rate": 0.10 });
        let results = enforce_mix_query_thresholds(&make_mix(), &query_base, &query_candidate);
        let by_key = results
            .iter()
            .map(|r| {
                (
                    r["target"].as_str().unwrap_or(""),
                    r["threshold"].as_str().unwrap_or(""),
                    r,
                )
            })
            .collect::<Vec<_>>();
        let candidate_failure = by_key
            .iter()
            .find(|(target, threshold, _)| {
                *target == "candidate" && *threshold == "max_query_failure_rate"
            })
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(candidate_failure["status"], "failed");
        let q99 = by_key
            .iter()
            .find(|(_, threshold, _)| *threshold == "max_query_p99_regression_pct")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(q99["status"], "failed");
        assert_eq!(q99["actual_pct"], 30.0);
    }

    #[test]
    fn query_gates_fail_on_missing_base() {
        let query_base = json!({ "p99_ms": Value::Null, "failure_rate": Value::Null });
        let query_candidate = json!({ "p99_ms": 260.0, "failure_rate": 0.0 });
        let results = enforce_mix_query_thresholds(&make_mix(), &query_base, &query_candidate);
        let by_key = results
            .iter()
            .map(|r| {
                (
                    r["target"].as_str().unwrap_or(""),
                    r["threshold"].as_str().unwrap_or(""),
                    r,
                )
            })
            .collect::<Vec<_>>();
        let base_failure = by_key
            .iter()
            .find(|(target, threshold, _)| {
                *target == "base" && *threshold == "max_query_failure_rate"
            })
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(base_failure["status"], "failed");
        let q99 = by_key
            .iter()
            .find(|(_, threshold, _)| *threshold == "max_query_p99_regression_pct")
            .map(|(_, _, r)| *r)
            .unwrap();
        assert_eq!(q99["status"], "failed");
        assert_eq!(q99["reason"], "missing or zero query p99 latency");
    }

    #[test]
    fn planned_mix_query_thresholds_entry() {
        let planned = planned_mix_query_thresholds(&make_mix());
        assert_eq!(
            planned
                .iter()
                .map(|p| p["threshold"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["max_query_failure_rate", "max_query_p99_regression_pct"]
        );
        assert!(planned.iter().all(|p| p["status"] == "planned"));
    }

    #[test]
    fn parse_scheduler_poll_metrics_parses_labels() {
        let text = "# HELP greptime_workload_scheduler_polls Cumulative task polls admitted by the workload scheduler\n\
            # TYPE greptime_workload_scheduler_polls gauge\n\
            greptime_workload_scheduler_polls{workload=\"query\"} 1234\n\
            greptime_workload_scheduler_polls{workload=\"write\"} 5678\n\
            greptime_workload_scheduler_queued_tasks{workload=\"query\"} 3\n\
            greptime_runtime_threads_alive{thread_name=\"global\"} 8\n";
        assert_eq!(
            parse_scheduler_poll_metrics(text),
            HashMap::from([("query".to_string(), 1234), ("write".to_string(), 5678),])
        );
        assert!(parse_scheduler_poll_metrics("greptime_runtime_threads_alive 8\n").is_empty());
    }

    #[test]
    fn scheduler_poll_deltas_match_python() {
        let before = json!({ "values": { "query": 100, "write": 900 } });
        let after = json!({ "values": { "query": 180, "write": 1780 } });
        assert_eq!(
            scheduler_poll_deltas(&after, &before),
            json!({ "query": 80, "write": 880 })
        );
    }

    #[test]
    fn scheduler_poll_deltas_unknown_on_missing_or_reset() {
        let before = json!({ "values": { "query": 100 } });
        let after = json!({ "values": { "query": 180, "write": 20 } });
        assert_eq!(
            scheduler_poll_deltas(&after, &before),
            json!({ "query": 80, "write": Value::Null })
        );
        let before = json!({ "values": { "query": 100, "write": 900 } });
        let after = json!({ "values": {} });
        assert_eq!(
            scheduler_poll_deltas(&after, &before),
            json!({ "query": Value::Null, "write": Value::Null })
        );
    }

    #[test]
    fn remote_write_command_includes_chunk_overrides() {
        let remote = make_remote();
        assert!(sample_chunks(&remote).unwrap().is_none());
        let mut chunked = make_remote();
        chunked.sample_chunk_size = Some(180);
        let chunks = sample_chunks(&chunked).unwrap().unwrap();
        assert_eq!(chunks.len(), 20);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].samples_per_series, 180);
        assert_eq!(chunks[0].start_unix_millis, 1_704_067_200_000);
        assert_eq!(chunks[19].offset, 3420);
        assert_eq!(chunks[19].samples_per_series, 180);
        let command = remote_write_command(Path::new("/bin/qpf"), 4000, &chunked, Some(&chunks[1]));
        let command = command.join(" ");
        assert!(command.contains("--value-sample-offset 180"), "{command}");
        assert!(
            command.contains("--value-total-samples-per-series 3600"),
            "{command}"
        );
        assert!(command.contains("--samples-per-series 180"), "{command}");
    }

    #[test]
    fn summarize_write_chunks_plans_dry_run_aggregate() {
        let remote = make_remote();
        let chunks = vec![
            json!({ "status": "dry-run", "command": ["/bin/qpf", "prom-remote-write"] }),
            json!({ "status": "dry-run", "command": ["/bin/qpf", "prom-remote-write"] }),
        ];
        let summary = summarize_write_chunks(&chunks, &remote, true);
        assert_eq!(summary["rows"], 2048 * 3600);
        assert_eq!(summary["samples_written"], 2048 * 3600);
        assert_eq!(summary["batches"], 2 * ((2048u64 + 255) / 256));
    }

    #[test]
    fn dry_run_report_plans_without_subprocess() {
        // Pure-function slice of the dry-run path: planned measurements and
        // thresholds line up with the Python planned_* helpers.
        let measurement =
            planned_write_throughput_measurement(&make_remote(), &make_write_measure());
        assert_eq!(measurement["status"], "planned");
        let mix = make_mix();
        let mix_measurement = planned_mix_query_measurement(&mix, &make_write_measure());
        assert_eq!(mix_measurement["status"], "planned");
        let mut thresholds = planned_write_throughput_thresholds(&make_write_measure());
        thresholds.extend(planned_mix_query_thresholds(&mix));
        assert_eq!(
            thresholds
                .iter()
                .map(|t| t["threshold"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "max_failure_rate",
                "max_mean_rps_regression_pct",
                "max_p99_latency_regression_pct",
                "min_rps_absolute",
                "max_query_failure_rate",
                "max_query_p99_regression_pct",
            ]
        );
        assert!(thresholds.iter().all(|t| t["status"] == "planned"));
    }

    #[test]
    fn target_cluster_layout_matches_python() {
        let ports = vec![4000, 4001, 4002, 4003, 4004, 4005, 4006, 4007];
        let cluster =
            TargetCluster::new(PathBuf::from("/bin/true"), PathBuf::from("/tmp/wt"), &ports)
                .unwrap();
        assert_eq!(cluster.metasrv_rpc_port, 4000);
        assert_eq!(cluster.metasrv_http_port, 4001);
        assert_eq!(cluster.datanode_rpc_port, 4002);
        assert_eq!(cluster.datanode_http_port, 4003);
        assert_eq!(cluster.http_port, 4004);
        assert_eq!(cluster.grpc_port, 4005);
        assert_eq!(cluster.mysql_port, 4006);
        assert_eq!(cluster.postgres_port, 4007);
        assert_eq!(
            cluster.datanode_data_dir,
            PathBuf::from("/tmp/wt/datanode-0/data")
        );
    }
}
