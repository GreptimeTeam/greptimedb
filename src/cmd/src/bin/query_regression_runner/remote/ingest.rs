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

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use reqwest::Client;
use serde_json::{Value, json};

use crate::query_regression_runner::model::{PromStore, RemoteWrite};
use crate::query_regression_runner::plan::normalized_remote_write;
use crate::query_regression_runner::sql::{
    extract_count_value, http_post_sql, sql_ident, sql_string, value_f64, value_u64,
};
use crate::query_regression_runner::{PrepareRemoteArgs, RenderRemoteConfigArgs, Result};

pub(super) async fn run_render_remote_config(args: RenderRemoteConfigArgs) -> Result<()> {
    let (_, remote) = normalized_remote_write(&args.fixture_generator, &args.case)?;
    fs::write(args.output, frontend_prom_config(&remote.prom_store)?)?;
    Ok(())
}

pub(crate) fn frontend_prom_config(prom: &PromStore) -> Result<String> {
    Ok(format!(
        "[prom_store]\nenable = true\nwith_metric_engine = true\npending_rows_flush_interval = {}\nmax_batch_rows = {}\nmax_concurrent_flushes = {}\nworker_channel_capacity = {}\nmax_inflight_requests = {}\n",
        serde_json::to_string(&prom.pending_rows_flush_interval)?,
        prom.max_batch_rows,
        prom.max_concurrent_flushes,
        prom.worker_channel_capacity,
        prom.max_inflight_requests,
    ))
}

pub(super) async fn run_prepare_remote(args: PrepareRemoteArgs) -> Result<()> {
    if !args.http_timeout.is_finite() || args.http_timeout < 0.0 {
        return Err("--http-timeout must be a non-negative finite number".into());
    }
    let (case_path, remote) = normalized_remote_write(&args.fixture_generator, &args.case)?;
    let client = Client::builder()
        .timeout(Duration::from_secs_f64(args.http_timeout))
        .build()?;
    let base = prepare_remote_target(
        "base",
        args.base_http_port,
        &args.fixture_generator,
        &remote,
        &client,
    )
    .await?;
    let candidate = prepare_remote_target(
        "candidate",
        args.candidate_http_port,
        &args.fixture_generator,
        &remote,
        &client,
    )
    .await?;
    let report = json!({
        "case_path": case_path,
        "scenario": "prom_remote_write_then_query",
        "base": base,
        "candidate": candidate,
        "status": "ok",
    });
    let text = format!("{}\n", serde_json::to_string_pretty(&report)?);
    if let Some(path) = args.output {
        fs::write(path, &text)?;
    }
    print!("{text}");
    Ok(())
}

async fn prepare_remote_target(
    name: &str,
    port: u16,
    generator: &Path,
    remote: &RemoteWrite,
    client: &Client,
) -> Result<Value> {
    let create_database = http_post_sql(
        client,
        port,
        &format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            sql_ident(&remote.database)
        ),
        "public",
    )
    .await;
    if !create_database["ok"].as_bool().unwrap_or(false) {
        return Err(format!(
            "CREATE DATABASE {} failed for {name}: {create_database}",
            remote.database
        )
        .into());
    }
    let (remote_write, flushes) = ingest_remote_write(generator, port, remote, client).await?;
    let expected_rows = remote
        .series_count
        .checked_mul(remote.samples_per_series)
        .ok_or("expected remote-write row count overflows u64")?;
    let visibility = poll_expected_count(
        client,
        port,
        &remote.metric,
        &remote.database,
        expected_rows,
        remote.visibility_timeout_seconds,
    )
    .await?;
    Ok(json!({
        "name": name,
        "create_database": create_database,
        "remote_write": remote_write,
        "flushes": flushes,
        "visibility": visibility,
        "status": "ok",
    }))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SampleChunk {
    pub(crate) index: u64,
    pub(crate) offset: u64,
    pub(crate) samples_per_series: u64,
    pub(crate) start_unix_millis: i64,
}

async fn ingest_remote_write(
    generator: &Path,
    port: u16,
    remote: &RemoteWrite,
    client: &Client,
) -> Result<(Value, Vec<Value>)> {
    let Some(chunks) = sample_chunks(remote)? else {
        let remote_write = run_remote_write(generator, port, remote, None)?;
        let flush = flush_remote_table(client, port, remote, "final", None).await?;
        return Ok((remote_write, vec![flush]));
    };
    let mut results = Vec::with_capacity(chunks.len());
    let mut flushes = Vec::new();
    let scheduled_flushes = scheduled_flushes(&chunks, remote.flush_every_sample_chunks);
    for chunk in &chunks {
        let mut result = run_remote_write(generator, port, remote, Some(chunk))?;
        result
            .as_object_mut()
            .ok_or("remote-write result must be an object")?
            .extend([
                ("sample_offset".to_string(), json!(chunk.offset)),
                (
                    "samples_per_series".to_string(),
                    json!(chunk.samples_per_series),
                ),
                ("chunk_index".to_string(), json!(chunk.index)),
            ]);
        results.push(result);
        if let Some((_, reason)) = scheduled_flushes
            .iter()
            .find(|(index, _)| *index == chunk.index)
        {
            flushes
                .push(flush_remote_table(client, port, remote, reason, Some(chunk.index)).await?);
        }
    }
    Ok((
        json!({
            "status": "ok",
            "mode": "sample-chunked",
            "sample_chunk_size": remote.sample_chunk_size,
            "flush_every_sample_chunks": remote.flush_every_sample_chunks,
            "chunks": results,
            "aggregate": summarize_remote_chunks(&results),
        }),
        flushes,
    ))
}

pub(crate) fn sample_chunks(remote: &RemoteWrite) -> Result<Option<Vec<SampleChunk>>> {
    let Some(chunk_samples) = remote.sample_chunk_size else {
        return Ok(None);
    };
    if chunk_samples == 0 {
        return Err("scenario.remote_write.sample_chunk_size must be positive".into());
    }
    if remote.flush_every_sample_chunks == 0 {
        return Err("scenario.remote_write.flush_every_sample_chunks must be positive".into());
    }
    let mut chunks = Vec::new();
    let mut offset = 0;
    while offset < remote.samples_per_series {
        let samples_per_series = chunk_samples.min(remote.samples_per_series - offset);
        chunks.push(SampleChunk {
            index: chunks.len() as u64 + 1,
            offset,
            samples_per_series,
            start_unix_millis: remote.start_unix_millis + offset as i64 * remote.step_millis,
        });
        offset += samples_per_series;
    }
    Ok(Some(chunks))
}

pub(crate) fn scheduled_flushes(
    chunks: &[SampleChunk],
    flush_every: u64,
) -> Vec<(u64, &'static str)> {
    let mut scheduled = chunks
        .iter()
        .filter(|chunk| chunk.index % flush_every == 0)
        .map(|chunk| (chunk.index, "periodic"))
        .collect::<Vec<_>>();
    if let Some(chunk) = chunks.last()
        && chunk.index % flush_every != 0
    {
        scheduled.push((chunk.index, "final"));
    }
    scheduled
}

fn summarize_remote_chunks(chunks: &[Value]) -> Value {
    let mut rows = 0;
    let mut samples_written = 0;
    let mut batches = 0;
    let mut elapsed_seconds = 0.0;
    for chunk in chunks {
        let summary = chunk.get("summary").unwrap_or(&Value::Null);
        let chunk_rows = value_u64(summary.get("rows")).unwrap_or(0);
        rows += chunk_rows;
        samples_written += value_u64(summary.get("samples_written")).unwrap_or(chunk_rows);
        batches += value_u64(summary.get("batches")).unwrap_or(0);
        elapsed_seconds += value_f64(summary.get("elapsed_seconds"))
            .or_else(|| value_f64(chunk.get("elapsed_seconds")))
            .unwrap_or(0.0);
    }
    json!({ "rows": rows, "samples_written": samples_written, "batches": batches, "elapsed_seconds": elapsed_seconds })
}

fn run_remote_write(
    generator: &Path,
    port: u16,
    remote: &RemoteWrite,
    chunk: Option<&SampleChunk>,
) -> Result<Value> {
    let command = remote_write_command(generator, port, remote, chunk);
    let started = Instant::now();
    let output = Command::new(generator).args(&command[1..]).output()?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        return Err(format!(
            "remote-write generator failed: command={command:?}, returncode={:?}, elapsed_seconds={elapsed_seconds:.3}, stderr={:.2000}",
            output.status.code(), stderr
        )
        .into());
    }
    let mut result = json!({
        "status": "ok",
        "command": command,
        "returncode": output.status.code(),
        "elapsed_seconds": elapsed_seconds,
        "stdout": stdout,
        "stderr": stderr,
    });
    match serde_json::from_str(result["stdout"].as_str().unwrap_or_default()) {
        Ok(summary) => result["summary"] = summary,
        Err(_) => result["summary_parse_error"] = result["stdout"].clone(),
    }
    Ok(result)
}

pub(crate) fn remote_write_command(
    generator: &Path,
    port: u16,
    remote: &RemoteWrite,
    chunk: Option<&SampleChunk>,
) -> Vec<String> {
    let (samples_per_series, start_unix_millis, sample_offset, total_samples_per_series) = chunk
        .map(|chunk| {
            (
                chunk.samples_per_series,
                chunk.start_unix_millis,
                Some(chunk.offset),
                Some(remote.samples_per_series),
            )
        })
        .unwrap_or((
            remote.samples_per_series,
            remote.start_unix_millis,
            None,
            None,
        ));
    let mut command = vec![
        generator.to_string_lossy().to_string(),
        "prom-remote-write".to_string(),
        "--endpoint".to_string(),
        format!("http://127.0.0.1:{port}/v1/prometheus/write"),
        "--database".to_string(),
        remote.database.clone(),
        "--metric".to_string(),
        remote.metric.clone(),
        "--physical-table".to_string(),
        remote.physical_table.clone(),
        "--series-count".to_string(),
        remote.series_count.to_string(),
        "--samples-per-series".to_string(),
        samples_per_series.to_string(),
        "--start-unix-millis".to_string(),
        start_unix_millis.to_string(),
        "--step-millis".to_string(),
        remote.step_millis.to_string(),
        "--chunk-series-count".to_string(),
        remote.chunk_series_count.to_string(),
        "--timeout-seconds".to_string(),
        remote.timeout_seconds.to_string(),
        "--value-pattern".to_string(),
        remote.value.pattern.clone(),
        "--value-base".to_string(),
        json_number(remote.value.base),
        "--value-step".to_string(),
        json_number(remote.value.step),
        "--value-cardinality".to_string(),
        remote.value.cardinality.to_string(),
        "--value-seed".to_string(),
        remote.value.seed.to_string(),
        "--value-run-length".to_string(),
        remote.value.run_length.to_string(),
        "--value-stall-every".to_string(),
        remote.value.stall_every.to_string(),
        "--value-stall-length".to_string(),
        remote.value.stall_length.to_string(),
        "--value-mixed-every".to_string(),
        remote.value.mixed_every.to_string(),
    ];
    if let Some(sample_offset) = sample_offset {
        command.extend([
            "--value-sample-offset".to_string(),
            sample_offset.to_string(),
        ]);
    }
    if let Some(total_samples_per_series) = total_samples_per_series {
        command.extend([
            "--value-total-samples-per-series".to_string(),
            total_samples_per_series.to_string(),
        ]);
    }
    command
}

async fn flush_remote_table(
    client: &Client,
    port: u16,
    remote: &RemoteWrite,
    reason: &str,
    chunk_index: Option<u64>,
) -> Result<Value> {
    let mut result = http_post_sql(
        client,
        port,
        &format!("ADMIN FLUSH_TABLE({})", sql_string(&remote.physical_table)),
        &remote.database,
    )
    .await;
    if !result["ok"].as_bool().unwrap_or(false) {
        return Err(format!(
            "ADMIN FLUSH_TABLE {} failed: {result}",
            remote.physical_table
        )
        .into());
    }
    result
        .as_object_mut()
        .ok_or("flush result must be an object")?
        .extend([
            ("physical_table".to_string(), json!(remote.physical_table)),
            ("reason".to_string(), json!(reason)),
            ("chunk_index".to_string(), json!(chunk_index)),
        ]);
    Ok(result)
}

async fn poll_expected_count(
    client: &Client,
    port: u16,
    table_name: &str,
    database: &str,
    expected_rows: u64,
    visibility_timeout_seconds: u64,
) -> Result<Value> {
    let sql = format!("SELECT count(*) FROM {}", sql_ident(table_name));
    let deadline = Instant::now() + Duration::from_secs(visibility_timeout_seconds);
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
            return Err(format!(
                "expected {expected_rows} rows but observed {observed_rows:?} after {attempts} attempts"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn json_number(value: f64) -> String {
    serde_json::to_string(&value).unwrap_or_else(|_| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_regression_runner::model::{PromStore, RemoteValue};

    #[test]
    fn schedules_remote_sample_chunks_and_flushes() {
        let remote = RemoteWrite {
            database: "public".to_string(),
            metric: "metric".to_string(),
            physical_table: "physical".to_string(),
            series_count: 2,
            samples_per_series: 5,
            start_unix_millis: 100,
            step_millis: 10,
            chunk_series_count: 1,
            timeout_seconds: 60,
            sample_chunk_size: Some(2),
            flush_every_sample_chunks: 2,
            visibility_timeout_seconds: 30,
            prom_store: PromStore {
                pending_rows_flush_interval: "1s".to_string(),
                max_batch_rows: 1,
                max_concurrent_flushes: 1,
                worker_channel_capacity: 1,
                max_inflight_requests: 1,
            },
            value: RemoteValue {
                pattern: "linear".to_string(),
                base: 0.0,
                step: 1.0,
                cardinality: 1,
                seed: 0,
                run_length: 1,
                stall_every: 0,
                stall_length: 0,
                mixed_every: 0,
            },
            storage: None,
            read_bench: None,
        };
        let chunks = sample_chunks(&remote).unwrap().unwrap();
        assert_eq!(
            chunks,
            vec![
                SampleChunk {
                    index: 1,
                    offset: 0,
                    samples_per_series: 2,
                    start_unix_millis: 100,
                },
                SampleChunk {
                    index: 2,
                    offset: 2,
                    samples_per_series: 2,
                    start_unix_millis: 120,
                },
                SampleChunk {
                    index: 3,
                    offset: 4,
                    samples_per_series: 1,
                    start_unix_millis: 140,
                },
            ]
        );
        assert_eq!(
            scheduled_flushes(&chunks, remote.flush_every_sample_chunks),
            vec![(2, "periodic"), (3, "final")]
        );
    }
}
