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

#![allow(clippy::print_stderr, clippy::print_stdout)]

use std::collections::HashMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use std::{fs, io};

use clap::{Parser, Subcommand};
use futures::{AsyncWriteExt as _, TryStreamExt};
use object_store::ObjectStore;
use object_store::config::ObjectStoreConfig;
use object_store::factory::new_raw_object_store;
use object_store::services::Fs;
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug, Parser)]
#[command(about = "Measure query endpoints or materialize a direct-SST fixture")]
struct Cli {
    #[command(subcommand)]
    command: RunnerCommand,
}

#[derive(Debug, Subcommand)]
enum RunnerCommand {
    /// Run normalized query regression cases against two HTTP endpoints.
    Measure(MeasureArgs),
    /// Create direct-SST tables, discover their regions, and generate fixtures.
    PrepareDirect(PrepareDirectArgs),
    /// Render the frontend Prometheus store configuration for a remote-write case.
    RenderRemoteConfig(RenderRemoteConfigArgs),
    /// Create a database and ingest a normalized Prometheus remote-write case.
    PrepareRemote(PrepareRemoteArgs),
    /// Inspect stopped remote-write storage and finalize the aggregate report.
    FinalizeRemote(FinalizeRemoteArgs),
    /// Run one externally managed OTLP trace-load target.
    RunOtlpTarget(RunOtlpTargetArgs),
    /// Combine externally managed OTLP trace-load target results.
    FinalizeOtlp(FinalizeOtlpArgs),
    /// Copy a direct-SST fixture into an object store destination.
    Materialize(MaterializeArgs),
}

#[derive(Debug, Parser)]
struct MeasureArgs {
    #[arg(long, value_name = "PATH")]
    case: PathBuf,
    #[arg(long, value_name = "PATH")]
    fixture_generator: PathBuf,
    #[arg(long)]
    base_http_port: u16,
    #[arg(long)]
    candidate_http_port: u16,
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 120.0)]
    http_timeout: f64,
}

#[derive(Debug, Parser)]
struct PrepareDirectArgs {
    #[arg(long, value_name = "PATH")]
    case: PathBuf,
    #[arg(long, value_name = "PATH")]
    fixture_generator: PathBuf,
    #[arg(long)]
    base_http_port: u16,
    #[arg(long)]
    candidate_http_port: u16,
    #[arg(long, value_name = "PATH")]
    fixture_dir: PathBuf,
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 120.0)]
    http_timeout: f64,
    #[arg(long)]
    allow_large_fixture: bool,
}

#[derive(Debug, Parser)]
struct RenderRemoteConfigArgs {
    #[arg(long, value_name = "PATH")]
    case: PathBuf,
    #[arg(long, value_name = "PATH")]
    fixture_generator: PathBuf,
    #[arg(long, value_name = "PATH")]
    output: PathBuf,
}

#[derive(Debug, Parser)]
struct PrepareRemoteArgs {
    #[arg(long, value_name = "PATH")]
    case: PathBuf,
    #[arg(long, value_name = "PATH")]
    fixture_generator: PathBuf,
    #[arg(long)]
    base_http_port: u16,
    #[arg(long)]
    candidate_http_port: u16,
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 120.0)]
    http_timeout: f64,
}

#[derive(Debug, Parser)]
struct FinalizeRemoteArgs {
    #[arg(long, value_name = "PATH")]
    case: PathBuf,
    #[arg(long, value_name = "PATH")]
    fixture_generator: PathBuf,
    #[arg(long, value_name = "PATH")]
    candidate_bin: PathBuf,
    #[arg(long, value_name = "PATH")]
    base_data_home: PathBuf,
    #[arg(long, value_name = "PATH")]
    candidate_data_home: PathBuf,
    #[arg(long, value_name = "PATH")]
    report: PathBuf,
}

#[derive(Debug, Parser)]
struct RunOtlpTargetArgs {
    #[arg(long, value_name = "PATH")]
    case: PathBuf,
    #[arg(long, value_name = "PATH")]
    fixture_generator: PathBuf,
    #[arg(long, value_name = "PATH")]
    otelgen_bin: PathBuf,
    #[arg(long)]
    http_port: u16,
    #[arg(long)]
    target_name: String,
    #[arg(long, value_name = "PATH")]
    work_dir: PathBuf,
    #[arg(long, value_name = "PATH")]
    output: PathBuf,
    #[arg(long, default_value_t = 120.0)]
    http_timeout: f64,
}

#[derive(Debug, Parser)]
struct FinalizeOtlpArgs {
    #[arg(long, value_name = "PATH")]
    case: PathBuf,
    #[arg(long, value_name = "PATH")]
    fixture_generator: PathBuf,
    #[arg(long, value_name = "PATH")]
    base_result: PathBuf,
    #[arg(long, value_name = "PATH")]
    candidate_result: PathBuf,
    #[arg(long, value_name = "PATH")]
    output: PathBuf,
}

#[derive(Debug, Parser)]
struct MaterializeArgs {
    #[arg(long, value_name = "PATH")]
    fixture_dir: PathBuf,
    #[arg(
        long,
        value_name = "PATH",
        help = "TOML file: data_home = \"...\" and object_store = { type = \"File\" }; object_store uses object_store::config::ObjectStoreConfig"
    )]
    destination: PathBuf,
}

#[derive(Debug, Deserialize)]
struct DestinationConfig {
    data_home: String,
    object_store: ObjectStoreConfig,
}

#[derive(Debug, Deserialize)]
struct FixtureSummary {
    region_dir: String,
}

#[derive(Debug, Default, Serialize)]
struct CopyCounts {
    files: u64,
    bytes: u64,
}

#[derive(Debug, Serialize)]
struct MaterializeResult {
    region_dir: String,
    object_store: CopyCounts,
    manifest: CopyCounts,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
enum Scenario {
    #[serde(rename = "direct_readable_sst")]
    DirectReadableSst {
        tables: Vec<Table>,
        layout: Layout,
        #[serde(default)]
        queries: Vec<Query>,
    },
    #[serde(rename = "prom_remote_write_then_query")]
    PromRemoteWriteThenQuery {
        remote_write: RemoteWrite,
        #[serde(default)]
        queries: Vec<Query>,
    },
    #[serde(rename = "otlp_trace_load")]
    OtlpTraceLoad { load: OtlpTraceLoad },
}

#[derive(Debug, Deserialize)]
struct Layout {
    regions: usize,
}

#[derive(Clone, Debug, Deserialize)]
struct Table {
    database: String,
    name: String,
    #[serde(default)]
    engine: String,
    #[serde(default)]
    columns: Vec<Column>,
    #[serde(default)]
    primary_key: Vec<String>,
    #[serde(default)]
    time_index: Option<String>,
    #[serde(default)]
    append_mode: Option<bool>,
    #[serde(default)]
    sst_format: Option<String>,
    #[serde(default = "default_show_create_engine")]
    validate_show_create_engine: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
}

const fn default_show_create_engine() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
struct RemoteWrite {
    database: String,
    metric: String,
    physical_table: String,
    series_count: u64,
    samples_per_series: u64,
    start_unix_millis: i64,
    step_millis: i64,
    chunk_series_count: u64,
    timeout_seconds: u64,
    sample_chunk_size: Option<u64>,
    flush_every_sample_chunks: u64,
    visibility_timeout_seconds: u64,
    prom_store: PromStore,
    value: RemoteValue,
    storage: Option<StorageConfig>,
    read_bench: Option<ReadBenchConfig>,
}

#[derive(Clone, Debug, Deserialize)]
struct PromStore {
    pending_rows_flush_interval: String,
    max_batch_rows: u64,
    max_concurrent_flushes: u64,
    worker_channel_capacity: u64,
    max_inflight_requests: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct RemoteValue {
    pattern: String,
    base: f64,
    step: f64,
    cardinality: u64,
    seed: u64,
    run_length: u64,
    stall_every: u64,
    stall_length: u64,
    mixed_every: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OtlpTraceLoad {
    load: OtlpLoad,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OtlpLoad {
    database: String,
    table: String,
    pipeline: String,
    duration_seconds: u64,
    warmup_seconds: u64,
    rate: u64,
    workers: usize,
    exporter_shards: usize,
    workload: String,
    visibility_timeout_seconds: u64,
    thresholds: OtlpThresholds,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OtlpThresholds {
    max_candidate_throughput_regression_pct: f64,
    max_candidate_mean_latency_regression_pct: f64,
    max_failure_count: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct StorageConfig {
    inspect: bool,
    column: String,
    root_suffix: Option<String>,
    include_metadata_files: bool,
    min_files: u64,
    min_files_with_column: u64,
    require_encodings: Vec<String>,
    forbid_encodings: Vec<String>,
    max_total_file_size_bytes: Option<u64>,
    max_column_compressed_size_bytes: Option<u64>,
    max_column_uncompressed_size_bytes: Option<u64>,
    max_candidate_total_file_size_regression_pct: Option<f64>,
    max_candidate_column_compressed_size_regression_pct: Option<f64>,
    max_candidate_column_uncompressed_size_regression_pct: Option<f64>,
}

#[derive(Clone, Debug, Deserialize)]
struct ReadBenchConfig {
    enabled: bool,
    parquetbench: bool,
    scanbench: bool,
    iterations: u64,
    projection: Vec<String>,
    parquet_reader: String,
    scan_scanner: String,
    parallelism: u64,
    max_files: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Query {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    query: String,
    #[serde(default)]
    warmup: usize,
    #[serde(default = "one")]
    iterations: usize,
    #[serde(default)]
    thresholds: Map<String, Value>,
}

const fn one() -> usize {
    1
}

#[derive(Debug, Serialize)]
struct QueryResult {
    validation: Vec<Value>,
    validation_errors: Vec<Value>,
    measurements: Vec<Measurement>,
    status: String,
}

#[derive(Debug, Serialize)]
struct Measurement {
    name: Option<String>,
    kind: Option<String>,
    iterations: usize,
    samples: Vec<Value>,
    latency_ms_median: Option<f64>,
    latency_ms_p95: Option<f64>,
    status: String,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("query_regression_runner: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    match Cli::parse().command {
        RunnerCommand::Measure(args) => run_measure(args).await,
        RunnerCommand::PrepareDirect(args) => run_prepare_direct(args).await,
        RunnerCommand::RenderRemoteConfig(args) => run_render_remote_config(args).await,
        RunnerCommand::PrepareRemote(args) => run_prepare_remote(args).await,
        RunnerCommand::FinalizeRemote(args) => run_finalize_remote(args).await,
        RunnerCommand::RunOtlpTarget(args) => run_otlp_target(args).await,
        RunnerCommand::FinalizeOtlp(args) => run_finalize_otlp(args).await,
        RunnerCommand::Materialize(args) => run_materialize(args).await,
    }
}

async fn run_measure(args: MeasureArgs) -> Result<()> {
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

#[derive(Clone, Debug, Serialize)]
struct Discovery {
    table: String,
    catalog: String,
    schema: String,
    table_id: u64,
    region_id: u64,
    region_seq: u64,
    table_dir: String,
    region_dir: String,
    peer_id: u64,
    peer_addr: Value,
    is_leader: Value,
    status: Value,
    discovery_queries: Value,
}

#[derive(Debug)]
struct PreparedTarget {
    creates: Vec<Value>,
    discoveries: Vec<Discovery>,
}

async fn run_prepare_direct(args: PrepareDirectArgs) -> Result<()> {
    if !args.http_timeout.is_finite() || args.http_timeout < 0.0 {
        return Err("--http-timeout must be a non-negative finite number".into());
    }
    let case_path = args.case.canonicalize()?;
    let plan = load_plan(&args.fixture_generator, &case_path)?;
    let scenario_value = plan
        .get("scenario")
        .cloned()
        .ok_or("fixture plan has no scenario")?;
    let scenario: Scenario = serde_json::from_value(scenario_value)?;
    let tables = direct_tables(scenario)?;
    let client = Client::builder()
        .timeout(Duration::from_secs_f64(args.http_timeout))
        .build()?;
    let base = prepare_target("base", args.base_http_port, &tables, &client).await?;
    let candidate = prepare_target("candidate", args.candidate_http_port, &tables, &client).await?;
    if base.discoveries.len() != candidate.discoveries.len()
        || base
            .discoveries
            .iter()
            .zip(&candidate.discoveries)
            .any(|(base, candidate)| {
                base.table != candidate.table
                    || base.region_id != candidate.region_id
                    || base.table_dir != candidate.table_dir
            })
    {
        return Err(format!(
            "base/candidate metadata mismatch: base={:?}, candidate={:?}",
            base.discoveries, candidate.discoveries
        )
        .into());
    }

    let mut fixtures = Vec::with_capacity(tables.len());
    for (index, (table, discovery)) in tables.iter().zip(&base.discoveries).enumerate() {
        let fixture_dir = table_fixture_dir(&args.fixture_dir, &tables, table, index);
        fixtures.push(generate_direct_fixture(
            &args.fixture_generator,
            &case_path,
            &fixture_dir,
            table,
            discovery,
            tables.len() > 1,
            args.allow_large_fixture,
        )?);
    }

    let report = json!({
        "case_path": case_path,
        "scenario": "direct_readable_sst",
        "base": { "create_table": base.creates, "discovery": base.discoveries },
        "candidate": { "create_table": candidate.creates, "discovery": candidate.discoveries },
        "fixtures": fixtures,
    });
    let text = format!("{}\n", serde_json::to_string_pretty(&report)?);
    if let Some(path) = args.output {
        fs::write(path, &text)?;
    }
    print!("{text}");
    Ok(())
}

fn direct_tables(scenario: Scenario) -> Result<Vec<Table>> {
    match scenario {
        Scenario::DirectReadableSst { tables, layout, .. } => {
            validate_direct_tables(tables, layout)
        }
        Scenario::PromRemoteWriteThenQuery { .. } => {
            Err("prepare-direct requires scenario kind direct_readable_sst".into())
        }
        Scenario::OtlpTraceLoad { .. } => {
            Err("prepare-direct requires scenario kind direct_readable_sst".into())
        }
    }
}

async fn prepare_target(
    name: &str,
    port: u16,
    tables: &[Table],
    client: &Client,
) -> Result<PreparedTarget> {
    let mut creates = Vec::with_capacity(tables.len());
    let mut discoveries = Vec::with_capacity(tables.len());
    for table in tables {
        let sql = create_table_sql(table)?;
        let result = http_post_sql(client, port, &sql, &table.database).await;
        if !result["ok"].as_bool().unwrap_or(false) {
            return Err(format!("CREATE TABLE {} failed for {name}: {result}", table.name).into());
        }
        creates.push(json!({ "table": table.name, "sql": sql, "result": result }));
        discoveries.push(discover_region(client, port, table).await?);
    }
    Ok(PreparedTarget {
        creates,
        discoveries,
    })
}

fn create_table_sql(table: &Table) -> Result<String> {
    if table.engine != "mito" {
        return Err("prepare-direct requires table engine mito".into());
    }
    let time_index = table
        .time_index
        .as_deref()
        .ok_or("direct-SST table requires time_index")?;
    if table.columns.is_empty() {
        return Err("direct-SST table requires columns".into());
    }
    let columns = table
        .columns
        .iter()
        .map(|column| format!("{} {}", sql_ident(&column.name), column.data_type))
        .collect::<Vec<_>>()
        .join(",\n  ");
    let primary_key = table
        .primary_key
        .iter()
        .map(|column| sql_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut options = Vec::new();
    if let Some(append_mode) = table.append_mode {
        options.push(("append_mode", append_mode.to_string()));
    }
    if let Some(sst_format) = table
        .sst_format
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        options.push(("sst_format", sst_format.to_string()));
    }
    let with = (!options.is_empty()).then(|| {
        format!(
            "\nWITH ({})",
            options
                .iter()
                .map(|(key, value)| format!("'{key}'='{value}'"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    });
    Ok(format!(
        "CREATE TABLE {} (\n  {columns},\n  TIME INDEX ({}),\n  PRIMARY KEY ({primary_key})\n) ENGINE=mito{};",
        sql_ident(&table.name),
        sql_ident(time_index),
        with.unwrap_or_default()
    ))
}

async fn discover_region(client: &Client, port: u16, table: &Table) -> Result<Discovery> {
    let schema = sql_string(&table.database);
    let table_name = sql_string(&table.name);
    let table_sql = format!(
        "SELECT table_id FROM information_schema.tables WHERE table_schema = {schema} AND table_name = {table_name}"
    );
    let table_result = http_post_sql(client, port, &table_sql, &table.database).await;
    if !table_result["ok"].as_bool().unwrap_or(false) {
        return Err(format!("table_id discovery failed: {table_result}").into());
    }
    let table_rows = extract_rows(table_result.get("response").unwrap_or(&Value::Null));
    if table_rows.len() != 1 {
        return Err(format!(
            "expected one information_schema.tables row, got {}: {table_result}",
            table_rows.len()
        )
        .into());
    }
    let table_id = row_u64(&table_rows[0], 0, "table_id")?;

    let region_sql = format!(
        "SELECT region_id, peer_id, peer_addr, is_leader, status FROM information_schema.region_peers WHERE table_schema = {schema} AND table_name = {table_name}"
    );
    let region_result = http_post_sql(client, port, &region_sql, &table.database).await;
    if !region_result["ok"].as_bool().unwrap_or(false) {
        return Err(format!("region_peers discovery failed: {region_result}").into());
    }
    let region_rows = extract_rows(region_result.get("response").unwrap_or(&Value::Null));
    if region_rows.len() != 1 {
        return Err(format!(
            "expected one information_schema.region_peers row, got {}: {region_result}",
            region_rows.len()
        )
        .into());
    }
    let row = &region_rows[0];
    let region_id = row_u64(row, 0, "region_id")?;
    let peer_id = row_u64(row, 1, "peer_id")?;
    let peer_addr = row_value(row, 2, "peer_addr")
        .cloned()
        .unwrap_or(Value::Null);
    let is_leader = row_value(row, 3, "is_leader")
        .cloned()
        .unwrap_or(Value::Null);
    let status = row_value(row, 4, "status").cloned().unwrap_or(Value::Null);
    if !matches!(
        value_text(&is_leader).to_ascii_lowercase().as_str(),
        "yes" | "true"
    ) {
        return Err(format!(
            "expected leader region peer, got is_leader={is_leader}: {region_result}"
        )
        .into());
    }
    if !value_text(&status).eq_ignore_ascii_case("ALIVE") {
        return Err(
            format!("expected ALIVE region peer, got status={status}: {region_result}").into(),
        );
    }
    if peer_id != 0 {
        return Err(format!("expected region leader on datanode peer_id=0, got {peer_id}").into());
    }
    if region_id >> 32 != table_id {
        return Err(format!(
            "table_id mismatch: tables={table_id}, region_id-derived={}",
            region_id >> 32
        )
        .into());
    }
    let region_seq = region_id & u32::MAX as u64;
    let (table_dir, region_dir) = storage_paths(&table.database, table_id, region_seq);
    Ok(Discovery {
        table: table.name.clone(),
        catalog: "greptime".to_string(),
        schema: table.database.clone(),
        table_id,
        region_id,
        region_seq,
        table_dir: table_dir.clone(),
        region_dir,
        peer_id,
        peer_addr,
        is_leader,
        status,
        discovery_queries: json!({ "table": table_result, "region": region_result }),
    })
}

fn storage_paths(database: &str, table_id: u64, region_seq: u64) -> (String, String) {
    let table_dir = format!("data/greptime/{database}/{table_id}/");
    let region_dir = format!("{table_dir}{table_id}_{region_seq:010}");
    (table_dir, region_dir)
}

fn extract_rows(body: &Value) -> Vec<Value> {
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

fn row_value<'a>(row: &'a Value, index: usize, name: &str) -> Option<&'a Value> {
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

fn row_u64(row: &Value, index: usize, name: &str) -> Result<u64> {
    let value =
        row_value(row, index, name).ok_or_else(|| format!("missing {name} in row {row}"))?;
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .ok_or_else(|| format!("invalid {name} in row {row}").into())
}

fn value_text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Null => "None".to_string(),
        _ => value.to_string(),
    }
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn table_fixture_dir(root: &Path, tables: &[Table], table: &Table, index: usize) -> PathBuf {
    if tables.len() == 1 {
        root.to_path_buf()
    } else {
        root.join(fixture_subdir(table, index))
    }
}

fn fixture_subdir(table: &Table, index: usize) -> String {
    let raw = format!("{index:02}_{}_{}", table.database, table.name);
    let mut safe = String::new();
    let mut replaced = false;
    for character in raw.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '_' | '.' | '-') {
            safe.push(character);
            replaced = false;
        } else if !replaced {
            safe.push('_');
            replaced = true;
        }
    }
    let safe = safe.trim_matches(['.', '_', '-']);
    if safe.is_empty() {
        format!("table_{index:02}")
    } else {
        safe.to_string()
    }
}

fn generate_direct_fixture(
    generator: &Path,
    case_path: &Path,
    fixture_dir: &Path,
    table: &Table,
    discovery: &Discovery,
    multi_table: bool,
    allow_large_fixture: bool,
) -> Result<Value> {
    if fixture_dir.exists() {
        fs::remove_dir_all(fixture_dir)?;
    }
    fs::create_dir_all(fixture_dir)?;
    let mut command = vec![
        generator.to_string_lossy().to_string(),
        "direct-sst".to_string(),
        "--case".to_string(),
        case_path.to_string_lossy().to_string(),
        "--out-dir".to_string(),
        fixture_dir.to_string_lossy().to_string(),
    ];
    if multi_table {
        command.extend(["--table".to_string(), table.name.clone()]);
    }
    command.extend([
        "--region-id".to_string(),
        discovery.region_id.to_string(),
        "--table-dir".to_string(),
        discovery.table_dir.clone(),
    ]);
    if allow_large_fixture {
        command.push("--allow-large".to_string());
    }
    let started = Instant::now();
    let output = Command::new(generator).args(&command[1..]).output()?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        return Err(format!(
            "fixture generator failed: command={command:?}, returncode={:?}, elapsed_seconds={elapsed_seconds:.3}, stderr={:.2000}",
            output.status.code(), stderr
        )
        .into());
    }
    let summary: Value = serde_json::from_slice(&fs::read(fixture_dir.join("summary.json"))?)?;
    assert_fixture_summary(&summary, table, discovery)?;
    Ok(json!({
        "status": "ok",
        "fixture_dir": fixture_dir,
        "command": command,
        "returncode": output.status.code(),
        "elapsed_seconds": elapsed_seconds,
        "stdout": stdout,
        "stderr": stderr,
        "summary": summary,
    }))
}

fn assert_fixture_summary(summary: &Value, table: &Table, discovery: &Discovery) -> Result<()> {
    if summary.get("table").and_then(Value::as_str) != Some(&table.name) {
        return Err(format!(
            "fixture table mismatch: {:?} != {}",
            summary.get("table"),
            table.name
        )
        .into());
    }
    if summary.get("database").and_then(Value::as_str) != Some(&table.database) {
        return Err(format!(
            "fixture database mismatch: {:?} != {}",
            summary.get("database"),
            table.database
        )
        .into());
    }
    if summary.get("region_id").and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    }) != Some(discovery.region_id)
    {
        return Err(format!(
            "fixture region_id mismatch: {:?} != {}",
            summary.get("region_id"),
            discovery.region_id
        )
        .into());
    }
    if summary.get("table_dir").and_then(Value::as_str) != Some(&discovery.table_dir) {
        return Err(format!(
            "fixture table_dir mismatch: {:?} != {}",
            summary.get("table_dir"),
            discovery.table_dir
        )
        .into());
    }
    if summary
        .get("region_dir")
        .and_then(Value::as_str)
        .map(|value| value.trim_matches('/'))
        != Some(discovery.region_dir.trim_matches('/'))
    {
        return Err(format!(
            "fixture region_dir mismatch: {:?} != {}",
            summary.get("region_dir"),
            discovery.region_dir
        )
        .into());
    }
    Ok(())
}

fn normalized_remote_write(generator: &PathBuf, case: &Path) -> Result<(PathBuf, RemoteWrite)> {
    let case_path = case.canonicalize()?;
    let plan = load_plan(generator, &case_path)?;
    let scenario = plan
        .get("scenario")
        .cloned()
        .ok_or("fixture plan has no scenario")?;
    match serde_json::from_value(scenario)? {
        Scenario::PromRemoteWriteThenQuery { remote_write, .. } => Ok((case_path, remote_write)),
        Scenario::DirectReadableSst { .. } => {
            Err("remote command requires scenario kind prom_remote_write_then_query".into())
        }
        Scenario::OtlpTraceLoad { .. } => {
            Err("remote command requires scenario kind prom_remote_write_then_query".into())
        }
    }
}

async fn run_render_remote_config(args: RenderRemoteConfigArgs) -> Result<()> {
    let (_, remote) = normalized_remote_write(&args.fixture_generator, &args.case)?;
    fs::write(args.output, frontend_prom_config(&remote.prom_store)?)?;
    Ok(())
}

fn frontend_prom_config(prom: &PromStore) -> Result<String> {
    Ok(format!(
        "[prom_store]\nenable = true\nwith_metric_engine = true\npending_rows_flush_interval = {}\nmax_batch_rows = {}\nmax_concurrent_flushes = {}\nworker_channel_capacity = {}\nmax_inflight_requests = {}\n",
        serde_json::to_string(&prom.pending_rows_flush_interval)?,
        prom.max_batch_rows,
        prom.max_concurrent_flushes,
        prom.worker_channel_capacity,
        prom.max_inflight_requests,
    ))
}

async fn run_prepare_remote(args: PrepareRemoteArgs) -> Result<()> {
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
        args.http_timeout,
        &client,
    )
    .await?;
    let candidate = prepare_remote_target(
        "candidate",
        args.candidate_http_port,
        &args.fixture_generator,
        &remote,
        args.http_timeout,
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
    http_timeout: f64,
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
    let (remote_write, flushes) =
        ingest_remote_write(generator, port, remote, http_timeout, client).await?;
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
struct SampleChunk {
    index: u64,
    offset: u64,
    samples_per_series: u64,
    start_unix_millis: i64,
}

async fn ingest_remote_write(
    generator: &Path,
    port: u16,
    remote: &RemoteWrite,
    http_timeout: f64,
    client: &Client,
) -> Result<(Value, Vec<Value>)> {
    let Some(chunks) = sample_chunks(remote)? else {
        let remote_write = run_remote_write(generator, port, remote, None)?;
        let flush = flush_remote_table(client, port, remote, http_timeout, "final", None).await?;
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
            flushes.push(
                flush_remote_table(
                    client,
                    port,
                    remote,
                    http_timeout,
                    reason,
                    Some(chunk.index),
                )
                .await?,
            );
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

fn sample_chunks(remote: &RemoteWrite) -> Result<Option<Vec<SampleChunk>>> {
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

fn scheduled_flushes(chunks: &[SampleChunk], flush_every: u64) -> Vec<(u64, &'static str)> {
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

fn remote_write_command(
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
    _http_timeout: f64,
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

fn extract_count_value(result: &Value) -> Option<u64> {
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

fn value_u64(value: Option<&Value>) -> Option<u64> {
    value?.as_u64().or_else(|| value?.as_str()?.parse().ok())
}

fn value_f64(value: Option<&Value>) -> Option<f64> {
    value?.as_f64().or_else(|| value?.as_str()?.parse().ok())
}

fn json_number(value: f64) -> String {
    serde_json::to_string(&value).unwrap_or_else(|_| value.to_string())
}

const OTLP_ROWS: &str = "greptime_frontend_otlp_traces_rows";
const OTLP_FAILURES: &str = "greptime_frontend_otlp_traces_failure_count";
const OTLP_ELAPSED_SUM: &str = "greptime_servers_http_otlp_traces_elapsed_sum";
const OTLP_ELAPSED_COUNT: &str = "greptime_servers_http_otlp_traces_elapsed_count";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OtlpSnapshot {
    captured_monotonic_seconds: f64,
    values: HashMap<String, f64>,
}

fn normalized_otlp_load(generator: &PathBuf, case: &Path) -> Result<(PathBuf, OtlpLoad)> {
    let case_path = case.canonicalize()?;
    let plan = load_plan(generator, &case_path)?;
    let scenario = plan
        .get("scenario")
        .cloned()
        .ok_or("fixture plan has no scenario")?;
    match serde_json::from_value(scenario)? {
        Scenario::OtlpTraceLoad { load } => Ok((case_path, load.load)),
        Scenario::DirectReadableSst { .. } | Scenario::PromRemoteWriteThenQuery { .. } => {
            Err("OTLP command requires scenario kind otlp_trace_load".into())
        }
    }
}

async fn run_otlp_target(args: RunOtlpTargetArgs) -> Result<()> {
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

async fn run_finalize_otlp(args: FinalizeOtlpArgs) -> Result<()> {
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

async fn run_finalize_remote(args: FinalizeRemoteArgs) -> Result<()> {
    let (_, remote) = normalized_remote_write(&args.fixture_generator, &args.case)?;
    let storage = remote.storage.as_ref().filter(|storage| storage.inspect);
    let mut report: Value = serde_json::from_slice(&fs::read(&args.report)?)?;
    let bench_root = args.report.parent().unwrap_or_else(|| Path::new("."));
    let mut inspections = Vec::new();
    let target_failed = {
        let targets = report
            .get_mut("targets")
            .and_then(Value::as_array_mut)
            .ok_or("report has no targets array")?;
        for (name, data_home) in [
            ("base", &args.base_data_home),
            ("candidate", &args.candidate_data_home),
        ] {
            let target = targets
                .iter_mut()
                .find(|target| target.get("name").and_then(Value::as_str) == Some(name))
                .ok_or_else(|| format!("report has no {name} target"))?;
            let Some(storage) = storage else {
                target
                    .as_object_mut()
                    .ok_or("report target must be an object")?
                    .insert(
                        "read_bench".to_string(),
                        json!({"status": "skipped", "reason": "storage inspection disabled"}),
                    );
                continue;
            };
            let inspection = run_storage_inspection(&args.fixture_generator, data_home, storage)?;
            let bench_dir = bench_root.join(name).join("read_bench");
            let read_bench = run_read_bench(
                &args.candidate_bin,
                data_home,
                &bench_dir,
                remote.read_bench.as_ref(),
                &inspection,
            )?;
            let inspection_failed = inspection["status"] == "failed";
            let bench_failed = read_bench["status"] == "failed";
            let target = target
                .as_object_mut()
                .ok_or("report target must be an object")?;
            target.insert("storage_inspection".to_string(), inspection.clone());
            target.insert("read_bench".to_string(), read_bench);
            if inspection_failed || bench_failed {
                target.insert("status".to_string(), json!("failed"));
            }
            inspections.push(inspection);
        }
        targets.iter().any(|target| {
            target["status"] == "failed"
                || target["storage_inspection"]["status"] == "failed"
                || target["read_bench"]["status"] == "failed"
        })
    };
    let storage_thresholds = storage
        .map(|storage| {
            enforce_storage_thresholds(
                storage,
                inspections.first().unwrap_or(&Value::Null),
                inspections.get(1).unwrap_or(&Value::Null),
            )
        })
        .unwrap_or_default();
    let threshold_failed = {
        let thresholds = report
            .get_mut("thresholds")
            .and_then(Value::as_array_mut)
            .ok_or("report has no thresholds array")?;
        thresholds.retain(|threshold| !is_storage_threshold_entry(threshold));
        thresholds.extend(storage_thresholds);
        thresholds
            .iter()
            .any(|threshold| threshold["status"] == "failed")
    };
    let failed = target_failed || threshold_failed;
    report["status"] = json!(if failed { "failed" } else { "ok" });
    let text = format!("{}\n", serde_json::to_string_pretty(&report)?);
    fs::write(&args.report, &text)?;
    print!("{text}");
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

fn is_storage_threshold_entry(threshold: &Value) -> bool {
    let Some(name) = threshold.get("threshold").and_then(Value::as_str) else {
        return false;
    };
    matches!(
        name,
        "max_candidate_total_file_size_regression_pct"
            | "max_candidate_column_compressed_size_regression_pct"
            | "max_candidate_column_uncompressed_size_regression_pct"
    ) || (threshold.get("target").is_some()
        && matches!(
            name,
            "min_files"
                | "min_files_with_column"
                | "max_total_file_size_bytes"
                | "max_column_compressed_size_bytes"
                | "max_column_uncompressed_size_bytes"
                | "require_encodings"
                | "forbid_encodings"
        ))
}

fn run_storage_inspection(
    generator: &Path,
    data_home: &Path,
    storage: &StorageConfig,
) -> Result<Value> {
    let root = storage
        .root_suffix
        .as_deref()
        .map_or_else(|| data_home.to_path_buf(), |suffix| data_home.join(suffix));
    let mut command = vec![
        generator.to_string_lossy().to_string(),
        "inspect-footer".to_string(),
        "--root".to_string(),
        root.to_string_lossy().to_string(),
        "--column".to_string(),
        storage.column.clone(),
    ];
    if storage.include_metadata_files {
        command.push("--include-metadata-files".to_string());
    }
    let started = Instant::now();
    let output = Command::new(generator).args(&command[1..]).output()?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        return Err(format!(
            "storage inspector failed: command={command:?}, returncode={:?}, elapsed_seconds={elapsed_seconds:.3}, stderr={:.2000}",
            output.status.code(), stderr
        )
        .into());
    }
    let mut result = json!({
        "status": "ok",
        "command": command,
        "root": root,
        "returncode": output.status.code(),
        "elapsed_seconds": elapsed_seconds,
        "stdout": stdout,
        "stderr": stderr,
    });
    match serde_json::from_str(result["stdout"].as_str().unwrap_or_default()) {
        Ok(summary) => result["summary"] = summary,
        Err(_) => {
            result["summary_parse_error"] = result["stdout"].clone();
            result["status"] = json!("failed");
        }
    }
    Ok(result)
}

fn storage_summary(inspection: &Value) -> Value {
    inspection
        .get("summary")
        .and_then(|summary| summary.get("summary"))
        .filter(|summary| summary.is_object())
        .cloned()
        .unwrap_or_else(|| json!({}))
}

fn enforce_storage_thresholds(
    storage: &StorageConfig,
    base_inspection: &Value,
    candidate_inspection: &Value,
) -> Vec<Value> {
    let base = storage_summary(base_inspection);
    let candidate = storage_summary(candidate_inspection);
    let targets = [("base", &base), ("candidate", &candidate)];
    let mut results = Vec::new();
    for (threshold, field, limit) in [
        ("min_files", "file_count", storage.min_files),
        (
            "min_files_with_column",
            "files_with_column",
            storage.min_files_with_column,
        ),
    ] {
        for (target, summary) in &targets {
            let actual = summary.get(field).cloned().unwrap_or(Value::Null);
            let ok = value_u64(Some(&actual)).is_some_and(|actual| actual >= limit);
            results.push(json!({ "target": target, "threshold": threshold, "status": if ok { "passed" } else { "failed" }, "actual": actual, "limit": limit }));
        }
    }
    for (threshold, field, limit) in [
        (
            "max_total_file_size_bytes",
            "total_file_size",
            storage.max_total_file_size_bytes,
        ),
        (
            "max_column_compressed_size_bytes",
            "column_compressed_size",
            storage.max_column_compressed_size_bytes,
        ),
        (
            "max_column_uncompressed_size_bytes",
            "column_uncompressed_size",
            storage.max_column_uncompressed_size_bytes,
        ),
    ] {
        let Some(limit) = limit else { continue };
        for (target, summary) in &targets {
            let actual = summary.get(field).cloned().unwrap_or(Value::Null);
            let ok = value_f64(Some(&actual)).is_some_and(|actual| actual <= limit as f64);
            results.push(json!({ "target": target, "threshold": threshold, "status": if ok { "passed" } else { "failed" }, "actual": actual, "limit": limit }));
        }
    }
    for (target, summary) in &targets {
        let encodings = summary
            .get("unique_encodings")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for encoding in &storage.require_encodings {
            results.push(json!({ "target": target, "threshold": "require_encodings", "encoding": encoding, "status": if encodings.iter().any(|value| value.as_str() == Some(encoding)) { "passed" } else { "failed" } }));
        }
        for encoding in &storage.forbid_encodings {
            results.push(json!({ "target": target, "threshold": "forbid_encodings", "encoding": encoding, "status": if encodings.iter().all(|value| value.as_str() != Some(encoding)) { "passed" } else { "failed" } }));
        }
    }
    for (threshold, field, limit) in [
        (
            "max_candidate_total_file_size_regression_pct",
            "total_file_size",
            storage.max_candidate_total_file_size_regression_pct,
        ),
        (
            "max_candidate_column_compressed_size_regression_pct",
            "column_compressed_size",
            storage.max_candidate_column_compressed_size_regression_pct,
        ),
        (
            "max_candidate_column_uncompressed_size_regression_pct",
            "column_uncompressed_size",
            storage.max_candidate_column_uncompressed_size_regression_pct,
        ),
    ] {
        let Some(limit) = limit else { continue };
        let base_value = base.get(field).cloned().unwrap_or(Value::Null);
        let candidate_value = candidate.get(field).cloned().unwrap_or(Value::Null);
        let (Some(base_number), Some(candidate_number)) = (
            value_f64(Some(&base_value)).filter(|value| *value != 0.0),
            value_f64(Some(&candidate_value)),
        ) else {
            results.push(json!({ "threshold": threshold, "status": "failed", "reason": "missing or zero base/candidate value", "base": base_value, "candidate": candidate_value }));
            continue;
        };
        let actual = (candidate_number - base_number) / base_number * 100.0;
        results.push(json!({ "threshold": threshold, "status": if actual <= limit { "passed" } else { "failed" }, "actual_pct": actual, "limit_pct": limit, "base": base_value, "candidate": candidate_value }));
    }
    results
}

#[derive(Clone, Debug)]
struct BenchTarget {
    relative_path: String,
    table_dir: String,
    region_id: String,
    path_type: String,
    file_id: String,
}

fn run_read_bench(
    candidate_bin: &Path,
    data_home: &Path,
    bench_dir: &Path,
    read_bench: Option<&ReadBenchConfig>,
    inspection: &Value,
) -> Result<Value> {
    let Some(read_bench) = read_bench.filter(|config| config.enabled) else {
        return Ok(json!({ "status": "skipped", "reason": "read_bench disabled" }));
    };
    if !read_bench.parquetbench && !read_bench.scanbench {
        return Ok(json!({ "status": "skipped", "reason": "parquetbench and scanbench disabled" }));
    }
    let report = inspection
        .get("summary")
        .filter(|summary| summary.is_object());
    let files = report
        .and_then(|report| report.get("files"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let root = report
        .and_then(|report| report.get("root"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .or_else(|| {
            inspection
                .get("root")
                .and_then(Value::as_str)
                .map(PathBuf::from)
        })
        .ok_or("storage inspection has no root")?;
    let mut targets = files
        .iter()
        .filter(|file| {
            file.get("relative_path")
                .and_then(Value::as_str)
                .is_some_and(|path| path.ends_with(".parquet"))
                && file
                    .get("columns")
                    .and_then(Value::as_array)
                    .is_some_and(|columns| !columns.is_empty())
        })
        .filter_map(|file| inspected_bench_target(data_home, &root, file).transpose())
        .collect::<Result<Vec<_>>>()?;
    if let Some(max_files) = read_bench.max_files {
        targets.truncate(max_files);
    }
    if targets.is_empty() {
        return Ok(
            json!({ "status": "failed", "reason": "no inspected data SST files available for read_bench" }),
        );
    }
    fs::create_dir_all(bench_dir)?;
    let config_toml = bench_dir.join("bench.toml");
    let scan_json = bench_dir.join("scan.json");
    fs::write(
        &config_toml,
        format!(
            "[storage]\ndata_home = \"{}\"\ntype = \"File\"\n\n[[region_engine]]\n[region_engine.mito]\n",
            data_home.display()
        ),
    )?;
    fs::write(
        &scan_json,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&json!({ "projection_names": read_bench.projection }))?
        ),
    )?;
    let mut parquet_runs = Vec::new();
    if read_bench.parquetbench {
        for target in &targets {
            let command = vec![
                candidate_bin.to_string_lossy().to_string(),
                "datanode".to_string(),
                "parquetbench".to_string(),
                "--config".to_string(),
                config_toml.to_string_lossy().to_string(),
                "--region-id".to_string(),
                target.region_id.clone(),
                "--table-dir".to_string(),
                target.table_dir.clone(),
                "--file-id".to_string(),
                target.file_id.clone(),
                "--scan-config".to_string(),
                scan_json.to_string_lossy().to_string(),
                "--path-type".to_string(),
                target.path_type.clone(),
                "--iterations".to_string(),
                read_bench.iterations.to_string(),
                "--reader".to_string(),
                read_bench.parquet_reader.clone(),
            ];
            parquet_runs.push(run_bench_command(command, bench_target_value(target))?);
        }
    }
    let mut scan_runs = Vec::new();
    if read_bench.scanbench {
        for (table_dir, region_id, path_type, files) in group_scan_paths(&targets) {
            let command = vec![
                candidate_bin.to_string_lossy().to_string(),
                "datanode".to_string(),
                "scanbench".to_string(),
                "--config".to_string(),
                config_toml.to_string_lossy().to_string(),
                "--region-id".to_string(),
                region_id.clone(),
                "--table-dir".to_string(),
                table_dir.clone(),
                "--scan-config".to_string(),
                scan_json.to_string_lossy().to_string(),
                "--path-type".to_string(),
                path_type.clone(),
                "--scanner".to_string(),
                read_bench.scan_scanner.clone(),
                "--parallelism".to_string(),
                read_bench.parallelism.to_string(),
                "--iterations".to_string(),
                read_bench.iterations.to_string(),
            ];
            scan_runs.push(run_bench_command(command, json!({ "table_dir": table_dir, "region_id": region_id, "path_type": path_type, "files": files }))?);
        }
    }
    let failed = parquet_runs
        .iter()
        .chain(&scan_runs)
        .any(|run| run["status"] == "failed");
    Ok(json!({
        "status": if failed { "failed" } else { "ok" },
        "config_path": config_toml,
        "scan_config_path": scan_json,
        "parquetbench": parquet_runs,
        "scanbench": scan_runs,
        "aggregate": {
            "parquetbench_median_average_ms": bench_median(&parquet_runs),
            "scanbench_median_average_ms": bench_median(&scan_runs),
        },
    }))
}

fn inspected_bench_target(
    data_home: &Path,
    root: &Path,
    file: &Value,
) -> Result<Option<BenchTarget>> {
    let relative_path = file
        .get("relative_path")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if relative_path.is_empty() {
        return Ok(None);
    }
    let root_suffix = root.strip_prefix(data_home).map_err(|_| {
        format!(
            "storage inspection root {} is not under datanode data home {}",
            root.display(),
            data_home.display()
        )
    })?;
    let full_relative = root_suffix.join(relative_path);
    let parts = full_relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    if !parts.last().is_some_and(|part| part.ends_with(".parquet")) {
        return Ok(None);
    }
    let Some((region_index, table_id, region_seq)) =
        parts.iter().enumerate().find_map(|(index, part)| {
            parse_region_dir(part).map(|(table_id, region_seq)| (index, table_id, region_seq))
        })
    else {
        return Ok(None);
    };
    if region_index == 0 {
        return Ok(None);
    }
    let file_index = parts.len() - 1;
    let path_type = if region_index + 1 < file_index
        && matches!(parts[region_index + 1].as_str(), "data" | "metadata")
    {
        parts[region_index + 1].clone()
    } else {
        "bare".to_string()
    };
    if path_type == "metadata" {
        return Ok(None);
    }
    let file_id = parts[file_index].trim_end_matches(".parquet").to_string();
    Ok(Some(BenchTarget {
        relative_path: full_relative.to_string_lossy().to_string(),
        table_dir: format!("{}/", parts[..region_index].join("/")),
        region_id: format!("{table_id}:{region_seq}"),
        path_type,
        file_id,
    }))
}

fn parse_region_dir(part: &str) -> Option<(u64, u64)> {
    let (table_id, region_seq) = part.split_once('_')?;
    (region_seq.len() == 10
        && table_id.chars().all(|character| character.is_ascii_digit())
        && region_seq
            .chars()
            .all(|character| character.is_ascii_digit()))
    .then(|| Some((table_id.parse().ok()?, region_seq.parse().ok()?)))?
}

fn bench_target_value(target: &BenchTarget) -> Value {
    json!({
        "relative_path": target.relative_path,
        "table_dir": target.table_dir,
        "region_id": target.region_id,
        "path_type": target.path_type,
        "file_id": target.file_id,
    })
}

fn group_scan_paths(targets: &[BenchTarget]) -> Vec<(String, String, String, Vec<String>)> {
    let mut groups: Vec<(String, String, String, Vec<String>)> = Vec::new();
    for target in targets {
        if let Some((_, _, _, paths)) =
            groups
                .iter_mut()
                .find(|(table_dir, region_id, path_type, _)| {
                    table_dir == &target.table_dir
                        && region_id == &target.region_id
                        && path_type == &target.path_type
                })
        {
            paths.push(target.relative_path.clone());
        } else {
            groups.push((
                target.table_dir.clone(),
                target.region_id.clone(),
                target.path_type.clone(),
                vec![target.relative_path.clone()],
            ));
        }
    }
    groups
}

fn run_bench_command(command: Vec<String>, mut run: Value) -> Result<Value> {
    let started = Instant::now();
    let output = Command::new(&command[0]).args(&command[1..]).output()?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let run = run.as_object_mut().ok_or("bench run must be an object")?;
    run.extend([
        ("command".to_string(), json!(command)),
        ("returncode".to_string(), json!(output.status.code())),
        ("elapsed_seconds".to_string(), json!(elapsed_seconds)),
        ("stdout".to_string(), json!(stdout)),
        ("stderr".to_string(), json!(stderr)),
        (
            "status".to_string(),
            json!(if output.status.success() {
                "ok"
            } else {
                "failed"
            }),
        ),
    ]);
    if let Some(average_ms) = parse_average_duration(run["stdout"].as_str().unwrap_or_default()) {
        run.insert("average_ms".to_string(), json!(average_ms));
    }
    Ok(Value::Object(run.clone()))
}

fn parse_average_duration(stdout: &str) -> Option<f64> {
    Regex::new(r"(?i)Average duration[^0-9]*([0-9.]+)\s*ms")
        .ok()?
        .captures(stdout)?
        .get(1)?
        .as_str()
        .parse()
        .ok()
}

fn bench_median(runs: &[Value]) -> Option<f64> {
    let values = runs
        .iter()
        .filter_map(|run| run.get("average_ms").and_then(Value::as_f64))
        .collect::<Vec<_>>();
    (!values.is_empty()).then(|| median(&values))
}

async fn run_materialize(args: MaterializeArgs) -> Result<()> {
    let destination: DestinationConfig = toml::from_str(&fs::read_to_string(args.destination)?)?;
    let result = materialize(&args.fixture_dir, destination).await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

async fn materialize(
    fixture_dir: &Path,
    destination: DestinationConfig,
) -> Result<MaterializeResult> {
    let summary: FixtureSummary =
        serde_json::from_slice(&fs::read(fixture_dir.join("summary.json"))?)?;
    let region_dir = validate_region_dir(&summary.region_dir)?;
    let object_source = fs_operator(&fixture_dir.join("object-store"))?;
    let manifest_source = fs_operator(&fixture_dir.join("manifest"))?;
    let destination =
        new_raw_object_store(&destination.object_store, &destination.data_home).await?;

    let region_prefix = format!("{region_dir}/");
    destination
        .delete_with(&region_prefix)
        .recursive(true)
        .await?;

    let object_store = copy_tree(&object_source, &destination, "/", "").await?;
    let manifest = copy_tree(
        &manifest_source,
        &destination,
        "/",
        &format!("{region_dir}/manifest/"),
    )
    .await?;
    Ok(MaterializeResult {
        region_dir,
        object_store,
        manifest,
    })
}

fn fs_operator(root: &Path) -> Result<ObjectStore> {
    Ok(ObjectStore::new(Fs::default().root(&root.to_string_lossy()))?.finish())
}

fn validate_region_dir(region_dir: &str) -> Result<String> {
    let region_dir = region_dir.trim_end_matches('/');
    if region_dir.is_empty()
        || region_dir.starts_with('/')
        || region_dir.contains('\\')
        || region_dir
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(
            "region_dir must be a non-empty relative OpenDAL key without dot components".into(),
        );
    }
    Ok(region_dir.to_string())
}

async fn copy_tree(
    source: &ObjectStore,
    destination: &ObjectStore,
    source_prefix: &str,
    destination_prefix: &str,
) -> Result<CopyCounts> {
    let mut lister = source.lister_with(source_prefix).recursive(true).await?;
    let mut counts = CopyCounts::default();
    while let Some(entry) = lister.try_next().await? {
        if entry.metadata().is_dir() {
            continue;
        }
        let source_path = entry.path().to_string();
        let reader = source
            .reader(&source_path)
            .await?
            .into_futures_async_read(0..entry.metadata().content_length())
            .await?;
        let mut writer = destination
            .writer(&format!("{destination_prefix}{source_path}"))
            .await?
            .into_futures_async_write();
        let bytes = futures::io::copy(reader, &mut writer).await?;
        writer.close().await?;
        counts.files += 1;
        counts.bytes += bytes;
    }
    Ok(counts)
}

fn load_plan(generator: &PathBuf, case_path: &PathBuf) -> Result<Value> {
    let output = Command::new(generator)
        .args(["plan", "--case"])
        .arg(case_path)
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("query_perf_fixture plan failed: {:.2000}", stderr).into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn normalize_scenario(scenario: Scenario) -> Result<(Vec<Table>, Vec<Query>)> {
    match scenario {
        Scenario::DirectReadableSst {
            tables,
            layout,
            queries,
        } => Ok((validate_direct_tables(tables, layout)?, queries)),
        Scenario::PromRemoteWriteThenQuery {
            remote_write,
            queries,
        } => Ok((
            vec![Table {
                database: remote_write.database,
                name: remote_write.metric,
                engine: "metric".to_string(),
                columns: vec![],
                primary_key: vec![],
                time_index: None,
                append_mode: None,
                sst_format: None,
                validate_show_create_engine: false,
            }],
            queries,
        )),
        Scenario::OtlpTraceLoad { .. } => {
            Err("measure requires a query scenario, not otlp_trace_load".into())
        }
    }
}

fn validate_direct_tables(tables: Vec<Table>, layout: Layout) -> Result<Vec<Table>> {
    if tables.is_empty() || layout.regions != 1 {
        return Err("runner supports one or more tables and exactly one region per table".into());
    }
    let mut pairs = HashMap::new();
    let mut names = HashMap::new();
    for table in &tables {
        if pairs.insert((&table.database, &table.name), ()).is_some() {
            return Err("duplicate (database, name) table entries are not supported".into());
        }
        if names.insert(&table.name, ()).is_some() {
            return Err("duplicate table names are not supported".into());
        }
    }
    Ok(tables)
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

async fn http_post_sql(client: &Client, port: u16, sql: &str, db: &str) -> Value {
    let started = Instant::now();
    let request = client
        .post(format!("http://127.0.0.1:{port}/v1/sql"))
        .form(&[("sql", sql), ("db", db), ("format", "json")]);
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
                        "sql": sql,
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
                    "sql": sql,
                }),
            }
        }
        Err(error) => json!({
            "ok": false,
            "status": Value::Null,
            "latency_ms": started.elapsed().as_secs_f64() * 1000.0,
            "error": error.to_string(),
            "sql": sql,
        }),
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

fn median(values: &[f64]) -> f64 {
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

fn round_ties_even(value: f64) -> isize {
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

fn sql_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn creates_exact_direct_sst_table_sql() {
        let table = Table {
            database: "public".to_string(),
            name: "metric\"name".to_string(),
            engine: "mito".to_string(),
            columns: vec![
                Column {
                    name: "host".to_string(),
                    data_type: "STRING".to_string(),
                },
                Column {
                    name: "ts".to_string(),
                    data_type: "TIMESTAMP(9)".to_string(),
                },
            ],
            primary_key: vec!["host".to_string()],
            time_index: Some("ts".to_string()),
            append_mode: Some(true),
            sst_format: Some("flat".to_string()),
            validate_show_create_engine: true,
        };
        assert_eq!(
            create_table_sql(&table).unwrap(),
            "CREATE TABLE \"metric\"\"name\" (\n  \"host\" STRING,\n  \"ts\" TIMESTAMP(9),\n  TIME INDEX (\"ts\"),\n  PRIMARY KEY (\"host\")\n) ENGINE=mito\nWITH ('append_mode'='true', 'sst_format'='flat');"
        );
    }

    #[test]
    fn extracts_named_and_positional_discovery_rows() {
        let rows = extract_rows(&json!({"output": [{"data": [{"TABLE_ID": "7"}]}]}));
        assert_eq!(rows, vec![json!({"TABLE_ID": "7"})]);
        assert_eq!(row_u64(&rows[0], 0, "table_id").unwrap(), 7);
        let rows = extract_rows(&json!({"data": [[30064771073_u64, 0, "addr", "YES", "ALIVE"]]}));
        assert_eq!(rows.len(), 1);
        assert_eq!(row_u64(&rows[0], 0, "region_id").unwrap(), 30_064_771_073);
        assert_eq!(row_u64(&rows[0], 1, "peer_id").unwrap(), 0);
        assert_eq!(
            value_text(row_value(&rows[0], 3, "is_leader").unwrap()),
            "YES"
        );
    }

    #[test]
    fn storage_paths_include_the_table_directory() {
        assert_eq!(
            storage_paths("public", 1024, 0),
            (
                "data/greptime/public/1024/".to_string(),
                "data/greptime/public/1024/1024_0000000000".to_string(),
            )
        );
    }

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

    #[test]
    fn enforces_storage_thresholds_for_each_target_and_regression() {
        let storage: StorageConfig = serde_json::from_value(json!({
            "inspect": true,
            "column": "value",
            "root_suffix": null,
            "include_metadata_files": false,
            "min_files": 2,
            "min_files_with_column": 1,
            "require_encodings": ["PLAIN"],
            "forbid_encodings": ["DELTA"],
            "max_total_file_size_bytes": 105,
            "max_column_compressed_size_bytes": null,
            "max_column_uncompressed_size_bytes": null,
            "max_candidate_total_file_size_regression_pct": 5.0,
            "max_candidate_column_compressed_size_regression_pct": null,
            "max_candidate_column_uncompressed_size_regression_pct": null
        }))
        .unwrap();
        let base = json!({"summary": {"summary": {"file_count": 2, "files_with_column": 1, "total_file_size": 100, "unique_encodings": ["PLAIN"]}}});
        let candidate = json!({"summary": {"summary": {"file_count": 2, "files_with_column": 1, "total_file_size": 110, "unique_encodings": ["PLAIN"]}}});
        let results = enforce_storage_thresholds(&storage, &base, &candidate);
        assert!(
            results
                .iter()
                .any(|result| result["threshold"] == "max_total_file_size_bytes"
                    && result["target"] == "candidate"
                    && result["status"] == "failed")
        );
        assert!(results.iter().any(|result| result["threshold"]
            == "max_candidate_total_file_size_regression_pct"
            && result["actual_pct"] == 10.0
            && result["status"] == "failed"));
        assert!(results.iter().any(
            |result| result["threshold"] == "require_encodings" && result["status"] == "passed"
        ));
    }

    #[test]
    fn identifies_prior_storage_threshold_entries() {
        assert!(is_storage_threshold_entry(
            &json!({"target": "base", "threshold": "min_files"})
        ));
        assert!(is_storage_threshold_entry(&json!({
            "threshold": "max_candidate_total_file_size_regression_pct"
        })));
        assert!(!is_storage_threshold_entry(&json!({
            "threshold": "max_candidate_latency_regression_pct"
        })));
    }

    #[test]
    fn parses_average_duration_and_groups_scan_paths() {
        assert_eq!(
            parse_average_duration("work\nAverage duration: 12.5 ms\n"),
            Some(12.5)
        );
        assert_eq!(parse_average_duration("completed"), None);
        let targets = vec![
            BenchTarget {
                relative_path: "data/a.parquet".to_string(),
                table_dir: "data/".to_string(),
                region_id: "1:2".to_string(),
                path_type: "data".to_string(),
                file_id: "a".to_string(),
            },
            BenchTarget {
                relative_path: "data/b.parquet".to_string(),
                table_dir: "data/".to_string(),
                region_id: "1:2".to_string(),
                path_type: "data".to_string(),
                file_id: "b".to_string(),
            },
        ];
        assert_eq!(
            group_scan_paths(&targets),
            vec![(
                "data/".to_string(),
                "1:2".to_string(),
                "data".to_string(),
                vec!["data/a.parquet".to_string(), "data/b.parquet".to_string()],
            )]
        );
    }

    #[test]
    fn region_dir_must_be_a_relative_opendal_key() {
        assert_eq!(
            validate_region_dir("data/db/region/").unwrap(),
            "data/db/region"
        );
        for region_dir in [
            "",
            "/region",
            ".",
            "a/../region",
            "a/./region",
            "a\\region",
            "a//region",
        ] {
            assert!(validate_region_dir(region_dir).is_err(), "{region_dir}");
        }
    }

    #[tokio::test]
    async fn materializes_fixture_from_fs_to_fs() {
        let fixture = tempfile::tempdir().unwrap();
        let destination = tempfile::tempdir().unwrap();
        let region_dir = "data/public/metrics/00000000000000000001";
        let sst = fixture
            .path()
            .join("object-store")
            .join(region_dir)
            .join("00000000000000000001.parquet");
        fs::create_dir_all(sst.parent().unwrap()).unwrap();
        fs::write(&sst, b"sst").unwrap();
        let checkpoint = fixture
            .path()
            .join("manifest/00000000000000000001.checkpoint");
        fs::create_dir_all(checkpoint.parent().unwrap()).unwrap();
        fs::write(&checkpoint, b"checkpoint").unwrap();
        fs::write(fixture.path().join("manifest/_last_checkpoint"), b"last").unwrap();
        fs::write(
            fixture.path().join("summary.json"),
            format!(r#"{{"region_dir":"{region_dir}"}}"#),
        )
        .unwrap();
        fs::write(fixture.path().join("files.jsonl"), "metadata").unwrap();
        fs::create_dir_all(destination.path().join(region_dir)).unwrap();
        fs::write(destination.path().join(region_dir).join("stale"), "stale").unwrap();
        fs::write(destination.path().join("unrelated"), "keep").unwrap();

        let result = materialize(
            fixture.path(),
            DestinationConfig {
                data_home: destination.path().to_string_lossy().to_string(),
                object_store: ObjectStoreConfig::default(),
            },
        )
        .await
        .unwrap();

        assert_eq!(result.object_store.files, 1);
        assert_eq!(result.object_store.bytes, 3);
        assert_eq!(result.manifest.files, 2);
        assert_eq!(result.manifest.bytes, 14);
        assert_eq!(
            fs::read(
                destination
                    .path()
                    .join(region_dir)
                    .join("00000000000000000001.parquet")
            )
            .unwrap(),
            b"sst"
        );
        assert_eq!(
            fs::read(
                destination
                    .path()
                    .join(region_dir)
                    .join("manifest/00000000000000000001.checkpoint")
            )
            .unwrap(),
            b"checkpoint"
        );
        assert_eq!(
            fs::read(
                destination
                    .path()
                    .join(region_dir)
                    .join("manifest/_last_checkpoint")
            )
            .unwrap(),
            b"last"
        );
        assert!(!destination.path().join(region_dir).join("stale").exists());
        assert_eq!(
            fs::read(destination.path().join("unrelated")).unwrap(),
            b"keep"
        );
        assert!(!destination.path().join("summary.json").exists());
        assert!(!destination.path().join("files.jsonl").exists());
    }
}
