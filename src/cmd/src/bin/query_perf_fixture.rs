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

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use cmd::query_perf::case::{
    CASE_SCHEMA_VERSION, ValuePattern, default_chunk_series_count, default_database,
    default_physical_table, default_remote_write_endpoint, default_samples_per_series,
    default_series_count, default_start_unix_millis, default_step_millis, default_timeout_seconds,
    default_value_base, default_value_cardinality, default_value_mixed_every,
    default_value_pattern, default_value_run_length, default_value_sample_offset,
    default_value_seed, default_value_stall_every, default_value_stall_length, default_value_step,
    load_case,
};
use cmd::query_perf::fixture::{
    DirectSstRequest, FooterRequest, RemoteWriteRequest, inspect_footer, run_direct_sst,
    run_prom_remote_write,
};

#[tokio::main]
async fn main() {
    let result = match Cli::parse().command {
        Command::DirectSst(args) => run_direct(args).await.and_then(to_json),
        Command::PromRemoteWrite(args) => run_remote(args).await.and_then(to_json),
        Command::InspectFooter(args) => run_footer(args).and_then(to_json),
        Command::Plan(args) => run_plan(args),
    };
    match result {
        Ok(output) => match serde_json::to_string_pretty(&output) {
            Ok(output) => println!("{output}"),
            Err(err) => {
                eprintln!("query_perf_fixture failed to serialize output: {err}");
                std::process::exit(1);
            }
        },
        Err(err) => {
            eprintln!("query_perf_fixture failed: {err}");
            std::process::exit(1);
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "query_perf_fixture",
    about = "Query performance fixture helper"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand, Debug)]
enum Command {
    DirectSst(DirectSstArgs),
    PromRemoteWrite(RemoteWriteArgs),
    InspectFooter(InspectFooterArgs),
    Plan(PlanArgs),
}
#[derive(Args, Debug)]
struct DirectSstArgs {
    #[arg(long)]
    case: PathBuf,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long, default_value = "4398046511104")]
    region_id: u64,
    #[arg(long)]
    table_dir: Option<String>,
    #[arg(long)]
    table: Option<String>,
    #[arg(long, default_value = "1000000")]
    checkpoint_version: u64,
    #[arg(long)]
    allow_large: bool,
    #[arg(long)]
    dry_run: bool,
}
#[derive(Args, Debug)]
struct InspectFooterArgs {
    #[arg(long)]
    root: PathBuf,
    #[arg(long, default_value = "greptime_value")]
    column: String,
    #[arg(long)]
    include_metadata_files: bool,
}
#[derive(Args, Debug)]
struct PlanArgs {
    #[arg(long)]
    case: PathBuf,
}
#[derive(Args, Debug)]
struct RemoteWriteArgs {
    #[arg(long, default_value_t = default_remote_write_endpoint())]
    endpoint: String,
    #[arg(long, default_value_t = default_database())]
    database: String,
    #[arg(long)]
    metric: String,
    #[arg(long, default_value_t = default_physical_table())]
    physical_table: String,
    #[arg(long, default_value_t = default_series_count())]
    series_count: u64,
    #[arg(long, default_value_t = default_samples_per_series())]
    samples_per_series: u64,
    #[arg(long, default_value_t = default_start_unix_millis())]
    start_unix_millis: i64,
    #[arg(long, default_value_t = default_step_millis())]
    step_millis: i64,
    #[arg(long, alias = "batch-size", default_value_t = default_chunk_series_count())]
    chunk_series_count: u64,
    #[arg(long, default_value_t = default_timeout_seconds())]
    timeout_seconds: u64,
    #[arg(long, default_value_t = default_value_pattern())]
    value_pattern: ValuePattern,
    #[arg(long, default_value_t = default_value_base())]
    value_base: f64,
    #[arg(long, default_value_t = default_value_step())]
    value_step: f64,
    #[arg(long, default_value_t = default_value_cardinality())]
    value_cardinality: u64,
    #[arg(long, default_value_t = default_value_seed())]
    value_seed: u64,
    #[arg(long, default_value_t = default_value_run_length())]
    value_run_length: u64,
    #[arg(long, default_value_t = default_value_stall_every())]
    value_stall_every: u64,
    #[arg(long, default_value_t = default_value_stall_length())]
    value_stall_length: u64,
    #[arg(long, default_value_t = default_value_mixed_every())]
    value_mixed_every: u64,
    #[arg(long, default_value_t = default_value_sample_offset())]
    value_sample_offset: u64,
    #[arg(long)]
    value_total_samples_per_series: Option<u64>,
}
async fn run_direct(
    args: DirectSstArgs,
) -> cmd::query_perf::error::Result<cmd::query_perf::fixture::DirectSstSummary> {
    let case_name = args
        .case
        .parent()
        .and_then(|path| path.file_name())
        .and_then(|name| name.to_str())
        .unwrap_or("query_perf_case")
        .to_string();
    run_direct_sst(DirectSstRequest {
        case: load_case(&args.case)?,
        case_name,
        out_dir: args.out_dir,
        region_id: args.region_id,
        table_dir: args.table_dir,
        table: args.table,
        checkpoint_version: args.checkpoint_version,
        allow_large: args.allow_large,
        dry_run: args.dry_run,
    })
    .await
}
async fn run_remote(
    args: RemoteWriteArgs,
) -> cmd::query_perf::error::Result<cmd::query_perf::fixture::RemoteWriteSummary> {
    run_prom_remote_write(RemoteWriteRequest {
        endpoint: args.endpoint,
        database: args.database,
        metric: args.metric,
        physical_table: args.physical_table,
        series_count: args.series_count,
        samples_per_series: args.samples_per_series,
        start_unix_millis: args.start_unix_millis,
        step_millis: args.step_millis,
        chunk_series_count: args.chunk_series_count,
        timeout_seconds: args.timeout_seconds,
        value_pattern: args.value_pattern,
        value_base: args.value_base,
        value_step: args.value_step,
        value_cardinality: args.value_cardinality,
        value_seed: args.value_seed,
        value_run_length: args.value_run_length,
        value_stall_every: args.value_stall_every,
        value_stall_length: args.value_stall_length,
        value_mixed_every: args.value_mixed_every,
        value_sample_offset: args.value_sample_offset,
        value_total_samples_per_series: args.value_total_samples_per_series,
    })
    .await
}
fn run_footer(
    args: InspectFooterArgs,
) -> cmd::query_perf::error::Result<cmd::query_perf::fixture::FooterObservation> {
    inspect_footer(FooterRequest {
        root: args.root,
        column: args.column,
        include_metadata_files: args.include_metadata_files,
    })
}
fn run_plan(args: PlanArgs) -> cmd::query_perf::error::Result<serde_json::Value> {
    let case = load_case(&args.case)?;
    Ok(serde_json::json!({"schema_version": CASE_SCHEMA_VERSION, "scenario": case.scenario_plan()}))
}
fn to_json(value: impl serde::Serialize) -> cmd::query_perf::error::Result<serde_json::Value> {
    serde_json::to_value(value).map_err(|err| {
        cmd::query_perf::error::Error::new(format!("failed to serialize output: {err}"))
    })
}
