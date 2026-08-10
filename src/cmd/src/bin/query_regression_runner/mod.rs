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

use std::error::Error;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod direct;
mod materialize;
mod measure;
mod model;
mod otlp;
mod plan;
mod remote;
mod sql;
mod write_throughput;

pub(super) type Result<T> = std::result::Result<T, Box<dyn Error>>;

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
    /// Run the write_throughput scenario against self-managed base/candidate
    /// clusters (datanode workload-scheduler env injection included).
    RunWriteThroughput(RunWriteThroughputArgs),
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
    base_data_home: Option<PathBuf>,
    #[arg(long, value_name = "PATH")]
    candidate_data_home: Option<PathBuf>,
    #[arg(long, value_name = "PATH")]
    base_destination: Option<PathBuf>,
    #[arg(long, value_name = "PATH")]
    candidate_destination: Option<PathBuf>,
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
struct RunWriteThroughputArgs {
    #[arg(long, value_name = "PATH")]
    case: PathBuf,
    #[arg(long, value_name = "PATH")]
    fixture_generator: PathBuf,
    #[arg(long, value_name = "PATH")]
    base_bin: PathBuf,
    #[arg(long, value_name = "PATH")]
    candidate_bin: PathBuf,
    #[arg(long, value_name = "PATH")]
    work_dir: PathBuf,
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 120.0)]
    http_timeout: f64,
    #[arg(long)]
    dry_run: bool,
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

pub(super) async fn run() {
    if let Err(error) = run_inner().await {
        eprintln!("query_regression_runner: {error}");
        std::process::exit(1);
    }
}

async fn run_inner() -> Result<()> {
    match Cli::parse().command {
        RunnerCommand::Measure(args) => measure::run_measure(args).await,
        RunnerCommand::PrepareDirect(args) => direct::run_prepare_direct(args).await,
        RunnerCommand::RenderRemoteConfig(args) => remote::run_render_remote_config(args).await,
        RunnerCommand::PrepareRemote(args) => remote::run_prepare_remote(args).await,
        RunnerCommand::FinalizeRemote(args) => remote::run_finalize_remote(args).await,
        RunnerCommand::RunOtlpTarget(args) => otlp::run_otlp_target(args).await,
        RunnerCommand::FinalizeOtlp(args) => otlp::run_finalize_otlp(args).await,
        RunnerCommand::RunWriteThroughput(args) => {
            write_throughput::run_write_throughput(args).await
        }
        RunnerCommand::Materialize(args) => materialize::run_materialize(args).await,
    }
}
