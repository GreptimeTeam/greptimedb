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

mod case;
mod direct_sst;
mod inspect_footer;
mod prom_remote_write;
mod util;

use std::fs;
use std::path::PathBuf;

use case::*;
use clap::{Args as ClapArgs, Parser, Subcommand};
use direct_sst::run_direct_sst;
use inspect_footer::{InspectFooterArgs, run_inspect_footer};
use prom_remote_write::{PromRemoteWriteArgs, run_prom_remote_write};
use serde_json::json;

#[derive(Parser, Debug)]
#[command(name = "query_perf_fixture")]
#[command(about = "Query performance fixture helper")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    legacy: DirectArgs,
}

#[derive(Subcommand, Debug)]
enum Command {
    DirectSst(DirectArgs),
    PromRemoteWrite(PromRemoteWriteArgs),
    InspectFooter(InspectFooterArgs),
    Plan(PlanArgs),
}

#[derive(ClapArgs, Debug, Clone)]
struct DirectArgs {
    /// TOML case file, for example tests/perf/query_cases/.../case.toml.
    #[arg(long, value_name = "PATH")]
    case: Option<PathBuf>,

    /// Output directory (creates object-store/, manifest/, files.jsonl, summary.json).
    #[arg(long, value_name = "DIR")]
    out_dir: Option<PathBuf>,

    /// Region ID to synthesize. Defaults to table id 1024, region 0.
    #[arg(long, default_value = "4398046511104")]
    region_id: u64,

    /// Table directory relative to object-store root. Defaults to data/{database}/{table}/.
    #[arg(long, value_name = "DIR")]
    table_dir: Option<String>,

    /// Table name to generate when the case contains multiple [[tables]].
    #[arg(long, value_name = "NAME")]
    table: Option<String>,

    /// Manifest/checkpoint version.
    #[arg(long, default_value = "1000000")]
    checkpoint_version: u64,

    /// Safety flag when sst_count exceeds 1000.
    #[arg(long)]
    allow_large: bool,

    /// Print plan only.
    #[arg(long)]
    dry_run: bool,
}

#[derive(ClapArgs, Debug)]
struct PlanArgs {
    #[arg(long, value_name = "PATH")]
    case: PathBuf,
}

pub async fn run() {
    let args = Args::parse();
    match args.command {
        Some(Command::DirectSst(direct)) => run_direct_sst(direct).await,
        Some(Command::PromRemoteWrite(rw)) => run_prom_remote_write(rw)
            .await
            .expect("prom remote write failed"),
        Some(Command::InspectFooter(inspect)) => {
            run_inspect_footer(inspect).expect("inspect footer failed")
        }
        Some(Command::Plan(plan)) => run_plan(plan).expect("plan failed"),
        None => run_direct_sst(args.legacy).await,
    }
}

fn run_plan(args: PlanArgs) -> Result<(), Box<dyn std::error::Error>> {
    let case_text = fs::read_to_string(&args.case)?;
    let mut case: CaseFile = toml::from_str(&case_text)?;
    if let Scenario::PromRemoteWriteThenQuery(s) = &mut case.scenario
        && let Some(storage) = &mut s.remote_write.storage
    {
        storage.populate_planned_thresholds();
        if storage.inspect {
            let mut rb = s.remote_write.read_bench.clone().unwrap_or_default();
            if rb.enabled && rb.projection.is_empty() {
                rb.projection = vec![storage.column.clone()];
            }
            s.remote_write.read_bench = Some(rb);
        } else if s
            .remote_write
            .read_bench
            .as_ref()
            .is_some_and(|rb| rb.enabled)
        {
            return Err("scenario.remote_write.read_bench requires scenario.remote_write.storage.inspect = true".into());
        }
    }
    if let Scenario::OtlpTraceLoad(s) = &case.scenario {
        if s.load.warmup_seconds >= s.load.duration_seconds.get() {
            return Err("scenario.load.warmup_seconds must be less than duration_seconds".into());
        }
        for (name, value) in [
            (
                "max_candidate_throughput_regression_pct",
                s.load.thresholds.max_candidate_throughput_regression_pct,
            ),
            (
                "max_candidate_mean_latency_regression_pct",
                s.load.thresholds.max_candidate_mean_latency_regression_pct,
            ),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(format!(
                    "scenario.load.thresholds.{name} must be a finite non-negative number"
                )
                .into());
            }
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"schema_version": 1, "scenario": case.scenario}))?
    );
    Ok(())
}
