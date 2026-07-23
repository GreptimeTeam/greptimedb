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
    println!(
        "{}",
        serde_json::to_string_pretty(&normalize_plan(&case_text)?)?
    );
    Ok(())
}

fn normalize_plan(case_text: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
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
    Ok(json!({"schema_version": 1, "scenario": case.scenario}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_full_physical_table_setup_is_normalized() {
        let plan = normalize_plan(
            r#"
[scenario]
kind = "prom_remote_write_then_query"

[scenario.remote_write]
metric = "mixed_metric"

[scenario.remote_write.physical_table_setup]
columns = [
  { name = "host", type = "STRING" },
  { name = "ts", type = "TIMESTAMP(3)" },
  { name = "value", type = "DOUBLE" },
]
time_index = "ts"
engine = "mito"
options = { compression = "zstd", append_mode = "false" }
target_options = { base = { compression = "plain" }, candidate = { compression = "zstd" } }

[scenario.remote_write.logical_stream_verification]
labels = ["host", "instance"]
timestamp_column = "greptime_timestamp"
value_column = "greptime_value"

[scenario.remote_write.read_bench]
iterations = 7
rounds = 3
projections = [{ name = "all_columns" }, { name = "value_only", columns = ["value"] }]
"#,
        )
        .unwrap();

        let remote = &plan["scenario"]["remote_write"];
        assert_eq!(remote["physical_table_setup"]["columns"][0]["name"], "host");
        assert_eq!(remote["physical_table_setup"]["time_index"], "ts");
        assert_eq!(
            remote["physical_table_setup"]["target_options"]["base"]["compression"],
            "plain"
        );
        assert_eq!(
            remote["logical_stream_verification"],
            serde_json::json!({
                "labels": ["host", "instance"],
                "timestamp_column": "greptime_timestamp",
                "value_column": "greptime_value",
            })
        );
        assert_eq!(remote["read_bench"]["rounds"], 3);
        assert!(remote["read_bench"]["projections"][0]["columns"].is_null());
        assert_eq!(
            remote["read_bench"]["projections"][1]["columns"],
            serde_json::json!(["value"])
        );
    }

    #[test]
    fn legacy_remote_write_case_keeps_legacy_read_bench_behavior_without_setup() {
        let plan = normalize_plan(include_str!(
            "../../../../../tests/perf/query_cases/prom_remote_write_seeded_random/case.toml"
        ))
        .unwrap();

        let remote = &plan["scenario"]["remote_write"];
        assert!(remote["physical_table_setup"].is_null());
        assert!(remote["logical_stream_verification"].is_null());
        assert_eq!(
            remote["read_bench"]["projection"],
            serde_json::json!(["greptime_value"])
        );
        assert_eq!(remote["read_bench"]["projections"], serde_json::json!([]));
        assert_eq!(remote["read_bench"]["rounds"], 1);
        assert_eq!(plan["schema_version"], 1);
    }

    #[test]
    fn mixed_series_case_normalizes_physical_setup_and_read_bench_matrix() {
        let plan = normalize_plan(include_str!(
            "../../../../../tests/perf/query_cases/prom_remote_write_mixed_series/case.toml"
        ))
        .unwrap();

        let remote = &plan["scenario"]["remote_write"];
        assert_eq!(remote["series_count"], 2_000);
        assert_eq!(remote["value"]["series_mix_every"], 20);
        assert_eq!(remote["value"]["gauge_residue"], 19);
        assert_eq!(
            remote["physical_table_setup"]["columns"],
            serde_json::json!([
                { "name": "greptime_timestamp", "type": "TIMESTAMP(3)" },
                { "name": "greptime_value", "type": "DOUBLE" },
            ])
        );
        assert_eq!(
            remote["physical_table_setup"]["options"],
            serde_json::json!({
                "physical_metric_table": "",
                "sst_format": "flat",
            })
        );
        assert_eq!(
            remote["physical_table_setup"]["target_options"],
            serde_json::json!({
                "base": { "experimental_sst_float_field_encoding": "default" },
                "candidate": { "experimental_sst_float_field_encoding": "byte_stream_split" },
            })
        );
        assert_eq!(
            remote["logical_stream_verification"],
            serde_json::json!({
                "labels": ["host", "instance"],
                "timestamp_column": "greptime_timestamp",
                "value_column": "greptime_value",
            })
        );
        assert_eq!(remote["read_bench"]["rounds"], 8);
        assert_eq!(remote["read_bench"]["iterations"], 8);
        assert_eq!(
            remote["read_bench"]["projections"][0],
            serde_json::json!({ "name": "target", "columns": ["greptime_value"] })
        );
        assert_eq!(remote["read_bench"]["projections"][1]["name"], "all");
        assert!(remote["read_bench"]["projections"][1]["columns"].is_null());
    }
}
