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
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use self::read_bench::run_read_bench;
use self::storage::{enforce_storage_thresholds, run_storage_inspection};
use crate::query_regression_runner::model::DestinationConfig;
use crate::query_regression_runner::plan::normalized_remote_write;
use crate::query_regression_runner::{
    FinalizeRemoteArgs, PrepareRemoteArgs, RenderRemoteConfigArgs, Result,
};

pub(crate) mod ingest;
mod read_bench;
mod storage;

pub(super) async fn run_render_remote_config(args: RenderRemoteConfigArgs) -> Result<()> {
    ingest::run_render_remote_config(args).await
}

pub(super) async fn run_prepare_remote(args: PrepareRemoteArgs) -> Result<()> {
    ingest::run_prepare_remote(args).await
}

pub(super) async fn run_finalize_remote(args: FinalizeRemoteArgs) -> Result<()> {
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
        for (name, data_home, destination) in [
            (
                "base",
                args.base_data_home.as_deref(),
                args.base_destination.as_deref(),
            ),
            (
                "candidate",
                args.candidate_data_home.as_deref(),
                args.candidate_destination.as_deref(),
            ),
        ] {
            let (data_home, destination) = match (data_home, destination) {
                (Some(data_home), None) => (data_home.to_path_buf(), None),
                (None, Some(path)) => {
                    let destination: DestinationConfig =
                        toml::from_str(&fs::read_to_string(path)?)?;
                    (
                        PathBuf::from(destination.data_home),
                        Some(path.to_path_buf()),
                    )
                }
                (Some(_), Some(_)) => {
                    return Err(format!(
                        "{name}: --{name}-data-home and --{name}-destination are mutually exclusive"
                    )
                    .into());
                }
                (None, None) => {
                    return Err(format!(
                        "{name}: one of --{name}-data-home or --{name}-destination is required"
                    )
                    .into());
                }
            };
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
            let inspection = run_storage_inspection(
                &args.fixture_generator,
                &data_home,
                destination.as_deref(),
                storage,
            )?;
            let bench_dir = bench_root.join(name).join("read_bench");
            let read_bench = run_read_bench(
                &args.candidate_bin,
                &data_home,
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
