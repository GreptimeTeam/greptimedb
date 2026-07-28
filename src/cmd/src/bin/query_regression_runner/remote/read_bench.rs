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
use std::process::Command;
use std::time::Instant;

use regex::Regex;
use serde_json::{Value, json};

use super::super::Result;
use super::super::measure::median;
use super::super::model::ReadBenchConfig;

#[derive(Clone, Debug)]
struct BenchTarget {
    relative_path: String,
    table_dir: String,
    region_id: String,
    path_type: String,
    file_id: String,
}

pub(super) fn run_read_bench(
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
