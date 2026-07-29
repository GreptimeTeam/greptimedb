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

use std::path::Path;
use std::process::Command;
use std::time::Instant;

use serde_json::{Value, json};

use crate::query_regression_runner::Result;
use crate::query_regression_runner::model::StorageConfig;
use crate::query_regression_runner::sql::{value_f64, value_u64};

pub(super) fn run_storage_inspection(
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

pub(super) fn enforce_storage_thresholds(
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

#[cfg(test)]
mod tests {
    use super::super::is_storage_threshold_entry;
    use super::*;

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
}
