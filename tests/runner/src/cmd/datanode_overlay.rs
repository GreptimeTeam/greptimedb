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

use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};
use toml::value::Table;

use crate::env::bare::WalConfig;
/// A dotted TOML path owned by the compatibility runner.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DottedPath(Vec<String>);

impl DottedPath {
    fn new(parts: &[&str]) -> Self {
        Self(parts.iter().map(|part| (*part).to_string()).collect())
    }

    fn parts(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

impl std::fmt::Display for DottedPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.join("."))
    }
}

/// Runner-owned datanode configuration paths that an overlay cannot override.
#[derive(Debug, Clone)]
pub(crate) struct DatanodeProtectionPolicy {
    protected_paths: Vec<DottedPath>,
}

impl DatanodeProtectionPolicy {
    /// Builds the policy for the runner-selected WAL configuration.
    pub(crate) fn for_wal(wal: &WalConfig) -> Self {
        let mut protected_paths = vec![
            DottedPath::new(&["mode"]),
            DottedPath::new(&["node_id"]),
            DottedPath::new(&["storage", "data_home"]),
            DottedPath::new(&["meta_client_options", "metasrv_addrs"]),
            DottedPath::new(&["wal", "provider"]),
        ];
        protected_paths.push(match wal {
            WalConfig::RaftEngine => DottedPath::new(&["wal", "dir"]),
            WalConfig::Kafka { .. } => DottedPath::new(&["wal", "broker_endpoints"]),
        });
        protected_paths.sort();

        Self { protected_paths }
    }
}

/// A parsed old-stage datanode sidecar, loaded from a confined case directory.
#[derive(Debug, Clone)]
pub(crate) struct DatanodeOverlay {
    source: PathBuf,
    value: toml::Value,
    profile_key: [u8; 32],
    profile_id: String,
}

impl DatanodeOverlay {
    /// Loads and parses a datanode sidecar referenced relative to `case_dir`.
    ///
    /// This is intended to prevent accidental case-directory escapes; it is not
    /// an adversarial concurrent-filesystem security boundary.
    pub(crate) fn load(case_dir: &Path, relative_ref: &Path) -> Result<Self, String> {
        validate_relative_reference(relative_ref)?;

        let canonical_case_dir = case_dir.canonicalize().map_err(|error| {
            format!(
                "Failed to canonicalize compatibility case directory {}: {error}",
                case_dir.display()
            )
        })?;
        let requested_path = case_dir.join(relative_ref);
        let canonical_target = requested_path.canonicalize().map_err(|error| {
            format!(
                "Failed to resolve datanode overlay {} relative to case directory {}: {error}",
                relative_ref.display(),
                case_dir.display()
            )
        })?;

        if !canonical_target.starts_with(&canonical_case_dir) {
            return Err(format!(
                "Datanode overlay {} resolves outside compatibility case directory {}",
                relative_ref.display(),
                canonical_case_dir.display()
            ));
        }

        // Check the type before opening so the error is identical on every
        // platform: `std::fs::metadata` succeeds on directories on both Unix
        // and Windows, whereas `File::open` on a directory fails up front on
        // Windows (with a platform-specific message).
        let metadata = std::fs::metadata(&canonical_target).map_err(|error| {
            format!(
                "Failed to inspect datanode overlay {}: {error}",
                canonical_target.display()
            )
        })?;
        if !metadata.is_file() {
            return Err(format!(
                "Datanode overlay {} must be a regular file",
                canonical_target.display()
            ));
        }

        let mut file = File::open(&canonical_target).map_err(|error| {
            format!(
                "Failed to open datanode overlay {}: {error}",
                canonical_target.display()
            )
        })?;

        let mut content = String::new();
        file.read_to_string(&mut content).map_err(|error| {
            format!(
                "Failed to read datanode overlay {}: {error}",
                canonical_target.display()
            )
        })?;
        let value: toml::Value = toml::from_str(&content).map_err(|error| {
            format!(
                "Failed to parse datanode overlay {}: {error}",
                canonical_target.display()
            )
        })?;
        if !value.is_table() {
            return Err(format!(
                "Datanode overlay {} must have a TOML table at its root",
                canonical_target.display()
            ));
        }

        let profile_key = semantic_profile_key(&value);
        let profile_id = hex::encode(profile_key)[..12].to_string();

        Ok(Self {
            source: canonical_target,
            value,
            profile_key,
            profile_id,
        })
    }

    /// Validates runner-owned path conflicts and records overridden protected paths.
    pub(crate) fn prepare(
        self,
        protection: &DatanodeProtectionPolicy,
    ) -> Result<PreparedDatanodeOverlay, String> {
        let mut touched_protected_paths = Vec::new();
        for path in &protection.protected_paths {
            validate_protected_ancestors(&self.value, path).map_err(|error| {
                format!(
                    "Datanode overlay {} (profile {}): {error}",
                    self.source.display(),
                    self.profile_id
                )
            })?;
            if value_at_path(&self.value, path).is_some() {
                touched_protected_paths.push(path.clone());
            }
        }
        touched_protected_paths.sort();

        Ok(PreparedDatanodeOverlay {
            source: self.source,
            value: self.value,
            profile_key: self.profile_key,
            profile_id: self.profile_id,
            protected_paths: protection.protected_paths.clone(),
            touched_protected_paths,
        })
    }
}

/// A validated datanode overlay ready for application to a rendered baseline.
#[derive(Debug, Clone)]
pub(crate) struct PreparedDatanodeOverlay {
    source: PathBuf,
    value: toml::Value,
    profile_key: [u8; 32],
    profile_id: String,
    protected_paths: Vec<DottedPath>,
    touched_protected_paths: Vec<DottedPath>,
}

impl PreparedDatanodeOverlay {
    /// Returns the canonical full SHA-256 profile key used for grouping.
    pub(crate) fn profile_key(&self) -> &[u8; 32] {
        &self.profile_key
    }

    /// Returns a truncated profile ID suitable only for diagnostics.
    pub(crate) fn profile_id(&self) -> &str {
        &self.profile_id
    }

    /// Returns the canonical sidecar path used for diagnostics.
    pub(crate) fn source(&self) -> &Path {
        &self.source
    }

    /// Returns protected paths declared by the sidecar, in dotted-path order.
    pub(crate) fn touched_protected_paths(&self) -> &[DottedPath] {
        &self.touched_protected_paths
    }

    /// Merges the sidecar into a rendered baseline and restores runner-owned fields.
    pub(crate) fn apply_to_rendered_baseline(&self, baseline: &str) -> Result<String, String> {
        let baseline_value: toml::Value = toml::from_str(baseline)
            .map_err(|error| format!("Failed to parse rendered datanode baseline: {error}"))?;
        if !baseline_value.is_table() {
            return Err(
                "Rendered datanode baseline must have a TOML table at its root".to_string(),
            );
        }

        let mut merged = baseline_value.clone();
        merge_toml_values(&mut merged, &self.value);

        for path in &self.protected_paths {
            restore_protected_path(&mut merged, &baseline_value, path)?;
        }

        toml::to_string(&merged)
            .map_err(|error| format!("Failed to serialize merged datanode configuration: {error}"))
    }
}

fn validate_relative_reference(relative_ref: &Path) -> Result<(), String> {
    if relative_ref.as_os_str().is_empty()
        || relative_ref
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "Datanode overlay reference {} must be a non-empty relative path with normal components only",
            relative_ref.display()
        ));
    }
    Ok(())
}

fn validate_protected_ancestors(value: &toml::Value, path: &DottedPath) -> Result<(), String> {
    let mut current = value;
    let parts: Vec<_> = path.parts().collect();
    for (index, part) in parts.iter().enumerate().take(parts.len().saturating_sub(1)) {
        let Some(table) = current.as_table() else {
            return Err(format!(
                "Datanode overlay makes ancestor {} of protected path {} non-table",
                parts[..index].join("."),
                path
            ));
        };
        let Some(next) = table.get(*part) else {
            return Ok(());
        };
        if !next.is_table() {
            return Err(format!(
                "Datanode overlay makes ancestor {} of protected path {} non-table",
                parts[..=index].join("."),
                path
            ));
        }
        current = next;
    }
    Ok(())
}

fn value_at_path<'a>(value: &'a toml::Value, path: &DottedPath) -> Option<&'a toml::Value> {
    let mut current = value;
    for part in path.parts() {
        current = current.as_table()?.get(part)?;
    }
    Some(current)
}

fn merge_toml_values(baseline: &mut toml::Value, overlay: &toml::Value) {
    match (baseline, overlay) {
        (toml::Value::Table(baseline), toml::Value::Table(overlay)) => {
            for (key, overlay_value) in overlay {
                match baseline.get_mut(key) {
                    Some(baseline_value) => merge_toml_values(baseline_value, overlay_value),
                    None => {
                        baseline.insert(key.clone(), overlay_value.clone());
                    }
                }
            }
        }
        (baseline, overlay) => *baseline = overlay.clone(),
    }
}

fn restore_protected_path(
    merged: &mut toml::Value,
    baseline: &toml::Value,
    path: &DottedPath,
) -> Result<(), String> {
    let Some(merged_table) = merged.as_table_mut() else {
        return Err("Merged datanode configuration must have a TOML table at its root".to_string());
    };
    let baseline_value = value_at_path(baseline, path).cloned();
    restore_path_in_table(merged_table, &path.0, baseline_value);
    Ok(())
}

fn restore_path_in_table(table: &mut Table, parts: &[String], baseline_value: Option<toml::Value>) {
    let Some((part, remaining)) = parts.split_first() else {
        return;
    };
    if remaining.is_empty() {
        match baseline_value {
            Some(value) => {
                table.insert(part.clone(), value);
            }
            None => {
                table.remove(part);
            }
        }
        return;
    }

    let Some(value) = table.get_mut(part) else {
        return;
    };
    if let Some(table) = value.as_table_mut() {
        restore_path_in_table(table, remaining, baseline_value);
    }
}

fn semantic_profile_key(value: &toml::Value) -> [u8; 32] {
    let mut encoded = Vec::new();
    encode_semantic_value(&mut encoded, value);
    Sha256::digest(encoded).into()
}

fn encode_semantic_value(output: &mut Vec<u8>, value: &toml::Value) {
    match value {
        toml::Value::String(value) => encode_bytes(output, b"string", value.as_bytes()),
        toml::Value::Integer(value) => {
            encode_bytes(output, b"integer", &value.to_be_bytes());
        }
        toml::Value::Float(value) => {
            encode_bytes(output, b"float", &value.to_bits().to_be_bytes());
        }
        toml::Value::Boolean(value) => {
            encode_bytes(output, b"boolean", &[u8::from(*value)]);
        }
        toml::Value::Datetime(value) => {
            encode_bytes(output, b"datetime", value.to_string().as_bytes())
        }
        toml::Value::Array(values) => {
            let mut payload = Vec::new();
            encode_length(&mut payload, values.len());
            for value in values {
                encode_semantic_value(&mut payload, value);
            }
            encode_bytes(output, b"array", &payload);
        }
        toml::Value::Table(table) => {
            let mut payload = Vec::new();
            encode_length(&mut payload, table.len());
            let mut entries: Vec<_> = table.iter().collect();
            entries.sort_unstable_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            for (key, value) in entries {
                encode_bytes(&mut payload, b"key", key.as_bytes());
                encode_semantic_value(&mut payload, value);
            }
            encode_bytes(output, b"table", &payload);
        }
    }
}

fn encode_bytes(output: &mut Vec<u8>, tag: &[u8], value: &[u8]) {
    encode_length(output, tag.len());
    output.extend_from_slice(tag);
    encode_length(output, value.len());
    output.extend_from_slice(value);
}

fn encode_length(output: &mut Vec<u8>, length: usize) {
    output.extend_from_slice(&(length as u64).to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_overlay(content: &str) -> DatanodeOverlay {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("overlay.toml");
        std::fs::write(&path, content).unwrap();
        let overlay = DatanodeOverlay::load(temp_dir.path(), Path::new("overlay.toml")).unwrap();
        // `load` owns the parsed TOML, so the temporary source need not outlive it.
        overlay
    }

    fn prepared_overlay(content: &str, wal: &WalConfig) -> PreparedDatanodeOverlay {
        load_overlay(content)
            .prepare(&DatanodeProtectionPolicy::for_wal(wal))
            .unwrap()
    }

    #[test]
    fn merges_recursive_tables_and_replaces_non_tables_atomically() {
        let prepared = prepared_overlay(
            r#"
            scalar = "replacement"
            primitive_array = [3, 2, 1]
            array_of_tables = [{ name = "new" }]
            [nested]
            new_key = true
            old_key = "replacement"
            "#,
            &WalConfig::RaftEngine,
        );
        let merged = prepared
            .apply_to_rendered_baseline(
                r#"
                scalar = 1
                primitive_array = [1, 2]
                array_of_tables = [{ name = "old" }, { name = "older" }]
                untouched = "kept"
                [nested]
                old_key = 1
                retained = "kept"
                "#,
            )
            .unwrap();
        let value: toml::Value = toml::from_str(&merged).unwrap();

        assert_eq!(value["scalar"].as_str(), Some("replacement"));
        assert_eq!(value["primitive_array"].as_array().unwrap().len(), 3);
        assert_eq!(value["array_of_tables"].as_array().unwrap().len(), 1);
        assert_eq!(value["nested"]["old_key"].as_str(), Some("replacement"));
        assert_eq!(value["nested"]["new_key"].as_bool(), Some(true));
        assert_eq!(value["nested"]["retained"].as_str(), Some("kept"));
        assert_eq!(value["untouched"].as_str(), Some("kept"));
    }

    #[test]
    fn restores_protected_paths_and_deletes_absent_raft_wal_dir() {
        let prepared = prepared_overlay(
            r#"
            mode = "standalone"
            node_id = 99
            [storage]
            data_home = "/overlay/data"
            [meta_client_options]
            metasrv_addrs = ["overlay:3002"]
            [wal]
            provider = "kafka"
            dir = "/overlay/wal"
            tuning = 42
            "#,
            &WalConfig::RaftEngine,
        );
        let merged = prepared
            .apply_to_rendered_baseline(
                r#"
                mode = "distributed"
                node_id = 1
                [storage]
                data_home = "/runner/data"
                [meta_client_options]
                metasrv_addrs = ["runner:3002"]
                [wal]
                provider = "raft_engine"
                "#,
            )
            .unwrap();
        let value: toml::Value = toml::from_str(&merged).unwrap();

        assert_eq!(value["mode"].as_str(), Some("distributed"));
        assert_eq!(value["node_id"].as_integer(), Some(1));
        assert_eq!(value["storage"]["data_home"].as_str(), Some("/runner/data"));
        assert_eq!(
            value["meta_client_options"]["metasrv_addrs"][0].as_str(),
            Some("runner:3002")
        );
        assert_eq!(value["wal"]["provider"].as_str(), Some("raft_engine"));
        assert!(value["wal"].get("dir").is_none());
        assert_eq!(value["wal"]["tuning"].as_integer(), Some(42));
    }

    #[test]
    fn kafka_policy_restores_broker_endpoints() {
        let prepared = prepared_overlay(
            r#"
            [wal]
            broker_endpoints = ["overlay:9092"]
            provider = "raft_engine"
            "#,
            &WalConfig::Kafka {
                needs_kafka_cluster: false,
                broker_endpoints: vec![],
            },
        );
        let merged = prepared
            .apply_to_rendered_baseline(
                r#"
                [wal]
                provider = "kafka"
                broker_endpoints = ["runner:9092"]
                "#,
            )
            .unwrap();
        let value: toml::Value = toml::from_str(&merged).unwrap();

        assert_eq!(value["wal"]["provider"].as_str(), Some("kafka"));
        assert_eq!(
            value["wal"]["broker_endpoints"][0].as_str(),
            Some("runner:9092")
        );
    }

    #[test]
    fn rejects_non_table_protected_ancestor() {
        let overlay = load_overlay("wal = \"not a table\"");
        let error = overlay
            .prepare(&DatanodeProtectionPolicy::for_wal(&WalConfig::RaftEngine))
            .unwrap_err();

        assert!(error.contains("ancestor wal"));
        assert!(error.contains("profile"));
        assert!(error.contains("wal.dir") || error.contains("wal.provider"));
    }

    #[test]
    fn collects_sorted_touched_protected_paths() {
        let prepared = prepared_overlay(
            r#"
            node_id = 4
            [wal]
            dir = "/overlay/wal"
            provider = "kafka"
            "#,
            &WalConfig::RaftEngine,
        );

        let paths: Vec<_> = prepared
            .touched_protected_paths()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(paths, ["node_id", "wal.dir", "wal.provider"]);
    }

    #[test]
    fn semantic_identity_ignores_formatting_and_table_order() {
        let first = load_overlay("[settings]\nb = 2\na = \"value\"\n");
        let second = load_overlay("# comment\n[settings]\na = \"value\"\nb = 2\n");

        assert_eq!(first.profile_key, second.profile_key);
        assert_eq!(first.profile_id, second.profile_id);
    }

    #[test]
    fn semantic_identity_preserves_scalar_types_and_array_order() {
        let integer = load_overlay("value = 1");
        let float = load_overlay("value = 1.0");
        let first_order = load_overlay("values = [1, 2]");
        let second_order = load_overlay("values = [2, 1]");

        assert_ne!(integer.profile_key, float.profile_key);
        assert_ne!(first_order.profile_key, second_order.profile_key);
    }

    #[test]
    fn prepared_overlay_exposes_profile_and_source_for_runner_grouping() {
        let overlay = load_overlay("value = 1");
        let expected_key = overlay.profile_key;
        let expected_id = overlay.profile_id.clone();
        let prepared = overlay
            .prepare(&DatanodeProtectionPolicy::for_wal(&WalConfig::RaftEngine))
            .unwrap();

        assert_eq!(prepared.profile_key(), &expected_key);
        assert_eq!(prepared.profile_id(), expected_id);
        assert!(prepared.source().ends_with("overlay.toml"));
    }

    #[test]
    fn rejects_absolute_and_traversal_references() {
        let temp_dir = tempfile::tempdir().unwrap();
        let absolute = temp_dir.path().join("overlay.toml");
        std::fs::write(&absolute, "value = 1").unwrap();

        assert!(DatanodeOverlay::load(temp_dir.path(), &absolute).is_err());
        assert!(DatanodeOverlay::load(temp_dir.path(), Path::new("../overlay.toml")).is_err());
        assert!(DatanodeOverlay::load(temp_dir.path(), Path::new("./overlay.toml")).is_err());
    }

    #[test]
    fn rejects_directories_and_parse_errors() {
        let temp_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp_dir.path().join("directory.toml")).unwrap();
        std::fs::write(temp_dir.path().join("broken.toml"), "[broken").unwrap();

        let directory_error =
            DatanodeOverlay::load(temp_dir.path(), Path::new("directory.toml")).unwrap_err();
        assert!(
            directory_error.contains("regular file"),
            "unexpected error for directory.toml: {directory_error}"
        );
        let parse_error =
            DatanodeOverlay::load(temp_dir.path(), Path::new("broken.toml")).unwrap_err();
        assert!(parse_error.contains("Failed to parse datanode overlay"));
        assert!(parse_error.contains("broken.toml"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let case_dir = tempfile::tempdir().unwrap();
        let outside_dir = tempfile::tempdir().unwrap();
        let outside_file = outside_dir.path().join("outside.toml");
        std::fs::write(&outside_file, "value = 1").unwrap();
        symlink(&outside_file, case_dir.path().join("escape.toml")).unwrap();

        let error = DatanodeOverlay::load(case_dir.path(), Path::new("escape.toml")).unwrap_err();
        assert!(error.contains("outside compatibility case directory"));
    }
}
