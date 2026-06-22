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

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Metadata for a compatibility test case, parsed from `case.toml`.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct CaseMetadata {
    /// Human-readable name of the case.
    pub name: String,
    /// Why this compatibility case exists.
    pub reason: String,
    /// What PR, issue, or feature introduced this case.
    pub introduced_by: String,
    /// Which topologies this case applies to (e.g. ["distributed"]).
    pub topologies: Vec<String>,
    /// Version range for the "from" binary. `*` means all versions.
    pub from_range: Vec<String>,
    /// Version range for the "to" binary. `*` means all versions.
    pub to_range: Vec<String>,
    /// Features required (e.g. ["table", "flow"]).
    pub features: Vec<String>,
    /// Owner team or individual.
    pub owner: String,
    /// Optional explicit namespace. If not set, derived from case directory name.
    /// Must match `[a-z0-9_]+`.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Optional isolation mode. Set to `"shared"` to allow duplicate namespaces
    /// across cases in the same batch.
    #[serde(default)]
    pub isolation: Option<String>,
}

impl CaseMetadata {
    /// Compute the effective namespace for this case.
    /// Uses explicit `namespace` field if set, otherwise derives from case directory name.
    pub fn effective_namespace(&self, case_dir_name: &str) -> String {
        self.namespace
            .clone()
            .unwrap_or_else(|| sanitize_namespace(case_dir_name))
    }
}

/// A loaded compatibility case (metadata + file paths).
#[derive(Debug, Clone)]
pub struct CompatCase {
    /// Parsed metadata from case.toml.
    pub metadata: CaseMetadata,
    /// Path to the case directory.
    pub dir: PathBuf,
    /// Effective namespace for this case.
    pub namespace: String,
}

/// Sanitize a name into a valid GreptimeDB namespace: lowercase alphanumeric + underscores.
fn sanitize_namespace(name: &str) -> String {
    let sanitized: String = name
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    // Must start with a letter
    if sanitized
        .chars()
        .next()
        .is_none_or(|c| !c.is_ascii_alphabetic())
    {
        format!("c_{sanitized}")
    } else {
        sanitized
    }
}

/// Discover all compat cases under `case_root`.
/// Each case is a directory containing `case.toml`, `setup.sql`, `verify.sql`, and `verify.result`.
pub fn discover_cases(case_root: &Path) -> Result<Vec<CompatCase>, String> {
    let mut cases = Vec::new();

    if !case_root.is_dir() {
        return Err(format!(
            "Case root directory not found: {}",
            case_root.display()
        ));
    }

    let entries = std::fs::read_dir(case_root)
        .map_err(|e| format!("Failed to read case root {}: {e}", case_root.display()))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read case dir entry: {e}"))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let case_toml_path = path.join("case.toml");
        if !case_toml_path.is_file() {
            println!("Skipping directory {}: no case.toml found", path.display());
            continue;
        }

        let setup_sql = path.join("setup.sql");
        let verify_sql = path.join("verify.sql");
        let verify_result = path.join("verify.result");

        for required in [&setup_sql, &verify_sql, &verify_result] {
            if !required.is_file() {
                return Err(format!(
                    "Missing required file {} in case directory {}",
                    required.display(),
                    path.display()
                ));
            }
        }

        let content = std::fs::read_to_string(&case_toml_path)
            .map_err(|e| format!("Failed to read {}: {e}", case_toml_path.display()))?;

        let metadata: CaseMetadata = toml::from_str(&content)
            .map_err(|e| format!("Failed to parse {}: {e}", case_toml_path.display()))?;

        let case_dir_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        let namespace = metadata.effective_namespace(case_dir_name);

        cases.push(CompatCase {
            metadata,
            dir: path,
            namespace,
        });
    }

    if cases.is_empty() {
        return Err(format!(
            "No compat cases found under {}",
            case_root.display()
        ));
    }

    Ok(cases)
}

/// Validate a set of compat cases.
/// Checks required metadata fields, namespace format, and duplicate namespace rules.
pub fn validate_cases(cases: &[CompatCase]) -> Result<(), String> {
    let mut namespaces: HashSet<&str> = HashSet::new();

    for case in cases {
        // Validate namespace format: must start with a lowercase letter, followed by
        // lowercase alphanumeric or underscores only.
        if !is_valid_namespace(&case.namespace) {
            return Err(format!(
                "Case '{}' has invalid namespace '{}': must match [a-z][a-z0-9_]*",
                case.metadata.name, case.namespace
            ));
        }

        // Check duplicate namespace
        let is_shared = case.metadata.isolation.as_deref() == Some("shared");

        if !is_shared && !namespaces.insert(&case.namespace) {
            return Err(format!(
                "Duplicate namespace '{}' for case '{}'. \
                 Set isolation = \"shared\" in case.toml to allow this.",
                case.namespace, case.metadata.name
            ));
        }

        // Validate required metadata fields are non-empty
        if case.metadata.name.is_empty() {
            return Err(format!("Case in {} has empty name", case.dir.display()));
        }
        if case.metadata.reason.is_empty() {
            return Err(format!("Case '{}' has empty reason", case.metadata.name));
        }
        if case.metadata.introduced_by.is_empty() {
            return Err(format!(
                "Case '{}' has empty introduced_by",
                case.metadata.name
            ));
        }
        if case.metadata.owner.is_empty() {
            return Err(format!("Case '{}' has empty owner", case.metadata.name));
        }
        if case.metadata.topologies.is_empty() {
            return Err(format!(
                "Case '{}' has empty topologies",
                case.metadata.name
            ));
        }
        if case.metadata.from_range.is_empty() {
            return Err(format!(
                "Case '{}' has empty from_range",
                case.metadata.name
            ));
        }
        if case.metadata.to_range.is_empty() {
            return Err(format!("Case '{}' has empty to_range", case.metadata.name));
        }
        if case.metadata.features.is_empty() {
            return Err(format!("Case '{}' has empty features", case.metadata.name));
        }

        // Validate isolation value
        if let Some(ref isolation) = case.metadata.isolation
            && isolation != "shared"
        {
            return Err(format!(
                "Case '{}' has invalid isolation '{}': only \"shared\" is supported",
                case.metadata.name, isolation
            ));
        }
    }

    Ok(())
}

/// Check whether a string is a valid namespace: starts with lowercase letter,
/// contains only lowercase alphanumeric + underscores.
fn is_valid_namespace(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_namespace() {
        assert_eq!(sanitize_namespace("basic_table"), "basic_table");
        assert_eq!(sanitize_namespace("my-case"), "my_case");
        assert_eq!(sanitize_namespace("123abc"), "c_123abc");
        assert_eq!(sanitize_namespace("UPPER"), "upper");
        assert_eq!(sanitize_namespace("a.b-c"), "a_b_c");
    }

    #[test]
    fn test_validate_cases_rejects_empty_required_vectors() {
        let case = CompatCase {
            metadata: CaseMetadata {
                name: "case".to_string(),
                reason: "reason".to_string(),
                introduced_by: "pr".to_string(),
                topologies: vec![],
                from_range: vec!["*".to_string()],
                to_range: vec!["*".to_string()],
                features: vec!["table".to_string()],
                owner: "team".to_string(),
                namespace: None,
                isolation: None,
            },
            dir: PathBuf::from("case"),
            namespace: "case".to_string(),
        };

        assert!(validate_cases(&[case]).is_err());
    }
}
