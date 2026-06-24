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
use std::process::Command;

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
/// Each case is a directory containing `case.toml`, `setup.sql`, and `verify.sql`.
/// `verify.result` is optional — it is auto-generated on first run.
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

        for required in [&setup_sql, &verify_sql] {
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

    // Sort by directory name for deterministic ordering across runs.
    cases.sort_by(|a, b| a.dir.file_name().cmp(&b.dir.file_name()));

    Ok(cases)
}

/// Validate per-case metadata for all discovered cases.
///
/// Checks that required fields are non-empty, version constraints are parseable,
/// namespace format is valid, and isolation values are supported.
///
/// Call this **before** version-range filtering so that invalid constraints
/// (e.g. `>=not-a-version`) cause a hard error instead of being silently
/// filtered out.
pub fn validate_cases_metadata(cases: &[CompatCase]) -> Result<(), String> {
    for case in cases {
        // Validate namespace format: must start with a lowercase letter, followed by
        // lowercase alphanumeric or underscores only.
        if !is_valid_namespace(&case.namespace) {
            return Err(format!(
                "Case '{}' has invalid namespace '{}': must match [a-z][a-z0-9_]*",
                case.metadata.name, case.namespace
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
        validate_version_constraints(&case.metadata.name, "from_range", &case.metadata.from_range)?;
        validate_version_constraints(&case.metadata.name, "to_range", &case.metadata.to_range)?;
        if case.metadata.features.is_empty() {
            return Err(format!("Case '{}' has empty features", case.metadata.name));
        }

        // Validate isolation value
        match case.metadata.isolation.as_deref() {
            Some("shared") | None => {}
            Some(isolation) => {
                return Err(format!(
                    "Case '{}' has invalid isolation '{}': only \"shared\" is supported",
                    case.metadata.name, isolation
                ));
            }
        }
    }

    Ok(())
}

/// Check for duplicate namespaces in the **final selected batch**.
///
/// Cases with `isolation = "shared"` are exempt from the duplicate check.
/// Call this **after** version-range filtering so unrelated filtered-out cases
/// do not trigger false-positive namespace conflicts.
pub fn validate_case_namespaces(cases: &[CompatCase]) -> Result<(), String> {
    let mut namespaces: HashSet<&str> = HashSet::new();

    for case in cases {
        let is_shared = case.metadata.isolation.as_deref() == Some("shared");

        if !is_shared && !namespaces.insert(&case.namespace) {
            return Err(format!(
                "Duplicate namespace '{}' for case '{}'. \
                 Set isolation = \"shared\" in case.toml to allow this.",
                case.namespace, case.metadata.name
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

fn validate_version_constraints(
    case_name: &str,
    field_name: &str,
    constraints: &[String],
) -> Result<(), String> {
    for constraint in constraints {
        parse_version_constraint(constraint).map_err(|e| {
            format!("Case '{case_name}' has invalid {field_name} entry '{constraint}': {e}")
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Version-range filtering
// ---------------------------------------------------------------------------

/// A simple 3-component version: `major.minor.patch`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Version {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl Version {
    /// Parse a version string like `v1.1.0` or `1.1.0`.
    pub(crate) fn parse(raw: &str) -> Result<Self, String> {
        let stripped = raw.strip_prefix('v').unwrap_or(raw);
        let core = stripped
            .split_once('-')
            .map(|(core, _)| core)
            .unwrap_or(stripped);
        let core = core.split_once('+').map(|(core, _)| core).unwrap_or(core);
        let parts: Vec<&str> = core.split('.').collect();
        if parts.len() != 3 {
            return Err(format!(
                "Invalid version '{}': expected major.minor.patch",
                raw
            ));
        }
        let major = parts[0]
            .parse::<u64>()
            .map_err(|_| format!("Invalid major version in '{}'", raw))?;
        let minor = parts[1]
            .parse::<u64>()
            .map_err(|_| format!("Invalid minor version in '{}'", raw))?;
        let patch = parts[2]
            .parse::<u64>()
            .map_err(|_| format!("Invalid patch version in '{}'", raw))?;
        Ok(Self {
            major,
            minor,
            patch,
        })
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// A version constraint used in `from_range` / `to_range`.
#[derive(Debug, Clone)]
enum VersionConstraint {
    Wildcard,
    Exact(Version),
    Gt(Version),
    Gte(Version),
    Lt(Version),
    Lte(Version),
}

impl VersionConstraint {
    /// Does this constraint match the given version?
    fn matches(&self, version: &Version) -> bool {
        match self {
            Self::Wildcard => true,
            Self::Exact(target) => version == target,
            Self::Gt(target) => version > target,
            Self::Gte(target) => version >= target,
            Self::Lt(target) => version < target,
            Self::Lte(target) => version <= target,
        }
    }
}

/// Parse a single range entry (e.g. `*`, `<=v1.1.0`, `>=v1.1.1`, `v1.0.0`).
fn parse_version_constraint(raw: &str) -> Result<VersionConstraint, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("Empty version constraint".to_string());
    }
    if trimmed == "*" {
        return Ok(VersionConstraint::Wildcard);
    }

    // Ordered from most-specific prefix to least
    if let Some(ver_str) = trimmed.strip_prefix(">=") {
        let ver = Version::parse(ver_str.trim())?;
        return Ok(VersionConstraint::Gte(ver));
    }
    if let Some(ver_str) = trimmed.strip_prefix("<=") {
        let ver = Version::parse(ver_str.trim())?;
        return Ok(VersionConstraint::Lte(ver));
    }
    if let Some(ver_str) = trimmed.strip_prefix("==") {
        let ver = Version::parse(ver_str.trim())?;
        return Ok(VersionConstraint::Exact(ver));
    }
    if let Some(ver_str) = trimmed.strip_prefix('=') {
        let ver = Version::parse(ver_str.trim())?;
        return Ok(VersionConstraint::Exact(ver));
    }
    if let Some(ver_str) = trimmed.strip_prefix('>') {
        let ver = Version::parse(ver_str.trim())?;
        return Ok(VersionConstraint::Gt(ver));
    }
    if let Some(ver_str) = trimmed.strip_prefix('<') {
        let ver = Version::parse(ver_str.trim())?;
        return Ok(VersionConstraint::Lt(ver));
    }

    // No operator → treat as exact
    let ver = Version::parse(trimmed)?;
    Ok(VersionConstraint::Exact(ver))
}

/// Check whether a version matches a list of OR-ed constraints.
///
/// * `version`: the effective version to test, or `None` if unknown.
/// * `constraints`: the list from `from_range` or `to_range`.
///
/// Returns `true` if `version` matches at least one entry.
///
/// Wildcard entries match any version including unknown.
/// Non-wildcard entries against an unknown version return `false`.
pub(crate) fn version_matches_range(version: Option<&Version>, constraints: &[String]) -> bool {
    if constraints.is_empty() {
        return false;
    }

    for raw in constraints {
        match parse_version_constraint(raw) {
            Ok(VersionConstraint::Wildcard) => return true,
            Ok(constraint) => {
                if version.is_some_and(|ver| constraint.matches(ver)) {
                    return true;
                }
                // Unknown version + non-wildcard → doesn't match this entry
            }
            Err(e) => {
                println!(
                    "Warning: invalid version constraint '{}' — skipping this entry: {e}",
                    raw
                );
            }
        }
    }

    false
}

/// Try to infer a version string by running `<bins_dir>/greptime --version`.
/// Returns `None` if the binary cannot be executed or the output isn't parseable.
pub(crate) fn try_infer_version(bins_dir: &Path) -> Option<Version> {
    let binary = bins_dir.join("greptime");
    if !binary.is_file() {
        return None;
    }
    let output = Command::new(&binary).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Typical output: "greptime 0.9.5-xxxxx" or just "greptime 0.9.5"
    // Grab the first token that looks like a version.
    for token in stdout.split_whitespace() {
        // Strip leading 'v' if present and try to parse
        let candidate = token.trim();
        let looks_like_version = candidate.starts_with('v')
            || candidate.chars().next().is_some_and(|c| c.is_ascii_digit());
        let parsed_version = looks_like_version
            .then(|| Version::parse(candidate).ok())
            .flatten();
        if let Some(ver) = parsed_version {
            return Some(ver);
        }
    }
    None
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
    fn test_validate_cases_metadata_rejects_empty_required_vectors() {
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

        assert!(validate_cases_metadata(&[case]).is_err());
    }

    #[test]
    fn test_validate_cases_metadata_catches_invalid_version_constraint() {
        let case = CompatCase {
            metadata: CaseMetadata {
                name: "bad_constraint".to_string(),
                reason: "test".to_string(),
                introduced_by: "test".to_string(),
                topologies: vec!["distributed".to_string()],
                from_range: vec![">=not-a-version".to_string()],
                to_range: vec!["*".to_string()],
                features: vec!["table".to_string()],
                owner: "test".to_string(),
                namespace: None,
                isolation: None,
            },
            dir: PathBuf::from("bad_constraint"),
            namespace: "bad_constraint".to_string(),
        };

        assert!(validate_cases_metadata(&[case]).is_err());
    }

    #[test]
    fn test_validate_case_namespaces_rejects_duplicate() {
        let case_a = CompatCase {
            metadata: CaseMetadata {
                name: "case_a".to_string(),
                reason: "test".to_string(),
                introduced_by: "test".to_string(),
                topologies: vec!["distributed".to_string()],
                from_range: vec!["*".to_string()],
                to_range: vec!["*".to_string()],
                features: vec!["table".to_string()],
                owner: "test".to_string(),
                namespace: None,
                isolation: None,
            },
            dir: PathBuf::from("case_a"),
            namespace: "shared_name".to_string(),
        };
        let case_b = CompatCase {
            metadata: CaseMetadata {
                name: "case_b".to_string(),
                reason: "test".to_string(),
                introduced_by: "test".to_string(),
                topologies: vec!["distributed".to_string()],
                from_range: vec!["*".to_string()],
                to_range: vec!["*".to_string()],
                features: vec!["table".to_string()],
                owner: "test".to_string(),
                namespace: None,
                isolation: None,
            },
            dir: PathBuf::from("case_b"),
            namespace: "shared_name".to_string(),
        };

        assert!(validate_cases_metadata(&[case_a.clone(), case_b.clone()]).is_ok());
        assert!(validate_case_namespaces(&[case_a, case_b]).is_err());
    }

    #[test]
    fn test_validate_case_namespaces_allows_shared_isolation_duplicate() {
        let case_a = CompatCase {
            metadata: CaseMetadata {
                name: "case_a".to_string(),
                reason: "test".to_string(),
                introduced_by: "test".to_string(),
                topologies: vec!["distributed".to_string()],
                from_range: vec!["*".to_string()],
                to_range: vec!["*".to_string()],
                features: vec!["table".to_string()],
                owner: "test".to_string(),
                namespace: None,
                isolation: Some("shared".to_string()),
            },
            dir: PathBuf::from("case_a"),
            namespace: "shared_name".to_string(),
        };
        let case_b = CompatCase {
            metadata: CaseMetadata {
                name: "case_b".to_string(),
                reason: "test".to_string(),
                introduced_by: "test".to_string(),
                topologies: vec!["distributed".to_string()],
                from_range: vec!["*".to_string()],
                to_range: vec!["*".to_string()],
                features: vec!["table".to_string()],
                owner: "test".to_string(),
                namespace: None,
                isolation: Some("shared".to_string()),
            },
            dir: PathBuf::from("case_b"),
            namespace: "shared_name".to_string(),
        };

        assert!(validate_case_namespaces(&[case_a, case_b]).is_ok());
    }

    #[test]
    fn test_validate_cases_metadata_rejects_invalid_version_range() {
        let case = CompatCase {
            metadata: CaseMetadata {
                name: "case".to_string(),
                reason: "reason".to_string(),
                introduced_by: "pr".to_string(),
                topologies: vec!["distributed".to_string()],
                from_range: vec![">=not-a-version".to_string()],
                to_range: vec!["*".to_string()],
                features: vec!["table".to_string()],
                owner: "team".to_string(),
                namespace: None,
                isolation: None,
            },
            dir: PathBuf::from("case"),
            namespace: "case".to_string(),
        };

        assert!(validate_cases_metadata(&[case]).is_err());
    }

    /// Write minimal required files for a compat case into `dir`.
    fn write_minimal_case(dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        let case_toml = dir.join("case.toml");
        let setup_sql = dir.join("setup.sql");
        let verify_sql = dir.join("verify.sql");

        std::fs::write(
            &case_toml,
            r#"
name = "test_case"
reason = "test"
introduced_by = "test"
topologies = ["distributed"]
from_range = ["*"]
to_range = ["*"]
features = ["table"]
owner = "test"
"#,
        )
        .unwrap();
        std::fs::write(&setup_sql, "CREATE TABLE t (a INT);").unwrap();
        std::fs::write(&verify_sql, "SELECT * FROM t;").unwrap();
    }

    #[test]
    fn test_discover_cases_allows_missing_verify_result() {
        let tmp = tempfile::tempdir().unwrap();
        let case_dir = tmp.path().join("my_case");
        write_minimal_case(&case_dir);
        // verify.result is intentionally absent
        assert!(!case_dir.join("verify.result").is_file());

        let cases =
            discover_cases(tmp.path()).expect("discover should succeed without verify.result");
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].metadata.name, "test_case");
    }

    #[test]
    fn test_discover_cases_rejects_missing_setup_sql() {
        let tmp = tempfile::tempdir().unwrap();
        let case_dir = tmp.path().join("my_case");
        write_minimal_case(&case_dir);
        std::fs::remove_file(case_dir.join("setup.sql")).unwrap();

        assert!(discover_cases(tmp.path()).is_err());
    }

    #[test]
    fn test_discover_cases_rejects_missing_verify_sql() {
        let tmp = tempfile::tempdir().unwrap();
        let case_dir = tmp.path().join("my_case");
        write_minimal_case(&case_dir);
        std::fs::remove_file(case_dir.join("verify.sql")).unwrap();

        assert!(discover_cases(tmp.path()).is_err());
    }

    #[test]
    fn test_discover_cases_rejects_missing_case_toml() {
        let tmp = tempfile::tempdir().unwrap();
        let case_dir = tmp.path().join("my_case");
        write_minimal_case(&case_dir);
        std::fs::remove_file(case_dir.join("case.toml")).unwrap();

        // No case.toml → skipped (not an error)
        let result = discover_cases(tmp.path());
        assert!(result.is_err()); // no cases found at all
    }

    // ------------------------------------------------------------------
    // Version-range matching tests
    // ------------------------------------------------------------------

    fn mkver(s: &str) -> Version {
        Version::parse(s).unwrap()
    }

    #[test]
    fn test_parse_version_constraint_wildcard() {
        let c = parse_version_constraint("*").unwrap();
        assert!(matches!(c, VersionConstraint::Wildcard));
    }

    #[test]
    fn test_parse_version_constraint_exact() {
        let c = parse_version_constraint("v1.2.3").unwrap();
        assert!(matches!(c, VersionConstraint::Exact(ref v) if v == &mkver("v1.2.3")));
    }

    #[test]
    fn test_parse_version_constraint_gte() {
        let c = parse_version_constraint(">=v1.1.0").unwrap();
        assert!(matches!(c, VersionConstraint::Gte(ref v) if v == &mkver("v1.1.0")));
    }

    #[test]
    fn test_parse_version_constraint_lte() {
        let c = parse_version_constraint("<=v1.1.0").unwrap();
        assert!(matches!(c, VersionConstraint::Lte(ref v) if v == &mkver("v1.1.0")));
    }

    #[test]
    fn test_parse_version_constraint_gt() {
        let c = parse_version_constraint(">v1.0.0").unwrap();
        assert!(matches!(c, VersionConstraint::Gt(ref v) if v == &mkver("v1.0.0")));
    }

    #[test]
    fn test_parse_version_constraint_lt() {
        let c = parse_version_constraint("<v2.0.0").unwrap();
        assert!(matches!(c, VersionConstraint::Lt(ref v) if v == &mkver("v2.0.0")));
    }

    #[test]
    fn test_parse_version_constraint_eq_double() {
        let c = parse_version_constraint("==v1.0.0").unwrap();
        assert!(matches!(c, VersionConstraint::Exact(ref v) if v == &mkver("v1.0.0")));
    }

    #[test]
    fn test_version_matches_range_wildcard() {
        assert!(version_matches_range(None, &["*".to_string()]));
        assert!(version_matches_range(
            Some(&mkver("v9.9.9")),
            &["*".to_string()]
        ));
    }

    #[test]
    fn test_version_matches_range_legacy_jsonb() {
        let from_range = vec!["<=v1.1.0".to_string()];
        let to_range = vec![">=v1.1.1".to_string()];

        // from matches <=v1.1.0
        assert!(version_matches_range(Some(&mkver("v0.9.5")), &from_range));
        assert!(version_matches_range(Some(&mkver("v1.1.0")), &from_range));
        assert!(!version_matches_range(Some(&mkver("v1.1.1")), &from_range));
        assert!(!version_matches_range(Some(&mkver("v1.2.0")), &from_range));

        // to matches >=v1.1.1
        assert!(version_matches_range(Some(&mkver("v1.1.1")), &to_range));
        assert!(version_matches_range(Some(&mkver("v2.0.0")), &to_range));
        assert!(!version_matches_range(Some(&mkver("v1.1.0")), &to_range));
        assert!(!version_matches_range(Some(&mkver("v0.9.5")), &to_range));
    }

    #[test]
    fn test_version_matches_range_unknown_version() {
        // Non-wildcard ranges should NOT match unknown version
        assert!(!version_matches_range(None, &["<=v1.1.0".to_string()]));
        assert!(!version_matches_range(None, &[">=v1.1.1".to_string()]));
        assert!(!version_matches_range(None, &["v1.0.0".to_string()]));
        // Wildcard still matches unknown
        assert!(version_matches_range(None, &["*".to_string()]));
    }

    #[test]
    fn test_version_matches_range_exact() {
        let range = vec!["v1.0.0".to_string(), "v2.0.0".to_string()];
        assert!(version_matches_range(Some(&mkver("v1.0.0")), &range));
        assert!(version_matches_range(Some(&mkver("v2.0.0")), &range));
        assert!(!version_matches_range(Some(&mkver("v1.5.0")), &range));
    }

    #[test]
    fn test_version_parse_without_v() {
        let ver = Version::parse("1.2.3").unwrap();
        assert_eq!(ver.major, 1);
        assert_eq!(ver.minor, 2);
        assert_eq!(ver.patch, 3);
    }

    #[test]
    fn test_version_parse_with_suffix() {
        let ver = Version::parse("1.2.3-alpha+build").unwrap();
        assert_eq!(ver.major, 1);
        assert_eq!(ver.minor, 2);
        assert_eq!(ver.patch, 3);
    }

    #[test]
    fn test_try_infer_version_no_binary() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(try_infer_version(tmp.path()).is_none());
    }
}
