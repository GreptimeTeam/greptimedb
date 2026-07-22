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

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use sqlness::QueryContext;
use sqlness::interceptor::template::DELIMITER as TEMPLATE_DELIMITER;
use sqlness::interceptor::{InterceptorRef, Registry};

use crate::cmd::bare::ServerAddr;
use crate::cmd::compat_case::{
    self, CompatCase, OldConfig, try_infer_version, version_matches_range,
};
use crate::env::bare::{Env, StoreConfig, WalConfig};
use crate::protocol_interceptor::{self, POSTGRES, PROTOCOL_KEY};
use crate::server_mode::validate_old_config_against_datanode_template;
use crate::util;

const COMPAT_TOPOLOGY: &str = "distributed";
const COMMENT_PREFIX: &str = "--";
const INTERCEPTOR_PREFIX: &str = "-- SQLNESS";
const QUERY_DELIMITER: char = ';';

/// A deterministic collection of cases that can share one compatibility lifecycle.
#[derive(Debug)]
struct CompatProfile {
    old_config: Option<OldConfig>,
    cases: Vec<CompatCase>,
}

impl CompatProfile {
    fn name(&self, index: usize) -> String {
        if self.old_config.is_some() {
            format!("old-config-{index}")
        } else {
            "baseline".to_string()
        }
    }

    fn config_keys(&self) -> Vec<&str> {
        self.old_config
            .as_ref()
            .map(|config| config.keys().collect())
            .unwrap_or_default()
    }
}

/// Groups cases by their complete normalized old-stage configuration profile.
fn group_cases_by_profile(cases: Vec<CompatCase>) -> Vec<CompatProfile> {
    let mut grouped: BTreeMap<Option<OldConfig>, Vec<CompatCase>> = BTreeMap::new();
    for case in cases {
        grouped
            .entry(case.metadata.old_config.clone())
            .or_default()
            .push(case);
    }

    grouped
        .into_iter()
        .map(|(old_config, cases)| CompatProfile { old_config, cases })
        .collect()
}

/// Records which setup cases may advance to verification.
#[derive(Default, Debug, PartialEq, Eq)]
struct SetupProgress {
    successful: Vec<usize>,
    failed: Vec<usize>,
}

impl SetupProgress {
    /// Records one attempted setup result and returns whether setup should continue.
    fn record(&mut self, index: usize, succeeded: bool, fail_fast: bool) -> bool {
        if succeeded {
            self.successful.push(index);
            true
        } else {
            self.failed.push(index);
            !fail_fast
        }
    }

    /// Returns whether successful setup cases may advance to the current stage.
    fn should_transition(&self, fail_fast: bool) -> bool {
        !self.successful.is_empty() && (!fail_fast || self.failed.is_empty())
    }
}

/// Run compatibility tests in bare distributed mode.
///
/// Starts an old-version distributed cluster, runs setup SQLs,
/// then restarts the cluster with a new version on preserved state,
/// and runs verify SQLs comparing results against `verify.result` files.
///
/// PR1 notes:
/// - Sqlness interceptor comments are supported for each statement.
/// - The runner starts the full distributed topology, including flownode.
#[derive(Debug, Parser)]
pub struct CompatCommand {
    /// Version of the "from" GreptimeDB binary (e.g. "v0.9.5") or "current".
    /// If neither --from-version nor --from-bins-dir is specified, the
    /// current debug build is used for both from and to.
    #[clap(long)]
    from_version: Option<String>,

    /// Path to the directory containing the "from" GreptimeDB binary.
    #[clap(long)]
    from_bins_dir: Option<PathBuf>,

    /// Path to the directory containing the "to" GreptimeDB binary.
    /// Defaults to the current debug build.
    #[clap(long)]
    to_bins_dir: Option<PathBuf>,

    /// Directory of compatibility test cases.
    /// Defaults to `tests/compatibility/cases` relative to workspace root.
    #[clap(long)]
    case_dir: Option<PathBuf>,

    /// Name of test cases to run. Accepts a regexp.
    #[clap(long, default_value = ".*")]
    test_filter: String,

    /// Fail this run as soon as one case fails.
    #[clap(long, default_value = "false")]
    fail_fast: bool,

    /// Preserve persistent state in the temporary directory after run.
    /// Etcd is always cleaned up regardless of this flag.
    #[clap(long, default_value = "false")]
    preserve_state: bool,

    /// Pull different versions of GreptimeDB on need.
    #[clap(long, default_value = "true")]
    pull_version_on_need: bool,

    /// Whether to set up etcd via Docker. Required for PR1 distributed compat.
    /// External metadata stores are not supported by the compat MVP yet.
    #[clap(long, default_value = "true")]
    setup_etcd: bool,

    /// Perform discovery and filtering only; print what would run without
    /// starting any services, mutating files, or running setup/verify.
    #[clap(long, default_value = "false")]
    dry_run: bool,
}

impl CompatCommand {
    pub async fn run(self) {
        let dry_run = self.dry_run;

        // ---- 1. Validate MVP runtime constraints ----
        if !dry_run && !self.setup_etcd {
            panic!(
                "compat MVP requires Docker etcd (--setup-etcd=true); external metadata stores are not supported yet"
            );
        }

        // ---- 2. Resolve case directory ----
        let case_dir = self
            .case_dir
            .clone()
            .unwrap_or_else(default_compat_case_dir);

        if !case_dir.is_dir() {
            panic!("Case directory not found: {}", case_dir.display());
        }

        // ---- 3. Discover cases ----
        let mut cases = compat_case::discover_cases(&case_dir).unwrap_or_else(|e| panic!("{e}"));

        // Filter by test_filter
        let filter_re = regex::Regex::new(&self.test_filter)
            .unwrap_or_else(|e| panic!("Invalid test filter regex '{}': {e}", self.test_filter));
        cases.retain(|c| filter_re.is_match(&c.metadata.name));

        // Filter by topology
        cases.retain(|c| c.metadata.topologies.iter().any(|t| t == COMPAT_TOPOLOGY));

        if cases.is_empty() {
            if dry_run {
                println!(
                    "DRY-RUN: no compat cases found matching filter '{}' and topology '{}'",
                    self.test_filter, COMPAT_TOPOLOGY
                );
            } else {
                println!(
                    "No compat cases found matching filter '{}' and topology '{}'",
                    self.test_filter, COMPAT_TOPOLOGY
                );
            }
            return;
        }

        // ---- 3b. Validate metadata (incl. version constraints) before filtering ----
        // Must run before version-range filtering so invalid constraints like
        // `>=not-a-version` cause a hard error instead of silent skip.
        compat_case::validate_cases_metadata(&cases).unwrap_or_else(|e| panic!("{e}"));

        // ---- 3c. Validate namespace dedup before version filtering ----
        // Validate globally for all selected topology/name cases so duplicated
        // namespaces cannot hide behind version filters.
        compat_case::validate_case_namespaces(&cases).unwrap_or_else(|e| panic!("{e}"));

        // ---- 4. Resolve "from" and "to" versions ----
        let (from_bins_dir, from_version, from_ver_parsed, to_bins_dir, to_version, to_ver_parsed) =
            if dry_run {
                // Dry-run: resolve versions without panicking on missing binaries.
                // try_infer_version returns None gracefully when the binary is absent.
                let dry_run_from_bins_dir = self
                    .from_bins_dir
                    .clone()
                    .unwrap_or_else(|| util::get_binary_dir("debug"));
                let from_ver_str = self
                    .from_version
                    .as_deref()
                    .and_then(|v| {
                        if v == "current" {
                            None
                        } else {
                            Some(v.to_string())
                        }
                    })
                    .or_else(|| try_infer_version(&dry_run_from_bins_dir).map(|v| v.to_string()));
                let from_ver_parsed = from_ver_str
                    .as_deref()
                    .and_then(|s| compat_case::Version::parse(s).ok());

                let dry_run_to_bins_dir = self
                    .to_bins_dir
                    .clone()
                    .unwrap_or_else(|| util::get_binary_dir("debug"));
                let to_ver_str = try_infer_version(&dry_run_to_bins_dir).map(|v| v.to_string());
                let to_ver_parsed = to_ver_str
                    .as_deref()
                    .and_then(|s| compat_case::Version::parse(s).ok());

                (
                    Some(dry_run_from_bins_dir),
                    from_ver_str,
                    from_ver_parsed,
                    Some(dry_run_to_bins_dir),
                    to_ver_str,
                    to_ver_parsed,
                )
            } else {
                // Normal path: resolve bins (may panic if binary not found).
                let from_bins_dir = resolve_bins(
                    self.from_bins_dir.as_ref(),
                    self.from_version.as_deref(),
                    self.pull_version_on_need,
                )
                .await;

                let from_version = if let Some(ref ver) = self.from_version {
                    if ver != "current" {
                        Some(ver.clone())
                    } else {
                        try_infer_version(&from_bins_dir).map(|v| v.to_string())
                    }
                } else {
                    try_infer_version(&from_bins_dir).map(|v| v.to_string())
                };

                let from_ver_parsed = from_version
                    .as_deref()
                    .and_then(|s| compat_case::Version::parse(s).ok());

                let to_bins_dir =
                    resolve_bins(self.to_bins_dir.as_ref(), None, self.pull_version_on_need).await;
                let to_version = try_infer_version(&to_bins_dir).map(|v| v.to_string());
                let to_ver_parsed = to_version
                    .as_deref()
                    .and_then(|s| compat_case::Version::parse(s).ok());

                (
                    Some(from_bins_dir),
                    from_version,
                    from_ver_parsed,
                    Some(to_bins_dir),
                    to_version,
                    to_ver_parsed,
                )
            };

        // ---- 5b. Filter by version range ----
        let pre_filter_count = cases.len();
        cases.retain(|c| {
            let from_ok = version_matches_range(from_ver_parsed.as_ref(), &c.metadata.from_range);
            if !from_ok {
                let from_label = from_ver_parsed
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                println!(
                    "Skipping case '{}': from_range {:?} does not match version '{}'",
                    c.metadata.name, c.metadata.from_range, from_label
                );
            }
            from_ok
        });
        cases.retain(|c| {
            let to_ok = version_matches_range(to_ver_parsed.as_ref(), &c.metadata.to_range);
            if !to_ok {
                let to_label = to_ver_parsed
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                println!(
                    "Skipping case '{}': to_range {:?} does not match version '{}'",
                    c.metadata.name, c.metadata.to_range, to_label
                );
            }
            to_ok
        });

        if pre_filter_count != cases.len() {
            println!(
                "Version-range filtering: {} → {} cases",
                pre_filter_count,
                cases.len()
            );
        }

        if cases.is_empty() {
            if dry_run {
                println!("DRY-RUN: no compat cases would run after version-range filtering");
            } else {
                println!("No compat cases remaining after version-range filtering");
            }
            return;
        }

        let selected_case_count = cases.len();
        let profiles = group_cases_by_profile(cases);
        for profile in &profiles {
            if let Some(old_config) = &profile.old_config {
                validate_old_config_against_datanode_template(old_config)
                    .unwrap_or_else(|e| panic!("Invalid old datanode configuration profile: {e}"));
            }
        }

        if dry_run {
            println!("DRY-RUN: would run {} compat case(s)", selected_case_count);
            println!("  topology:     {}", COMPAT_TOPOLOGY);
            println!(
                "  from version: {}",
                from_version.as_deref().unwrap_or(
                    "unknown (use --from-version, --from-bins-dir, or build debug binary)"
                )
            );
            println!(
                "  to version:   {}",
                to_version
                    .as_deref()
                    .unwrap_or("unknown (use --to-bins-dir or build debug binary)")
            );
            if pre_filter_count != selected_case_count {
                println!();
                println!(
                    "Version-range filtering reduced {} → {} cases (see 'Skipping case' messages above)",
                    pre_filter_count, selected_case_count
                );
            }
            println!();
            for (index, profile) in profiles.iter().enumerate() {
                println!("  profile: {}", profile.name(index));
                let keys = profile.config_keys();
                if keys.is_empty() {
                    println!("    config keys: baseline");
                } else {
                    println!("    config keys: {}", keys.join(", "));
                }
                for c in &profile.cases {
                    println!("    case:      {}", c.metadata.name);
                    println!("      namespace: {}", c.namespace);
                    println!("      from_range: {:?}", c.metadata.from_range);
                    println!("      to_range:   {:?}", c.metadata.to_range);
                    println!("      features:   {:?}", c.metadata.features);
                }
            }
            println!();
            println!("Dry run complete. Remove --dry-run to execute.");
            return;
        }

        println!(
            "Running compatibility profiles with topology {}:",
            COMPAT_TOPOLOGY
        );
        for (index, profile) in profiles.iter().enumerate() {
            println!(
                "  - {}: {} case(s)",
                profile.name(index),
                profile.cases.len()
            );
        }

        // ---- 6. Build interceptor registry ----
        let interceptor_registry = create_interceptor_registry();
        let from_bins_dir =
            from_bins_dir.expect("from_bins_dir must be resolved in non-dry-run mode");
        let to_bins_dir = to_bins_dir.expect("to_bins_dir must be resolved in non-dry-run mode");
        let mut failed = Vec::new();
        let mut preserved_homes = Vec::new();

        for (index, profile) in profiles.into_iter().enumerate() {
            let outcome = self
                .run_profile(
                    index,
                    profile,
                    &interceptor_registry,
                    &from_bins_dir,
                    &to_bins_dir,
                )
                .await;
            if let Some(home) = outcome.preserved_home {
                preserved_homes.push(home);
            }
            let profile_failed = !outcome.failed_cases.is_empty();
            failed.extend(outcome.failed_cases);
            if profile_failed && self.fail_fast {
                break;
            }
        }

        if self.preserve_state {
            println!("Preserved compatibility state directories:");
            for home in preserved_homes {
                println!("  - {}", home.display());
            }
        }

        if failed.is_empty() {
            println!("\n\x1b[32mAll compat tests passed!\x1b[0m");
        } else {
            println!("\n\x1b[31mFailed cases: {}\x1b[0m", failed.join(", "));
            panic!("Compatibility profiles failed");
        }
    }

    async fn run_profile(
        &self,
        index: usize,
        profile: CompatProfile,
        interceptor_registry: &Registry,
        from_bins_dir: &PathBuf,
        to_bins_dir: &PathBuf,
    ) -> ProfileOutcome {
        let profile_name = profile.name(index);
        let temp_dir = tempfile::Builder::new()
            .prefix(&format!("sqlness-compat-{profile_name}-"))
            .tempdir()
            .unwrap();
        let sqlness_home = temp_dir.keep();
        unsafe {
            std::env::set_var("SQLNESS_HOME", sqlness_home.display().to_string());
        }

        let store_config = StoreConfig {
            store_addrs: if self.setup_etcd {
                vec!["127.0.0.1:2379".to_string()]
            } else {
                vec![]
            },
            setup_etcd: self.setup_etcd,
            setup_pg: None,
            setup_mysql: None,
            enable_flat_format: false,
            enable_gc: false,
        };
        let env = Env::new(
            sqlness_home.clone(),
            ServerAddr::default(),
            WalConfig::RaftEngine,
            self.pull_version_on_need,
            Some(from_bins_dir.clone()),
            store_config,
            vec![],
        );
        let configured_old_profile = profile.old_config.is_some();
        if let Some(old_config) = profile.old_config.clone() {
            env.compat_activate_old_profile(old_config);
        }

        // Arm only immediately before this profile starts etcd. Scope guarantees
        // cleanup finishes before the next profile can acquire the same container.
        let mut etcd_guard = self.setup_etcd.then(EtcdGuard::new);
        println!("Starting profile '{profile_name}' with old-version distributed cluster...");
        let mut db = env.compat_start_distributed(0).await;
        let mut failed_cases = Vec::new();
        let mut setup_progress = SetupProgress::default();

        println!("Running setup phase for profile '{profile_name}'...");
        for (case_index, case) in profile.cases.iter().enumerate() {
            match run_compat_phase(&db, case, interceptor_registry, CompatPhase::Setup).await {
                Ok(()) => {
                    setup_progress.record(case_index, true, self.fail_fast);
                    println!("  Setup: {} - OK", case.metadata.name);
                }
                Err(e) => {
                    println!("  Setup: {} - FAILED: {e}", case.metadata.name);
                    if !setup_progress.record(case_index, false, self.fail_fast) {
                        break;
                    }
                }
            }
        }
        failed_cases.extend(
            setup_progress
                .failed
                .iter()
                .map(|index| profile.cases[*index].metadata.name.clone()),
        );

        if setup_progress.should_transition(self.fail_fast) {
            // This transition is deliberately before every current process spawn.
            // Subsequent verify-stage restart directives observe the clean stage too.
            if configured_old_profile {
                env.compat_activate_current_profile();
            }
            println!("Restarting profile '{profile_name}' with new-version binary...");
            env.compat_restart_all(&db, to_bins_dir.clone()).await;

            println!("Running verify phase for profile '{profile_name}'...");
            for case_index in setup_progress.successful {
                let case = &profile.cases[case_index];
                match run_compat_phase(&db, case, interceptor_registry, CompatPhase::Verify).await {
                    Ok(()) => println!("  Verify: {} - PASSED", case.metadata.name),
                    Err(e) => {
                        println!("  Verify: {} - FAILED: {e}", case.metadata.name);
                        failed_cases.push(case.metadata.name.clone());
                        if self.fail_fast {
                            break;
                        }
                    }
                }
            }
        }

        db.compat_stop();
        if self.setup_etcd {
            println!("Stopping etcd for profile '{profile_name}'");
            util::stop_rm_etcd();
        }
        if let Some(mut guard) = etcd_guard.take() {
            guard.disarm();
        }

        let preserved_home = if self.preserve_state {
            Some(sqlness_home)
        } else {
            println!("Removing state in {:?}", sqlness_home);
            tokio::fs::remove_dir_all(sqlness_home)
                .await
                .unwrap_or_else(|e| println!("Warning: failed to clean up temp dir: {e}"));
            None
        };

        ProfileOutcome {
            failed_cases,
            preserved_home,
        }
    }
}

/// Result of one isolated profile lifecycle.
struct ProfileOutcome {
    failed_cases: Vec<String>,
    preserved_home: Option<PathBuf>,
}

/// Guard that stops/removes Docker etcd on drop (panic or early exit).
/// Disarm before normal cleanup to avoid double-cleanup.
///
/// The guard refuses to arm if a container named `etcd` already exists, so a
/// failed compat run never deletes a developer-owned container with that name.
struct EtcdGuard {
    active: bool,
}

impl EtcdGuard {
    fn new() -> Self {
        let inspect_status = std::process::Command::new("docker")
            .args(["container", "inspect", "etcd"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        if inspect_status.is_ok_and(|status| status.success()) {
            panic!(
                "A Docker container named `etcd` already exists. \
                 Remove it before running compat tests so the cleanup guard \
                 cannot delete a container it did not create."
            );
        }
        Self { active: true }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for EtcdGuard {
    fn drop(&mut self) {
        if self.active {
            println!("EtcdGuard: emergency etcd cleanup (panic or early exit)");
            // Best-effort: don't panic in Drop
            let _ = std::process::Command::new("docker")
                .args(["container", "stop", "etcd"])
                .status();
            let _ = std::process::Command::new("docker")
                .args(["container", "rm", "etcd"])
                .status();
        }
    }
}

/// Phase of compat execution.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CompatPhase {
    Setup,
    Verify,
}

/// Create an interceptor registry matching the ordinary sqlness runner.
fn create_interceptor_registry() -> Registry {
    let mut interceptor_registry: Registry = Default::default();
    interceptor_registry.register(
        protocol_interceptor::PREFIX,
        Arc::new(protocol_interceptor::ProtocolInterceptorFactory),
    );
    interceptor_registry
}

/// Resolve binary directory: explicit path takes priority, then version (pulls if needed),
/// otherwise default to current debug build.
///
/// Validates that `<dir>/greptime` exists after resolution and canonicalizes the path.
async fn resolve_bins(
    bins_dir: Option<&PathBuf>,
    version: Option<&str>,
    pull_version_on_need: bool,
) -> PathBuf {
    let dir = if let Some(dir) = bins_dir {
        dir.clone()
    } else if let Some(ver) = version {
        if ver == "current" {
            util::get_binary_dir("debug")
        } else {
            util::maybe_pull_binary(ver, pull_version_on_need).await;
            let root = std::path::PathBuf::from(util::get_workspace_root());
            std::path::PathBuf::from_iter([root, std::path::PathBuf::from(ver)])
        }
    } else {
        // Default: current debug build
        util::get_binary_dir("debug")
    };

    // Canonicalize when possible (may fail if dir doesn't exist)
    let dir = match dir.canonicalize() {
        Ok(canon) => canon,
        Err(e) => panic!(
            "Cannot resolve binary directory '{}': {e}. \
             Use --from-bins-dir / --to-bins-dir to specify the correct path, \
             or --from-version to pull a release.",
            dir.display()
        ),
    };

    if !dir.join(util::PROGRAM).is_file() {
        panic!(
            "greptime binary not found in '{}'. \
             Use --from-bins-dir / --to-bins-dir to specify the correct directory, \
             or build greptime first (e.g. `cargo build -p greptime`). \
             Note: if you use a custom target-dir, the binary may be elsewhere; \
             pass the actual directory with --from-bins-dir or --to-bins-dir.",
            dir.display()
        );
    }

    dir
}

/// Default case directory: `tests/compatibility/cases` relative to workspace root.
fn default_compat_case_dir() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // CARGO_MANIFEST_DIR is tests/runner
    // Pop to tests/
    path.pop();
    path.push("compatibility");
    path.push("cases");
    path
}

/// Run a single compat phase (setup or verify) for one case.
async fn run_compat_phase(
    db: &crate::env::bare::GreptimeDB,
    case: &CompatCase,
    registry: &Registry,
    phase: CompatPhase,
) -> Result<(), String> {
    let sql_file = match phase {
        CompatPhase::Setup => case.dir.join("setup.sql"),
        CompatPhase::Verify => case.dir.join("verify.sql"),
    };

    let sql_content = std::fs::read_to_string(&sql_file)
        .map_err(|e| format!("Failed to read {}: {e}", sql_file.display()))?;

    let mut statements = parse_sql_file(&sql_content, registry)?;

    // Execute statements
    let mut verify_output = String::new();

    for statement in &mut statements {
        let (display, results) = statement.execute(db, &case.namespace).await?;

        match phase {
            CompatPhase::Setup => {
                // Setup: just check for success (already returned Ok)
            }
            CompatPhase::Verify => {
                verify_output.push_str(&display);
                for result in results {
                    verify_output.push_str(&result);
                    verify_output.push('\n');
                    verify_output.push('\n');
                }
            }
        }
    }

    if phase == CompatPhase::Verify {
        trim_trailing_blank_lines(&mut verify_output);

        let result_path = case.dir.join("verify.result");

        // If verify.result doesn't exist, generate it from actual output but
        // return an error so the author must review, commit, and rerun.
        if !result_path.is_file() {
            std::fs::write(&result_path, &verify_output)
                .map_err(|e| format!("Failed to create {}: {e}", result_path.display()))?;
            return Err(format!(
                "Created missing verify.result for case '{}'; review the generated file, commit it, and rerun",
                case.metadata.name
            ));
        }

        let expected = std::fs::read_to_string(&result_path)
            .map_err(|e| format!("Failed to read {}: {e}", result_path.display()))?;

        if verify_output != expected {
            // Update the result file with actual output to aid local update.
            std::fs::write(&result_path, &verify_output)
                .map_err(|e| format!("Failed to update {}: {e}", result_path.display()))?;

            // Generate a simple diff
            let diff = simple_diff(&expected, &verify_output);
            return Err(format!(
                "Result mismatch for case '{}'.\nDiff:\n{diff}",
                case.metadata.name
            ));
        }
    }

    Ok(())
}

/// Keep generated snapshots compatible with `git diff --check` by avoiding a
/// trailing blank line at EOF while preserving the final newline.
fn trim_trailing_blank_lines(output: &mut String) {
    while output.ends_with("\n\n") {
        output.pop();
    }
}

/// Execute the namespace prelude (CREATE DATABASE IF NOT EXISTS + USE) for a case.
/// This is NOT written into verify.result.
///
/// The prelude is protocol-aware:
/// - `CREATE DATABASE` is always sent via gRPC so it works regardless of
///   statement-level protocol directives.
/// - For Postgres-protocol statements, `SET search_path` selects the case
///   namespace instead of running `USE` (which is not valid PG SQL).
/// - For MySQL and default/gRPC statements, `USE <ns>` runs through the
///   statement's effective context.
async fn run_namespace_prelude(
    db: &crate::env::bare::GreptimeDB,
    namespace: &str,
    query_ctx: &QueryContext,
) -> Result<(), String> {
    // CREATE DATABASE always via gRPC — no protocol override
    let create_db = format!("CREATE DATABASE IF NOT EXISTS {namespace}");
    let default_ctx = QueryContext::default();
    db.compat_query(&create_db, &default_ctx).await?;

    // Postgres: select the namespace via search_path instead of USE.
    if query_ctx
        .context
        .get(PROTOCOL_KEY)
        .is_some_and(|p| p == POSTGRES)
    {
        let set_search_path = format!("SET search_path TO '{namespace}'");
        db.compat_query(&set_search_path, query_ctx).await?;
        return Ok(());
    }

    // MySQL / default (gRPC): execute USE
    let use_db = format!("USE {namespace}");
    db.compat_query(&use_db, query_ctx).await?;

    Ok(())
}

/// A parsed SQL statement with sqlness comments and interceptors.
struct ParsedStatement {
    comment_lines: Vec<String>,
    display_query: Vec<String>,
    execute_query: Vec<String>,
    interceptors: Vec<InterceptorRef>,
}

impl ParsedStatement {
    fn new() -> Self {
        Self {
            comment_lines: Vec::new(),
            display_query: Vec::new(),
            execute_query: Vec::new(),
            interceptors: Vec::new(),
        }
    }

    fn push_comment(&mut self, line: String) {
        self.comment_lines.push(line);
    }

    fn push_interceptor(&mut self, line: &str, registry: &Registry) -> Result<(), String> {
        let Some((_, remaining)) = line.split_once(INTERCEPTOR_PREFIX) else {
            return Err(format!(
                "Missing sqlness interceptor prefix in line: {line}"
            ));
        };
        let interceptor = registry.create(remaining).map_err(|e| e.to_string())?;
        self.interceptors.push(interceptor);
        Ok(())
    }

    fn append_query_line(&mut self, line: &str) {
        self.display_query.push(line.to_string());
        self.execute_query.push(line.to_string());
    }

    fn is_empty(&self) -> bool {
        self.comment_lines.is_empty()
            && self.display_query.is_empty()
            && self.execute_query.is_empty()
            && self.interceptors.is_empty()
    }

    fn has_query(&self) -> bool {
        !self.execute_query.is_empty()
    }

    fn display_text(&self) -> String {
        let mut output = String::new();
        for comment in &self.comment_lines {
            output.push_str(comment);
            output.push('\n');
        }
        for line in &self.display_query {
            output.push_str(line);
        }
        output.push('\n');
        output.push('\n');
        output
    }

    fn concat_query_lines(&self) -> String {
        self.execute_query
            .iter()
            .fold(String::new(), |query, line| query + line)
            .trim_start()
            .to_string()
    }

    async fn before_execute_intercept(&mut self) -> QueryContext {
        let mut context = QueryContext::default();
        for interceptor in &self.interceptors {
            interceptor
                .before_execute_async(&mut self.execute_query, &mut context)
                .await;
        }
        context
    }

    async fn after_execute_intercept(&self, result: &mut String) {
        for interceptor in &self.interceptors {
            interceptor.after_execute_async(result).await;
        }
    }

    async fn execute(
        &mut self,
        db: &crate::env::bare::GreptimeDB,
        namespace: &str,
    ) -> Result<(String, Vec<String>), String> {
        let display = self.display_text();
        let context = self.before_execute_intercept().await;
        db.compat_prepare_query_context(&context).await;
        run_namespace_prelude(db, namespace, &context).await?;
        let sql = self.concat_query_lines();
        let mut results = Vec::new();

        for sql in sql.split(TEMPLATE_DELIMITER) {
            if sql.trim().is_empty() {
                continue;
            }
            let sql = if sql.ends_with(QUERY_DELIMITER) {
                sql.to_string()
            } else {
                format!("{sql};")
            };
            let mut result = db.compat_query(&sql, &context).await?;
            self.after_execute_intercept(&mut result).await;
            results.push(result);
        }

        Ok((display, results))
    }
}

/// Parse a SQL file into statements using the same sqlness comment/interceptor
/// conventions as the ordinary runner.
fn parse_sql_file(content: &str, registry: &Registry) -> Result<Vec<ParsedStatement>, String> {
    let mut statements = Vec::new();
    let mut current_stmt = ParsedStatement::new();

    for line in content.lines() {
        if line.starts_with(COMMENT_PREFIX) {
            current_stmt.push_comment(line.to_string());

            if line.starts_with(INTERCEPTOR_PREFIX) {
                current_stmt.push_interceptor(line, registry)?;
            }
            continue;
        }

        if line.is_empty() {
            continue;
        }

        current_stmt.append_query_line(line);

        // Check for statement terminator
        if line.ends_with(QUERY_DELIMITER) {
            if current_stmt.has_query() {
                statements.push(current_stmt);
            }
            current_stmt = ParsedStatement::new();
        } else {
            current_stmt.append_query_line("\n");
        }
    }

    // Flush any remaining statement
    if !current_stmt.is_empty() && current_stmt.has_query() {
        statements.push(current_stmt);
    }

    if statements.is_empty() {
        return Err("No SQL statements found in file".to_string());
    }

    Ok(statements)
}

/// Generate a simple line-based diff between expected and actual.
fn simple_diff(expected: &str, actual: &str) -> String {
    let mut diff = String::new();
    let expected_lines: Vec<&str> = expected.lines().collect();
    let actual_lines: Vec<&str> = actual.lines().collect();
    let max_len = expected_lines.len().max(actual_lines.len());

    for i in 0..max_len {
        let exp = expected_lines.get(i).unwrap_or(&"(missing)");
        let act = actual_lines.get(i).unwrap_or(&"(missing)");
        if exp != act {
            diff.push_str(&format!("  Line {}:\n", i + 1));
            diff.push_str(&format!("    expected: {exp}\n"));
            diff.push_str(&format!("    actual:   {act}\n"));
        }
    }

    if diff.is_empty() {
        diff.push_str("  (files differ but no line-level diff found — may be whitespace)\n");
    }

    diff
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::*;
    use crate::cmd::compat_case::CaseMetadata;

    #[test]
    fn test_trim_trailing_blank_lines_preserves_single_final_newline() {
        let mut output = "SELECT 1;\n\n+---+\n\n".to_string();
        trim_trailing_blank_lines(&mut output);

        assert_eq!(output, "SELECT 1;\n\n+---+\n");
    }

    fn test_case(name: &str, old_config: Option<OldConfig>) -> CompatCase {
        CompatCase {
            metadata: CaseMetadata {
                name: name.to_string(),
                reason: "test".to_string(),
                introduced_by: "test".to_string(),
                topologies: vec![COMPAT_TOPOLOGY.to_string()],
                from_range: vec!["*".to_string()],
                to_range: vec!["*".to_string()],
                features: vec!["table".to_string()],
                owner: "test".to_string(),
                namespace: None,
                old_config,
            },
            dir: PathBuf::from(name),
            namespace: name.to_string(),
        }
    }

    fn old_profile(value: bool) -> OldConfig {
        OldConfig {
            datanode: BTreeMap::from([(
                "region_engine.metric.sparse_primary_key_encoding".to_string(),
                value,
            )]),
        }
    }

    #[test]
    fn test_group_cases_by_full_normalized_config_profile() {
        let profiles = group_cases_by_profile(vec![
            test_case("baseline_a", None),
            test_case("same_a", Some(old_profile(false))),
            test_case("baseline_b", None),
            test_case("same_b", Some(old_profile(false))),
            test_case("distinct", Some(old_profile(true))),
        ]);

        assert_eq!(profiles.len(), 3);
        assert!(profiles[0].old_config.is_none());
        assert_eq!(profiles[0].cases.len(), 2);
        assert_eq!(profiles[1].cases.len(), 2);
        assert_eq!(profiles[2].cases.len(), 1);
        assert_eq!(
            profiles[0]
                .cases
                .iter()
                .map(|case| case.metadata.name.as_str())
                .collect::<Vec<_>>(),
            vec!["baseline_a", "baseline_b"]
        );
        assert_eq!(
            profiles[1]
                .cases
                .iter()
                .map(|case| case.metadata.name.as_str())
                .collect::<Vec<_>>(),
            vec!["same_a", "same_b"]
        );
        assert_ne!(profiles[1].old_config, profiles[2].old_config);
    }

    #[test]
    fn test_setup_failure_in_middle_verifies_other_successful_cases() {
        let mut progress = SetupProgress::default();
        for (index, succeeded) in [true, false, true].into_iter().enumerate() {
            assert!(progress.record(index, succeeded, false));
        }

        assert_eq!(progress.successful, vec![0, 2]);
        assert_eq!(progress.failed, vec![1]);
        assert!(progress.should_transition(false));
    }

    #[test]
    fn test_fail_fast_stops_setup_without_marking_unattempted_cases() {
        let mut progress = SetupProgress::default();
        assert!(progress.record(0, true, true));
        assert!(!progress.record(1, false, true));

        assert_eq!(progress.successful, vec![0]);
        assert_eq!(progress.failed, vec![1]);
        assert!(!progress.should_transition(true));
    }
}
