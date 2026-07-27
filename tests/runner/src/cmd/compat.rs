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

use clap::{Parser, ValueEnum};
use sqlness::QueryContext;
use sqlness::interceptor::template::DELIMITER as TEMPLATE_DELIMITER;
use sqlness::interceptor::{InterceptorRef, Registry};

use crate::cmd::bare::ServerAddr;
use crate::cmd::compat_case::{self, CompatCase, try_infer_version, version_matches_range};
use crate::cmd::datanode_overlay::{
    DatanodeOverlay, DatanodeProtectionPolicy, PreparedDatanodeOverlay,
};
use crate::env::bare::{Env, GreptimeDB, StoreConfig, WalConfig};
use crate::protocol_interceptor::{self, POSTGRES, PROTOCOL_KEY};
use crate::util;

const COMMENT_PREFIX: &str = "--";
const INTERCEPTOR_PREFIX: &str = "-- SQLNESS";
const QUERY_DELIMITER: char = ';';

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum CompatTopology {
    Distributed,
    Standalone,
}

impl CompatTopology {
    fn as_str(self) -> &'static str {
        match self {
            Self::Distributed => "distributed",
            Self::Standalone => "standalone",
        }
    }
}

/// Run compatibility tests in bare mode.
///
/// Starts a "from" distributed cluster, runs setup SQLs,
/// then restarts the cluster with a "to" version on preserved state,
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

    /// Version of the "to" GreptimeDB binary (e.g. "v1.1.4") or "current".
    /// Downloads the release binary when needed. Cannot be used with
    /// `--to-bins-dir`.
    #[clap(long)]
    to_version: Option<String>,

    /// Directory of compatibility test cases.
    /// Defaults to `tests/compatibility/cases` relative to workspace root.
    #[clap(long)]
    case_dir: Option<PathBuf>,

    /// Name of test cases to run. Accepts a regexp.
    #[clap(long, default_value = ".*")]
    test_filter: String,

    /// Require exactly this many cases after all filters are applied.
    #[clap(long)]
    expect_cases: Option<usize>,

    /// Topology to start for the compatibility test.
    #[clap(long, value_enum, default_value_t = CompatTopology::Distributed)]
    topology: CompatTopology,

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
        let topology = self.topology;

        if self.to_bins_dir.is_some() && self.to_version.is_some() {
            panic!("--to-version cannot be used with --to-bins-dir");
        }

        // ---- 1. Validate MVP runtime constraints ----
        if !dry_run && topology == CompatTopology::Distributed && !self.setup_etcd {
            panic!(
                "compat MVP requires Docker etcd (--setup-etcd=true); external metadata stores are not supported yet"
            );
        }

        // ---- 2. Resolve case directory ----
        let case_dir = self.case_dir.unwrap_or_else(default_compat_case_dir);

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
        cases.retain(|c| {
            c.metadata
                .topologies
                .iter()
                .any(|candidate| candidate == topology.as_str())
        });

        if cases.is_empty() {
            if let Some(expected_cases) = self.expect_cases {
                panic!(
                    "Expected {expected_cases} compatibility cases after name and topology filtering, found 0"
                );
            }
            if dry_run {
                println!(
                    "DRY-RUN: no compat cases found matching filter '{}' and topology '{}'",
                    self.test_filter,
                    topology.as_str()
                );
            } else {
                println!(
                    "No compat cases found matching filter '{}' and topology '{}'",
                    self.test_filter,
                    topology.as_str()
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

                let dry_run_to_bins_dir = self.to_bins_dir.clone().unwrap_or_else(|| {
                    self.to_version
                        .as_deref()
                        .filter(|version| *version != "current")
                        .map(|version| PathBuf::from(util::get_workspace_root()).join(version))
                        .unwrap_or_else(|| util::get_binary_dir("debug"))
                });
                let to_ver_str = self
                    .to_version
                    .as_deref()
                    .filter(|version| *version != "current")
                    .map(str::to_string)
                    .or_else(|| try_infer_version(&dry_run_to_bins_dir).map(|v| v.to_string()));
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

                let to_bins_dir = resolve_bins(
                    self.to_bins_dir.as_ref(),
                    self.to_version.as_deref(),
                    self.pull_version_on_need,
                )
                .await;
                let to_version = self
                    .to_version
                    .as_deref()
                    .filter(|version| *version != "current")
                    .map(str::to_string)
                    .or_else(|| try_infer_version(&to_bins_dir).map(|v| v.to_string()));
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
            if let Some(expected_cases) = self.expect_cases {
                panic!(
                    "Expected {expected_cases} compatibility cases after all filtering, found 0"
                );
            }
            if dry_run {
                println!("DRY-RUN: no compat cases would run after version-range filtering");
            } else {
                println!("No compat cases remaining after version-range filtering");
            }
            return;
        }

        let wal_config = WalConfig::RaftEngine;
        let profiles =
            prepare_compat_profiles(cases, &wal_config).unwrap_or_else(|error| panic!("{error}"));
        emit_protected_path_warnings(&profiles);

        if let Some(expected_cases) = self.expect_cases {
            assert_eq!(
                profiles.case_count(),
                expected_cases,
                "Expected {expected_cases} compatibility cases after all filtering, found {}",
                profiles.case_count()
            );
        }

        if dry_run {
            println!(
                "DRY-RUN: would run {} compat case(s)",
                profiles.case_count()
            );
            println!("  topology:     {}", topology.as_str());
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
            if pre_filter_count != profiles.case_count() {
                println!();
                println!(
                    "Version-range filtering reduced {} → {} cases (see 'Skipping case' messages above)",
                    pre_filter_count,
                    profiles.case_count()
                );
            }
            println!();
            for c in profiles.cases() {
                println!("  case:        {}", c.metadata.name);
                println!("    namespace:   {}", c.namespace);
                println!("    from_range:  {:?}", c.metadata.from_range);
                println!("    to_range:    {:?}", c.metadata.to_range);
                println!("    features:    {:?}", c.metadata.features);
            }
            println!("DRY-RUN: compatibility profiles:");
            for profile in profiles.iter() {
                println!("  profile:      {}", profile.profile_id());
                println!("    cases:       {}", profile.case_names().join(", "));
                for source in profile.sources() {
                    println!("    sidecar:     {}", source.display());
                }
            }
            println!();
            println!("Dry run complete. Remove --dry-run to execute.");
            return;
        }

        println!(
            "Running {} compat case(s) with topology {}:",
            profiles.case_count(),
            topology.as_str()
        );
        for c in profiles.cases() {
            println!(
                "  - {} (namespace: {}, topologies: {:?})",
                c.metadata.name, c.namespace, c.metadata.topologies
            );
        }

        // ---- 6. Build interceptor registry ----
        let interceptor_registry = create_interceptor_registry();
        let to_bins_dir = to_bins_dir.expect("to_bins_dir must be resolved in non-dry-run mode");
        let profile_config = ProfileRunConfig {
            interceptor_registry: &interceptor_registry,
            from_bins_dir,
            to_bins_dir,
            pull_version_on_need: self.pull_version_on_need,
            setup_etcd: self.setup_etcd,
            fail_fast: self.fail_fast,
            topology,
            preserve_state: self.preserve_state,
            wal_config,
        };
        let mut failures = Vec::new();
        let mut preserved_states = Vec::new();
        for profile in profiles.iter() {
            let outcome = run_profile(profile, &profile_config).await;
            failures.extend(outcome.failures);
            if let Some(state) = outcome.preserved_state {
                preserved_states.push(state);
            }
            if outcome.stop_remaining_profiles {
                break;
            }
        }

        for state in &preserved_states {
            println!("Preserved compat profile state: {}", state.display());
        }

        if failures.is_empty() {
            println!("\n\x1b[32mAll compat tests passed!\x1b[0m");
        } else {
            panic!("\n\x1b[31mFailed cases: {}\x1b[0m", failures.join(", "));
        }
    }
}

/// Cases sharing one old-stage datanode configuration lifecycle.
#[derive(Debug)]
enum CompatProfile {
    Baseline {
        cases: Vec<CompatCase>,
    },
    Overlay {
        overlay: Arc<PreparedDatanodeOverlay>,
        cases: Vec<CompatCase>,
        sources: Vec<PathBuf>,
    },
}

impl CompatProfile {
    fn profile_id(&self) -> &str {
        match self {
            Self::Baseline { .. } => "baseline",
            Self::Overlay { overlay, .. } => overlay.profile_id(),
        }
    }

    fn cases(&self) -> &[CompatCase] {
        match self {
            Self::Baseline { cases } | Self::Overlay { cases, .. } => cases,
        }
    }

    fn case_names(&self) -> Vec<&str> {
        self.cases()
            .iter()
            .map(|case| case.metadata.name.as_str())
            .collect()
    }

    fn sources(&self) -> &[PathBuf] {
        match self {
            Self::Baseline { .. } => &[],
            Self::Overlay { sources, .. } => sources,
        }
    }

    fn overlay(&self) -> Option<Arc<PreparedDatanodeOverlay>> {
        match self {
            Self::Baseline { .. } => None,
            Self::Overlay { overlay, .. } => Some(Arc::clone(overlay)),
        }
    }

    fn touched_protected_paths(&self) -> &[crate::cmd::datanode_overlay::DottedPath] {
        match self {
            Self::Baseline { .. } => &[],
            Self::Overlay { overlay, .. } => overlay.touched_protected_paths(),
        }
    }
}

/// Deterministically ordered compatibility profiles.
#[derive(Debug)]
struct CompatProfiles(Vec<CompatProfile>);

impl CompatProfiles {
    fn iter(&self) -> impl Iterator<Item = &CompatProfile> {
        self.0.iter()
    }

    fn cases(&self) -> impl Iterator<Item = &CompatCase> {
        self.0.iter().flat_map(CompatProfile::cases)
    }

    fn case_count(&self) -> usize {
        self.0.iter().map(|profile| profile.cases().len()).sum()
    }
}

#[derive(Debug)]
struct OverlayProfileGroup {
    overlay: Arc<PreparedDatanodeOverlay>,
    cases: Vec<CompatCase>,
    sources: Vec<PathBuf>,
}

fn prepare_compat_profiles(
    cases: Vec<CompatCase>,
    wal_config: &WalConfig,
) -> Result<CompatProfiles, String> {
    let protection = DatanodeProtectionPolicy::for_wal(wal_config);
    let mut baseline_cases = Vec::new();
    let mut overlays: BTreeMap<[u8; 32], OverlayProfileGroup> = BTreeMap::new();

    for case in cases {
        let Some(reference) = case.metadata.old_datanode_overlay() else {
            baseline_cases.push(case);
            continue;
        };
        let overlay = DatanodeOverlay::load(&case.dir, reference).map_err(|error| {
            format!(
                "Failed to load old datanode overlay for compatibility case '{}': {error}",
                case.metadata.name
            )
        })?;
        let prepared = Arc::new(overlay.prepare(&protection).map_err(|error| {
            format!(
                "Failed to prepare old datanode overlay for compatibility case '{}': {error}",
                case.metadata.name
            )
        })?);
        let key = *prepared.profile_key();
        let source = prepared.source().to_path_buf();
        let entry = overlays.entry(key).or_insert_with(|| OverlayProfileGroup {
            overlay: Arc::clone(&prepared),
            cases: Vec::new(),
            sources: Vec::new(),
        });
        entry.cases.push(case);
        entry.sources.push(source);
    }

    baseline_cases.sort_by(|left, right| left.metadata.name.cmp(&right.metadata.name));
    let mut profiles = Vec::new();
    if !baseline_cases.is_empty() {
        profiles.push(CompatProfile::Baseline {
            cases: baseline_cases,
        });
    }
    for (
        _,
        OverlayProfileGroup {
            overlay,
            mut cases,
            mut sources,
        },
    ) in overlays
    {
        cases.sort_by(|left, right| left.metadata.name.cmp(&right.metadata.name));
        sources.sort();
        sources.dedup();
        profiles.push(CompatProfile::Overlay {
            overlay,
            cases,
            sources,
        });
    }

    Ok(CompatProfiles(profiles))
}

fn emit_protected_path_warnings(profiles: &CompatProfiles) {
    for profile in profiles.iter() {
        let paths: Vec<_> = profile
            .touched_protected_paths()
            .iter()
            .map(ToString::to_string)
            .collect();
        if paths.is_empty() {
            continue;
        }
        println!(
            "{}",
            format_protected_path_warning(profile.profile_id(), profile.case_names(), paths)
        );
    }
}

fn format_protected_path_warning(
    profile_id: &str,
    mut case_names: Vec<&str>,
    mut paths: Vec<String>,
) -> String {
    case_names.sort_unstable();
    paths.sort_unstable();
    format!(
        "Warning: datanode overlay profile {profile_id} touches runner-owned paths [{}] for cases [{}]",
        paths.join(", "),
        case_names.join(", ")
    )
}

#[derive(Debug)]
struct ProfileOutcome {
    failures: Vec<String>,
    stop_remaining_profiles: bool,
    preserved_state: Option<PathBuf>,
}

/// Pure compatibility profile progress policy, independent of process ownership.
#[derive(Default)]
struct ProfileProgress {
    successful_setup_indexes: Vec<usize>,
    failures: Vec<String>,
    fail_fast_setup_failure: bool,
    fail_fast_verify_failure: bool,
    cleanup_failed: bool,
}

impl ProfileProgress {
    fn record_setup_success(&mut self, case_index: usize) {
        self.successful_setup_indexes.push(case_index);
    }

    fn record_setup_failure(&mut self, failure: String, fail_fast: bool) -> bool {
        self.failures.push(failure);
        self.fail_fast_setup_failure = fail_fast;
        fail_fast
    }

    fn record_verify_failure(&mut self, failure: String, fail_fast: bool) -> bool {
        self.failures.push(failure);
        self.fail_fast_verify_failure = fail_fast;
        fail_fast
    }

    fn record_cleanup_failure(&mut self, failure: String) {
        self.failures.push(failure);
        self.cleanup_failed = true;
    }

    fn should_transition_to_current(&self) -> bool {
        !self.successful_setup_indexes.is_empty() && !self.fail_fast_setup_failure
    }

    fn should_stop_remaining_profiles(&self) -> bool {
        self.fail_fast_setup_failure || self.fail_fast_verify_failure || self.cleanup_failed
    }
}

struct ProfileRunConfig<'a> {
    interceptor_registry: &'a Registry,
    from_bins_dir: Option<PathBuf>,
    to_bins_dir: PathBuf,
    pull_version_on_need: bool,
    setup_etcd: bool,
    fail_fast: bool,
    topology: CompatTopology,
    preserve_state: bool,
    wal_config: WalConfig,
}

async fn run_profile(profile: &CompatProfile, config: &ProfileRunConfig<'_>) -> ProfileOutcome {
    println!("Running compatibility profile {}", profile.profile_id());
    let temp_dir = tempfile::Builder::new()
        .prefix(&format!("sqlness-compat-{}-", profile.profile_id()))
        .tempdir()
        .unwrap();
    let profile_state = ProfileStateGuard::new(temp_dir, config.preserve_state);
    let sqlness_home = profile_state.path().to_path_buf();
    // Standalone runs need no etcd.
    let setup_etcd = config.topology == CompatTopology::Distributed && config.setup_etcd;
    unsafe {
        std::env::set_var(
            "SQLNESS_HOME",
            sqlness_home.join("copy").display().to_string(),
        );
    }

    let store_config = StoreConfig {
        store_addrs: setup_etcd
            .then(|| "127.0.0.1:2379".to_string())
            .into_iter()
            .collect(),
        setup_etcd,
        setup_pg: None,
        setup_mysql: None,
        enable_flat_format: false,
        enable_gc: false,
    };
    let env = Env::new(
        sqlness_home.clone(),
        ServerAddr::default(),
        config.wal_config.clone(),
        config.pull_version_on_need,
        config.from_bins_dir.clone(),
        store_config,
        vec![],
    );
    if let Some(overlay) = profile.overlay() {
        env.activate_compat_old(overlay);
    }

    // Arm immediately before starting the profile so earlier preflight failures
    // cannot remove a developer-owned etcd container.
    let etcd_guard = setup_etcd.then(EtcdGuard::new);
    println!(
        "Starting old-version {} cluster...",
        config.topology.as_str()
    );
    let db = match config.topology {
        CompatTopology::Distributed => env.compat_start_distributed(0).await,
        CompatTopology::Standalone => env.compat_start_standalone(0).await,
    };

    let mut progress = ProfileProgress::default();
    for (case_index, case) in profile.cases().iter().enumerate() {
        match run_compat_phase(&db, case, config.interceptor_registry, CompatPhase::Setup).await {
            Ok(()) => {
                println!("  Setup: {} - OK", case.metadata.name);
                progress.record_setup_success(case_index);
            }
            Err(error) => {
                println!("  Setup: {} - FAILED: {error}", case.metadata.name);
                if progress.record_setup_failure(
                    format!("{} (setup): {error}", case.metadata.name),
                    config.fail_fast,
                ) {
                    break;
                }
            }
        }
    }

    if progress.should_transition_to_current() {
        println!("Restarting cluster with new-version binary on preserved state...");
        env.activate_compat_current();
        env.compat_restart(&db, config.to_bins_dir.clone())
            .await;

        println!("Running verify phase...");
        for case_index in progress.successful_setup_indexes.clone() {
            let case = &profile.cases()[case_index];
            match run_compat_phase(&db, case, config.interceptor_registry, CompatPhase::Verify)
                .await
            {
                Ok(()) => println!("  Verify: {} - PASSED", case.metadata.name),
                Err(error) => {
                    println!("  Verify: {} - FAILED: {error}", case.metadata.name);
                    if progress.record_verify_failure(
                        format!("{} (verify): {error}", case.metadata.name),
                        config.fail_fast,
                    ) {
                        break;
                    }
                }
            }
        }
    } else if progress.successful_setup_indexes.is_empty() {
        println!("Skipping current-version restart: no setup cases succeeded");
    }

    let cleanup = cleanup_profile(db, etcd_guard, setup_etcd, profile_state).await;
    for failure in cleanup.failures {
        progress.record_cleanup_failure(failure);
    }
    let stop_remaining_profiles = progress.should_stop_remaining_profiles();
    ProfileOutcome {
        failures: progress.failures,
        stop_remaining_profiles,
        preserved_state: cleanup.preserved_state,
    }
}

struct CleanupOutcome {
    failures: Vec<String>,
    preserved_state: Option<PathBuf>,
}

/// Owns one profile state directory until normal cleanup or unwind finalization.
struct ProfileStateGuard {
    temp_dir: Option<tempfile::TempDir>,
    preserve_state: bool,
}

impl ProfileStateGuard {
    fn new(temp_dir: tempfile::TempDir, preserve_state: bool) -> Self {
        Self {
            temp_dir: Some(temp_dir),
            preserve_state,
        }
    }

    fn path(&self) -> &std::path::Path {
        self.temp_dir.as_ref().unwrap().path()
    }

    fn finalize(mut self) -> CleanupOutcome {
        let temp_dir = self.temp_dir.take().unwrap();
        let mut failures = Vec::new();
        if self.preserve_state {
            let path = temp_dir.keep();
            if let Err(error) = std::fs::metadata(&path) {
                failures.push(format!(
                    "failed to confirm preserved profile state {}: {error}",
                    path.display()
                ));
            }
            return CleanupOutcome {
                failures,
                preserved_state: Some(path),
            };
        }

        println!("Removing state in {}", temp_dir.path().display());
        if let Err(error) = temp_dir.close() {
            failures.push(format!("failed to remove profile state: {error}"));
        }
        CleanupOutcome {
            failures,
            preserved_state: None,
        }
    }
}

impl Drop for ProfileStateGuard {
    fn drop(&mut self) {
        let Some(temp_dir) = self.temp_dir.take() else {
            return;
        };
        if self.preserve_state {
            let path = temp_dir.keep();
            println!(
                "Preserved compat profile state after abnormal exit: {}",
                path.display()
            );
        }
    }
}

async fn cleanup_profile(
    mut db: GreptimeDB,
    mut etcd_guard: Option<EtcdGuard>,
    setup_etcd: bool,
    profile_state: ProfileStateGuard,
) -> CleanupOutcome {
    db.compat_stop();
    drop(db);
    let mut failures = Vec::new();
    if setup_etcd {
        println!("Stopping etcd");
        match util::stop_rm_etcd_checked() {
            Ok(()) => {
                if let Some(guard) = etcd_guard.as_mut() {
                    guard.disarm();
                }
            }
            Err(error) => failures.push(format!("profile etcd cleanup: {error}")),
        }
    }
    // On failed checked cleanup this Drop performs one best-effort retry before
    // profile state is finalized.
    drop(etcd_guard);

    let mut state_outcome = profile_state.finalize();
    failures.append(&mut state_outcome.failures);
    CleanupOutcome {
        failures,
        preserved_state: state_outcome.preserved_state,
    }
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
            if let Err(error) = util::stop_rm_etcd_checked() {
                println!("EtcdGuard: emergency etcd cleanup failed: {error}");
            }
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
    use std::path::Path;

    use super::*;
    use crate::cmd::compat_case::{CaseMetadata, OldConfigMetadata};

    fn test_case(temp_dir: &Path, name: &str, overlay: Option<&str>) -> CompatCase {
        let case_dir = temp_dir.join(name);
        std::fs::create_dir_all(&case_dir).unwrap();
        if let Some(overlay) = overlay {
            std::fs::write(case_dir.join("overlay.toml"), overlay).unwrap();
        }
        CompatCase {
            metadata: CaseMetadata {
                name: name.to_string(),
                reason: "test".to_string(),
                introduced_by: "test".to_string(),
                topologies: vec![CompatTopology::Distributed.as_str().to_string()],
                from_range: vec!["*".to_string()],
                to_range: vec!["*".to_string()],
                features: vec!["table".to_string()],
                owner: "test".to_string(),
                namespace: None,
                old_config: overlay.map(|_| OldConfigMetadata {
                    datanode: PathBuf::from("overlay.toml"),
                }),
            },
            dir: case_dir,
            namespace: name.to_string(),
        }
    }

    #[test]
    fn test_trim_trailing_blank_lines_preserves_single_final_newline() {
        let mut output = "SELECT 1;\n\n+---+\n\n".to_string();
        trim_trailing_blank_lines(&mut output);

        assert_eq!(output, "SELECT 1;\n\n+---+\n");
    }

    #[test]
    fn profiles_are_baseline_first_and_group_by_full_semantic_digest() {
        let temp_dir = tempfile::tempdir().unwrap();
        let profiles = prepare_compat_profiles(
            vec![
                test_case(temp_dir.path(), "overlay_b", Some("[x]\nb = 2\na = 1\n")),
                test_case(temp_dir.path(), "baseline", None),
                test_case(temp_dir.path(), "overlay_a", Some("[x]\na = 1\nb = 2\n")),
                test_case(temp_dir.path(), "overlay_c", Some("value = 3\n")),
            ],
            &WalConfig::RaftEngine,
        )
        .unwrap();
        let profiles: Vec<_> = profiles.iter().collect();

        assert!(matches!(profiles[0], CompatProfile::Baseline { .. }));
        assert_eq!(profiles[0].case_names(), ["baseline"]);
        assert_eq!(profiles[1].case_names(), ["overlay_a", "overlay_b"]);
        assert_eq!(profiles[1].sources().len(), 2);
        let overlay_keys: Vec<_> = profiles[1..]
            .iter()
            .map(|profile| match profile {
                CompatProfile::Overlay { overlay, .. } => *overlay.profile_key(),
                CompatProfile::Baseline { .. } => unreachable!(),
            })
            .collect();
        assert!(overlay_keys.windows(2).all(|keys| keys[0] < keys[1]));
    }

    #[test]
    fn non_fail_fast_setup_verifies_only_successful_cases() {
        let mut progress = ProfileProgress::default();
        progress.record_setup_success(0);
        assert!(!progress.record_setup_failure("case_b (setup): failed".to_string(), false));
        progress.record_setup_success(2);

        assert!(progress.should_transition_to_current());
        assert_eq!(progress.successful_setup_indexes, [0, 2]);
        assert!(!progress.should_stop_remaining_profiles());
    }

    #[test]
    fn fail_fast_setup_blocks_current_and_later_profiles() {
        let mut progress = ProfileProgress::default();
        progress.record_setup_success(0);

        assert!(progress.record_setup_failure("case_b (setup): failed".to_string(), true));
        assert!(!progress.should_transition_to_current());
        assert!(progress.should_stop_remaining_profiles());
    }

    #[test]
    fn zero_successful_setups_skips_current() {
        let mut progress = ProfileProgress::default();
        progress.record_setup_failure("case_a (setup): failed".to_string(), false);

        assert!(!progress.should_transition_to_current());
        assert!(!progress.should_stop_remaining_profiles());
    }

    #[test]
    fn verify_fail_fast_stops_later_profiles_after_cleanup() {
        let mut progress = ProfileProgress::default();
        progress.record_setup_success(0);

        assert!(progress.record_verify_failure("case_a (verify): failed".to_string(), true));
        assert!(progress.should_stop_remaining_profiles());
    }

    #[test]
    fn non_fail_fast_aggregates_failures_but_cleanup_failure_stops_profiles() {
        let mut progress = ProfileProgress::default();
        progress.record_setup_failure("case_a (setup): failed".to_string(), false);
        progress.record_setup_success(1);
        progress.record_verify_failure("case_b (verify): failed".to_string(), false);

        assert!(!progress.should_stop_remaining_profiles());
        progress.record_cleanup_failure("failed to remove profile state".to_string());
        assert_eq!(progress.failures.len(), 3);
        assert!(progress.should_stop_remaining_profiles());
    }

    #[test]
    fn protected_path_warning_is_sorted_and_value_free() {
        let warning = format_protected_path_warning(
            "abcdef123456",
            vec!["case_z", "case_a"],
            vec!["wal.provider".to_string(), "mode".to_string()],
        );

        assert_eq!(
            warning,
            "Warning: datanode overlay profile abcdef123456 touches runner-owned paths [mode, wal.provider] for cases [case_a, case_z]"
        );
        assert!(!warning.contains("secret-overlay-value"));
    }

    #[test]
    fn profile_state_guard_preserves_state_on_forced_unwind() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        let unwind = std::panic::catch_unwind(|| {
            let _state = ProfileStateGuard::new(temp_dir, true);
            panic!("forced profile unwind");
        });

        assert!(unwind.is_err());
        assert!(path.is_dir());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn profile_state_guard_removes_state_on_drop_without_preservation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        drop(ProfileStateGuard::new(temp_dir, false));

        assert!(!path.exists());
    }

    #[test]
    fn profile_state_guard_normal_finalization_transfers_ownership_once() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        let cleanup = ProfileStateGuard::new(temp_dir, true).finalize();

        assert!(cleanup.failures.is_empty());
        assert_eq!(cleanup.preserved_state.as_deref(), Some(path.as_path()));
        assert!(path.is_dir());
        std::fs::remove_dir_all(path).unwrap();
    }
}
