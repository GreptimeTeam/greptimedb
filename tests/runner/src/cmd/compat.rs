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

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use sqlness::QueryContext;
use sqlness::interceptor::Registry;

use crate::cmd::bare::ServerAddr;
use crate::cmd::compat_case::{self, CompatCase};
use crate::env::bare::{Env, StoreConfig, WalConfig};
use crate::{protocol_interceptor, util};

const COMPAT_TOPOLOGY: &str = "distributed";

/// Run compatibility tests in bare distributed mode.
///
/// Starts an old-version distributed cluster, runs setup SQLs,
/// then restarts the cluster with a new version on preserved state,
/// and runs verify SQLs comparing results against `verify.result` files.
///
/// PR1 notes:
/// - Only default gRPC protocol is supported; SQLNESS interceptors are rejected.
/// - `-- SQLNESS PROTOCOL mysql/postgres` is not supported.
/// - Ordinary `--` comments are allowed but not preserved in verify.result.
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
}

impl CompatCommand {
    pub async fn run(self) {
        // ---- 1. Create temp directory ----
        let temp_dir = tempfile::Builder::new()
            .prefix("sqlness-compat")
            .tempdir()
            .unwrap();
        let sqlness_home = temp_dir.keep();

        // ---- 2. Validate MVP runtime constraints ----
        if !self.setup_etcd {
            panic!(
                "compat MVP requires Docker etcd (--setup-etcd=true); external metadata stores are not supported yet"
            );
        }

        // ---- 3. Resolve case directory ----
        let case_dir = self.case_dir.unwrap_or_else(default_compat_case_dir);

        if !case_dir.is_dir() {
            panic!("Case directory not found: {}", case_dir.display());
        }

        // ---- 4. Discover and validate cases ----
        let mut cases = compat_case::discover_cases(&case_dir).unwrap_or_else(|e| panic!("{e}"));

        compat_case::validate_cases(&cases).unwrap_or_else(|e| panic!("{e}"));

        // Filter by test_filter
        let filter_re = regex::Regex::new(&self.test_filter)
            .unwrap_or_else(|e| panic!("Invalid test filter regex '{}': {e}", self.test_filter));
        cases.retain(|c| filter_re.is_match(&c.metadata.name));
        cases.retain(|c| c.metadata.topologies.iter().any(|t| t == COMPAT_TOPOLOGY));

        if cases.is_empty() {
            println!(
                "No compat cases found matching filter '{}' and topology '{}'",
                self.test_filter, COMPAT_TOPOLOGY
            );
            return;
        }

        println!(
            "Running {} compat case(s) with topology {}:",
            cases.len(),
            COMPAT_TOPOLOGY
        );
        for c in &cases {
            println!(
                "  - {} (namespace: {}, topologies: {:?})",
                c.metadata.name, c.namespace, c.metadata.topologies
            );
        }

        // ---- 5. Resolve "from" binary path ----
        let from_bins_dir = resolve_bins(
            self.from_bins_dir.as_ref(),
            self.from_version.as_deref(),
            self.pull_version_on_need,
        )
        .await;

        // ---- 6. Build interceptor registry ----
        let interceptor_registry = create_interceptor_registry();

        // ---- 6b. Create Env for bare distributed mode ----
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
        };

        let env = Env::new(
            sqlness_home.clone(),
            ServerAddr::default(),
            WalConfig::RaftEngine,
            self.pull_version_on_need,
            Some(from_bins_dir),
            store_config,
            vec![],
        );

        // ---- 6c. Etcd cleanup guard ----
        // Arm this only immediately before starting the cluster. Earlier validation
        // failures should not stop an unrelated local container named `etcd`.
        let mut etcd_guard = if self.setup_etcd {
            Some(EtcdGuard::new())
        } else {
            None
        };

        // ---- 7. Run setup phase on old cluster ----
        println!("Starting old-version distributed cluster with flownode...");
        let mut db = env.compat_start_distributed(0).await;

        println!("Running setup phase...");
        for case in &cases {
            run_compat_phase(&db, case, &interceptor_registry, CompatPhase::Setup)
                .await
                .unwrap_or_else(|e| panic!("Setup failed for case '{}': {e}", case.metadata.name));
            println!("  Setup: {} - OK", case.metadata.name);
        }

        // ---- 8. Switch to "to" binary and restart cluster ----
        let to_bins_dir =
            resolve_bins(self.to_bins_dir.as_ref(), None, self.pull_version_on_need).await;

        println!("Restarting cluster with new-version binary on preserved state...");
        env.compat_restart_all(&db, to_bins_dir).await;

        // ---- 9. Run verify phase on new cluster ----
        println!("Running verify phase...");
        let mut failed = Vec::new();
        for case in &cases {
            match run_compat_phase(&db, case, &interceptor_registry, CompatPhase::Verify).await {
                Ok(()) => println!("  Verify: {} - PASSED", case.metadata.name),
                Err(e) => {
                    println!("  Verify: {} - FAILED: {e}", case.metadata.name);
                    failed.push(case.metadata.name.clone());
                    if self.fail_fast {
                        break;
                    }
                }
            }
        }

        // ---- 10. Stop cluster ----
        db.compat_stop();

        // ---- 11. Cleanup ----
        // Etcd is always cleaned up; --preserve-state only preserves sqlness_home.
        if self.setup_etcd {
            println!("Stopping etcd");
            util::stop_rm_etcd();
        }

        if !self.preserve_state {
            println!("Removing state in {:?}", sqlness_home);
            tokio::fs::remove_dir_all(sqlness_home)
                .await
                .unwrap_or_else(|e| println!("Warning: failed to clean up temp dir: {e}"));
        }

        // Disarm the etcd guard now that we've done normal cleanup.
        if let Some(mut guard) = etcd_guard.take() {
            guard.disarm();
        }

        if failed.is_empty() {
            println!("\n\x1b[32mAll compat tests passed!\x1b[0m");
        } else {
            println!("\n\x1b[31mFailed cases: {}\x1b[0m", failed.join(", "));
            // Explicitly drop the guard before exit so it doesn't double-cleanup.
            std::process::exit(1);
        }
    }
}

/// Guard that stops/removes Docker etcd on drop (panic or early exit).
/// Disarm before normal cleanup to avoid double-cleanup.
struct EtcdGuard {
    active: bool,
}

impl EtcdGuard {
    fn new() -> Self {
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

/// Create an interceptor registry with the protocol interceptor registered.
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

    // Parse SQL into statements with interceptor contexts
    let statements = parse_sql_file(&sql_content, registry)?;

    // Run namespace prelude (only for setup and verify; not written into verify result)
    run_namespace_prelude(db, &case.namespace).await?;

    // Execute statements
    let mut verify_output = String::new();

    for parsed in &statements {
        let result = db.compat_query(&parsed.sql, &parsed.query_ctx).await?;

        match phase {
            CompatPhase::Setup => {
                // Setup: just check for success (already returned Ok)
            }
            CompatPhase::Verify => {
                // Verify: accumulate output in sqlness-style format
                // Format: display query, blank line, result, blank line
                verify_output.push_str(&parsed.sql);
                verify_output.push('\n');
                verify_output.push('\n');
                verify_output.push_str(&result);
                verify_output.push('\n');
                verify_output.push('\n');
            }
        }
    }

    if phase == CompatPhase::Verify {
        let result_path = case.dir.join("verify.result");

        // If verify.result doesn't exist, create it (like sqlness does on first run)
        if !result_path.is_file() {
            std::fs::write(&result_path, &verify_output)
                .map_err(|e| format!("Failed to create {}: {e}", result_path.display()))?;
            println!(
                "  Created initial verify.result for case '{}'",
                case.metadata.name
            );
        } else {
            let expected = std::fs::read_to_string(&result_path)
                .map_err(|e| format!("Failed to read {}: {e}", result_path.display()))?;

            if verify_output != expected {
                // Update the result file with actual output (sqlness behavior)
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
    }

    Ok(())
}

/// Execute the namespace prelude (CREATE DATABASE IF NOT EXISTS + USE) for a case.
/// This is NOT written into verify.result.
async fn run_namespace_prelude(
    db: &crate::env::bare::GreptimeDB,
    namespace: &str,
) -> Result<(), String> {
    let create_db = format!("CREATE DATABASE IF NOT EXISTS {namespace}");
    let use_db = format!("USE {namespace}");

    // Use default context (gRPC protocol) for namespace management
    let default_ctx = QueryContext::default();

    db.compat_query(&create_db, &default_ctx).await?;
    db.compat_query(&use_db, &default_ctx).await?;

    Ok(())
}

/// A parsed SQL statement with its interceptor-derived query context.
struct ParsedStatement {
    sql: String,
    query_ctx: QueryContext,
}

/// Parse a SQL file into a list of (statement, QueryContext) pairs.
///
/// PR1 behavior:
/// - Lines starting with `--` are ordinary comments (not preserved in verify.result)
/// - `-- SQLNESS <INTERCEPTOR>` lines are REJECTED — PR1 does not support interceptors
/// - Statements are terminated by `;`
/// - Multi-line statements are supported
fn parse_sql_file(content: &str, registry: &Registry) -> Result<Vec<ParsedStatement>, String> {
    let mut statements = Vec::new();
    let mut current_ctx = QueryContext::default();
    let mut current_stmt = String::new();
    let mut in_stmt = false;

    for line in content.lines() {
        let trimmed = line.trim();

        // Check for interceptor directive
        if trimmed.starts_with("-- SQLNESS ") {
            if in_stmt {
                // Flush current statement before applying new interceptor
                let stmt = current_stmt.trim().to_string();
                if !stmt.is_empty() {
                    statements.push(ParsedStatement {
                        sql: stmt,
                        query_ctx: current_ctx.clone(),
                    });
                }
                current_stmt.clear();
                in_stmt = false;
            }
            // Parse interceptor: strip "-- SQLNESS " prefix
            let directive = trimmed.strip_prefix("-- SQLNESS ").unwrap();
            apply_interceptor_directive(directive, registry, &mut current_ctx)?;
            continue;
        }

        // Skip pure comment lines
        if trimmed.starts_with("--") || trimmed.is_empty() {
            if in_stmt {
                // Blank line or comment inside a statement is fine, just skip
            }
            continue;
        }

        // Accumulate statement content
        if in_stmt {
            current_stmt.push('\n');
        }
        current_stmt.push_str(line); // preserve original formatting
        in_stmt = true;

        // Check for statement terminator
        if trimmed.ends_with(';') {
            let stmt = current_stmt.trim().to_string();
            if !stmt.is_empty() {
                statements.push(ParsedStatement {
                    sql: stmt,
                    query_ctx: current_ctx.clone(),
                });
            }
            current_stmt.clear();
            in_stmt = false;
        }
    }

    // Flush any remaining statement
    if in_stmt {
        let stmt = current_stmt.trim().to_string();
        if !stmt.is_empty() {
            statements.push(ParsedStatement {
                sql: stmt,
                query_ctx: current_ctx,
            });
        }
    }

    if statements.is_empty() {
        return Err("No SQL statements found in file".to_string());
    }

    Ok(statements)
}

/// Apply an interceptor directive (e.g., "PROTOCOL mysql") to a QueryContext.
///
/// PR1 rejects ALL SQLNESS interceptors. Only default gRPC protocol is supported.
fn apply_interceptor_directive(
    directive: &str,
    _registry: &Registry,
    _ctx: &mut QueryContext,
) -> Result<(), String> {
    // Reject PROTOCOL interceptor: PR1 only supports default gRPC
    if let Some(protocol) = directive.strip_prefix("PROTOCOL ") {
        let protocol = protocol.trim().to_lowercase();
        return Err(format!(
            "PR1 does not support '-- SQLNESS PROTOCOL {protocol}'. \
             Compat tests use default gRPC protocol only. \
             SQLNESS interceptors are not supported in PR1."
        ));
    }

    // Any other SQLNESS directive is an error in PR1
    Err(format!(
        "Unknown SQLNESS directive: '-- SQLNESS {directive}'. \
         PR1 does not support SQLNESS interceptors. \
         Ordinary '--' comments are allowed."
    ))
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
