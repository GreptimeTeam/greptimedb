# GreptimeDB Compatibility Test Framework

Compatibility tests verify that one GreptimeDB version can restart on state written by another version.

Tests are run via `cargo sqlness compat` and reuse the sqlness-runner infrastructure.

## Quick Start

```shell
# Self-compat smoke test (current binary only):
cargo run -p sqlness-runner -- compat

# Test from a specific released version to current:
cargo run -p sqlness-runner -- compat --from-version v0.9.5

# Test between two local binary directories:
cargo run -p sqlness-runner -- compat --from-bins-dir ./bins/old --to-bins-dir ./bins/new

# Test a downgrade from the current build to a released binary:
cargo run -p sqlness-runner -- compat --from-bins-dir ./bins/current --to-version v1.1.4

# Run a compatibility case in standalone mode:
cargo run -p sqlness-runner -- compat --topology standalone --test-filter "downgrade_compatibility"

# Run a specific case:
cargo run -p sqlness-runner -- compat --test-filter "basic_table"

# Preview which cases would run (no services started):
cargo run -p sqlness-runner -- compat --dry-run --from-version v0.9.5

# See all options:
cargo run -p sqlness-runner -- compat --help
```

## Prerequisites

- **Docker** (for etcd): PR1 always uses Docker etcd for distributed metadata. External metadata stores are future work.
- **From binary**: Either `--from-version <version>` to auto-pull a release, or `--from-bins-dir <path>` to use a local build. The binary `greptime` must exist directly inside the given directory.
- **To binary**: Defaults to the current debug build (`target/debug/greptime`). Override with `--to-bins-dir <path>` or fetch a release with `--to-version <version>`.
- **Custom target-dir**: If you use a non-default `CARGO_TARGET_DIR`, the debug binary won't be at `target/debug/greptime`. Pass `--from-bins-dir` / `--to-bins-dir` explicitly pointing to your custom target directory. Alternatively, run `cargo build -p greptime` without a custom target-dir.

## Case Format

Each compat case is a directory under `tests/compatibility/cases/` containing three required files plus an expected output file:

```
my_case/
  case.toml       # Metadata (required)
  setup.sql       # SQL to run on the from version (required)
  verify.sql      # SQL to run on the to version (required)
  verify.result   # Expected output from verify.sql
```

### `case.toml` — Required Metadata

```toml
name = "my_case"
reason = "Why this compatibility case exists"
introduced_by = "PR #1234 or feature name"
topologies = ["distributed", "standalone"]
from_range = ["*"]
to_range = ["*"]
features = ["table"]
owner = "team-name"
# optional:
namespace = "my_explicit_namespace"   # defaults to sanitized directory name
```

**Required fields**: `name`, `reason`, `introduced_by`, `topologies`, `from_range`, `to_range`, `features`, `owner`.

### Old-Stage Datanode Configuration Overlay

To apply a datanode configuration overlay while running the old stage, add this
strict optional table to `case.toml`:

```toml
[old_config]
datanode = "old-datanode.overlay.toml"
```

`datanode` is required whenever `[old_config]` is present; empty tables and
unknown keys are rejected. The reference is relative to the case directory and
must remain confined to that directory. The sidecar is native datanode TOML,
which the runner loads and preflights before starting services or creating
state.

The runner first applies the datanode baseline, then merges the sidecar. Tables
merge recursively only when both values are tables. Scalars, type mismatches,
arrays, and arrays of tables replace the baseline value atomically. In
particular, `region_engine` has no special merge behavior.

Runner-owned fields cannot be changed by an overlay: `mode`, `node_id`,
`storage.data_home`, `meta_client_options.metasrv_addrs`, and `wal.provider`,
plus `wal.dir` for Raft WAL or `wal.broker_endpoints` for Kafka WAL. The runner
restores these fields to its baseline values, or deletes them when the baseline
has no value. It warns about protected-field overrides without printing their
values.

### Old-Stage Persisted Procedure Snapshot

To verify recovery of a procedure state written by the old binary, add this
strict optional table to `case.toml`:

```toml
[old_procedure]
type_name = "metasrv-procedure::ReconcileTable"
```

The distributed runner starts watching the procedure store before `setup.sql`,
captures the first persisted root-procedure step with the exact type name, and
clones that old-binary message under a fresh procedure ID. The current binary
must then recover the cloned snapshot after restart. This avoids
timing-dependent process kills while preserving the old binary's actual
serialized procedure data.

This option requires the distributed topology and the runner-owned Docker etcd.
The setup SQL must submit a root procedure of the configured type and allow it to
reach a persisted step within 30 seconds.

### Version-Range Filtering

`from_range` and `to_range` control which binary versions a case applies to:

| Entry | Meaning |
|-------|---------|
| `"*"` | Matches any version (including unknown). |
| `"vX.Y.Z"` or `"=vX.Y.Z"` | Matches exactly version X.Y.Z. |
| `">=vX.Y.Z"` | Matches X.Y.Z or later. |
| `">vX.Y.Z"` | Matches versions strictly later than X.Y.Z. |
| `"<=vX.Y.Z"` | Matches X.Y.Z or earlier. |
| `"<vX.Y.Z"` | Matches versions strictly earlier than X.Y.Z. |

The range list is **OR**: a case matches if **any** entry matches.

**Best-effort enforcement**: The runner tries to determine the effective version:
- `--from-version` is used directly.
- `--from-bins-dir` / `--to-bins-dir` (or the default debug build) runs `<binary> --version` to infer the version.
- When the version **cannot** be determined (e.g. binary missing or `--version` fails), non-wildcard ranges are **skipped** with a message; wildcard (`*`) ranges still match.

**Example** (`legacy_jsonb`):
```toml
from_range = ["<=v1.1.0"]
to_range = [">=v1.1.1"]
```
This case only runs when the old binary is <= v1.1.0 and the new binary is >= v1.1.1.

### CI Version Window

The CI job uses `tests/compatibility/ci.toml` to choose the small sliding
window of recent released `from` versions to test against the PR-built `to`
binary:

```toml
from_versions = ["v1.0.0", "v1.1.0"]
```

Keep this window small for PR and merge-queue latency: the goal is to catch
upgrade compatibility issues from recent releases to the latest build, not to
retest every historical version on every PR. Case-level `from_range`/`to_range`
still decides which cases run for each version pair; the CI window only decides
which old binaries are sampled. Broader historical windows belong in nightly or
release-validation workflows.

The GitHub Actions workflow delegates the window loading and compat invocation
to `.github/scripts/run-compat.py`; the workflow YAML should stay as a thin
wrapper around artifact download/extraction and this script.

`downgrade_to_versions` optionally lists releases that CI restarts after the
PR-built cluster. Those runs select only the `downgrade_compatibility` case in
both distributed and standalone topologies.

### `setup.sql` — Setup Phase (From Version)

SQL statements executed on the **from** version cluster. These must succeed (any error fails the case). Setup output is NOT compared against any result file.

Rules:
- Statements are semicolon-terminated
- `--` prefix for ordinary comments
- `-- SQLNESS ...` interceptor comments follow ordinary sqlness semantics
### `verify.sql` — Verify Phase (To Version)

SQL statements executed on the **to** version cluster. Output is compared against `verify.result` in sqlness snapshot style.

### `verify.result` — Expected Output

Expected output in sqlness format. If this file is missing, the runner generates it from actual output and **fails** — the author must review, commit the generated file, and rerun.

```
<statement>;

<output>

<next statement>;

<output>

```

If output differs from expected, the run fails and `verify.result` is updated with actual output.

## PR1 Limitations

- **Sqlness interceptors**: `-- SQLNESS ...` comments are applied per statement using the same interceptor registry as the ordinary sqlness runner, including the GreptimeDB `PROTOCOL` interceptor. For `PROTOCOL POSTGRES`, the namespace prelude uses `SET search_path` instead of `USE`. Avoid unqualified PostgreSQL-protocol table names starting with `pg_`: GreptimeDB's current PostgreSQL compatibility parser rewrites them to `pg_catalog.<table>`.
- **Distributed topology**: The compat runner starts 1 metasrv + 3 datanodes + 1 frontend + 1 flownode. Standalone compatibility runs need no external metadata store.
- **No comment-based compat config**: The compat runner does not define extra compatibility configuration in SQL comments; sqlness comments keep their normal sqlness meaning.

## Namespace Isolation

Each case runs in its own database namespace to prevent cross-case interference:

- Default namespace is derived from the case directory name (sanitized to `[a-z][a-z0-9_]*`)
- Override with `namespace` in `case.toml`
- Duplicate namespaces are **rejected** at discovery time (before version filtering)
- Before each statement, the runner executes a namespace prelude (not written to verify.result): `CREATE DATABASE IF NOT EXISTS <ns>` via gRPC; then `USE <ns>` for gRPC/MySQL statements or `SET search_path TO '<ns>'` for PostgreSQL statements.

## Batch Behavior

- The baseline (no-overlay) profile runs first. Cases whose old datanode TOML
  is semantically equivalent share one profile; profiles run serially and in
  isolation.
- Each profile has its own state and etcd lifecycle. Its overlay is applied
  only to old-stage datanodes and remains in effect through old-stage setup
  restarts. The current stage always uses a clean configuration.
- Cases run **serially** (no parallelism in PR1). Namespace state is
  session/protocol state and cannot be shared concurrently.
- Same namespace across cases is rejected.
- Without fail-fast, the runner verifies only cases whose setup succeeded.
  With fail-fast, it cleans up the active profile before stopping.
- `--dry-run` displays the selected profiles, cases, and sidecar paths without
  printing configuration values; it starts no services.

## xfail Policy (Future)

For PR1, all cases are expected to pass. Future PRs will add `xfail` support with required `issue` and `expiry` fields.

## Cross-Job Distributed State

PR1 runs setup and verify in the **same job** (same process). Cross-job artifact restore for distributed state is not supported in PR1 due to port randomization and etcd lease expiration.
