# GreptimeDB Compatibility Test Framework

Compatibility tests verify that a newer version of GreptimeDB can read data written by an older version.

Tests are run via `cargo sqlness compat` and reuse the sqlness-runner infrastructure.

## Quick Start

```shell
# Self-compat smoke test (current binary only):
cargo run -p sqlness-runner -- compat

# Test from a specific released version to current:
cargo run -p sqlness-runner -- compat --from-version v0.9.5

# Test between two local binary directories:
cargo run -p sqlness-runner -- compat --from-bins-dir ./bins/old --to-bins-dir ./bins/new

# Run a specific case:
cargo run -p sqlness-runner -- compat --test-filter "basic_table"

# See all options:
cargo run -p sqlness-runner -- compat --help
```

## Prerequisites

- **Docker** (for etcd): PR1 always uses Docker etcd for distributed metadata. External metadata stores are future work.
- **From binary**: Either `--from-version <version>` to auto-pull a release, or `--from-bins-dir <path>` to use a local build. The binary `greptime` must exist directly inside the given directory.
- **To binary**: Defaults to the current debug build (`target/debug/greptime`). Override with `--to-bins-dir <path>`.
- **Custom target-dir**: If you use a non-default `CARGO_TARGET_DIR`, the debug binary won't be at `target/debug/greptime`. Pass `--from-bins-dir` / `--to-bins-dir` explicitly pointing to your custom target directory. Alternatively, run `cargo build -p greptime` without a custom target-dir.

## Case Format

Each compat case is a directory under `tests/compatibility/cases/` containing three required files plus an optional generated result file:

```
my_case/
  case.toml       # Metadata (required)
  setup.sql       # SQL to run on old version (required)
  verify.sql      # SQL to run on new version (required)
  verify.result   # Expected output from verify.sql (auto-generated on first run)
```

### `case.toml` — Required Metadata

```toml
name = "my_case"
reason = "Why this compatibility case exists"
introduced_by = "PR #1234 or feature name"
topologies = ["distributed"]  # full distributed topology, including flownode
from_range = ["*"]
to_range = ["*"]
features = ["table"]
owner = "team-name"
# optional:
namespace = "my_explicit_namespace"   # defaults to sanitized directory name
isolation = "shared"                  # only "shared" to allow duplicate namespace
```

**Required fields**: `name`, `reason`, `introduced_by`, `topologies`, `from_range`, `to_range`, `features`, `owner`.

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

### `setup.sql` — Setup Phase (Old Version)

SQL statements executed on the **old** version cluster. These must succeed (any error fails the case). Setup output is NOT compared against any result file.

Rules:
- Statements are semicolon-terminated
- `--` prefix for ordinary comments
- `-- SQLNESS ...` interceptor comments follow ordinary sqlness semantics

### `verify.sql` — Verify Phase (New Version)

SQL statements executed on the **new** version cluster. Output is compared against `verify.result` in sqlness snapshot style. If `verify.result` does not exist, it is created automatically on first run.

### `verify.result` — Expected Output

Auto-generated on first run in sqlness format:
```
<statement>;

<output>

<next statement>;

<output>

```

If output differs from expected, the run fails and `verify.result` is updated with actual output.

## PR1 Limitations

- **Sqlness interceptors**: `-- SQLNESS ...` comments are applied per statement using the same interceptor registry as the ordinary sqlness runner, including the GreptimeDB `PROTOCOL` interceptor.
- **Full distributed topology**: The compat runner starts 1 metasrv + 3 datanodes + 1 frontend + 1 flownode.
- **No comment-based compat config**: The compat runner does not define extra compatibility configuration in SQL comments; sqlness comments keep their normal sqlness meaning.

## Namespace Isolation

Each case runs in its own database namespace to prevent cross-case interference:

- Default namespace is derived from the case directory name (sanitized to `[a-z][a-z0-9_]*`)
- Override with `namespace` in `case.toml`
- Duplicate namespaces are **rejected** unless `isolation = "shared"` is set
- The runner executes `CREATE DATABASE IF NOT EXISTS <ns>; USE <ns>;` before each statement (not written to verify.result)

## Batch Behavior

- All cases in a run share one cluster lifecycle: start old cluster → run all setups → restart with new binary → run all verifies
- Cases run **serially** (no parallelism in PR1). `USE <db>` is session state and cannot be shared concurrently.
- Same namespace across cases is rejected by default. Use `isolation = "shared"` for cases that intentionally share state.

## xfail Policy (Future)

For PR1, all cases are expected to pass. Future PRs will add `xfail` support with required `issue` and `expiry` fields.

## Cross-Job Distributed State

PR1 runs setup and verify in the **same job** (same process). Cross-job artifact restore for distributed state is not supported in PR1 due to port randomization and etcd lease expiration.
