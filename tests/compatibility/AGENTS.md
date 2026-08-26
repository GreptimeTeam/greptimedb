# Agent Guidance for Compatibility Framework

This file is intended for AI agents editing compat cases or the compat runner. Follow these rules to avoid common pitfalls.

## `verify.result` is Expected Output (Not Auto-Generated Silently)

- `verify.result` contains the **expected** output of `verify.sql`.
- If the file is **missing**, the runner generates it from actual output and **fails**.
  The agent must review the generated file (`git diff`), hand-verify correctness,
  and commit it before rerunning. Do **not** blindly commit generated output.
- If actual output **differs** from the expected file, the runner **updates**
  `verify.result` with actual output and **fails**. The agent must inspect the
  diff and decide whether the change is intentional (accept) or a bug (fix code).

## Namespace Isolation

- Each case owns a **unique** namespace. No shared namespace support exists.
- Duplicate namespaces are a **hard error** detected before version filtering.
- The removed `isolation` field is no longer recognized; `case.toml` uses
  `deny_unknown_fields`, so any stale `isolation = "shared"` entry causes a
  parse error.

## `case.toml` is Strict

- `deny_unknown_fields` is enabled. Unknown keys cause a hard parse error.
- Version constraint entries in `from_range` / `to_range` are validated early;
  invalid constraints (e.g. `>=not-a-version`) are hard errors, not silent skips.
- All required fields must be non-empty: `name`, `reason`, `introduced_by`,
  `topologies`, `from_range`, `to_range`, `features`, `owner`.

## Old-Stage Datanode Overlays

- An old-stage datanode overlay is declared only as:

  ```toml
  [old_config]
  datanode = "old-datanode.overlay.toml"
  ```

  `datanode` is required if `[old_config]` exists. Empty tables and unknown
  keys are parse errors.
- The sidecar path is relative to its case directory and must remain confined
  there. It contains native datanode TOML and is loaded and preflighted before
  services or state are created.
- The runner merges tables recursively only when both values are tables.
  Scalars, type mismatches, arrays, and arrays of tables replace atomically;
  `region_engine` has no special merge behavior.
- Do not use an overlay to set runner-owned fields: `mode`, `node_id`,
  `storage.data_home`, `meta_client_options.metasrv_addrs`, `wal.provider`, or
  `wal.dir` for Raft WAL / `wal.broker_endpoints` for Kafka WAL. The runner
  restores or deletes them according to its baseline and warns without showing
  values.

## Old-Stage Persisted Procedure Snapshots

- Declare a recovery snapshot only as:

  ```toml
  [old_procedure]
  type_name = "metasrv-procedure::ReconcileTable"
  ```

- `type_name` must be the exact persisted procedure type and cannot be empty.
- This option requires distributed topology with the runner-owned Docker etcd.
- `setup.sql` must submit the target root procedure. The runner captures and clones
  the old binary's real persisted step, so do not add timing-based queues or
  process-kill assumptions to the SQL case.

## Phase Semantics

- `setup.sql` runs on the **old (from)** binary. Only success is required;
  output is not compared to any file.
- `verify.sql` runs on the **new (to)** binary. Output is compared against
  `verify.result`.
- Overlays apply only to old-stage datanodes and survive old-stage setup
  restarts; the current stage uses a clean configuration.
- The baseline profile runs first. Cases with semantically equivalent datanode
  TOML share one sequential, isolated profile, and every profile has an
  independent state and etcd lifecycle.
- Without fail-fast, only cases with successful setup are verified. Fail-fast
  cleans up the active profile before stopping.

## PostgreSQL Protocol Cases

- When using `-- SQLNESS PROTOCOL POSTGRES`, avoid unqualified table names
  starting with `pg_`. GreptimeDB issue #8359 causes the parser to rewrite them
  to `pg_catalog.<table>`. Qualify such names explicitly or rename the table.

## Previewing with `--dry-run`

```shell
cargo run -p sqlness-runner -- compat --dry-run [--from-version vX.Y.Z] [--test-filter "..."]
```

The dry-run performs full discovery and filtering (name, topology, metadata
validation, namespace dedup, version-range matching) and displays selected
profiles, cases, and sidecar paths without configuration values. It starts no
services, creates no temp dirs, and mutates no files. Use it to check which
cases would be selected before a real run.

## CI Version Window

- `tests/compatibility/ci.toml` controls the small PR window: the latest patch
  of the two most recent stable minor lines whose tags have published release
  assets. Do not hard-code old versions directly in workflow YAML.
- Exact `=vX.Y.Z` `from_range` anchors in case.toml are not retained in the PR
  window; `--check-anchors` validates that they are released tags and
  `--nightly-window` exercises them on nightly schedule runs.
- Keep the PR/merge-queue window short; wider compatibility coverage belongs in
  nightly or release-validation workflows.
- Case `from_range` / `to_range` metadata still controls whether each case runs
  for a sampled version pair.
- `.github/scripts/run-compat.py` owns the CI-side window parsing and compat
  invocation. Keep workflow YAML thin; update the script instead of embedding
  parsing or loops in `.github/workflows/integration.yml`.
