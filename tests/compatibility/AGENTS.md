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

## Phase Semantics

- `setup.sql` runs on the **old (from)** binary. Only success is required;
  output is not compared to any file.
- `verify.sql` runs on the **new (to)** binary. Output is compared against
  `verify.result`.

## PostgreSQL Protocol Cases

- When using `-- SQLNESS PROTOCOL POSTGRES`, avoid unqualified table names
  starting with `pg_`. GreptimeDB issue #8359 causes the parser to rewrite them
  to `pg_catalog.<table>`. Qualify such names explicitly or rename the table.

## Previewing with `--dry-run`

```shell
cargo run -p sqlness-runner -- compat --dry-run [--from-version vX.Y.Z] [--test-filter "..."]
```

The dry-run performs full discovery and filtering (name, topology, metadata
validation, namespace dedup, version-range matching) but starts no services,
creates no temp dirs, and mutates no files. Use it to check which cases
would be selected before a real run.

## CI Version Window

- `tests/compatibility/ci.toml` controls the small sliding window of recent
  released versions used by the CI smoke job. Do not hard-code old versions
  directly in workflow YAML.
- Keep the PR/merge-queue window short; wider compatibility coverage belongs in
  nightly or release-validation workflows.
- Case `from_range` / `to_range` metadata still controls whether each case runs
  for a sampled version pair.
