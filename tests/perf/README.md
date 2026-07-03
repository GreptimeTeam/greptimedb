# Query performance regression harness

This directory is for query performance cases that compare a base build with a
candidate build. It is not a replacement for sqlness: the goal is to measure the
effect of optimizer/query-engine changes on realistic scan work.

## Phase 1: direct readable SST fixtures

Phase 1 should generate data by writing readable Mito SST files and matching
manifest checkpoints directly. This follows the `gc_readable_sst_fixture` lab
approach from `~/greptimedb-gc-huge-stress`: use Mito's SST writer to create
queryable files, then write a checkpoint and `_last_checkpoint` that reference
those files.

The generator itself must be generic. It should not know about a specific issue
such as #7913 or a specific PromQL query. Cases provide declarative table schema,
data layout, distributions, and queries; the generator turns those declarations
into readable SST fixtures.

The intended flow for each case is:

1. Start a GreptimeDB build and create an empty table to seed catalog/table
   metadata.
2. Stop the process.
3. Use the seed region metadata/manifest to generate deterministic readable SSTs
   and a replacement manifest checkpoint offline.
4. Start the same build on the generated data directory.
5. Run warmup and measured queries.
6. Repeat the same fixture/query process for the candidate build.
7. Compare base vs candidate metrics and write a regression report.

Direct SST fixtures are the default for phase 1 because they provide stable file
counts, time ranges, row groups, and label distributions without spending CI time
on ingestion and flush. Ingestion-path cases can be added later for nightly or
release-level realism.

## Generator contract

The direct-SST generator should accept a case definition with:

- one or more table definitions: columns, semantic types, primary key, time
  index, SST format, append mode
- deterministic distributions: seed, series/tag cardinalities, label/value
  functions, timestamp layout
- physical layout: regions, SST count, rows per SST, row group size, time ranges
  per SST, optional overlap/skew
- output paths for object-store files, manifest checkpoints, and fixture metadata

This keeps query regression cases reusable: the same generator can produce
PromQL, SQL, pruning, projection, join, or aggregation fixtures by changing only
case config.

## What a case owns

Each optimization PR should add or update the query case for the pattern it is
expected to affect. A case should define:

- schema and seed table SQL
- deterministic data shape: seed, series count, rows per SST, SST count, time
  range layout, label distribution, region/partition layout
- queries to run
- warmup/measurement repetitions
- metrics to collect
- base-vs-candidate thresholds

Cases may also declare the workload kind. Omitted `[workload]` sections default
to `direct_readable_sst` for compatibility with older cases:

```toml
[workload]
kind = "direct_readable_sst"

[workload.direct_readable_sst]
# currently no required fields
```

The runner currently supports only `direct_readable_sst`. The enum-style shape is
reserved for future `write_then_query` and `write_while_query` workloads.

## Metrics

Primary gates should compare query work rather than plan text:

- scanned files / file ranges
- scanned rows or row groups
- bytes read when available
- pruning ratio
- query latency median/p95
- output row count as a sanity check

Plan details such as pushed filters are useful diagnostics, but should not be the
main pass/fail signal.

## Runner MVP

`query_regression_runner.py` is the base-vs-candidate orchestration layer. The
current MVP parses a case, creates per-target work directories, and in real query
mode starts a local distributed cluster for each target: metasrv (memory-store,
region failover disabled), one datanode (`node_id=0`), and one frontend. It
creates the configured Mito table(s) through frontend HTTP SQL, discovers the
real one-region-per-table metadata via `information_schema`, stops only the
owning datanode, generates one shared direct-SST fixture per table using the
discovered `--region-id`, `--table-dir`, and `--table`, injects those region
subtrees into the datanode data home, restarts the datanode, then validates and
measures through frontend. Reports are written as JSON under the work directory.

The runner intentionally keeps metasrv alive for the whole target run because
memory-store metadata would otherwise be lost. It replaces only the discovered
datanode region directory under `data/greptime/<schema>/<table_id>/...` with
generated SST files and a manifest checkpoint. For multi-table cases this is
repeated per table, enabling true JOIN fixtures while still requiring exactly one
region per table. Base and candidate must discover identical per-table
`table_dir` and `region_id`; otherwise the run fails.

Multi-table direct-SST cases must use unique table names as well as unique
`(database, name)` pairs because the generator currently selects a table with
`--table <name>`. Per-table fixture directories are derived from table index,
database, and table name with path-unsafe characters sanitized.

Currently enforced threshold:

- `max_candidate_latency_regression_pct`, based on client-side median latency.

Server-side scan thresholds such as file ranges and scanned rows are reported as
`not_enforced` diagnostics until the runner extracts those metrics from query
responses or logs.

Dry-run example:

```bash
uv run --no-project python tests/perf/query_regression_runner.py \
  --case tests/perf/query_cases/promql_pushdown_7913/case.toml \
  --base-bin /path/to/base/greptime \
  --candidate-bin /path/to/candidate/greptime \
  --work-dir /tmp/query-perf-work \
  --dry-run
```

With a fixture generator:

```bash
uv run --no-project python tests/perf/query_regression_runner.py \
  --case tests/perf/query_cases/promql_pushdown_7913/case.toml \
  --base-bin /path/to/base/greptime \
  --candidate-bin /path/to/candidate/greptime \
  --fixture-generator /path/to/query_perf_fixture \
  --allow-large-fixture \
  --work-dir /tmp/query-perf-work
```

This mode launches metasrv, datanode, and frontend for each target with explicit
localhost HTTP/gRPC/MySQL/Postgres ports and writes component stdout/stderr under
each target's `logs/` directory.

By default query mode requires fresh base/candidate work directories and fails if
either target directory already exists with contents. Use `--reuse-work-dir` only
when intentionally debugging an existing run directory. SQL HTTP requests default
to a 120 second timeout; override with `--http-timeout <seconds>` for slow lab
runs.

Fixture generator smoke test:

```bash
cargo run -p cmd --bin query_perf_fixture -- \
  --case tests/perf/query_cases/smoke_direct_sst/case.toml \
  --out-dir /tmp/query-perf-smoke
```

Runner smoke test with fixture generation only:

```bash
uv run --no-project python tests/perf/query_regression_runner.py \
  --case tests/perf/query_cases/smoke_direct_sst/case.toml \
  --base-bin /path/to/query_perf_fixture \
  --candidate-bin /path/to/query_perf_fixture \
  --fixture-generator /path/to/query_perf_fixture \
  --work-dir /tmp/query-perf-runner-smoke \
  --fixture-only
```

`--fixture-only` preserves the earlier smoke behavior: it does not start
standalone servers, and it materializes the generated fixture into base and
candidate data directories for plumbing validation.

## GitHub Actions

`.github/workflows/query-regression.yml` provides an opt-in CI entrypoint for
query regression runs. It builds its own binaries for now:

- base `greptime` from the PR base commit, or `workflow_dispatch` `base_ref`
- candidate `greptime` and `query_perf_fixture` from the PR merge ref/current
  candidate checkout
- runner and summary formatter from the candidate checkout

The workflow runs automatically only for PRs labeled `query-regression` (on
label/synchronize/reopen events). PR runs default to the
`promql_pushdown_7913` case and pass `--allow-large-fixture`; manual
`workflow_dispatch` runs can override the case path and refs. It always uploads
`query-regression-work/**` and `query-regression-summary.md`, writes the Markdown
summary to the workflow step summary, and updates a sticky PR comment when the PR
comes from the same repository. Fork PRs still get artifacts and the step summary,
but comments are skipped.

## Built-in cases

The `promql_pushdown_7913` case is only one case using the generic fixture
format. It should generate a metric-like table with a nanosecond time index and
many SSTs with non-overlapping time ranges. A narrow PromQL/TQL query should
touch only a small time window. The candidate build is expected to scan
materially fewer files/ranges/rows than the base build when an optimizer PR
claims to improve this path.

Additional SQL optimizer cases:

- `sql_topk_order_by`: single-table TopK / `ORDER BY` on a DOUBLE field with
  time and tag predicates.
- `sql_aggregate_order_by`: grouped aggregate ordered by aggregate value with a
  `LIMIT`.
- `sql_join_filter_order`: two direct-SST tables joined on a shared tag with
  time filters, aggregate ordering, and `LIMIT`.
