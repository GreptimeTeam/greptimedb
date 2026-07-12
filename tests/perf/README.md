# Query performance regression harness

This directory is for query performance cases that compare a base build with a
candidate build. It is not a replacement for sqlness: the goal is to measure the
effect of optimizer/query-engine changes on realistic scan work.

The Prometheus remote-write cases are end-to-end regression coverage for the
automatic write, flush, SST, and query path. They do not replace controlled
encoding experiments that explicitly compare `plain`/dictionary,
`no_dictionary`, BYTE_STREAM_SPLIT, or Auto policies.

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

## Prometheus remote-write scenario

The runner also supports `scenario.kind = "prom_remote_write_then_query"` for a
bounded write-path smoke/regression flow. This path is explicit and separate from
the direct-SST fixture path: it starts the base and candidate distributed clusters,
writes deterministic Prometheus remote-write v1 samples through
`/v1/prometheus/write`, flushes the configured physical metric table, checks
visibility, runs the configured SQL/TQL queries, stops and awaits the datanode,
then optionally inspects the landed SST/Parquet footer, encoding, and size and
runs the read-bench against the quiescent data directory.

Remote-write cases configure one database, one logical metric, and one physical
table under `[scenario.remote_write]`:

```toml
[scenario]
kind = "prom_remote_write_then_query"

[scenario.remote_write]
database = "public"
metric = "prom_remote_write_seeded_random"
physical_table = "greptime_physical_table"
series_count = 512
samples_per_series = 1440
sample_chunk_size = 480
flush_every_sample_chunks = 1
start_unix_millis = 1_704_067_200_000
step_millis = 60_000
chunk_series_count = 128
timeout_seconds = 180
visibility_timeout_seconds = 120
# Optional: split by time so each helper invocation writes only this many samples
# per series, then periodically flush the physical metric table to produce
# multiple time-interval SSTs.

[scenario.remote_write.prom_store]
pending_rows_flush_interval = "1s"
max_batch_rows = 100000
```

Cases can optionally add `[scenario.remote_write.value]` to control the generated
sample values without changing label cardinality or the runner lifecycle:

```toml
[scenario.remote_write.value]
pattern = "quantized_signal" # linear, constant, modulo, unique, seeded_random,
                              # run_length, quantized_signal,
                              # signal_with_sporadic_stalls, mixed_signal_repeated
base = 0.0
step = 0.125
cardinality = 4096           # buckets for modulo/seeded_random/quantized_signal/run_length
seed = 12345                 # deterministic seeded_random input
run_length = 8               # adjacent samples per bucket for run_length/quantized_signal
stall_every = 100            # interval for signal_with_sporadic_stalls
stall_length = 16            # held samples inside each stall interval
mixed_every = 5              # every Nth sample becomes the repeated base value
```

The default `linear` pattern preserves the helper's historical formula. Use
`constant` or low-cardinality `modulo`/`seeded_random` values for repeated-value
data shapes, `run_length` for run-heavy low-cardinality series, `quantized_signal`
for signal-like values collapsed into a finite bucket set, `signal_with_sporadic_stalls`
for mostly continuous signals with periodic flat spots, and `mixed_signal_repeated`
for signal-plus-periodic-default mixtures. `unique` or high-cardinality buckets
still work for broad sample-value distributions. This is a generic sample-value
control for query/ingestion cases; it does not inspect or assert storage
encoding, Parquet footers, or storage policy choices. For chunked remote-write
ingestion, the runner passes the sample offset and total sample count to the
helper so non-linear value patterns use a stable global/per-series ordinal across
chunks.

Case schema, value distribution defaults, storage defaults, and read-bench
defaults are owned by Rust. The Python runner calls
`query_perf_fixture plan --case <case.toml>` and orchestrates the normalized JSON.
The same helper exposes `direct-sst`, `prom-remote-write`, and `inspect-footer`
subcommands; the old direct invocation (`query_perf_fixture --case ... --out-dir
...`) remains compatible.

Remote-write cases that need to validate storage output can add
`[scenario.remote_write.storage]`. When present, the scenario body becomes:
remote-write → flush → visibility check → query measurement → stop and await
datanode → Parquet footer/size/encoding inspection → optional read-bench. Footer
inspection is handled by `query_perf_fixture inspect-footer`, which reads local
Parquet footers directly; it does not require Python `pyarrow`.

```toml
[scenario.remote_write.storage]
inspect = true
column = "greptime_value"
include_metadata_files = false
# Optional: inspect below datanode data home instead of the whole fresh data dir.
# root_suffix = "greptime/public/<table_id>"
min_files = 1
min_files_with_column = 1
require_encodings = ["BYTE_STREAM_SPLIT"]
forbid_encodings = ["PLAIN_DICTIONARY"]
max_total_file_size_bytes = 104857600
max_column_compressed_size_bytes = 52428800
max_column_uncompressed_size_bytes = 209715200
max_candidate_total_file_size_regression_pct = 10.0
max_candidate_column_compressed_size_regression_pct = 10.0
max_candidate_column_uncompressed_size_regression_pct = 10.0
```

The `*_pct` storage thresholds are percent regression limits comparing candidate
against base; `pct` means percent, not percentile. Per-target storage checks
(`min_files`, `min_files_with_column`, required/forbidden encodings, and absolute
`max_*_bytes`) run for both base and candidate. Comparative
`max_candidate_*_regression_pct` checks remain base-vs-candidate. These checks
are generic footer and byte-size assertions and do not encode product-specific
heuristics. When storage inspection is enabled, `min_files` and
`min_files_with_column` default to `1` even if omitted, so dry-runs show these
planned checks and an empty flush or missing target column fails the scenario.
By default the inspector root is the target datanode data home, which is suitable
for a fresh single-case data directory. For reused or more complex data homes, set
`root_suffix` to a path relative to the datanode data home to narrow inspection.

When storage inspection is enabled, `[scenario.remote_write.read_bench]` defaults
to enabled with both datanode `parquetbench` and `scanbench`. Disable it with
`enabled = false`. `parquetbench` measures per-SST reader cost, `scanbench`
measures region scan cost, and query measurements still exercise the SQL/TQL
frontend path. Treat all performance conclusions as release-only; debug builds
are suitable only for command wiring and correctness checks.

The runner creates the configured database if needed, writes a per-target
frontend config enabling `[prom_store]` with metric engine storage and a non-zero
`pending_rows_flush_interval`, and validates that the logical metric table reaches
`series_count * samples_per_series` rows before trusting the query measurements.
Use `--fixture-generator /path/to/query_perf_fixture` to provide the Rust helper.
Deprecated `--remote-write-generator` and `--storage-inspector` options are kept
only for CLI compatibility. `--fixture-only` is rejected for remote-write cases;
use `--dry-run` for planning.

Large manual remote-write cases can set `sample_chunk_size` to split ingestion by
time. For each chunk, the runner invokes `query_perf_fixture prom-remote-write` with the
same series cardinality but a shorter `--samples-per-series` and an advanced
`--start-unix-millis`. `flush_every_sample_chunks` controls periodic
`ADMIN FLUSH_TABLE('<physical_table>')` calls; with `flush_every_sample_chunks = 1`,
each time chunk is flushed separately. The final visible SST/file-range layout is
still determined by the storage engine's normal compaction policy, so cases that
need multi-window file distribution should span multiple compaction windows. If
`sample_chunk_size` is omitted, the runner keeps the older single-helper-invocation
behavior and flushes once at the end.

The default remote-write coverage set contains four cases with the same 2048
series × 14,400 samples (29,491,200 rows per target) shape. Each writes ten
one-day chunks, flushes every chunk, requires at least two visible SSTs with the
value column, runs seven read-bench iterations with `parquetbench` capped at
four SSTs while `scanbench` covers the landed region, and measures one ten-day
TQL selector with two warmups and 15 iterations:

- `prom_remote_write_seeded_random`: high-distinct seeded-random values with
  cardinality 29,491,200 and seed 8444.
- `prom_remote_write_run_heavy`: low-cardinality values in exact 16-sample runs.
- `prom_remote_write_mixed_every`: continuous signal values with every fifth
  sample repeated at the base value.
- `prom_remote_write_integer_counter`: strictly increasing integer counter
  values, with disjoint ranges for each series.

These cases record normal storage/footer and read-bench results but do not gate
specific value encodings or storage-size outcomes. Use the controlled encoding
experiment matrix for those policy comparisons.

`tests/perf/query_cases/prom_remote_write_7913/case.toml` is a larger manual
case for issue #7913. It writes 8192 series × 20160 samples through remote-write
in 1440-sample daily time chunks, flushing after each chunk before running 1d/7d/14d
TQL selectors. It is not included in the default case set because ingestion cost
dominates routine CI validation.

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

The `[case]` table is metadata for reports. The executable regression config
lives under `[scenario]`. A scenario owns data generation, queries, and
thresholds:

```toml
[case]
name = "example"
description = "what this regression protects"

[scenario]
kind = "direct_readable_sst"
seed = 12345

[[scenario.tables]]
# table schema and distributions

[scenario.layout]
# SST and series layout

[[scenario.queries]]
# query, warmups, iterations, thresholds
```

The runner currently supports `direct_readable_sst` and
`prom_remote_write_then_query`.

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

Server-side scan thresholds such as file ranges and scanned rows are planned for
a follow-up PR that extracts them from structured `EXPLAIN ANALYZE VERBOSE`
output. Do not add those threshold keys until the runner enforces them.

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
  --fixture-cache-dir /mnt/query-regression-fixtures \
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

For large direct-SST fixtures, pass `--fixture-cache-dir <dir>` to store generated
fixtures in a persistent content-addressed cache keyed by case name and fixture
data configuration. Query and threshold edits reuse the same cached data as long
as the scenario layout and table definitions do not change. Cached fixtures are
reused automatically when their `summary.json`
matches the discovered table/region metadata; incompatible entries are
regenerated instead of reused. Fixture materialization keeps base and candidate
data directories isolated, but copies files efficiently by trying filesystem
reflinks first, hardlinks for immutable SST/object files next, and normal copies
as a fallback. Manifest files are reflinked or copied, not hardlinked.

Fixture generator smoke test:

```bash
cargo run -p cmd --bin query_perf_fixture --features dev-tools -- \
  direct-sst \
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

Remote-write runner dry-run:

```bash
uv run --no-project python tests/perf/query_regression_runner.py \
  --case tests/perf/query_cases/prom_remote_write_seeded_random/case.toml \
  --base-bin /path/to/base/greptime \
  --candidate-bin /path/to/candidate/greptime \
  --fixture-generator /path/to/query_perf_fixture \
  --work-dir /tmp/query-perf-remote-write \
  --dry-run
```

## GitHub Actions

`.github/workflows/query-regression.yml` provides an opt-in CI entrypoint for
query regression runs. It builds its own binaries for now:

- base `greptime` from the PR base commit, or `workflow_dispatch` `base_ref`
- candidate `greptime` and `query_perf_fixture` from the PR merge ref/current
  candidate checkout
- runner and summary formatter from the candidate checkout

The workflow builds base and candidate `greptime` as normal release-equivalent
binaries. Candidate `query_perf_fixture` is the extra head-side helper binary;
the runner uses candidate `greptime datanode parquetbench/scanbench` as the
read-bench tool against each target's data directory.

The workflow runs automatically only when `query-regression` is added to a
non-draft PR; it does not rerun on pushes, ready-for-review, or reopen events.
PR runs build base/candidate once and then run the default case set with
`--allow-large-fixture`. Manual `workflow_dispatch` runs can pass `all`, one case
path, or a comma/whitespace-separated list of case paths, and can override refs.
The main report artifact uploads only aggregate/per-target JSON reports,
component logs, and `query-regression-summary.md` with seven-day retention;
fixture data, SSTs, and cluster state are excluded. PR runs also upload a
separate trusted-comment artifact containing PR metadata and aggregate reports.
The workflow writes the Markdown summary to the workflow step summary and
updates a sticky PR comment through the trusted follow-up workflow.

## Built-in cases

The `promql_pushdown_7913` case is only one case using the generic fixture
format. It generates a high-cardinality metric-like table with a nanosecond time
index and many SSTs with non-overlapping time ranges. Its `timestamp_major`
series layout writes one sample for every series at each scrape timestamp, so
short PromQL/TQL selector windows still scan realistic raw sample volumes. The
queries should show scan-level time filters, tight SST pruning, and enough raw
rows to make distributed PromQL pipeline placement meaningful instead of a
millisecond-scale canary.

Additional SQL optimizer cases:

- `sql_topk_order_by`: single-table TopK / `ORDER BY` on a DOUBLE field with
  time and tag predicates.
- `sql_aggregate_order_by`: grouped aggregate ordered by aggregate value with a
  `LIMIT`.
- `sql_join_filter_order`: two direct-SST tables joined on a shared tag with
  time filters, aggregate ordering, and `LIMIT`.
