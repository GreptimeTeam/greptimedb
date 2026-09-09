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

The outer driver supports `scenario.kind = "prom_remote_write_then_query"` for a
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
                              # signal_with_sporadic_stalls, mixed_signal_repeated,
                              # bounded_mixed
base = 0.0
step = 0.125
cardinality = 4096           # buckets for modulo/seeded_random/quantized_signal/run_length;
                              # baseline buckets for bounded_mixed
seed = 12345                 # deterministic seeded_random input and bounded_mixed series hash
run_length = 8               # adjacent samples per bucket for run_length/quantized_signal;
                              # interpolation interval for bounded_mixed
stall_every = 100            # interval for signal_with_sporadic_stalls
stall_length = 16            # held samples inside each stall interval
mixed_every = 5              # every Nth sample becomes the repeated base value;
                              # one fractional bounded_mixed series per N series
```

The default `linear` pattern preserves the helper's historical formula. Use
`constant` or low-cardinality `modulo`/`seeded_random` values for repeated-value
data shapes, `run_length` for run-heavy low-cardinality series, `quantized_signal`
for signal-like values collapsed into a finite bucket set, `signal_with_sporadic_stalls`
for mostly continuous signals with periodic flat spots, and `mixed_signal_repeated`
for signal-plus-periodic-default mixtures. `unique` or high-cardinality buckets
still work for broad sample-value distributions.

`bounded_mixed` is synthetic rather than an empirical workload. For series `s`,
`h = splitmix64(s ^ seed)` selects a span from `[10, 1_000, 100_000, 10_000_000]`
using its top two bits, multiplied by `1 + ((h >> 32) % 10)`. Its baseline is
`(h % max(cardinality, 1)) * span / 10`. Ranges can overlap. At local sample
`l = sample_offset + sample_idx`, it linearly interpolates hash-derived anchors
in `[-span + 1, span - 1]`, changing anchors every `max(run_length, 1)` samples.
It applies `base + step * signal`, then rounds to an integer. Every
`max(mixed_every, 1)`-th series adds a hash-derived fraction from `1/1000` to
`999/1000`. The 95/5 finite, nonintegral guarantee applies to the case's bounded
parameters, not arbitrary huge or nonfinite base/step inputs. With `base = 0`
and `step = 1`, output lies within `baseline ± span`, allowing one unit for
rounding and the fractional addition. The explicit case uses 1,000 equal-length
series and `mixed_every = 20`, giving exactly 95% integer-valued and 5%
fractional Float64 samples. Its ranges and temporal shape are synthetic design
parameters, not measured properties from the survey.

This is a generic sample-value control for query/ingestion cases; it does not
inspect or assert storage encoding, Parquet footers, or storage policy choices.
For chunked remote-write ingestion, the runner passes the sample offset and total
sample count to the helper so non-linear value patterns use a stable global/per-series
ordinal across chunks.

Case schema, value distribution defaults, storage defaults, and read-bench
defaults are owned by Rust. The outer CI driver calls
`query_perf_fixture plan --case <case.toml>` once, then uses the normalized plan
to select the Rust runner lifecycle. The fixture helper exposes `direct-sst`,
`prom-remote-write`, and `inspect-footer` subcommands.

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

`prepare-remote` creates the configured database if needed. A remote-write case
can provide `base_setup_sql` and `candidate_setup_sql` lists; each target runs its
own complete statements in order after database creation and before ingestion.
The outer driver writes a per-target frontend config enabling `[prom_store]` with
metric engine storage and a non-zero `pending_rows_flush_interval`, and validates
that the logical metric table reaches `series_count * samples_per_series` rows
before trusting the query measurements. Use
`--fixture-generator /path/to/query_perf_fixture` to provide the Rust helper to
the outer driver.

Large manual remote-write cases can set `sample_chunk_size` to split ingestion by
time. For each chunk, `prepare-remote` invokes `query_perf_fixture prom-remote-write` with the
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
TQL selectors. It is not included in the default `all` case set because ingestion
cost dominates routine CI validation. Adding the `heavy-regression` PR label runs
only this case; `query-regression` runs the seven routine default cases. Manual
workflow dispatch accepts the `heavy` token to select this case.

## OTLP trace load scenario

`scenario.kind = "otlp_trace_load"` runs a bounded native `otelgen` process
against each local distributed cluster. The outer driver runs the base target
alone with Rust `run-otlp-target`, fully stops it, then runs the candidate alone
and calls Rust `finalize-otlp` to aggregate metrics, thresholds, and the final
report. The case is intentionally outside the default set until its variance is
known.

Build base/candidate `greptime` binaries with the same profile and build the
candidate `query_perf_fixture` and `query_regression_runner`, then run the outer
driver explicitly:

```bash
WORK_DIR="$(mktemp -d /tmp/query-perf-otlp.XXXXXX)"
uv run --no-project python .github/scripts/query-regression-run.py \
  --cases tests/perf/query_cases/otlp_trace_load/case.toml \
  --base-bin /path/to/base/target/nightly/greptime \
  --candidate-bin /path/to/candidate/target/nightly/greptime \
  --fixture-generator /path/to/candidate/target/nightly/query_perf_fixture \
  --runner /path/to/candidate/target/nightly/query_regression_runner \
  --otelgen-bin /path/to/otelgen \
  --work-dir "$WORK_DIR"
REPORT="$WORK_DIR/otlp_trace_load/query-regression-report.json"
```

For each target, `accepted_spans` should equal `table_rows`, and `failures`
should stay within `max_failure_count`. Throughput is better when
`spans_per_second` is higher; its `actual_pct` is
`(base - candidate) / base * 100`. Latency is better when `mean_latency_ms` is
lower; its `actual_pct` is `(candidate - base) / base * 100`. A positive
`actual_pct` is a candidate regression, while a negative value is an
improvement. The case passes when every `actual_pct` is at or below its
`limit_pct` and every failure-count threshold passes. For local results, run
the case at least three times on an otherwise idle machine and compare the
median regressions rather than relying on one run.

The CI runner image includes the pinned `otelgen` binary. Until this case is
added to the default set, run it explicitly with `workflow_dispatch`:

```bash
gh workflow run query-regression.yml \
  --ref <workflow-branch> \
  -f case=tests/perf/query_cases/otlp_trace_load/case.toml \
  -f base_ref=<full-base-sha> \
  -f candidate_ref=<full-candidate-sha> \
  -f cargo_profile=nightly \
  -f http_timeout=300 \
  -f runner=aliyun-ecs
```

The `aliyun-ecs` path provisions a fresh ECS instance per run from the custom
image built from the current query-regression Dockerfile; see
`.github/runner-scale-sets/query-regression/README.md` for its configuration.

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

The outer driver currently supports `direct_readable_sst`,
`prom_remote_write_then_query`, and `otlp_trace_load`.

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

## Runner lifecycle

`.github/scripts/query-regression-run.py` is the outer CI driver. It resolves the
base and candidate `greptime` binaries, candidate `query_perf_fixture`, and
candidate `query_regression_runner`; allocates disjoint localhost
meta/datanode/frontend HTTP, gRPC, MySQL, and Postgres ports for each target; and
writes component logs below `<work-dir>/<target>/logs/`.

The Rust runner consumes the normalized plan and endpoint ports. For
`direct_readable_sst`, the driver starts both clusters, runs `prepare-direct`,
stops only both datanodes, writes File-object-store destination TOMLs, invokes
`materialize` for every fixture and target, restarts datanodes, then runs
`measure` (with one frontend restart retry). `materialize` uses OpenDAL and only
replaces the fixture's exact region prefix in each target data home.

For `prom_remote_write_then_query`, it renders a target frontend configuration
with `render-remote-config`, starts both clusters, runs `prepare-remote` and
`measure`, stops both datanodes, then runs `finalize-remote`. Finalization owns
storage inspection and read-bench against the quiescent data homes. Both paths
write `query-regression-report.json` in the case work directory and always clean
up the remaining components.

For local orchestration after building the three binaries, invoke the outer
driver directly:

```bash
uv run --no-project python .github/scripts/query-regression-run.py \
  --cases tests/perf/query_cases/smoke_direct_sst/case.toml \
  --base-bin /path/to/base/greptime \
  --candidate-bin /path/to/candidate/greptime \
  --fixture-generator /path/to/query_perf_fixture \
  --runner /path/to/query_regression_runner \
  --work-dir /tmp/query-regression-work
```

### SST float BSS comparison

`tests/perf/query_cases/sst_float_bss/case.toml` is included in the routine
`all` default case group and compares a default empty physical metric table with
a candidate byte-stream-split (BSS) physical table. It writes 1,000 series × 4,320 samples
(4,320,000 rows) using synthetic `bounded_mixed` values: 95% integral series and
5% nonintegral series, with bounded per-series fluctuation rather than globally
unique values. This deliberately matches a 95/5 design; it is not an empirical
claim about any production population. A separate survey sample found 94.79%
integral values, but that observation does not make its values globally unique.
Current mixed-data evidence, including SST inspection, exact-bit row verification,
warmed endpoint SQL, and both warm-reader projections, is recorded in
[`query_cases/sst_float_bss/RESULTS.md`](query_cases/sst_float_bss/RESULTS.md).
Timing observations were collected while other builds saturated the shared host;
they are not performance acceptance evidence. Before rerunning timings, ensure
the host is idle (not merely this agent), record load throughout, and avoid
concurrent builds. CPU affinity alone does not isolate memory or I/O contention.
The historical unique-integer workload is preserved only in Git history and does
not apply to this case.
Both targets must use the exact same release `greptime` binary; only the
per-target table setup SQL differs. Run the existing driver from a checkout
containing that binary, with absolute paths and fresh data directories for every
run:

```bash
REPO="$(pwd -P)"
RELEASE_GREPTIME="/absolute/path/to/release/greptime"
FIXTURE_GENERATOR="/absolute/path/to/release/query_perf_fixture"
RUNNER="/absolute/path/to/release/query_regression_runner"
WORK_DIR="/absolute/path/to/fresh/sst-float-bss-run-1"
cd "$REPO"
uv run --no-project python "$REPO/.github/scripts/query-regression-run.py" \
  --cases "$REPO/tests/perf/query_cases/sst_float_bss/case.toml" \
  --base-src "$REPO" \
  --candidate-src . \
  --base-bin "$RELEASE_GREPTIME" \
  --candidate-bin "$RELEASE_GREPTIME" \
  --fixture-generator "$FIXTURE_GENERATOR" \
  --runner "$RUNNER" \
  --work-dir "$WORK_DIR" \
  --summary-script "$REPO/.github/scripts/query-regression-summary.py"
```

Repeat the command three times with a different fresh absolute `WORK_DIR` each
run. In each `query-regression-report.json`, compare base and candidate
`targets[].storage_inspection.summary.summary.total_file_size`, and each query's
`targets[].measurements[].latency_ms_median`. Storage percentage is
`(candidate_total_file_size - base_total_file_size) / base_total_file_size * 100`;
query latency percentage is
`(candidate_latency_ms_median - base_latency_ms_median) / base_latency_ms_median * 100`.
The configured three warmups occur after the initial query validation and before
that query's 15 measured endpoint requests, so query latency is a warmed
frontend/cache measurement. The case restricts storage inspection to
`data/greptime/public`, excluding `greptime_private` SSTs. After the datanodes
stop, it also runs seven iterations of value-only `parquetbench` for all
inspected SST files and sequential `scanbench` over the corresponding regions,
with parallelism one. The three explicit flushes yielded six SSTs in the observed
mixed runs—three 1,105,920-row files and three 334,080-row files—because storage split each
flush; this is an observation, not a guaranteed file layout. Their per-run
output and aggregate `parquetbench_median_average_ms` and
`scanbench_median_average_ms` are under
`targets[].read_bench`; they are quiescent local-file read/scan diagnostics, not
warmed frontend-query latency measurements. Bench averages include iteration one;
no OS cache is dropped, and the driver runs base before candidate.

Historical [PR #8548](https://github.com/GreptimeTeam/greptimedb/pull/8548)
reported storage savings alongside warm-read slowdowns for a different mixed
counter/gauge study. It used value-only and all-column projections, discarded the
first iteration, and alternated target order. This case reuses that reader
measurement approach, not its dataset or results. For the supplementary warm
comparison, reuse recorded commands against stopped data directories with eight
iterations, discard iteration one, and alternate target order for eight rounds.
For parquetbench, sum all per-file warm medians before taking the outer median;
for scanbench, take the outer median of whole-region warm medians. Keep
post-flush and post-compaction measurements separate.

The case's `-5.0` storage target and `25` query-latency guardrail are experimental
acceptance targets, not observed-benefit claims; do not relax them if a run
fails. Its three periodic flushes and shared high TWCS trigger avoid the normal
four-file compaction trigger from confounding the layout. Check footer encodings
(BSS on candidate and no BSS on base) and data equality from the artifacts rather
than through a new harness framework.

The Rust runner subcommands are also useful for focused diagnostics:

```bash
query_regression_runner prepare-direct --case <case.toml> --fixture-generator <fixture> \
  --base-http-port <port> --candidate-http-port <port> --fixture-dir <dir> --output <json>
query_regression_runner materialize --fixture-dir <dir> --destination <toml>
query_regression_runner measure --case <case.toml> --fixture-generator <fixture> \
  --base-http-port <port> --candidate-http-port <port> --output <report.json>
```

## GitHub Actions

`.github/workflows/query-regression.yml` provides an opt-in CI entrypoint for
query regression runs. It builds its own binaries for now:

- base `greptime` from the PR base commit, or `workflow_dispatch` `base_ref`
- candidate `greptime`, `query_perf_fixture`, and `query_regression_runner` from
  the PR merge ref/current candidate checkout
- outer driver and summary formatter from the candidate checkout

The workflow builds base and candidate `greptime` as normal release-equivalent
binaries. Candidate `query_perf_fixture` and `query_regression_runner` are the
extra head-side helpers; `finalize-remote` uses candidate `greptime datanode
parquetbench/scanbench` as the read-bench tool against each target's data directory.

The workflow runs automatically only when `query-regression` or `heavy-regression`
is added to a non-draft PR; it does not rerun on pushes, ready-for-review, or
reopen events. `query-regression` runs the seven routine default cases, while
`heavy-regression` runs only the high-cardinality remote-write #7913 case. PR runs
build base/candidate once and use `--allow-large-fixture`. Manual
`workflow_dispatch` runs can pass `all`, `heavy`, one case path, or a
comma/whitespace-separated list of case paths, and can override refs.
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
