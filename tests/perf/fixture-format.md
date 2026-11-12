# Direct-SST fixture format

The phase-1 generator should be a reusable fixture generator, not a collection
of issue-specific data loaders.

## Inputs

Each case describes:

```toml
[case]
name = "example_metric"
description = "example direct-readable-SST regression"

[scenario]
kind = "direct_readable_sst"
seed = 12345
# Optional: pin the frontend `[query] parallelism` for base and candidate.
query_parallelism = 3

[[scenario.tables]]
database = "public"
name = "example_metric"
engine = "mito"
append_mode = true
sst_format = "flat"
primary_key = ["host", "instance"]
time_index = "ts"

[[scenario.tables.columns]]
name = "host"
type = "STRING"
semantic = "tag"
distribution = { kind = "cardinality", values = 100, prefix = "host" }

[[scenario.tables.columns]]
name = "value"
type = "DOUBLE"
semantic = "field"
distribution = { kind = "deterministic_wave", min = 0.0, max = 100.0 }

[[scenario.tables.columns]]
name = "ts"
type = "TIMESTAMP(9)"
semantic = "timestamp"

[scenario.layout]
regions = 1
sst_count = 1024
rows_per_sst = 4096
row_group_size = 512
time_range_layout = "non_overlapping_per_sst"
series_layout = "round_robin"

[[scenario.queries]]
name = "count_all"
kind = "sql"
query = "SELECT count(*) FROM example_metric"
warmup = 0
iterations = 1
```

### Query kinds

`kind = "prom_http"` runs a Prometheus range query by POSTing form fields to
`/v1/prometheus/api/v1/query_range`. `query`, `start`, `end`, and `step` are
sent as form fields; `start` and `end` define the range, and `step` defines its
evaluation interval. `start`, `end`, and `step` are optional in the case model
and default to empty form values when omitted. The table database is sent as the
`db` URL query parameter, rather than as a form field.

The measurement starts before the HTTP request is sent and ends after the full
response body is read and parsed as JSON. It therefore includes request send,
server-side Prometheus JSON response building, and response-body read and JSON
parsing; it is not server-side-only latency.

`series_layout = "round_robin"` advances the timestamp once per generated row and
cycles series labels across rows. `series_layout = "timestamp_major"` writes all
series for one timestamp before advancing to the next timestamp; use it for
Prometheus-like high-cardinality scrape fixtures where short query windows should
still contain many raw samples. `timestamp_major` requires `rows_per_sst` to be
divisible by `series_count`. `per_sst` uses one series selected from the SST
index. These describe logical generation order. Before either flat or primary-key
SST format is written, each completed generated batch is physically sorted by
encoded primary key ascending, timestamp ascending, and sequence descending, as
required by the Mito Parquet writer; sorting preserves every generated column and
row, including deterministic-wave values.

### Query plan and scan-metric assertions

A query whose purpose is to protect one optimizer feature must assert that the
feature ran. `EXPLAIN ANALYZE VERBOSE` responses carry the physical plan of every
stage, so the case can declare the plan shape and the scan counters it requires:

```toml
[[scenario.queries]]
name = "dyn_membership_join"
kind = "sql"
query = "EXPLAIN ANALYZE VERBOSE SELECT ..."
warmup = 2
iterations = 7

[scenario.queries.plan_checks]
require = ["DynamicFilter", 'trace_id@\d+\s+IN\s+\(']
forbid = [
  'DynamicFilter \[ empty \]',
  'DynamicFilter \[ true \]',
  'DynamicFilter \[[^\n]*\bAND true\b',
]

[scenario.queries.thresholds]
max_candidate_latency_regression_pct = 30
max_candidate_page_size_to_fetch_store_bytes = 1048576
max_candidate_num_sst_rows = 8192
```

`plan_checks.require` and `plan_checks.forbid` are unanchored regular
expressions over the concatenated `plan` rows of the analyze response. `require`
patterns must match at least once, `forbid` patterns must not match at all, and
unknown keys are rejected so a typo cannot silently disable a gate.

The scan-metric thresholds are absolute upper bounds on totals collected from
the same plan text: `max_candidate_page_size_to_fetch_store_bytes` totals
`page_size_to_fetch_store`, `max_candidate_num_sst_rows` totals `num_sst_rows`,
and `max_candidate_rows_before_filter` totals `rows_before_filter` (rows before
row-group pruning, so prefer `num_sst_rows` when the case prunes row groups). A
metric that does not appear in the plan text fails the assertion instead of
passing trivially. Values are taken as the maximum across the cold pre-warmup
execution and every measured iteration, because warm scans are served from the
Mito page cache and report no store reads.

Forbid patterns should match the fallback shapes precisely. A pattern such as
`DynamicFilter \[ [^\]]*\btrue\b` over-matches: the probe plan line also
contains unrelated JSON such as `"flat_format": true` after the filter, and a
membership list itself contains `]`, so bracket-limited patterns stop early.

All plan and scan-metric assertions gate the candidate only. The base target is
the control and may lack the optimization; its values are reported as
`base_actual` diagnostics. Results appear in the report `thresholds` array with
`query`, `target`, `threshold`, `actual`, `limit`, and `status`.

### String field columns and `dense_string`

`type = "STRING"` with `semantic = "field"` is a plain UTF-8 column, not a tag
dictionary, so JOIN keys can carry high cardinality without sharing the primary
key encoding. String fields must use the `dense_string` distribution:

```toml
[[scenario.tables.columns]]
name = "trace_id"
type = "STRING"
semantic = "field"
distribution = { kind = "dense_string", values = 642, matching_rows = 1926, prefix = "trace", noise_prefix = "noise" }
```

For logical row `r` (the row index across all SSTs of the table, not the row
index inside one SST): if `r < matching_rows`, the value is `prefix + (r %
values)`; otherwise it is `noise_prefix + r`. The first `matching_rows` rows
therefore hold a bounded membership set of `values` keys, and every later row is
a unique value that never matches, which is what JOIN membership and
prefiltering cases need. Any other distribution on a STRING field is rejected at
generation time. Use `values` equal to the build-side key count and
`matching_rows` equal to the number of probe rows that should match; keep the
build-side `noise_prefix` distinct from the probe-side `noise_prefix` so the two
join sides cannot collide by accident.

### Frontend query parallelism

`scenario.query_parallelism` pins the frontend query engine setting
(`[query] parallelism`) for the case's base and candidate frontends. The value
must be greater than zero when present; leaving it out keeps the GreptimeDB
default (one worker per core).

The fixture generator only reports it in the normalized plan JSON that
`query_perf_fixture plan` prints. `.github/scripts/query-regression-run.py`
writes one `frontend-query.toml` per target into the target work directory and
passes it to `greptime frontend start --config-file`, so pinning parallelism
keeps the base/candidate comparison from being dominated by core-count-scaled
scan parallelism. Cases that do not set it launch the frontend exactly as
before, with no `--config-file`.

`[scenario]` is required. Other scenario variants are intentionally unsupported
for now, but `scenario.kind` leaves room for future `write_then_query` and
`cache_warm_query` configuration.

The generator should use these declarations to produce:

- object-store SST files written through the real Mito SST writer
- manifest checkpoint and `_last_checkpoint`
- fixture summary with file IDs, row counts, time ranges, and generated schema

## Why this is generic

The same fixture format should support different query-regression families:

- PromQL/TQL time-index pushdown
- SQL predicate pruning
- projection and row-group pruning
- series scan behavior
- joins or aggregation over controlled layouts

Issue-specific behavior belongs in case configs and thresholds, not in the
generator implementation.

## Direct generation vs realism

Direct SST generation is phase-1 because it is fast and reproducible:

- SST count, file time ranges, row groups, and label distributions are fixed by
  the case config.
- The same fixture can be used for base and candidate builds.
- Large pruning-sensitive datasets can be created without spending CI time on
  ingestion, memtable flush, or compaction.

It is less realistic than ingestion-path data because it bypasses writes,
memtables, flush scheduling, and compaction. That tradeoff is intentional for
PR-level query regression. A later nightly/release suite can add ingestion-based
cases for end-to-end realism.

Multi-table cases are supported by generating one fixture directory per table.
Each table is still limited to one region, and the runner passes `--table`, the
discovered `--region-id`, and the discovered `--table-dir` for each table before
materializing all generated region subtrees into the same datanode data home.
This supports JOIN regression cases without changing existing single-table case
files.

Multi-table cases must use unique table names and unique `(database, name)`
pairs. The runner derives each fixture subdirectory from table index, database,
and table name, sanitizing path-unsafe characters to avoid collisions and unsafe
paths.

The preferred compatibility path is:

1. create an empty table with the target build to seed catalog/table metadata;
2. stop the process;
3. generate readable SSTs and replacement manifest checkpoints offline using the
   seeded region metadata;
4. restart and query the fixture.

Fully synthetic metadata is useful for generator smoke tests, but seeded metadata
is safer for end-to-end query performance cases.
