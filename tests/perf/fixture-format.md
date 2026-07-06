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

[[scenario.tables]]
database = "public"
name = "example_metric"
engine = "mito"
append_mode = true
sst_format = "flat"
primary_key = ["host", "instance"]
time_index = "ts"
# Optional: emit MetricEngine physical-data-like SSTs for manual value encoding
# experiments. Defaults are false/plain.
metric_physical = false
metric_engine_value_encoding = "plain" # plain | no_dictionary | byte_stream_split

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
# For metric-like value encoding experiments, use a deterministic signal with
# per-series baseline/phase, trend, periodic components, and pseudo-noise.
# distribution = { kind = "metric_signal", min = 0.0, max = 100.0 }

[[scenario.tables.columns]]
name = "ts"
type = "TIMESTAMP(9)"
semantic = "timestamp"

[scenario.layout]
regions = 1
sst_count = 1024
rows_per_sst = 4096
# Optional: split each SST source into chunks before passing them to the writer.
# If omitted, the generator preserves the historical one RecordBatch per SST.
source_batch_rows = 1024
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

`[scenario]` is required. Other scenario variants are intentionally unsupported
for now, but `scenario.kind` leaves room for future `write_then_query` and
`cache_warm_query` configuration.

The generator should use these declarations to produce:

- object-store SST files written through the real Mito SST writer
- manifest checkpoint and `_last_checkpoint`
- fixture summary with file IDs, row counts, time ranges, and generated schema

`metric_physical = true` is intended for manual MetricEngine value-column
encoding suites. It injects the physical data-region marker tags `__table_id`
and `__tsid` into the synthetic Mito region metadata and primary key so the
MetricEngine `greptime_value` encoding selector is exercised. Existing query
regression cases should omit it unless they intentionally need this storage
layout.

For `DOUBLE` fields, `deterministic_wave` is kept for backward compatibility.
New metric encoding perf cases should prefer `metric_signal` to avoid a short
exact-value cycle that makes Parquet dictionary encoding unrealistically strong.

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
