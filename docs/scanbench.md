# Scanbench Usage

`scanbench` benchmarks region scans directly from storage through:

```bash
greptime datanode scanbench ...
```

## Build

```bash
cargo build -p cmd --bin greptime
```

## Command

```bash
./target/debug/greptime datanode scanbench \
  --config <CONFIG_TOML> \
  --region-id <REGION_ID> \
  --table-dir <TABLE_DIR> \
  [--scanner <seq|unordered|series>] \
  [--scan-config <SCAN_CONFIG_JSON>] \
  [--scan-configs <SCAN_CONFIGS_JSON>] \
  [--parallelism <N>] \
  [--iterations <N>] \
  [--path-type <bare|data|metadata>] \
  [--force-flat-format] \
  [--enable-wal] \
  [--pprof-file <FLAMEGRAPH_SVG>] \
  [--result-file <RESULT_JSON>] \
  [--pprof-after-warmup] \
  [--verbose]
```

## Required Arguments

- `--config`: Datanode/standalone TOML config.
- `--region-id`: Region ID in one of:
  - `<u64>` (example: `4398046511104`)
  - `<table_id>:<region_number>` (example: `1024:0`)
- `--table-dir`: Table directory used in open request (example: `greptime/public/1024`).

## Optional Arguments

- `--scanner`: Scan strategy. Default: `seq`.
  - `seq`: default scan
  - `unordered`: time-windowed distribution
  - `series`: per-series distribution
- `--scan-config`: JSON file to tune scan request.
- `--scan-configs`: JSON array of named scan requests. Each entry is executed
  exactly once in file order. This conflicts with `--scan-config`; the array
  length replaces `--iterations`, which must remain at its default value of `1`.
- `--parallelism`: Simulated scan parallelism. Default: `1`.
- `--iterations`: Benchmark iterations. Default: `1`.
- `--path-type`: Region path type (`bare`, `data`, `metadata`). Default: `bare`.
- `--force-flat-format`: Force reading the region in flat format. Default: disabled.
- `--enable-wal`: Enable WAL replay when opening the region. Default: disabled. When enabled, scanbench uses the log store configured in the `[wal]` section of the config TOML (raft-engine or Kafka). When disabled or when no WAL is configured, a `NoopLogStore` is used.
- `--pprof-file`: Output flamegraph path (Unix only).
- `--result-file`: Write structured benchmark and scanner analyze results as
  JSON. Existing files are overwritten after all scans complete successfully.
  Supplying this option collects verbose scanner metrics even without
  `--verbose`.
- `--pprof-after-warmup`: Start profiling after the first iteration, using it as a warmup. Requires `--pprof-file`. Default: disabled.
- `--verbose` / `-v`: Print scanner metrics plus per-partition row counts,
  first-batch latency, total elapsed time, and partition skew. Parquet scanner
  metrics also identify the physical prefilter columns and split fetch metrics
  into `primary_key_io`, `prefilter_io`, and `projection_io`. Prefilter timing
  separates column read/decode, predicate evaluation, and selection construction.

### Verbose prefilter diagnostics

The `fetch_metrics` object in scanner explain output includes these additional
fields when prefiltering runs:

- `prefilter_columns_read`: physical columns decoded for predicate cache misses.
- `prefilter_candidate_rows`, `prefilter_selected_rows`, and
  `prefilter_filtered_rows`: prefilter selectivity.
- `prefilter_rows_read` and `prefilter_batches_read`: work actually decoded;
  these can be lower than candidate rows when the predicate-result cache hits.
- `prefilter_column_read_elapsed`, `prefilter_filter_eval_elapsed`, and
  `prefilter_selection_elapsed`: time spent reading/decoding columns, evaluating
  predicates, and constructing the row selection.
- `prefilter_result_cache_hits` and `prefilter_result_cache_misses`: reuse of
  cached predicate masks.
- `primary_key_io`, `prefilter_io`, and `projection_io`: cache/store requests,
  bytes, and fetch elapsed time attributed to the three Parquet read phases. The
  existing top-level fetch fields remain totals across all phases.

## Scan Config JSON

```json
{
  "projection": [0, 1, 2],
  "projection_names": ["host", "cpu"],
  "filters": ["host = 'web-1'", "cpu > 80"],
  "series_row_selector": "last_row"
}
```

Notes:
- All fields are optional.
- Use either `projection` (indexes) or `projection_names` (column names), not both.
- `projection_names` uses exact (case-sensitive) column name matching.
- `filters` is a list of SQL expressions (not full SQL statements), e.g. `"host = 'web-1'"`.
- `series_row_selector` currently supports only `"last_row"`.

## Multiple Scan Configs

Use `--scan-configs` to run a sequence of different scan requests:

```json
[
  {
    "name": "cold",
    "projection_names": ["hostname", "usage_user"],
    "filters": ["hostname = 'host_1'"]
  },
  {
    "name": "hot-001",
    "projection_names": ["hostname", "usage_user"],
    "filters": ["hostname = 'host_2'"]
  }
]
```

The array must contain at least one config. `name` is optional; omitted names
become `query-001`, `query-002`, and so on. Names must be non-empty and unique.
Scanbench validates every config before starting the benchmark, executes the
array once in order, and prints both an overall mean and a per-query summary.

With `--pprof-after-warmup`, the first entry is the warmup query and profiling
starts before the second entry. The warmup query remains part of the reported
overall statistics.

## Result JSON

`--result-file` writes a versioned JSON document with benchmark settings,
ordered run results, and overall and per-query summaries. Each run contains its
normalized config, row and batch counts, setup/scan/total timing, memory sizes,
per-partition statistics, and verbose scanner explain output. Normalized
projections use resolved column indexes.

Timing fields end in `_ns` and use nanoseconds. Size fields end in `_bytes`. If
config validation or any scan fails, scanbench returns the error without writing
the result file.

## Examples

Default sequential scan:

```bash
./target/debug/greptime datanode scanbench \
  --config /path/to/config.toml \
  --region-id 1024:0 \
  --table-dir greptime/public/1024
```

Unordered scan with parallelism:

```bash
./target/debug/greptime datanode scanbench \
  --config /path/to/config.toml \
  --region-id 1024:0 \
  --table-dir greptime/public/1024 \
  --scanner unordered \
  --parallelism 8 \
  --iterations 5
```

Run 10 different queries once each by placing 10 entries in the JSON array:

```bash
./target/debug/greptime datanode scanbench \
  --config /path/to/config.toml \
  --region-id 1024:0 \
  --table-dir greptime/public/1024 \
  --scanner seq \
  --parallelism 8 \
  --scan-configs /path/to/scan-configs.json \
  --result-file /path/to/scanbench-results.json \
  --verbose
```

Series scan with scan config and flamegraph:

```bash
./target/debug/greptime datanode scanbench \
  --config /path/to/config.toml \
  --region-id 1024:0 \
  --table-dir greptime/public/1024 \
  --scanner series \
  --scan-config /path/to/scan-config.json \
  --pprof-file /tmp/scanbench.svg
```

Force flat-format read:

```bash
./target/debug/greptime datanode scanbench \
  --config /path/to/config.toml \
  --region-id 1024:0 \
  --table-dir greptime/public/1024 \
  --force-flat-format
```

Scan with WAL replay enabled (uses `[wal]` config from TOML):

```bash
./target/debug/greptime datanode scanbench \
  --config /path/to/config.toml \
  --region-id 1024:0 \
  --table-dir greptime/public/1024 \
  --enable-wal
```
