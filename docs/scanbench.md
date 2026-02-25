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
  [--parallelism <N>] \
  [--iterations <N>] \
  [--path-type <bare|data|metadata>] \
  [--force-flat-format] \
  [--skip-wal-replay <true|false>] \
  [--pprof-file <FLAMEGRAPH_SVG>] \
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
- `--parallelism`: Simulated scan parallelism. Default: `1`.
- `--iterations`: Benchmark iterations. Default: `1`.
- `--path-type`: Region path type (`bare`, `data`, `metadata`). Default: `bare`.
- `--force-flat-format`: Force reading the region in flat format. Default: disabled.
- `--skip-wal-replay`: Skip WAL replay when opening the region. Default: `true`. Set to `false` to enable WAL replay using the log store configured in the `[wal]` section of the config TOML (raft-engine or Kafka). When `true` or when no WAL is configured, a `NoopLogStore` is used.
- `--pprof-file`: Output flamegraph path (Unix only).
- `--verbose` / `-v`: Enable verbose output.

## Scan Config JSON

```json
{
  "projection": [0, 1, 2],
  "projection_names": ["host", "cpu"],
  "filters": ["host = 'web-1'", "cpu > 80"],
  "limit": 100000,
  "series_row_selector": "last_row"
}
```

Notes:
- All fields are optional.
- Use either `projection` (indexes) or `projection_names` (column names), not both.
- `projection_names` uses exact (case-sensitive) column name matching.
- `filters` is a list of SQL expressions (not full SQL statements), e.g. `"host = 'web-1'"`.
- `series_row_selector` currently supports only `"last_row"`.

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
  --skip-wal-replay false
```
