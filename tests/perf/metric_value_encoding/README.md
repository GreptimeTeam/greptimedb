# Metric value encoding perf suite

Manual, non-default perf suite for comparing MetricEngine physical value-column
Parquet modes:

- `plain`
- `no_dictionary`
- `byte_stream_split`

It generates direct-readable Mito SST fixtures with MetricEngine physical marker
columns. The `greptime_value` field uses the fixture generator's deterministic
`metric_signal` distribution (per-series baseline/phase, trend, periodic signal,
and pseudo-noise) rather than a tiny repeated wave, so dictionary effectiveness is
not dominated by artificial exact-value repetition. It then runs:

1. `query_perf_fixture` to generate SST + manifest fixtures;
2. `greptime datanode parquetbench` for low-level Parquet decode/read cost;
3. `greptime datanode scanbench` for direct Mito scan-path cost.

This suite is intentionally **not** part of the default query-regression cases.
Run it manually when investigating value encoding tradeoffs.

```bash
uv run --no-project python tests/perf/metric_value_encoding/run.py
```

For a larger run:

```bash
uv run --no-project python tests/perf/metric_value_encoding/run.py \
  --rows-per-sst 1048576 \
  --source-batch-rows 4096 \
  --row-group-size 65536 \
  --series-count 64 \
  --iterations 5
```

Outputs are written to `/tmp/opencode/metric-value-encoding-perf` by default,
including `report.md`, `report.json`, generated case files, fixtures, materialized
data homes, and raw benchmark logs.
