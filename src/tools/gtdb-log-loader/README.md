# gtdb-log-loader

High-throughput log ingestion tool for the o11ybench regression benchmark: it
reads a JSONL file and writes the rows into GreptimeDB over gRPC (port 4001)
using the official `client` crate, with parallel in-flight insert requests. It
is a data-loading companion for benchmark setups that need a large, stable
volume of log rows written as fast as possible.

## Build

```bash
cargo build --release -p gtdb-log-loader
# binary: target/release/gtdb-log-loader
```

## Usage

```bash
gtdb-log-loader \
  --endpoint 127.0.0.1:4001 \
  --data agent_observations.jsonl \
  --table agent_observations \
  --database public \
  --columns-file columns_agent_observations.json \
  --batch-size 10000 \
  --concurrency 8
```

| Option | Default | Description |
| --- | --- | --- |
| `--endpoint` | (required) | GreptimeDB gRPC address, e.g. `127.0.0.1:4001`. |
| `--data` | (required) | Path to the input JSONL file. |
| `--table` | `app_logs` | Target table name. |
| `--database` | `public` | Target database name. |
| `--batch-size` | `10000` | Rows per insert request. |
| `--concurrency` | `8` | Number of parallel in-flight insert tasks. |
| `--columns-file` | (none) | JSON file describing the target table columns (see below). Omit to use the built-in `app_logs` schema. |
| `--skip-bad-lines` | `false` | Skip JSONL lines that fail to parse (printed as warnings) instead of aborting the whole load. The number of skipped lines is reported in the final summary. |

The tool exits with a non-zero status when the total number of rows the server
reports as affected does not match the source line count (this can happen, for
example, when rows are deduplicated because the table does not actually use
`append_mode`).

## Columns file

`--columns-file` points to a JSON array of column descriptors. The descriptor
keys are:

| Key | Required | Description |
| --- | --- | --- |
| `name` | yes | Column name; must match the JSONL object key. |
| `kind` | yes | One of `ts`, `string`, `int64`, `date`, `decimal`, `json`. |
| `precision` | no | Decimal precision (default `38`), only used for `decimal`. |
| `scale` | no | Decimal scale (default `10`), only used for `decimal`. |

`kind` semantics:

- `ts` — time index; accepts an integer epoch-millis value or an RFC3339
  string (`Z` or `±HH:MM` / `±HHMM` offsets, optional fractional seconds),
  normalized to UTC.
- `string` — text column; `null` maps to SQL `NULL`.
- `int64` — integer column; `null` maps to SQL `NULL`.
- `date` — `"YYYY-MM-DD"` string converted to days since epoch.
- `decimal` — JSON number or decimal string parsed with integer/fixed-point
  arithmetic (never `f64`) and scaled by `10^scale`, so all 38 digits of a
  `DECIMAL(38, scale)` survive. More fractional digits than `scale` are
  rounded half away from zero.
- `json` — raw JSON value serialized to a string; the server converts it to
  JSONB.

Example (`columns_agent_observations.json`, the 23-column
agent-observability schema shipped with this tool):

```json
[
  {"name": "event_time", "kind": "ts"},
  {"name": "biz_date", "kind": "date"},
  {"name": "trace_id", "kind": "string"},
  {"name": "session_id", "kind": "string"},
  {"name": "observation_id", "kind": "string"},
  {"name": "parent_observation_id", "kind": "string"},
  {"name": "type", "kind": "string"},
  {"name": "status", "kind": "string"},
  {"name": "tenant", "kind": "string"},
  {"name": "app", "kind": "string"},
  {"name": "environment", "kind": "string"},
  {"name": "task_category", "kind": "string"},
  {"name": "trace_archetype", "kind": "string"},
  {"name": "model", "kind": "string"},
  {"name": "tool_name", "kind": "string"},
  {"name": "input", "kind": "string"},
  {"name": "output", "kind": "string"},
  {"name": "seq_no", "kind": "int64"},
  {"name": "input_tokens", "kind": "int64"},
  {"name": "output_tokens", "kind": "int64"},
  {"name": "latency_ms", "kind": "int64"},
  {"name": "total_cost", "kind": "decimal"},
  {"name": "payload", "kind": "json"}
]
```

## Sample data

`agent_observations.sample.jsonl` contains three lines matching the schema
above. It is meant for smoke-testing the tool end to end:

```bash
gtdb-log-loader \
  --endpoint 127.0.0.1:4001 \
  --data agent_observations.sample.jsonl \
  --table agent_observations \
  --columns-file columns_agent_observations.json
```

## Table setup

The target table must already exist: create it with the SQL DDL from the
benchmark setup so schema and indexes match the benchmark contract exactly.
The table's `DECIMAL` columns should be declared with the same
`precision`/`scale` used in the columns file. If the server is started with
`auto_create_table` enabled, a missing table is created on the first insert —
but then column types/options are inferred from the request, which is only
suitable for quick smoke tests, not for reproducible benchmarks.

## Write hints

Every insert request carries engine-option hints (`append_mode=true`,
`compaction.type=twcs`, `compaction.twcs.time_window=2h`). The hints take
effect only when the target table does not exist and the server is started
with `auto_create_table` enabled: the first insert then creates the table
with these options. When the table already exists, the write path ignores
the hints entirely — declare `append_mode` and the compaction options
(`compaction.type=twcs`, `compaction.twcs.time_window=2h`, etc.) explicitly
in the table DDL. The hint keys correspond to the Mito engine option
constants in `src/store-api/src/mito_engine_options.rs`; the server derives
the remaining compaction configuration from `compaction.type`. If the DDL
and the actual table behavior disagree, the tool's row-count check
(`rows_written != source`) fails and reports the mismatch.
