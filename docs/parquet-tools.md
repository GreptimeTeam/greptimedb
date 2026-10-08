# Parquet Development Tools

GreptimeDB provides the following datanode CLI commands for inspecting,
rewriting, benchmarking, and replacing Parquet SST files:

- `parquet-meta`: inspect a local Parquet file.
- `parquet-rewrite`: rewrite a local Parquet file with different writer
  properties.
- `parquetbench`: benchmark reads from a local GreptimeDB SST or an SST in a
  configured object store.
- `sst-replace`: replace an existing Mito SST object and update its manifest
  metadata.

These are development and recovery tools, not stable user-facing interfaces.
Always test a workflow on disposable data before using it on an important
region.

## Build

`parquet-meta`, `parquet-rewrite`, and `sst-replace` require the `dev-tools`
feature. The same build also includes `parquetbench`:

```bash
cargo build -p cmd --bin greptime --features dev-tools
```

The examples below use:

```bash
GREPTIME=./target/debug/greptime
```

Run `$GREPTIME datanode <COMMAND> --help` for the authoritative argument list.

## Inspect a Parquet file

`parquet-meta` reads a local Parquet footer and optional page indexes. It reports
file, row-group, and column metadata, including compression, encodings, sizes,
page offsets, statistics presence, and bloom-filter/index offsets.

Text output:

```bash
$GREPTIME datanode parquet-meta \
  --input /tmp/source.parquet
```

Machine-readable JSON output:

```bash
$GREPTIME datanode parquet-meta \
  --input /tmp/source.parquet \
  --format json > /tmp/source-meta.json
```

`--format` accepts `text` (the default) or `json`. This command can inspect a
general Parquet file; it does not require GreptimeDB SST key-value metadata.

## Rewrite a Parquet file

`parquet-rewrite` has two modes. First, dump a TOML properties file inferred
from an existing file:

```bash
$GREPTIME datanode parquet-rewrite \
  --input /tmp/source.parquet \
  --dump-properties /tmp/writer-properties.toml
```

Review and edit the generated file. Its shape is:

```toml
[writer]
dictionary_enabled = true
compression = "zstd"
max_row_group_row_count = 8192

[[columns]]
path = ["host"]
dictionary_enabled = true
compression = "zstd"
encoding = "plain"
```

Supported compression names are `uncompressed`, `snappy`, `gzip`, `lzo`,
`brotli`, `lz4`, `zstd`, and `lz4-raw`. Supported encodings are `plain`,
`delta-binary-packed`, `delta-length-byte-array`, `delta-byte-array`, and
`byte-stream-split`.

The `[writer]` table also accepts `compression_level`,
`data_page_size_limit`, `data_page_row_count_limit`, and
`dictionary_page_size_limit`. It also accepts
`column_index_truncate_length` and `statistics_truncate_length`; set either to
zero to disable that truncation. A `[[columns]]` entry overrides dictionary,
compression, compression level, or encoding for its column path. Unknown TOML
fields are rejected. Compression levels are supported for `gzip`, `brotli`, and
`zstd`. A column-level `compression_level` without `compression` inherits the
writer compression codec; it is rejected when there is no writer codec to
inherit.

Rewrite the data using the edited properties:

```bash
$GREPTIME datanode parquet-rewrite \
  --input /tmp/source.parquet \
  --properties /tmp/writer-properties.toml \
  --output /tmp/rewritten.parquet
```

Use `--batch-size <ROWS>` to control reader batch size. Output and dumped
properties files are not replaced unless `--overwrite` is passed. The output or
dump path must not be exactly the same path as the input, even with
`--overwrite`. This check does not resolve symbolic links, hard links, or other
spellings of the same path.

The rewrite decodes and writes the Arrow record batches and copies the Parquet
key-value metadata, but it creates a new physical Parquet layout. Inspect and
validate the result before using it as an SST:

```bash
$GREPTIME datanode parquet-meta \
  --input /tmp/rewritten.parquet
```

The dumped properties are inferred primarily from the first row group. Review
them when the source uses different properties across row groups.

## Benchmark an SST

`parquetbench` expects GreptimeDB region metadata embedded in the SST. It can
read a local file with the direct reader:

```bash
$GREPTIME datanode parquetbench \
  --file-path /tmp/rewritten.parquet \
  --reader direct \
  --iterations 5 \
  --batch-size 8192
```

Local-file mode cannot be combined with `--config`, `--region-id`,
`--table-dir`, or `--file-id`, and it does not support the `flat-prune` reader.

To benchmark an SST in the object store configured for a datanode or standalone
deployment:

```bash
$GREPTIME datanode parquetbench \
  --config /path/to/datanode.toml \
  --region-id 1024:0 \
  --table-dir data/greptime/public/1024 \
  --file-id 00020380-009c-426d-953e-b4e34c15af34 \
  --path-type bare \
  --reader flat-prune \
  --iterations 5
```

Region mode requires all four of `--config`, `--region-id`, `--table-dir`, and
`--file-id`. `--region-id` accepts either the packed unsigned integer or
`<table-id>:<region-number>`. `--path-type` accepts `bare`, `data`, or
`metadata` and defaults to `bare`.

An optional scan configuration selects columns and row groups:

```json
{
  "projection_names": ["host", "value", "ts"],
  "row_groups": [0, 2]
}
```

```bash
$GREPTIME datanode parquetbench \
  --file-path /tmp/rewritten.parquet \
  --scan-config /tmp/parquet-scan.json \
  --iterations 5
```

Use `--pk-as-binary` to expose `__primary_key` as binary with the direct reader.
On Unix, `--pprof-file <SVG>` writes a flamegraph. Add
`--pprof-after-warmup` and use at least two iterations to exclude the first
iteration from profiling.

## Replace an existing region SST

> **Warning:** `sst-replace` mutates both an SST object and its region manifest.
> Stop the datanode that owns the region and back up the target SST and manifest
> before using `--confirm`. The SST write happens before the manifest update, so
> an interrupted or failed operation may require restoring the backup.

`sst-replace` replaces the contents of an existing SST file ID. It does not add
a new file ID to a region. The command requires the replacement to have the same
row count and row-group count recorded in the manifest when those manifest
values are nonzero. It does not validate schema or row contents. The replacement
is loaded into memory in full, so ensure the machine has enough memory for the
SST.

Start with the default dry run using a local replacement file:

```bash
$GREPTIME datanode sst-replace \
  --config /path/to/datanode.toml \
  --region-id 1024:0 \
  --table-dir data/greptime/public/1024 \
  --file-id 00020380-009c-426d-953e-b4e34c15af34 \
  --replacement-file /tmp/rewritten.parquet
```

The dry run locates the manifest and target SST, reads and validates the
replacement footer, and prints the old and new sizes without writing anything.
`--path-type` defaults to `auto`, which probes `bare`, `data`, and `metadata`.
Specify the path type if the file ID is visible in more than one manifest.

The replacement can instead be read from the configured object store:

```bash
$GREPTIME datanode sst-replace \
  --config /path/to/datanode.toml \
  --region-id 1024:0 \
  --table-dir data/greptime/public/1024 \
  --file-id 00020380-009c-426d-953e-b4e34c15af34 \
  --replacement-object staging/rewritten.parquet \
  --path-type bare
```

After reviewing the dry-run output and confirming that the datanode is stopped,
repeat the exact command with `--confirm`:

```bash
$GREPTIME datanode sst-replace \
  --config /path/to/datanode.toml \
  --region-id 1024:0 \
  --table-dir data/greptime/public/1024 \
  --file-id 00020380-009c-426d-953e-b4e34c15af34 \
  --replacement-file /tmp/rewritten.parquet \
  --path-type bare \
  --confirm
```

The confirmed operation overwrites the existing SST object, recalculates file
size and row-group statistics, and appends a manifest edit for the existing file
ID. Restart the datanode and validate queries against the region before removing
the backup.

## Recommended rewrite and replacement workflow

1. Back up the source SST and its region manifest.
2. Inspect the source with `parquet-meta`.
3. Dump and edit writer properties with `parquet-rewrite`.
4. Rewrite to a new local file; never rewrite directly over the source SST.
5. Inspect the output and benchmark it with `parquetbench`.
6. Stop the owning datanode.
7. Run `sst-replace` without `--confirm` and review its resolved target and
   statistics.
8. Repeat with `--confirm`, restart the datanode, and validate the region.
