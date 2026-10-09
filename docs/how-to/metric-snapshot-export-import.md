# Export and restore Metric snapshots

The experimental Metric path exports logical tables through shared physical
scans. Packed snapshots store complete, independent Parquet streams in shared
objects, including tables with different schemas. Restore creates target tables
and inserts through their normal write paths; it does not copy source table IDs
or reuse source physical-table bindings. Keep source DDL, selected data and TTL
effects stable during export; this is not a transactionally consistent online
backup. The server authorizes selected tables and the COPY destination.

Enable `experimental_metric_export = true` in the source frontend or standalone
configuration. Use an upgraded, consistently configured frontend endpoint.
Packed export requires the server's `metric_packed_export: 1` capability;
packed restore requires `metric_packed_import: 1`. The experimental gate remains
in place while the [release acceptance criteria](../rfcs/2026-09-11-metric-export-import.md#integration-and-release-acceptance)
are open.

## Create, inspect and restore

For a standalone server, put local snapshots under its configured
`storage.copy_root`. Both the CLI and the server must be able to access the same
files. Distributed servers require object storage for COPY.

The following example assumes `storage.copy_root = "/srv/greptime/backups"`
on both standalone hosts and that the snapshot is available at that location
on the target. Replace the addresses, schema and time range for your deployment.
The time range is start-inclusive and end-exclusive.

```sh
greptime cli data export-v2 create \
  --addr source:4000 --schemas metrics \
  --to file:///srv/greptime/backups/metrics-snapshot \
  --start-time 2026-10-01T00:00:00Z --end-time 2026-10-02T00:00:00Z \
  --experimental-metric-export --metric-data-layout packed \
  --parallelism 4 --chunk-parallelism 1

greptime cli data export-v2 list \
  --location file:///srv/greptime/backups

greptime cli data export-v2 verify \
  --snapshot file:///srv/greptime/backups/metrics-snapshot

greptime cli data import-v2 \
  --addr target:4000 \
  --from file:///srv/greptime/backups/metrics-snapshot \
  --task-parallelism 1 --state-path ./metrics-import-state.json
```

`list` reports snapshot/chunk status. `verify` checks structural integrity,
including inventory, indexes and object lengths; it is not a checksum of every
value and need not detect same-length data corruption. Validate application
values separately. `import-v2 --dry-run` validates the snapshot and prints the
planned DDL/data work without applying it.

For S3-compatible storage, use `s3://bucket/prefix` for `--to`, `--from`,
`--snapshot` or `--location`, and supply the same storage options to each command:

```sh
--s3 --s3-endpoint https://object-store.example.com \
--s3-region us-east-1 --s3-access-key-id "$S3_ACCESS_KEY_ID" \
--s3-secret-access-key "$S3_SECRET_ACCESS_KEY"
```

The CLI accesses snapshot metadata and the database server accesses data objects.
Both must reach the supplied endpoint. Database authentication is separate:
use `--auth-basic "$DB_USER:$DB_PASSWORD"` on create/import as required.
`--timeout` controls database HTTP requests; an expired timeout does not prove
that the server has stopped its work.

## Compatibility and concurrency

`export-v2` and `import-v2` handle manifest versions 1 and 2. The separate
legacy `cli data export` / `cli data import` commands use their own directory
layout; they are not the readers for these snapshots.

| Snapshot | Export selection | Restore requirements |
| --- | --- | --- |
| Version 1 | Default export, shared scans without `--metric-data-layout packed`, or `--schema-only` | An `import-v2` reader supporting version 1 and the selected Parquet/CSV/JSON format |
| Version 2 | `--experimental-metric-export --metric-data-layout packed`, with data | A version-2 importer and target packed-import capability |

Packed export supports Parquet. Ordinary tables and large logical tables use
standalone objects; empty logical tables retain valid zero-row streams. A pack
stays within one schema and time chunk. Resuming keeps the existing snapshot
version/layout; it does not convert version 1 to version 2. Use a new destination
to change layout. Older `import-v2` readers without version-2 support must reject it before
target DDL; do not restore packed snapshots by listing only `.parquet` files.

The importer discovers `metric_batch_ddl` independently of packed support.
When available, it groups contiguous compatible logical CREATE statements for
one physical table, up to 128 statements and 1 MiB SQL per request. It preserves
dependency order, options and authorization. Targets without this capability
use ordinary DDL, provided they support the snapshot's data layout. Batch DDL
also applies to version-1 and schema-only imports.

Export `--parallelism` bounds server work within each schema/chunk;
`--chunk-parallelism` controls concurrent client chunks. Import
`--task-parallelism` controls concurrent schema/chunk data tasks. The latter two
accept 1–64 and default to 1; import does not expose an export-style
`--parallelism` option. More chunks/tasks multiply request-local resource use.
Start conservatively when many physical groups or wide column unions are involved.

Packed writes and upload parts are at most 8 MiB; entire objects and logical
Parquet streams can be larger. Packed reads retain at most two 8 MiB windows
per COPY request, tied to that request's storage identity. These are payload
budgets, not a bound on process RSS: catalog/index metadata, query plans,
scan/sort buffers, Parquet codecs and transport staging need additional memory.
Distributed ordering and wide schemas can dominate that memory.
See the [native validation results](../rfcs/2026-09-11-metric-export-import/experiment-results.md#scan-repair-and-remaining-wide-schema-limit)
for the tested wide-schema limit and server `query.parallelism` workaround;
that setting is separate from CLI export `--parallelism`.

The shared-scan writer currently permits up to 4,096 row groups per logical
table in each time chunk, with at most 8,192 rows per group. Memory-based flushes
can produce smaller groups. Exceeding the footer metadata budget fails the
export. For larger tables, use a bounded time range and `--chunk-time-window`
with a new destination; keep `--chunk-parallelism` conservative.

## Resume and failures

Re-run `export-v2 create` with the same destination, schema selection, format,
time bounds and chunk window to resume. Keep the experimental flag for packed
snapshots. Completed chunks are retained; incomplete owned output is cleaned
before retry. Unexpected objects are not permission to delete the destination.
Wait until the previous server request and storage writes have ended before
retrying, including after a client timeout. `--force` deletes and recreates the
snapshot; it is not resume.

Re-run `import-v2` with the same snapshot, target identity, schema selection and
state file to resume. The default state file is under `~/.greptime/import_state`;
`--state-path` makes its location explicit. Successfully checkpointed tasks and
completed DDL are skipped, and the state file is removed after success. A failed
data task can already have inserted rows and is replayed as a whole; restore is
not a transaction across tasks or tables.

A failed DDL request can also have applied changes. Earlier successful batches
remain, and an ambiguous batch failure stops the import without automatic
fallback to ordinary SQL or automatic replay. Inspect the target and establish
that outstanding work has ended before explicitly resuming. Invalid snapshot
metadata or missing packed capability must be resolved before retrying.
