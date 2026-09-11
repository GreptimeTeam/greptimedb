---
Feature Name: Metric Export and Import Optimization
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/9120
Date: 2026-09-11
Status: Draft for discussion
---

# Summary

Export every selected Metric physical group, including multi-region tables,
through an ordered physical-table query, route rows by `__table_id`, and write the existing logical-table Parquet
files. Restore the same V2 snapshot with batched logical-table DDL and the existing
data COPY path. A separate generic COPY fix removes repeated directory scans.

The design preserves `manifest.version = 1` and the current V2 layout. It does
not introduce physical backup files or require importing source table IDs.
Merged data writes remain a first-release scope decision, pending representative
measurements after the COPY fix.

# Motivation and evidence

[Discussion #7394](https://github.com/orgs/GreptimeTeam/discussions/7394)
includes reports of exports taking days for roughly 100,000 logical tables with
only several GiB of data. Table count, physical schema width and data volume must
be measured separately; total database bytes do not describe the transfer cost.

Three local PoC slices establish different improvements:

| Change | Experiment | Observation | Evidence |
| --- | --- | --- | --- |
| Physical scan and logical projection | 64 populated logical tables, 1,000 rows each; narrow/wide physical unions; two execution orders | Data export 2.82–3.39x faster | [Export results](2026-09-11-metric-export-import/experiment-results.md#physical-group-export) |
| Batch logical DDL | 10,000 logical tables; SQL HTTP, in-process single, in-process batch | Logical CREATE: 29.181 / 19.524 / 5.776 s; calls: 10,000 / 10,000 / 79 | [DDL results](2026-09-11-metric-export-import/experiment-results.md#batched-logical-ddl) |
| Remove repeated directory scans | 10,000 files; batch DDL in both variants | Data COPY: 146.496 → 5.481 s; total restore: 153.647 → 12.300 s | [COPY results](2026-09-11-metric-export-import/experiment-results.md#copy-file-lookup) |

The restore fixture has two rows per populated table and one empty table:
19,998 rows at the 10,000-table scale. These are local standalone, warm-cache,
unoptimized builds on an ARM64 machine with 16 GiB RAM. Each restore variant/scale
has one measured run. The comparisons are separate experiments; their speedups
must not be multiplied or presented as one controlled end-to-end result.

The 5.481 s result still writes each logical table separately. A disabled-probe
control took 5.449 s. It is evidence about small-file overhead, not merged writes
or production ingestion throughput.

# Physical export and logical files

## Query and routing

A physical group consists of one physical table and the logical tables selected
for export that share it.

For each selected schema and V2 time chunk, freeze the logical-table descriptors
and group them by physical table. Resolve each descriptor from the catalog:
source table ID, name, physical association, column names/types/order, time index
and primary key. Use the same descriptor set to produce schema DDL and data.
The server must authorize every selected logical table and the destination
before accessing the physical group; routing filters are not an authorization
boundary. The current internal PoC assumes a trusted caller.

For example, suppose `cpu` has `(ts, host, cpu)` and `requests` has
`(ts, host, service, requests)`, sharing `public.phy`. The physical query is
equivalent to:

```sql
SELECT __table_id, ts, host, service, cpu, requests
FROM public.phy
WHERE ts >= '2026-09-01T00:00:00Z'
  AND ts <  '2026-09-02T00:00:00Z'
ORDER BY __table_id;
```

The implementation builds this plan through the existing query engine, using
COPY's time-filter semantics and properly resolved columns. It reads the
physical table directly; it does not first write and reread `phy.parquet`.

```text
physical table query: selected column union + __table_id, ordered by ID
  → reject rows whose IDs are absent from the frozen logical descriptor set
  → contiguous rows for cpu      → project (ts, host, cpu)             → cpu.parquet
  → contiguous rows for requests → project (ts, host, service, requests) → requests.parquet
```

Only one logical writer is active per group. At an ID transition, close the
previous writer and open the next. Verify non-null UInt32 IDs and monotonic order
across input batches. Create a valid zero-row file for every selected empty table.
Never put `__table_id`, `__tsid`, or another table's columns into logical files.

The frozen catalog IDs are the routing whitelist; physical rows are not the
source of table membership. Dropped tables can leave residual rows. Predicate
pushdown for selected IDs/ranges is an optional optimization to evaluate against
residual density and index effectiveness; it does not replace routing checks.

## Physical column union and memory

The physical schema can contain the union of thousands of logical schemas.
Project the scan to `__table_id` plus the union needed by the selected tables.
For each ID run, project again to that logical schema **before** expanding Arrow
dictionary columns. Preserve field order, types, NULLs and empty strings.

This reduces output conversion but does not eliminate wide incoming batches.
The PoC's narrow/wide comparison kept expanded output near 0.148 MiB while input
array estimates grew from 0.267 to about 2.4 MiB. Account separately for retained
scan buffers, merge inputs, conversion, codec reservations, row-group metadata
and object-store buffers. Bound concurrent groups as well as per-group work.

Input checks after a scan batch arrives cannot cap the scan's allocation peak.
Likewise, a writer flush threshold is not a hard allocation limit: the measured
timestamp encoder can reserve about 1 MiB even for a small batch. Production
resource control must combine schema validation with query memory accounting,
bounded conversion slices, row-group limits and cancellation. Reject an oversized row
explicitly. Do not advertise these limits as a process RSS guarantee.

## Ordering and multiple regions

The first release must export every selected Metric physical table, regardless
of its region count. Issue one query per physical group and time chunk covering
all its regions. The query engine provides globally ordered results through
`ORDER BY __table_id`, including any sorting and merging needed across datanodes.
The exporter consumes that ordered result stream; it does not concatenate
independently read region streams or implement its own region scheduler.

The implementation constructs the SQL-equivalent logical plan on the server.
The contract is SQL ordering semantics, not a whitelist of physical operators.
Keep the runtime ID-monotonicity check as a defensive assertion, but do not
reject a valid query because its plan contains `SortExec`. If query resources
are exhausted, fail the chunk and retain retry semantics; do not silently switch
to per-logical-table export. Performance and resource behavior still require
multi-region measurement before release.

The existing PoC has only validated one physical region and Float64 fields,
string tags and timestamps. These are evidence limits, not first-release scope.
Extend implementation and tests to multiple physical groups, multiple regions,
distributed execution and the Metric column types supported by existing Parquet
COPY. A per-logical-table fallback does not satisfy physical-export acceptance.

# Snapshot and metadata contract

Retain the current implementation's layout:

```text
manifest.json                         # version 1
schema/schemas.json                   # schema index
schema/ddl/public.sql                 # canonical database/table/view DDL
data/public/1/cpu.parquet
data/public/1/requests.parquet
```

Schema path segments retain existing V2 encoding. Logical data filenames retain
COPY's raw table-name semantics and import's `file_stem` matching, including dots.
There is no existing general table-filename escaping contract to reuse. Reject
path separators for the fast path; changing that contract requires a separate
compatibility design.

Keep database, physical-table, logical/ordinary-table and view DDL in dependency
order. Physical DDL describes the table and its options; logical DDL describes
each logical schema and its physical association. Normal target CREATE procedures
reconstruct Metric data/metadata regions and logical metadata with newly allocated
IDs. Do not copy hidden metadata-region records or source IDs as restore data.
Verify effective options, column mappings and physical routes after recreation.

The unmodified import-v2 command used in the experiment restored PoC files into a fresh target
with different IDs. This proves compatibility with that code path, not with all
released binaries. Pin and test actual supported release versions before claiming
cross-version compatibility. The older V2 RFC describes some structures that
differ from today's code; this RFC follows the current DDL-based implementation.

# Restore path

## Batched logical DDL

Parse the existing DDL and batch contiguous logical CREATE statements sharing
catalog, schema and the literal `on_physical_table` value. A dot in that option
does not make it a qualified identifier. Database, physical, ordinary and view
statements are barriers and retain their order.

Use the existing logical-table batch procedure. The PoC limits each call to 128
tables and 1 MiB of rendered SQL, processes batches sequentially, and validates
the group before submission. Preserve normal CREATE validation, schema-option
inheritance and target metadata resolution. These request bounds do not bound
the importer's whole DDL list in memory.

The current fast executor is in-process and test-only. A production CLI needs an
authenticated, bounded server operation that checks CREATE authorization for
every member and reuses the ordinary DDL path. The SQL-versus-in-process control
separates transport savings from procedure batching; the measured in-process
latency is not a promised batch HTTP latency. Exact transport/capability discovery
must be agreed before this part of the RFC is accepted. An older server uses
ordinary DDL; do not retry an ambiguously completed batch through another path.

## Data COPY and generic directory fix

Read each logical Parquet file through the current COPY path and write through
normal target logical-table routing. No source-ID remapping belongs in these
files, and batch DDL does not imply batched data insertion.

The generic fix changes explicit-file lookup from `stat → list parent → find`
to `stat → use path`. Directory COPY retains listing, pattern matching and
file-type filtering. Backend authorization and error handling remain in place.
With N files in one directory, this removes roughly N² directory-entry work.
COPY bypasses the common Lister's filename branch; the branch itself is unchanged.
This fix can ship independently of the Metric design.

## Completion, interruption and retry

Source schemas, selected data and TTL effects must stay stable during export.
This proposal does not provide a transactionally consistent online backup.

Keep V2 completion units: export chunk completion and import `(chunk, schema)`
data tasks, with the existing DDL-completed flag. Finish and close every required
file before recording completion. A closed subset of files is not a completed
chunk. On export retry, replace only the unfinished attempt's owned outputs;
never touch completed chunks. Cancellation must close/abort writers and leave
the chunk incomplete. Local and object-store failure tests must establish this
behavior before production integration; the PoC currently leaves a partial
directory and does not implement publication or resume.

Batch DDL is not a transaction across the restore. A later failure can leave
earlier batches created. Keep DDL completion false until all statements succeed,
then use the current durable state update. The PoC verifies failure after a
successful 128-table batch and retry of the original DDL. Data retries retain
existing COPY semantics and may replay an incomplete task; there is no new
exactly-once guarantee or per-file checkpoint in this proposal.

# Merged writes: first-release decision

A candidate importer could decode several logical files under row/byte/in-flight
bounds, resolve each target table and send multiple logical insert requests in
one existing region batch dispatch. Each request must preserve its target logical
identity, schema and routing; bypassing logical validation with raw physical
writes is not proposed. The data files can remain unchanged.

This may reduce dispatch/WAL overhead, but the present profile does not isolate
WAL, locks or RPC counts and cannot predict that benefit. Partial batch success,
ambiguous completion, retries and checkpoint advancement need explicit tests.

Before deciding first-release scope, compare the corrected per-table path with
a bounded merged-write PoC under the same batch DDL and concurrency. Include
small files, larger files, wide schemas and distributed/multi-region targets.
Report repeated total-restore and data-phase measurements, rows/s, bytes/s,
request counts and memory. Agree a minimum useful gain and memory ceiling before
running the comparison. Include merged writes only if gains justify the added
recovery complexity; otherwise record the evidence and defer them explicitly.

# Delivery and acceptance

The [implementation plan](2026-09-11-metric-export-import/metric-export-import-tracking.md) defines
seven required PRs and one conditional merged-write PR. Production integration
remains pending. The [experiment summary](2026-09-11-metric-export-import/experiment-results.md)
contains the supporting comparisons and conclusions.

| Slice | Acceptance boundary |
| --- | --- |
| Generic COPY fix | Isolated change; explicit-file and directory semantics; local and object-store regression coverage |
| Physical export | All selected Metric physical groups; logical schema/value equivalence, dense/sparse keys, empty/residual tables, existing Metric Parquet types and memory controls; production COPY integration and authorization |
| Batch DDL | Agreed transport, per-table authorization, remote CLI round trip, dependency order, batch failure/retry |
| V2 lifecycle | Real release-reader compatibility, interrupted export cleanup/retry, import replay, local and object-store verification |
| Multi-region and scale | Multiple physical groups and regions, including distributed execution; global ordering and complete logical files below/at/above partition budget; resource measurements and failure/retry |
| Merged-write decision | Controlled comparison and documented include/defer decision with retry semantics |

Proposed integration uses existing COPY DATABASE orchestration to group Metric
tables and retain ordinary COPY for non-Metric tables and other formats.
Keep direct COPY TABLE behavior unchanged. Gate the unfinished Metric fast path
behind an `experimental_` configuration option, with its exact name and defaults
reviewed alongside the integration. Disable it to select the existing path.
With Metric Parquet export enabled, every selected physical group uses the
physical-query path. Region count is not a fallback condition.

Benchmarks must vary table count (including a scale near the discussion's 100,000),
rows/file, physical union width, regions and backend independently. Use release
builds and repeated runs, report median and spread, control execution order/cache,
and verify all schemas and typed rows outside timing. Measure schema export and
metadata discovery too: they still perform per-table work and are excluded from
the current data-export timings.

Remote API, object-store, failure/recovery and distributed acceptance, together
with the transport decision, remain required before the Metric path is presented
as production-ready.

# Appendix: observed query plans

The measured single-region plans use `SeriesScan` and
`SortPreservingMergeExec`, with no `SortExec`: `__table_id` matches a physical
primary-key prefix. This explains the PoC behavior without assuming an external
sort on every export.

In the inspected code, `MergeScanExec` declares ordering only when
`output_partition_count >= regions.len()`. Outside that condition the frontend
may need additional sorting to satisfy SQL ordering. Use this distinction to
design performance experiments and investigate resource costs, not to limit
which physical tables can be exported. These operator details describe current
implementation behavior and are not part of the export interface contract.
