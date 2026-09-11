---
Feature Name: Metric Export and Import Optimization
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/9120
Date: 2026-09-11
Status: Draft for discussion
---

# Summary

Group the selected Metric logical tables by physical table. Query each physical
table across all its regions, order the rows by `__table_id`, and project each
logical schema into the existing logical-table Parquet files. Restore the same
V2 snapshot with batched logical-table DDL and the existing data COPY path. A
separate generic COPY fix removes repeated directory scans.

The design preserves `manifest.version = 1` and the current V2 layout. It does
not introduce physical backup files or require importing source table IDs.
Merged data writes remain a first-release scope decision, pending representative
measurements after the COPY fix.

# Motivation and evidence

[Discussion #7394](https://github.com/orgs/GreptimeTeam/discussions/7394)
includes reports of exports taking days for roughly 100,000 logical tables with
only several GiB of data. Table count, physical schema width and data volume must
be measured separately; total database bytes do not describe the transfer cost.

Local experiments support three independent optimizations: shared physical scans,
batched logical CREATE, and removal of repeated file lookup. The
[experiment results](2026-09-11-metric-export-import/experiment-results.md)
contain the measurements, controls and coverage limits. The COPY improvement
uses per-table writes; merged-write benefits remain unmeasured.

# Terminology

| Term | Meaning in this proposal |
| --- | --- |
| Physical table | A Metric Engine table whose data regions store rows for multiple logical tables; its schema includes their column union. |
| Logical table | A user-facing Metric table with its own schema and an association with a physical table. |
| Region | A storage partition of a physical table. A logical table's rows may span regions. |
| Export unit | The selected logical tables sharing one physical table, within one schema and V2 time chunk. This is an export grouping, not a storage object or completion checkpoint. |
| Query execution partition | A stream of work in the query plan. Frontend `target_partitions` influences the partition count; execution partitions are distinct from storage regions and export units. |
| Logical-table descriptor | Captured catalog metadata: source ID, name, physical association, column names/types/order, time index and primary key. Capturing and reusing it provides no snapshot or locking guarantee. |
| Parquet writer | The writer for one logical table's output file in an export unit. |
| Merged writes | Combining insert requests for multiple target logical tables into a region batch dispatch while retaining each table's identity, schema and routing. |

# Physical export and logical files

## Query and routing

Capture the selected logical-table descriptors and reuse them for schema DDL
and data export. For each schema and V2 time chunk, group the selected logical
tables by physical table to form export units. Source schemas, selected data and
TTL effects must stay stable during export; this proposal does not provide a
transactionally consistent online backup.

The server must authorize every selected logical table and the destination
before querying the physical table. Routing filters are not an authorization
boundary.

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
  → reject rows whose IDs are absent from the captured descriptor set
  → contiguous rows for cpu      → project (ts, host, cpu)             → cpu.parquet
  → contiguous rows for requests → project (ts, host, service, requests) → requests.parquet
```

Only one Parquet writer is active per export unit. At an ID transition, close the
previous writer and open the next. Verify non-null UInt32 IDs and monotonic order
across input batches. Create a valid zero-row file for every selected empty table.
Never put `__table_id`, `__tsid`, or another table's columns into logical files.

The captured catalog IDs are the routing whitelist; physical rows are not the
source of table membership. Dropped tables can leave residual rows. Predicate
pushdown for selected IDs/ranges is an optional optimization to evaluate against
residual density and index effectiveness; it does not replace routing checks.

## Physical column union and memory

The physical schema can contain the union of thousands of logical schemas.
Project the scan to `__table_id` plus the union needed by the selected tables.
For each ID run, project again to that logical schema **before** expanding Arrow
dictionary columns. Preserve field order, types, NULLs and empty strings.

Logical projection reduces output conversion but does not eliminate wide scan
batches or upstream sorting. Account separately for retained scan and merge
buffers, conversion, codec reservations, row-group metadata and object-store
buffers. Bound concurrent export units as well as each unit's work.

Input checks after a batch arrives cannot cap the scan's allocation peak, and a
writer flush threshold does not cap codec allocations. Combine schema validation
with query memory accounting, bounded conversion slices, row-group limits and
cancellation. Reject an oversized row explicitly. These controls do not provide
a hard process RSS guarantee.

## Ordering and multiple regions

The first release covers every selected Metric physical table, any region count,
and the Metric column types supported by existing Parquet COPY. Issue one query
per export unit covering all regions of its physical table. The query engine
provides globally ordered results through `ORDER BY __table_id`, including any
sorting and merging needed across datanodes. The exporter consumes that stream
without implementing its own region scheduler.

In the current implementation, `MergeScanExec` declares the requested ordering
only when `output_partition_count >= regions.len()`. When the region count
exceeds frontend `target_partitions`, the frontend may need an additional sort
to satisfy SQL ordering. That sort operates on the selected column union before
per-logical-table projection. Parquet writer limits and downstream conversion
slices cannot bound its memory or spill cost.

SQL ordering is the contract. Keep the runtime ID-monotonicity check, but do not
reject a valid plan because it contains `SortExec`. If query resources are
exhausted, cancel the query, fail the chunk and retain retry semantics. Region
count or resource exhaustion must not silently switch an enabled physical
export to per-logical-table queries. The
[ordering acceptance gate](#ordering-acceptance-gate) is required before release.

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

# Restore path

## Batched logical DDL

Parse the existing DDL and batch contiguous logical CREATE statements sharing
catalog, schema and the literal `on_physical_table` value. A dot in that option
does not make it a qualified identifier. Database, physical, ordinary and view
statements are barriers and retain their order.

Use the existing logical-table batch procedure. Process bounded batches
sequentially and validate each batch before submission. Preserve normal CREATE
validation, schema-option inheritance and target metadata resolution. Bound the
request size and the importer's DDL buffering separately.

The production CLI needs an authenticated server operation that checks CREATE
authorization for every member and reuses the ordinary DDL path. Transport,
capability discovery and request limits must be agreed before accepting this
interface. An older server uses ordinary DDL; do not retry an ambiguously
completed batch through another path.

## Data COPY and generic directory fix

Read each logical Parquet file through the current COPY path and write through
normal target logical-table routing. No source-ID remapping belongs in these
files, and batch DDL does not imply batched data insertion.

The generic fix changes explicit-file lookup from `stat → list parent → find`
to `stat → use path`, with a sandboxed file-type check for explicit local
inputs to preserve symlink filtering. Directory COPY retains listing, pattern
matching and file-type filtering. Backend authorization and error handling
remain in place. With N files in one directory, this removes roughly N²
directory-entry work.
COPY bypasses the common Lister's filename branch; the branch itself is unchanged.
This fix can ship independently of the Metric design.

## Completion, interruption and retry

Keep V2 completion units: export chunk completion and import `(chunk, schema)`
data tasks, with the existing DDL-completed flag. Finish and close every required
file before recording completion. A closed subset of files is not a completed
chunk. On export retry, replace only the unfinished attempt's owned outputs;
never touch completed chunks. Cancellation must close/abort writers and leave
the chunk incomplete.

Batch DDL is not a transaction across the restore. A later failure can leave
earlier batches created. Keep DDL completion false until all statements succeed,
then use the current durable state update. Data retries retain existing COPY
semantics and may replay an incomplete task; there is no new exactly-once
guarantee or per-file checkpoint in this proposal.

# Rationale and alternatives

| Choice | Reason and trade-off |
| --- | --- |
| Keep logical-table Parquet files | Reuses V2 DDL, file lookup and normal target CREATE/routing with fresh IDs. Physical backup files would need a new restore contract for mixed schemas and source metadata. The retained format still has one file per logical table per time chunk. |
| Scan by physical table | Shares query and scan work among logical tables. Per-logical-table COPY is simpler but retains the table-count overhead; physical scans instead pay for the selected column union and ID routing. |
| Global ID ordering and one Parquet writer per export unit | Contiguous IDs allow a file to be closed at each table transition. Unordered routing would require many open writers or intermediate buffering/files. Ordering bounds writer state, but query sorting and merging need separate resource controls. |
| Use query-engine ordering across regions | Reuses distributed planning and SQL semantics. An exporter-owned region scheduler would add merge, cancellation and retry coordination. The chosen approach must handle both streaming merge and additional-sort plans. |
| Batch DDL while retaining per-table data COPY | Reuses logical CREATE procedures and current data-task replay semantics. Merged writes may reduce dispatch/WAL overhead, but introduce partial-success and checkpoint questions and require a separate benefit comparison. |

# Unresolved questions

- Which authenticated batch-DDL operation and capability-discovery mechanism
  should the CLI use? Agree request byte/table limits and DDL buffering bounds.
- What are the query memory, spill-space, writer and export-unit concurrency
  limits? Agree the test workloads and resource/performance thresholds before
  running the ordering acceptance benchmark.
- Which released import-v2 versions and object-store backends are compatibility
  targets? Test those readers against the retained V2 format.
- What name and defaults should the `experimental_` export option use?
- Should merged writes ship in the first release? Compare a bounded merged-write
  PoC against corrected per-table COPY with the same batch DDL and concurrency.
  Bound decoded rows/bytes and in-flight requests, preserving normal logical
  validation and target routing through the existing region batch dispatch.
  Cover small/larger files, wide schemas and distributed/multi-region targets;
  report repeated total/data times, rows/s, bytes/s, request counts and memory.
  Agree a minimum useful gain and memory ceiling before the comparison. Validate
  partial success, ambiguous completion and checkpoint advancement, then record
  an explicit include/defer decision.

# Integration and release acceptance

Use existing COPY DATABASE orchestration to form Metric export units. Retain
ordinary COPY for non-Metric tables and other formats, and direct COPY TABLE
behavior. Gate the unfinished Metric fast path behind an `experimental_`
configuration option; disabling it selects the existing path.

The [tracking issue](https://github.com/GreptimeTeam/greptimedb/issues/9120) owns
the PR breakdown and implementation dependencies. Each implementation slice
must verify its own correctness and failure behavior. Release acceptance requires:

| Area | Required evidence |
| --- | --- |
| Export | Actual CLI export with authorization, multiple physical tables and regions, dense/sparse keys, empty/residual tables, supported Metric Parquet types, and logical schema/value equivalence. |
| Restore | Authenticated remote batch DDL, per-table authorization, fresh target IDs and reconstructed metadata, dependency ordering, older-server behavior and batch failure/retry. |
| V2 lifecycle | Supported released readers; local and object-store interruption, owned-output cleanup, durable completion state and import replay. |
| Scale | Repeated release-build measurements varying table count up to approximately 100,000, rows/file, physical union width, regions and backend. Report median/spread, control execution order/cache, and include schema export and metadata discovery in end-to-end timing. |
| Open decisions | Agreed batch-DDL interface, resource limits, compatibility targets and experimental option; documented merged-write include/defer decision. |

## Ordering acceptance gate

The first release must pass a distributed export benchmark with region counts
**below, equal to and above frontend `target_partitions`**, using representative
row counts and narrow/wide selected column unions. Record actual execution
partition counts rather than assuming the configured target determines the plan.
Include multiple physical tables and a logical table whose rows span regions.
Exercise both streaming-merge and additional-sort plans.

For each case, report frontend and datanode execution plans, peak memory,
query memory-accounting metrics, spill volume and elapsed time. Run with the
agreed query memory, spill-space, writer and concurrency limits. Verify global
ID order and exactly one complete file per selected logical table in each export
unit, checking schemas and typed rows outside the timed interval.

Passing requires correct exports within the agreed resource/performance
thresholds on both plan paths. Also force resource exhaustion: cancellation
must leave the chunk incomplete, and retry with sufficient resources must
produce complete files without altering completed chunks. Writer-only memory
measurements and single-region runs cannot satisfy this gate. Until both the
normal and failure/retry cases pass, multi-region resource acceptance remains
open.
