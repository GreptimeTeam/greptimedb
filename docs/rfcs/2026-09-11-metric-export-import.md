---
Feature Name: Metric Export and Import Optimization
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/9120
Date: 2026-09-11
Updated: 2026-09-21
Status: Draft for discussion
---

# Summary

Scan Metric tables by physical group, project each logical schema, and pack
small, complete logical-table Parquet streams into shared objects. An index maps
each table to its object and byte range. Restore through a bounded range reader
and normal target-table insertion, with batched logical-table DDL.

Packing is required for the first release of this optimization. Packed snapshots
use `manifest.version = 2`; new importers read versions 1 and 2, while old
importers reject version 2 before executing DDL. The command names remain
`export-v2` and `import-v2`. Existing version-1 snapshots retain their layout.

Each storage write and multipart data part is at most 8 MiB. Objects can be
larger, and a logical table is not split to meet that write size. Shared physical
scans, bounded writer concurrency, removal of redundant object checks, schema
export batching and batch CREATE together address the complete transfer cost.
Merged database insert requests remain a separate follow-up.

# Motivation and evidence

[Discussion #7394](https://github.com/orgs/GreptimeTeam/discussions/7394)
includes reports of exports taking days for roughly 100,000 logical tables with
only several GiB of data. Table count, physical schema width and data volume must
be measured separately; total database bytes do not describe the transfer cost.

The [experiment conclusions](2026-09-11-metric-export-import/experiment-results.md)
support shared scans, batch DDL, explicit-file COPY and packed-object transfer.
Packing with a bounded range reader substantially reduces storage requests and
improves full export and restore for many small logical tables. Benefits depend
on request latency, and memory use increases. Remaining CREATE work makes batch
DDL part of the first release. These local experiments establish direction;
production V2 recovery and release performance still require acceptance tests.

# Terminology

| Term | Meaning in this proposal |
| --- | --- |
| Physical table | A Metric Engine table whose data regions store rows for multiple logical tables; its schema includes their column union. |
| Logical table | A user-facing Metric table with its own schema and an association with a physical table. |
| Region | A storage partition of a physical table. A logical table's rows may span regions. |
| Export unit | The selected logical tables sharing one physical table, within one schema and V2 time chunk. This is an export grouping, not a storage object or completion checkpoint. |
| Query execution partition | A stream of work in the query plan. Frontend `target_partitions` influences the partition count; execution partitions are distinct from storage regions and export units. |
| Logical-table descriptor | Captured catalog metadata: source ID, name, physical association, column names/types/order, time index and primary key. Capturing and reusing it provides no snapshot or locking guarantee. |
| Parquet writer | An encoder for one complete logical-table Parquet stream, including its schema and footer. |
| Pack | An object containing consecutive complete Parquet streams, confined to one schema and time chunk. |
| Pack index | The table-to-object/range mapping and object lengths for one schema and time chunk, including standalone objects. |
| Merged writes | Combining insert requests for multiple target logical tables into a region batch dispatch while retaining each table's identity, schema and routing. |

# Physical export and logical Parquet streams

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
  → contiguous cpu rows      → project (ts, host, cpu)              → cpu Parquet
  → contiguous requests rows → project (ts, host, service, requests) → requests Parquet
  → append complete small Parquet streams to a shared pack and record their ranges
```

The router sends each table's contiguous batches to one encoder. At an ID
transition, finish that encoder's input; earlier encoders may finish concurrently
under a request-wide writer budget. Append only complete Parquet streams, so
completion order need not match source table-ID order. Verify non-null UInt32 IDs
and monotonic input order across batches. Encode a valid zero-row Parquet for
every selected empty table. Never include `__table_id`, `__tsid`, or another
table's columns in a logical Parquet stream.

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

Start with a 64 MiB request-wide budget for retained/queued Arrow payload and an
8 MiB encoded small-table buffer per admitted writer. When a stream outgrows that
buffer, flush its prefix into a standalone object and continue streaming the
same Parquet file. Bound encoder and upload concurrency by the request's
parallelism, shared across physical groups and ordinary-table work. Count shared
Arrow backing allocations and buffers retained by in-flight tasks, not only the
visible slice sizes. Codec and query reservations remain separate controls.

Finishers retain their writer permits until their buffers are appended or
released. This prevents completed small files from forming an unbounded queue.
Each upload submission is at most 8 MiB, including standalone files and indexes.
CLI chunk concurrency multiplies per-request budgets and must be included in
resource sizing. Index and catalog memory scale with table count and are measured
separately. These initial budgets require the release resource tests below.

## Schema export and destination checks

Reuse an HTTP client and retrieve SHOW CREATE results in bounded multi-statement
requests, initially at most 128 statements and 128 KiB of SQL. Preserve statement
order, identifier quoting, per-statement errors and authorization. Respect response
limits and reject an oversized single request rather than bypassing the byte cap.
This reduces round trips without changing database/physical/logical/view DDL order.

Reuse destination validation only within the same export request and exclusively
owned chunk directory. Check the destination before admission, and preserve
existing collision handling and local-file access rules for independent COPY
operations. An environment switch that globally skips HEAD is not the production
interface. A preflight check alone does not provide ownership against a live
overlapping exporter.

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

## Versions and capability checks

Packed Metric exports write manifest version 2 with a required data-layout marker
`metric-parquet-packs`. Parquet remains the encoding of each table. Ordinary
exports can keep version 1. A version-2 snapshot indexes all data in its schema
chunks, including ordinary tables stored as standalone Parquet objects.

Before creating or replacing a snapshot, the CLI checks the source server's
explicit packed-export capability. Before target DDL or data writes, the importer
checks the manifest version/layout and the target's packed-import capability.
An unsupported packed layout fails; it must not fall through to listing `.parquet`
files and silently omit packed tables. Batch-DDL capability is checked separately.

| Reader and snapshot | Behavior |
| --- | --- |
| New importer, version 1 | Use the existing file layout and normal restore semantics. |
| New importer, version 2 | Require the packed layout and a target supporting packed import. |
| Old importer, version 2 | Reject the unsupported manifest version before DDL or data writes. |
| Unknown version or layout | Reject explicitly. |

Update create, resume, verify, status and import together. Resume retains the
snapshot's original version and layout; it does not migrate an unfinished
version-1 snapshot. Validate old-reader rejection with supported release binaries.

## Manifest and chunk schema

The following is a complete version-2 manifest example. Field names and enum
values are part of the persisted contract; the timestamps and object names are
illustrative.

```json
{
  "version": 2,
  "data_layout": "metric-parquet-packs",
  "snapshot_id": "123e4567-e89b-42d3-a456-426614174000",
  "catalog": "greptime",
  "schemas": ["public"],
  "time_range": {"start": "2026-09-01T00:00:00Z", "end": "2026-09-02T00:00:00Z"},
  "schema_only": false,
  "format": "parquet",
  "chunks": [{
    "id": 1,
    "time_range": {"start": "2026-09-01T00:00:00Z", "end": "2026-09-02T00:00:00Z"},
    "status": "completed",
    "files": [
      "data/public/1/pack-000000.bin",
      "data/public/1/table-000000.parquet",
      "data/public/1/pack-index.json"
    ]
  }],
  "created_at": "2026-09-19T00:00:00Z",
  "updated_at": "2026-09-19T00:01:00Z"
}
```

All top-level and chunk fields shown are required in version 2. `snapshot_id`
is a UUID; timestamps are RFC3339; `time_range` retains the existing half-open interval semantics,
with an absent or null bound meaning unbounded on that side. Schema names are
literal identifiers and must be unique. Chunk IDs are unique positive `u32`
integers. Chunk statuses remain `pending`, `in_progress`, `completed`, `skipped`
and `failed`; a failed chunk may carry an optional string `error`.
`schema_only = true` requires an empty `chunks` array and no data objects;
schema-only exports continue to produce version 1.

For version 1, an absent or null `data_layout` selects the existing layout and
all existing field defaults remain valid. Version 2 requires the exact non-null
`data_layout` above and `format = "parquet"`. Reject a layout marker on version
1, a missing or unknown version-2 layout, an unsupported version, or an
incompatible format before DDL, data writes or resume cleanup. Unknown manifest
and chunk fields may be ignored as optional metadata; a required decoding or
integrity feature must change the manifest version, not rely on such a field.

For a completed version-2 chunk, `files` is the duplicate-free, order-independent
inventory of **all** schema indexes, pack objects and standalone objects in that
time chunk. It contains snapshot-relative paths using `/` on every platform.
For each schema, its subset must equal `pack-index.json` plus every object named
by that index, under `data/<encoded-schema>/<chunk-id>/`. No other files belong
in this list, and DDL stays under `schema/`. Require canonical paths with no
leading slash, backslash or traversal; use the existing schema path encoding.
A filename extension never selects the layout or identifies a table.

Every completed schema chunk has an index, including an empty index when its
DDL selects no data-bearing tables. A selected table with no rows still has a
zero-row Parquet entry. A chunk may be `skipped` with `files = []` only when
none of its schemas selects a data-bearing table, not merely when a time window
contains no rows. Pending, in-progress and failed chunks are never importable;
their files or index presence cannot establish completion.

## Objects and index

```text
manifest.json                         # version 2, metric-parquet-packs layout
schema/schemas.json
schema/ddl/public.sql
data/public/1/pack-000000.bin
data/public/1/table-000000.parquet      # large logical or ordinary table
data/public/1/pack-index.json
```

Schema and chunk paths retain V2 encoding and final-directory placement. Object
names are generated within the chunk; table identity comes from the index, not
from a filename or a source ID. A dotted table name remains a literal identifier.
Version-1 file naming and `file_stem` matching remain unchanged.

The matching `pack-index.json` has this shape; the lengths and row counts below
illustrate the mapping rather than an actual encoded fixture:

```json
{
  "version": 1,
  "objects": [
    {"path": "pack-000000.bin", "kind": "pack", "length": 4096},
    {"path": "table-000000.parquet", "kind": "parquet", "length": 8192}
  ],
  "tables": [
    {"table_name": "cpu", "object": "pack-000000.bin", "offset": 0, "length": 1536, "row_count": 10},
    {"table_name": "requests", "object": "pack-000000.bin", "offset": 1536, "length": 2560, "row_count": 20},
    {"table_name": "events", "object": "table-000000.parquet", "offset": 0, "length": 8192, "row_count": 100}
  ]
}
```

The index version is independent of the manifest version and starts at 1.
All shown fields are required. Reject unknown index versions, fields or object
kinds. `length`, `offset` and `row_count` are unsigned `u64` integers; offsets
are zero-based bytes. Object paths are direct chunk children matching
`pack-[0-9]+.bin` or `table-[0-9]+.parquet` according to kind. The `object` field
references an object by its exact path. Table names are literal identifiers.
A standalone entry covers its entire object. Empty tables still have a nonempty,
valid zero-row Parquet stream. Each selected table has exactly one entry per
schema chunk; an empty index has `objects = []` and `tables = []`.

Validate unique object paths and table names, object kinds/references, unsigned
range arithmetic, bounds, and nonoverlapping complete Parquet ranges. For a pack,
the ranges cover its bytes consecutively; standalone objects have one full-range
entry. Restrict object paths to generated direct children of the chunk directory;
reject absolute paths, separators and traversal. Resolve destination tables only
within the request's authorized catalog/schema. An index is not an authorization
grant. Match index membership to the snapshot table DDL, excluding Metric
physical tables and views, so a missing entry cannot silently omit a table.
Validate the index structure and membership before admitting writes, each stream's
Parquet schema through normal COPY checks, and its decoded row count against
the index before reporting that table complete.

Use one streaming pack writer per schema chunk initially. Multiple physical
groups can feed it, with bounded multipart upload concurrency. Append each small
Parquet intact. A pack may exceed 8 MiB; 8 MiB limits one write/part, not the object.
Roll to a new pack at a table boundary before exhausting a backend object/part
limit. A standalone table is subject to both the backend object-size limit and
its multipart part-count limit. With at most N parts and an 8 MiB part cap, its
effective size ceiling is no greater than min(object-size limit, N × 8 MiB), and
can be lower if emitted parts are smaller. Before submitting an out-of-limit
part or exceeding the object limit, fail explicitly, abort the upload and leave
the chunk incomplete. Do not enlarge parts or split the logical table to bypass
these limits. Exercise this boundary with a reduced-part-limit test backend.

Record object lengths while writing and include all standalone outputs in the
index. Restore need not list the directory or rediscover a pack's size for each
table. Stream index serialization in bounded writes; account for retained table
and index metadata independently of data buffers.

## Validation and checksum coverage

Before target DDL, import validates the manifest contract and, for every selected
schema and completed chunk, loads the required index, matches its inventory to
`files`, validates ranges and DDL membership, and checks object existence and
length once per object. Perform the same preflight on import resume, even when
local state records DDL or data tasks as completed. A malformed snapshot must
not reach the DDL executor or create new completed-task records.

Version 2 retains the existing optional `checksum` fields at manifest and chunk
level, but writers omit both. Readers accept absent or null values and reject
non-null values as unsupported before DDL or resume cleanup. The existing
version-1 exporter does not compute these fields and `verify` does not validate
cryptographic checksums; its behavior remains unchanged. This version-2 contract
therefore provides no checksum coverage for the manifest, DDL, indexes or data
objects. Object-store ETags are not substituted for content checksums. Adding
checksums requires a versioned definition of the algorithm, covered bytes and
aggregation order before readers may claim to verify them.

For version 2, `verify` applies the same metadata/index checks across all schemas,
checks completion and object lengths, and reports missing or unexpected data
files. Its result establishes structural completeness, not content-hash or
full row validation. Import additionally validates each Parquet stream and its
decoded row count through COPY; a same-length payload change can escape the
structural check. Detecting corrupt Parquet data during restore may leave partial
target writes, as described in the retry contract below.

## DDL and target metadata

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
interface. Initially bound a batch to 128 statements and 1 MiB of SQL, with an
explicit oversized-statement error; separately bound the importer's parsed DDL
buffer. A target without batch-DDL capability uses ordinary DDL if it supports
the snapshot's data layout. Do not retry an ambiguously completed batch through
another path.

## Packed data COPY

The CLI retains one `COPY DATABASE FROM` operation per `(chunk, schema)` task.
Add an explicit `metric_data_layout = 'packed'` COPY option, paired with
`FORMAT = 'parquet'`, for the advertised capability. The server validates the
index and selected destination tables, then schedules table streams by object
and offset. Use the same explicit layout option for packed export. The existing
Metric feature gate continues to control the experimental export path.

A reader owned by that request translates each Parquet-relative read into a
bounded object range. Start with two shared 8 MiB read windows per request;
account for windows pinned by decoders before fetching replacements. Bound active
decoders and decoded batches separately. Large standalone tables stream through
the existing reader rather than being loaded into a small-file buffer.

Reuse adjacent bytes for multiple tables and coalesce duplicate window fetches.
The cache is scoped to this request, authorized storage backend and snapshot;
it is not a process-global cache keyed only by object name. Use index lengths
and perform any necessary existence/size checks once per object, not per table.
Check short reads and Parquet schema/metadata errors normally. Objects must stay
stable for the duration of restore.

Reuse COPY's schema validation, Parquet decoding and normal target-table insertion.
Preserve per-table authorization for all admitted entries and backend/local-file
access controls. Keep current schema selection. Per-table selection is a separate
future interface. Pack order changes storage access, not target table routing.

## Version-1 COPY

Keep the explicit-file optimization from #9126: `stat → use path`, including
the sandboxed local file-type check. Directory COPY retains listing, matching
and filtering. New importers continue to use this path for version-1 snapshots.
General removal of redundant restore metadata requests benefits that path too
and must be included in the baseline when measuring packing's incremental gain.

## Completion, interruption and retry

Keep export chunk completion, import `(chunk, schema)` data tasks and the existing
DDL-completed flag. Write data directly to final chunk paths. Close every pack
and standalone object, then finish the schema chunk's index. Only after all
required schema outputs and indexes succeed may V2 mark the chunk Completed.
Index presence alone does not establish completion; a failed manifest update
leaves the chunk incomplete.

On failure, stop admission, cancel and drain started work, and abort unfinished
multipart uploads. Explicit retry cleans only owned outputs of an incomplete
chunk, including pack/index files, and reruns that chunk. Preserve completed
chunks and unrelated objects. A transport timeout does not establish that the
server stopped: require the existing stable-source and stopped-writer conditions
before cleanup/retry. Handle normal cancellation's multipart abort separately
from abandoned multipart uploads after a process crash; listing final objects
does not find the latter.

The importer records a data task complete only after every indexed table succeeds.
Disable continue-on-error for this path: skipped failed tables must not produce a
successful task result. Partial target writes may remain after failure. Retry
retains existing COPY replay semantics, without per-table checkpoints or an
exactly-once guarantee. Cancel and drain active readers/inserts before returning
failure, so a later attempt cannot overlap leftover work from that request.

Batch DDL is not transactional across restore. Earlier batches can remain after
a later failure. Keep DDL completion false until all statements succeed, then use
the existing durable state update.

# Rationale and alternatives

| Choice | Reason and trade-off |
| --- | --- |
| Pack complete logical Parquet streams | Reduces per-object requests while preserving different logical schemas, Parquet metadata and normal insertion. Requires a new snapshot version, index and reader. |
| Scan by physical table | Shares query/scan work; the selected column union and global ordering still need resource controls. |
| Ordered routing with bounded concurrent encoders | Keeps each table contiguous while overlapping encoding and output. Shared byte and task budgets bound downstream work. |
| 8 MiB write/part bound | Limits submission size and in-flight buffering while allowing larger objects. It is an initial balance, not a measured universal optimum. |
| Request-owned range windows | Shares object reads across tables without cross-request or cross-credential cache reuse. Decoder pinning must remain inside the byte budget. |
| Batch DDL and ordinary target insertion | Addresses remaining CREATE overhead while retaining target routing and existing replay semantics. Database merged writes remain a separate follow-up. |

# Decisions still requiring implementation review

- Bind the authenticated batch-DDL operation to its production transport and
  capability response before PR05; preserve ordinary CREATE validation,
  per-table authorization and the batch/byte bounds above.
- Validate the initial writer and reader budgets against codec/query memory,
  wide physical schemas, concurrent chunks and distributed sorting. Agree
  deployment-level memory/spill ceilings before performance runs.
- Pin supported old-reader binaries for version-2 rejection and version-1
  compatibility tests. The first-release storage targets are local files and
  S3-compatible storage exercised through MinIO.
- Decide when release acceptance permits removing the existing experimental
  gate. Packed export/restore capability is required while mixed versions exist.

# Integration and release acceptance

Use existing COPY DATABASE orchestration to form Metric export units. Retain
ordinary COPY for non-Metric tables and other formats, and direct COPY TABLE
behavior. Gate the unfinished Metric fast path behind an `experimental_`
configuration option; disabling it selects the existing path. Add packed-import
support before enabling packed snapshot creation. The writer and reader form one
first-release feature, even though they land in separate PRs.

The [tracking issue](https://github.com/GreptimeTeam/greptimedb/issues/9120) owns
the PR breakdown and implementation dependencies. Each implementation slice
must verify its own correctness and failure behavior. Release acceptance requires:

| Area | Required evidence |
| --- | --- |
| Export | Actual CLI export with authorization, multiple physical tables and regions, dense/sparse keys, empty/residual tables, supported Metric Parquet types, and logical schema/value equivalence. |
| Restore | Authenticated remote batch DDL, per-table authorization, fresh target IDs and reconstructed metadata, dependency ordering, older-server behavior and batch failure/retry. |
| V2 lifecycle | New-reader version-1/2 coverage, old-reader version-2 rejection, target capability preflight; local/object-store interruption, multipart abort, owned-output cleanup, index/manifest failure and import replay. |
| Scale | Repeated release-build measurements varying table count up to approximately 100,000, rows/file, physical union width, regions and backend. Report median/spread, control execution order/cache, and include schema export and metadata discovery in end-to-end timing. |
| Open decisions | Agreed batch-DDL interface, resource limits, compatibility targets and experimental gate. |

## Snapshot format acceptance

Run these cases against actual CLI entry points on local files and S3-compatible
storage. Shared serialization tests alone do not establish lifecycle behavior.

| Case | Required result |
| --- | --- |
| Version-1 compatibility | Existing Parquet, CSV, JSON and schema-only fixtures retain import, resume and verify behavior, including omitted optional fields. Resuming export preserves version 1. |
| Valid version 2 | Round-trip multiple schemas/chunks, shared packs with different logical schemas, standalone ordinary/large tables, zero-row tables and empty schemas. Check index membership, typed rows and `/`-separated manifest paths on Windows too. |
| Invalid format before DDL | Missing required fields, unsupported manifest/index version or layout, version/layout/format mismatch, non-null checksums, duplicate or unsafe paths, inventory mismatch, invalid ranges, missing/wrong-length objects, and DDL/index membership mismatch fail before any target DDL or data task, including import resume. Ignorable manifest metadata remains accepted. |
| Old reader | Supported release binaries reject a valid version-2 snapshot before target DDL or writes. |
| Export resume | Interrupt object upload, index finalization and manifest completion separately; resume preserves version/layout and completed chunks, cleans only owned incomplete outputs, then produces the same complete inventory. Reject incompatible metadata before cleanup. |
| Import resume | Revalidate metadata before honoring saved state; skip successfully checkpointed tasks, retain DDL completion and replay semantics, and never mark a partially restored task complete. |
| Verify | Valid completed snapshots pass; incomplete chunks, malformed indexes, inventory mismatch, missing/extra files and object-length mismatches fail. A same-length data mutation is not required to fail a structural-only check. |

## Performance acceptance

Compare four controls with the same data and concurrency: (A) the existing
per-logical-table export and restore with the explicit-file COPY fix; (B) shared
scans, concurrent writers, redundant export HEAD removal and schema-export
batching, still writing standalone table objects; (C) B plus packing and its
reader, with identical DDL handling; (D) C plus batch DDL. If general restore
request deduplication is available, include it in B as well.

The proposed release target is at least 3x faster full export and full restore
for D versus A on 10,000 and 100,000 small logical tables with MinIO request
latency increased by 5 ms. Fix this target during RFC review, before running
release measurements. The independent local C-versus-B experiment establishes
neither this comparison nor proof of that target.

Use Linux release builds and at least three alternating rounds for primary
comparisons. Report medians/spread, schema/data/full durations, CPU, RSS, bytes,
objects, request counts and queue/cache peaks. Include zero-added-latency,
local-file, mixed small/large-table and wide-schema controls. Investigate and
remeasure full-duration regressions exceeding 5% versus A; do not generalize the
latency-injected speedup to these cases. Check values and schemas outside timing.

Validate actual write/part sizes at or below 8 MiB, aggregate concurrency across
groups/chunks, and resource release after success, cancellation and failure.
Metadata growth and query/codec memory remain visible separately from data
buffer budgets. Report C versus B and D versus C independently; do not multiply
speedups from different workloads or attribute all reader savings to packing.

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
ID order and exactly one complete indexed Parquet stream per selected logical
table in each schema chunk, checking schemas and typed rows outside timing.

Passing requires correct exports within the agreed resource/performance
thresholds on both plan paths. Also force resource exhaustion: cancellation
must leave the chunk incomplete, and retry with sufficient resources must
produce complete indexed data without altering completed chunks. Writer-only
memory measurements and single-region runs cannot satisfy this gate. Until both the
normal and failure/retry cases pass, multi-region resource acceptance remains
open.
