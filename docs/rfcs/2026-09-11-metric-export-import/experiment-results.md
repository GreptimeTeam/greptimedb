# Metric export/import experiment results

These experiments cover physical-table export, batched logical DDL, explicit-file
COPY and packed-object transfer. They measure different stages and workloads;
their speedups should not be multiplied.

The first three experiments were measured on September 10–11, 2026, on an ARM64 machine with 16 GiB
RAM, using local standalone instances, local files, warm caches and unoptimized
test builds. Each table entry represents one run. The results establish useful
optimization directions, not production throughput estimates.

## Physical-table export

The comparison uses 64 populated logical tables with 1,000 source rows each,
plus an empty table. A time filter selects 63,744 rows, written into 65 logical
Parquet files. Ordinary COPY DATABASE uses parallelism one. Timings cover data
export and exclude schema export and fixture preparation. Both execution orders
were measured at narrow and wide physical schemas.

| Physical tag columns | Execution order | Ordinary COPY (ms) | Physical-table export (ms) | Speedup |
| ---: | --- | ---: | ---: | ---: |
| 4 | Ordinary COPY first | 889.4 | 262.7 | 3.39x |
| 4 | Physical export first | 893.7 | 309.5 | 2.89x |
| 67 | Ordinary COPY first | 1062.0 | 316.0 | 3.36x |
| 67 | Physical export first | 869.0 | 308.3 | 2.82x |

Physical-table export was **2.82–3.39x faster** in these runs. Each populated
logical file still contains only its own four columns. Projecting the logical
schema before dictionary expansion kept the expanded-array peak at 0.148 MiB
and the Parquet row-group estimate at 1.112 MiB for both widths. Incoming array
estimates grew from 0.267 MiB to 2.291–2.408 MiB: logical projection does not remove
the memory cost of a wide physical schema. These estimates are not process RSS
limits, and a writer flush threshold does not cap codec allocations.

The measured single-region query used `SeriesScan` and `SortPreservingMergeExec`
without a separate `SortExec`; `__table_id` matches a physical primary-key prefix.
Only Float64 fields, string tags and timestamps were exercised. These runs do not
establish sorting memory, spill or performance when region count exceeds
frontend `target_partitions`; see the RFC's
[ordering acceptance gate](../2026-09-11-metric-export-import.md#ordering-acceptance-gate).
The timestamp encoder reserved about 1 MiB even for a small batch, illustrating
why a writer flush threshold is not an allocation ceiling.

## Batched logical DDL

The restore fixture has logical tables sharing one physical table, one file per
logical table, two rows per populated table and one empty table. The 10,000-table
case contains 19,998 rows. The three paths use SQL HTTP per table, an internal call per table, and
internal batches of up to 128 tables and 1 MiB of rendered SQL, submitted
sequentially. These request bounds do not bound the whole parsed DDL list in
memory. The internal single-call control separates transport savings from
procedure batching.

| Logical tables | SQL HTTP CREATE (s) | Internal single CREATE (s) | Batched CREATE (s) | Logical calls: single → batch |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0.318 | 0.177 | 0.056 | 100 → 1 |
| 1,000 | 3.091 | 1.601 | 0.544 | 1,000 → 8 |
| 10,000 | 29.181 | 19.524 | 5.776 | 10,000 → 79 |

These are logical CREATE call durations. At 10,000 tables, the complete DDL phase
fell from 30.752 s with SQL HTTP to 7.019 s with batching. Total restore fell from
187.279 s to 157.492 s; unchanged data COPY still took about 150 s. Data-phase
variation is not a DDL benefit. Batching improves CREATE itself, but the original
COPY bottleneck limits its effect on total restore.

The batch executor was in-process. A production authenticated server interface
is still needed; these times do not predict batch HTTP latency.

## COPY file lookup

Both variants below use batch DDL and the same COPY concurrency. Previously,
COPY listed a file's parent directory even when it already knew the filename.
With N files in one directory, repeating this for each file causes roughly N²
directory-entry work. The fix checks the known path directly; directory COPY
still lists files normally.

| Logical tables / files | COPY before (s) | COPY after (s) | Total restore before (s) | Total restore after (s) |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0.094 | 0.071 | 0.183 | 0.164 |
| 1,000 | 2.135 | 0.612 | 2.849 | 1.289 |
| 10,000 | 146.496 | 5.481 | 153.647 | 12.300 |

At 10,000 files, COPY improved **26.73x** and total restore improved **12.49x**.
Before the fix, backend/file lookup accounted for about 92% of cumulative active
file-COPY time. A control with profiling disabled completed COPY in 5.449 s.

The **5.481 s result still uses per-table writes**, with only 19,998 rows. It is
not a merged-write result. Larger files, distributed targets and object stores
are needed to decide whether merged writes provide enough additional benefit.

## Packed-object export and restore

The packing experiment uses 1,000 or 10,000 logical tables with 100 rows each,
two physical tables, two logical schemas, and two ordinary tables with 100 rows
each. The larger fixture therefore has 1,000,200 rows. It uses a macOS development
binary and local Docker MinIO. Both paths use concurrency eight, shared physical
scans, bounded parallel writers, batched SHOW CREATE, HTTP client reuse and
removal of redundant export HEAD requests in an exclusively owned destination.

The packed path adds a streaming object containing complete logical Parquet
files, an index, and a reader sharing two 8 MiB range windows. It still performs
normal per-table database insertion. Both paths use the same independent driver:
schema-only V2 export/import plus actual database COPY operations. This validates
the data path, not production version-2 manifest handling or V2 packed resume.

### Full-duration comparison

For 10,000 logical tables with 5 ms added to each MinIO request, three alternating
rounds gave the following medians. Schema time is included in the full durations.

| Phase | Optimized per-table objects (s) | Packed objects and reader (s) | Speedup |
| --- | ---: | ---: | ---: |
| Full export | 21.76 | 8.12 | 2.68x |
| Export schema | 2.72 | 2.75 | 0.99x |
| Export data | 18.12 | 5.35 | 3.39x |
| Full restore | 88.55 | 30.33 | 2.92x |
| Restore schema | 24.53 | 24.42 | 1.00x |
| Restore data | 64.02 | 5.95 | 10.76x |

Full export ranged from 20.58–23.32 s for per-table objects and 8.05–8.26 s for
packing. Full restore ranged from 87.50–92.03 s and 30.26–32.26 s respectively.
Phase medians need not sum to the full median; the full measurements also include
driver orchestration and metadata work.

| Additional control | Per-table objects | Packed objects and reader | Speedup |
| --- | ---: | ---: | ---: |
| 1,000 tables, +5 ms, full export, one pair | 2.46 s | 1.72 s | 1.44x |
| 1,000 tables, +5 ms, full restore, one pair | 10.54 s | 5.07 s | 2.08x |
| 10,000 tables, no added latency, full export, one pair | 14.16 s | 7.98 s | 1.77x |

The benefit increases with small-object count and request latency. The
zero-added-latency result does not establish a multi-fold full-export improvement.
After packing, remaining schema/scan/encoding costs limit export; roughly 24 s
of CREATE work dominates restore and motivates batch DDL.

### Storage requests and resources

The following compares the 10,000-table +5 ms runs. Request/object counts were
the same in each round; process RSS peaks and CPU time are three-run medians.

| Metric | Per-table objects | Packed objects and reader |
| --- | ---: | ---: |
| Data objects | 10,002 | 3: one pack and two ordinary Parquet objects |
| Parquet data bytes | 19,317,488 | 19,317,488 |
| Full-export storage requests | 10,020 | 16 |
| Full-restore storage requests | 60,017 | 22 |
| Export peak RSS | 408.2 MiB | 454.8 MiB |
| Restore peak RSS | 271.8 MiB | 372.8 MiB |
| Export process CPU | 28.99 s | 22.59 s |
| Restore process CPU | 76.48 s | 45.90 s |

Counts include schema, index, experiment manifest and multipart requests. The
packed path adds a 984,344-byte index. Data bytes are unchanged. Request savings
are evidence for lower request overhead; no provider billing model was measured.
Restore improvement includes removal of repeated metadata/range requests by the
reader, some of which can also be optimized for standalone files.

The 19,315,054-byte pack was uploaded in parts of 8,388,608, 8,388,608 and 2,537,838
bytes. Observed request bodies never exceeded 8 MiB, and HTTP concurrency did not
exceed eight. This tests a write-size bound, not an object-size bound or a total
RSS bound. No comparison of different part sizes was performed.

### Verification and limits

Ten exports and eight complete restores passed schema, DDL, row-count and typed
value checks. The affected modules had 241 passing tests and one skip, including
large-table streaming fallback, cross-window reads, pack/index failures and
cleanup of owned partial files. These checks do not replace release acceptance.

The experiment's process-global reader cache must become request-owned before
production use. Production packed-format versioning, capability checks, normal
V2 completion/resume, distributed execution and large-table throughput remain
to be validated. Large-table fallback has module coverage; these throughput
fixtures contain small logical tables. Peak RSS was sampled for the whole
process and increased with packing. A few proxy idle-connection reconnects
occurred; client retry time remains included in the results.


## Earlier experiments: correctness and remaining coverage

The experiments compared exported/restored schemas and typed rows with the
source. Checks covered dense/sparse keys, different logical schemas, NULL and
empty values, empty tables, dropped-table residue and fresh target IDs. The
unmodified import-v2 command used in the experiment restored the exported files
into a fresh target with different IDs. This does not establish compatibility
with all released readers. Batch-DDL checks covered two schemas, ordinary
tables/views, failure after a successful 128-table batch and retry of the original
DDL. Existing standalone COPY cases also passed.

The internal exporter assumed a trusted caller. It could leave a partial output
directory after failure and did not implement completion publication or resume.
These limitations require production integration work.

Production acceptance still requires release-build measurements with repeated
runs, larger workloads, distributed/multi-region execution, object stores,
version-1 compatibility, old-reader rejection of packed snapshots, and export
cancellation/recovery. These are tracked
in the [tracking issue](https://github.com/GreptimeTeam/greptimedb/issues/9120).
