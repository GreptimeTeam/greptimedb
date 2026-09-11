# Metric import DDL PoC

The second slice measures logical-table creation during a real import-v2 restore.
It keeps the V2 snapshot format and data COPY path. An internal executor groups
logical CREATE ASTs by catalog, schema and physical table, then calls the existing
batch DDL procedure. Physical-table names in `on_physical_table` are literal option
values, including dots; they are not split into qualified identifiers.

This is a test harness, enabled through `cli/testing` and `operator/testing`.
It adds no batch SQL/HTTP endpoint. The internal batch entry assumes a trusted
caller; frontend authentication and authorization are not part of this slice.

## Compared paths

- `sql`: the existing DdlExecutor sends each statement through the SQL HTTP client.
- `single`: physical and other DDL still use HTTP; each logical CREATE calls the
  internal entry individually. This control separates removal of per-table HTTP
  from the effect of batching the DDL procedure.
- `batch`: the same internal entry receives up to 128 logical tables and 1 MiB of
  rendered SQL per request. Groups run sequentially. Non-logical statements are
  barriers, so database/physical creation and views keep their execution order.

The batch entry reuses ordinary CREATE's schema-option inheritance and validates
logical partition rules. It rejects mixed physical groups, duplicate names,
non-logical CREATEs, oversized statements and oversized table counts before
submitting a DDL procedure. The existing batch path's per-table metadata work and
catalog rereads are included in timing. A batch is not a transaction spanning the
whole restore: a later failure can leave earlier batches created.
The importer still loads the whole DDL list in memory; the limits above bound
individual logical CREATE calls, not the whole import process.

## Experiment method

Each combination of 100, 1,000 or 10,000 logical tables and the SQL, in-process
single or batch DDL path was measured in a separate process: nine reported runs.

Each scale/mode uses a separate process and fresh source/target standalone
instances with the default local raft-engine metadata backend and file storage.
The source uses ordinary SQL CREATE independently of the batch entry. It has
one physical group, two varying logical tag schemas,
two rows per populated table and one empty logical table. The counts above include
the empty table. Actual export-v2 produces the DDL; the first PoC exports its
logical Parquet files. Fixture generation and export are outside restore timing.
All files stay in temporary directories under the workspace COPY root.

The recorded measurements report total restore wall time, DDL phase time,
data phase time, physical/logical/other DDL execution time, SQL DDL request count,
logical CREATE call count, and maximum batch table/SQL-byte counts. The data phase
includes the unchanged data COPY tasks and their resume-state writes. The DDL
phase includes the executor and its completion-state write; parsing/grouping time
is included there but excluded from individual CREATE call durations. Snapshot
loading, validation and initial state preparation contribute only to total time.
Call counts are at the executor boundary, not counts of all underlying KV or
region RPCs. In-process logical calls have no HTTP request.
All modes classify CREATE ASTs in the harness to collect phase statistics. This
adds a client-side parse to the SQL baseline; its cost is included in DDL and
total time, but excluded from the per-call logical CREATE durations.

The correctness case crosses the 128-table boundary, uses two schemas including
one containing a dot, restores ordinary-table data and a dependent view, and
injects an unresolved physical-table reference into a real logical CREATE call
after one successful batch. It checks that the operator error is returned, the
first 128 tables remain created and DDL completion is not recorded early. It
retries the original exported DDL through the existing resume flow and confirms
successful completion removes the resume state. Every restored logical table is checked against the
source for schema, defaults, primary keys, effective SHOW CREATE options, physical
route and typed row values; target IDs are independently allocated. SHOW CREATE
materializes schema-level TTL in the exported table DDL. The benchmark also checks
every table outside the measured interval: it reads each complete source/target
physical group, groups rows by the independently resolved table IDs, projects each
logical schema and compares every typed row. It verifies total rows and nonempty
table counts to reject unexplained rows, and also queries the first, last populated
and empty logical tables directly. The 260-table correctness case queries every
logical table directly.

These local unoptimized runs measure small-file restore and DDL overhead. They do
not establish distributed KV performance, production throughput or batch HTTP
performance. Optimized data writes and production endpoint integration remain
separate work.

## Results: 2026-09-10

Measured on Darwin 25.6.0 ARM64, 16 GiB RAM, with Rust
`1.96.0-nightly (2026-03-20)`. The unoptimized test profile disables
debug symbols. Each scale/mode below is one separate-process run, in the listed
order. Exported files have just been written, so these are warm-file-cache runs.
All nine reported runs passed validation.
Full-precision measurements are preserved in
[the results JSON](metric-import-ddl-poc-results.json).

Logical CREATE call durations exclude client-side classification/grouping:

| Logical tables | SQL HTTP (s) | In-process single (s) | Batch (s) | SQL / batch | Single / batch | Logical calls: single → batch |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 0.318 | 0.177 | 0.056 | 5.70x | 3.18x | 100 → 1 |
| 1,000 | 3.091 | 1.601 | 0.544 | 5.68x | 2.94x | 1,000 → 8 |
| 10,000 | 29.181 | 19.524 | 5.776 | 5.05x | 3.38x | 10,000 → 79 |

Restore phase durations, including parsing/grouping and completion-state writes:

| Tables | Mode | Physical CREATE (ms) | All DDL (s) | Data COPY (s) | Total restore (s) |
| ---: | --- | ---: | ---: | ---: | ---: |
| 100 | Sql | 14.2 | 0.356 | 0.106 | 0.469 |
| 100 | Single | 14.1 | 0.209 | 0.099 | 0.314 |
| 100 | Batch | 13.5 | 0.084 | 0.092 | 0.184 |
| 1,000 | Sql | 35.4 | 3.294 | 2.160 | 5.488 |
| 1,000 | Single | 12.9 | 1.750 | 2.008 | 3.789 |
| 1,000 | Batch | 13.7 | 0.669 | 2.302 | 2.999 |
| 10,000 | Sql | 18.6 | 30.752 | 156.222 | 187.279 |
| 10,000 | Single | 13.4 | 21.030 | 145.977 | 167.285 |
| 10,000 | Batch | 24.8 | 7.019 | 150.196 | 157.492 |

The SQL path makes N + 2 SQL DDL requests: N logical tables, one physical table
and one database statement. Single and Batch each make two SQL DDL requests,
plus their in-process logical calls. The maximum batch was 128 tables and 33,024
rendered SQL bytes, below the 1 MiB request limit. Each scale restores N files
and 2 × (N − 1) rows; one table is empty.

Observed total restore speedups are 2.55x, 1.83x and 1.19x at 100, 1,000 and
10,000 tables. These totals include data-stage variation: at 10,000 tables, DDL
saves 23.73 s, while unchanged data COPY differs by
6.03 s between SQL and Batch. That data difference is not attributed to
the DDL optimization. These single observations are not confidence intervals or
evidence of a data-import speedup.

Batch logical CREATE is about 5.1–5.7x faster than SQL HTTP and
2.9–3.4x faster than in-process single CREATE in these runs. This supports
batching the DDL procedure itself. At 10,000 tables, however, data COPY still takes
about 150 s in the Batch run, roughly 95% of total restore time.
The [COPY profile](metric-import-copy-profile.md) separately identifies repeated
directory enumeration as the dominant cost for this fixture. This DDL comparison
alone does not attribute data-stage scaling to per-table writes.

The 260-table, two-schema correctness case passed direct queries for every
logical table, failure after a successful DDL batch, retry, effective schema TTL,
ordinary-table/view restoration and state cleanup. Admission tests passed for
count/byte limits, mixed physical groups, duplicate names and non-logical CREATE.
A separate 10,000-table SQL baseline also passed direct per-table data queries.

Regression verification passed 383 tests: the non-ignored CLI and operator suites,
the first slice's dense/sparse round trip, and the two new batch-import cases.
Clippy passed for all three crates including tests with warnings denied. Rustfmt,
TOML formatting, whitespace and license-header checks passed.
