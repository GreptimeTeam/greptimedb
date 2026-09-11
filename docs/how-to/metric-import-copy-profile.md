# Metric import COPY profile

This third PoC slice profiles the unchanged data COPY path after batch DDL.
It uses the second slice's source, snapshot, real import-v2 execution and complete
metadata/typed-row validation. Source tables are created through ordinary SQL.
Each fresh standalone has one physical table, 100/1,000/10,000 logical tables,
two rows per populated table and one empty table. Every table has its own Parquet
file in one directory. Fixture creation, export and validation are outside restore
timing. This measures small-file overhead, not large-volume ingestion throughput.

## Measurement

Timers are test-only, disabled by default and enabled only around the measured
restore. The benchmark reads before/after counter deltas;
the counters are process-wide, so experiments must run in separate processes.
No public configuration or snapshot format changes are introduced.

Each stage reports calls and elapsed seconds, including time suspended at await.
These are **not CPU times**. The stages have different scopes:

- `client_copy_request` and `database_total` cover the client request and server
  COPY DATABASE, respectively. The one-schema experiment makes one request.
- `database_list` lists the snapshot directory once.
- `permit_wait` is cumulative time queued for a COPY table concurrency permit.
  Many files wait simultaneously; this sum is not additive to COPY wall time.
- `table_total` sums active per-file COPY durations after permit acquisition.
  `table_lookup`, `table_backend_list`, `file_metadata`, `schema_mapping`,
  `stream_build`, `stream_next`, `vectors_request` and `insert_wait` partition
  most of that work. Small uninstrumented gaps and timer overhead remain.
- `insert_total` and its lookup/convert/dispatch stages are nested within
  `insert_wait`. Do not add these again to per-file totals. Dispatch includes
  routing, storage execution and async waiting; it does not isolate WAL or locks.
- `client_state_save` includes all state saves during the measured restore:
  initial state, DDL completion and data-task transitions. It is not exclusively
  part of the data phase. `client_state_delete` covers successful cleanup.

The stream-next count includes the final EOF poll. Read/decode time can include
I/O and executor scheduling; it cannot be interpreted as decoding CPU alone.
The test also checks file, metadata, insertion and COPY request counts, including
the empty table that makes no insertion request.

## Finding and change

Before this change, COPY TABLE with an explicit filename used `Source::Filename`.
That branch performs `stat`, lists the entire parent directory, then finds the
requested filename. COPY DATABASE already enumerates that directory once, but
then repeats the full listing for every file. N files therefore cause roughly
N squared directory-entry work in this layout.

The change is confined to COPY's path selection: an explicit filename uses
`stat` to check existence and file type, then passes that path to the existing
reader. Directory requests retain their listing, pattern matching and file-type
filter. Backend construction and local-file authorization still run first.
No insert batching or storage-engine optimization is part of this change.

## Experiment method

Both variants use batch logical DDL and the same COPY concurrency. The before
variant lists the parent directory for each known file; the after variant uses
stat and the known file path. Each variant was measured at 100, 1,000 and 10,000
files in a separate process. Disabled-probe controls measure the before variant
at 1,000 files and the after variant at 1,000 and 10,000 files.

All runs use the local raft-engine metadata backend, file storage and an
unoptimized test profile with debug symbols disabled. Files were freshly
exported, so these are warm-cache runs. Builds and other benchmarks were kept
separate from the timed experiments.

## Results

Measured on 2026-09-11 on Darwin ARM64 with 16 GiB RAM.
Both variants retain batch DDL; the comparison isolates the additional COPY
path-selection change. Every row is one fresh-process run. All six profiled runs
and three disabled-probe controls passed complete data and metadata comparison.
Full values and counts are in [the results JSON](metric-import-copy-profile-results.json).

| Tables | COPY before (s) | COPY after (s) | COPY speedup | Total before (s) | Total after (s) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 0.094 | 0.071 | 1.33x | 0.183 | 0.164 |
| 1,000 | 2.135 | 0.612 | 3.49x | 2.849 | 1.289 |
| 10,000 | 146.496 | 5.481 | 26.73x | 153.647 | 12.300 |

At 10,000 tables, total restore falls from 153.65 s to 12.30 s (12.49x).
DDL varies from 6.87 s to 6.54 s; this small difference is not attributed to
the COPY change. The original per-table-SQL DDL baseline from the previous slice
is not the baseline in this table.

The following are **cumulative per-file seconds at 10,000 tables**, including
async suspension. Percentages use `table_total`, not COPY wall time. Parent and
nested insert rows are deliberately separated.

| Stage | Calls | Before cumulative (s) | After cumulative (s) | Before / table total |
| --- | ---: | ---: | ---: | ---: |
| Table lookup | 10,000 | 0.210 | 0.167 | 0.01% |
| Backend setup and file lookup | 10,000 | 1339.994 | 6.939 | 91.78% |
| File metadata | 10,000 | 78.576 | 21.724 | 5.38% |
| Schema mapping | 10,000 | 0.064 | 0.041 | 0.00% |
| Reader construction | 10,000 | 6.682 | 3.103 | 0.46% |
| Stream reads / decode / EOF | 19,999 | 19.138 | 9.529 | 1.31% |
| Vectors and request preparation | 9,999 | 0.087 | 0.048 | 0.01% |
| Insertion wait | 9,999 | 14.930 | 9.264 | 1.02% |
| Total active file COPY | 10,000 | 1459.961 | 50.996 | 100.00% |

Backend/file lookup drops from 1339.99 to 6.94 cumulative seconds.
Before the fix it accounts for about 92% of active-file time. Going from 1,000
to 10,000 files increases that stage from 13.57 to 1,339.99 cumulative seconds,
consistent with the repeated directory enumeration visible in the code. The
controlled path-selection change removes that enumeration and the large delay.

Nested insertion at 10,000 tables, already included in insertion wait:

| Insert stage | Before cumulative (s) | After cumulative (s) |
| --- | ---: | ---: |
| Table lookup | 0.337 | 0.155 |
| Request conversion / partitioning | 0.636 | 0.410 |
| Routing and storage dispatch/wait | 13.829 | 8.606 |

There is one client COPY request and one top-level directory listing. Four state
saves total 17.4 ms before and 12.0 ms after; they do not explain
the earlier 146-second data phase. Concurrent permit-wait sums are large queue
integrals (710,238 s before, 27,925 s after), not elapsed restore time or lock CPU.

Disabled-probe controls:

| Variant | Tables | COPY with probes (s) | COPY without probes (s) |
| --- | ---: | ---: | ---: |
| before | 1,000 | 2.135 | 2.048 |
| after | 1,000 | 0.612 | 0.607 |
| after | 10,000 | 5.481 | 5.449 |

These single-run controls combine instrumentation overhead and ordinary run
variation; they do not estimate a statistical overhead bound. The 10,000-table
after control also restores all data in 5.45 seconds, so the improvement does not
depend on probes being enabled. Other stages also get faster when listing work
is removed; elapsed async timers include shared scheduling/I/O contention, so
those changes must not be reported as separate decoder or storage optimizations.

For this fixture, repeated directory scanning was the dominant data-path cost.
After the fix, file metadata/read work is more prominent and DDL is roughly half
of total restore time. Merged Metric writes are not justified as the next step
by this small-file result alone. Distributed deployments, object stores, large
Parquet files and wider physical schemas still need separate measurements.

## Verification

The CLI/operator suites and relevant export/import integration cases passed
383 tests after the fix, including dense/sparse export, empty tables, two schemas,
real batch-DDL failure/retry, ordinary data and a view. All profile experiments
also verify counts, schemas, defaults, effective options, physical routes and
every typed row.

A normal build of `greptime` without the testing feature succeeded. Its four
standalone SQLness cases passed with unchanged expected results:
`copy_from_fs_parquet`, `copy_from_fs_csv`, `copy_from_fs_json` and
`copy_database_from_fs_parquet`. They cover explicit files, directories, patterns,
time filters, projected/default columns and LIMIT.

Final Clippy passed for CLI, operator and integration tests with warnings denied.
The 260-table round trip was repeated with probes disabled and explicitly enabled;
both passed, including the profile call-count checks in the enabled run. Rustfmt,
TOML formatting and whitespace checks passed. The normal build's feature graph
also confirms that operator and CLI were built without `testing`.
