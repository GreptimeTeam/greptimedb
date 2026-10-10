# Metric snapshot export/import validation

## Native production-path validation

The current release CLI and server were tested on native macOS with the PR13
scan and metadata-write fixes. Release acceptance remains incomplete, and the
experimental gate remains enabled.

The host was an Apple M2 Pro with 10 CPUs and 16 GiB RAM. MinIO ran in Docker
with 2 CPUs and 1 GiB RAM; the Docker VM had about 9.7 GiB available. The native
query pool was 2 GiB with query parallelism 4. Export parallelism was 4, client
chunk/task parallelism was 1, and restore used the server's existing COPY
concurrency. These settings were held constant across controls.

A is the current per-table path, including explicit-file COPY and batched schema
export. B adds shared physical scans with standalone files, C adds packed
objects, and D adds batch DDL on restore. Timings cover the complete CLI command.
Each primary fixture has 10,000 or 100,000 populated logical tables with 32 rows
per table. Values, schemas and remapped target table IDs are checked outside
timing, with fresh restore destinations and unchanged source SSTs.

Added latency applies once per object-store HTTP request, including multipart
operations and range reads. CLI and server use the same endpoint; SQL is not
delayed. This is controlled request delay, not a cloud-provider performance
profile.

### Results and acceptance status

For 10,000 tables, three alternating A/B/C/D rounds produced the following
complete-command medians. Seconds in parentheses show the minimum and maximum.

| Added MinIO delay | A export | D export | A restore | D restore |
| --- | ---: | ---: | ---: | ---: |
| 0 ms | 19.101 (16.550–19.629) | 1.466 (1.454–1.531) | 18.233 (17.063–18.494) | 1.875 (1.779–1.894) |
| 100 ms, affected by host interference | 306.331 (304.389–310.290) | 3.475 (3.196–3.520) | 656.619 (654.838–669.252) | 3.445 (3.134–3.480) |

The zero-delay control showed about 13.0x faster export and 9.7x faster restore,
with no recorded new host swap-out or compiler overlap. MinIO cgroup swap was
not sampled during these rounds, so backend interference was not fully excluded.

All three 100 ms rounds passed correctness checks, but every round recorded
host swap-out during at least one baseline phase; one also overlapped compilation.
The entire comparisons are excluded from performance acceptance. The raw timings
show a large observed difference, but do not establish an accepted speedup.
The completed 10,000-table +50 ms comparisons were also excluded. The 100,000-table
high-latency matrix is unfinished. No primary high-latency case is accepted yet.
A fresh-MinIO diagnostic removed observed container memory pressure but still
recorded host swap-out. Valid high-latency measurements need a stable host.

### Scan repair and remaining wide-schema limit

A single-source scan unnecessarily split about 320,000 rows into 131,485 batches
for a merge that it bypassed. The PR13 guard reduced that to 41 batches and the
same ordered query from about 22.57 to 0.062 seconds. Multi-source scan behavior
is unchanged.

Two product limitations remain:

- **Wide, multi-SST exports can exceed query memory.** Combining a wide physical
  schema with 4 KiB labels exhausted the 2 GiB SortPreservingMerge reservation
  at query parallelism 4, while the per-table path completed. Changing only server
  query parallelism to 1 completed the same export/restore with matching data and
  schemas. This is a tested workaround, not a fix for the parallelism-4 failure.
- **Large-table-only export can regress.** A 3,145,728-row logical table exported
  18.3%–24.4% slower across local-file and low-latency MinIO controls. The original
  writer produced three large row groups and about 20.83 MB; the shared writer
  produced 384 smaller groups and about 24.61 MB, both using ZSTD. Its upload
  concurrency is also lower. Shared standalone and packed export took about
  0.538/0.548 seconds in a separate comparison, locating most overhead before
  packing. The individual causes still need controlled measurement. The 8 MiB
  write limit does not require such small Parquet row groups.

### Correctness and resource checks

438 selected tests, workspace/all-target/all-feature release Clippy, eight real
CLI tests and six native two-datanode round trips passed. Distributed checks
covered standalone and packed layouts at query parallelism 1, 2 and 4. Packed
restore also passed against a target without batch-DDL capability. A real
v1.2.1 Linux/arm64 importer rejected a version-2 snapshot before target DDL;
this is compatibility evidence, not full Linux or Windows acceptance.

Mixed-schema, empty-table, large-label and long-series controls passed 54 complete
A/D round trips. These controls do not replace the unfinished high-latency matrix.
The failing wide-schema export left a failed snapshot with no open multipart
uploads and unchanged source data.

Metadata writes now obey the same 8 MiB per-write bound as packed uploads and
abort on write/close failure. The complete metadata buffer is still allocated
before writing. Packed reads retain at most two 8 MiB windows per COPY request;
neither limit bounds total process memory.

## Historical prototype conclusions

These historical prototype experiments support shared physical scans,
explicit-file COPY, batched logical DDL, and packing small logical-table Parquet
streams into shared objects. They are not release acceptance measurements.

- **Shared scans reduce repeated query work.** Projecting each logical schema
  before dictionary expansion preserves its columns and types, but does not
  remove the memory cost of a wide physical schema or distributed ordering.
- **Explicit-file COPY removes repeated directory traversal.** This improvement
  uses ordinary per-table insertion; it is independent of database merged writes.
- **Packing and its range reader reduce small-object overhead.** They improve
  complete export and restore and sharply reduce object-store requests. Benefits
  grow with table count and request latency; low-latency export gains are smaller.
  Some reader request savings can also benefit standalone files.
- **Batch DDL remains necessary.** Once data restore is faster, logical-table
  creation becomes the main remaining restore cost. The batch-DDL experiment used
  an internal interface; its numbers do not measure production transport and
  retry handling.
- **Bounded writes allow larger objects.** The packing experiment kept writes
  and upload parts within 8 MiB without imposing that limit on whole objects or
  splitting logical tables. This was the chosen size, not a measured optimum.
- **Packing has a memory trade-off.** CPU and request overhead fell, while peak
  process memory increased. Encoding buffers, upload concurrency, range windows
  and table/index metadata need separate accounting.

The exercised fixtures preserved schemas, DDL and typed values after restore.
Earlier standalone-file experiments used the existing importer; the packing
experiment used an independent driver and prototype range reader. The latter
does not establish production packed-format compatibility or V2 resume safety.

These were local standalone experiments using development builds, local files
and Docker MinIO. The production CLI now implements packed export/import and
batch logical DDL; see the [user guide](../../how-to/metric-snapshot-export-import.md).
The production validation above is separate from these prototype results. See
the [RFC](../2026-09-11-metric-export-import.md) and
[tracking issue](https://github.com/GreptimeTeam/greptimedb/issues/9120).
