# Metric snapshot export/import validation

## Native production-path validation

The native macOS campaign started on 2026-10-09, using the actual release CLI
and server on base `e4872ec1a2d0` with the PR13 changes. The measured binary's
SHA-256 is `eab046a73628c8342c75deeb0b36cf82e3e37057a081687228296dca42f94532`.
Rust source and dependency hashes are frozen; documentation is updated as
results arrive. Raw fixtures, requests, resource samples and failed runs are
retained locally.

The 10k/100k primary matrix with +50/+100 ms per object-store request is
incomplete. Its measurement queue was interrupted on 2026-10-09; the resource
audit below also excludes all completed high-latency cases from performance
acceptance. No primary speedup or completed release-acceptance claim is made.
The experimental gate remains in place: the combined wide-schema/large-label
case below still fails at the primary query parallelism, and these native
measurements do not establish Linux or Windows behavior.

### Environment and controls

- Apple M2 Pro, 10 CPUs, 16 GiB RAM, macOS 27.0.1, AC power; Rust 1.98.1,
  optimized release build. No thermal/performance warning was recorded at setup.
- Native database query pool: 2 GiB; `query.parallelism = 4`. Export parallelism:
  4; CLI chunk/task parallelism: 1. Restore uses the existing server COPY defaults.
- Docker Desktop VM: 10 CPUs and 10,419,826,688 bytes RAM. MinIO container:
  2 CPUs, 1 GiB RAM, 2 GiB memory-plus-swap limit. Database and CLI run natively.
- A is the current per-logical-table path, including explicit-file COPY and
  SHOW CREATE batching. B adds shared physical scans with standalone files.
  C adds packed objects with ordinary DDL. D enables batch DDL as well. C and D
  have the same export implementation. Only the batch-DDL capability is masked
  for A/B/C; packed-import capability remains available.
- Delay is injected once for every object HTTP request, including HEAD, LIST,
  range GET, DELETE and multipart create/part/complete/abort. SQL is undelayed;
  this is added request delay, not a bandwidth limit or an exact measured RTT.
- Timing covers complete CLI export or restore. Inventory, schemas, every typed
  value, shifted target IDs and procedure completion are checked outside timing.
  Sources are restarted per control and source SST hashes are preserved.
  All controls retain their existing encoding and upload-concurrency policies.

### Correctness and local checks

438 selected scan, CLI, datasource, operator and integration tests passed,
including the new single-input scan case, bounded metadata writes with abort on
write/close failure, and packed import without batch-DDL capability. Full
workspace/all-target/all-feature release Clippy passed with `-D warnings`.
Eight additional real CLI tests passed. Six native two-datanode round trips
covered standalone and packed layouts at query parallelism 1, 2 and 4, with
complete value/schema checks and different target IDs. At q=1 the frontend
retained SortExec and MergeSortExec; q=2/4 used MergeSortExec, with remote
SortPreservingMergeExec on the datanodes.

A real v1.2.1 Linux/arm64 importer rejected a version-2 snapshot before target
contact or DDL. That compatibility probe is separate from native macOS
performance and is not a full Linux acceptance run.

### Campaign status and resource validity

The main plan contains 20 complete A/B/C/D cases. Thirteen cases completed all
value, schema, target-ID and procedure checks, but only five meet the recorded
resource criteria: three 10k zero-delay S3 rounds, one 10k +5 ms S3 round and
one 10k local-file round. Eight complete cases are excluded because at least
one timed phase recorded new system swap-out or a compiler/test process. These
include all six completed 10k +50/+100 ms cases and both completed 100k S3
controls. Controls from an affected case are not selectively retained. There
are no accepted primary high-latency cases yet.

The next 100k local-file case stopped during source validation. Its request
trace contains only source SELECTs and preflight requests, with no COPY or
object writes and no timed CLI result. Both the driver and proxy exited with
status 137 (SIGKILL), without a recorded application error or host reboot. The
origin of the signal is unknown. The frozen binary, Rust/dependency files and
source SST hashes still match. The incomplete case requires a new destination
and a complete rerun. In total, seven unfinished cases and eight replacements
remain; partial timings will not be combined into a result.

Across the 104 completed main-case phases, 21 recorded interference. The
largest sampled database RSS was 2,110 MiB and the largest CLI maximum RSS was
181.5 MiB. Database RSS is sampled every 500 ms; CLI maximum RSS comes from
`time -l`. The swap counters describe the whole macOS host and do not identify
which process was swapped. Compiler overlap is directly observed in some
phases; database and Docker VM RSS changes alone cannot attribute the remaining
swap activity. Absence of a compiler sample is not proof of an idle host.
Those phases did not sample the MinIO cgroup, so the five cases passing the
recorded host checks do not establish absence of backend swap.

A later inspection also found pressure inside the test MinIO container: it was
near its 1 GiB memory limit, with about 234 MiB of cgroup swap, substantial
reclaim activity and no cgroup OOM event. This is a backend resource problem in
addition to the observed host interference; the inspection does not establish
its exact contribution to earlier timings or the SIGKILL. The original container
and snapshots are retained offline. An independent empty volume with the same
image, 2-CPU/1-GiB limits and endpoint reduced initial container memory to about
311 MiB with zero cgroup swap. A separate complete 10k +50 ms A/B/C/D diagnostic
then passed all correctness checks, with a MinIO cgroup memory peak of 843 MiB,
zero cumulative cgroup swap peak, no memory-limit/OOM events and no observed
compiler overlap. However, during A export the host swap-out counter increased
by 2.38 GiB. The complete diagnostic is therefore excluded from performance
acceptance under the same rule. The fresh backend avoided observed container
pressure while host swap-out persisted; this does not identify which host
process was swapped. Both test containers and the proxy
are now stopped, with both data volumes retained.

Long measurements are paused until a stable memory/load window or a separate
idle host is available. The remaining complete cases and replacements are
estimated to need 30–40 hours at the observed request rates. A short idle
preflight alone is insufficient: the previous 60-second check and the later
120-second check both failed to prevent interference during a case.

### Low-latency and large-object controls

Each row reports three complete A/D round trips in AD, DA, AD order. Values are
median seconds, followed by the minimum and maximum. All 54 round trips passed
full correctness checks. Across their 108 measured phases there were no new
swap-outs, compiler samples, writes/parts over 8 MiB, or timing-clock mismatches
over 0.1 seconds. Swap-ins remain recorded; absence of swap-out is not a claim
that the host had no other activity.

The mixed fixture contains two physical groups, 128 logical tables with
heterogeneous tags (including two empty tables), a large ordinary table and a
view. The label fixture contains 6,144 rows with unique 4 KiB labels. The series
fixture contains 3,145,728 rows, 16 short tags, nullable exact-quarter DOUBLE
values, and a roughly 24.61 MB standalone Parquet object in the packed layout.

| Fixture / backend | A export | D export | A restore | D restore |
| --- | ---: | ---: | ---: | ---: |
| Mixed / S3 +0 ms | 0.340 (0.328–0.375) | 0.285 (0.269–0.291) | 0.750 (0.688–1.302) | 0.397 (0.351–0.454) |
| Mixed / S3 +5 ms | 0.555 (0.537–0.605) | 0.435 (0.373–0.456) | 0.926 (0.922–0.964) | 0.746 (0.737–0.786) |
| Mixed / local file | 0.645 (0.615–0.647) | 0.237 (0.226–0.250) | 0.340 (0.325–0.354) | 0.209 (0.201–0.221) |
| Large labels / S3 +0 ms | 0.303 (0.295–0.309) | 0.269 (0.267–0.274) | 0.345 (0.334–0.365) | 0.335 (0.315–0.359) |
| Large labels / S3 +5 ms | 0.344 (0.340–0.366) | 0.377 (0.374–0.379) | 0.711 (0.706–0.738) | 0.685 (0.676–0.701) |
| Large labels / local file | 0.182 (0.172–0.184) | 0.162 (0.154–0.164) | 0.189 (0.189–0.208) | 0.179 (0.178–0.188) |
| Long series / S3 +0 ms | 0.424 (0.403–0.456) | 0.528 (0.515–0.543) | 1.136 (1.108–1.160) | 0.730 (0.664–0.740) |
| Long series / S3 +5 ms | 0.505 (0.494–0.513) | 0.624 (0.622–0.640) | 1.688 (1.670–2.182) | 1.179 (1.167–1.194) |
| Long series / local file | 0.246 (0.244–0.257) | 0.291 (0.280–0.298) | 0.766 (0.739–0.794) | 0.555 (0.549–0.562) |

Long-series export regressed by 18.3%–24.4%. A used three 1,048,576-row groups
and about 20.83 MB; B/D used 384 8,192-row groups and about 24.61 MB. Both used
ZSTD. The original path allows eight upload operations per file; the bounded
shared and packed writers use one. These existing resource policies were kept
in the controls. In an additional complete A/B/D diagnostic, B and D export
were 0.538/0.548 seconds with matching row-group counts and nearly equal file
sizes. This locates most of that overhead in the shared bounded writer rather
than the packing metadata alone; it does not isolate an exact causal percentage.

Large-label export at +5 ms regressed by 9.7% (about 33 ms). Packed output adds
index/inventory work and serializes upload parts; with only one logical table,
there is no small-object reduction to offset those costs. The source, request
trace and negative result are retained. The optimization is not uniformly
faster for large-table-only exports.

### Scan repair and remaining wide-schema limit

The original 10k-table ordered query produced 131,485 distributor batches and
spent about 21.90 seconds yielding under backpressure. The scan was splitting
batches for a merge that a single input bypassed. Skipping that split for one
range with one source reduced the same query from about 22.57 to 0.062 seconds:
41 batches and 179 microseconds of distributor yield, without changing query
memory, parallelism, data or the multi-source split policy. Full 10k A/B/C/D
round trips passed on the repaired binary.

Three fresh zero-delay S3 rounds used ABCD, DCBA and BCDA order. All values,
schemas and target IDs passed, with no new swap-outs in the 24 timed phases.
The 10,000 logical tables contained 32 rows each. Seconds are median (min–max):

| Control | Export | Restore |
| --- | ---: | ---: |
| A | 19.101 (16.550–19.629) | 18.233 (17.063–18.494) |
| B | 11.211 (11.115–11.725) | 18.424 (17.338–19.206) |
| C | 1.523 (1.454–1.564) | 7.244 (7.230–7.972) |
| D | 1.466 (1.454–1.531) | 1.875 (1.779–1.894) |

B restore was 1.0% slower than A by median. D improved complete export/restore
by about 13.0x/9.7x in this zero-delay control.

The object-operation counts below were identical in all three zero-delay
rounds. Byte columns are median MiB sent/received by the object client, including
metadata and listing responses. GET excludes LIST and range GET; unlisted
operations, including DELETE and multipart abort, had zero calls.

| Control / phase | Object operations | Sent / received MiB |
| --- | --- | ---: |
| A export | HEAD 1, LIST 11, PUT 10,005 | 19.764 / 2.410 |
| B export | HEAD 1, LIST 13, PUT 10,005 | 19.764 / 2.411 |
| C/D export | HEAD 1, LIST 3, GET 1, PUT 6, multipart create 1 / part 3 / complete 1 | 20.302 / 0.940 |
| A/B restore | HEAD 30,001, GET 2, LIST 21, range GET 30,000 | 0 / 35.493 |
| C/D restore | HEAD 4, GET 5, LIST 1, range GET 3 | 0 / 23.608 |

Export used 79 SHOW CREATE requests and one COPY request. A/B/C restore used
10,001 ordinary CREATE TABLE requests, including the physical table, and
10,000 logical-create procedures. D used one ordinary physical-table CREATE
and 79 batch requests carrying 10,000 logical statements, with 79 logical-create
procedures. Each restore used one COPY request. Procedure completion was checked
outside timing for every control.

| Control / phase | DB process CPU seconds, median | DB sampled peak RSS MiB, min–max | CLI maximum RSS MiB, largest round |
| --- | ---: | ---: | ---: |
| A export | 38.77 | 678–715 | 54.1 |
| A restore | 14.60 | 268–329 | 55.5 |
| B export | 4.50 | 344–346 | 55.7 |
| B restore | 14.80 | 302–306 | 55.6 |
| C export | 2.65 | 354–376 | 57.7 |
| C restore | 8.17 | 314–377 | 61.1 |
| D export | 2.61 | 366–373 | 57.0 |
| D restore | 2.79 | 352–367 | 61.1 |

CPU here is the cumulative database-process delta, not the query's CPU metric.
The 8 MiB write/part bound and two 8 MiB packed-read windows are separate payload
budgets. The metadata writer still receives a complete metadata buffer; bounded
write calls do not bound that allocation, catalog state, query/codec memory or
process RSS.

The combined stress fixture adds the 4 KiB-label logical table to the wide
mixed fixture and contains multiple SSTs. A completes at q=4, but D still
exhausts the 2 GiB SortPreservingMerge reservation. The failed snapshot is marked
failed, source SSTs are unchanged, and no multipart uploads remain open. This
is a query-memory reservation failure, not evidence of equivalent process RSS.
With only server `query.parallelism` changed to 1, D completed export/restore
and all value/schema/new-ID checks. That is a verified workaround for this
fixture; the q=4 acceptance failure remains. CLI `--parallelism` is a separate
setting.

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
