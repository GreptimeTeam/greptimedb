# Metric snapshot export/import validation

## Native production-path validation

The wide, multi-SST query-memory failure is fixed and its full round trip now
passes at query parallelism 4 with a 2 GiB query pool. Large-table export
overhead changes are implemented and pass correctness checks, but their
performance benefit remains unverified under paging-free conditions. Scale
acceptance is also incomplete: host paging affected measured comparisons, and
new host swap-out stopped the first 10,000-table +50 ms round. The experimental
gate remains enabled.

The host was an Apple M2 Pro with 10 CPUs and 16 GiB RAM, running macOS 27.0.1.
MinIO ran in Docker with 2 CPUs and 1 GiB RAM. The final campaign used a
10-vCPU, 2 GiB Docker VM with both VM and container swap disabled. The native
query pool was 2 GiB with query parallelism 4; this pool is not a limit on total
process memory. Export parallelism was 4 and client chunk/task parallelism was
1. Ordinary COPY uses upload concurrency 8; experimental export uses 1. These
settings were fixed across the final campaign, and earlier configurations are
excluded from its comparisons.

A is the current per-table path, including explicit-file COPY and batched schema
export. B adds shared physical scans with standalone files, C adds packed
objects, and D adds batch DDL on restore. Timings cover the complete CLI
command. Each scale fixture has 10,000 or 100,000 populated logical tables with
32 rows per table. Full typed values, schemas, remapped target table IDs and
completed procedures are checked outside timing, using fresh restore
destinations and unchanged source SSTs.

Added latency applies once per object-store HTTP request, including multipart
operations and range reads. CLI and server use the same endpoint; SQL is not
delayed. This is controlled request delay, not a cloud-provider performance
profile.

### Results and acceptance status

The following comparisons passed full correctness checks without new host
swap-out, but some timed phases still recorded host swap-in. Their timing is
observational evidence, not paging-free performance acceptance. Three-round
entries use alternating ABCD, DCBA and BCDA orders and report complete-command
medians. The 100,000-table zero-delay entry is one control round.

| Tables | Added MinIO delay | Rounds | A export (s) | D export (s) | A restore (s) | D restore (s) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 0 ms | 3 | 16.685 | 1.470 | 17.134 | 1.814 |
| 10,000 | 100 ms | 3 | 303.145 | 3.524 | 659.288 | 3.314 |
| 100,000 | 0 ms | 1 | 168.291 | 20.657 | 174.317 | 23.854 |

For 10,000 tables at +100 ms, D was 86.0x faster on export and 199.0x faster on
restore in the observed medians. This exceeds the numeric 3x target but does not
establish acceptance: timed phases recorded up to 34.48 MiB of host swap-in,
whose process attribution and timing impact were not isolated. A export ranged
from 302.481–309.267 seconds and restore from 658.866–661.615 seconds; D export
ranged from 3.482–3.589 seconds and restore from 3.145–3.316 seconds. The
zero-delay controls observed 11.3x/9.4x export/restore speedups at 10,000 tables
and 8.1x/7.3x at 100,000 tables.

The 10,000-table local-file and +5 ms controls also completed with matching full
data and schemas. In the three zero-delay S3 rounds, B restore was 5.7% slower
than A. The same restore commands read identical request counts and byte totals,
and the local-file A/B exports were byte-identical, including all 10,000 Parquet
files and DDL. The difference did not recur in the local-file, +5 ms or
100,000-table zero-delay controls. The exact cause of the waiting difference was
not isolated; this remains a condition-specific observation, not evidence of
universal restore parity.

The first 10,000-table +50 ms round recorded 358.9 MiB of new macOS swap-out
during A export despite passing the resource preflight. VM and MinIO swap and
OOM counters remained zero. The same sampling interval saw about 1.04 GiB of
additional transient host-process RSS; this is evidence of concurrent host
activity, not proof of a unique swap trigger. The entire comparison is excluded
from performance acceptance. The campaign stopped after retaining that round's
correctness evidence, with no automatic retry. All four controls in that round
passed the full correctness checks.

Nine comparisons completed before the swap-out event. The runner rejected new
swap-out but did not reject swap-in; the final evidence audit also excludes
measured swap-in from acceptance. No primary case has three paging-free rounds.
The remaining 10,000-table +50 ms rounds, both 100,000-table high-latency cases,
and the 100,000-table local-file and +5 ms controls remain unrun in this
campaign. Completing the matrix requires a host that sustains the workload
without swap or concurrent resource interference. Earlier swap-affected runs are
also excluded.

### Observed work and resource usage

For the 10,000-table +100 ms observations, server CPU is the three-round median;
RSS is the highest 0.5-second sample across those rounds. Both remain
observations from runs with host swap-in. Export/restore values are separated by
`/`.

| Control | Server CPU (s) | Server peak RSS (MiB) | Export objects | Object HTTP requests |
| --- | ---: | ---: | ---: | ---: |
| A | 79.80 / 27.45 | 516.1 / 346.8 | 10,003 | 10,017 / 60,024 |
| B | 10.40 / 27.55 | 346.4 / 343.8 | 10,003 | 10,019 / 60,024 |
| C | 2.80 / 8.41 | 377.2 / 381.3 | 5 | 16 / 13 |
| D | 2.90 / 3.01 | 370.5 / 378.6 | 5 | 16 / 13 |

A/B uploaded 20.72 MB of request payload and downloaded 37.28 MB on restore; C/D
uploaded 21.29 MB and downloaded 24.75 MB. CLI peak RSS stayed below 63 MiB in
these rounds. Maximum observed simultaneous object requests were 4/10 for A/B
export/restore and 1/1 for C/D. These counts distinguish reduced object work
from concurrency changes. Moving from A to B, B to C and C to D gave observed
speedups of 1.06x, 84.08x and 0.96x on export, and 1.00x, 81.24x and 2.45x on
restore.

Each control used a fresh database process, but host and backend caches were not
globally evicted. MinIO was limited to 1 GiB with no recorded OOM or container
swap; host pressure and paging were recorded separately. Sampling can miss short
peaks, and process RSS is not query-pool accounting.

### Scan, memory and large-table fixes

A single-source scan unnecessarily split about 320,000 rows into 131,485 batches
for a merge that it bypassed. The PR13 guard reduced that to 41 batches and the
same ordered query from about 22.57 to 0.062 seconds. Multi-source scan behavior
is unchanged by that guard.

Wide, multi-SST scans retained unused dictionary values through sliced batches
before the ordered merge. Compacting that backing storage removes the observed
query-memory exhaustion. The wide-schema fixture with 4 KiB labels now completes
export and restore with matching typed data and schemas at the original 2 GiB
query pool and query parallelism 4.

Large-table export now avoids needless routing work for a single logical table,
preserves compatible string dictionaries, skips no-op conversion handoffs and
uses larger Parquet row groups while retaining the 8 MiB write limit. Six
balanced control permutations on a 3,145,728-row fixture produced export medians
of 383.769 ms for A, 487.063 ms for the earlier D writer and 384.020 ms for the
updated D writer. These observed medians suggest a 21.2% improvement over the
earlier writer and a 0.07% difference from A, but the timed phases recorded up
to 8.19 MiB of host swap-in. They do not establish that the performance
regression is fixed. Full export/restore correctness checks passed. A separate
local-file validation with MinIO stopped aborted before measurement: the host
swapped in 256 KiB during its resource preflight. No retry was made.

### Correctness and resource checks

The original scan and metadata-write changes passed 438 selected tests,
workspace/all-target/all-feature release Clippy, eight real CLI tests and six
native two-datanode round trips. The subsequent memory and export fixes passed
42 targeted release tests plus the focused wide-schema and large-table round
trips above. The broader checks were not rerun for the subsequent fixes.

Distributed checks covered standalone and packed layouts at query parallelism 1,
2 and 4. Packed restore also passed against a target without batch-DDL
capability. A real v1.2.1 Linux/arm64 importer rejected a version-2 snapshot
before target DDL; this is compatibility evidence, not full Linux or Windows
acceptance. Earlier mixed-schema, empty-table, large-label and long-series
controls passed 54 complete A/D round trips.

Metadata writes obey the same 8 MiB per-write bound as packed uploads and abort
on write/close failure. The complete metadata buffer is still allocated before
writing. Packed reads retain at most two 8 MiB windows per COPY request; neither
limit bounds total process memory. Resource observations distinguish host, VM
and container swap; container swap being zero does not establish host validity.

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
