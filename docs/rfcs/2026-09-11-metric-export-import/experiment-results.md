# Metric export/import experiment conclusions

The experiments support shared physical scans, explicit-file COPY, batched
logical DDL, and packing small logical-table Parquet streams into shared objects.

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
  an internal interface; production transport and retry integration remain work.
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
and Docker MinIO. Release acceptance still needs the actual packed V2 path,
request-owned reader isolation, old-reader rejection, large-table throughput,
distributed ordering, cancellation and recovery. See the [RFC](../2026-09-11-metric-export-import.md)
and [tracking issue](https://github.com/GreptimeTeam/greptimedb/issues/9120).
