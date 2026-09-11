# Metric export PoC

The first slice exports a single Metric physical group as ordinary V2 logical
Parquet files. It uses the existing query planner and physical scan, then routes
an ordered `__table_id` stream to one active writer. The implementation is enabled
only by `operator/testing` (and unit tests); it adds no SQL or HTTP endpoint.

## Experiment method

The experiment compared ordinary COPY DATABASE with physical-group export at
two physical schema widths, running both execution orders in separate processes.
The correctness and measurement procedures are described below.

The integration harness uses isolated source and target standalone instances.
Snapshot files use temporary local storage within the configured COPY root.

The round-trip case covers dense and sparse primary keys, multiple batches per
logical table, different tag schemas, NULL/empty strings, an empty logical table,
a dropped table's residual rows, a table name containing a dot and a bounded time
range. It checks Parquet field types and order against the logical catalog schema and
ordinary COPY's output. Existing export-v2 generates the schema/DDL files; the
harness marks the data chunk completed only after the PoC writer succeeds, then
the **unchanged import-v2 command** restores it into a second standalone with
different target IDs. Every restored table's typed values are compared with the
source, including the distinction between NULL and empty strings. A real scan
with an insufficient input-batch budget must fail before opening a writer.
At a one-row-group metadata limit, exactly one flushed group must succeed and
data requiring a second group must be refused before writing that group.
This exercises the unmodified importer used in the experiment, not a separately
released binary.
Every case, including both physical-union widths, also reads each exported
Parquet file and compares its typed rows with the source outside the timed export.

The explicit performance case fixes 64 logical tables and 1,000 rows/table, with
two tags per logical table, and changes the physical union from shared tags to
64 distinct tag columns. Measurements include data-export wall
time, input batch counts and dictionary-column counts, sampled process RSS
before/during each export, retained input-array
estimates, expanded-array and writer peaks, file counts, the frontend plan,
returned region metrics and the region EXPLAIN ANALYZE profile. Input-array
bytes are **not** disk I/O or RSS. The baseline is ordinary COPY DATABASE with
parallelism one; timings exclude data generation and schema export. EXPLAIN
warms the source first. Each width/order pair runs in a fresh test process; both
execution orders are measured. RSS is sampled every 50 ms and can miss short
peaks; it includes the standalone, allocator retention and the sampler, rather
than only exporter-owned memory. These are warm-cache experiments, not production
throughput or cold-cache I/O claims. The experiments use an unoptimized test build.

## Scope and failure behavior

This section describes the measured prototype. The RFC requires broader
multi-region and type coverage for the first release.

- Source data, schemas and TTL effects must remain unchanged during export.
- The first slice admits one physical region and Float64 fields/string tags.
  Other field types, path separators in table names and COPY format/limit/pattern
  options fail explicitly.
- The measured prototype requires one partition ordered by `__table_id` without
  a compensating frontend SortExec. Its tests check SeriesScan and the absence of
  SortExec, plus ID monotonicity across batches. These plan checks delimit this
  experiment; the RFC relies on SQL ordering and also supports plans with sorting.
- Logical column projection precedes dictionary expansion. Expanded value sizes
  determine slices before conversion; an oversized row fails. Input buffers stay
  alive while a slice is processed. `writer_bytes` triggers a flush after a write;
  codec allocations can be much larger than the incoming slice. It is a flush
  threshold, not a hard allocation cap. The reported writer peak is Parquet's
  in-progress row-group memory estimate, excluding flushed metadata and the
  object-store buffer. These limits are not a global query/process memory pool.
- Each file is closed once. Empty tables get valid zero-row files. Existing files
  are rejected. A failure leaves a partial directory and no completed snapshot;
  resume/publication is not implemented in this slice.

Batch DDL is measured separately in the [DDL report](metric-import-ddl-poc.md).
Production CLI/API integration, object-storage export and recovery validation
are not covered by this experiment.

## Results: 2026-09-10

Tested on Darwin 25.6.0, ARM64, 16 GiB RAM, with Rust
`1.96.0-nightly (2026-03-20)`. These are unoptimized test builds with
debug symbols disabled. Builds completed before timed experiments began. Each
width/order pair below is one run, so the results are preliminary.

- The full operator suite passed: 137 tests, one skipped.
- Clippy passed for operator and tests-integration, including tests, with warnings
  denied. Formatting, TOML and license-header checks passed.
- The dense/sparse round trip passed. Each encoding exported 29,988 selected
  rows from three populated logical tables plus an empty table, then restored
  all typed values using the unmodified import-v2 command and new IDs.
  Both encodings passed the input-budget and row-group-boundary checks.
- All four width/order experiments passed, including reading and comparing
  every logical Parquet file with its source table. Each exported 63,744 selected
  rows in 65 files (64 populated tables and one empty table). One dropped-table
  residual row was skipped. The input arrived in eight batches.

The variable tag is shared at width 1 and distinct per populated table at width
64. The physical tag totals below also include the shared host tag, empty-table
tag and dropped-table tag; the query excludes the dropped-table tag. Each
populated logical output still has only its own four columns.

| Physical / projected tag columns | Execution order | Legacy COPY (ms) | PoC (ms) | Legacy / PoC |
| --- | --- | ---: | ---: | ---: |
| 4 / 3 | Legacy first | 889.4 | 262.7 | 3.39x |
| 4 / 3 | PoC first | 893.7 | 309.5 | 2.89x |
| 67 / 66 | Legacy first | 1062.0 | 316.0 | 3.36x |
| 67 / 66 | PoC first | 869.0 | 308.3 | 2.82x |

The actual input contained UInt32-key dictionary columns (3 and 66 respectively).
Both the executed PoC's returned region metrics and EXPLAIN ANALYZE showed
`SortPreservingMergeExec(__table_id)` over `SeriesScan` with `PerSeries`
distribution and `two_phase` mode. The frontend plan was `CooperativeExec` over
one-region `MergeScanExec`. There was no SortExec. This establishes the ordered
stream for this single-region fixture; it does not establish one disk-I/O pass.

| Physical tag columns | Input-array peak (MiB) | Projected/expanded array peak (MiB) | Parquet row-group estimate peak (MiB) | Legacy sampled RSS increase (MiB) | PoC sampled RSS increase (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4 | 0.267 | 0.148 | 1.112 | 6.25–7.50 | 4.09–4.75 |
| 67 | 2.291–2.408 | 0.148 | 1.112 | 3.52–4.59 | 5.88–8.72 |

RSS increase means sampled peak minus the value immediately before that export.
It is not allocated bytes or an exporter-only memory measurement. Retained input
buffers are counted by Arrow and may share backing allocations. Region
`output_bytes` also describes emitted batches, not storage I/O; this experiment
does not measure disk read bytes.

The tighter correctness fixture also exposed a writer-budget limitation:
with a 4 KiB expanded-value budget and a 16 KiB flush threshold, Parquet still
reported a row-group peak of about 1.08 MiB. In Parquet 58.3.0, the timestamp's
delta-bit-pack encoder starts with a 1 MiB bit-writer buffer. A hard writer memory
limit would need to account for codec reservations before writing; flushing
afterward cannot provide that guarantee.

At this scale, physical-query fanout improves export time in both execution
orders. Logical projection keeps the expanded output and row-group estimate
stable as the physical union widens, but the incoming Arrow arrays grow by about
9x and the wide case has higher PoC RSS growth. This supports continuing the
staged PoC while retaining wide-schema admission and measurement as requirements.
Larger unions, multi-region behavior and release-build throughput are not
covered here. Restore measurements are in the [DDL report](metric-import-ddl-poc.md)
and [COPY report](metric-import-copy-profile.md).
