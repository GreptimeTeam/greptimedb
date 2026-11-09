---
Feature Name: Sketch Reader Framework
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/10292
Date: 2026-11-09
Author: "WenyXu <wenymedia@gmail.com>"
---

# Summary

Expose an optional, in-process sketch-reading capability through RegionServer. A Region-bound handle provides `stat()` for coverage and `query()` for estimates. Each operation captures its own SST snapshot. OSS prepares the physical read context; an injected handler interprets persisted sketches.

This RFC defines the in-process reader contract. It builds on the *Explicit Index Build Plans*, but introduces no protobuf, SQL, system-table or remote routing changes.

# Motivation

Callers need to distinguish missing statistics from zero rows. SST time ranges overlap, compaction replaces files, and Region splits can change logical scope while sharing physical files. Returning only a merged estimate would hide these differences.

A full-table manifest scan or Parquet scan is unsuitable as an application-facing default for large tables. The API requires a time range and reads persisted summaries. This does not imply that the current snapshot selection has a sublinear index: selecting candidates can still inspect the Region's file metadata.

## Goals

Provide time-bounded coverage and estimates from persisted sketches through an optional engine capability. Report missing coverage alongside each result.

## Non-Goals

No RPC, cross-Region aggregation, memtable coverage or Parquet scan fallback is introduced.

# Details

## Application and engine APIs

```rust
let reader = region_server.sketch_reader(region_id)?;
let coverage = reader.stat(request.clone()).await?;
let result = reader.query(request).await?;
```

`RegionSketchReader` holds a Region identity and engine reference. Acquiring it checks capability availability without file I/O or taking an SST snapshot. An unsupported engine returns an explicit error.

`RegionEngine::sketch_reader()` exposes `Option<&dyn SketchReader>`. The engine trait has separate `stat(region_id, request)` and `query(region_id, request)` methods. Mito shares `build_sketch_read` between them; there is no public boolean mode switch.

`SketchRequest` contains a required bounded, half-open event-time range with unit-aware `Timestamp` endpoints, column names and operations. Operations are distinct count, quantile fraction, and frequency of a specified value. The internal caller must enforce Region, column and tenant access, including frequency probes for individual values. Names are resolved against Region metadata; callers do not need to discover column IDs through metasrv first. The engine validates a nonempty range, nonempty column/query lists and finite quantile fractions in [0, 1] before preparing a snapshot. The handler validates column types and resource limits before enumerating files.

## Snapshot and plugin boundary

```text
RegionSketchReader.stat/query
  -> engine SketchReader capability
  -> Mito build_sketch_read
       -> validate common request parameters
       -> resolve Region and capture current version
       -> retain SST snapshot and create a lazy time-filtered view
       -> prepare the existing Puffin manager
  -> injected SketchReadHandler.stat/query
       -> validate request before enumerating candidates
       -> check manifest descriptors before Puffin I/O
       -> directory inspection / compatible payload merge
  -> SketchResponse with coverage and optional values
```

`SketchReadContext` borrows metadata, a `SketchReadFiles` view and the existing Puffin manager. Preparing the view retains an immutable SST snapshot without enumerating, cloning individual handles, sorting or collecting a selection. The handler iterates borrowed candidates only when needed. Invalid requests can fail before enumeration.

The view pins the **entire** SST snapshot during the operation, including files outside the requested interval. Holding that snapshot can delay reclamation while a request is active or waiting for admission. Lazy iteration defers enumeration and avoids an eager selection vector; it does not remove snapshot retention. It does not retain memtables. File metadata views share the SST lifetime. With object-store GC enabled, the file reference manager tracks exact index generations for collection by GC. Local storage does not register these references. When GC runs without them, superseded index generations are reclaimed after the removed-files retention window; with GC disabled, they remain until SST retirement and final lifetime release. The reader requires exact index-generation retention for the lifetime of an operation, including when rebuild replaces an index on an SST that remains live. The local-mode retention window alone does not establish that guarantee; exact generation protection and concurrent read/rebuild/GC validation are required before production use. Each call gets a fresh snapshot; a prior `stat()` does not reserve the same files for a later `query()`. Query therefore includes its own coverage.

SST selection respects its inclusive maximum timestamp: a file intersects `[start, end)` when `file.min < end && file.max >= start`. This selects candidate files, not exact per-row coverage. Traversal remains O(N) in Region file metadata in the worst case, has no ordering guarantee and can stop early. Laziness removes eager materialization; it does not introduce a time-range index.

## Response semantics

`SketchResponse.results` follows request column order, then query order within each column. Each `ColumnSketchResult` identifies a column and operation, and contains:

- Nonempty complete buckets: time range, covered file count, missing file count and represented rows.
- File-level gaps: unconfigured algorithm, missing index, legacy format, scope mismatch or incompatibility. Each gap carries the physical source Region ID, file ID and index version as separate typed fields.
- An optional algorithm-specific value; `stat()` leaves it absent.

`NotConfigured` means the requested algorithm is absent from the current column configuration. It takes precedence over persisted descriptors and requires no Puffin read. `MissingIndex` means the algorithm is configured but the file has no descriptor; `Incompatible` covers descriptor- and directory-level incompatibility, including unsupported directory versions. Payload property mismatches and corrupt payloads return errors. A separate mapping for unsupported payload formats remains future work.

Represented rows include NULLs and describe contributing physical data. Distinct count and quantile exclude NULLs; frequency can request NULL explicitly. HLL returns an estimate and relative standard error, not a hard maximum error. KLL returns a quantile value, without exposing its capacity parameter or claiming a deterministic bound. MG returns inclusive frequency bounds.

For example, a request for `[0, 120000)` milliseconds could return the following conceptual projection for one column/query:

```text
buckets:
  [0, 60000):      covered_files=2, missing_files=0, represented_rows=200
  [60000, 120000): covered_files=1, missing_files=1, represented_rows=80
gaps:
  file-C@version-7: MissingIndex
value:
  DistinctCount { estimate: 140.0, relative_standard_error: 0.0325 }
```

The estimate describes available physical contributions, not all current logical rows in the requested interval. Coverage reports missing statistics; complete coverage does not establish freshness after deletion or deduplication. A missing SST can intersect multiple buckets. File gaps have no exact missing-row estimate, and this API does not infer one from the SST's minimum/maximum timestamps.

Candidate selection uses output SST timestamp bounds. With input-summary reuse, deleted contributions can fall outside those bounds: a narrow request can skip a file whose retained bucket appears in a wider request. This range-dependent omission is accepted for approximate planning statistics. The API does not guarantee identical contributions for the same bucket across different request ranges, even with complete reported coverage. This RFC does not require separate sketch bounds or restrict reuse to prevent it. The current windowed writer builds from output rows; this limitation applies when bucket-level reuse is introduced.

Only full aligned buckets inside the request contribute. The handler rejects conflicting bucket widths within one column/algorithm result instead of choosing a width based on file traversal order. Partial edge buckets are excluded. Coverage concerns persisted SSTs only. Flush is outside the extension-build scope, and memtables have no sketch coverage in this API. These gaps can make even a fully aligned recent request incomplete. No bucket entries does not prove that the table is empty: memtables, excluded edges and missing statistics may contain data. Callers decide whether coverage is sufficient; the framework does not silently extrapolate partial distributions.

## Overlap and Region scope

Scope is the serialized `partition_expr` captured at writing; an empty expression denotes an unpartitioned Region.

The handler merges each unique physical index identity once within a request. Distinct overlapping SSTs both contribute; no logical-row deduplication is performed. An HLL's set-union property does not make KLL or MG duplicate-insensitive.

A persisted sketch for an unfiltered file generally cannot answer a filtered Region scope. The handler reports scope mismatch rather than applying a predicate to retained representatives and claiming exact coverage. Cross-Region aggregation must additionally handle shared physical files; this RFC does not define such a coordinator.

## Caches and concurrency

OSS exposes existing Puffin access and cache infrastructure rather than a new generic blob storage API. The handler may use `common_cache::Cache` for immutable directories and payloads.

Use the manager's shared `PuffinFileKey` rather than assembling a sketch-specific namespace. It combines an Arc-shared structured storage scope (backend instance, scheme, name and root), the exact object path, and the immutable file handle including index generation. Bloom, inverted and sketch caches use this same file identity; blob or page identity extends it when needed. Parameter/type/scope validation is still required after a cache hit.

Missing requested column/kind descriptors produce coverage gaps without opening Puffin, even when the file contains other indexes. Metadata-only Puffin reads do not construct a blob-key lookup table; the lookup is built on first key access. That table is still per reader and scales with the footer's blob count.

Concurrent identical misses should coalesce; failed loads must be retryable. Request admission precedes directory/payload accumulation, with separate permits for cold loads. Decode, merge and answers run on the shared blocking runtime. A blocking task retains its admission permit until completion even if its caller is cancelled.

The handler groups operations by requested column and algorithm: multiple quantiles share bucket decoding and merged state. Payloads are processed in chunks into request-local aggregates, without mutating cached state. Budget checks include cache hits and response growth, but estimated retained bytes are not a hard process-RSS bound. Resource limits remain implementation policy, not fixed values in the OSS trait. The handler charges duplicated bucket/gap entries for every returned operation, including grouped quantiles, and fails on quota exhaustion rather than silently truncating. This is an entry quota, not a bound on serialized response bytes; long values and column names need a separate byte budget before an RPC is exposed. Future operations returning N items must charge N entries rather than one.

Cache identity and its scope are defined in *Explicit Index Build Plans*, under “Shared Puffin access”.

## Compatibility and Rollout

Unavailable capability or a requested index operation unsupported by the engine returns an explicit error (`Unsupported` for an unsupported operation). The reader does not scan Parquet to replace it. Invalid requests and corrupt data also return errors. Missing or incompatible statistics remain explicit gaps when the handler can safely identify them. Unsupported directory versions must not be decoded as current payloads.

Manifest compatibility and the minimum rollback version follow *Explicit Index Build Plans*, under “Compatibility and Rollout”. Tolerating an unknown descriptor does not make its index queryable.

The request/response types are Rust contracts, not a promised wire format. The first RPC design must separately define versioning, routing, authorization, response limits and snapshot semantics. There is no generic persisted sketch decoder in OSS.

## Interface Sketch

Simplified signatures; lifetimes, reference aliases and concrete error types are omitted.

```rust
trait SketchReader {
    /// Captures a fresh Region snapshot and returns coverage without algorithm payload reads.
    async fn stat(&self, region: RegionId, request: SketchRequest) -> Result<SketchResponse>;
    /// Captures a fresh snapshot and returns estimates with their own coverage.
    async fn query(&self, region: RegionId, request: SketchRequest) -> Result<SketchResponse>;
}

struct SketchReadContext<'a> {
    metadata: &'a RegionMetadata,
    files: &'a SketchReadFiles,
    puffin_manager: &'a SstPuffinManager,
}

// Holds an immutable snapshot; does not materialize matching files.
impl SketchReadFiles {
    /// Lazily visits borrowed files from the retained snapshot.
    fn iter(&self) -> impl Iterator<Item = &FileHandle>;
}

trait SketchReadHandler {
    /// Inspects directories using the borrowed context; does not decode algorithm payloads.
    async fn stat(&self, context: &SketchReadContext, request: &SketchRequest)
        -> Result<SketchResponse>;
    /// Loads compatible selected payloads and reports gaps alongside merged estimates.
    async fn query(&self, context: &SketchReadContext, request: &SketchRequest)
        -> Result<SketchResponse>;
}
```

The engine owns snapshot preparation. The handler borrows that context for one operation. The Region-bound application handle supplies the Region ID to the engine capability.

# Implementation Plan

Ensure active reads retain the exact index generation they address in every supported storage mode, then validate concurrent index rebuild and GC with an active read. Measure metadata selection at large file counts before production use.

Implement the store-api contracts, RegionServer handle, Mito snapshot preparation and injected handler dispatch. Before exposing physical metric Regions, add explicit forwarding from MetricEngine to Mito. Logical-table requests also require table scope and column mapping; they must not be forwarded as unfiltered physical requests.

Validate the RegionServer API against real SST/Puffin files: overlapping files, missing statistics, cache coalescing, corruption, NULLs, scope changes and unflushed rows. Cover fresh snapshots, unsupported capabilities, timestamp boundaries including sub-millisecond requests and files spanning the request, retained snapshot lifetimes, order-independent lazy selection, descriptor-only gaps and shared decoding for multiple quantiles. Measure metadata selection and memory behavior at large file counts.

# Drawbacks

Long-running reads retain the Region's full SST snapshot. The API also returns physical, potentially incomplete distributions, so applications must inspect coverage before using an estimate. An RPC needs a response-byte budget in addition to the current entry quota.

# Unresolved Questions and Future Work

- Candidate discovery (which values dominate) needs an operation that returns retained MG candidates. It can use the existing payload format.
- Requests apply one shared query list to all selected columns. Per-column request items are future work; candidate discovery should use that shape.
- Should a future response explicitly report excluded boundary intervals?
- What completeness threshold should an application require before automatic repartition?
- Is metadata indexing needed for Regions with very large SST counts?

# Alternatives

Adding query methods directly for each algorithm would spread capability checks and lifecycle logic. A generic index-statistics method with a mode flag obscures coverage-only behavior. A system table is useful for inspection but does not replace a bounded merge operation. The Region-bound handle keeps the application API small while preserving an optional engine capability.
