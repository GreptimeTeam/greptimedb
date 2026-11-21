---
Feature Name: Explicit Index Build Plans
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/10291
Date: 2026-11-09
Author: "WenyXu <wenymedia@gmail.com>"
---

# Summary

Make index planning explicit while retaining the existing Mito index lifecycle. Built-in indexes and injected extension handlers use the same plan, creator, Puffin writer, and publication flow. Enterprise supplies algorithm implementations; OSS owns scheduling, row delivery, file ownership, and manifest installation.

The companion *Sketch Reader Framework* describes the read API.

# Motivation

Adding an index should not require a second compaction or rebuild pipeline. Decisions about eligible columns, input reuse, row scanning, and successful output metadata need explicit ownership. In particular, preparing an index after decoding an SST is too late to avoid an unnecessary rebuild scan.

The [Async Index Build RFC](2025-08-16-async-index-build.md) already defines scheduling and installation. This proposal changes how index work is planned and executed inside that lifecycle; it does not replace its concurrency protections.

## Goals

Keep built-in index behavior and file publication under the existing engine lifecycle. Let injected handlers plan work before SST decoding and reuse compatible inputs.

## Non-Goals

This RFC does not define sketch algorithms, time-window formats or an application query API.

# Details

## Responsibilities

| Component | Responsibility |
| --- | --- |
| `IndexKindProvider` | Supply extension handlers through plugin injection |
| `IndexRegistry` | Register unique owners, validate required kinds, assemble plans |
| `IndexKindHandler` | Validate configuration and resolve typed plans |
| `PlannedIndex` | Describe entries, check currency, prepare one output creator |
| `IndexCreator` | Consume borrowed batches or reuse inputs, write completed blobs |
| Existing `Indexer` | Coordinate creators, resource accounting, errors and completion |
| Existing storage lifecycle | Close Puffin, install metadata, clean up files |

`IndexKind` distinguishes `BuiltinIndexKind` and `ExtensionIndexKind`. HLL, KLL and MG have one extension identity each; there is no second built-in HLL identity. Registration rejects overlapping ownership. Runtime kind identity maps to persisted `IndexType`. Executable kinds are a finite OSS set. Injection supplies implementations for those kinds. Adding one requires updating kind classification, output descriptors and required-provider validation together; tolerant metadata parsing does not add an implementation.

A handler may own several related kinds and prepare one creator. This permits shared traversal without forcing users to enable all algorithms together. Built-in handlers remain organized by index type.

## Planning and execution

```text
flush / compaction / BUILD INDEX
  -> registry validation and typed IndexBuildPlan
  -> PlannedIndex::build(BuildContext), once per output
       -> inspect compatible input indexes when reuse is allowed
       -> create row-consuming or reuse-only creator
  -> shared batch stream, only when index work requires it
  -> IndexCreator::finish(existing SstPuffinWriter)
  -> close Puffin successfully
  -> publish completed column/index descriptors through existing lifecycle
```

Flush builds built-in indexes only. Compaction also builds configured extension indexes, subject to admission budgets. Rebuild prepares replacement indexes and retains the old generation on failure; whether work is needed follows the rebuild reason and currency checks. Multi-region admission may omit extension work while retaining built-in work.

The immutable plan is shared across outputs; mutable algorithm state is output-specific. `PlanContext` carries schema, index options, lifecycle event and existing descriptors. `BuildContext` adds pinned inputs, input/output Puffin managers, output identity, reuse constraints and optional resource limits.

The key contracts are:

```rust
// Selected creator methods; ancillary methods are omitted.
/// Reports whether output rows are needed after input reuse is prepared.
fn requires_row_scan(&self) -> bool;
/// Consumes a borrowed legacy batch in the shared output stream.
async fn update(&mut self, batch: &mut Batch) -> Result<(), BoxedError>;
/// Consumes a borrowed flat batch without taking ownership of its buffers.
async fn update_flat(&mut self, batch: &RecordBatch) -> Result<(), BoxedError>;
/// Writes completed blobs; the engine closes Puffin and publishes descriptors.
async fn finish(&mut self, writer: &mut SstPuffinWriter)
    -> Result<Vec<CompletedEntry>, BoxedError>;
```

Both legacy and flat batches remain supported. Creators borrow batches; the engine can share primary-key decoding. This avoids an SST decode per index, but does not promise that independent algorithms perform no separate CPU traversal. A shared extension creator can eliminate repeated traversal and encoding internally.

A rebuild prepares creators before opening Parquet when the plan permits it. Only inverted-index plans need the actual row-group size early for segment alignment; other plans defer footer and reader construction until a creator requires rows. Reuse-only and metadata-only rebuilds without an inverted-index plan therefore avoid Parquet footer and row reads. Compaction still reads rows to produce its output SST; skipping index accumulation does not eliminate that data rewrite. Reuse saves creator updates, value extraction and encoding, and can reduce algorithm-state memory; it does not remove the compaction data I/O.

## Reuse is a scope permission

`can_merge_inputs` permits coarse input reuse only when the operation covers the complete input scope without rollover, time slicing or Region filtering, and required input objects are available. Multiple possible outputs require output-specific accumulation; a whole-input summary cannot be attached to each output.

The flag permits input-derived statistics, which may differ from live output rows. Current compaction may deduplicate or remove deleted rows. Approximate handlers that accept this must preserve input-derived coverage separately from actual output row counts. Input-derived provenance is preserved in Puffin properties for subsequent reuse, not exposed by the read API. Consumers must treat results as physical statistics that may include reused input contributions. Repeated reuse can retain removed contributions until rebuilding from output rows. An exact index requires row equivalence separately. Payload compatibility is checked by the handler, not inferred from the flag. Once every algorithm has fallen back to output-row accumulation, preparation stops opening later input indexes.

## Shared Puffin access

Use the existing Puffin manager for both built-in and extension indexes. Its `PuffinFileKey` provides one cache identity for Bloom, inverted and sketch data: an Arc-shared structured storage scope (backend instance, scheme, name and root), the exact object path, and the immutable file handle including index generation. A generation is the version of the Puffin object attached to an SST; `RegionIndexId` combines Region/file identity with that version. Blob/page keys extend that identity; handlers do not construct parallel namespace strings or introduce another storage-reader trait. Obtaining a key or reader does not load the footer; metadata remains lazy and concurrent first reads share its initialization.

A shared Puffin factory reuses backend identity for cloned operators. Independently created operators or factories conservatively use different cache identities, even when their displayed scheme/name/root match. This separates distinct S3 endpoints with identical bucket names without inferring endpoint identity from those labels.

This identity change covers Puffin footer and the affected in-memory index/sketch caches. Disk staging, write-cache and index-result-cache identities retain their existing lifecycle; this is not a migration of every cache.

## Configuration and persistence

Plans select required indexes from the current column configuration. OSS owns configuration validation and provider checks; enterprise supplies algorithm execution and payload codecs. New-region creation calls the registered handlers' configuration validation before creating its manifest. Recovery of an existing region does not rerun that strict validation. Required-provider checks apply when compaction or maintenance plans build these indexes, rather than being a universal create/open check.

Completed types are recorded in both `ColumnIndexMetadata.created_indexes` (per column) and `FileMeta.available_indexes` (file-level index types), using the existing manifest model. No parallel sketch descriptor list is introduced. `FileMeta::exists_index()` recognizes both file-level available types and per-column created types, so a sketch-only file participates in index-generation bookkeeping and cleanup even without legacy file-level markers. Extension formats are treated as the first version at this layer; `is_current` checks whether manifest descriptors satisfy the requested build; it cannot inspect Puffin payloads. Ordinary `BUILD INDEX` can skip files whose manifest descriptors match, so it does not guarantee payload-format migration. Reuse must validate payload properties. The existing built-in fulltext version conversion remains checked and fallible.

## Compatibility and Rollout

Change the elements of both `FileMeta.available_indexes` and `ColumnIndexMetadata.created_indexes` from a closed enum to a string-preserving representation: known names resolve to supported kinds, while unknown names retain their original spelling. Keep the existing serialized string names unchanged so manifest decoding and rewriting preserve both fields. Loading metadata must not require an implementation for every recorded index. An operation that requires an unsupported index returns `Unsupported`. Compaction rejects kinds still required by the current configuration, but may omit unavailable kinds left only in input descriptors after configuration removal. It records the omission and publishes no descriptors for those kinds in the new output. Rebuild remains strict: unavailable persisted kinds cause failure and retain the existing index generation. Neither path copies unknown descriptors into new Puffin generations or introduces a scan fallback.

Rollback is supported only to versions that recognize the stored kinds or implement this tolerant decoding contract. All potential manifest readers and compactors must meet that minimum before column options are enabled: datanodes, standalone servers, compactors, followers/read replicas and recovery tools. Older versions that reject unknown variants remain unsupported rollback targets. Disabling writes does not remove existing descriptors. Record the minimum supported version when the compatibility reader ships.

Unknown names need bounded warnings, without logging once per SST. Validate preservation of both descriptor fields and rejection of unsupported operations before enabling writes that depend on this compatibility contract.

## Build types and target sets

A rebuild replaces the whole Puffin generation, so its plan must include every built-in target the current schema still requires, including JSON-path inverted targets. Whenever inverted-index construction is enabled, its target set includes JSON-path targets for every build type; `Manual` and `SchemaChange` must not omit them. Flush and compaction still honor their existing automatic-build settings. An extension that reports a file as not current can trigger replacement even when built-in descriptors already match; that replacement must retain the configured JSON-path indexes.

## Failure and resource policy

A kind still enabled in the current configuration requires a registered provider; otherwise compaction fails before input/output I/O. Both local-unit and whole-region entrypoints perform this preflight. If configuration has been removed and only persisted descriptors remain, compaction may omit kinds without a provider. It warns and increments `greptime_index_create_discarded_total{type, reason="missing_provider"}` once per distinct unavailable kind within each target Region preflight. The output contains no descriptors for those omitted kinds. The reader returns `NotConfigured` while the algorithm is disabled, or `MissingIndex` if it is later enabled without rebuilding those files. BUILD INDEX still validates both configured and persisted kinds before scheduling tasks; file-specific checks also run in each task. Registration errors propagate rather than falling back to a registry without the injected handlers, including the second registry assembly during multi-region source preparation. This preserves the existing production skip policy for ordinary built-in preparation failures.

Multi-region admission can omit extension plans before creators exist. `omit_extensions_for_budget(region_id)` retains built-in plans and records each requested extension kind once per target plan, even when several columns request it. Warnings include the target Region, kind and `admission_budget` reason; the existing `greptime_index_create_discarded_total{type, reason}` counter exposes that omission. Repeating the operation on an already filtered plan does not count it again.

During construction or updates, memory-budget discard is recorded as a set of kinds and increments the same counter with reason `memory_budget`. Write paths warn while continuing data publication. Rebuild rejects replacement if discarded kinds intersect the old file's index kinds, preserving the old generation. Dropping only newly requested kinds can warn without removing an existing index. These records are operation-local; the manifest does not persist a discard reason or coverage gap. Ordinary planning or creator errors remain recorded failures, distinct from budget discard, and prevent rebuild replacement.

An extension update error aborts that creator; other creators can continue on write paths. Rebuild rejects the failed replacement. Finish is all-or-nothing because creators share one Puffin writer: a finish, completion-validation or close failure publishes no index descriptors from that output. There is no per-creator rollback after bytes enter the shared writer.

Creators release retained state on drop and temporary resources through their lifecycle methods. Successful descriptors are published only after Puffin finalization. Existing rebuild checks still prevent installing results for obsolete or compacting files.

Use existing index metrics. A shared extension creator reports aggregate time and retained memory once; algorithm-attributable work can be measured by its implementation. Splitting aggregate measurements among every owned kind would double count.

## Interface Sketch

The following omits concrete error and metadata types; it describes ownership rather than a copy-paste implementation.

```rust
trait IndexKindProvider {
    /// Supplies handlers once when assembling the index registry.
    fn handlers(&self) -> Vec<Arc<dyn IndexKindHandler>>;
}

trait IndexKindHandler {
    /// Declares the index kinds exclusively owned by this handler.
    fn kinds(&self) -> &[IndexKind];
    /// Resolves typed work from metadata without decoding SST rows.
    fn plan(&self, context: &PlanContext) -> Result<Option<Arc<dyn PlannedIndex>>>;
}

trait PlannedIndex {
    /// Lists the column/kind outputs requested by this immutable plan.
    fn entries(&self) -> &[PlanEntry];
    /// Prepares one output creator and decides reuse before row scanning.
    async fn build(&self, context: &BuildContext) -> Result<Option<Box<dyn IndexCreator>>>;
    /// Checks manifest-level currency; payload format checks belong to reuse.
    fn is_current(&self, file: &FileMeta, context: &CurrencyContext) -> bool;
}
```

`PlanContext` describes requested work. `BuildContext` supplies output identity, pinned inputs, Puffin managers and reuse/budget constraints. `IndexCreator` owns mutable state and follows the batch/finish contract above; it does not publish the manifest.

# Implementation Plan

Implement the registry, built-in adapters, extension hooks, output metadata and lifecycle integration. Complete tolerant manifest decoding before enabling writes that require the new rollback contract. Review must compare existing built-in eligibility, batch order, spill behavior, error handling and installation behavior before and after the refactor.

Targeted tests must cover both compaction entrypoints, configured-provider rejection, omission of unavailable residual descriptors, ownership conflicts, discarded kinds, rebuild preservation, output metadata, and reuse versus row accumulation. A rebuild regression must read the JSON-path inverted blob after extension maintenance replaces the Puffin generation. A separate rebuild regression must overwrite Parquet bytes and verify that reuse-only work still succeeds, while a row-consuming creator must read the file. This checks input I/O rather than inferring it from update-call counts. Performance acceptance additionally requires controlled comparisons with extensions disabled, one enabled, and all enabled; unit tests alone do not establish absence of overhead.

Unknown-kind parsing needs round-trip tests for both descriptor fields, Region recovery tests and unsupported-operation tests. Quantify reuse against reconstruction with fixed datasets, row counts, merge mode and deletion ratios. Compare distinct-count relative error, MG frequency estimates against both input and surviving-row counts, KLL p50/p95/p99 shifts, and downstream split-column and partition-count choices. Before automatic repartition consumes these results, its owner must define and validate decision tolerances; zero drift is not the criterion. Persisted `represented_rows` and `output_rows` can expose the row-count difference without another scan; they cannot measure or reconstruct the distribution difference.

# Drawbacks

Reused summaries describe their represented physical input, which may differ from surviving output rows. Algorithm error guarantees do not bound that population difference. Applications must account for this when using estimates; neither counts nor ratios have a promised direction of drift.

Current flush wiring builds built-in indexes without injecting extension handlers. Newly flushed SSTs can therefore lack sketches until compaction; the reader must report that gap. Single-region compaction and rebuild do not supply a unified extension total-memory budget. Per-creator accounting is not a process RSS limit. These are explicit follow-up items.

# Unresolved Questions and Future Work

A future `force` flag for `BUILD INDEX` should bypass currency checks and disable input-summary reuse, rebuilding from SST rows. It is not part of the current implementation.

- Where should the shared budget for single-region compaction and rebuild originate?
- When should flush opt into extension handlers?
- Which released version will be the minimum supported rollback target?

# Alternatives

A separate extension writer would duplicate publication and cleanup. A single opaque callback would hide scan requirements. Requiring one creator per algorithm would prevent shared traversal. The proposed plan/creator split avoids those constraints at the cost of more explicit contracts.
