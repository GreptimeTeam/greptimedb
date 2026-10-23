---
Feature Name: Exact metadata counts for non-append Mito tables
Tracking Issue: N/A
Date: 2026-10-22
Status: Experimental prototype
---

# Summary

Allow ordinary SQL `COUNT(*)` queries over non-append Mito tables to avoid SST
page reads when file metadata and the pinned scan view prove the exact result.
The first implementation supports a deliberately restricted set of scans and
retains the normal scan whenever the proof is incomplete.

The prototype is enabled with `region_engine.mito.experimental_metadata_count`.
It is disabled by default. It builds on DataFusion's aggregate-statistics rule
and GreptimeDB's existing rewrite from `COUNT(*)` to the non-null time index.
Distributed `__count_state` aggregates are also eligible at the region server.

# Motivation

Physical SST row counts do not generally equal query-visible row counts on
non-append tables. Files may contain multiple versions or deletion markers;
other SSTs and memtables may override rows; predicates and region partition
filters may select only part of a file. Some legacy FileMeta records also omit
physical row counts, represented by zero rather than an empty file.

Statistics used as query results need a stronger contract than estimates used
for costing. This first step establishes that contract and a normal SQL
execution path that can consume it.

# Constraints

- No additional per-row work or I/O on ordinary ingestion for this prototype.
- Reuse existing compaction deduplication and deletion filtering results.
- Metadata preparation is optional, asynchronous, bounded and cancellable.
- Synchronous execution-plan statistics never perform I/O or wait on a future.
- Missing evidence, unsupported semantics and exhausted preparation budgets
  leave ordinary execution available; no partial count is published as exact.
- Metadata loading must not trigger SST background downloads or data-page
  prefetch. A footer lookup reads only the trailer and footer ranges.

# Persisted file capability

`FileMeta.num_rows_is_logical` means the physical rows in this immutable file
are unique by primary key and timestamp, with no deletion markers. It says
nothing about query predicates, sequence visibility or other sources.

The field defaults to false and is omitted from serialization when false.
The prototype sets it only for newly rewritten non-append compaction outputs
using `last_row` with deletion filtering enabled. Their existing `num_rows`
count is sufficient; no additional pass over records is needed. Flush outputs
remain uncertified. Future work may certify more write paths if their input
invariants can be proved without meaningful ingestion overhead.

Compaction computes the capability from the actual output pipeline, never by
summing logical row counts from inputs. Merely moving/reusing an existing SST
does not acquire a new capability. Old files remain readable and may acquire
the capability when normal compaction actually rewrites them. Old writers that
drop the optional field only remove optimization eligibility.

# Query eligibility

The logical scan-hint rule recognizes one ungrouped COUNT directly over a table
scan, including count of the time index and a non-null literal. It excludes
DISTINCT, aggregate filters, residual filter nodes, computed projections and
inner limits. The provider request is cloned per scan use-site so shared
providers do not leak hints to other branches.

The Mito engine checks the same pinned input used by normal execution:

1. No relevant memtable ranges or enterprise extension ranges.
2. No sequence-range intent, explicit sequence fences, source-skipping intent,
   row selector or scan limit. Non-append scans use `last_row`.
3. Predicates consist only of same-type time-index/literal comparisons joined
   by AND. Each file is completely excluded or completely included. Partial
   coverage, OR, NOT, field/tag predicates and casts fall back.
4. Region partition filtering is absent or proven redundant for every selected
   file using the existing partition-expression check.
5. Each selected non-append SST has the file capability, and their inclusive
   time ranges are strictly disjoint. This excludes cross-file duplicate and
   deletion effects. Time ranges touching at an endpoint are treated as overlap.

The scanner publishes `exact_num_rows: Option<usize>` for the complete static
scan. This is separate from the existing physical-source count. Dynamic-filter
attachment and logical-region wrapping invalidate the prepared count.

`RegionScanExec` exposes exact row counts and only schema-guaranteed zero null
counts. It does not infer distinct counts or nullable-column null counts.
DataFusion's existing AggregateStatistics rule can then eliminate the scan.

# Asynchronous footer preparation

After eligibility checks, physical counts come from manifest metadata, decoded
metadata cache, or a bounded footer fetch, in that order. The footer count can
repair a missing physical count but cannot certify deduplication of a legacy
non-append file. Files lacking the logical capability fall back before I/O.

Current fixed prototype limits, per region scan:

- At most 64 candidate source files examined.
- At most 8 uncached footer fetches.
- At most 1 MiB of cumulative footer range reads, reserved before each request.
- A 20 ms preparation timeout and one in-flight fetch at a time.

Metadata is cached by immutable file identity. No manifest writeback is done
on the query path. Failed optional metadata acquisition retains ordinary scan
error handling: it neither invents rows nor turns an unreadable SST into empty
data. Dropping the enclosing query future cancels the awaited preparation;
there are no detached read tasks. A preparation timeout falls back only while
the enclosing query is still active.

These limits are per region, not a global distributed-query budget. Sharing a
budget across region planning/RPC fanout is an explicit follow-up. The prototype
does not introduce a new cross-node global snapshot guarantee.

# Validation

- Frozen legacy FileMeta fixture under `tests/compatibility/mito/metadata_count`:
  missing fields, physical counts without logical trust, optional serialization.
- Engine test: overlapping writes, deletion, compaction, manifest reopen, SQL
  aggregate elimination and zero SST opens at the object-store boundary.
- Eligibility tests: partial time coverage, OR, non-time predicates, overlap at
  boundaries, sequence fences, source limits and empty selections.
- Footer tests: exact byte budget, cache reuse, timeout and cancellation.
- SQLness case: ordinary SQL results before/after flush and compaction, partial
  predicates, nullable counts and writes/deletes after compaction.

Performance evaluation must compare eligible COUNT, rejected COUNT, ordinary
queries and ingestion/compaction with the feature disabled and enabled. Passing
functional tests alone does not establish a latency or throughput guarantee.

# Deferred work

More selective key-range disjointness, hybrid metadata-plus-data counts,
row-group partial coverage, last-non-null semantics, memtable contributions,
additional flush certification, and globally shared preparation budgets are
outside the first implementation. Broader CBO estimates can reuse trustworthy
facts without treating approximate estimates as exact query results.
