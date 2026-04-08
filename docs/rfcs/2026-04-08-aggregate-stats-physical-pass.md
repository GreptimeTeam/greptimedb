---
Feature Name: Aggregate Statistics Physical Optimizer Pass
Tracking Issue: TBD
Date: 2026-04-08
Author: @OpenAgent
---

# Summary

This RFC proposes a new physical-plan optimization pass for aggregate queries in GreptimeDB.
Instead of relying primarily on DataFusion's relation-level `Statistics/ColumnStatistics` to optimize whole-query planning, the new pass opportunistically consumes per-file statistics during physical optimization and execution planning.

If some files have usable statistics for an aggregate, GreptimeDB will avoid scanning those files and synthesize partial aggregate state from their metadata. Files whose statistics are missing, incompatible, or unsafe to use will still be scanned normally. The two paths are then merged with the existing state/merge aggregate wrapper mechanism so the query remains correct while extracting as much value as possible from available statistics.

# Motivation

Today GreptimeDB's aggregate optimization is still mostly constrained by DataFusion's `ColumnStatistics/Statistics` model.
That model is useful for planner-level estimation and some coarse optimizations, but it is not a good fit for GreptimeDB's desired behavior:

1. We want to optimize at the **physical file level**, not only at the relation level.
2. We want to support **mixed execution**:
   - files with usable statistics -> answer from stats
   - files without usable statistics -> fall back to scan
3. We want this to work even when only **part of the input** can benefit.
4. We want the optimization to degrade gracefully when file statistics become unavailable or semantically unusable, for example after repartition or other transformations.

A physical pass is a better fit because it can inspect the concrete scan node, the concrete aggregate node, and the exact file set that would otherwise be scanned.

# Goals

1. Add a GreptimeDB physical optimizer rule that rewrites eligible aggregate plans to use file statistics opportunistically.
2. Allow `RegionScan` / `ScanInput` to skip files already covered by statistics-derived aggregate state.
3. Reuse the existing step-aggregate state/merge mechanism instead of inventing a separate aggregation framework.
4. Preserve correctness by falling back to normal scan whenever statistics are unavailable or unsafe.
5. Make mixed execution explicit and observable in `EXPLAIN` and metrics.

# Non-Goals

1. This RFC does **not** attempt to replace all DataFusion statistics usage.
2. This RFC does **not** try to optimize arbitrary expressions above aggregates.
3. This RFC does **not** guarantee support for every aggregate function in v1.
4. This RFC does **not** require exact group-by-from-statistics optimization in v1.
5. This RFC does **not** require statistics reconstruction for files whose metadata has already lost the needed semantics.
6. This RFC does **not** require support for non-append-only tables in v1.

# Proposal

## 1. Add a physical optimizer pass

Add a new `PhysicalOptimizerRule` in GreptimeDB's query engine, alongside existing rules such as `ParallelizeScan` and `PassDistribution`.

Conceptually, the rule targets plans of the form:

```text
AggregateExec
  RegionScanExec
```

or other small variants where the aggregate is still directly attributable to a single `RegionScanExec` input.

The rule will:

1. inspect the aggregate expressions;
2. inspect the underlying region scan and candidate files;
3. classify each file as either:
   - **stats-eligible**, or
   - **scan-required**;
4. rewrite the physical plan into a mixed plan that merges:
   - partial aggregate state synthesized from file statistics, and
   - partial aggregate state produced by scanning only the remaining files.

This is a GreptimeDB-specific optimization and should live in GreptimeDB's physical optimizer pipeline, not in generic DataFusion statistics estimation.

## 2. Optimization shape

The core idea is to convert one aggregate over raw rows into one merge aggregate over two partial-state sources.

For an original query like:

```sql
select max(v) from t;
```

the optimized execution is conceptually equivalent to:

1. compute state from scanned files: `__max_state(v)`
2. compute state from stats-only files: `__max_state(file_max(v))`
3. merge them with `__max_merge(...)`

The exact expression syntax above is illustrative only. The physical rewrite should use the existing state/merge wrapper machinery rather than depend on SQL syntax.

A conceptual physical shape is:

```text
AggregateExec(mode=Final, aggr=[__max_merge(state_col)])
  UnionExec / MixedPartialSourceExec
    AggregateExec(mode=Partial, aggr=[__max_state(v)])
      RegionScanExec(files = scan-required)
    Literal partial-state input / values=[__max_state(file_max(v)), ...]
```

The same pattern extends to other supported aggregates.

For v1, this RFC prefers **literal partial-state input** over a dedicated `StatisticsStateExec`.
The main reason is that the needed statistics are easiest to collect during optimization / rewrite, when the optimizer still has direct access to the concrete aggregate shape and the concrete file set under `RegionScanExec`.
At that point the optimizer can classify files, compute stats-derived partial states, and bake those states directly into the rewritten plan as constants or precomputed batches.

By contrast, a dedicated `StatisticsStateExec` would need its own execution-time path to rediscover or reload the same file statistics, which adds another metadata access boundary and makes the feature more complex without changing the core semantics.

## 3. File classification

For each candidate file in the underlying scan, the optimizer classifies whether it can contribute via statistics.

A file is **stats-eligible** for a given aggregate only if all required conditions hold.
Typical examples:

- `MAX(col)`: file has usable max statistics for `col`
- `MIN(col)`: file has usable min statistics for `col`
- `COUNT(*)`: file has exact row count
- `COUNT(col)`: file has exact row count and null count for `col`

A file is **scan-required** if any of the following apply:

- required statistic is missing;
- required statistic is known to be inexact or semantically unsafe;
- the file contains semantics not captured by the statistic needed by this aggregate;
- the file has gone through transformations where the available metadata can no longer safely answer the aggregate (for example, after repartition and before compaction);
- the query shape prevents file-level attribution.

This classification is per file, not all-or-nothing for the whole query.

## 4. Why physical pass instead of `Statistics/ColumnStatistics`

The current `RegionScanExec::partition_statistics()` integration is relation-oriented and coarse.
It is useful for estimation and some generic optimizer decisions, but it cannot naturally express:

- a query answered by **some files from stats and some files from scan**;
- skipping specific files in `ScanInput.files` while still scanning others;
- building synthetic partial aggregate input from per-file metadata;
- graceful fallback when a subset of files lose usable statistics after repartition or similar operations.

In other words, `Statistics/ColumnStatistics` is still useful metadata, but it is no longer the right execution boundary for this feature.

## 5. Supported aggregates in v1

The recommended v1 scope is deliberately narrow:

1. `MAX(col)`
2. `MIN(col)`
3. `COUNT(*)`
4. `COUNT(col)`

These functions map naturally to common file statistics.

### Deferred from v1

- `SUM(col)`
- `AVG(col)`
- `FIRST_VALUE` / `LAST_VALUE`
- `DISTINCT` aggregates
- grouped aggregation from file statistics

`SUM/AVG` are intentionally deferred unless GreptimeDB has exact, semantics-preserving file-level summaries for them. Reusing the step-aggregate framework does not by itself make them safe; the underlying statistics must still be correct and complete enough.

# Detailed Design

## 1. Eligibility rules

The physical rule should only fire when all of the following hold:

1. The aggregate node is recognized and all aggregate expressions are in the supported set.
2. The aggregate input can still be traced to a concrete `RegionScanExec` file set.
3. The query shape is single-stage or can be safely rewritten into partial/final form.
4. There is no intermediate operator that destroys file-level attribution needed by this optimization.
5. At least one file is stats-eligible.
6. The underlying table is append-only in v1.

The optimizer should bail out if the input has already crossed a boundary where "which file contributes which rows" is no longer meaningful for this optimization, for example after repartition or exchange that hides the original file set.

## 2. `RegionScan` and `ScanInput` changes

This RFC proposes that the physical rewrite eventually drives scan execution by excluding files already covered by statistics-derived partial state.

At a high level, the scan path needs one of these equivalent capabilities:

1. construct a new `RegionScanExec` whose scanner produces a `ScanInput` containing only `scan-required` files; or
2. pass an explicit `excluded_file_ids` / `stats_covered_files` hint into the scanner so `ScanInput.files` omits those files.

The key requirement is:

- **stats-eligible files must not be scanned again**.

The memtable path remains unchanged in v1 and is always scanned normally.

## 3. Materializing statistics-derived partial state

For v1, the recommended design is to materialize stats-derived partial state during optimization and embed it into the rewritten plan as literal values or precomputed batches.

Responsibilities of this materialization step:

1. compute stats-derived partial aggregate states during rewrite;
2. expose a schema compatible with the upper merge aggregate;
3. feed one or more state rows into the merge side of the aggregate;
4. preserve enough explainability to show how many files were answered from statistics.

This keeps the feature optimizer-driven: the same rewrite pass that classifies files also decides which files are skipped and what partial state replaces them.

If a future version needs lazy metadata access or reusable stats computation, GreptimeDB can still introduce a dedicated `StatisticsStateExec` later.

## 4. Reusing state/merge wrappers

GreptimeDB already has step aggregate infrastructure in `aggr_wrapper` and distributed planning.
This RFC proposes reusing that model directly.

Instead of introducing a separate "stats aggregate result" merge path, the optimizer should normalize both sources into the same intermediate representation:

- scan path -> ordinary partial aggregate state
- stats path -> synthetic partial aggregate state

Then the upper merge aggregate remains the single correctness boundary.

This has two advantages:

1. mixed execution becomes structurally uniform;
2. future aggregate extensions can piggyback on the same state/merge contract.

## 5. Correctness rules

Correctness is more important than hit rate.
The optimizer must fall back to scan whenever correctness cannot be proven.

### 5.1 Null semantics

Statistics-based answers must preserve SQL null semantics.
For example:

- `COUNT(*)` uses exact row count
- `COUNT(col)` requires exact null count semantics
- `MIN/MAX(col)` must not treat missing stats as real values

### 5.2 Delete / merge semantics

If a file's visible query result can differ from simple file statistics because of deletion markers, merge semantics, or other storage-level visibility rules not reflected in the statistic, that file is scan-required.

For that reason, a conservative v1 can explicitly restrict the optimization to **append-only** tables.
In append-only mode, the correctness surface is much smaller because files do not need stats-based reasoning across delete markers or row replacement semantics.
This significantly reduces the chance of classifying an unsafe file as stats-eligible.

### 5.3 Mixed correctness

The final answer must be the same as scanning all files.
The mixed plan is valid because it partitions the input file set into disjoint subsets:

- subset A -> answered by stats-derived state
- subset B -> answered by scan-derived state

and merges both through the same aggregate state contract.

### 5.4 Repartition and degraded metadata

After repartition or similar operations, some files may no longer have usable metadata for this optimization.
That is an expected case, not an error.
The rule should simply classify those files as scan-required and proceed with a mixed or pure-scan plan.

One concrete example is the period **after repartition and before compaction**.
In that window, files may still physically originate from older partitioning/layout assumptions, while the current read path is already interpreting them under the new region layout.
Even if some original file statistics still exist, they may no longer be a safe answer source for the current aggregate optimization contract.
So those files should be treated as scan-required until a later compaction regenerates files and metadata under the new layout.

## 6. Explain and observability

The optimized plan should be visible in `EXPLAIN`.
At minimum we should be able to tell:

- the aggregate was rewritten by the stats physical pass;
- how many files are answered from statistics;
- how many files remain in scan;
- whether the stats side is literal/precomputed input;
- which aggregate functions are optimized.

Recommended metrics:

- aggregate-stats eligible files
- aggregate-stats skipped files
- aggregate-stats fallback files
- aggregate-stats queries hit/miss

# Rollout Plan

## Phase 1: MVP

1. Add the physical optimizer rule.
2. Restrict the optimization to append-only tables.
3. Support `MIN/MAX/COUNT(*)/COUNT(col)`.
4. Materialize stats-derived partial state as literal/precomputed input during rewrite.
5. Add the ability for scan planning to skip stats-covered files.
6. Add `EXPLAIN` output and metrics.

## Phase 2: Scope expansion

1. Revisit support for `SUM/AVG` only if exact semantics are available.
2. Revisit non-append-only tables once delete / merge visibility semantics are modeled safely.
3. Consider grouped aggregation if file-level summaries can safely support it.
4. Explore better costing / heuristics when using statistics is possible but not necessarily profitable.

# Testing Plan

1. Unit tests for file classification by aggregate type.
2. Unit tests for stats-state materialization.
3. Plan rewrite tests for:
   - pure stats
   - mixed stats + scan
   - pure fallback scan
4. Correctness tests comparing optimized vs non-optimized answers.
5. Edge-case tests for:
   - null-heavy columns
   - missing statistics
   - memtable + SST mixed inputs
   - repartitioned / degraded-stat files
   - append-only gating
6. `EXPLAIN` tests to verify plan visibility.

# Risks

1. Incorrectly classifying a file as stats-eligible would produce wrong answers.
2. Forcing this optimization too broadly may complicate aggregate planning and debugging.
3. The physical rewrite may become awkward if state/merge wrappers remain only partially exposed at the physical layer.
4. If scan skipping is not wired cleanly into `RegionScan` / `ScanInput`, the implementation may accidentally double count files.
5. Embedding too much precomputed state directly in the plan may become awkward if future workloads rely on much larger stats-derived inputs.

# Alternatives

## 1. Continue to rely on `Statistics/ColumnStatistics`

Rejected for this feature because it cannot naturally express file-level mixed execution with scan skipping.

## 2. Add a storage-side aggregate API only

This would hide some complexity in storage, but it makes the optimization less transparent at the query layer and harder to compose with existing state/merge aggregate infrastructure.

## 3. Require all files to have usable statistics before optimizing

Rejected because it gives up the main benefit of this design: partial wins are still wins.

## 4. Introduce `StatisticsStateExec` in v1

Deferred.
It may become useful later, but for the current scope the optimizer already has the most convenient place to read and classify the relevant file statistics.

# Open Questions

1. Is `UnionExec` over scan-state and literal/precomputed stats-state sufficient, or do we still want a dedicated mixed-source helper node?
2. Where is the cleanest API boundary for excluding stats-covered files from `RegionScanExec`?
3. Should v1 support only aggregates without `GROUP BY`, or should we allow a narrow grouped case when grouping columns align with file partition metadata?
4. Do we want a session option to disable this pass for debugging and staged rollout?

# Conclusion

The desired behavior is not "use statistics instead of scan when relation-level statistics happen to be complete".
The desired behavior is "at physical planning time, exploit file statistics wherever they are correct and fall back to scan for the rest".

A dedicated GreptimeDB physical optimizer pass is the right abstraction boundary for that behavior.
It matches the concrete file-level execution model, composes naturally with `RegionScan` / `ScanInput`, and can reuse the existing step aggregate state/merge design to keep mixed execution both efficient and correct.
