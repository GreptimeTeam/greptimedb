---
Feature Name: Time-Slice Aggregate Cache
Tracking Issue: TBD
Date: 2026-11-18
Author: "Dennis Zhuang <killme2008@gmail.com>"
---

# Summary

Cache the partial states of an aggregation over a region scan, one entry per aligned time slice, on the node that runs the scan. A repeated or sliding dashboard query reuses the slices whose data has not changed and recomputes only the rest. A cached slice is valid exactly as long as the region reports the same data fingerprint for its time range, so late writes, deletes, flushes, compactions and TTL never return a stale result.

# Motivation

Dashboards run the same aggregation every few seconds over a window that slides forward: `count(*)`, `avg(latency)` or `sum(bytes)` grouped by a time bucket and a few tags over the last 24 hours. Each refresh scans and aggregates the whole window again, although all but the newest few minutes of it have not changed since the previous refresh.

GreptimeDB has no cache that covers this. The query layer has no result cache. The mito range result cache stores scanned rows, not aggregation results, and only for scans with tag filters. Flow can maintain the aggregate continuously, but it has to be declared ahead of time for each query shape, and ad hoc dashboards are not declared.

OpenObserve caches merged partial aggregation states per time partition. It does not invalidate them on writes: a window that ends within `cache_delay_secs` (300 seconds by default) of now is not reused, and data that arrives later than that for a cached window is not reflected until the cache is cleared by hand. We want the reuse without that staleness window.

# Goals

1. Reuse aggregation work across queries whose time ranges overlap, including windows that slide forward.
2. Return exactly what an uncached execution returns, including after late writes, deletes and any change to the files of the region.
3. Work in standalone and distributed mode without a new protocol between frontend and datanode.

# Non-Goals

- Caching final results or raw rows. Only partial aggregation states are cached.
- PromQL. Its range evaluation has a different plan shape; it can reuse the fingerprint later (see Future Work).
- Sharing cache entries across nodes or persisting them. The cache is in memory and local to a node.
- Caching slices that still have unflushed data. They are recomputed on every query.

# Proposal

## Architecture

```
Final AggregateExec                       (unchanged)
  └─ SliceAggregateExec                   replaces Partial AggregateExec
       ├─ slice [00:00, 01:00) ── fingerprint matches ──> cached partial states
       ├─ slice [01:00, 02:00) ── fingerprint matches ──> cached partial states
       ├─ ...
       └─ slice [23:00, 23:37) ── memtable overlaps ────> Partial AggregateExec
                                                            └─ RegionScanExec(request + slice bounds)
```

A physical optimizer rule finds a `Partial` aggregation that reads a region scan through row-wise operators only (filter, projection), where the scan request bounds the time index on both ends. It splits the time range into slices aligned to a fixed width and replaces the partial aggregation with `SliceAggregateExec`. For each slice, the exec either returns the cached partial states or runs the original partial aggregation over a new scan of that slice, then emits the states of all slices. The final aggregation above merges partial states regardless of how the input rows were split, so its output does not change.

The rule runs where the partial aggregation sits on the region scan: in-process in standalone, on the datanode in distributed mode. The frontend plan and the region request protocol do not change.

## Data fingerprint

The cache is only as correct as its invalidation, so the decision this RFC pins down is an engine contract:

```rust
trait RegionEngine {
    /// Returns a fingerprint of the rows that a scan of `[start, end)` reads from
    /// the region's current version, or `None` if the range cannot be fingerprinted.
    fn time_range_fingerprint(&self, region_id: RegionId, start: Timestamp, end: Timestamp)
        -> Option<u64>;
}
```

Equal fingerprints must imply that the same scan request restricted to the range returns the same rows. The default implementation returns `None`, which disables caching for that engine.

Mito implements it from the region version:

- If any memtable overlaps the range, return `None`. Unflushed rows have no stable identity.
- Otherwise hash the region id, the schema version, the partition expression version, and the sorted `(file_id, index_version)` of every SST whose time range overlaps the range.

SST files are immutable. Every operation that changes the rows of a range on disk replaces the set of files overlapping it: flush, compaction, TTL expiry, truncate, region edit, file ingestion. A write or a delete lands in a memtable first and turns the range uncacheable until it is flushed into a new file. The schema version covers altered columns. The partition expression version covers repartitioning, where a region keeps files it shares with other regions and filters them by its partition expression at read time.

The exec checks the fingerprint before using an entry, and checks it again after computing a slice. It stores the result only if the two match, so a flush or compaction that runs during the scan cannot store states computed from a version that no longer exists.

## Cache key and entry

- Key: a hash of the region id, the partial aggregation and the operators below it without the time bounds, and the slice bounds. Leaving the time bounds out lets two queries with different windows share their interior slices.
- Entry: the fingerprint it was computed under, and the partial state batches.
- One cache per node, bounded in bytes, weighted by the size of the batches.

## Worked example

A dashboard panel refreshes every 5 minutes:

```sql
SELECT date_bin(INTERVAL '5 minutes', ts) AS b, service, count(*), avg(latency_ms)
FROM logs
WHERE ts >= now() - INTERVAL '24 hours' AND ts < now()
GROUP BY b, service;
```

At 12:37 the range `[12:37 yesterday, 12:37 today)` splits into 25 slices with a width of one hour: a partial slice `[12:37, 13:00)`, 23 whole hours, and a partial slice `[12:00, 12:37)` today. The first run computes all 25 and caches every slice without unflushed data; assume only the current hour has some.

At 12:42 the range is `[12:42 yesterday, 12:42 today)`. The 23 whole hours from 13:00 yesterday to 12:00 today have the same keys and fingerprints as before, so they come from the cache. The exec recomputes only the two edge slices, `[12:42, 13:00)` yesterday and `[12:00, 12:42)` today.

If a late row for 03:10 today arrives in between, it sits in a memtable, so the 03:00 slice returns `None` and is recomputed until the memtable is flushed. After the flush, the slice's file set includes the new file, the fingerprint differs from the cached one, and the slice is recomputed once more and cached again.

# Benefits and Drawbacks

Benefits:

- Results are exact. The fingerprint changes on every change to the rows of a slice, so there is no staleness window to tune and nothing to clear by hand.
- No new protocol or persisted format. The engine contract is one method with a default, and the cache is in memory.
- The fingerprint is useful beyond this cache. Any result cache keyed by time range, such as PromQL range steps, can use it.
- A prototype on 24 million log rows (48 hourly SSTs, standalone, warm OS cache) measured a 24-hour dashboard aggregation at 37 ms without the cache. With the cache, a window sliding by 5 minutes took 13.5 ms, the same window again took 4.3 ms, and after a late write it took 17 ms. All 72 results matched uncached execution.

Drawbacks:

- The first execution of a query does more work: one scan per slice instead of one scan for the range. The prototype measured about 46.5 ms instead of 43 ms for a cold 24-hour query, and no difference for queries that never repeat.
- Recent slices are never cached while they have unflushed data. A dashboard that only looks at the last 15 minutes gains nothing.
- Correctness depends on the fingerprint seeing every change to the rows of a range. A new engine operation that changes rows without replacing files or bumping one of the hashed versions would make the cache return stale results. This has to be part of the engine's contract and its tests.
- Metric engine logical regions share one physical region, so a write to any logical table changes the fingerprint for all of them in the overlapping range. The result stays correct; the hit rate is lower.

# Implementation Plan

1. Add `RegionEngine::time_range_fingerprint`, implement it for mito, and test every change path listed in [Data fingerprint](#data-fingerprint).
2. Add the optimizer rule and `SliceAggregateExec`, disabled by default behind a query option, with the cache charged to the query memory budget.
3. Report cache hits and misses, and the metrics of the slice scans, in `EXPLAIN ANALYZE`.
4. Delegate the fingerprint from the metric engine to the physical region.

# Alternatives

- **Cache final results per query.** Simpler, but only an identical query hits. A window that slides by 5 minutes misses every time, which is the main dashboard pattern.
- **Invalidate by time, as OpenObserve does.** Do not reuse slices newer than a delay, and accept late data being ignored. It avoids the engine contract, but returns wrong results for late data, and the delay has to be tuned against ingestion lag.
- **Cache at the frontend.** The frontend would have to learn which regions and files back each slice, which needs new information in the region protocol. The datanode already has the region version.
- **Extend the range result cache.** It caches scanned rows, so a hit still pays for aggregation, and its key requires tag filters. The fingerprint is the part worth sharing.
- **Use Flow.** Flow keeps an aggregate up to date continuously and is the better choice for a fixed, known query. It does not help queries nobody declared ahead of time.

# Open Questions

1. **Slice width.** A fixed hour is simple. Deriving the width from the compaction window of the region, or from the query's bucket width, could raise the hit rate.
2. **Plan identity.** The prototype hashes the displayed form of the plan below the aggregation. It needs to include everything that changes the partial states, including any session setting that an expression reads at execution time rather than at planning.
3. **Parallelism.** A slice computes on one partition. A query with few slices runs with less parallelism than the original plan unless a slice can split across partitions.

# Future Work

- PromQL range queries, caching per-step results keyed by the same fingerprint.
- Caching slices from the current hour once flushed data covers part of it, by splitting at the memtable's minimum timestamp.

# References

- OpenObserve streaming aggregation cache: `src/search/src/datafusion/physical_optimizer/stream_aggregate.rs` and `ZO_CACHE_DELAY_SECS` in `src/config/src/config.rs`, [openobserve/openobserve](https://github.com/openobserve/openobserve).
- DataFusion aggregation modes: [`AggregateMode`](https://docs.rs/datafusion/latest/datafusion/physical_plan/aggregates/enum.AggregateMode.html).
