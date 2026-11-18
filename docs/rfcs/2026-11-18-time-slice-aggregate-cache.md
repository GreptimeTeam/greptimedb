---
Feature Name: Time-Slice Aggregate Cache
Tracking Issue: TBD
Date: 2026-11-18
Author: "Dennis Zhuang <killme2008@gmail.com>"
---

# Summary

Cache the partial states of an aggregation over a region scan, one entry per aligned time slice, on the node that runs the scan. A repeated or sliding dashboard query reuses the slices whose data has not changed and recomputes only the rest. A cached slice is used only while the region reports the same data fingerprint for its time range, so late writes, deletes, flushes, compactions and TTL are always reflected.

# Motivation

Dashboards run the same aggregation every few seconds over a window that slides forward: `count(*)`, `avg(latency)` or `sum(bytes)` grouped by a time bucket and a few tags over the last 24 hours. Each refresh scans and aggregates the whole window again, although all but the newest few minutes of it have not changed since the previous refresh.

GreptimeDB has no cache that covers this. The query layer has no result cache. The mito range result cache stores scanned rows, not aggregation results, and only for scans with tag filters. Flow can maintain the aggregate continuously, but it has to be declared ahead of time for each query shape, and ad hoc dashboards are not declared.

OpenObserve caches merged partial aggregation states per time partition. It does not invalidate them on writes: a window that ends within `cache_delay_secs` (300 seconds by default) of now is not reused, and data that arrives later than that for a cached window is not reflected until the entry is evicted or explicitly cleared. We want the reuse without that staleness window.

# Goals

1. Reuse aggregation work across queries whose time ranges overlap, including windows that slide forward.
2. Aggregate the same rows as an uncached execution, including after late writes, deletes and any change to the files of the region.
3. Work in standalone and distributed mode without a new protocol between frontend and datanode.

# Non-Goals

- Caching final results or raw rows. Only partial aggregation states are cached.
- PromQL. Its range evaluation has a different plan shape; it can reuse the fingerprint later (see Future Work).
- Sharing cache entries across nodes or persisting them. The cache is in memory and local to a node.
- Caching slices that still have unflushed data. They are recomputed on every query.

# Proposal

## Architecture

```
AggregateExec (Final)                     (unchanged)
  └─ SliceAggregateExec                   replaces Partial AggregateExec
       ├─ slice [00:00, 01:00) ── fingerprint matches ──> cached partial states
       ├─ slice [01:00, 02:00) ── fingerprint matches ──> cached partial states
       ├─ ...
       └─ slice [23:00, 23:37) ── memtable overlaps ────> Partial AggregateExec
                                                            └─ RegionScanExec(request + slice bounds)
```

A physical optimizer rule finds a `Partial` aggregation that reads a region scan through row-wise operators only (filter, projection), where the scan request bounds the time index on both ends. It splits the time range into slices aligned to a fixed width and replaces the partial aggregation with `SliceAggregateExec`. For each slice, the exec either returns the cached partial states or runs the original partial aggregation over a new scan of that slice, then emits the states of all slices. The final aggregation above merges partial states regardless of how the input rows were split, so its output does not change.

The rule runs where the partial aggregation sits on the region scan: in-process in standalone, on the datanode in distributed mode. In distributed mode the aggregation above it is the one that produces states for the frontend to merge. Aggregations that the distributed planner keeps above `MergeScan`, such as those that cannot be split into steps, are not affected. The frontend plan and the region request protocol do not change.

Results are not bit-identical to an uncached run. Merging states in a different order can change the last digits of a floating-point `sum` or `avg`, as a different partition count already does. Each slice reads the region version current when it runs, so a write that lands during the query can be visible in one slice and not in another. This is weaker than an uncached scan, which reads one region version. Requests that need a fixed snapshot carry a sequence bound and are never rewritten.

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

Equal fingerprints must imply that the same scan request restricted to the range returns the same rows. The contract covers requests that read the current version only. Requests bound to a sequence or a snapshot, as Flow and snapshot reads issue, are never rewritten. The engine returns `None` when it cannot vouch for the range, including when a scan reads rows from somewhere other than its memtables and SST files, such as a follower region reading unflushed rows from its leader. The default implementation returns `None`, which disables caching for that engine.

Mito implements it from the region version:

- If any memtable overlaps the range, return `None`. Unflushed rows have no stable identity.
- Otherwise hash the region id, the schema version, the partition expression version, and the identity of every SST whose time range overlaps the range, sorted. A file's identity is its file id plus the per-file metadata that affects how its rows are read, the same fields the range result cache already keys on: index version, source region and sequence.

SST files are immutable. Every operation that changes the rows of a range on disk replaces the set of files overlapping it: flush, compaction, TTL expiry, truncate, region edit, file ingestion. A write or a delete lands in a memtable first and turns the range uncacheable until it is flushed into a new file. The schema version covers altered columns. The partition expression version covers repartitioning, where a region keeps files it shares with other regions and filters them by its partition expression at read time.

The exec checks the fingerprint before using an entry, and checks it again after computing a slice. It stores the result only if the two match, so a flush or compaction that runs during the scan cannot store states computed from a version that no longer exists.

## Cache key and entry

- Key: a hash of the region id, the partial aggregation and the operators below it, and the slice bounds. Only range predicates on the time index that the slice bounds provably imply are left out of the hash, which lets two queries with different windows share their interior slices. Any other predicate on the time index, such as `date_part('hour', ts) = 3`, stays in the key.
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

If a late row for 03:10 today arrives in between, it lands in the memtable of today's time partition. That memtable now spans 03:10 to 12:42, so every slice from 03:00 to 12:00 returns `None` and is recomputed until the memtable is flushed. After the flush, those slices include the new file in their file sets, their fingerprints differ from the cached ones, and they are recomputed once more and cached again.

# Benefits and Drawbacks

Benefits:

- No stale results. The fingerprint changes on every change to the rows of a slice, so there is no staleness window to tune and nothing to clear by hand.
- No new protocol or persisted format. The engine contract is one method with a default, and the cache is in memory.
- The fingerprint is useful beyond this cache. Any result cache keyed by time range, such as PromQL range steps, can use it.
- A prototype on 24 million log rows (48 hourly SSTs, standalone, warm OS cache) measured a 24-hour dashboard aggregation at 37 ms without the cache. With the cache, a window sliding by 5 minutes took 13.5 ms, the same window again took 4.3 ms, and after a late write it took 17 ms. All 72 results matched uncached execution.

Drawbacks:

- The first execution of a query does more work: one scan per slice instead of one scan for the range. The prototype measured about 46.5 ms instead of 43 ms for a cold 24-hour query, and no difference for queries that never repeat.
- Slices are not cached while a memtable overlaps them, and a memtable spans everything from its oldest to its newest row. A late write therefore disables the cache from its timestamp to now until the next flush. A dashboard that only looks at the last 15 minutes gains nothing.
- Correctness depends on the fingerprint seeing every change to the rows of a range. A new engine operation that changes rows without replacing files or bumping one of the hashed versions would make the cache return stale results. This has to be part of the engine's contract and its tests.
- Metric engine logical regions share one physical region, so a write to any logical table changes the fingerprint for all of them in the overlapping range. The result stays correct; the hit rate is lower.

# Implementation Plan

1. Add `RegionEngine::time_range_fingerprint`, implement it for mito, and test every change path listed in [Data fingerprint](#data-fingerprint).
2. Add the optimizer rule and `SliceAggregateExec`, disabled by default behind a query option, with a node-level cache bounded by its own size option.
3. Report cache hits and misses, and the metrics of the slice scans, in `EXPLAIN ANALYZE`.
4. Delegate the fingerprint from the metric engine to the physical region.

# Alternatives

- **Cache final results per query.** Simpler, but only an identical query hits. A window that slides by 5 minutes misses every time, which is the main dashboard pattern.
- **Invalidate by time, as OpenObserve does.** Do not reuse slices newer than a delay, and accept late data being ignored. It avoids the engine contract, but returns wrong results for late data, and the delay has to be tuned against ingestion lag.
- **Invalidate per region instead of per range.** Use the region's manifest version or committed sequence as the fingerprint. It is simpler and needs no per-file reasoning, but any write, flush or compaction anywhere in the region invalidates every slice, so a region that receives writes continuously would almost never hit.
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
