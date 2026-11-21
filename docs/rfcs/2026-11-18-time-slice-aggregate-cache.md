---
Feature Name: Time-Slice Aggregate Cache
Tracking Issue: TBD
Date: 2026-11-18
Author: "Dennis Zhuang <killme2008@gmail.com>"
---

# Summary

Cache the partial states of an aggregation over a region scan on the node that runs the scan, with one entry per aligned time slice. A repeated or sliding dashboard query reuses the slices whose data has not changed and recomputes only the rest. A cached slice is used only while the region reports the same data fingerprint for the slice's time range, so query results always reflect late writes, deletes, flushes, compactions, and TTL expiry.

# Motivation

Dashboards run the same aggregation every few seconds over a window that slides forward: `count(*)`, `avg(latency)`, or `sum(bytes)`, grouped by a time bucket and a few tags over the last 24 hours. Each refresh scans and aggregates the whole window again, even though only the newest few minutes have changed since the previous refresh.

GreptimeDB has no cache for this workload. The query layer has no result cache. The mito range result cache stores scanned rows rather than aggregation results, and only for scans with tag filters. Flow can maintain an aggregate continuously, but each query shape must be declared in advance, and ad hoc dashboard queries are not.

# Prior Art

Observability and analytics systems differ in what they cache and in how they keep cached results fresh.

| System | What is cached | Freshness |
|---|---|---|
| VictoriaMetrics | Result series of a query expression, extended as the range moves | Data newer than `-search.cacheTimestampOffset` (5 minutes) is not cached. A single-node instance resets the whole cache when it ingests a sample older than that offset. Otherwise, an API call resets the cache. |
| Grafana Mimir | Query-frontend results, split by `-query-frontend.split-queries-by-interval` (24 hours) | Results newer than `-query-frontend.max-cache-freshness` (10 minutes) are not cached. Entries expire after a TTL: 1 week, or 10 minutes inside the out-of-order window. |
| ClickHouse | Final result of a `SELECT` | Entries expire after `query_cache_ttl` (60 seconds). Inserts do not invalidate them. |
| OpenObserve | Merged partial aggregation states per time partition | Windows that end within `ZO_CACHE_DELAY_SECS` (300 seconds) of the current time are not reused. Later data for a cached window is not reflected until the entry is evicted or cleared. |
| Elasticsearch | Aggregation results per shard, for requests with `size=0` | Entries are invalidated whenever the shard refreshes. |
| Apache Druid | Partial results per immutable segment | Entries stay valid as long as the segment does not change. Results for real-time segments are computed at query time. |
| StarRocks | Partial aggregation results per tablet, keyed with the tablet version | When the tablet version advances, StarRocks reads only the rowsets added since the cached version and merges them with the cached result (tables without a primary key). |

Time-based freshness is the common choice for metrics and log systems. It needs no support from storage. However, a late write inside a cached range is either missed until the entry expires or clears the whole cache. Elasticsearch drops a shard's entries at every refresh, so a shard that receives writes continuously keeps losing its entries. Druid and StarRocks tie the cache to immutable storage units and their versions, which gives exact results.

This RFC follows the Druid and StarRocks approach, adapted to the GreptimeDB storage model. Like Druid segments, SST files are immutable. However, TWCS files do not line up with dashboard windows, and compaction replaces them. The cache unit is therefore a time slice, and its version is the set of files that overlap the slice.

# Goals

1. Reuse aggregation work across queries whose time ranges overlap, including windows that slide forward.
2. Aggregate the same rows as an uncached execution, including after late writes, deletes, and any change to the region's files.
3. Work in standalone and distributed mode without a new protocol between frontend and datanode.

# Non-Goals

- Caching final results or raw rows. Only partial aggregation states are cached.
- PromQL. Its range evaluation has a different plan shape. PromQL can reuse the fingerprint later; see [Future Work](#future-work).
- Metric engine tables. The metric engine keeps the default `time_range_fingerprint`, which returns `None`, so the cache is off for its tables. These tables are queried mostly through PromQL and are handled together with it.
- Sharing cache entries across nodes or persisting them. The cache is in memory and local to a node.
- Caching slices that still have unflushed data. These slices are recomputed on every query.

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

A physical optimizer rule looks for a `Partial` aggregation that reads a region scan through row-wise operators only (filter and projection), where the scan request bounds the time index on both ends. The rule splits the time range into slices aligned to a fixed width and replaces the partial aggregation with `SliceAggregateExec`. For each slice, `SliceAggregateExec` either returns the cached partial states or runs the original partial aggregation over a new scan of that slice. It then emits the states of all slices. The final aggregation merges partial states regardless of how the input rows were split, so its output does not change.

The rule runs where the partial aggregation reads the region scan: in-process in standalone mode, and on the datanode in distributed mode. In distributed mode, the aggregation above `SliceAggregateExec` produces the states that the frontend merges. Aggregations that the distributed planner keeps above `MergeScan`, such as those that cannot be split into steps, are not affected. The frontend plan and the region request protocol do not change.

Results are not bit-identical to those of an uncached run. Merging states in a different order can change the last digits of a floating-point `sum` or `avg`, as a different partition count already does. Each slice reads the region version that is current when the slice runs, so a write that lands during the query can be visible in one slice but not in another. An uncached scan reads a single region version, so this guarantee is weaker. Requests that need a fixed snapshot carry a sequence bound and are never rewritten.

## Data fingerprint

The correctness of the cache depends entirely on invalidation, so the decision that this RFC pins down is an engine contract:

```rust
trait RegionEngine {
    /// Returns a fingerprint of the rows that a scan of `[start, end)` reads from
    /// the region's current version, or `None` if the range cannot be fingerprinted.
    fn time_range_fingerprint(&self, region_id: RegionId, start: Timestamp, end: Timestamp)
        -> Option<u64>;
}
```

Equal fingerprints must imply that the same scan request, restricted to the range, returns the same rows. The contract covers only requests that read the current version. Requests bound to a sequence or a snapshot, such as those issued by Flow and by snapshot reads, are never rewritten. The engine returns `None` when it cannot vouch for the range. This includes scans that read rows from sources other than memtables and SST files, such as a follower region that reads unflushed rows from its leader. The default implementation returns `None`, which disables caching for that engine.

Mito implements the method from the region version:

- If any memtable overlaps the range, return `None`. Unflushed rows have no stable identity.
- Otherwise, hash the region ID, the schema version, the partition expression version, and the sorted identities of all SSTs whose time ranges overlap the range. A file's identity is its file ID plus the per-file metadata that affects how its rows are read: index version, source region, and sequence. The range result cache already keys on the same fields.

SST files are immutable. Every operation that changes the persisted rows of a range replaces the set of files that overlap it: flush, compaction, TTL expiry, truncation, region edits, and file ingestion. A write or a delete lands in a memtable first, which makes the range uncacheable until the memtable is flushed into a new file. The schema version covers altered columns. The partition expression version covers repartitioning, in which a region keeps files that it shares with other regions and filters them by its partition expression at read time.

`SliceAggregateExec` checks the fingerprint before it uses an entry, and again after it computes a slice. It stores the result only if both fingerprints match. A flush or compaction that runs during the scan therefore cannot cause states computed from an outdated version to be stored.

## Cache key and entry

- Key: a hash of the region ID, the partial aggregation and the operators below it, and the slice bounds. The hash leaves out only the time-index range predicates that the slice bounds provably imply, so two queries with different windows can share their interior slices. Any other predicate on the time index, such as `date_part('hour', ts) = 3`, stays in the key.
- Entry: the fingerprint under which the entry was computed, and the partial state batches.
- Capacity: one cache per node, bounded in bytes and weighted by batch size.

## Worked example

A dashboard panel refreshes every 5 minutes:

```sql
SELECT date_bin(INTERVAL '5 minutes', ts) AS b, service, count(*), avg(latency_ms)
FROM logs
WHERE ts >= now() - INTERVAL '24 hours' AND ts < now()
GROUP BY b, service;
```

At 12:37, the range `[12:37 yesterday, 12:37 today)` splits into 25 one-hour slices: a partial slice `[12:37, 13:00)` yesterday, 23 whole hours, and a partial slice `[12:00, 12:37)` today. The first run computes all 25 slices and caches every slice without unflushed data. In this example, only the current hour has unflushed data.

At 12:42, the range is `[12:42 yesterday, 12:42 today)`. The 23 whole hours from 13:00 yesterday to 12:00 today keep their keys and fingerprints, so they come from the cache. `SliceAggregateExec` recomputes only the two edge slices: `[12:42, 13:00)` yesterday and `[12:00, 12:42)` today.

Suppose a late row for 03:10 today arrives between the two runs. The row lands in the memtable of today's time partition, which now spans 03:10 to 12:42. Every slice from 03:00 to 12:00 then returns `None` and is recomputed until the memtable is flushed. After the flush, the file sets of those slices include the new file, so their fingerprints differ from the cached ones. The slices are recomputed once more and cached again.

# Benefits and Drawbacks

Benefits:

- No stale results. The fingerprint changes whenever the rows of a slice change, so there is no staleness window to tune and nothing to clear manually.
- No new protocol or persisted format. The engine contract is one method with a default implementation, and the cache is in memory.
- The fingerprint is useful beyond this cache. Any result cache keyed by time range, such as one for PromQL range steps, can use it.
- A prototype measured the effect on 24 million log rows (48 hourly SSTs, standalone mode, warm OS cache). Without the cache, a 24-hour dashboard aggregation took 37 ms. With the cache, a window that slid by 5 minutes took 13.5 ms, the same window again took 4.3 ms, and the window after a late write took 17 ms. All 72 results matched uncached execution.

Drawbacks:

- The first execution of a query does more work: one scan per slice instead of one scan for the whole range. The prototype measured about 46.5 ms instead of 43 ms for a cold 24-hour query, and no difference for queries that never repeat.
- Slices are not cached while a memtable overlaps them, and a memtable spans everything from its oldest row to its newest. A late write therefore disables the cache from its timestamp to the present until the next flush. A dashboard that only shows the last 15 minutes gains nothing.
- Correctness depends on the fingerprint observing every change to the rows of a range. A new engine operation that changes rows without replacing files or bumping one of the hashed versions would make the cache return stale results. The engine contract and its tests must cover this requirement.

# Implementation Plan

1. Add `RegionEngine::time_range_fingerprint`, implement it for mito, and test every change path listed in [Data fingerprint](#data-fingerprint).
2. Add the optimizer rule and `SliceAggregateExec`, disabled by default behind a query option, with a node-level cache bounded by its own size option.
3. Report cache hits and misses, and the metrics of the slice scans, in `EXPLAIN ANALYZE`.

# Alternatives

- **Cache final results per query.** This approach is simpler, but only an identical query hits the cache. A window that slides by 5 minutes misses every time, and sliding windows are the main dashboard pattern.
- **Invalidate by time, as VictoriaMetrics, Mimir, and OpenObserve do.** Data newer than a delay is not cached. This approach avoids the engine contract. However, late data inside a cached range is either missed until the entry expires or clears the whole cache, and the delay must be tuned against ingestion lag.
- **Invalidate per region instead of per range,** as the Elasticsearch shard request cache does. The fingerprint would be the region's manifest version or committed sequence. This approach is simpler and needs no per-file reasoning. However, any write, flush, or compaction anywhere in the region invalidates every slice, so a region that receives writes continuously almost never hits the cache.
- **Cache per SST file,** as Druid caches per segment. Compaction replaces files, so every compaction drops the cache entries for its input files. In addition, a query window rarely covers whole files.
- **Cache at the frontend.** The frontend would need to know which regions and files back each slice, which requires new information in the region protocol. The datanode already has the region version.
- **Extend the range result cache.** The range result cache stores scanned rows, so a hit still pays for aggregation, and its key requires tag filters. The fingerprint is the part worth sharing.
- **Use Flow.** Flow keeps an aggregate up to date continuously and is the better choice for a fixed, known query. Flow does not help queries that nobody declared in advance.

# Open Questions

1. **Slice width.** A fixed one-hour width is simple. Deriving the width from the region's compaction window or from the query's bucket width could raise the hit rate.
2. **Plan identity.** The prototype hashes the displayed form of the plan below the aggregation. The key must include everything that changes the partial states, including any session setting that an expression reads at execution time rather than at planning time.
3. **Parallelism.** Each slice is computed on one partition. A query with few slices runs with less parallelism than the original plan unless a slice can be split across partitions.

# Future Work

- PromQL range queries, with per-step results cached under the same fingerprint, and the metric engine fingerprint that they need. Logical tables share one physical region, so a flush or compaction of the physical region invalidates slices of every logical table it covers.
- Caching slices of the current hour once flushed data covers part of the hour, by splitting the slice at the memtable's minimum timestamp.
- Incremental refresh, as StarRocks does for tablets: when a slice has only gained new files since its entry was computed, aggregate the new files and merge the result into the cached states instead of recomputing the slice.

# References

- VictoriaMetrics rollup result cache: `app/vmselect/promql/rollup_result_cache.go` in [VictoriaMetrics/VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics), and the [rollup result cache documentation](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#rollup-result-cache).
- Grafana Mimir query-frontend flags `-query-frontend.split-queries-by-interval`, `-query-frontend.max-cache-freshness`, and `-query-frontend.results-cache-ttl`: `cmd/mimir/help-all.txt.tmpl` in [grafana/mimir](https://github.com/grafana/mimir).
- [ClickHouse query cache](https://clickhouse.com/docs/operations/query-cache).
- OpenObserve streaming aggregation cache: `src/search/src/datafusion/physical_optimizer/stream_aggregate.rs` and `ZO_CACHE_DELAY_SECS` in `src/config/src/config.rs`, in [openobserve/openobserve](https://github.com/openobserve/openobserve).
- [Elasticsearch shard request cache](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/shard-request-cache).
- [Apache Druid query caching](https://druid.apache.org/docs/latest/querying/caching/).
- StarRocks query cache: `be/src/exec/query_cache/cache_operator.cpp` in [StarRocks/starrocks](https://github.com/StarRocks/starrocks).
- DataFusion aggregation modes: [`AggregateMode`](https://docs.rs/datafusion/latest/datafusion/physical_plan/aggregates/enum.AggregateMode.html).
