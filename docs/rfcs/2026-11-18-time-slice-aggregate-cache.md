---
Feature Name: Time-Slice Aggregate Cache
Tracking Issue: TBD
Date: 2026-11-18
Author: "Dennis Zhuang <killme2008@gmail.com>"
---

# Summary

Cache the partial states of an aggregation over a region scan on the node that runs the scan, with one entry per aligned time slice. A repeated or sliding dashboard query reuses the slices whose data has not changed and recomputes only the rest. A cached slice is used only when the scan reports the same data fingerprint for the slice's time range as the entry was computed under, so query results always reflect late writes, deletes, flushes, compactions, and TTL expiry.

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
- Metric engine tables. The rule does not rewrite scans of metric engine tables. These tables are queried mostly through PromQL and are handled together with it.
- Sharing cache entries across nodes or persisting them. The cache is in memory and local to a node.
- Caching slices that still have unflushed data. These slices are recomputed on every query.

# Proposal

## Architecture

```
AggregateExec (Final)                     (unchanged)
  └─ SliceAggregateExec                   emits cached states of slices whose fingerprint matches
       └─ Partial AggregateExec           also groups by the slice of each row
            └─ RegionScanExec             the original scan; cached slices are pruned
```

A physical optimizer rule looks for a `Partial` aggregation that reads a region scan through row-wise operators only (filter and projection), where the scan request bounds the time index on both ends. The rule splits the time range into slices aligned to a fixed width, adds the slice of each row as an extra grouping key of the partial aggregation, and places `SliceAggregateExec` above it.

When the query runs, `SliceAggregateExec` asks the scan for the fingerprint of each slice. Slices whose cached entry has the same fingerprint are emitted from the cache, and a dynamic filter lets the scan skip their rows. The remaining slices are computed by the original scan in one pass, and the extra grouping key is removed from their states before they are emitted. Once every partition of the scan has finished, their states are merged across partitions, split by slice, and cached. The final aggregation merges partial states regardless of how the input rows were split, so its output does not change.

The rule runs where the partial aggregation reads the region scan: in-process in standalone mode, and on the datanode in distributed mode. In distributed mode, the aggregation above `SliceAggregateExec` produces the states that the frontend merges. Aggregations that the distributed planner keeps above `MergeScan`, such as those that cannot be split into steps, are not affected. The frontend plan and the region request protocol do not change.

Every slice, cached or computed, describes the data of the version the scan was built on, so a query reads a single region version, as an uncached query does. Results are not bit-identical to those of an uncached run: merging states in a different order can change the last digits of a floating-point `sum` or `avg`, as a different partition count already does.

## Data fingerprint

The correctness of the cache depends entirely on invalidation, so the decision that this RFC pins down is a storage contract:

```rust
trait RegionScanner {
    /// Returns the identity of the rows this scanner reads in `[start, end)` of the
    /// time index, or `None` if it cannot vouch for them.
    fn time_range_fingerprint(&self, start: Timestamp, end: Timestamp)
        -> Option<TimeRangeFingerprint>;
}
```

Equal fingerprints from two scanners built from the same request must imply that they read the same rows in the range. `TimeRangeFingerprint` is opaque to the query engine and compared in full rather than by a truncated hash, so the engine can change what it contains. The fingerprint describes the version the scanner was built on, not the state of the region when it is called. The states a scan computes therefore always match the fingerprint it reports. The contract covers only requests that read the current version. Requests bound to a sequence or a snapshot, such as those issued by Flow and by snapshot reads, are never rewritten. The scanner returns `None` when it cannot vouch for the range. This includes scans that read rows from sources other than memtables and SST files, such as a follower region that reads unflushed rows from its leader. The default implementation returns `None`, which disables caching for scanners that do not implement it.

Mito implements the method from the version the scan was built on:

- If any memtable overlaps the range, return `None`. Unflushed rows have no stable identity.
- Otherwise, describe what decides the rows the scan returns for the range: the region's schema, partition expression, and merge mode, and the identity of every SST of the scan that overlaps the range, including how its rows are read. The exact fields are settled in the implementation.

SST files are immutable. Every operation that changes the persisted rows of a range replaces the set of files that overlap it: flush, compaction, TTL expiry, truncation, region edits, and file ingestion. A write or a delete lands in a memtable first, which makes the range uncacheable until the memtable is flushed into a new file. The schema covers altered columns. The partition expression covers repartitioning, in which a region keeps files that it shares with other regions and filters them by its partition expression at read time.

## Cache key and entry

- Key: the region ID, the partial aggregation and the operators below it, the scan request, the session settings, and the slice bounds. The key is kept in full and compared on lookup, so two plans never share an entry by a hash collision. It leaves out only the time-index range predicates that the slice bounds provably imply, so two queries with different windows can share their interior slices. Any other predicate on the time index, such as `date_part('hour', ts) = 3`, stays in the key. Session settings are in the key because functions read some of them, such as the time zone, at execution time.
- Exclusions: dynamic filters change while a query runs, so the rule skips scans that hold one, such as a filter pushed down by a TopK or a join, and runs after filter pushdown to see them. Plans with expressions that are not immutable are skipped too, because their values can change between queries.
- Entry: the fingerprint under which the entry was computed, and the partial state batches.
- Capacity: one cache per node, bounded in bytes and weighted by batch size. A query whose states exceed a per-query bound caches nothing, and the rule stops rewriting its plan, which would otherwise pay for the extra grouping on every run.

## Worked example

A dashboard panel refreshes every 5 minutes:

```sql
SELECT date_bin(INTERVAL '5 minutes', ts) AS b, service, count(*), avg(latency_ms)
FROM logs
WHERE ts >= now() - INTERVAL '24 hours' AND ts < now()
GROUP BY b, service;
```

At 12:37, the range `[12:37 yesterday, 12:37 today)` splits into 25 one-hour slices: a partial slice `[12:37, 13:00)` yesterday, 23 whole hours, and a partial slice `[12:00, 12:37)` today. The first run computes all 25 slices in one scan and caches every slice without unflushed data. In this example, only the current hour has unflushed data.

At 12:42, the range is `[12:42 yesterday, 12:42 today)`. The 23 whole hours from 13:00 yesterday to 12:00 today keep their keys and fingerprints, so they come from the cache. The scan skips them and computes only the two edge slices: `[12:42, 13:00)` yesterday and `[12:00, 12:42)` today.

Suppose a late row for 03:10 today arrives between the two runs. The row lands in the memtable of today's time partition, which now spans 03:10 to 12:42. Every slice from 03:00 to 12:00 then returns `None` and is recomputed until the memtable is flushed. After the flush, the file sets of those slices include the new file, so their fingerprints differ from the cached ones. The slices are recomputed once more and cached again.

# Benefits and Drawbacks

Benefits:

- Exact results. A query reads one region version, as an uncached query does, and the fingerprint changes whenever the rows of a slice change, so there is no staleness window to tune and nothing to clear manually.
- No new protocol or persisted format. The engine contract is one method with a default implementation, and the cache is in memory.
- The fingerprint is useful beyond this cache. Any result cache keyed by time range, such as one for PromQL range steps, can use it.
- A prototype measured four dashboard queries over 384 million request log rows spanning 8 days, in standalone mode, on local disk with a warm OS cache. The table shows median milliseconds. A first run is the first query after the server starts; a slid window starts 5 minutes after the previous one.

| Query | Window | Uncached, first run | Cached, first run | Uncached, later runs | Cached, slid window | Cached, same window |
|---|---|---|---|---|---|---|
| P99 latency per bucket and service | 24 hours | 181.9 | 208.3 | 148.2 | 20.1 | 7.4 |
| | 7 days | 1164.9 | 1272.5 | 1011.9 | 22.3 | 14.2 |
| Error count with a full-text term | 24 hours | 315.0 | 322.6 | 269.0 | 21.1 | 5.3 |
| | 7 days | 2209.9 | 2232.6 | 1931.3 | 32.2 | 13.3 |
| Top 10 endpoints | 24 hours | 149.4 | 173.8 | 125.0 | 15.2 | 3.4 |
| | 7 days | 1146.4 | 1491.8 | 1200.4 | 28.2 | 15.0 |
| Distinct users (`approx_distinct`) | 24 hours | 101.5 | 128.4 | 90.8 | 13.9 | 3.9 |
| | 7 days | 701.4 | 901.0 | 785.8 | 26.6 | 13.7 |

  Cached and uncached results were identical except for `approx_percentile_cont`, whose result depends on the order in which states are merged: cached values differed from uncached ones by up to 3.3%, and two uncached runs differed by up to 4.3%. A light query, `count(*)` and `avg` per bucket and service over 24 hours of 24 million rows, took 38.5 ms uncached and 9.9 ms cached on a slid window.

Drawbacks:

- The first execution of a query does more work. The partial aggregation also groups by slice, which multiplies its groups by the number of slices it covers. The prototype measured 1% to 30% more time on a first run, the most for queries with many groups, such as the top endpoints over 7 days. A query that never repeats pays this cost and gains nothing, so the cache is off by default.
- Slices are not cached while a memtable overlaps them, and a memtable spans everything from its oldest row to its newest. A late write therefore disables the cache from its timestamp to the present until the next flush. A dashboard that only shows the last 15 minutes gains nothing.
- Aggregations whose states grow with their input, such as an exact `count(DISTINCT)`, exceed the per-query bound and are never cached; their first run pays for the extra grouping.
- Only aggregations with a partial stage are rewritten. A query that groups by the whole primary key aggregates each series within one partition, and a `min` or `max` without grouping pushes its own dynamic filter into the scan; neither uses the cache.
- Correctness depends on the fingerprint observing every change to the rows of a range. A new engine operation that changes rows without replacing files or bumping one of the versions in the fingerprint would make the cache return stale results. The engine contract and its tests must cover this requirement.

# Implementation Plan

1. Add `RegionScanner::time_range_fingerprint`, implement it for mito, and test every change path listed in [Data fingerprint](#data-fingerprint).
2. Add the optimizer rule and `SliceAggregateExec`, disabled by default behind a query option, with a node-level cache bounded by its own size option.
3. Report cache hits and misses, and the metrics of the slice scans, in `EXPLAIN ANALYZE`.

# Alternatives

- **Cache final results per query.** This approach is simpler, but only an identical query hits the cache. A window that slides by 5 minutes misses every time, and sliding windows are the main dashboard pattern.
- **Invalidate by time, as VictoriaMetrics, Mimir, and OpenObserve do.** Data newer than a delay is not cached. This approach avoids the engine contract. However, late data inside a cached range is either missed until the entry expires or clears the whole cache, and the delay must be tuned against ingestion lag.
- **Invalidate per region instead of per range,** as the Elasticsearch shard request cache does. The fingerprint would be the region's manifest version or committed sequence. This approach is simpler and needs no per-file reasoning. However, any write, flush, or compaction anywhere in the region invalidates every slice, so a region that receives writes continuously almost never hits the cache.
- **Compute each missed slice with its own scan.** The partial aggregation stays unchanged. However, each slice then reads the version that is current when its scan starts, so a query no longer reads a single version, and a first run pays the setup of a scan once per slice.
- **Cache per SST file,** as Druid caches per segment. Compaction replaces files, so every compaction drops the cache entries for its input files. In addition, a query window rarely covers whole files.
- **Cache at the frontend.** The frontend would need to know which regions and files back each slice, which requires new information in the region protocol. The datanode already has the region version.
- **Extend the range result cache.** The range result cache stores scanned rows, so a hit still pays for aggregation, and its key requires tag filters. The fingerprint is the part worth sharing.
- **Use Flow.** Flow keeps an aggregate up to date continuously and is the better choice for a fixed, known query. Flow does not help queries that nobody declared in advance.

# Open Questions

1. **Slice width.** A fixed one-hour width is simple. Deriving the width from the region's compaction window or from the query's bucket width could raise the hit rate.
2. **Plan identity.** The prototype identifies the plan by the debug form of its expressions plus the session settings. This relies on the debug form of every expression covering everything that affects its result. A stable serialization of physical expressions would remove that reliance.
3. **Cost of a full hit.** A 30-day window with hourly slices still takes about 18 ms when every slice hits. Where that time goes has not been measured.

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
