---
Feature Name: Time-Slice Aggregate Cache
Tracking Issue: TBD
Date: 2026-11-18
Author: "Dennis Zhuang <killme2008@gmail.com>"
---

# Summary

Cache the partial states of an aggregation over a region scan on the node that runs the scan, with one entry per aligned time slice. A repeated or sliding dashboard query reuses the slices whose data has not changed and recomputes only the rest. A cached slice is used only when the scan reports the same data fingerprint for the slice's time range as the entry was computed under, so query results always reflect late writes, deletes, flushes, compactions, and TTL expiry.

# Motivation

Dashboards over logs and traces run the same SQL aggregation again and again over a window that slides forward: `count(*)`, `avg(latency)`, or `sum(bytes)`, grouped by a time bucket and a few tags over the last day or week. Each refresh scans and aggregates the whole window again, even though only the newest minutes have changed since the previous refresh.

GreptimeDB's own front ends issue such queries:

- The GreptimeDB Dashboard's trace view draws request rate, errors, and a duration heatmap with `date_bin` over the selected window and ranks services by count.
- Its log view draws log volume by level and the top values of a label.
- The Grafana data source plugin issues a log volume query and ships dashboards whose panels refresh every 30 seconds.
- A port of Langfuse to GreptimeDB charts trace counts, cost by model, latency percentiles, and scores over the last day or week.

All of them bound the time index on both ends and re-run with a shifted window. The cost is in the scan: on a deployment with 7 days of traces, the trace view's aggregations took about 5 seconds each, nearly all of it in reading files, while the aggregation itself took milliseconds.

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
- PromQL. Range dashboards over metrics are evaluated by the windowed PromQL path in #10569.
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

A physical optimizer rule looks for a `Partial` aggregation that reads a region scan through row-wise operators only (filter and projection), where the scan request bounds the time index on both ends. The rule splits the time range into slices aligned to a fixed width and places `SliceAggregateExec` above the partial aggregation.

If the aggregation already groups by a time bucket that lies within one slice, the slice of each group follows from its bucket. Otherwise, the rule adds the slice of each row as an extra grouping key.

When the query runs, `SliceAggregateExec` asks the scan for the fingerprint of each slice. Slices whose cached entry has the same fingerprint are emitted from the cache, and a dynamic filter lets the scan skip their rows. The remaining slices are computed by the original scan in one pass, and an extra grouping key is removed from their states before they are emitted. Once every partition of the scan has finished, their states are merged across partitions and cached per slice, without delaying the query. The final aggregation merges partial states regardless of how the input rows were split, so its output does not change.

The rule runs where the partial aggregation reads the region scan: in-process in standalone mode, and on the datanode in distributed mode:

```
Frontend   AggregateExec (merges the outputs of all regions)    (unchanged)
             └─ MergeScanExec                                   region request unchanged
Datanode   AggregateExec (FinalPartitioned)                     (unchanged)
             └─ RepartitionExec
                  └─ SliceAggregateExec
                       └─ Partial AggregateExec
                            └─ RegionScanExec
```

In distributed mode, the datanode does all the cache work: it computes fingerprints, emits cached states, prunes the scan, merges the states of the partitions, and stores them. The frontend does not know whether a region's states came from the cache. Aggregations that the distributed planner keeps above `MergeScan`, such as those that cannot be split into steps, are not affected. The frontend plan and the region request protocol do not change.

Each datanode caches the regions it hosts. A recomputed slice replaces its entry as a whole; states are never appended to an entry. After a region moves to another datanode, the new datanode starts without entries for it, and the old datanode evicts them once they are idle.

A row written while a query runs may or may not be seen by a plain scan, with or without the cache. The cache keeps that visibility and does not add another outcome. A slice computed with such a row is cached under a fingerprint that does not match again, because the row stays in a memtable or moves to a new file.

Results are not bit-identical to those of an uncached run: merging states in a different order can change the last digits of a floating-point `sum` or `avg`, as a different partition count already does.

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

Equal fingerprints from two scanners built from the same request must imply that they read the same rows in the range. `TimeRangeFingerprint` is opaque to the query engine, which keeps its SHA-256 digest rather than a truncated hash, so the engine can change what it contains. A fingerprint grows with the files overlapping its range, and a query holds one for every slice it misses, so the digest also bounds that memory. The fingerprint describes the version the scanner was built on, not the state of the region when it is called. The states a scan computes therefore always match the fingerprint it reports. The contract covers only requests that read the current version. Requests bound to a sequence or a snapshot, such as those issued by Flow and by snapshot reads, are never rewritten. The scanner returns `None` when it cannot vouch for the range. This includes scans that read rows from sources other than memtables and SST files, such as a follower region that reads unflushed rows from its leader, and scans with a series row selector such as `LastRow`, where a newer row outside the range changes which rows are selected inside it. The default implementation returns `None`, which disables caching for scanners that do not implement it.

Mito implements the method from the version the scan was built on:

- If any memtable overlaps the range, return `None`. Unflushed rows have no stable identity.
- Otherwise, describe what decides the rows the scan returns for the range: the region's schema, partition expression, and merge mode, and the identity of every SST of the scan that overlaps the range, including how its rows are read. The exact fields are settled in the implementation.

SST files are immutable. Every operation that changes the persisted rows of a range replaces the set of files that overlap it: flush, compaction, TTL expiry, truncation, region edits, and file ingestion. A write or a delete lands in a memtable first, which makes the range uncacheable until the memtable is flushed into a new file. The schema covers altered columns. The partition expression covers repartitioning, in which a region keeps files that it shares with other regions and filters them by its partition expression at read time.

## Cache key and entry

- Key: the region ID, the partial aggregation and the operators below it, the scan request, the session settings, and the slice bounds. The plan part is stored as its SHA-256 digest, so a key has a fixed size however large the plan is, and two plans share an entry only by a SHA-256 collision. It leaves out only the time-index range predicates that the slice bounds provably imply, so two queries with different windows can share their interior slices. Any other predicate on the time index, such as `date_part('hour', ts) = 3`, stays in the key. Session settings are in the key because functions read some of them, such as the time zone, at execution time.
- Exclusions: dynamic filters change while a query runs, so the rule skips scans that hold one, such as a filter pushed down by a TopK or a join, and runs after filter pushdown to see them. Plans with expressions that are not immutable are skipped too, because their values can change between queries.
- Entry: the fingerprint under which the entry was computed, and the partial state batches.
- Capacity: one cache per node, bounded in bytes. Entries unused for an idle timeout are evicted. The states an aggregation collects count against the query memory limit, and an aggregation whose states exceed a per-aggregation bound caches nothing. The bound is per aggregation because it identifies plans whose states are too large to be worth caching; total memory is bounded by the query memory limit.
- The cache is experimental and off by default, with a node-level switch that a single query can override.

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

- Exact results. The fingerprint changes whenever the persisted rows of a slice change, so there is no staleness window to tune and nothing to clear manually.
- No new protocol or persisted format. The engine contract is one method with a default implementation, and the cache is in memory.
- The fingerprint is useful beyond this cache. Any result cache keyed by time range, such as one for PromQL range steps, can use it.
- A prototype measured dashboard queries over 384 million request log rows spanning 8 days, in standalone mode, on local disk with a warm OS cache. The table shows median milliseconds. A first run is the first query after the server starts; a slid window starts 5 minutes after the previous one. The uncached runs use `main` at 8e885bfed5, which already prunes these windows by time range.

| Query | Window | Uncached, first run | Cached, first run | Uncached, slid window | Cached, slid window | Cached, same window |
|---|---|---|---|---|---|---|
| P99 latency per bucket and service | 24 hours | 181.3 | 187.0 | 145.9 | 17.1 | 7.0 |
| | 7 days | 1180.6 | 1197.1 | 974.3 | 23.3 | 14.5 |
| Error count with a full-text term | 24 hours | 319.7 | 329.8 | 257.4 | 21.8 | 5.2 |
| | 7 days | 2212.7 | 2375.9 | 1910.5 | 25.8 | 12.8 |
| Top 10 endpoints | 24 hours | 165.9 | 161.2 | 119.1 | 14.0 | 3.3 |
| | 7 days | 1151.4 | 1121.1 | 1014.6 | 38.8 | 15.8 |
| Distinct users (`approx_distinct`) | 24 hours | 105.2 | 108.2 | 84.1 | 12.3 | 3.7 |
| | 7 days | 675.5 | 682.3 | 523.6 | 17.6 | 11.3 |

  The cache held between 0.4 MB and 19 MB of states per cached query. Cached and uncached results were identical except for `approx_percentile_cont`, whose result depends on the order in which states are merged: cached values differed from uncached ones by up to 4.3%, as much as two uncached runs differed.

  The same queries over 24 hours ran on a cluster of two datanodes, with 96 million rows in four regions on MinIO behind 10 ms of added latency. The datanodes restarted with empty local caches before each first run.

| Query | Uncached, first run | Cached, first run | Uncached, slid window | Cached, slid window | Cached, same window |
|---|---|---|---|---|---|
| P99 latency per bucket and service | 370.5 | 374.9 | 154.9 | 34.4 | 9.1 |
| Error count with a full-text term | 525.8 | 543.3 | 256.4 | 38.8 | 7.1 |
| Top 10 endpoints | 357.0 | 366.8 | 127.8 | 32.7 | 5.3 |
| Distinct users (`approx_distinct`) | 289.8 | 307.2 | 108.5 | 43.5 | 19.7 |

Drawbacks:

- The first execution of a query does more work: up to 7% more time in the measurements above. When the rule must add a grouping key, the key multiplies the groups by the number of slices, so a plan that needs one is rewritten only from its second run.
- Slices are not cached while a memtable overlaps them, and a memtable spans everything from its oldest row to its newest. A late write therefore disables the cache from its timestamp to the present until the next flush. A dashboard that only shows the last 15 or 30 minutes, the default window of several of these front ends, gains little.
- Aggregations whose states grow with their input are not cached. On the 8-day data, an exact `count(DISTINCT user_id)` per bucket and a top 10 of `user_id` by count, with 2 million distinct users, both exceeded the 256 MiB per-aggregation bound. The run that exceeds the bound pays for collecting the states: 17% more time for the distinct count over 24 hours, 9% over 7 days, and 26% and 1% for the top 10. The plan is then not rewritten until the idle timeout passes, and later runs take the same time as uncached ones.
- Only aggregations with a partial stage are rewritten. A query that groups by the whole primary key aggregates each series within one partition, and a `min` or `max` without grouping pushes its own dynamic filter into the scan; neither uses the cache.
- Correctness depends on the fingerprint observing every change to the rows of a range. A new engine operation that changes rows without replacing files or bumping one of the versions in the fingerprint would make the cache return stale results. The engine contract and its tests must cover this requirement.

# Implementation Plan

1. Add `RegionScanner::time_range_fingerprint`, implement it for mito, and test every change path listed in [Data fingerprint](#data-fingerprint).
2. Add the optimizer rule and `SliceAggregateExec`, experimental and disabled by default.
3. Report slice hits and misses in `EXPLAIN ANALYZE` and as a metric.
4. Test writes that interleave with a query, and verify distributed mode.

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
4. **Sequence fence.** Bounding the memtable reads of rewritten scans by a sequence would make each query read exactly one version, at the cost of behaving differently from uncached scans.
5. **Leaving experimental.** The cache stays off by default until there is evidence from real dashboards: the hit rate by query shape, the reasons queries are not rewritten, the bytes held by the cache, and the first-run overhead on larger clusters with object storage. Distributed mode has so far been measured on two datanodes with a local MinIO only.

# Future Work

- A metric engine fingerprint, which the windowed PromQL evaluation in #10569 could use to cache results. Logical tables share one physical region, so a flush or compaction of the physical region invalidates slices of every logical table it covers.
- Caching slices of the current hour once flushed data covers part of the hour, by splitting the slice at the memtable's minimum timestamp.
- Incremental refresh, as StarRocks does for tablets: when a slice has only gained new files since its entry was computed, aggregate the new files and merge the result into the cached states instead of recomputing the slice.
- Admission by estimated state size. When the grouping and aggregated columns have HLL sketches in their SSTs, merging the sketches of the files that overlap the window estimates the number of groups and distinct values, so the rule can skip plans whose states would exceed the per-aggregation bound before running them. High-cardinality top-N and exact distinct counts would then pay nothing on their first run, while low-cardinality distinct counts stay cached. Columns without sketches keep the current behavior.

# References

- VictoriaMetrics rollup result cache: `app/vmselect/promql/rollup_result_cache.go` in [VictoriaMetrics/VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics), and the [rollup result cache documentation](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#rollup-result-cache).
- Grafana Mimir query-frontend flags `-query-frontend.split-queries-by-interval`, `-query-frontend.max-cache-freshness`, and `-query-frontend.results-cache-ttl`: `cmd/mimir/help-all.txt.tmpl` in [grafana/mimir](https://github.com/grafana/mimir).
- [ClickHouse query cache](https://clickhouse.com/docs/operations/query-cache).
- OpenObserve streaming aggregation cache: `src/search/src/datafusion/physical_optimizer/stream_aggregate.rs` and `ZO_CACHE_DELAY_SECS` in `src/config/src/config.rs`, in [openobserve/openobserve](https://github.com/openobserve/openobserve).
- [Elasticsearch shard request cache](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/shard-request-cache).
- [Apache Druid query caching](https://druid.apache.org/docs/latest/querying/caching/).
- StarRocks query cache: `be/src/exec/query_cache/cache_operator.cpp` in [StarRocks/starrocks](https://github.com/StarRocks/starrocks).
- DataFusion aggregation modes: [`AggregateMode`](https://docs.rs/datafusion/latest/datafusion/physical_plan/aggregates/enum.AggregateMode.html).
