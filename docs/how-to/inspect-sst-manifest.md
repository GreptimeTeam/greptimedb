# Inspect SST files before compaction

Use `information_schema.ssts_manifest` to inspect the files referenced by a region's manifest. Start with a summary and narrow the time range before requesting file details. The queries below are read-only; they do not schedule compaction.

Replace the example region ID and timestamps with the target region and time range.

```sql
SELECT COUNT(*) AS file_count,
       SUM(file_size) AS referenced_bytes,
       COUNT(*) FILTER (WHERE file_size < 8388608) AS small_file_count,
       SUM(file_size) FILTER (WHERE file_size < 8388608) AS small_file_bytes
FROM information_schema.ssts_manifest
WHERE region_id = 4664334483496
  AND max_ts >= '2026-01-01T00:00:00Z'::TIMESTAMP_NS
  AND min_ts < '2026-01-02T00:00:00Z'::TIMESTAMP_NS;
```

The time predicates select files overlapping the interval, including files extending outside it. Eight MiB is an example diagnostic threshold, not a compaction setting. Repeat this query for adjacent intervals to compare their file counts and input bytes. `SUM` returns NULL when no files qualify, while `COUNT` returns zero.

For a table summary, replace the region predicate with `table_id = <table_id>`. Counts describe manifest entries: shared SST references or copies on different nodes can appear more than once. The byte sum is not unique physical storage usage. Add a `node_id` predicate when investigating a particular copy.

Inspect a bounded sample after choosing a region and interval:

```sql
SELECT file_id, min_ts, max_ts, file_size, visible
FROM information_schema.ssts_manifest
WHERE region_id = 4664334483496
  AND max_ts >= '2026-01-01T00:00:00Z'::TIMESTAMP_NS
  AND min_ts < '2026-01-02T00:00:00Z'::TIMESTAMP_NS
ORDER BY min_ts, file_id
LIMIT 100;
```

A manifest also includes staging entries. Add `visible = true` if the investigation should cover only files in the region's visible version. This requires a visibility snapshot, and its predicate is checked by the query engine. Sorting remains at the frontend in distributed mode; `LIMIT 100` bounds the result, not the number of files examined for the sort.

## Aggregate pushdown

Distributed queries can send scalar aggregates to datanodes and merge their states at the frontend. Supported forms have no `GROUP BY` or `DISTINCT`: `COUNT(*)`, `COUNT(1)`, and `SUM(file_size)`, including combinations in one query. Aggregate `FILTER` clauses can compare `file_size` with an integer literal using a simple comparison. Ordinary `WHERE` predicates still apply before aggregation; supported region, table, node, path and timestamp predicates also prune metadata enumeration.

Standalone mode executes the same SQL through the ordinary aggregate plan. Grouped or distinct aggregates, complex aggregate inputs such as `SUM(file_size + 1)`, and aggregates over a limited subquery retain ordinary execution. Constant timestamp casts such as the examples above are supported. WHERE expressions containing other scalar-function calls or `LIKE` cause aggregate pushdown to fall back. Ordinary inspection still evaluates unsupported source predicates as residual filters; these conditions are never discarded.

Use `EXPLAIN` to check the actual distributed plan:

```sql
EXPLAIN SELECT COUNT(*), SUM(file_size),
       COUNT(*) FILTER (WHERE file_size < 8388608)
FROM information_schema.ssts_manifest
WHERE region_id = 4664334483496;
```

For an eligible distributed query, inspect the logical `SstManifestAggregate` marker, or the physical `DistributedInspectExec: query=…` beneath the frontend aggregate merge. A plain SST inspection followed by frontend aggregation indicates fallback. Pushdown reduces rows transferred to the frontend; it still visits the selected manifest metadata. File counts and bytes help choose investigation ranges, but do not establish a compaction CPU or memory budget.
