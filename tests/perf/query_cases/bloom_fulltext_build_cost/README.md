# Bloom full-text index build-cost fixture

This fixed `prom_remote_write_then_query` workload ingests 64 series × 512
samples (32,768 rows) through the existing remote-write path, then copies the
same source rows into eight fresh indexed Mito tables and eight no-index
controls. Each insert uses identical message construction. A one-query
`EXPLAIN ANALYZE VERBOSE SELECT count(*)` follows each insert to observe rows
before an explicit one-query `ADMIN FLUSH_TABLE`. Inserts, pre-flush scans, and
flushes have separate timings; the fixture specifies eight separate flushes per
arm. The indexed schema uses the English analyzer and Bloom backend with
`case_sensitive=false`. This index option does not make the SQL
`matches_term` scalar function case-insensitive: the cold explain and exact
`INFO` query use the uppercase indexed token, while a separate `request` query
checks a lowercase token through the English analyzer. `zqxabsenttoken` checks
absent-term behavior.

`validate_remote_write_source` is deliberately the first query and only counts
the source. It does not change source data. A pre-flush analyzed `count(*) WHERE msg IS NOT NULL` scan for each target
forces a read of the populated message column and is diagnostic outside the
insert/flush latency; do not infer a storage scan from an unfiltered aggregate. One later query
reports per-table materialized row counts, and a metadata query reports index
columns and types from `information_schema.statistics`. After all flushes, the
first full-text query is the cold indexed `EXPLAIN ANALYZE VERBOSE`; subsequent
`WHERE matches_term` queries report per-table exact `INFO`, lowercase `request`,
and absent-token counts. Query thresholds of 30% on insert/flush entries are
regression diagnostics, not workload assertions.

For official CI B, verify before interpreting performance results that the
source and every target count are exactly 32,768, each indexed positive-term
query returns 32,768, each indexed absent-term query returns zero, and
`information_schema.statistics` confirms full-text metadata on indexed tables
only. Each pre-flush filtered `EXPLAIN ANALYZE VERBOSE` must show a scan of all
32,768 input rows from memory and no files read (the aggregate result itself is
one row); if a background flush already produced SSTs, do not treat that
insert/flush timing as the intended measurement. The
post-flush region-statistics query is advisory: `information_schema` statistics
may be heartbeat-cached and can lag immediate writes/flushes. Main must validate
flush row counts and resulting SST/index artifacts from run logs and available
file metadata; do not treat cached region statistics alone as proof. If required
invariants are not met, do not treat the comparison as a valid baseline/candidate
result.

The timing includes task completion and flush work (including synchronous
index generation, plus any storage-engine work incurred); it is **not** an
isolated tokenizer benchmark. It does not establish compaction cost. Do not
claim those effects are excluded unless the run's correctness/storage evidence
confirms it. The fixture uses the existing query runner unchanged and is a new
fixed workload, not a replay of the original 1,048,576-row input archive.
