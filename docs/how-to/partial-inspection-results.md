# Partial datanode inspection results

Distributed inspection of `information_schema.ssts_manifest`, `ssts_storage`, and
`ssts_index_meta` normally fails if any contacted datanode fails. For diagnostics
during an outage, opt in to incomplete results:

```sql
SET query.allow_partial_inspection_results = true;
SHOW VARIABLES query.allow_partial_inspection_results;
SELECT * FROM information_schema.ssts_manifest;
```

The option defaults to `false` and persists for the MySQL or PostgreSQL session.
HTTP requests can set the same option for one request:

```sh
curl 'http://localhost:4000/v1/sql' \
  -H 'x-greptime-hints: query.allow_partial_inspection_results=true' \
  --data-urlencode 'sql=SELECT * FROM information_schema.ssts_manifest'
```

The Flight/gRPC hints metadata accepts the same key. Request hints override
session defaults. Each query snapshots its options before execution.

Only datanode transport unavailability and per-node deadlines are tolerated.
Metadata lookup, authorization, planning, schema mismatches, and other execution
errors still fail the query. If no datanode completes successfully, the query
fails even if some rows have already been streamed. A successful empty datanode
response counts as a success. A query limit can stop consumption early.

If a datanode disconnects after returning batches, those batches are retained.
Aggregates, joins, and ordering therefore operate on incomplete input and must
not be treated as complete cluster statistics. Ordinary distributed table scans
are unaffected by this option.

The inspection executor records a notice when partial results are enabled and
logs each omitted datanode. On failure it replaces the notice with a bounded
warning identifying an affected datanode. The HTTP response includes warnings
as a JSON array in `x-greptime-warnings`; MySQL exposes them via `SHOW WARNINGS`.
PostgreSQL's existing notice path runs before stream consumption, so it may
only report that partial results are enabled. Flight does not currently expose
these notices. Server logs contain the full node failure details.

The option is handled by the frontend inspection executor; it does not require
a new datanode RPC format or a corresponding option on older datanodes. It is
omitted when serializing remote query contexts, so ordinary queries in the same
session do not send an unknown option to older datanodes.
