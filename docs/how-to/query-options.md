# Query options

Query options let a caller adjust selected query behavior for a connection or a
single request. They are not server-wide settings, execution/runtime controls,
or a way to configure the server's resource pools.

## Supported options

| Canonical option | SQL aliases | Values |
| --- | --- | --- |
| `query.parallelism` | `query_parallelism`, `query.parallelism` | Integer `1`–`1024`; server value `0` continues to mean automatic CPU-based parallelism. Sets the query's parallelism/partition bound, not a server resource-pool limit. |
| `query.allow_query_fallback` | `query_fallback`, `allow_query_fallback`, `query.allow_query_fallback` | `true` or `false` |
| `query.enable_remote_dynamic_filter_pushdown` | same | `true` or `false` |
| `datafusion.optimizer.*` | canonical name | Boolean, limited to: `repartition_joins`, `repartition_aggregations`, `repartition_sorts`, `repartition_windows`, `enable_round_robin_repartition`, `prefer_existing_sort`, `prefer_hash_join`, `join_reordering`, `enable_topk_aggregation`, `enable_dynamic_filter_pushdown` |

For example:

```sql
SET query_parallelism = 4;
SHOW VARIABLES query.parallelism;
SET query.allow_query_fallback = true;
SHOW VARIABLES query.allow_query_fallback;
SET datafusion.optimizer.repartition_joins = false;
```

`SHOW VARIABLES <option>` shows the effective value. PostgreSQL also accepts
`SET query.parallelism TO 4` and `SHOW query.parallelism`. Boolean values must be
`true` or `false`. Unknown, unsupported, and non-allowlisted DataFusion options
are rejected; arbitrary DataFusion configuration is not enabled.

The existing `ALLOW_QUERY_FALLBACK` SQL syntax and `query_fallback` hint remain
available as aliases for `query.allow_query_fallback`.

Unspecified options inherit the query engine's settings. With the default server
configuration, parallelism is automatic, query fallback is `false`, and remote
dynamic-filter pushdown is `true`. For the pinned DataFusion version, the listed
optimizer switches default to `true` except `prefer_existing_sort`, which defaults
to `false`. `SHOW VARIABLES <option>` reports the effective value rather than an
unset session override.

## Scope and precedence

- SQL `SET` is connection-scoped. Later statements on that connection observe
  the setting; a different connection does not inherit it.
- HTTP `X-Greptime-Hints` supplies request-scoped options, for example
  `X-Greptime-Hints: query.parallelism=4`.
- A request hint takes precedence over a session `SET` for the same option. In a
  multi-statement HTTP call it continues to win even if a statement executes
  `SET`.
- `SET LOCAL` transaction lifecycle semantics are not implemented; do not
  rely on transaction-scoped restoration.

HTTP SQL example (`/v1/sql` accepts form data):

```sh
curl 'http://localhost:4000/v1/sql' \
  -H 'x-greptime-hints: query.parallelism=4,datafusion.optimizer.repartition_joins=false' \
  --data-urlencode 'sql=EXPLAIN SELECT * FROM my_table'
```

For gRPC, hints are sent as `x-greptime-hints` request metadata, not as a
protobuf `Hints` field. The Rust Flight client supports
`Database::sql_with_hint(sql, &[...])` and `flight_request().with_hints(&[...])`;
for example, pass `[("query_parallelism", "4")]` as the hints slice.

## Dynamic filters

`query.enable_remote_dynamic_filter_pushdown` controls dynamic-filter
propagation across the frontend-to-datanode remote query boundary. It is
separate from `datafusion.optimizer.enable_dynamic_filter_pushdown`, which
controls DataFusion's native optimizer pushdown. Changing one does not imply
changing the other.

## Other request controls and limitations

Read preference accepts the generic `read_preference` hint and the dedicated
`x-greptime-read-preference` HTTP header; when both are supplied, the dedicated
header wins. HTTP response deadlines use `x-greptime-timeout` (a duration such
as `5s`). The server default is zero, meaning no response deadline. A header
value of zero also disables the response deadline; an invalid value falls back
to the server default. A larger valid header value can currently override the
server default; no new cap is imposed. This limits the HTTP response, not
individual statements or response-stream/body processing.

Internal flow/query-reserved keys are not public query options. Internal
server-marked Flight metadata remains internal. Invalid values for recognized
query options produce an error. Existing tolerance for unknown initialization
settings from third-party MySQL/PostgreSQL clients is preserved.
