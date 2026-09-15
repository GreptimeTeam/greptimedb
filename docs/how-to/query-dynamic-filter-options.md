# Configure Query Dynamic-Filter Pushdown

Dynamic-filter pushdown can be controlled for an individual query or for a MySQL
session. The available Boolean options are:

- `enable_dynamic_filter_pushdown` (master switch)
- `enable_aggregate_dynamic_filter_pushdown`
- `enable_join_dynamic_filter_pushdown`
- `enable_topk_dynamic_filter_pushdown`

Values are resolved independently as: query hint, then session setting, then
the DataFusion default. After resolution, setting the master switch to `false`
disables every variant, even when a child option is `true`.

## Per-query HTTP hint

Send `x-greptime-hints` on every HTTP SQL request that needs an override. Its
value is a comma-separated list of `name=value` pairs:

```bash
curl -G 'http://127.0.0.1:4000/v1/sql' \
  -H 'x-greptime-hints: enable_dynamic_filter_pushdown=true, enable_topk_dynamic_filter_pushdown=false' \
  --data-urlencode 'sql=SELECT * FROM my_table'
```

HTTP requests do not share a session setting. In particular, a `SET` sent in
one HTTP request does not affect a later HTTP request; send the hint with each
query that needs it. An invalid Boolean value in a query hint returns an error.

## gRPC metadata hint

For a gRPC request, put the same hint value in its outgoing metadata:

```rust
request.metadata_mut().insert(
    "x-greptime-hints",
    "enable_dynamic_filter_pushdown=true,enable_topk_dynamic_filter_pushdown=false"
        .parse()?,
);
```

## MySQL session setting

On one MySQL connection, use Boolean literals with `SET`, then inspect a named
option with `SHOW VARIABLES`:

```sql
SET enable_dynamic_filter_pushdown = true;
SET enable_topk_dynamic_filter_pushdown = false;
SHOW VARIABLES enable_dynamic_filter_pushdown;
```

A per-query hint overrides the corresponding MySQL session setting.

## Deployment requirement

Both the frontend (FE) and datanode (DN) must use versions that support these
options. This requires no service TOML configuration change and no Protocol
Buffers schema change.
