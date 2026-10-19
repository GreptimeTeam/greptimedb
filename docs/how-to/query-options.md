# Query options

Query options adjust selected behavior for a connection or request; they are not server-wide settings or resource-pool controls.

| Canonical option | SQL aliases | Values |
| --- | --- | --- |
| `query.parallelism` | `query_parallelism`, `query.parallelism` | Integer `1`–`1024`; server default `0` means automatic CPU-based parallelism. This is not a resource-pool limit. |
| `query.allow_query_fallback` | `query_fallback`, `allow_query_fallback`, `query.allow_query_fallback` (legacy `ALLOW_QUERY_FALLBACK` syntax / `query_fallback` hint also work) | `true` or `false` |
| `query.enable_remote_dynamic_filter_pushdown` | canonical name | `true` or `false` |
| `datafusion.optimizer.*` | canonical name | Keys, types, defaults, and validation come from the linked DataFusion version (for example, numeric `max_passes` and Boolean `filter_null_join_keys`). |

Use `SET query_parallelism = 4` or `SET query.parallelism TO 4`; `SHOW VARIABLES query.parallelism` works in MySQL and PostgreSQL, which also accepts `SHOW query.parallelism`. `SHOW VARIABLES <option>` reports the effective value. Unspecified options inherit engine settings; defaults are automatic parallelism, fallback `false`, and remote dynamic-filter pushdown `true`. Invalid values and unsupported names are errors; unknown third-party MySQL/PostgreSQL initialization settings retain existing tolerance.

HTTP `X-Greptime-Hints` (for example, `query.parallelism=4`) and gRPC `x-greptime-hints` request metadata set request options; gRPC hints are not a protobuf `Hints` field. The Rust Flight client offers `Database::sql_with_hint` and `flight_request().with_hints`. Hints override session `SET`, including within a multi-statement HTTP request. SQL `SET` lasts for its connection only; `SET LOCAL` transaction-scoped restoration is not implemented.

`query.enable_remote_dynamic_filter_pushdown` controls propagation across frontend-to-datanode remote queries; it is distinct from DataFusion's native `datafusion.optimizer.enable_dynamic_filter_pushdown`.

Internal flow/query-reserved keys are not public query options. HTTP response timeout `x-greptime-timeout` defaults to `0` (no deadline); this is a response deadline, not a per-statement or stream/body-processing limit.
