# frontend — Agent & Contributor Guide

Navigation aid for `src/frontend`. Keep it short and point to code. Paths are
relative to the repo root.

Repo-wide rules that apply here: [`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

## What this crate does

Frontend is the request entry point and orchestration layer. It accepts
multi-protocol requests (gRPC, HTTP, MySQL, PostgreSQL, InfluxDB, OTLP, Jaeger,
Prometheus, OpenTSDB), checks permissions, parses/plans SQL, and dispatches:
reads go to the query engine (`query` crate), writes go to the inserter/deleter
(`operator` crate).

Boundary with `servers`: the `servers` crate implements the wire protocols and
network I/O; `frontend` provides the business logic by implementing handler
traits (`SqlQueryHandler`, `GrpcQueryHandler`, `InfluxdbLineProtocolHandler`,
...). In standalone mode the `standalone` crate bridges the frontend to an
embedded datanode `RegionServer`; in distributed mode the frontend talks to
remote datanodes via `operator`/`client`.

## Module map

| Module | Path | Purpose |
| --- | --- | --- |
| `instance` | `src/frontend/src/instance.rs` | `Instance`: the core handler; implements `SqlQueryHandler`, `PrometheusHandler`, etc. |
| `instance/builder` | `src/frontend/src/instance/builder.rs` | `FrontendBuilder` assembles `Instance` from its dependencies |
| `instance/grpc` | `src/frontend/src/instance/grpc.rs` | `GrpcQueryHandler`: insert/delete/query/promql over gRPC |
| `instance/region_query` | `src/frontend/src/instance/region_query.rs` | Routes distributed region reads to datanodes |
| `instance/*` | `src/frontend/src/instance/` | Per-protocol handlers (`influxdb.rs`, `promql.rs`, `otlp/`, `jaeger.rs`, `logs.rs`, `prom_store.rs`, ...) |
| `frontend` | `src/frontend/src/frontend.rs` | `Frontend` lifecycle wrapper (`FrontendOptions`, start/shutdown) |
| `server` | `src/frontend/src/server.rs` | `Services`: builds and wires the protocol servers |
| `heartbeat` | `src/frontend/src/heartbeat.rs` | Heartbeat to metasrv; handles suspend / cache invalidation |
| `service_config` | `src/frontend/src/service_config/` | Per-protocol option structs |

## Request lifecycles

- **SQL query** (`instance.rs`): `do_query_inner` handles parsing, interceptors,
  permission checks, timeout/cancellation, and delegates planning/execution to
  `StatementExecutor`. Distributed scans enter through `region_query.rs`.
- **Insert** (`instance/grpc.rs`): `handle_inserts` / `handle_row_inserts` →
  `check_permission` → `operator`'s `Inserter` (schema validation, optional
  auto-create, partition routing, meter admission) → local `RegionServer`
  (standalone) or RPC to datanodes (distributed). Arrow bulk inserts pass the
  request channel to `Inserter` and check meter admission for each nonempty batch.
- Finite ingestion requests split internally admit their total rows per database
  before dispatch (`operator::insert::admit_write` / `admit_row_insert_batches`).
  The returned context covers chunks and derived writes while preserving WCU
  accounting and the original protocol channel.
- Internal gRPC listeners mark requests with `Channel::Internal` in middleware
  (`server.rs`), including requests handled by Enterprise Flight wrappers.

- **Flight bulk insert** (`instance/grpc.rs`): initializes on the first batch after
  the lazy schema handshake; checks permissions and reconciles missing columns
  through `Inserter` once per stream, then reuses the refreshed table.

- **Logical-table batching** (`instance/logical_batcher.rs`): `Services` initializes
  one shared batcher for opted-in HTTP Prom and nonlegacy OTLP metric-engine
  writes. OTLP checks operator eligibility and falls back for incompatible tables.
  The schema adapter holds a weak instance reference to avoid an ownership cycle.

- **Table batching** (`instance/builder.rs`): protocol entry points opt in through
  `QueryContext`. The primary inserter prepares eligible ordinary-table writes
  for `servers::batcher::table::TablePendingRowsBatcher`. A separate execution-only
  inserter, with no batcher attached, sends the prepared bulk writes to datanodes,
  avoiding recursive batching. The batcher handles successful-write Flow notifications.

## Public surface

- `Instance` (`instance.rs`) — the business-logic container.
- `Frontend` (`frontend.rs`) — lifecycle wrapper around `Instance` + servers + heartbeat.
- Created from `cmd`: `src/cmd/src/frontend.rs` (distributed) and
  `src/cmd/src/standalone.rs` (standalone, with embedded datanode).

## When you change X, also touch Y

- **`servers` handler traits**: a new/changed protocol handler requires the
  matching `impl` here.
- **`operator` Inserter/Deleter or `query` QueryEngine API**: update the call
  sites in `instance.rs` / `instance/grpc.rs`.
- **`session::QueryContext`**: new context fields thread through most handlers.
- **`sql` statements**: new statement kinds need handling in `query_statement`.

## Testing

```bash
cargo nextest run -p frontend
```

## Gotchas

- Keep the frontend/servers split straight: wire format and network live in
  `servers`; permissions, planning, and routing live here.
- Standalone vs distributed diverge in datanode access (the `standalone` crate's
  local `RegionServer` adapter vs `NodeClients` RPC), MetaClient usage, and
  whether heartbeat matters. In standalone, the cache invalidator is a no-op.

## Maintenance contract

Update this file when you add a protocol handler, change the query/insert
lifecycle, or change how `Instance` is constructed or wired to `servers`.
