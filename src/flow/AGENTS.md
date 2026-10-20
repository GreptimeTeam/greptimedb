# flow — Agent & Contributor Guide

Navigation aid for `src/flow`. Keep it short and point to code. Paths are
relative to the repository root.

Repo-wide rules that apply to here: [`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

## What this crate does

Flownode runs **one** flow execution profile: the batching scheduler.

- **Batching mode** (`batching_mode`): splits data into time windows and
  periodically runs aggregation SQL through the frontend, writing results back
  to a sink table. A flow whose SQL has no usable time window runs as an
  explicit full-query flow and needs `EVAL INTERVAL`.

The legacy streaming executor (per-insert stateless DataFusion query) has been
removed. There is no mode selection left: `FlowEngine` in
`src/flow/src/engine.rs` is implemented by `FlownodeEngine`
(`adapter/flownode_impl.rs`), which forwards everything to `BatchingEngine`.
`StatementExecutor::determine_flow_type` in `src/operator/src/statement/ddl.rs`
now only *validates* a flow definition and always reports batching.

Historical `flow_type: streaming` metadata stays readable and droppable, but it
must never reach a runtime: `FlownodeEngine::create_flow` rejects it with a
"recreate this flow" error. Do not delete `FlowType::Streaming` or
`get_flow_type_from_options` — old persisted options and old procedure data
must keep deserializing.

## Module map

| Module | Path | Purpose |
| --- | --- | --- |
| `engine` | `src/flow/src/engine.rs` | `FlowEngine` lifecycle contract |
| `adapter` | `src/flow/src/adapter.rs`, `src/flow/src/adapter/` | `FlownodeEngine` (create/drop/flush/recover, insert routing), auto-column constants shared with batching, and the retained-plan → sink-schema helper |
| `batching_mode` | `src/flow/src/batching_mode.rs`, `src/flow/src/batching_mode/` | `BatchingEngine`, scheduling, time windows, frontend client, and checkpoints |
| `server` | `src/flow/src/server.rs` | gRPC `Flow` service and flownode builders |
| `heartbeat` | `src/flow/src/heartbeat.rs` | Reports flownode state/stats to metasrv |

Flow metadata lives in `common-meta`, not here:
`src/common/meta/src/key/flow/` and `src/common/meta/src/ddl/create_flow.rs`.

## Data flow

`Frontend → Flownode (gRPC)` → `FlowService` (`server.rs`) → `FlownodeEngine`
(`adapter/flownode_impl.rs`) → `BatchingEngine`.

- `FlowService::handle_mirror_request` → `FlowEngine::handle_flow_inserts` →
  `BatchingEngine::handle_inserts_inner`. The mirrored full-row payload is how
  batching marks dirty windows (and `set_dirty()` for flows without a time
  window). **Keep this RPC and this path**: it is not a leftover of the removed
  streaming executor.
- A flow round later runs its SQL through the frontend client and writes the
  sink table.

## Public surface

- gRPC `Flow` service in `src/flow/src/server.rs`.
- Batching query execution uses `Channel::Internal` so meter collectors can
  account for sink writes without charging ingestion quota.
- `FlowEngine` trait in `src/flow/src/engine.rs`.
- Started from the `cmd` crate via `FlownodeBuilder` / `FlownodeInstance`.

## Testing

```bash
cargo nextest run -p flow
```

There are no stateless streaming tests left; adapter tests cover the retained
plan-to-schema helper shared with batching.

## Gotchas

- Internal flow timestamps and table time columns are separate contracts.
- `adapter.rs` still owns `AUTO_CREATED_PLACEHOLDER_TS_COL` /
  `AUTO_CREATED_UPDATE_AT_TS_COL` and `output_column_schemas`; batching imports
  them. Do not move or delete them without updating `batching_mode`.
- `FlowConfig` no longer has `num_workers`: flow workers are gone. Old TOMLs that
  still set `flow.num_workers` keep parsing because serde ignores unknown fields
  (covered by a test in `adapter/tests.rs`). Do not re-add the field.
- Recovery collects per-flow errors and logs them
  (`adapter/flownode_impl.rs`): one rejected (old streaming) flow must not stop
  the other flows from recovering.
- `flow_type` is still written into flow options so the metadata shape does not
  change; it is a reserved internal key and must stay out of `SHOW CREATE FLOW`
  rendering. Writing it is a deliberate keep, not an oversight: dropping it would
  change the persisted metadata and the `options` column users can query, for no
  runtime benefit. Reading it must keep accepting `streaming` so legacy metadata
  stays listable and droppable (`get_flow_type_from_options`).
