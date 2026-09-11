# flow — Agent & Contributor Guide

Navigation aid for `src/flow`. Keep it short and point to code. Paths are
relative to the repository root.

Repo-wide rules that apply to here: [`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

## What this crate does

Flownode provides two flow profiles:

- **Streaming mode**: a single stateless DataFusion query profile. Each mirror
  insert is materialized as a transient input table, executed against the
  retained logical plan, and written directly to the sink through
  `batching_mode::frontend_client::FrontendClient`.
- **Batching mode**: splits data into time windows and periodically runs
  aggregation SQL through the frontend, writing results back to a sink table.

Users cannot select a mode directly: `flow_type` is a reserved internal option.
`StatementExecutor::determine_flow_type` in `src/operator/src/statement/ddl.rs`
owns current mode selection. `FlowDualEngine` defaults missing internal
`flow_type` metadata to batching for compatibility. Read those paths before
changing routing rules.

## Module map

| Module | Path | Purpose |
| --- | --- | --- |
| `engine` | `src/flow/src/engine.rs` | `FlowEngine` lifecycle contract |
| `adapter` | `src/flow/src/adapter.rs`, `src/flow/src/adapter/` | Stateless streaming execution, dual-engine dispatch, shared source schema/default normalization, and sink creation |
| `batching_mode` | `src/flow/src/batching_mode.rs`, `src/flow/src/batching_mode/` | `BatchingEngine`, scheduling, time windows, frontend client, and checkpoints |
| `repr` | `src/flow/src/repr.rs` | Shared row and relation schema representations |
| `server` | `src/flow/src/server.rs` | gRPC `Flow` service and flownode builders |
| `heartbeat` | `src/flow/src/heartbeat.rs` | Reports flownode state/stats to metasrv |

Flow metadata lives in `common-meta`, not here:
`src/common/meta/src/key/flow/` and `src/common/meta/src/ddl/create_flow.rs`.

## Data flow

`Frontend → Flownode (gRPC)` → `FlowService` (`server.rs`) → `FlowDualEngine`
(`adapter/flownode_impl.rs`) routes by `FlowType`:

- Streaming: mirror rows are normalized against the source schema, evaluated by
  the retained stateless DataFusion plan, and sent to the sink via
  `FrontendClient`.
- Batching: marks dirty windows; a task later runs aggregation SQL via the
  frontend client and writes the sink table.

`FlowType::Streaming` and mirror routing are shared contracts; preserve both
when changing dispatch. Preserve batching behavior and the shared schema,
default, and sink-creation helpers.

## Public surface

- gRPC `Flow` service in `src/flow/src/server.rs`.
- `FlowEngine` trait in `src/flow/src/engine.rs`.
- Started from the `cmd` crate via `FlownodeBuilder` / `FlownodeInstance`.

## Testing

```bash
cargo nextest run -p flow
```

Stateless streaming tests are under `src/flow/src/adapter/stateless.rs`.

## Gotchas

- Batching and streaming differ in latency and state. Confirm the selected mode
  before reasoning about a flow.
- Streaming is finite and stateless per mirror request; it has no worker graph,
  background runtime, or replay state.
- Internal flow timestamps and table time columns are separate contracts.
