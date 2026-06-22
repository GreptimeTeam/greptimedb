# flow — Agent & Contributor Guide

Navigation aid for `src/flow`. Keep it short and point to code. Paths are
relative to the repo root.

Repo-wide rules that apply here: [`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

## What this crate does

Flownode is the stream-processing engine behind continuous aggregation /
materialized views. It runs in two modes:

- **Batching mode** (the default and the actively developed path): splits data
  into time windows and periodically runs aggregation SQL through the frontend,
  writing results back to a sink table.
- **Streaming mode** (the older dataflow engine): an incremental DFIR/dataflow
  compute graph that processes row-level diffs.

A flow without an explicit `flow_type` is created as **batching**.

## Module map

| Module | Path | Purpose |
| --- | --- | --- |
| `engine` | `src/flow/src/engine.rs` | `FlowEngine` trait: create/remove/flush/insert lifecycle |
| `adapter` | `src/flow/src/adapter.rs`, `src/flow/src/adapter/` | `StreamingEngine`, worker pool, dual-engine dispatch, table sources/sinks |
| `batching_mode` | `src/flow/src/batching_mode.rs`, `src/flow/src/batching_mode/` | `BatchingEngine`, task scheduling, time windows, frontend client, checkpoints |
| `compute` | `src/flow/src/compute/` | Streaming dataflow render/state |
| `expr` | `src/flow/src/expr.rs`, `src/flow/src/expr/` | Scalar/aggregate expressions and Map-Filter-Project |
| `plan` | `src/flow/src/plan.rs` | `TypedPlan` (reduce/join/MFP) |
| `transform` | `src/flow/src/transform.rs` | Substrait → flow plan |
| `df_optimizer` | `src/flow/src/df_optimizer.rs` | SQL → DataFusion logical plan → optimized plan |
| `repr` | `src/flow/src/repr.rs` | `Row`, `DiffRow`, `Batch`, `RelationDesc` |
| `server` | `src/flow/src/server.rs` | gRPC `Flow` service, `FlownodeBuilder`/`FlownodeInstance` |
| `heartbeat` | `src/flow/src/heartbeat.rs` | Reports flownode state/stats to metasrv |

Flow metadata lives in `common-meta`, not here:
`src/common/meta/src/key/flow/` and `src/common/meta/src/ddl/create_flow.rs`.

## Data flow

`Frontend → Flownode (gRPC)` → `FlowService` (`server.rs`) →
`FlowDualEngine` (`adapter/flownode_impl.rs`) routes by `FlowType`:

- Batching: marks dirty windows; a task later runs aggregation SQL via the
  frontend client and writes the sink table (`batching_mode/task.rs`).
- Streaming: worker threads apply incremental diffs and push to the sink
  (`adapter/worker.rs`, `compute/render.rs`).

## Public surface

- gRPC `Flow` service in `src/flow/src/server.rs`
  (`handle_create_remove`, `handle_insert`, `handle_delete`, `handle_mark_window_dirty`).
- `FlowEngine` trait in `src/flow/src/engine.rs`.
- Started from the `cmd` crate via `FlownodeBuilder` / `FlownodeInstance`.

## When you change X, also touch Y

- **Flow definition / options**: validation in `common-meta`'s `ddl/create_flow.rs`
  and the serialized `FlowInfoValue` in `common-meta`'s `key/flow/`.
- **New scalar/aggregate function** (`expr/`): also wire up evaluation in
  `compute/render.rs` (streaming) and ensure batching SQL handles it.
- **Persisted flow metadata**: keep `FlowInfoValue` backward compatible
  (`serde(default)` / `serde(alias)`).
- A change to one engine often needs the mirror change in the other.

## Testing

```bash
cargo nextest run -p flow
```

Helpers in `src/flow/src/test_utils.rs` (test context, test query engine).

## Gotchas

- Batching vs streaming differ a lot in latency, debuggability, and code path —
  confirm which mode a flow uses before reasoning about it. Batching is the
  default and the primary target.
- Streaming workers are `!Send`; cross-thread interaction goes through
  `WorkerHandle`, not the worker directly.
- Internal flow timestamps (`repr::Timestamp`, ms) are not necessarily the
  table's time column; window functions key off the diff timestamp.

## Maintenance contract

Update this file when the dual-engine routing, the gRPC surface, or the flow
metadata contract (shared with `common-meta`) changes.
