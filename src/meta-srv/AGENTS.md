# meta-srv — Agent & Contributor Guide

Navigation aid for `src/meta-srv`. Keep it short and point to code. Paths are
relative to the repo root.

Repo-wide rules that apply here: [`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

## What this crate does

Metasrv is the metadata and coordination service for distributed mode: it
persists cluster/table/region metadata, runs leader election, drives the
heartbeat loop that tracks cluster topology, and orchestrates distributed
procedures (DDL, region migration, repartition). Data models, the KV backend
abstraction, election, key encoding, and the DDL manager live in `common-meta`;
`meta-srv` implements the server, state machine, and control logic on top.

## Module map

| Module | Path | Purpose |
| --- | --- | --- |
| `bootstrap` | `src/meta-srv/src/bootstrap.rs` | Wires up KV backend, election, gRPC services, procedure manager |
| `metasrv` | `src/meta-srv/src/metasrv.rs`, `src/meta-srv/src/metasrv/builder.rs` | `Metasrv` struct, `MetasrvOptions`, `MetasrvBuilder`, startup |
| `state` | `src/meta-srv/src/state.rs` | Leader/Follower state machine and transitions |
| `handler` | `src/meta-srv/src/handler/` | Heartbeat handler chain (`HeartbeatHandler`): leader check, region lease, stats, mailbox |
| `service` | `src/meta-srv/src/service/` | gRPC services (`heartbeat`, `procedure`, `cluster`, `store`) and HTTP admin |
| `procedure` | `src/meta-srv/src/procedure/` | Distributed procedures: `region_migration/`, `repartition.rs`, `wal_prune/` |
| `region` | `src/meta-srv/src/region/` | Region supervisor, lease keeper, failover triggers |
| `selector` | `src/meta-srv/src/selector/` | Region placement strategies (round-robin / load-based / lease-based) |
| `cluster` | `src/meta-srv/src/cluster.rs` | `MetaPeerClient`: internal RPC with leader fallback |
| `cache_invalidator` | `src/meta-srv/src/cache_invalidator.rs` | Pushes cache-invalidation to frontends/datanodes |
| `key` | `src/meta-srv/src/key/` | Metasrv-side KV key encoding |

## Core flows

- **Heartbeat**: `service/heartbeat.rs` receives datanode/frontend heartbeats and
  runs the handler chain in `handler/`; responses carry mailbox messages (DDL
  results, cache invalidation).
- **DDL**: `service/procedure.rs` (`ddl`) is leader-only and hands off to
  `common-meta`'s `DdlManager`, executed via `common-procedure`.
- **Region migration**: `procedure/region_migration/manager.rs` plus per-step
  files; triggered by failover or an explicit command.
- **Leader election**: via `common-meta`'s election; transitions in `state.rs`.

## Public surface

Clients use the `meta-client` crate (`MetaClient`). On the server side the entry
points are the gRPC services under `src/meta-srv/src/service/` and the HTTP admin
API under `src/meta-srv/src/service/admin/`.

## When you change X, also touch Y

- **Heartbeat interval / leases**: timing constants live in `common-meta`
  (`distributed_time_constants`); changing them affects lease and failover timing
  in `handler/region_lease_handler.rs` and `region/supervisor.rs`.
- **Procedures**: state must stay persisted in the KV backend and `execute()`
  must be idempotent for crash recovery.
- **Cache invalidation**: new metadata types need matching invalidation in
  `cache_invalidator.rs` and the heartbeat publish handler.
- **Selector stats fields**: keep `selector/` implementations in sync with the
  stats they consume.

## Testing

```bash
cargo nextest run -p meta-srv
# mock election / KV backend:
cargo nextest run -p meta-srv --features mock
```

Helpers: `src/meta-srv/src/mocks.rs`, `src/meta-srv/src/test_util.rs`,
`src/meta-srv/src/procedure/test_util.rs`.

## Gotchas

- Most mutating operations are leader-only — guard them (see the leader check in
  `handler/` and `service/procedure.rs`). Non-leaders return "not leader".
- In-memory state is not durable; anything needed after a leader change must be
  persisted to the KV backend. The leader uses a cached KV backend that resets on
  leader change.
- Region lease renewal timing and the supervisor's check interval must be
  coordinated, or a region can appear active on two datanodes.

## Maintenance contract

Update this file when you add a procedure, change the heartbeat handler chain,
change leader-only boundaries, or alter the metasrv ↔ common-meta split.
