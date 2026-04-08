---
Feature Name: Remote Dynamic Filter Propagation
Tracking Issue: N/A
Date: 2026-04-08
Author: @discord9
---

# RFC: Remote Dynamic Filter Propagation

# Summary

This RFC proposes a remote dynamic filter propagation mechanism for distributed queries. It lets frontend-produced dynamic filters reach remote datanode scans through a lightweight control plane, while preserving one rule: remote dynamic filters are an optimization only, never a correctness dependency.

# Motivation

Today, dynamic filters can improve local execution, but they do not automatically propagate to remote datanode scans in distributed queries. As a result, the frontend may already know that a probe-side scan can be narrowed, while the remote scan still runs with a weaker predicate and loses pruning opportunities.

We want a minimal design that:

- propagates dynamic filter updates to remote scans,
- keeps filter identity and lifecycle stable across register/update/unregister,
- and safely degrades when encoding, routing, RPC, or apply logic fails.

# Details

The high-level flow is:

1. A join on the frontend produces an alive dynamic filter.
2. `MergeScanExec` identifies the remote subscribers, generates a stable `filter_id`, and registers the alive filter into a query-scoped registry.
3. The initial remote read establishes the corresponding registration on the datanode side.
4. The frontend registry watches for dynamic filter updates via `wait_update` or generation changes.
5. Later updates and unregister messages are sent through the existing region unary RPC path.
6. The datanode applies these updates to query-scoped remote filter state and scan wrappers.
7. Query finish, cancel, or no-consumer conditions trigger unregister and cleanup.

## Identity

The logical identity of a remote dynamic filter is `query_id + filter_id`.

Region and scan metadata are routing information, not part of filter-state identity. `filter_id` only needs to be stable and unique within a single query.

The current recommendation is to derive `filter_id` from:

- `region_id`
- `producer-local ordinal`
- `canonicalized children fingerprint`

The following should not be included:

- `partition`
- transport metadata
- memory addresses or temporary runtime object ids

## Transport

This design reuses the existing region unary gRPC path:

- `RegionRequest.body.remote_dyn_filter`
- `RemoteDynFilterRequest.oneof action`
  - `update`
  - `unregister`

The initial remote read is responsible for register and scan setup. The unary RPC path is only for later `update` and `unregister` messages.

## Frontend registry

The frontend uses a query-engine runtime map:

- implementation near `src/query/src/dist_plan/remote_dyn_filter_registry.rs`
- storage model: `query_id -> Arc<RemoteDynFilterRegistry>`

This registry should not live on a single `MergeScanExec`, and it should not be stored in `QueryContext.mutable_session_data`. It is a query execution runtime object that owns watcher tasks, cleanup tail, and fanout state.

The registry lifecycle has three states:

- `Active`: accepts registrations and sends updates
- `Closing`: query ended; stop new registrations, send final cleanup messages, drain in-flight RPCs
- `Closed`: watchers stopped, state removable from the runtime map

The registry may outlive the main query execution briefly for cleanup, but it is not intended to be a long-lived global object.

## Responsibilities

On the frontend:

- the join produces alive dynamic filters,
- `MergeScanExec` bridges producers to remote subscribers,
- the registry watches updates and fans out RPCs.

On the datanode:

- the unary handler receives `update` and `unregister`,
- query-scoped remote filter state is keyed by `query_id + filter_id`,
- remote wrappers apply updates through existing predicate and scan refresh paths.

## Failure semantics

All failures must degrade safely:

- encode failure -> local-only filter
- RPC failure -> log/metric and degrade
- early update or missing target -> explicit buffer, drop+metric, or retry policy
- decode or remap failure -> disable remote optimization only

# Alternatives

## Registry on `MergeScanExec`

Rejected because lifecycle and cleanup would become fragmented across multiple bridge or exec instances in the same query.

## Registry in `QueryContext.mutable_session_data`

Rejected because this is the wrong ownership model. The registry is not session metadata; it is a query runtime object with watcher tasks and cleanup behavior.

## Long-lived global manager

Rejected for now because it is heavier than necessary. A query-engine runtime map is sufficient for the current design.

# Drawbacks

- The design introduces extra query runtime state and cleanup logic on both frontend and datanode.
- The initial version only covers the current minimal filter form and leaves larger membership propagation to later work.
- A clear policy is still required for updates that arrive before scan registration.

# Unresolved questions

1. What is the exact policy for updates that arrive before scan registration?
2. Should children fingerprint canonicalization become a shared helper?
3. What is the strict timing relationship between `is_complete` and final unregister?
4. Does the runtime map need a background sweep task, or is explicit reap enough?
5. How should large build-side membership evolve beyond `IN` in later work?
