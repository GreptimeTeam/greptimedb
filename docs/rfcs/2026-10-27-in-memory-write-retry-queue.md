---
Feature Name: Frontend Write Retry Queue (in-memory v1)
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
---

# Summary

This RFC defines **Frontend In-memory Write Retry Queue — Core v1**. A per-region write that still fails after the dispatcher's existing retry loop is currently lost; Core v1 adds an optional bounded in-memory frontend queue. At the dispatcher's give-up point, each retryable per-region `RoutedWrite` is encoded and retained; one replay worker later sends it directly to a datanode. The client still receives its original error, and `affected_rows` keeps its applied-rows meaning.

Core v1 is deliberately not the complete retry-queue design. Durable queue and recovery, topology-aware replay, and strong replay consistency and DDL fencing are separate design responsibilities. This RFC defines only the in-memory mechanism, its guarantees, and its explicit limitations.

The Core v1 guarantee is **bounded best-effort retry with duplicate-capable RPC submissions** — not delivery, at-least-once application, or exactly-once. A record can be admitted and never sent, sent and never applied, or applied more than once; a successful replay is not reported to the client. Admission is bounded by charged bytes and record count. A stored record replays in per-source-region enqueue order, with at most one in-flight record per key. Every release path releases its charge exactly once, no record is silently kept without a consumer, and no replay send is authorized without passing the final send gate under the state lock.

Core v1 is deliberately small: no writer task, channel, supervisor, gate registry, queue-side post-admission rebuild, multi-region unit replay, durable store, DDL fence, or exactly-once. The queue is off by default because a retrying client can duplicate a replayed write. It does not survive process exit; its DDL version check is a timing window, not an atomic fence; `TRUNCATE` and `UNDROP` are not detected. Flow source tables, `BulkInsert`, the pending-rows batchers, instant-TTL inserts, and paths without per-region plan metadata are excluded.

This Core v1 draft is not production-ready, and no test or benchmark was executed for it. The full rationale, exact source evidence, validation rows, and revision audit are in the [v4.4 design rationale and evidence document](41-v4.4-design-rationale-and-evidence.md); that companion predates this scope alignment and is not synchronized with it.

# Motivation

The frontend already retries a per-region write. Defaults are `max_retries = 3`, `initial_backoff = 1.5s`, `max_backoff = 10s`, and `backoff_multiplier = 2.0` (`src/operator/src/write_dispatcher/options.rs:35-43`). `WriteRetry::retry` uses `backon::ExponentialBuilder` with jitter (`write_dispatcher.rs:405-411`). In `backon 1.6.0` jitter is additive, so delays are `[1.5,3)`, `[3,6)`, and `[6,12)` seconds: a **10.5 to less than 21 second** backoff window plus up to four RPCs. The frontend-to-datanode channel deliberately has no request timeout (`src/cmd/src/frontend.rs:475-478`), so those RPCs are unbounded.

That loop is right for a brief outage. The intended Core v1 case is a brief datanode restart or region migration, not a tens-of-minutes outage. A datanode restart, a region migration, or a partition can take longer than the retry window, and only the client can retry today. A client has no per-region view: it either replays the whole statement, duplicating the regions that succeeded, drops the failed ones, or has exhausted its own budget. A 5-minute TTL is a proposed starting value, not an SLA. Region failover is off by default (`metasrv.rs:368`); when enabled it is phi-accrual triggered (`failure_detector.rs:81`), not lease-based. The TTL is a bound on how long a record may be replayed, not an outage-recovery SLA; the intended recovery window is calibrated later against measured restart and migration times. The queue does not cover every failover shape; that is a Core v1 limitation, not an open design choice.

Extending the loop is insufficient. It runs inside the client request, a client timeout drops it, it re-enters table and route refresh every round, and it returns one error. The queue decouples remaining work from the client while preserving the original response.

Issue #8085 asks for a bounded frontend-side buffer that replays asynchronously. The queue cannot make the client see success: a queued write has not been routed, is invisible to reads until replay, and in Core v1 lives only in one frontend's memory. Core v1 therefore keeps the error response and gets the core right first: admission, ordering, replay, and exact record-byte accounting.

# Goals

- Retain a bounded number of failed per-region writes after give-up, with explicit byte and record ceilings and a per-record size ceiling.
- Preserve every per-region failure, including its source region id, long enough to decide admission even when a sibling is terminal. A terminal sibling must not suppress a retryable one.
- Replay per source-region key in FIFO order, with at most one in-flight record per key. Different keys are independent.
- Check and charge admission in one critical section, and encode only after the reservation succeeds. The success path performs no queue work.
- Release every stored or reserved charge exactly once, including TTL expiry, attempt exhaustion, panic recovery, fatal teardown, and shutdown drain.
- Keep the client response and `affected_rows` unchanged. A queued write is never reported as success.
- Bound replay RPC occupancy with a client-side deadline, and bound record age and attempts.
- Make lifecycle and failure behavior explicit: what is shed, dead-lettered, deferred, drained, or lost with the process.
- Be removable by configuration and be off by default.
- Add no new error classification and no persisted or wire format.

# Non-goals

Core v1 is intentionally narrower than a complete retry-queue design. The following are separate design responsibilities; this RFC states the boundary and the risk it leaves, but does not design them:

- **Durable queue and recovery.** No durable retention, frontend-restart recovery, crash recovery, durable acknowledgment, or cross-process record recovery. Records live in process memory and die with the process.
- **Topology-aware replay.** No queue-side rebuild after admission, split/merge target remapping, target-set gate, Gate Registry, multi-region unit replay, cross-region unit replay, old-before-new ordering, or multi-target progress. Scope is one source region per record.
- **Strong replay consistency and DDL fencing.** No region-side DDL epoch, atomic send fence, region-side idempotency, client idempotency key, newer-write detection, or stale-replay protection. The plan-time table-version check does not close the check-to-send window; `TRUNCATE` and `DROP` followed by `UNDROP` are not detected. The DDL risks are documented in Consistency and Drawbacks; the fence is a separate future design.
- **Exactly-once, at-most-once submission, or acknowledged writes.** A record may be submitted more than once, and a successful replay is not reported to the client.

The following are Core v1 exclusions or extensions not provided by this version:

- Per-region or per-tenant fairness, and adaptive replay rate limiting or write-buffer-aware pacing.
- A queryable dead-letter store or a replay API. Core v1 exposes metrics and structured logs only.
- Automatic replay-worker restart or an explicit unhealthy state. Core v1 is fail-closed plus drain.
- A separate replay-timeout configuration key. Core v1 reuses `datanode.client.timeout` as a replay-only client deadline.
- Standalone mode. `write_queue.enable = true` is rejected by standalone option validation, as in PR #10045, because Core v1 has no standalone validation or shutdown path for the queue.
- Writes that never reach the dispatcher retry/give-up path. Whether every enterprise Flow child path stays outside that path was not audited, so Core v1 makes no coverage claim for those paths.
- Paths without per-region plan metadata, and requests that are not exactly one insert or delete region. Such a request is shed, never converted.
- `BulkInsert`, including both pending-rows batchers. `WriteDispatcherPhysicalFlushAdapter` (`src/servers/src/batcher/logical_table/region_write.rs:123-139`, `:235-247`) and the table batcher (`src/servers/src/batcher/table/batch.rs:67-81`) flush as `BulkInsert` through `Inserter::flush_bulk_batch` (`src/operator/src/bulk_insert.rs:52-80`); `send_batch` routes a non-insert/delete body through `send` (`write_dispatcher.rs:271-272`). With the queue enabled and retry installed, a retryable `BulkInsert` is retained and then shed at admission as `bulk_insert_out_of_scope`; it is never replayed. This exclusion is a Core v1 author decision.
- Instant-TTL inserts, which are filled in the flownode and are not covered by the admission path.
- Flow source tables. `Inserter::do_request` builds a `FlowMirrorTask` from the same region requests and detaches it before `dispatcher.prepare`/`execute`; the detached task resolves flownode peers from `table_flownode_set_cache` and issues `handle_inserts` per peer, dropping rows over the pending-rows budget (`insert.rs:837-866`, `:2116-2182`, `:2196-2272`, `:2219-2235`). Replay is a direct send of the retained write and does not re-enter `do_request`, so replaying such a write would add an unmirrored source write. Because the live mirror is already best effort this is not a proven new source/sink inconsistency, but Core v1 keeps `flow_source_table` fail-closed rather than invent mirror-replay semantics.
- If live retry rebuilt one statement into several target regions before admission, treating those targets as one unit. Each failed target is admitted independently under its own target key: there is no unit atomicity, cross-target order, or unit completion.

# Details

## Where the queue sits

Core v1 implements the queue as an operator-layer module under `src/operator/src/write_queue/`, wired by the frontend. This avoids a new workspace member and a durable-shaped trait while the only implementation is in memory.

Three parts:

- **Queue state** — one `Mutex<QueueState>` holding charged counters and per-key deques. There is no writer task and no channel.
- **Admission** — once per retained write at give-up: check eligibility, reserve ledger room, encode, commit.
- **Replay worker** — one task on the global runtime that joins children, leases ready heads, spawns bounded per-record tasks, and recovers `JoinError`s.

```text
client write -> Inserter/Deleter -> plan + PlanMetadata
  -> WriteDispatcher::prepare (peer-grouped RegionRequest)
  -> execute / send_batch (per-region RoutedWrite on failure)
  -> retry / give-up: MergedAttempts (all terminal + all retryable writes)
  -> give_up: log every terminal error; admission for each retryable write
  -> client receives the original error
  -> replay worker: join -> sweep -> lease -> checks -> route -> final gate
       -> one RPC -> task applies exactly one transition -> JoinSet result
```

A successful write never calls admission, encodes a second time, takes the queue state lock, or allocates a queue payload buffer. It may carry plan metadata when the queue is on; with the queue off, none is built. `spawn_global` returns a tokio `JoinHandle`; the runtime has no supervision or restart policy (`src/common/runtime/src/global.rs`); v1 adds none.

Scope is one source region per record. The queue does not reconstruct a multi-region statement. If live retry rebuilt one statement into several target regions before admission, each failed target is admitted independently under its own target key: no unit atomicity, cross-target order, or unit completion.

Relative to the original PR #10045 RFC, Core v1 changes the implementation shape, not just the prose: an operator-local `Mutex<QueueState>` instead of a new `common-retry-queue` crate, `QueueStore`, and writer task/channel; source-region-absent dead-letter instead of queue-side rebuild and new-key gates; and a bounded-best-effort guarantee instead of the original qualified at-least-once wording. It keeps the client-error/`affected_rows` contract, per-region FIFO, the append/flow/bulk/instant exclusions, and standalone rejection. The 64x/768 MiB numbers are downgraded to `UNVERIFIED`. The guarantee change needs maintainer acceptance.

## Architecture and ownership

| State | Meaning | Charge owner |
| --- | --- | --- |
| Reserved | Ledger check succeeded; payload may still be encoding. | `AdmissionCharge` |
| Stored | A `QueueEntry` is at a key. | `QueueState` |
| Leased | A replay task holds the head under a `LeaseToken`. | `QueueState`; the lease has replay rights, not release authority |
| Completed / Dead-lettered / Lost | Removed after success, terminal decision, or shutdown drain. | Released |

`Record` is immutable. `QueueEntry` is mutable scheduling state owned by `QueueState`. A lease holds `Arc<Record>` plus a `LeaseToken`; it does not own charge release or mutable fields (`send_attempts`, `consecutive_failures`, `ready_at`, `in_flight`). There is no `LeaseGuard` and no Drop-based state mutation. The task performs at most one transition; the worker recovers from `JoinError`, and stops scheduling on a fatal invariant for the wrapper to tear down.

Full shapes and helper signatures are in the [Evidence document](41-v4.4-design-rationale-and-evidence.md).

```rust
struct Record {                 // immutable, shared by Arc
    id: u64, key: RegionId,
    table_id: TableId, rows: u64, table_version: u64, // table_id = key.table_id()
    payload: Bytes, payload_len: u64,       // payload_len == payload.len()
    enqueued_at: Instant, deadline: Instant,
    give_up_failure: FailureContext,        // summary only, no full Error
}
struct QueueEntry { record: Arc<Record>, send_attempts: u32,
    consecutive_failures: u32, ready_at: Instant }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LeaseToken { record_id: u64, lease_epoch: u64 }
struct Lease { token: LeaseToken, record: Arc<Record> }
struct QueueState { charged_bytes: u64, charged_records: u64, closed: bool,
    next_record_id: u64, next_lease_epoch: u64, keys: HashMap<RegionId, RegionQueue> }
struct RegionQueue { entries: VecDeque<QueueEntry>, in_flight: Option<LeaseToken> }
struct TaskMeta { key: RegionId, record_id: u64, lease_token: LeaseToken }
// wrapper-owned map, borrowed by the worker: HashMap<tokio::task::Id, TaskMeta>
```

`RoutedWrite` gains `region_id: RegionId` and `meta: Option<AdmissionMeta>`; `TableVersion` is carried as its `u64`. `AdmissionCharge` owns the reservation before commit; `LeaseToken` is the replay capability and never owns a charge or removal.

**Unique outcome model (Model A).** There is **one** authority for each normal transition. `begin_send` is a gate, not a transition: under the state lock it checks `closed`, head id, lease token, deadline, and `send_attempts`, and returns `Allowed`, `TtlExpired`, `AttemptsExhausted`, `QueueClosed`, or `LeaseMismatch`. It never pops or releases. `Allowed` increments `send_attempts` and is the send-authorization linearization point. The per-record task then calls at most one of `complete`, `defer`, or `dead_letter` under the same token; `TtlExpired`/`AttemptsExhausted` map to one `dead_letter`. `QueueClosed` performs no transition: it returns so drain can own removal. `LeaseMismatch` is an internal invariant violation: it sets `closed`, records the reason, fires the separate `fatal` signal, and returns without touching any record; the worker exits scheduling and the wrapper tears down.

The task returns a `TaskOutcome` describing what it did, for metrics/logs only. The worker's transition authority covers only `JoinError`; a successful join is diagnostic:

- `Ok((id, outcome))`: remove `TaskMeta`; if the entry is absent, fatal `task_meta_missing` — the outcome is not inspected and no record is guessed; otherwise record the outcome. The task already transitioned; the worker does not touch the record. `TaskOutcome` is diagnostic; a wrong or incomplete outcome cannot change state.
- `Err(e) if e.is_panic()`: remove `TaskMeta`; if absent, fatal `task_meta_missing`; otherwise recover under the token. If head and token still match, apply the unified terminal order (Replay): deadline passed -> one `dead_letter` (`ttl_expired_after_send_authorized` if the record has ever passed the gate, i.e. `send_attempts > 0`, else `ttl_expired_before_first_send`); else `send_attempts >= max_attempts` -> `dead_letter(send_attempts_exhausted)`; else clear `in_flight` and defer with backoff. If the task already transitioned, the match fails and recovery is a no-op.
- `Err(e) if e.is_cancelled()`: remove `TaskMeta`; if absent, fatal `task_meta_missing`; otherwise this is not a normal failure. If the queue is open and the token matches, apply the same terminal order; if neither condition holds (closed, or shutdown drain), leave the record for drain.

**Fatal invariant violations.** `LeaseMismatch`, a checked charge subtraction that would underflow, lease-epoch overflow, and a join result with no `TaskMeta` are fatal. The discoverer sets `closed`, records a `FatalReason`, fires the `fatal` token, and performs no record transition. It never guesses a record id or lease and never runs token recovery, because a join result identifies a task, not a lease: `JoinError` itself carries no `TaskMeta`, and `JoinError::id()` yields only the task id. This holds for a successful join as well as panic or cancellation: a missing entry for an `Ok` result is the same map invariant violation. The worker leaves its scheduling loop; the wrapper joins children and drains. The queue never keeps scheduling after a fatal condition. This is a control-flow signal, not a second transition authority and not a supervisor.

A record cannot be joined twice; `JoinSet` removes the entry. A record cannot be re-leased before the worker processes its result because the worker drains all join results **before** the lease loop, and a panic leaves `in_flight` set until recovery. A task that panics after a terminal transition cannot cause a second release or a second terminal metric: the token is gone, recovery is a no-op, and the panic is reported only by the separate join-error metric. Loss of a terminal metric in that window is possible; duplication is not. A panic does not imply the RPC was not submitted.

Why this is not a second authority:

- On the normal path, the task's transition is the only state change. The join result carries no mutation primitive; the worker cannot call `complete`/`defer`/`dead_letter` from `TaskOutcome`.
- On the panic path, the task never returned a normal outcome. The worker's recovery is the first and only transition for that attempt, token-guarded. If the task panicked after committing its transition, the token is already cleared and recovery matches nothing. A new same-key record has a different `record_id`, so it cannot match either.
- On the abort path, the task cannot have committed a normal transition after its last await; if it did so before, the same token check makes recovery a no-op. Drain removes only after all children have joined.
- A stale `TaskMeta` cannot mutate a new record: its `(record_id, lease_epoch)` is checked against the head, and the epoch is unique per lease. Removing the map entry twice is impossible because the map is keyed by a unique task id.
- The worker never holds the state lock across an `await`; each transition is a short critical section, so a task and a recovery cannot interleave inside one release.

## Admission and memory accounting

Admission is per retained write, in observed order. Eligibility checks may `await` table/flow cache reads. After the last awaited check and before the reserve, admission takes a second cancellation checkpoint; the final reserve/encode/commit phase has no `await` and no user code under the state lock.

Admission rejects, with the client error unchanged:

| Condition | Shed reason |
| --- | --- |
| Cancellation observed before admission or at the pre-reserve checkpoint | no record |
| `BulkInsert` body | `bulk_insert_out_of_scope` |
| Not exactly one insert/delete region | `malformed_record` |
| `RoutedWrite.meta` is `None` | `plan_metadata_missing` |
| Region id or table id mismatch | `malformed_record` |
| Table info missing or lookup error | `table_info_unavailable` |
| `TableIdent.version` differs from plan time | `plan_version_changed` |
| Append-mode table | `append_mode_table` |
| Flow-source table | `flow_source_table` |
| Flow-cache lookup error | `flow_source_lookup_failed` |
| Impure default on a dedup key | `impure_default_on_dedup_key` |
| Encoded length over `max_record_size` | `record_too_large` |
| Deadline overflow | `deadline_overflow` |
| Ledger or id reservation fails | `budget_exhausted`, `record_id_exhausted`, `queue_closed` |

A record that is not admitted is simply not compensated. The queue never waits for room and never evicts an older record; rejection equals not having the queue. Core v1 uses one global byte and record budget with first-come admission. There is no per-region or per-tenant fairness reservation. This is a Core v1 author decision; the contention risk is in Drawbacks. Append mode is read from `TableInfo.meta.options.extra_options[APPEND_MODE_KEY]` (`src/table/src/requests.rs:203`; `src/store-api/src/mito_engine_options.rs:23`). A flow source is a non-empty or erroring `table_flownode_set_cache.get(table_id)` (`insert.rs:251-257`), fail closed.

Admission outcomes are exhaustive:

| Outcome | Ledger | `keys` | Client error |
| --- | --- | --- | --- |
| Not eligible | unchanged | unchanged | unchanged |
| Reserved, then encode panic/mismatch | reservation released | unchanged | unchanged |
| Reserved, commit sees `closed` | reservation released | unchanged | unchanged |
| Reserved, commit succeeds | charge converted to stored | entry pushed | unchanged |
| Caller cancelled before the admission call | unchanged | unchanged | unchanged |
| Caller cancelled during an eligibility `await`; checkpoint observes it | unchanged | unchanged | unchanged |
| Caller cancelled after the checkpoint, during reserve/encode/commit; commit wins | stored | entry pushed | unchanged |

There is no “admitted but uncharged” and no “charged but unreachable” state: the guard either releases or converts to a stored entry under the same lock as the push.

**Reserve, encode, commit.**

1. **Cancellation checkpoint.** After every awaited eligibility check and before any reservation, read the request cancellation token. If cancelled, return the `cancelled` disposition with no reservation and no record; do not set `closed`. This is the admission cancellation linearization point. The remaining phase is synchronous, so a cancellation observed after this point cannot stop the commit; if commit wins, the record is admitted.
2. **Reserve.** Under the state lock, check `closed`, `charged_bytes.checked_add(payload_len) <= max_bytes`, and `charged_records.checked_add(1) <= max_records`. Allocate the next record id with `checked_add(1)`; overflow sheds `record_id_exhausted`. Add both counters and return an armed `AdmissionCharge`.
3. **Encode.** Outside the lock, allocate exactly the reserved capacity and encode. `encoded_len()` and `encode` run inside `catch_unwind(AssertUnwindSafe(...))`; a panic drops the guard and sheds `encode_panic`; a length mismatch sheds `encode_size_mismatch`. `Bytes::from(vec)` moves the vector; no payload copy.
4. **Commit.** Under the state lock, recheck `closed`. If open, push the entry and disarm the guard. If closed, subtract the reservation, disarm, and shed `queue_closed`.

`payload_len` is `u64` and must equal `payload.len()`. `encoded_len()` and `payload.len()` are `usize`; conversion uses `u64::try_from` and allocation uses `usize::try_from(payload_len)`, both checked. No `as` cast and no unchecked `Instant` arithmetic. `AdmissionCharge::drop` releases only the reservation and never touches `keys`; commit and guard never release the same reservation. A huge `Vec::with_capacity` can still abort on allocation failure; that is outside the panic-recovery claim.

## Metadata safety

Admission needs two values not recoverable from the encoded request: `table_version`, captured while the plan is built, and `impure_key_filled`, whether `fill_reqs_with_impure_default` appended a primary-key or time-index column for this request. Reading the version at admission or handoff is too late: retries refresh the table cache, so an enqueue-time read can already be post-ALTER and pass against itself.

`PlanMetadata` is built only when the queue is enabled. `Inserter::do_request` already has `table_infos` and calls `fill_reqs_with_impure_default` (`insert.rs:800-809`); the filler already knows `primary_key_indices` and the impure column set (`fill_impure_default.rs:44-69`, `:84-116`). The code PR extends `fill_rows` to report whether an appended column is in the dedup key and returns a per-region `HashSet<RegionId>`, so a non-key fill on one region does not shed another region of the same table.

`Deleter::handle_table_delete` already has `table_info` (`delete.rs:119`); `do_request` builds and executes the delete at `:131-149`. Delete paths return versions they already load; the impure set is empty.

The dispatcher carries `PlanMetadata` to `execute`/`send_batch`/`send` (a signature addition). `send_batch` is the only place that creates per-region `RoutedWrite`s after an error. For each split request it computes `write_region_ids` (the current `write_region_id` accepts one region and also accepts `BulkInsert`, `write_dispatcher.rs:715-733`) and attaches `AdmissionMeta` only when the table's plan-time version is present. Missing version attaches no meta; the map is never indexed speculatively.

| Path | Plan source | Admission behavior |
| --- | --- | --- |
| Normal insert | `Inserter::do_request` holds `table_infos`; filler reports per-region fill set | `PlanMetadata` with per-table version and per-region impure set |
| Delete | `handle_table_delete` holds `table_info` | Versions from that info; empty impure set |
| Logical/physical split | `send_batch` computes region ids and attaches meta | Attaches only when the table's plan-time version is present |
| Live retry rebuild | `reroute_failed_writes` -> `prepare` -> `send_batch` | Same rule; a rebuilt target without a plan-time version gets no meta |
| Ordinary `dispatch` | no `PlanMetadata` | `meta: None` -> shed `plan_metadata_missing` |
| Bulk/batcher flush | no `PlanMetadata`; body is `BulkInsert` | Shed `bulk_insert_out_of_scope` |

Those paths are non-goals, not a silently weaker mode.

**Impure default.** `fill_reqs_with_impure_default` fills normal insert requests only; instant inserts are filled in the flownode. v1 extends it to report, per region, whether an appended column is in the dedup key. A table with an impure default only on a non-key value column remains admissible, because the replay's dedup key is unchanged. Delete is always empty. Core v1 sheds the dedup-key case. Admitting it would preserve the write but would document a duplicate risk that cannot be collapsed without idempotency; Core v1 does not provide that. This is a Core v1 author decision, not an open policy question.

**Bulk, batcher, and missing metadata.**

| Case | Source | What happens | Queue reason |
| --- | --- | --- | --- |
| `BulkInsert` from `flush_bulk_batch` | `bulk_insert.rs:52-80` | Enters `prepare`/`execute`; non-insert/delete routes through `send` | Shed `bulk_insert_out_of_scope`; never queued |
| `BulkInsert` from either pending-rows batcher | `logical_table/region_write.rs:123-139`, `:235-247`; `table/batch.rs:67-81` | Same dispatcher path; both flush as `BulkInsert` | Shed `bulk_insert_out_of_scope`; never queued |
| Ordinary `dispatch` with no `PlanMetadata` | `dispatch_inner`, `write_dispatcher.rs:339-350` | A retryable failure can retain a `RoutedWrite` with `meta: None` | `plan_metadata_missing` |
| Future caller constructs a `BulkInsert` `RoutedWrite` | defense in depth | Admission rejects the body | `bulk_insert_out_of_scope` |

With the queue enabled, retry is always installed, so a retryable `BulkInsert` failure is retained and reaches admission, where it is shed; without the queue, existing retry applies. Either way it is not queued. This exclusion is a Core v1 author decision.

## Replay

A wrapper owns the `JoinSet` and `HashMap<tokio::task::Id, TaskMeta>`; the worker borrows them mutably, so the wrapper can still shut down and drain if the worker future panics. On each ~200 ms tick the worker drains finished tasks, sweeps expired non-leased heads, leases ready heads while permits are available, and spawns one task per record. A permit is acquired before the lease and held from before metadata lookup and decode until the send outcome. A record waiting for a permit is not leased.

`lease_ready` increments `next_lease_epoch` with `checked_add(1)` and marks `in_flight = Some(LeaseToken { record_id, lease_epoch })`. The lease carries the token and `Arc<Record>`. The task's normal transition requires the exact token; a stale token is a no-op. There is no `LeaseGuard`; no Drop implementation touches queue state. If epoch allocation overflows, the worker sets `closed`, records `lease_epoch_exhausted`, fires the `fatal` signal, and leaves the scheduling loop; the wrapper tears down and drains. Unreachable in practice, but checked rather than wrapping.

```text
loop {
    select! {
        _ = cancel.cancelled() => break,       // normal shutdown
        _ = fatal.cancelled() => break,        // invariant violation
        _ = ticker.tick() => {}
    }
    while let Some(joined) = tasks.try_join_next_with_id() {
        let id = match &joined { Ok((id, _)) => *id, Err(e) => e.id() };
        let Some(meta) = map.remove(&id) else {   // before any state change
            fail_closed(TaskMetaMissing); break;  // no token recovery possible
        };
        handle_join(joined, meta);
    }
    if fatal.is_cancelled() { break; }
    sweep_expired_unleased(now);
    while let Ok(permit) = permits.try_acquire() {
        let Some((key, lease)) = state.lock().lease_ready(now) else { break };
        let token = lease.token;
        let meta = TaskMeta { key, record_id: token.record_id, lease_token: token };
        let handle = tasks.spawn(run_record(key, lease, permit)); // moves lease
        map.insert(handle.id(), meta);        // this worker cannot join before insert
    }
}
```

The code must not use `try_join_next()`: it returns `Result<T, JoinError>` with no id. It must not treat `JoinSet::spawn()` as returning a task id: it returns an `AbortHandle`. In locked tokio 1.52.3, `spawn` returns `AbortHandle` (`join_set.rs:142`), `AbortHandle::id()` is the stable task id (`runtime/task/abort.rs:72`), `try_join_next_with_id()` returns `Option<Result<(Id, T), JoinError>>` (`join_set.rs:352`), and `JoinError::id()`/`is_panic()`/`is_cancelled()` exist (`runtime/task/error.rs:40,63,130`). The success id comes from the `Ok((id, outcome))` tuple; the panic and cancellation id comes from `JoinError::id()` — the two are taken separately, never from a field that does not exist. The map key is exactly `handle.id()`. If a join result has no map entry, the worker fails closed: it sets `closed`, records `task_meta_missing`, fires `fatal`, and stops; it does not inspect the outcome, guess a record, or attempt token recovery. This holds for success as well as panic/cancellation, because the task id does not identify the lease. `LeaseToken` is `Copy` (derived; two integers), so `let token = lease.token` copies the token and `lease` is still movable into `run_record`; with a non-`Copy` token the line would need `lease.token.clone()` and a partial move would not compile. There is no `await` between `spawn` and `insert`, so this worker cannot enter the join drain in that window; the spawned task may run and finish on another worker thread, but only this worker consumes join results and the wrapper drains only after this worker returns, so the entry is inserted before any join result can remove it. The worker drains joins before leasing, so panic recovery, defer, or dead-letter is processed before the same record can be re-leased. A panic leaves `in_flight` set; no successor can take the key until recovery clears or removes the record.

Worker invariants, checked by debug assertions and tests:

- `map.len()` equals **spawned tasks whose join result has not yet been removed**; it may include a completed task that is waiting to be joined, so it is not “live children.”
- Every `spawn` inserts exactly one entry; every success, panic, and cancellation join the worker consumes removes exactly one by the real task id. A join result with no entry is fatal, not guessed. After a fatal stop, the wrapper teardown clears entries whose join results the worker did not consume; no entry survives teardown.
- A key has at most one `in_flight` token, and its `record_id` names the queue head.
- A record is leased only when `now >= ready_at`, it is the head, and no token exists; a permit is held before the lease.
- A record with `in_flight = Some(token)` is never swept, never leased again, and never removed by a non-owner except drain after all children joined.
- `charged_records` equals the sum of deque lengths plus outstanding reservations; `charged_bytes` equals the sum of stored `payload_len` plus outstanding reservations.
- `closed` is monotonic: once true, no commit, no new lease, and no new `Allowed` gate result.
- The worker never holds the map or state lock across an `await`; the map is wrapper-owned and borrowed, so no other task can observe a partially updated entry.
- A join result is handled before the next lease, so the number of in-flight tasks never exceeds `replay_concurrency`.

**Per-record path.** Phases: `Preparing` (steps 1–6), `SendAuthorized` (`begin_send` returned `Allowed`), `RpcInFlight` (requester invoked once), `Finished` (one terminal transition or a no-op). Expiry before `SendAuthorized` means **this pass** did not authorize a send; expiry after it may have a remote effect. The terminal reason is chosen by **lifetime** authorization, not by the expiring pass: `send_attempts == 0` -> `ttl_expired_before_first_send`; `send_attempts > 0` -> `ttl_expired_after_send_authorized`, even when the expiring pass never reached the gate. Five events are distinct and must not be collapsed: the deadline passes before authorization; `Allowed` is returned under the state lock; the requester future is first polled; the client-side RPC deadline fires; and the remote application outcome, which the client cannot know. The diagnostic records `send_attempts`, `current_pass_authorized`, `ever_send_authorized` (`send_attempts > 0`), and `remote_outcome_unknown`; `after_send_authorized` means a send gate was passed at some point, not that an RPC was submitted, delivered, or applied.

1. **Preparation deadline.** Steps 2–6 run under `tokio::time::timeout_at(record.deadline, ...)` (the absolute instant, not a recomputed duration). If it expires, the task calls its single `dead_letter` with the lifetime reason: `ttl_expired_before_first_send` when `send_attempts == 0`, otherwise `ttl_expired_after_send_authorized`; this pass never passed the gate, and the diagnostic records `current_pass_authorized = false`. In the first case there is no remote effect; in the second an earlier pass was authorized and may have applied. This outer bound does not race the RPC: after `SendAuthorized`, the RPC has its own absolute deadline, so two timeouts never compete for one lease.
2. **Early TTL.** If `now >= deadline`, dead-letter with the same lifetime TTL reason without decoding. This is an optimization; the gate in step 7 is authoritative.
3. **Table metadata.** Missing -> `table_not_found`; version differs -> `table_version_changed`; now append-mode or flow-fed -> `table_became_ineligible`; transient error -> defer with backoff.
4. **Route.** Resolve the physical route and classify `Leader`, `Leaderless`, `Absent`. `Leaderless` defers; a first `Absent` triggers the one forced refresh in Routing and layout changes before it can become terminal. A route lookup or invalidation error defers with backoff, subject to TTL.
5. **Decode.** Decode error is terminal `decode_error` (the in-memory record is corrupt, not a datanode rejection).
6. **Client acquisition.** `node_manager.datanode(&peer)` is a cache lookup, bounded by remaining preparation TTL; timeout -> the same lifetime TTL reason (`current_pass_authorized = false`).
7. **Final gate.** `begin_send` under the state lock checks `closed`, head id and lease token, then the deadline, then `send_attempts < max_attempts` (deadline first, so an expired-and-exhausted record gets the TTL reason), and returns exactly one outcome. It never removes or releases. The task maps it as follows:

   | `BeginSendOutcome` | Task action |
   | --- | --- |
   | `Allowed` | `send_attempts += 1`; enter `SendAuthorized`; this is the send-authorization linearization point |
   | `TtlExpired` | one `dead_letter` with the lifetime reason: `ttl_expired_before_first_send` when `send_attempts == 0`, else `ttl_expired_after_send_authorized`; no send on this pass |
   | `AttemptsExhausted` | one `dead_letter(send_attempts_exhausted)`; no new send |
   | `QueueClosed` | return without a transition; drain owns removal |
   | `LeaseMismatch` | fatal invariant violation: set `closed`, record `lease_mismatch`, fire `fatal`, return without touching any record; worker exits scheduling; wrapper tears down |

8. **Send.** After `Allowed`, exactly one requester call guarded by `tokio::time::timeout_at(rpc_deadline, call)`, where

   ```text
   client_deadline = now.checked_add(datanode_client_timeout).unwrap_or(record.deadline)
   rpc_deadline = client_deadline.min(record.deadline)
   ```

   are computed once after authorization; no `WriteRetry` re-entry, no fan-out. The addition is checked because `Instant` arithmetic is not guaranteed to be representable far in the future. On overflow, `record.deadline` is a valid instant (admission computed it with checked arithmetic and shed the record on overflow, `deadline_overflow`) and is the tighter or equal of the two bounds, so the fallback can only shorten the client wait, never extend it past the record's TTL. No unchecked `now + duration` remains on this path. An absolute instant is used so a task delayed between authorization and first poll cannot extend its wait; `timeout_at` polls the call once before reporting elapsed, so a call first polled after `rpc_deadline` may still have been submitted (unknown outcome, classified as after-send). The guarantee is about **authorization**, not invocation: `Allowed` is checked and returned under the state lock with `now < record.deadline`, but the requester future may be first polled, and the RPC may complete, after the deadline. The state lock is released before this await. A concurrent close after the gate does not revoke the authorization, and this task may invoke the requester after `closed = true`. There is no “no await so close cannot race” guarantee; close stops new leases and new `Allowed` results only. Shutdown aborts and joins such tasks, then drains; an aborted or timed-out call does not prove the datanode did not execute it. A no-invocation-after-close rule would need an additional barrier/handshake and is not claimed by v1.
9. **Outcome after send authorization.** One terminal order applies to every retryable outcome (retryable RPC error, RPC timeout, `RetryAfterRouteRefresh` after invalidation, and recovery from panic/cancellation while the queue is open): if `now >= deadline`, one `dead_letter(ttl_expired_after_send_authorized)` with an explicit “remote side effect may exist” diagnostic; else if `send_attempts >= max_attempts`, one `dead_letter(send_attempts_exhausted)`; else defer with backoff. A successful RPC response -> `complete`, even if the deadline has passed (a known applied write is not discarded). `RetryAfterRouteRefresh` first invalidates the shared route caches; invalidation failure is logged and then takes the same terminal order, not immediate death. `NonRetryable` -> dead-letter `non_retryable`. The after-send reason is never reported as “not sent,” and it is not interchangeable with the before-send reason. `AttemptsExhausted` in the gate remains the safety net for a defer that races the budget; the normal path dead-letters at this step instead of taking a useless extra backoff and lease cycle that would block the key.

A per-record task panic is a `JoinError`, not a worker panic. A panic before `begin_send` leaves `send_attempts` unchanged; after `begin_send` it leaves it incremented. Recovery uses `TaskMeta`'s token under the state lock: a mismatched token no-ops, `closed` waits for drain, and an expired/exhausted record is terminal-removed by recovery, otherwise deferred. A task panic does not imply the RPC was not submitted. A shutdown abort is a cancelled task: no defer; drain removes.

**`send_attempts` semantics.** `send_attempts` counts successful passes through the final send gate. It is incremented in `begin_send` when the gate returns `Allowed`, with `checked_add`; overflow is exhausted/dead-letter, never wrapping. The requester is invoked later, outside the lock, so the counter records authorization, not a confirmed invocation. It is never rolled back on panic, abort, timeout, or error. It is **not** a confirmed-RPC count: the transport can fail to send, retry internally, or lose the client future. Actual RPC counts require transport instrumentation.

| Event | `send_attempts` | `consecutive_failures` |
| --- | --- | --- |
| Metadata/route lookup failure, leaderless | no | yes |
| Route invalidation failure (the step, not the RPC before it) | no | yes |
| Panic before / after `begin_send` | no / yes | yes / yes |
| RPC timeout or retryable RPC response | yes | yes |

Conservative rule: where source cannot distinguish, treat the event as an unknown-outcome retry. `max_attempts` compares `send_attempts`.

**TTL, backoff, and the replay deadline.**

| Bound | Counts | Does not count | Terminal when |
| --- | --- | --- | --- |
| `max_attempts` | final send-gate passes for one record | lookup, decode, backoff, permit wait, no-send deferrals | the outcome sees `send_attempts >= max_attempts` (the next `begin_send` gate is the race safety net) |
| `ttl` | monotonic age from admission | nothing; checked before every send and at the task boundary | no new send may be **authorized**; an already-authorized RPC may be invoked and may finish |
| `max_record_size` | encoded payload bytes | decoded size | admission, before allocation |

No-send paths must not create a tight loop. `consecutive_failures` is updated on every retryable path, whether or not an RPC was sent. Equal jitter, increment-before-compute:

```text
consecutive_failures = consecutive_failures.saturating_add(1)
n = min(consecutive_failures, 5)
base = min(1s * 2^n, 30s)
delay = base/2 + U[0, base/2)          // never zero
ready_at = min(now.checked_add(delay), record.deadline)
```

First delay 1–<2 s, then 2–<4, 4–<8, 8–<16, 15–<30, then capped. With `max_attempts = 10`, nine backoffs sum to 90 s–<180 s; with nine 10 s RPC deadlines the tenth send starts at 180 s–<270 s. `max_attempts` generally binds before the 5-minute TTL; TTL is the backstop. These are arithmetic bounds for proposed defaults, not measurements.

TTL semantics: TTL forbids **authorizing** a new replay send at or after the deadline. `begin_send` returns `Allowed` only inside its state-lock critical section with `now < record.deadline`, and that is the only send-authorization linearization point. Authorization is not invocation and not transport submission: the task may be scheduled late, the requester future may be first polled later, and the RPC may be delivered and executed later still. To keep the wait from stretching with scheduling delay, the requester call is wrapped in `tokio::time::timeout_at(rpc_deadline, call)` with `rpc_deadline = now.checked_add(datanode_client_timeout).unwrap_or(record.deadline).min(record.deadline)` — an absolute instant computed once after authorization, not a relative duration recomputed per poll. The checked fallback is conservative: `record.deadline` is the tighter bound, so an `Instant` overflow shortens the wait rather than producing an invalid or later deadline. The replay deadline is a **client-side** cap; dropping/timing out the client future does not stop datanode execution, which may apply after timeout. A timed-out replay is unknown and retryable, and an after-authorization timeout is reported as `ttl_expired_after_send_authorized`, never as “no remote effect.” An in-flight RPC's charge stays until the terminal outcome or outer deadline; TTL does not release memory by itself. For no-send paths (metadata or route lookup, leaderless, decode, permit wait), TTL is the final time bound and `max_attempts` is not consumed, so a lookup outage cannot exhaust a record that never sent.

The frontend sets `channel_config.timeout = None` for frontend-to-datanode channels (`src/cmd/src/frontend.rs:475-478`); `RegionRequester::handle_inner` adds no unary timeout (`src/client/src/region.rs:176-238`). `ClientOptions.timeout` defaults to 10 s (`src/common/options/src/datanode.rs:28-52`), but normal writes overwrite it. Core v1 reuses that value as a replay-only deadline; when the queue is enabled, configuration requires it non-zero. Normal writes remain unbounded. This is a Core v1 author decision; no separate replay-timeout key is introduced.

## Routing and layout changes

The queue key is the source `RegionId` from the request header. `RegionId::new(table_id, region_number)` packs the table id in the high 32 bits and the region number in the low 32 bits (`src/store-api/src/storage/descriptors.rs:29-33,70-83`). `TableId` is documented universal-unique and `RegionNumber` unique within one table, so a full `RegionId` is globally unique. That is an encoding fact, **not a route-safety proof**: for a logical table the key carries the logical table id (`src/common/meta/src/ddl/create_logical_tables/region_request.rs:55-69`), while the cached physical route's region ids carry the physical table id (`src/partition/src/manager.rs:290-294`). Replay must therefore resolve physical identity before matching.

Replay and fail-closed rule:

1. `(physical_id, route) = find_physical_table_route_with_id(record.table_id)`; it returns the physical table id and route for both physical and logical tables (`manager.rs:81-118`). Scan only this route.
2. Collect candidates whose `region.id.region_number() == record.key.region_number()`. Accept a candidate only if its `region.id.table_id()` is `record.key.table_id()` or `physical_id`.
3. Exactly one candidate -> classify by leader. More than one -> `route_ambiguous` (terminal). Unexpected table id -> `route_inconsistent` (terminal). Never take the first match.
4. Zero candidates -> do **not** dead-letter on the first look. Invalidate the relevant route caches and resolve once more. Still zero -> `source_region_absent` (dead-letter); exactly one valid candidate -> continue as in step 3; multiple or inconsistent -> fail closed as above. A refresh (lookup or invalidation) error defers with backoff, subject to TTL; it is not treated as absence. There is at most one forced refresh per replay pass, so this cannot loop.

A single zero-candidate observation is not proof of absence: the cached `TableRoute` may predate a split/merge that added or removed the region, and the route cache is eventually consistent with the metadata store. The forced refresh uses the same invalidation surface as live retry — `CacheIdent::TableId` for the logical table id and the physical table id returned by `find_physical_table_route_with_id`, which drops `TableInfoKey`, `TableRouteKey`, `ViewInfoKey`, and the table's tombstone keys so the next get reloads from the metadata store (`cache_invalidator.rs:90-115`). At PR head the invalidation helper is the private `WriteDispatcher::invalidate_routes` and needs `&self` (`write_dispatcher.rs:576-599`); the queue therefore either shares that helper or is handed the `PartitionRuleManager` and `CacheInvalidator` handles — a wiring choice, not a new architecture.

At PR head `router::find_region_leader_ref` uses `.find(...)` and takes the first matching region number; it does not detect duplicates (`src/common/meta/src/rpc/router.rs:125-136`). Uniqueness of region numbers inside one physical route is an expected metadata property, but Core v1 does not treat it as verified input, so ambiguity fails closed rather than silently choosing. The same helper collapses “region absent” and “region present but leaderless” into `None`, so the queue does not use it for classification; it inspects `PhysicalTableRouteValue.region_routes` directly: `Leader(peer)` -> dispatch; `Leaderless` (one route, no leader) -> defer with backoff, rebuilding would be wrong; `Absent` (zero candidate) -> one forced refresh, then, if still zero, region gone (split/merge/repartition) and `source_region_absent`. The residual risk is now narrower: if the metadata store itself still returns the old physical route after invalidation (metadata propagation lag rather than frontend cache staleness), the second look still sees zero and the record is dead-lettered incorrectly. That window is not measured here and remains `UNVERIFIED`. Core v1 proposes terminal dead-letter on the second zero. The source-region-disappearance question asks only whether to add bounded re-confirmation within TTL; it does not ask for queue-side rebuild, target remapping, or old-before-new ordering, which are outside this RFC. Either way, there is at most one forced refresh per pass, and TTL bounds any re-confirmation.

`PhysicalTableRouteValue.version` is a route revision, not a record-validity token (`src/common/meta/src/key/table_route.rs:66-75`, `:139-160`). It is not compared to a record field; doing so would dead-letter on a leader move. The record carries only the plan-time `TableIdent.version`.

On `RetryAfterRouteRefresh`, the queue calls the same shared helper as live retry: `invalidate_routes` collects logical table ids, adds physical ids from `find_physical_table_route_with_id`, and invalidates `CacheIdent::TableId` for all of them (`write_dispatcher.rs:576-599`; `src/common/meta/src/cache_invalidator.rs:90-108`). The queue does not rebuild, call `prepare`, or create a new target; it re-resolves the same source region on the next pass.

Post-admission layout changes are a documented limitation: the write may be lost even though admitted; no rows go to a new target; no gate is needed because a rebuilt record never lands in a new region's queue; per-key FIFO and one-in-flight remain true. If live retry rebuilt a request **before** admission, each failed target is admitted under its own key; a target that succeeded is not queued; there is no rollback or unit-level dead letter. The old-before-new guarantee is not provided. Topology-aware replay and queue-side rebuild are a separate future design, not a Core v1 option.

## Consistency

**Ordering.** Per-key FIFO applies to records admitted under the same queue key (a source region id). Different keys are independent. There is no global order and no order against newer live writes. A failed head blocks its key until it completes, dead-letters, or TTL expires; the worker never leases a younger entry while the head exists, but may serve other keys. If live retry rebuilt a write into several target regions before admission, those targets become independent records under their own keys.

`prepare` groups per-peer requests through a `HashMap` (`write_dispatcher.rs:135-170`), so cross-peer order is not deterministic across runs. `attempt_ordinal` is the index in the exact observed `Vec<WriteAttempt>`; it is diagnostic, not a promise about client or peer order.

**Duplicate risks.** Five risks are distinct: (1) **existing retry** can resend a request that already applied; (2) **queue replay** can resend a request whose original RPC applied but whose response was lost; (3) **replay timeout** may have applied and cannot be distinguished from “not applied”; (4) **client retry** after the error can race a pending queued record; (5) **rebuilt live retry** may resend a target that succeeded in an earlier round, and queued targets can duplicate each other's rows. v1 adds no unit protocol, no deduplication, and no idempotency key. A timed-out replay is unknown. A successful replay is not reported. The append-mode and impure-dedup-key sheds are the only cases v1 refuses to admit.

**What the plan-time version check does.** The plan captures `TableIdent.version` from the `TableInfo` already loaded by `Inserter`/`Deleter` (`insert.rs:800-809`, `delete.rs:119`). Admission and replay compare it against current cached table info. The version is bumped when table metadata such as schema changes (`src/table/src/metadata.rs:122-130`); ALTER does `new_info.ident.version = table_info.ident.version + 1` (`src/common/meta/src/ddl/alter_table/executor.rs:323`).

This is a **timing window, not an atomic DDL fence**:

| When the new version becomes visible | Result |
| --- | --- |
| Before the admission check | Admission sheds `plan_version_changed`; no record |
| After admission, before the replay check | Replay dead-letters `table_version_changed`; no send |
| After the replay check, before the RPC takes effect | No atomic fence. ALTER can commit concurrently; the datanode applies under whatever schema it has. The race remains. |
| After the RPC has taken effect | Cannot be revoked. A late duplicate or late effect is possible. |
| Never visible locally (stale cache) | The check passes. Visibility depends on invalidation delivery, not the version value alone. |

`CatalogManager::table_info_by_id` reads `TableInfoCache::get_by_ref` (`src/catalog/src/kvbackend/manager.rs:420-431`). The cache uses `InitStrategy::VersionChecked` (`src/common/meta/src/cache/table/table_info.rs:50`), which retries initialization when the version changes during a load; it does not revalidate a cached value against the metadata store on every hit (`src/common/meta/src/cache/container.rs:48-57`, `:140-190`). “Visible” means the local invalidation was delivered and the reload completed; a frontend that has not received it can pass a stale check. `TableInfoValue.version` is a separate metadata-store revision used by the cache (`src/common/meta/src/key/table_info.rs:84-114`); it is not the plan-time `TableIdent.version`.

**TRUNCATE, DROP, UNDROP.**

- **DROP.** Missing table at admission sheds `table_info_unavailable`; at replay dead-letters `table_not_found`. A recreated table has a new table id and cannot satisfy the old region key.
- **UNDROP.** `UndropTableProcedure` restores stored dropped-table metadata (`src/common/meta/src/ddl/undrop_table.rs:78-155`; `restore_table_metadata` at `src/common/meta/src/key.rs:1284-1304`), including the stored `TableInfoValue`. A plan-time version check may therefore pass across a drop/undrop gap. Core v1 does **not** claim detection; the code PR must test the exact version behavior.
- **TRUNCATE.** `TruncateTableProcedure` checks the table name, gets the physical route, and sends truncate requests; it does not update the table-info version (`src/common/meta/src/ddl/truncate_table.rs:55-216`). The version check cannot detect a truncate. Today the retry loop finishes before the client receives the error (`write_dispatcher.rs:398-458`), so a client `TRUNCATE` after the error cannot race a frontend retry. With the queue it can: the write fails, the client truncates, and old rows can return up to `ttl` later. This is a correctness boundary, not a feature.

`PhysicalTableRouteValue.version` is a route revision, not a table-validity token; it is not checked against the record. Core v1 adds no DDL coordination protocol. A region-side DDL epoch or atomic send fence is a separate future consistency design, not implemented here. The DDL risk acceptance question asks only whether maintainers accept this documented Core v1 risk; it is not a request to design the fence in this RFC.

## Failure handling

**Terminal failures are not queue records.** At PR head, `merge_attempts` returns `Err` on the first terminal attempt and drops all retryable siblings (`write_dispatcher.rs:649-675`); `retry` uses `merge_attempts(...)?` at `:429` and `:454`, and `reroute_failed_writes(...).await?` at `:442`. A terminal sibling therefore discards retryable work before admission.

Core v1 changes the internal give-up result to keep everything: `MergedAttempts.terminal` holds one `TerminalFailure` per failed attempt with one `FailureContext` per region and the full `Error`; `MergedAttempts.retryable` holds every retryable write, context, and full error until the one-time diagnostic. `GiveUp.client_error` is the first terminal error in observed order, otherwise the first retryable error. `GiveUp.terminal_failures` keeps one `FailureContext` per terminal region (summary only). `GiveUp.retained` keeps every retryable write with its context; the full error is not retained. `Record` never holds an `Error`.

**Terminal failures do not enter the retry queue.** They are not dead-lettered, because dead-lettering is a queue-record outcome and no queue record exists. `finish` calls `log_terminal_failure(&contexts, &error, selected)` for every terminal attempt before moving or dropping its error; the selected attempt is logged too; every unselected full error is diagnosed before discard. The low-cardinality metric is `write_terminal_failure_total{stage,status,retry_hint}`; it has no `RegionId`, `TableId`, or error text. Region id and formatted error appear only in the structured log.

For retryable failures, `finish` logs the full live error once after admission returns, with the outcome (`admitted`, `shed`, `cancelled`, or `not_retained_queue_disabled`). The selected retryable error is logged once with `selected = true`; it is not logged again as an unselected sibling. The record's `give_up_failure` stores only the context. A later replay dead letter is a different event (`write_queue_dead_letter_total{reason}`) with a different log. If the queue is disabled but retry is installed, the same terminal diagnostics run and retryable siblings are logged `not_retained_queue_disabled` before being dropped. If retry itself is not installed, there is no merged give-up and no such claim.

`merge_attempts` takes a `FailureStage` (`Initial` or `Retry`), records the zero-based `attempt_ordinal`, and expands `region_ids` into contexts. It never returns early on a terminal attempt. `retry` returns the same merged set to `finish`; only `finish` calls admission, so a record cannot be admitted twice. The public `dispatch` path converts `GiveUp` back to `Result<RegionResponse>`.

| Error kind | Client | Retained? | Logged? | Queue dead letter? |
| --- | --- | --- | --- | --- |
| Terminal, selected | first in observed order | context only | once, full error, `selected = true` | No — no queue record |
| Terminal, unselected | no | context only | once, full error, `selected = false`, before discard | No |
| Retryable, selected | fallback if no terminal | yes, if admitted | once with disposition | Not at give-up |
| Retryable, unselected | no | yes, if admitted | once with disposition, not duplicated | Not at give-up |
| Reroute failure | first terminal or fallback | context; affected writes not requeued | once at `finish` | No |

“Selected” is the error returned to the client; “unselected” is every other failure in the same merged set. The metric is emitted once per terminal context with `stage`, `status`, and `retry_hint`; no full error or identity label.

**Route-refresh failure preserves siblings.** `reroute_failed_writes` is fallible, and current `retry` uses `?`; a route invalidation, metadata lookup, rebuild, or re-prepare failure discards every other pending and refresh write. Core v1 changes this: a group-wide invalidation or re-prepare failure terminalizes the affected refresh group with one `FailureContext` per region at the `Retry` stage; a per-source rebuild failure terminalizes only that source; other source writes continue; writes outside the failed set continue retrying. A reroute failure is **not** an RPC attempt: it does not increment `send_attempts` and does not consume `max_attempts`; the original error is kept until `finish` logs it. Already-collected contexts are kept; `retry` does not return early on a terminal sibling. A retryable refresh-failure policy is a separate design; v1 fail-closes the affected set.

**Charge release.** Every stored-record release goes through one primitive, `remove_head_and_release(key, token, match_state)`. It checks the head id and token, applies the in-flight condition (`Unleased` for sweep, `Leased(token)` for the lease owner, `Any` only after all children are joined), pops the head, clears `in_flight` only for that exact token, subtracts `payload_len` and one record with checked arithmetic, and removes an empty key. Callers never subtract stored charges directly.

| Path | Owner | Identity / in-flight | Entry removed | Charge |
| --- | --- | --- | --- | --- |
| `complete`, `dead_letter` (incl. gate `TtlExpired`/`AttemptsExhausted`) | task | head id + `Leased(token)` | yes | released |
| `begin_send` returns `QueueClosed` | task | own head; no transition | no | remains; drain releases |
| `sweep_expired` | worker | unleased head only | yes | released |
| drain after join (normal shutdown, fatal teardown, worker panic) | wrapper teardown routine | `Any` | yes, loop | popped and freed; exact counters only if the ledger was consistent |
| panic, still leased | worker recovery | exact token; defer | no | remains |
| panic, expired/exhausted | worker recovery | exact token | yes | released |
| abort/cancel, open / shutdown | unified terminal order / drain | exact token / `Any` | yes if terminal, else no; yes on drain | released if terminal/drained, else remains |
| fatal invariant (`LeaseMismatch`, accounting underflow, epoch exhaustion, missing `TaskMeta`) | wrapper teardown routine | discoverer records reason and fires `fatal`; teardown then `Any` | all stored | popped and freed; exact counters only if the ledger was consistent |
| worker panic | wrapper teardown routine | close, shutdown/join, clear map, drain | all stored | popped and freed |
| reservation drop or commit sees closed | admission | reservation only | no | reservation released |
| commit succeeds | admission | reservation becomes stored | push | no subtraction |

`defer` never removes and never releases; it requires the exact token and sets `ready_at`. A removed record cannot be deferred. A stale lease token cannot clear a new record. The reservation guard is the only path that subtracts a reservation, and only when the record never committed. Drain never subtracts a live reservation because reservations are not in `keys`.

Sweep is the only removal path not driven by the lease owner: for each key, skip if `in_flight.is_some()`; if the head's `deadline <= now`, remove it with `Unleased`. Eligibility and decode use the same head identity; a head replaced between them is impossible because a leased head cannot be removed and the queue never removes a non-head entry. This is a state condition, not “idempotent”: a missing key, different head id, or non-matching token removes nothing and subtracts nothing.

Release proof:

- **exactly once** — a successful removal pops the head and subtracts in the same critical section; a second caller finds a different head or no key and subtracts nothing. There is no path that subtracts without popping.
- **no sweep of an in-flight record** — sweep checks `in_flight.is_some()` under the same lock it removes under. A lease and a sweep cannot both pass.
- **no implicit Drop release** — there is no `LeaseGuard`; every release is an explicit token-checked call. A stale token from an old lease cannot remove or release a successor record.
- **reservation vs stored** — a reservation is only in the counters, never in `keys`; the guard releases only the counters. A committed entry is only in `keys`; the release primitive decrements only after popping it. A failed commit subtracts the reservation before disarming, so the guard cannot subtract again.
- **checked arithmetic** — every add and subtract is checked; an impossible underflow is a fatal invariant violation: the discoverer sets `closed`, records `charge_underflow`, fires `fatal`, and performs no partial subtraction, so the ledger is not made worse; the wrapper tears down. Drain then pops and frees stored entries, but the ledger was already inconsistent, so teardown does not re-run the checked decrement as an invariant: it uses a saturating decrement, records the discrepancy, and does not re-enter fatal. “Released exactly once” then means each stored entry is popped and freed once, not that the counters are restored.

**Failure table.** “Queued” means admitted and charged; “client” is the response to the failing write.

| # | Condition | Client | Queue | Duplicate/ordering |
| --- | --- | --- | --- | --- |
| 1 | Write succeeds | Success | No record | Normal path |
| 2 | Retryable failure, budget exhausted | Error | All retryable failures retained with contexts | Replay can duplicate an applied RPC |
| 3 | Terminal sibling plus retryable siblings | First terminal error | Retryable retained; every terminal context logged; terminal not queued | No sibling drop |
| 4 | Several terminal siblings | First terminal error | All contexts logged; no retry | First-in-observed-order choice |
| 5 | Queue disabled | Error | No record | Existing behavior; diagnostics if retry installed |
| 6 | Admission shed | Error | No record | Equivalent to no queue |
| 7 | Client cancels before admission / at the checkpoint / after the checkpoint | Original error | Before or at the checkpoint: no record. After the checkpoint: commit may win; record independent | No compensation / replay proceeds if admitted |
| 8 | Leader moves | Original error | Retained; invalidate route cache; replay resolves new leader | Check-to-send race possible |
| 9 | Source region absent at replay | Original error | One forced route-cache refresh; if still zero, dead-letter `source_region_absent`; multiple/inconsistent fails closed | Post-admission layout change not compensated |
| 10 | Live retry rebuilt into several targets | Original error | Each failed target admitted independently | No unit atomicity; partial success |
| 11 | One target succeeds, another nonretryable | Original error | Successful target not queued; failed target dead-letters | No rollback |
| 12 | Route refresh/reroute fails | First terminal error | Failed group/source terminalized with contexts; other pending writes continue | No valid target; no silent sibling drop |
| 13 | Retryable replay | Original error | Record stays at head; backoff | Re-sending can duplicate |
| 14 | Nonretryable replay | Original error | Dead-letter `non_retryable` | Earlier duplicate may remain |
| 15 | Replay RPC times out | Original error | Unknown; defer, or dead-letter by TTL/attempts | Server may have applied |
| 16 | TTL expires with zero / nonzero send authorizations | Original error | `send_attempts == 0`: `ttl_expired_before_first_send`. `send_attempts > 0`: `ttl_expired_after_send_authorized`, unknown outcome, even if this pass never reached the gate | Only the zero case has no remote effect; an earlier pass may have applied. Never reported as “not sent” when attempts exist |
| 17 | Table missing or version changed | Original error | Dead-letter | No send |
| 18 | Append-mode/flow/impure-key at admission | Original error | Shed | No compensation |
| 19 | Per-record task panic | Original error | Worker recovery by `TaskMeta` token: stale -> no-op; else defer, or terminal if expired/exhausted. A join result with no `TaskMeta` is fatal teardown, never token recovery | Panic does not prove no RPC; no duplicate release/transition; terminal metric may be lost |
| 20 | Worker loop panic or fatal invariant (`LeaseMismatch`, charge underflow, epoch exhaustion, missing `TaskMeta`) | Original error | Queue closes; the single wrapper teardown runs: worker returns, children shut down and joined, map cleared, drain | No silent no-consumer; no identity guess without a token |
| 21 | Graceful shutdown / abrupt exit | Original error / none | Graceful: close, worker returns, `JoinSet::shutdown` (join), map clear, drain; records lost. Abrupt: no cleanup. | Same teardown routine as fatal; no recovery |
| 22 | Client retries while a record is pending | Client sees its own retry result | Both may apply | Dedup may not collapse; impure-key shed |
| 23 | `BulkInsert`/batcher flush | Error | With queue on, retained then shed; without queue, existing retry | Never queued |
| 24 | Ordinary `RoutedWrite` with no metadata | Error | Shed `plan_metadata_missing` | No queue semantics |
| 25 | Client runs `TRUNCATE` after the error | Original error | A replay may land after the truncate (up to TTL) | Ordering introduced by the queue; not detected |
| 26 | Table dropped, then undropped before replay | Original error | Plan-time version check may pass; no detection claim | DDL risk |

## The `affected_rows` contract

A successful write today means three things at once: routed to the region's peer, owned by the region's WAL, and observed by a following read. A queued write has none of them. Returning success would redefine `affected_rows` from rows applied to rows accepted, and the count is load-bearing: an insert returns applied rows, a delete returns the affected rows the region reported, and the bulk-batch path checks it against the batch row count, a check that would then pass for rows that may never land. Three properties are at stake — an exact count, immediate visibility, and applying the write after the client stopped waiting; any two can hold, all three cannot, because a delete's matched-row count is known only once it is applied.

v1 takes **compensation**, not acknowledgment. The client receives the original error; the queue makes the write land later. An error already does not mean “not applied,” so compensation widens an existing window rather than opening one. `affected_rows` keeps its applied-rows meaning for successful writes and is not returned on error. A client that retries after the error can duplicate rows; a replay is not reported to that client.

An acknowledged-write contract — a new outcome meaning accepted and pending — would need a client idempotency key with region-side deduplication, a queue that survives pod loss with fencing, and pending state in all four protocols. That is future work, not a v1 behavior.

## Configuration

Proposed config; names/defaults are proposals, and numeric values are `UNVERIFIED` until measured. Core v1 keeps these proposed defaults. The 5-minute TTL is a proposed starting value, not an SLA; it is the intended recovery-window parameter and is not derived from the memory or concurrency bounds. Calibration of the TTL and of the 512 B / 64x constants belongs to code-PR measurement, not this design review.

```toml
[write_queue]
enable = false              # off: a retrying client can duplicate a replayed write
max_bytes = "512MiB"        # strict charged-payload ceiling, not a disk quota
max_records = 100000        # all reserved/charged records, including leased
max_record_size = "512KiB"  # rejected before allocation; <= max_bytes and <= u32::MAX
ttl = "5m"                  # monotonic age from admission
max_attempts = 10           # final send-gate passes per record
replay_concurrency = 8      # records in flight; one RPC each

# Reused, not a new key:
# [datanode.client]
# timeout = "10s"           # replay-only client deadline; must be > 0 when queue enabled
```

Internal constants: `REPLAY_TRANSIENT_FACTOR = 64` (planning candidate), replay tick 200 ms, replay backoff initial 1 s / cap 30 s / equal jitter, `PER_RECORD_OVERHEAD = 512 B` (planning). Validation rejects zero bounds, `max_record_size > max_bytes`, `max_record_size > u32::MAX`, zero `ttl`, zero `max_attempts`, zero `replay_concurrency`, zero `datanode.client.timeout` when the queue is enabled, failed `ByteSize`-to-`usize` conversion, and arithmetic overflow. It does not clamp. The `u32::MAX` cap is retained from the original PR proposal; no source at head proves tonic requires it, and the need is `UNVERIFIED`, so it is a defensive configuration bound rather than a proven transport limit.

Operational limit: `replay_concurrency` is a **concurrency** bound, not a **rate** bound. It caps how many records are in flight at once, not how many requests per second a datanode receives. After a datanode recovers, up to `replay_concurrency` records finish and the next batch is leased immediately, so a large backlog can drain at a much higher request rate than steady-state traffic. That burst can hit the datanode's write path with old timestamps and the original write shape, adding flush and compaction work; a write-buffer rejection surfaces as another retryable failure, which the queue treats as unknown-outcome retry, not as a signal to slow down. `max_attempts`/TTL bound how long one record retries, not the aggregate load. Core v1 has no per-datanode adaptive pacing, token bucket, or supervisor; those are future work.

## Memory and performance

The success path has zero queue calls. On failure, per failed region: table/flow/version lookup (cache hit once per table per failed batch; a miss falls back to the metadata store); `encoded_len` (prost field walk, no allocation); ledger reserve (one short mutex section, integer arithmetic); payload allocation and encode (one allocation of exact reserved length, one encode); commit (one mutex section, one `VecDeque` push, no payload copy). No channel, task spawn, RPC, or disk I/O on the request path. A cache miss happens after the write already failed, so its latency is part of the error response.

The code PR must measure, queue off vs on under the same failure injection: handoff p50/p99 at 1, 8, 64 threads; added CPU and lock contention per failed region; end-to-end failure-path p99 against the queue-disabled baseline; success-path allocation count and CPU, including queue-on plan-metadata tracking. Targets: no payload-sized allocation on success, and failure-path p99 within 5% of the queue-disabled baseline. These are acceptance targets, not results.

The memory bound has three terms. It models the queue's own memory, not a hard cap on frontend RSS.

1. **Charged payload and reservations: exact for stored bytes.** `charged_bytes` sums `RegionRequest::encoded_len()` for every reserved, stored, or leased record not yet released. `encoded_len()` is a length computation, not an encode. The reservation is taken before allocation; encode uses `Vec::with_capacity(reserved_len)` and commit verifies the written length equals the reservation. A mismatch is not admitted and does not stay charged. There is no second **stored** payload copy: `Bytes::from(Vec)` moves the buffer. There is, however, an admission-time peak: the caller's original `Arc<RegionRequest>` is still alive while the encoded buffer is built, so one admission can hold the original request plus the reserved buffer. That original is not part of `max_bytes` and is caller-owned; with concurrent admissions the transient is bounded by admission concurrency, not by the three-term model. The original is dropped after admission returns.
2. **Per-record bookkeeping: `UNVERIFIED`.** Record structs, map/deque nodes, `Arc`/`Bytes` headers, allocator metadata. Planning uses **512 bytes per record**, about 49 MiB at 100,000 records. The code PR must pin or replace it.
3. **Replay transient: `UNVERIFIED` candidate.** Replay holds the encoded `Bytes`, decodes one `RegionRequest`, and gives it to tonic, which builds a transport buffer; at peak the encoded payload, decoded request, and transport buffer coexist for one permit. The planning candidate is **64x** `max_record_size` over that whole per-permit peak; it is not a proven maximum.

The 64x candidate is not enough as evidence. An empty `Value` is 2 bytes on the wire and at least 24 bytes in memory; a small int is 4 bytes; each `Row` adds a 24-byte `Vec` header; null- or small-int-heavy records can expand well past 3x before the transport buffer. The earlier single-null derivation (about 52x before transport) is one shape, not proof of the worst. The code PR must generate null-heavy, small-int, short/long string, binary, list, struct, JSON, empty-row, and mixed shapes at `max_record_size`; recursively sum decoded container capacities plus the transport buffer; derive the factor from the observed maximum with headroom; and pin it with a test that fails if a future shape exceeds it. If the maximum exceeds 64, lower `max_record_size` or raise the documented bound; do not silently keep 64.

```text
charged payload bytes    <= max_bytes
record bookkeeping       <= max_records * overhead
replay decode and send   <= replay_concurrency * factor * max_record_size
```

With proposed defaults and the 64 candidate: 512 MiB + about 49 MiB + 256 MiB = about 817 MiB. This is a **planning model**, not RSS. The 768 MiB figure is only the first and third terms. Allocator fragmentation, runtime stacks, connection machinery, metadata caches, and other process memory are outside the model. Operators size the container above the modeled total plus other process memory, not at `max_bytes`. A record in an RPC when TTL expires stays charged until the RPC or its client-side deadline converges; the replay deadline prevents an unbounded wait from holding a permit and charge.

Worked arithmetic, assuming the 64 candidate and 100,000 records:

| Term | At defaults | Worst at `max_records` |
| --- | --- | --- |
| Charged payload | at most 512 MiB | at most 512 MiB, independent of record count |
| Bookkeeping (512 B each) | 48.8 MiB | 48.8 MiB |
| Replay transient (8 x 64 x 512 KiB) | 256 MiB | 256 MiB |
| Modeled total | about 817 MiB | about 817 MiB |

The payload term binds by bytes, the bookkeeping term by records, and the transient term by concurrency; whichever is reached first stops admission. A record count at `max_records` with small payloads can therefore use far less than 512 MiB, while large payloads hit `max_bytes` first.

## Lifecycle and wiring

**Worker panic and shutdown.** The wrapper owns the `JoinSet`, the task map, and the state; the worker borrows the `JoinSet` and the map mutably. The wrapper runs `AssertUnwindSafe(worker.run(&mut tasks, &mut map)).catch_unwind().await`. The worker never tears down on its own: it stops scheduling and returns, whether woken by `cancel` (normal shutdown) or by `fatal` (invariant violation). The wrapper then runs **one** teardown routine for all three cases — normal shutdown, fatal invariant, worker panic/error: (1) set `closed`; (2) ensure the worker future has ended (await its handle; never abort it while it borrows `tasks`); (3) `tasks.shutdown().await` to join every child; (4) `map.clear()`; (5) drain every remaining stored record through `remove_head_and_release(Any)`; (6) log the reason. There is exactly one teardown path, so normal shutdown and fatal handling cannot disagree about ordering, and all of them release every stored record exactly once. The wrapper does not restart. Fail-closed plus drain is the Core v1 author decision; automatic restart and supervision are not part of this RFC.

`JoinSet::shutdown` in locked tokio 1.52.3 is `abort_all` followed by `while join_next().await.is_some() {}` (`tokio-1.52.3/src/task/join_set.rs:381-383`; the manifest says `tokio = "1.47"` at `Cargo.toml:268`, the lock resolves 1.52.3). This is what actually joins children. Dropping or aborting the parent `JoinHandle` does not join them.

Shutdown sequence (order is the contract): (1) `state.lock().closed = true`; (2) `cancel.cancel()`; (3) await the worker handle, do **not** abort it; (4) wrapper teardown: `tasks.shutdown().await`; (5) wrapper teardown: `map.clear()`; (6) wrapper teardown: for each remaining `(key, head)`, `remove_head_and_release(Any)`. Step 3 deliberately does not abort, because the worker holds the mutable borrows; it must return before the wrapper can touch `tasks`. Step 4 both aborts and joins every child; only after it can `Any` be used, and only after step 5 is the map known to contain no entry for a task that could still act. On a fatal signal, step 1 was already performed by the discoverer; the worker's `select!` returns; the wrapper still runs steps 4–6. On a worker panic, `catch_unwind` returns after the borrows end; the wrapper sets `closed` and runs steps 4–6. An abort is delivered only at an await point and a record transition is a synchronous critical section, so teardown cannot interleave with half a removal. `JoinSet::shutdown()` exposes no per-task ids, so Core v1 does not attempt per-task recovery after it; drain owns every remaining record. A per-record panic during normal operation is recovered per Architecture and ownership; a shutdown abort happens after `closed` is set, so it is not deferred and drain releases it.

The drain uses stored records only; it never decodes, sends, or consults a route, and cannot fail. The queue does not flush at shutdown; that needs a deadline and a stuck-datanode policy. There is no frontend-wide shutdown timeout at PR head (`src/frontend/src/frontend.rs:167-175`); v1 promises no bounded shutdown time. A child that cannot reach an await point delays shutdown, as a stuck server task does today.

`Frontend::shutdown` currently returns early if `servers.shutdown_all()` fails; Core v1 captures that result, **always** runs queue shutdown, and returns the server error first; if server shutdown succeeded and queue shutdown failed, the queue error is returned. `FrontendBuilder::try_build` creates the queue only after all fallible `Instance` steps, or keeps a build guard that shuts it down on a later error; a partially constructed queue must not be dropped without shutdown. The handle is cloneable; `Instance` gains `write_queue: Option<WriteQueueHandle>` at `src/frontend/src/instance/builder.rs:420-441`. The queue is built when `write_queue.enable` is true, using the already-loaded `self.options.datanode.client.timeout` as the replay deadline source. The dispatcher installs `WriteRetry` when `max_retries > 0` **or** the queue is enabled; a queue-enabled, `max_retries = 0` configuration still runs the merged give-up path and admission.

Boundary at shutdown return: shutdown prevents new commits and drains stored entries. An outstanding `AdmissionCharge` may still be released later by its guard; shutdown does not await caller admission futures. This is not a “zero counters at the instant the handle returns” guarantee.

**Cancellation.** `execute` receives an optional `CancellationToken`. `retry` stops issuing new rounds when cancelled. `finish` checks the token immediately before each admission, and admission checks it again after all awaited eligibility work and before reserve. If cancelled at either checkpoint, that sibling is not admitted and further siblings are not attempted; siblings committed before the check remain. The second checkpoint is the admission cancellation linearization point.

v1 admission is **per-record best effort**, not atomic across siblings: cancellation observed before the admission call or at the second checkpoint leaves no record for that sibling; after the checkpoint the reserve/encode/commit phase is synchronous, so a cancellation can still race the commit and the record may be admitted if commit wins; the state lock has no `await`; partial sibling commit is possible; there is no multi-region atomicity; once committed, a record is independent of the client future; `closed` is the final commit defense. `Inserter::do_request` cancels when the caller is dropped, while `execute` continues in a detached `spawn_global` task (`insert.rs:861-865`); the checkpoints are therefore the only admission-time observations of caller cancellation. A client whose timeout is shorter than the backoff-only window (10.5 to less than 21 seconds, plus RPC time) can stop before give-up and queue nothing. This is intended.

## Observability

Metrics, without table or region labels: `write_queue_admitted_total`; `write_queue_shed_total{reason}` (single shed counter); `write_queue_charged_bytes`; `write_queue_charged_records`; `write_queue_replay_total{result}` (completed, deferred, dead_letter, skipped); `write_queue_dead_letter_total{reason}`; `write_queue_task_join_error_total{kind}` (panic, cancelled); `write_queue_route_refresh_total{result}`; `write_queue_replay_latency_seconds`; `write_queue_inflight`; `write_queue_closed`; `write_terminal_failure_total{stage,status,retry_hint}` (dispatcher-level, emitted from merged give-up even when queue disabled); `write_retryable_give_up_total{disposition}` (admitted, shed, cancelled, not_retained_queue_disabled).

Shed reasons: `encode_panic`, `encode_size_mismatch`, `plan_metadata_missing`, `malformed_record`, `bulk_insert_out_of_scope`, `budget_exhausted`, `record_too_large`, `record_id_exhausted`, `deadline_overflow`, `table_info_unavailable`, `plan_version_changed`, `append_mode_table`, `flow_source_table`, `flow_source_lookup_failed`, `impure_default_on_dedup_key`, `queue_closed`.

Dead-letter logs contain record id, table id, source region id, rows, `send_attempts`, `current_pass_authorized`, `ever_send_authorized`, `remote_outcome_unknown`, `consecutive_failures`, age, reason, the give-up context (region id, ordinal, stage, status, retry hint), and the replay terminal error (or `none`). The three authorization fields make the TTL reason auditable: a `ttl_expired_before_first_send` has `send_attempts = 0` and `remote_outcome_unknown = false`; a `ttl_expired_after_send_authorized` has `ever_send_authorized = true`, and `current_pass_authorized` distinguishes “expired while preparing this pass” from “expired after this pass sent.” This is diagnostics only: no query API, no persistence. Metrics plus structured logs are the Core v1 author decision; a queryable dead-letter store and manual replay are future work.

# Alternatives

Alternatives that correspond to separate design responsibilities are listed so the boundary is explicit; rejecting them here does not adopt them as Core v1 commitments.

| Alternative | Why not |
| --- | --- |
| A new `common-retry-queue` crate with a `QueueStore` trait (`append`, `lease`, `complete`) | The original PR #10045 shape. It creates a workspace member and a durable-shaped trait while the only implementation is in memory. v1's queue calls only append-like commit, lease, and complete; extra store operations have no caller and shape the core around a phase that is not being built. An operator-local `Mutex<QueueState>` keeps the seam replaceable without claiming durability. |
| A bounded channel plus writer task | Decouples admission latency from the lock and makes backpressure explicit, but adds a handed-off state, capacity, closed-channel failure, and a panic path that must release or drain. The store is a `HashMap` plus `VecDeque`; direct commit is a short mutex section. |
| A supervisor with automatic worker restart | Preserves records across a worker panic, but adds restart backoff, generation handshakes, an unhealthy threshold, and a sweep policy. Core v1 chooses fail-closed plus drain. |
| Queue-side rebuild across split/merge with target gates and a Gate Registry | Preserves old-before-new across a split but needs target sets, progress rules, gate tokens, and a fan-out cap. Core v1 admits per-target writes produced by live retry; the queue never sees a unit. |
| Durable queue in Core v1 | A `LogStore`-backed store with startup recovery, `sync_write`, disk bounds, and a low-water mark is a second crash-safe log to own and test, and it obscures the part v1 needs to get right: exact admission, ordering, and replay. The encoded payload is kept; the durable phase adds framing and store operations. |
| A log-shaped `QueueStore` from day one | The durable-shaped interface gives an adapter a head start, but v1 calls only append, lease, and complete. The extra methods have no v1 caller and shape the core around a phase that is not being built. |
| Store decoded requests instead of encoded payloads | Removes the replay decode, but makes the exact byte bound approximate (Rust container capacities) and couples the queue to the write protocol. The encoded payload is what a durable store needs anyway. |
| Region-side DDL fence in Core v1 | Closes the `TRUNCATE`/`UNDROP` window, but adds a proto field, a manifest field, and a region path before the queue's core is validated. Core v1 bounds the window with checks and a short TTL; the fence is a later phase's prerequisite. |
| Extending the in-process retry loop | It runs inside the client request, a client timeout drops it, it re-enters table and route refresh every round, and it returns one error. The queue exists to decouple remaining work from the client. |

# Drawbacks

- **Delivery is best effort, not reliable.** An admitted record can be lost with the frontend process, dead-lettered on TTL or attempts, or never sent because the worker has failed and the queue is closed. The queue is compensation, not a delivery guarantee.
- **Replay can duplicate.** A replay may resend a request whose original RPC applied but whose response was lost; a timed-out replay is unknown; a client retry can race a pending record. There is no idempotency key, no deduplication, and no exactly-once claim. Append-mode tables are shed precisely because deduplication cannot hide a duplicate there.
- **The client is told the write failed while it may still land.** A retrying client can duplicate rows, and a stale replay can override a newer live write; the window extends to the TTL. The error response is deliberate: returning success would redefine `affected_rows`.
- **Per-key head-of-line blocking.** A failed head blocks its key until it completes, dead-letters, or TTL expires; younger records for that source region wait. Other keys continue, but there is no global fairness or per-tenant reservation, so one region's backlog can consume the shared budget and reject other regions' admissions.
- **Split/merge can dead-letter a live region.** After admission, a source region that disappears is dead-lettered after one forced route refresh; if metadata propagation lags behind the frontend cache, the second look can still be zero and the record can be lost even though the region exists. The queue does not rebuild or compensate a layout change.
- **The DDL check is a timing window.** ALTER can land between the replay check and the RPC, and a stale frontend cache can pass the version check. The check does not make replay DDL-safe.
- **`TRUNCATE` and `UNDROP` are not detected.** A replay can put old rows back after a truncate, or across a drop/undrop gap, producing zombie rows until the TTL. This is the main correctness debt, bounded by the off-by-default switch, the documented window, and the future region-side fence.
- **Flow, batcher, and bulk paths are excluded.** Flow source tables are fail-closed because replay would add an unmirrored source write; `BulkInsert` and both pending-rows batchers are shed at admission; instant-TTL inserts and paths without plan metadata are not covered. These are boundaries, not a silently weaker mode.
- **Memory planning is uncertain.** The 512 B bookkeeping and 64x replay transient are planning candidates, not measurements; the 817 MiB total is a model, not RSS. A container sized only for `max_bytes` can be OOM-killed, and allocator fragmentation, stacks, connections, and caches sit outside the model.
- **Replay can arrive as a burst.** `replay_concurrency` bounds in-flight records, not request rate. After a datanode recovers, a backlog can drain at a much higher rate than steady-state traffic, with old timestamps and the original write shape, adding flush and compaction work. v1 has no adaptive pacing or token bucket.
- **Process exit loses everything.** There is no durable log, recovery, or acknowledgment. A frontend restart loses every stored record; an abrupt exit also skips the drain.
- **The failure path becomes a load path.** Under a cluster-wide outage, every failed write reaches admission, adding CPU, lock contention, and metadata reads to requests that are already failing. The success path is untouched, but the failure response carries the admission cost.

# Future work

The following are potential future directions, not Core v1 commitments and not a promised v2/v3 roadmap. Each is a separate design responsibility rather than an omitted detail of this one.

- **Durable queue and recovery.** A durable queue, startup recovery, acknowledged writes with region-side idempotency, and cross-process recovery.
- **Topology-aware replay.** Queue-side rebuild across split/merge with target sets, old-before-new ordering, and unit semantics.
- **Strong replay consistency.** A region-side DDL epoch/fence, newer-write detection, and server-side bounds on RPC execution.
- **Operational extensions.** Automatic replay-worker restart and an explicit unhealthy state; queryable dead-letter retention and manual replay; per-region or per-tenant fairness reservations; adaptive replay rate limiting and write-buffer-aware pacing.
- **Measurement.** Measured replacements for the 512 B per-record overhead, the 64x replay transient, and the 5-minute TTL.

# Open questions and validation items

## Open design questions

The following are author decisions for Core v1, not open questions. Their rationale is in the referenced sections:

- Worker panic is fail-closed plus drain; there is no automatic restart or supervisor (Lifecycle and wiring).
- An impure default on a dedup key is shed (Metadata safety).
- One global byte/record budget uses first-come admission; Core v1 adds no fairness mechanism (Admission and memory accounting; Drawbacks).
- Dead letters are visible through metrics and structured logs only; there is no query API or persistence (Observability).
- Replay reuses `datanode.client.timeout` as a replay-only client deadline; normal writes remain unchanged (Replay).
- `BulkInsert` and both pending-rows batchers are excluded (Non-goals; Metadata safety).
- Numeric defaults, including the 5-minute TTL, are proposed values marked `UNVERIFIED`; calibration belongs to the code PR, not this RFC (Configuration).

The remaining maintainer decisions are:

- **Source-region disappearance after one forced refresh.** Core v1 keeps the route lookup, one forced refresh, and dead-letter on the second zero. The only open choice is whether that second zero should instead trigger bounded re-confirmation within TTL. Immediate terminalization is cheaper but can dead-letter a region that still exists when metadata propagation lags; bounded re-confirmation spends head-of-line latency for that key and delays duplicate visibility, and must remain TTL-bounded. This is a maintainer decision because it changes the data-loss/head-of-line trade-off. Queue-side rebuild, target remapping, target-set gates, and old-before-new ordering are outside this question and outside Core v1.
- **DDL risk acceptance.** Core v1 keeps the plan-time version check, but it is not an atomic fence. ALTER can land after the replay check and before the RPC; a stale cache can pass the check; `TRUNCATE` does not bump the version; `DROP` followed by `UNDROP` may pass; delayed replay can resurrect post-TRUNCATE data. This question asks whether maintainers accept these documented Core v1 risks, or require a region-side DDL epoch/fence before queue work. The fence is a separate design and is not specified here.
- **Contract acceptance — bounded best effort.** Core v1 promises no delivery, no at-least-once application, and no exactly-once; a record may be admitted and never sent, sent and never applied, or applied more than once. This differs from the original PR #10045 wording and is user-visible, so it cannot be author-closed; maintainers must accept it or reject the scope. This question does not ask for a stronger design in this RFC.

Author-closed implementation choices are not escalated: the crate-private `GiveUp` shape, `src/operator/src/write_queue/` placement, the `Record`/`QueueEntry` split, `RegionId` keys, reserve/encode/commit, the shared route-cache helper, `parking_lot`, installing retry when the queue is enabled even with `max_retries = 0`, empty-key removal, the task-map key type, and the exact log fields. The rationale and evidence document predates this scope alignment; its §4 still records the former MQ items as open and needs a status update before publication.

## Validation to record in the code PR

No test or benchmark was executed for this draft. Every executable item in the Evidence `Validation Status` table is `Not started`; source reading is not a test result. The status vocabulary is exactly `Not started`, `In progress`, `Passed`, `Failed`, `Not applicable`. `Passed` requires a linked command and output; `Failed` requires the failure output and the affected guarantee; source reading closes nothing. A benchmark row is not closed by a source argument, and a maintainer-decision row is not closed by a test. The RFC intentionally does not duplicate the 125-row Evidence table.

What the code PR must distinguish:

- **Source-confirmed facts.** The current retry defaults and jitter, the `MergeAttempts` early return, the absence of a frontend-to-datanode request timeout, `RegionId` encoding, `find_physical_table_route_with_id`, the route-invalidation surface, the plan-time version bump on ALTER, the cache's version-checked initialization, the `TRUNCATE` procedure not bumping the version, and the `UNDROP` metadata restore. These are citations, not tests; a future code change can invalidate any of them.
- **Design decisions.** The reserve/encode/commit sequence, the second cancellation checkpoint, Model A's single-transition authority, `LeaseToken`, the `remove_head_and_release` primitive, the one wrapper teardown for normal shutdown/fatal/panic, source-region-absent dead-letter, and the bounded-best-effort guarantee. These are choices, not measurements. They need review, not a green test.
- **Inferences.** The FlowMirrorTask replay gap is inferred from construction/detachment/per-peer send and the best-effort pending-rows drop; it is not a proven new source/sink inconsistency. The replay-burst pressure is inferred from the concurrency bound and the absence of pacing. The metadata-propagation false-absence window is inferred from cache invalidation and eventual consistency.
- **Unverified assumptions.** The 512 B per-record bookkeeping; the 64x replay transient; 817 MiB as a planning model, not RSS; 768 MiB as payload plus transient only; whether a 5-minute TTL covers the intended restart-to-serving window; the proposed numeric defaults; tokio 1.52.3 `JoinSet`/`AbortHandle`/`JoinError` behavior under the locked version and workspace panic strategy; `catch_unwind` under that strategy; the single fatal-teardown routine and its exactly-once drain; the `TaskMeta`-missing fail-closed path; the second admission cancellation checkpoint and its race with synchronous commit; whether the requester call is submitted on its first poll (a call first polled after `rpc_deadline` may still reach the datanode, so the client deadline bounds waiting, not remote execution); whether one forced route refresh is enough when metadata propagation lags behind the frontend cache; the burst write pressure `replay_concurrency` can put on a recovering datanode; `datanode.client.timeout` as a replay-only deadline; and the exact UNDROP/TRUNCATE version behavior.
- **Tests not started.** Existing dispatcher tests pin current behavior only; they do not exercise the queue. No admission, replay, lease, release, cancellation, shutdown, or route test has run.
- **Benchmarks not started.** The failure-path 5% p99 target, success-path allocation count, and lock contention at 1, 8, and 64 threads are acceptance targets, not results. The code PR must record measurements and link them.
- **Maintainer decisions pending.** Only the narrowed source-region-disappearance question, the narrowed DDL-risk question, and acceptance of the bounded-best-effort contract remain. Every other former MQ item is an author decision recorded in the body and is not a pending design choice. A maintainer answer is not a test result and does not close a validation row by itself.
