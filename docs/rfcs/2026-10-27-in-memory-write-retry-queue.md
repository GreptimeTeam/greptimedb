---
Feature Name: Frontend Write Retry Queue (in-memory v1)
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
---

# Summary

This RFC proposes an optional in-memory write retry queue for the frontend. When a per-region write still fails after the dispatcher's retry loop, Core v1 admits eligible retryable writes to an in-process queue and replays them to the owning datanode from a background worker. Terminal failures are not queued. The client response is unchanged: it receives the original error, and `affected_rows` keeps its applied-rows meaning.

Replay is best effort. A retryable replay outcome is never treated as success; the record remains at the head of its key while the queue is open and its limits allow another attempt. A successful RPC response completes the record. Authorizing a replay send does not imply that the requester future is polled, that the RPC reaches the datanode, or that the write is applied. Core v1 claims no at-least-once delivery, no at-least-once application, and no exactly-once application, and never reports a replay success to the client.

# Motivation

The dispatcher already retries a per-region write with `max_retries = 3`, exponential backoff with additive jitter, and `initial_backoff = 1.5s`, `max_backoff = 10s`, `backoff_multiplier = 2.0`. The resulting delays are `[1.5,3)`, `[3,6)`, and `[6,12)` seconds: a 10.5 to under 21 second window plus up to four RPCs. The frontend-to-datanode channel has no request timeout, so each RPC can take arbitrarily long.

That loop handles a brief outage. A datanode restart, region migration, or partition can outlast it, and today only the client can retry. The client has no per-region view: it either replays the whole statement, duplicating regions that succeeded, or drops the failed regions. Extending the loop does not solve this, because the loop runs inside the client request. A client timeout drops the caller future, `Inserter::do_request` cancels the request, and the detached `execute` stops at its next cancellation check. Cancellation before admission prevents the queue from retaining anything; only a record that commits at admission is independent of the client future. A background queue at the dispatcher give-up point extends retention for those committed writes. Issue #8085 asks for a bounded frontend buffer and for restart survival; Core v1 covers admission, ordering, replay, and record-byte accounting, while restart survival is outside it.

# Goals

- Retain a bounded number of failed per-region writes after give-up, with byte, record, and per-record size ceilings.
- Preserve retryable siblings when another sibling is terminal.
- Replay per source region in FIFO order with at most one in-flight record per key; different keys are independent.
- Account for charged payload exactly: reserve before allocation, encode into the reserved size, commit once.
- Keep the client response and `affected_rows` unchanged; never report a queued write as success.
- Make every lifecycle outcome explicit: shed, dead-letter, defer, drain, or lost with the process.

# Non-Goals

- **Durable queue and recovery.** No durable retention, restart recovery, or cross-process recovery.
- **Topology-aware replay.** No queue-side rebuild after admission, split/merge target remapping, target-set gate, multi-region unit replay, or old-before-new ordering.
- **Strong replay consistency.** No region-side DDL epoch, atomic send fence, region-side idempotency, client idempotency key, newer-write detection, or stale-replay protection.
- **Exactly-once or acknowledged writes.** A record may be submitted more than once, and a successful replay is not reported to the client.
- **Excluded write paths.** `BulkInsert`, including both pending-rows batchers, and instant-TTL inserts are excluded. Flow source tables are excluded. Requests without per-region plan metadata, or that are not exactly one insert or delete region, are shed. Excluded writes are never stored or replayed.
- **Unsupported operations.** No fairness reservations, adaptive replay pacing, queryable dead-letter store or manual replay API, automatic worker restart, or standalone mode. Standalone is rejected by option validation.
- **No new external contract.** No client-visible error type, wire-format field, or persisted state.

# Details

## Architecture

Core v1 is an operator-layer module wired by the frontend. It has three parts:

- **Queue state.** One `Mutex<QueueState>` holding charged counters and per-source-region deques. Admission commits directly under the state lock.
- **Admission.** Once per retained write at give-up: check eligibility, reserve ledger room, encode, commit.
- **Replay worker.** One task on the global runtime. It leases ready heads, spawns bounded per-record tasks, and joins their results.

```text
client write
  -> Inserter/Deleter -> plan + PlanMetadata
  -> dispatcher retry -> give-up
       -> terminal failures: logged, not queued
       -> retryable per-region writes -> admission
            reserve -> encode -> commit
                  |
                  v
       QueueState: per-source-region FIFO
                  |
                  v
       replay worker -> per-record task
            prepare -> final send gate -> one RPC
            -> complete / defer / dead-letter
```

A record moves through four states. The charge owner changes only at commit and removal.

| State | Meaning | Charge owner |
| --- | --- | --- |
| Reserved | Ledger check passed; payload may still be encoding. | Admission guard |
| Stored | The record is at a key. | `QueueState` |
| Leased | A replay task holds the head under a lease token. | `QueueState`; the lease authorizes a transition, not charge release |
| Removed | Completed, dead-lettered, or drained. | Released |

All queue-state mutations run under the `QueueState` lock. A replay task leases the head under a token naming the record id, and the token authorizes that task's later transition. A lease mismatch, counter underflow, lease-epoch overflow, or missing task metadata is fatal: the queue closes and drains.

## Admission

Admission runs once per retained write at the give-up point, in observed order. A successful write never enters admission; with the queue enabled it may carry plan metadata, whose success-path cost the implementation measures. The dispatcher installs the retry path when `max_retries > 0` or the queue is enabled, so a queue-enabled configuration with zero retries still reaches give-up and admission.

Eligibility checks may `await` table and flow cache reads. After the last awaited check and before the reserve, admission takes a cancellation checkpoint; reserve, encode, and commit contain no `await` and no user code under the state lock.

| Rejection | Causes |
| --- | --- |
| Ineligible write | `BulkInsert`; not exactly one insert or delete region; missing plan metadata; region or table id mismatch; append-mode table; Flow source or Flow lookup failure; impure default on a dedup key |
| Stale plan | Missing table info; plan-time version changed |
| Size or budget | Record too large; encode panic or size mismatch; deadline overflow; record-id exhaustion; charged-byte or charged-record budget exhausted; closed queue |

Rejection leaves the client error unchanged and is equivalent to not having the queue. Cancellation before admission is not a shed record. The queue never waits for room and never evicts an older record. Core v1 uses one global byte and record budget with first-come admission and no per-region or per-tenant fairness.

**Reserve, encode, commit.** Admission reserves under the state lock by checking `closed`, charged bytes, charged records, and record-id allocation. It encodes outside the lock under `catch_unwind`, into the reserved capacity, then commits under the lock after re-checking `closed`. A panic, a length mismatch, or a closed queue releases the reservation. Cancellation after the checkpoint cannot stop the synchronous commit, so a committed record may exist even if the caller is gone. The guard either releases the reservation or converts it to a stored entry under the same lock as the push.

**One removal.** Every removal validates ownership before mutating: the lease's record id and token must match the head, or a queue-owned removal such as expiry or drain must find no in-flight lease. It computes the next charged-byte and charged-record counters with checked arithmetic. If either count would underflow, it mutates nothing; the queue closes, records `charge_underflow`, and drains. Otherwise it pops the record, clears the matching in-flight flag, assigns the prevalidated counters, and releases the charge. No `await`, allocation, or fallible user code sits between validation and mutation. Drain validates the totals of the set it removes before clearing state, so a record removed by one path cannot be released again by the drain.

## Memory Accounting

`max_bytes` bounds charged encoded payload, not Frontend RSS. The model has three terms. Charged payload is booked at encoded length: admission checks the per-record ceiling and reserves the length before the single allocation, `Bytes` owns that buffer, and removal releases the charge once. Bookkeeping is outside `max_bytes`; `max_records` bounds count, and planning allows 512 B per record, about 49 MiB at 100,000 records. Replay transient charge has an enforced cap, `replay_concurrency × factor × max_record_size`; a task reserves `record_len × factor` before decode, and an RAII guard releases it on success, error, panic unwind, cancellation, or shutdown. At eight slots, factor 64, and a 512 KiB maximum, the cap is about 256 MiB.

| Term | Value | Meaning |
| --- | ---: | --- |
| Charged payload | 512 MiB | Queue-owned encoded payload budget |
| Bookkeeping allowance | ~49 MiB | 512 B × 100,000 records |
| Estimated replay transient | ~256 MiB | 8 × 64 × 512 KiB |
| Planning model | ~817 MiB | Above three terms |
| Measured decoded expansion | ~44× | Largest observed decoded sample |
| Combined additive estimate | ~46× | Decode plus separately measured transport buffer |
| Planning factor | 64× | Provisional factor |
| Headroom | ~39% | Relative to the additive estimate |

Factor 64 is provisional. The 2026-11-09 standalone probe measured decoded live allocation after `RegionRequest::decode` for ten shapes; for those shapes live equals peak during decode because nothing counted was freed before the counters were read, and the encoded payload was outside the measurement. The probe measured the tonic encode buffer separately and dropped it before decode. The 44× figure is decoded live allocation; adding that separately measured buffer gives the 46× additive estimate for the single-null shape, not a simultaneous peak. The largest decoded value is 44× (38× at an exact power of two); the other shapes are 17.6–29.3×. Factor 64 leaves about 39% headroom over the 46× estimate and is not a bound for all payload shapes.

With compression off, tonic starts each encoded request in an 8 KiB `BytesMut`; a record under 128 B can allocate more than 64× its length. Factor 64 models one transport buffer, but it does not cover that starting floor: at most one such buffer exists per active replay task, so the uncharged part attributable to the floor is at most 64 KiB at eight slots. Compression is off by default; the frontend enables it only for `transport` or `all`, and `RegionRequester` then applies Zstd to inserts. With compression on, tonic also holds an uncompressed `uncompression_buf` alongside the compressed encode buffer; that extra allocation is outside factor 64 and is `UNVERIFIED`. The planning model excludes the 8 KiB floor, compression buffers, the caller's original request, allocator fragmentation, runtime stacks, connections, and metadata caches. Operators size the container above the modeled total; the cap bounds estimated charges, not allocated bytes, and no hard RSS bound is claimed.

## Replay

The replay worker periodically drains finished tasks, sweeps expired non-leased heads, leases ready heads while permits are available, and spawns one task per leased record. A permit is acquired before the lease and held until the send outcome. A record waiting for a permit is not leased. A key has at most one in-flight token, and its record id names the queue head.

A task prepares under the record deadline, re-checks table metadata, resolves the physical route, decodes, acquires the datanode client, passes the final gate, sends exactly one RPC, and applies one outcome:

| Gate result | Task action |
| --- | --- |
| `Allowed` | `send_attempts += 1`; this pass is authorized to send |
| `TtlExpired` | Dead-letter with the lifetime TTL reason; no send |
| `AttemptsExhausted` | Dead-letter `send_attempts_exhausted`; no send |
| `QueueClosed` | Return without a transition; drain owns removal |
| `LeaseMismatch` | Fatal invariant; close, record `lease_mismatch`, fire fatal; no record transition |

`send_attempts` counts `Allowed` results from the final gate for queue replay. It is not confirmed RPC invocations and not dispatcher attempts. It is incremented under the lock, never rolled back on panic, abort, timeout, or error; actual RPC counts require transport instrumentation. Metadata or route lookup failures and panics before the gate do not consume an attempt; any outcome after the gate, including panic, timeout, or a retryable or non-retryable response, consumes one.

Admission commit stores the record and does not send. `begin_send` is the replay send-authorization point: `Allowed` increments `send_attempts` and authorizes one requester call. It does not invoke the requester, submit transport, or apply the write; the requester future may be polled and the RPC may complete after the deadline.

After `Allowed`, exactly one requester call is guarded by an absolute `rpc_deadline = min(now + datanode_client_timeout, record.deadline)`, computed with checked arithmetic; an `Instant` overflow falls back to `record.deadline` and can only shorten the wait. There is no `WriteRetry` re-entry and no fan-out. The state lock is released before the await. A concurrent close after the gate does not revoke the authorization, and the task may invoke the requester after `closed = true`. Shutdown stops scheduling and joins such tasks, then drains; a call that is cancelled, timed out, or dropped does not prove the datanode did not execute it.

One terminal order applies to every retryable outcome after an authorized send. If `now >= deadline`, dead-letter `ttl_expired_after_replay_authorization` with a remote-side-effect diagnostic. Otherwise, if `send_attempts >= max_attempts`, dead-letter `send_attempts_exhausted`. Otherwise defer with backoff. A successful RPC response completes the record even if the deadline has passed. `RetryAfterRouteRefresh` first invalidates the shared route caches and then takes the same order; invalidation failure is logged and defers. A non-retryable response dead-letters `non_retryable`.

**TTL.** TTL prevents new replay sends from being authorized after the record deadline. It does not bound the completion time or remote side effects of an RPC authorized before that deadline; such a request may take effect after the client-side timeout or record deadline. TTL does not release memory by itself; the charge stays until the terminal outcome or the wrapper drain. `ttl_expired_before_replay_authorization` reports that no replay send was authorized; admission follows at least one failed dispatcher attempt whose outcome may be unknown. `ttl_expired_after_replay_authorization` reports at least one authorized replay send, so a remote effect may exist. On no-send paths (metadata or route lookup, absent source region, leaderless, decode, permit wait), TTL is the final time bound and `max_attempts` is not consumed; there is no separate lookup-attempt counter, so a lookup outage cannot exhaust a record that never sent.

**Backoff and panics.** Every retryable path increments `consecutive_failures`, a diagnostic backoff counter separate from the attempt budget, whether or not an RPC was sent. The delay is exponential with equal jitter and a 30 s cap. `max_attempts` and TTL bound different things: final-gate passes and record age. For a path that consumes attempts, the intended terminating bound is `max_attempts`; no-send paths terminate at TTL. Which bound is reached first depends on the backoff base and RPC duration and is not measured.

Each lease carries a token naming the record id it leased. A panicking per-record task is recovered under that token. If the task already applied a terminal transition, recovery is a no-op; otherwise recovery applies exactly one defer or terminal transition. A stale token cannot cause a second queue transition or charge release, or remove a successor. The token protects queue state, not remote application: if the task panicked after submitting the request, the datanode may have applied it, and a later replay may apply it again. The worker's only transition is recovery of a `JoinError`; a successful join carries no mutation.

## Routing and Layout Changes

The queue key is the source `RegionId` from the request header. A `RegionId` encodes a table id, but for a logical table the key carries the logical table id while cached physical routes carry the physical table id. Replay resolves physical identity before matching.

The record does not carry a route revision, and replay does not compare one. `PhysicalTableRouteValue.version` covers the whole physical table and increments when the physical table route is rewritten, including a leader move for any region and leader-staging updates. A difference proves only that some route changed, not that this record is invalid; comparing it would dead-letter records that replay can safely send to the moved leader. Only the plan-time `TableIdent.version` is carried, for the DDL check.

Route outcomes are record-level; none closes the queue:

| Condition | Outcome |
| --- | --- |
| Table or route lookup error | Defer with backoff; not treated as absence |
| Route resolves but the source region has no leader | Defer with backoff |
| Source region is present with a leader | Send to that leader; a leader move needs no special case |
| Table missing or `TableIdent.version` changed | Dead-letter `table_changed`; no send |
| More than one matching candidate, or a contradictory table id | Dead-letter the current record as `invalid_route`; the queue stays open |
| Source region absent | Defer until TTL expiry; no send, so no attempt consumed |

An absent look does not prove permanent loss. The route comes from `TableRouteCache`, initialized from the metadata store. `InitStrategy::VersionChecked` prevents a dirty value during an invalidate/load race, but a cache hit is not revalidated against the metadata store. The frontend invalidates an entry when a heartbeat carries `Instruction::InvalidateCaches` for the table; the default cache also expires entries after 10 minutes, with a 5-minute idle timeout. Replay does not refresh or invalidate the cache when the source region is absent: no RPC is sent, and `RetryAfterRouteRefresh` is only a response to a send. A stale positive route can therefore report the same absence until invalidation or expiry. The cache TTL is longer than the five-minute record TTL, and replay's repeated reads keep the entry from expiring through the idle timeout. Whether every layout change emits an invalidation is unresolved, and Core v1 does not assume refresh within the record TTL; the false-absence window is `UNVERIFIED`.

A real split or merge leaves no source region. Core v1 does not rebuild or remap, so the record expires at TTL even when the new targets are healthy. A pre-admission rebuild into several targets creates independent records under their own keys, with no old-before-new order or cross-target unit completion. Post-admission layout changes can lose an admitted write; per-key FIFO and one-in-flight remain true.

## Consistency

**Ordering.** Per-key FIFO applies to records admitted under the same source region. A failed head blocks its key until it completes, dead-letters, or expires; other keys continue. There is no global order. `prepare` groups requests per peer, so cross-peer order is not deterministic across runs; the attempt ordinal is diagnostic, not a client or peer ordering promise.

**No order against newer live writes.** The queue orders admitted records only against each other. It does not detect, fence, or reorder against a live write that reaches the same key after admission. If the original RPC was slow or lost and a newer live write applies first, a later replay can apply the older row and change the final value. There is no newer-write detection, version vector, per-key epoch, or stale-replay rejection.

**Duplicate risks.** Four risks are distinct:

- The existing retry can resend a request that already applied.
- Queue replay can resend a request whose original RPC applied but whose response was lost.
- A replay timeout may have applied and cannot be distinguished from "not applied."
- A panic after RPC submission can leave a request applied while recovery replays it again.

A stale lease token prevents a second queue transition and a second charge release; it does not prevent a second remote application. Core v1 adds no unit protocol, deduplication, or idempotency key. Append-mode and impure-dedup-key sheds are the only admission checks motivated by duplicate application. A client retry after the error can also race a pending record; that is not a queue-side duplicate but has the same visible effect.

**Plan-time version check.** The plan captures `TableIdent.version` from the `TableInfo` already loaded. Admission and replay compare it against current cached table info; ALTER bumps the version. The check is a timing window, not an atomic DDL fence:

| Timing | Behavior |
| --- | --- |
| Version changes before admission | Shed `plan_version_changed`; no record |
| Visible after admission, before the replay check | Dead-letter `table_changed`; no send |
| Visible after the replay check, before the RPC takes effect | No fence; ALTER can commit concurrently |
| After the RPC takes effect | Cannot be revoked; a late duplicate or effect is possible |
| Never visible locally because the cache is stale | The check passes; visibility depends on invalidation delivery |

Cache initialization is version-checked, not per-hit revalidated against the metadata store, so a frontend that has not received an invalidation can pass a stale check. `TableInfoValue.version` is a separate metadata-store revision and is not `TableIdent.version`.

**TRUNCATE, DROP, UNDROP.** `DROP` is caught as a missing table, and a recreated table has a new table id. `UNDROP` restores stored dropped-table metadata, so a plan-time version check may pass across a drop/undrop gap; Core v1 makes no detection claim. `TRUNCATE` does not update the table-info version, so the version check cannot detect it. Without the queue, the retry loop finishes before the client receives the error, so a client `TRUNCATE` after the error cannot race a frontend retry. With the queue it can: the write fails, the client truncates, and a replay authorized before the record deadline can later apply old rows. TTL bounds when a new replay send may be authorized, not when an already-authorized RPC takes effect.

## Failure Handling and Client Contract

**Give-up.** The existing `merge_attempts` returns `Err` on the first terminal attempt and drops retryable siblings. Core v1 changes the internal give-up result to keep both: terminal failures retain one context per region for logging and client selection, and retryable writes are retained until admission. `GiveUp.client_error` is the first terminal error in observed order, otherwise the first retryable error. Terminal failures are logged once and are not dead-lettered, because a dead letter is a queue-record outcome and no queue record exists.

**Retryable siblings.** The existing retry path uses `?` on a fallible reroute, so one route or re-prepare failure discards every pending write. Core v1 terminalizes only the affected refresh group, or only one source when its rebuild fails; other source writes continue.

**Client contract.** A successful write today means the region's leader returned a successful response for the region request; the datanode applies the write to its region engine and owns that region's WAL. It does not promise that an arbitrary later read observes the write; routing and the read path are unchanged. A queued write has no such response. Returning success at admission would report rows that have not been applied and would change `affected_rows` from applied rows to accepted rows; for a delete, the matched-row count is known only after application.

Core v1 therefore chooses compensation, not acknowledgment. The client receives the original error, and the queue may make the write land later. An error already does not mean "not applied," so compensation widens an existing window rather than opening one. `affected_rows` keeps its applied-rows meaning for successful writes and is not returned on error. A client that retries after the error can duplicate rows; a replay is not reported to that client. An acknowledged-write contract would require a client idempotency key with region-side deduplication, a queue that survives pod loss with fencing, and pending state in all four protocols; that is future work.

## Shutdown and Lifecycle

The wrapper owns the `JoinSet`, the task map, and the state; the worker borrows them. One teardown routine handles normal shutdown, a fatal invariant, and a worker panic: set `closed`; signal `cancel`; await the worker handle without aborting it while it borrows the task set; shut down and join every child; clear the task map; drain every remaining stored record through the removal path; log the reason. The wrapper does not restart the worker.

Draining uses stored records only; it never decodes, sends, or consults a route. An outstanding admission guard may still release a reservation after shutdown; shutdown does not await caller admission futures. `Frontend::shutdown` currently returns early if server shutdown fails; Core v1 always runs queue shutdown and returns the server error first. The frontend has no queue-specific shutdown timeout, and Core v1 promises no bounded queue shutdown time.

**Cancellation.** `finish` checks the cancellation token immediately before the admission call, and admission checks it again after all awaited eligibility work and before reserve. If cancelled at either checkpoint, that sibling is not admitted and further siblings are not attempted; siblings committed earlier remain. After the second checkpoint, the synchronous reserve/encode/commit can still race cancellation and commit may win; once committed, a record is independent of the client future. `Inserter::do_request` cancels when the caller is dropped while `execute` continues in a detached task, so these checkpoints are the only admission-time observations of caller cancellation. A client whose timeout is shorter than the backoff-only window can stop before give-up and queue nothing; this is intended.

## Configuration and Observability

Configuration names and defaults are proposals; numeric values are `UNVERIFIED` until measured. The queue is disabled by default. The five-minute TTL is a starting value, not an SLA.

```toml
[write_queue]
enable = false              # off: a retrying client can duplicate a replayed write
max_bytes = "512MiB"        # charged-payload ceiling, not a disk quota
max_records = 100000        # all reserved or charged records, including leased
max_record_size = "512KiB"  # rejected before allocation
ttl = "5m"                  # monotonic age from admission
max_attempts = 10           # final send-gate passes per record
replay_concurrency = 8      # records in flight; one RPC each

# Reused, not a new key:
# [datanode.client]
# timeout = "10s"           # replay-only client deadline; must be > 0 when the queue is enabled
```

Validation rejects zero bounds, `max_record_size > max_bytes`, zero `ttl`, zero `max_attempts`, zero `replay_concurrency`, and zero `datanode.client.timeout` when the queue is enabled; it does not clamp. After a datanode recovers, a backlog can drain faster than steady-state traffic, with old timestamps and the original write shape, adding flush and compaction work. Core v1 has no adaptive pacing.

Metrics carry no table or region labels: `write_queue_admitted_total`, `write_queue_shed_total{reason}`, `write_queue_charged_bytes`, `write_queue_charged_records`, `write_queue_replay_total{result}`, `write_queue_dead_letter_total{reason}`, `write_queue_task_join_error_total{kind}`, `write_queue_route_refresh_total{result}`, `write_queue_replay_latency_seconds`, `write_queue_inflight`, and `write_terminal_failure_total{stage,status,retry_hint}`.

Dead-letter logs include record, table, and source region ids, rows, `replay_authorizations`, `ever_replay_authorized`, age, reason, and the replay terminal error. `replay_authorizations` counts final-gate passes; neither a lookup deferral nor a no-send path increments it or `max_attempts`. The queue does not retain per-attempt application results from the dispatcher's earlier retries, so these logs describe replay authorization history only. `ever_replay_authorized = false` identifies the before-authorization TTL reason; the write chain may still have had a remote effect. This is diagnostics only; there is no query API and no persistence.

# Implementation Plan

No queue test or benchmark has been executed. The 2026-11-09 standalone probe is the only measurement. Existing dispatcher tests cover existing behavior only.

1. Admission accounting and sibling behavior: reserve/encode/commit, rejection categories, terminal contexts, retryable sibling retention, and cancellation checkpoints.
2. Lease, final gate, removal, and panic recovery: one transition, checked counters, underflow close and drain, stale-token no-op, and no double release.
3. TTL, deadline, retries, and RPC outcomes: before/after authorization reasons, `send_attempts` only on `Allowed`, no-send paths consuming no attempt, and timeout or deadline behavior.
4. Route refresh and source-region absence: heartbeat invalidation, cache TTL and expiry, absent-region deferral until TTL, no queue-side refresh, and `RetryAfterRouteRefresh`.
5. DDL behavior: the ALTER version window, `TRUNCATE`, `DROP`, and `UNDROP`.
6. Shutdown, cancellation, and drain: normal shutdown, fatal invariant, worker panic, child join, charge release, and abrupt exit.
7. Memory-shape measurement and allocator verification: replace the 512 B constant, pin factor 64 with null/small-int/string/binary/list/struct/JSON/empty/mixed shapes, measure the transport and compression buffers, and verify the charge-cap arithmetic.
8. Queue enabled/disabled performance comparison:
   - successful-path allocation and CPU;
   - admission latency;
   - reserve/encode/commit latency;
   - `QueueState` lock wait and hold time;
   - replay latency;
   - failure-path p99;
   - recovery replay pressure.

# Prototype Status

The current prototype is not the proposed design. It uses a writer task, an admission channel, and a `QueueStore` trait instead of direct commit; charge release is not atomic with removal and can leak on cancellation. It has no lease token, panic recovery, record deadline, or final gate, and it compares the physical route revision at replay. It has no frontend shutdown hook, and its replay transient factor is 3 rather than 64. These gaps must close before implementation is evaluated against this design.

# Drawbacks

- Retention is process-local. An admitted record can be lost with the process, dead-lettered on TTL or attempts, or never authorized if the queue closes.
- Replay can duplicate an applied request or race a client retry, while the client is told the write failed. There is no idempotency key, deduplication, or exactly-once claim; a stale lease token protects queue state, not the remote write.
- Replay has no order against newer live writes, and the DDL version check is a timing window. A later replay can change the final value; `TRUNCATE` and `UNDROP` are not detected.
- A post-admission split or merge can lose the write: the source region disappears, replay finds no matching candidate, and the record expires at TTL even when the new targets are healthy.
- Memory planning is best-effort: 512 B bookkeeping is an allowance, factor 64 is provisional, the 8 KiB transport floor and compression buffers are outside the charge, and 817 MiB is a model rather than an RSS bound. A failed head blocks its key, and recovery can replay in a burst.

# Unresolved Questions and Future Work

**Unresolved.**

- Whether every layout change emits a cache invalidation for the affected table, and how quickly heartbeat invalidation reaches a frontend relative to the five-minute record TTL. The false-absence window is `UNVERIFIED`; Core v1 does not assume refresh within the record TTL.
- Whether every enterprise Flow child path reaches the dispatcher give-up point has not been audited.
- Payload shapes outside the ten-shape measurement, the transport-compression extra allocation, the 512 B bookkeeping allowance, and the proposed numeric defaults remain unmeasured.

**Future work.**

- Durable queue and recovery: durable retention, startup recovery, and acknowledged writes with region-side idempotency.
- Topology-aware replay and strong replay consistency: split/merge rebuild with target sets and ordering, a region-side DDL fence, and newer-write detection.
- Operations: adaptive pacing, manual dead-letter replay, and automatic worker restart.

# Alternatives

- **Separate `common-retry-queue` crate with a `QueueStore` trait.** It creates a workspace member and a durable-shaped trait with no v1 caller; operator-local state matches the in-memory scope.
- **Bounded channel plus writer task.** It decouples admission latency from the lock but adds an admitted-but-not-yet-stored state, channel capacity, closed-channel failure, and a panic release path; direct commit is a short mutex section.
- **Queue-side rebuild across split/merge.** It preserves old-before-new but needs target sets, progress rules, gate tokens, and a fan-out cap; Core v1 never sees a unit.
- **Region-side DDL fence.** It closes the `TRUNCATE`/`UNDROP` window but adds a proto field, a manifest field, and a region path before the queue core is validated; Core v1 accepts the documented timing window.
