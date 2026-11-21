---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
Status: Draft for discussion
---

# Summary

This RFC proposes an in-memory, per-Frontend retry queue for ordinary inserts (including eligible Prometheus
remote write) that fail per entry in the first dispatcher round (`Error::PartialRegion`). The first dispatch
stays in the request path; `WriteDispatcher::finish` selects the client error and hands eligible retryable
per-region failures to admission.

The foundation is per-`RegionId` serial replay with global memory and record bounds. Bounded completion wait,
broader retryable admission, and an absolute retry budget may land separately, but `enable = true` waits for
all three and validation; v1 ships `enable = false`. An admitted record is retried only by the queue, and a
rejected failure returns the selected client error without an inline retry. A same-`RegionId` refresh retargets
and keeps the waiter, while a cross-region rebuild fans out and ends it. The queue is neither durable nor
exactly-once or at-least-once.

# Motivation

The dispatcher retries a failed per-region write inline with `write_retry.max_retries = 3`, jittered backoff,
and no per-attempt Frontend-to-datanode timeout: roughly 10.5s to under 21s plus RPC time. Restart, migration,
or partition can outlast that, and a client without a per-region view duplicates or drops regions. Issue #8085
asks for a bounded Frontend buffer with persistence; this RFC covers only the in-memory part.

The foundation alone is insufficient: a queue that gives up after the first round loses the common
leader-change case, and an attempt-count budget expires near the inline window or admits failures the queue
cannot represent. Completion wait, broader admission, and the absolute budget address those gaps.

# Goals and Non-Goals

**Goals.** One bounded queue per Frontend with per-`RegionId` serial replay, at most one in-flight record,
global byte and record bounds, reusing `write_retry` backoff, `TableRouteCache`, `TablePartitioner`, and
`RetryAfterRouteRefresh`. Admit retryable per-region failures from `PartialRegion`, whole-RPC, and single-entry
collapse when plan metadata makes them safe; admit `IssueUnknown` with its duplicate risk; preserve retryable
siblings when another sibling is terminal; reject an ineligible table shape instead of retrying it inline.
Give every record an absolute retry deadline inherited by successors and fan-out one shared invocation cap.

**Non-Goals.** No durability or restart recovery (a separate RFC), no exactly-once or at-least-once, and no
error proves non-application. No first-round timeout rescue, live-write ordering, or atomic DDL fence. No successor
completion group and no promise that the original client waits for all successors. No inline fallback: a
rejected failure may not bypass capacity or an existing `RegionId` record, and one failed entry never has two
owners. No exact `affected_rows` total, RSS bound, new wire field, Flow consistency, or automatic compensation.
Delete and `BulkInsert` stay on the separate inline dispatcher.

# Design

## Architecture

The queue is an operator-layer component owned by the Frontend and installed only on the queue-enabled
dispatcher used by ordinary inserts; Delete and `BulkInsert` keep a separate inline dispatcher. It never calls
`WriteDispatcher::execute`, `finish`, `WriteRetry::retry`, or a second round, and a successful write takes no
queue lock and allocates no record.

With the queue enabled, `finish` splits the first round into successes, retryable per-region failures, and the
selected terminal error before any retry decision. Eligible failures are admitted independently and an
ineligible table shape is rejected. Per-entry retry context is installed when `max_retries > 0` or the queue is
enabled, so `max_retries = 0` still yields per-region `RoutedWrite`s.

## Record and Retry Work Model

A record retains the target `region_id`, `RoutedWrite`, first-round provenance (`IssueUnknown` or observed
failure; absent for a successor), the absolute `retry_deadline`, lineage `(sequence, record id)`,
`consecutive_failures`, `next_ready`, one lease snapshot `{ cell, invocation, attempts }` whose `attempts` is
this record's only send counter, and at most one completion sender. `RoutedWrite` gains `plan: PlanMetadata`;
retaining it preserves the `Arc` and allocation with no second payload copy.

`PlanMetadata` carries the plan-time `TableIdent.version`, `partition_expr_version`, engine, append-mode and
instant-TTL flags, and Flow-source status. It is captured during planning and preserved through `prepare`,
`send_batch`, peer merging, and `finish`; a retarget keeps it, and a fan-out successor re-captures it. It never
inherits a stale `partition_expr_version`; `table_changed` compares each record's own plan-time version.
`TableIdent.version` (logical), `TableInfoValue.version` (metadata revision), and `partition_expr_version`
(physical) stay distinct, and only a partition-expression mismatch catches a physical repartition.
**Implementation prerequisite (unverified):** `RegionRequestHeader` cannot carry this sidecar.

The lineage sequence is unique within one Frontend queue lifetime and the record id is unique per record; a
successor keeps the sequence with a later id. Each lineage is also the key of one `LineageBudget` owned by
`QueueState`, holding `replay_invocations` and an `unsettled` count. Every successor references that same
lineage id, so a fan-out does not receive a fresh cap.

**Scheduling key.** The key is the target `RegionId` (table id, region group, and region number;
`src/store-api/src/storage/descriptors.rs`). FIFO and at-most-one-in-flight are per `(Frontend queue,
RegionId)`. A successor is enqueued under its destination key; successors rebuilding several old keys into one
destination region append in admission order, with no order across old keys.

A successor inherits `consecutive_failures`, the original `retry_deadline`, membership in the same
`LineageBudget`, and the lineage sequence; it does **not** inherit `attempts` or the parent's cell: it starts
`Empty` / `NotIssued` with its own `attempts = 0`. A route-refresh rebuild saves the triggering classification
through `Rebuilding` (`Issued` for an observed error, `IssueUnknown` if no result was observed; already
charged); a topology rebuild keeps `NotIssued` with no charge. A same-`RegionId` rebuild retaining one
request with the same `partition_expr_version` is a **retarget**. The Flow mirror snapshot is captured before
`dispatcher.prepare` and never re-captured on replay; eligibility uses the plan-time Flow-source flag, so
admission never queries Flow state.

## Admission and Ownership

Admission runs in `finish` only when the queue is enabled, after the first round is split; it cannot change the
selected terminal error, response shape, or `affected_rows`. A per-region failure is eligible when it is
`Body::Inserts` with one `InsertRequest`, its `PlanMetadata` is present and consistent with the target region,
it targets `mito`/`mito2`/`metric`, is not append mode or instant TTL, is not a Flow source, the queue is open,
and it fits the admission limits.

Eligibility is per entry, not per batch: it covers retryable failures from the `PartialRegion` and whole-RPC
branches, including a single-entry tonic error retaining a `RoutedWrite`. A retryable failure with no retained
`RoutedWrite` is a rejection. `IssueUnknown` is eligible whenever the table shape is, with its duplicate risk
unremoved. First-round provenance is conservative: whole-RPC retryable is `IssueUnknown`, `PartialRegion`
per-entry is an observed failure.

The stable eligibility input is `PlanMetadata`, not an admission-time table or Flow lookup. Missing or
inconsistent metadata, a multi-region body, or an unsupported table shape is an `UnsafeContext` rejection,
returning the selected client error with no record and no inline retry.

**Mixed batches.** Every eligible retryable entry is admitted independently and appended to its `RegionId`
queue. A table-shape-ineligible entry sends nothing, returns the selected client error, and is never retried
inline while the queue is enabled. This single-owner rule prevents overtaking an existing `RegionId` record or
bypassing capacity, replacing both fallbacks. A rejected retryable entry makes the request fail with the
selected terminal error, or the original error when none exists, even if siblings are admitted. A terminal
sibling is returned while retryable siblings are still admitted.

Capacity, closed, and unsafe-context rejections never fall back to inline and never wait for room; a non-insert
body is `UnsafeContext`. Reservation and commit happen under the state lock after a final open and limit check.

## Queue Invariants and Recovery

**Accounting authority.** `QueueState` owns both the per-record `LeaseSnapshot { cell, invocation, attempts }`
and one `LineageBudget { replay_invocations, unsettled }` per lineage, under the same lock. Workers hold only
a record id and lease generation and re-read the authority at each commit; supervisor recovery reads the
snapshot and budget under one lock acquisition, and that pair is the authoritative recovery view. Admission
increments `unsettled`, settlement decrements it, and the budget is removed only at zero.

| Cell | Invocation classification | Meaning | Attempt charge |
| --- | --- | --- | --- |
| `Empty` | `NotIssued` | Boundary not crossed | 0 |
| `IssueUnknown` | `IssueUnknown` | Boundary crossed; no result observed | charged once |
| `Issued` | `Issued` | Result observed | retained |
| `Succeeded` | `Issued` | Success observed; never replay | already charged |
| `Rebuilding` | saved trigger classification | Rebuild published; never twice | no new RPC charge |
| `Settled` | saved settlement classification | Terminal settlement completed | unchanged |

Classifications are boundary-based, not poll-based: `IssueUnknown` does not prove the future was polled or an
RPC was sent, and it is never reclassified as `NotIssued`. `Succeeded` is a cell, not a classification; `Ok`
may move `IssueUnknown` directly to `Succeeded`.

A new lease starts `Empty` / `NotIssued` with the record's persisted `attempts`, so a retried record may have
`Empty` with a non-zero count, while a successor starts at zero. The authorization commit, not the first poll,
is the boundary. Transitions preserve the carried classification, including `IssueUnknown` → `Rebuilding`,
which keeps `IssueUnknown`; a rebuild triggered by an observed error enters `Rebuilding` with `Issued`.

After `Ok`, the first action publishes `Succeeded` / `Issued` with no fallible step; only an abort can interrupt
it, and recovery uses the last committed snapshot, never success. An observed RPC error moves to `Issued`; a
cancelled or panicking future with no result stays `IssueUnknown` and settles `issue_unknown`, not a resend.
A published `Rebuilding` decision is atomic and never executed twice, and settlement happens at most once: the
worker settles under the lease, recovery uses the snapshot, and drain never re-settles.

**Completion ownership.** A record owns at most one completion sender until it settles. The settling worker,
supervisor recovery, or drain takes it under the state lock and sends exactly once after marking `Settled`, so
a second path finds nothing. A dropped receiver does not delete or settle the record; a panicked worker
completes only through the supervisor snapshot path.

**Close and drain.** Close rejects new admission, leases, and send authorization and stops scheduling. A task
stopped before the gate releases without joining; one past the gate is joined and handled by its snapshot:

| Cell / classification | Shutdown behavior |
| --- | --- |
| `Empty` / `NotIssued` | No boundary crossed: release lease and permit; record remains for drain |
| `IssueUnknown` / `IssueUnknown` | Wait for the in-flight future or `replay_rpc_timeout`; never reclassify as `NotIssued`; settle `issue_unknown` |
| `Issued` / `Issued` | Join for classification, rebuild, or settlement; no further RPC wait |
| `Succeeded` / `Issued` | Never replay; settle `applied` |
| `Rebuilding` / saved | Never rebuild twice; recover the published decision and saved classification |
| `Settled` / saved | No further action |

`replay_rpc_timeout` bounds only the unobserved-result wait; `Issued` does not wait, and cancellation or panic
never downgrades `IssueUnknown` to `NotIssued`.

## Memory and Resource Bounds

`max_bytes`, `max_records`, and `max_record_size` bound retained estimates and count, not RSS: the charge is
the retained `RegionRequest`'s `encoded_len()` plus fixed `RoutedWrite`/`Arc` overhead. Reserved, stored, and
leased records count against `max_records`; a reservation converts without a second charge, and the count is
released only on removal. `max_record_size` is checked before reservation; an oversized request is rejected
through admission, sends no replay RPC, and consumes no attempt, but cannot prevent the first-round
allocation.

`max_transient_bytes` bounds replay and rebuild reservations, separate from retained accounting and not RSS.
Its object, formula, and factor source are implementation prerequisites: charge each materialization once,
avoid double-charging retained records, use checked arithmetic that fails closed, release only through RAII,
and treat a failed reservation as `TransientFull` (dead-letter `transient_full`). Whether replay and rebuild
share one factor is undecided, and the peak is unbounded until the bounded rebuild helper exists.

## Retry Scheduling and Budget

**Authorization commit.** `attempts` counts charged queue replays, never the original round or any action that
does not cross the boundary. Each crossing charges once; `IssueUnknown` → `Issued` or `Succeeded` never
recharges, a retarget does not recharge, and `NotIssued` never charges. Authorization and
publication are one locked transition: it validates open state, `now < retry_deadline`, lease generation,
`attempts < max_attempts`, and `replay_invocations < MAX_REPLAY_INVOCATIONS`, then (1) increments the lineage
counter and (2) publishes `cell = IssueUnknown`, `invocation = IssueUnknown`, `attempts + 1`, consuming the
lease generation. The RPC starts after the lock is released with no intervening fallible step.

Because the RPC has not started before (2), a panic after (1) but before (2) over-counts the lineage and leaves
the record `NotIssued`; recovery may authorize, so the only possible error is **over-counting**. A panic after
(2) leaves `IssueUnknown` with one charge; one before the commit leaves no charge.

**Lookups and no-leader deferral.** Lookups precede authorization, bounded by a provisional 5s
`lookup_deadline`; failure reinserts with backoff and consumes no attempt, and a lookup finishing after
`retry_deadline` must not authorize. A leaderless region never crosses the boundary, keeps `NotIssued`, stays
schedulable until `retry_deadline`, must not busy-loop, and must produce an executable `next_ready`. Whether it
increments `consecutive_failures` and the deferral policy remain implementation prerequisites.

**Retry authorization deadline.** The only send gate is the authorization commit; backoff, lookups, and
rebuild consume time but never extend the deadline. No separate `send_authorized` flag exists; the
`IssueUnknown` snapshot proves authorization. The budget is one absolute `retry_deadline = admission +
retry_budget`, the last time a new replay may be **authorized**. It is not refreshed by retarget, rebuild,
lookup retry, or a new attempt; every successor inherits it. An invocation authorized before the deadline may
run to its own `replay_rpc_timeout`, which starts at that commit, and settle after it. This RFC does not claim a
strict `retry_budget + replay_rpc_timeout` lifetime, because rebuild and local steps do not all have a reliable
timeout.

**In-flight RPC timeout.** `replay_rpc_timeout` is queue-owned. The Frontend-to-datanode channel has no
timeout (`channel_config.timeout = None`), so `datanode.client.timeout` does not bound this wait and must not be
a prerequisite. Deadline expiry does not cancel an issued RPC, and the RPC timeout does not authorize a new
send. A timeout, cancellation, or panic after the boundary leaves `IssueUnknown` and settles `issue_unknown`
without retry, because a resend could duplicate an already-applied write.

**Lineage cap.** `MAX_REPLAY_INVOCATIONS` is an internal safety cap on total sends in one lineage, carried by
`LineageBudget.replay_invocations` and charged with `attempts`. Every successor references the same lineage id,
so a fan-out does not multiply the cap, and one locked check-and-increment prevents two successors taking the
last slot. The budget survives until `unsettled` reaches zero. `max_attempts` is a per-record secondary cap.

**Failure progression.** `consecutive_failures` counts results that prevent completion and require
rescheduling or rebuild; it is not an RPC failure count. Terminal outcomes add nothing, observed success ends
the record, and it drives backoff and `next_ready` and is inherited by successors. Backoff is exponential with
jitter, capped at 10s (the existing `max_backoff`).

| Path | Counter | Successor | Invocation charge | `next_ready` |
| --- | --- | --- | --- | --- |
| Retryable RPC error | +1 | no | +1 (`Issued`) | backoff |
| `RetryAfterRouteRefresh` | +1 | fan-out only (retarget keeps the record) | +1 (`Issued`) | backoff |
| `TopologyRefresh` | +1 | fan-out | 0 (`NotIssued`) | backoff |
| Lookup error | +1 | no | 0 (`NotIssued`) | backoff |
| No leader | implementation prerequisite | no | 0 (`NotIssued`) | deferral; policy pending |
| Transient reservation failure | n/a | no | 0 (`NotIssued`) | dead-letter `transient_full` |

`Invocation charge` is the current invocation's charge, not the record's `attempts`; terminal outcomes follow
Failure Handling.

## Routing and Retarget / FanOut

A target region is resolved from `TableRouteCache` after all lookups. A missing table, id mismatch,
contradictory target, or changed plan-time `TableIdent.version` dead-letters `table_changed` with no RPC and no
attempt; a lookup error or leaderless region defers (`NotIssued`, no attempt, bounded by `retry_deadline`). A
region absent from the physical route rebuilds as `TopologyRefresh` without RPC or attempt;
`RetryAfterRouteRefresh`, including a partition-expression mismatch, also rebuilds.

**Retarget.** A leader change with the same `RegionId` or an unchanged `partition_expr_version` retargets:
`rebuild_insert_requests` retains the original `InsertRequest` (`is_partition_expr_unchanged`). It updates only
the peer and request, keeping the queue id, plan metadata, cell, attempts, failures, `retry_deadline`, lineage
budget, FIFO position, and waiter without settling or refreshing.

**FanOut.** Any rebuild producing zero, multiple, or different `RegionId`s, or triggered by region removal or
a changed partition expression, is a fan-out. The parent records `Rebuilding`, admits successors independently
under their destination keys, and settles `rebuilt` only when at least one is admitted. With zero admitted
successors it settles `observed_failure` for an `Issued` trigger, `issue_unknown` for `IssueUnknown`, and
`not_sent` for `NotIssued`. Its waiter is always notified with the original failure. Successors inherit
`consecutive_failures`, `retry_deadline`, the same `LineageBudget`, and lineage, but not `attempts` or
`PlanMetadata`; each starts at zero, re-captures plan metadata from the rebuilt request, and has no sender. A
rejected successor is not a record; its rows are lost (`successor_rejected_total`).

Rebuild invalidates routes and builds a `TablePartitioner`. The current `rebuild_write_requests` returns a fully
materialized `Result<Vec<RegionRequest>>`, `rebuild_insert_requests` builds the full `Vec`, and
`RowSplitter::split` materializes all destination rows without early stop. A streaming bounded helper is an
implementation blocker: it must enforce `MAX_SUCCESSORS_PER_RECORD` (provisional 256) and the transient
reservation before materializing the peak, and never call the unbounded insert path.

## Completion Wait

The client wait is bounded by `completion_wait`, measured from the first round's return and including
admission, so the client returns no later than the first round's return plus the window.

The wait runs only when every retryable failure was admitted and there is no terminal sibling. A terminal
sibling or a rejected retryable failure is returned immediately, and any admitted siblings continue without a
waiter; the wait cannot turn such a request into success.

Success requires every admitted record to settle `applied` inside the window; the response then reports the
summed observed applied rows of merged first-round successes and replay successes. Any `observed_failure`,
`issue_unknown`, `not_sent`, or `rebuilt` settlement, or window expiry, returns the selected client error; a
partial result is never success. `affected_rows` is observed applied rows, not an exact logical total.

Only final settlement notifies; transient failures, backoff, lookups, and no-leader deferral do not. A retarget
keeps the same waiter; a fan-out parent notifies the original failure, and successors have no sender. Timeout
and client cancellation drop the receiver and end only the client wait: they do not delete, settle, cancel, or
reassign a record, which continues under the deadline.

## Failure Handling and Client Contract

Settlement outcomes are `applied` when a replay success was observed, `observed_failure` when a failure result
was observed, `issue_unknown` when an invocation crossed the boundary but no result was observed, `not_sent`
when no queue invocation crossed the boundary, and `rebuilt` when a fan-out parent admitted at least one
successor. `not_sent` is queue-local state only: it never proves the original dispatcher round did not apply,
and `issue_unknown` and `observed_failure` also do not prove non-application. Every replay is
duplicate-capable; no outcome implies exactly-once or at-least-once, and `rebuilt` carries unadmitted rows as
lost.

| Terminal path | Outcome | Current invocation charge | Reason |
| --- | --- | --- | --- |
| Replay success | `applied` | `Issued`, +1 | Replayed rows observed |
| Deadline reached with cell `Empty` | `not_sent` | `NotIssued`, 0 | `deadline_before_attempt` |
| Stopped with cell `IssueUnknown`, no result | `issue_unknown` | `IssueUnknown`, +1 | `rpc_timeout` / `rpc_cancelled` / `worker_panicked` / `rpc_unknown` |
| Retryable result after deadline | `observed_failure` | `Issued`, +1 | `deadline_after_attempt` |
| Record attempts or lineage cap exhausted before invocation | `not_sent` | `NotIssued`, 0 | `attempts_exhausted` / `invocation_cap` |
| Table changed or transient full | `not_sent` | `NotIssued`, 0 | `table_changed` / `transient_full` |
| Non-retryable response | `observed_failure` | `Issued`, +1 | `non_retryable` |
| Panic after observed RPC error, before settlement | `observed_failure` | `Issued`, +1 | `worker_panicked` |
| Rebuild done or interrupted after admissions | `rebuilt` | saved classification; no new charge | at least one successor admitted; `rebuild_interrupted` |
| Rebuild failed before admitting a successor | `observed_failure` if saved `Issued`; `issue_unknown` if saved `IssueUnknown`; `not_sent` if saved `NotIssued` | saved classification; no new charge | `rebuild_failed` / `rebuild_interrupted` |
| Drain with cell `Empty` | `not_sent` | `NotIssued`, 0 | `shutdown` |

A completion-wait timeout is not a settlement outcome; the record keeps its lifecycle. `IssueUnknown`'s
reasons record the event, not the remote outcome. The drain row covers only an invocation that never crossed
the boundary, and `NotIssued, 0` never resets the record's `attempts`. Drain removes remaining unsettled
records and completes their waiters with the failure outcome; `Succeeded`, `Settled`, and published
`Rebuilding` records are not drained.

# Configuration and Observability

```toml
[write_queue]
enable = false
max_bytes = "512MiB"
max_records = 100000
max_record_size = "512KiB"
max_transient_bytes = "128MiB"
retry_budget = "5m"
completion_wait = "30s"
replay_rpc_timeout = "10s"
# max_attempts = 3  # optional per-record cap; disabled when unset
max_replay_invocations = 32
replay_concurrency = 8
```

`enable = false` is the v1 default; `enable = true` is opt-in, and default-on is considered only after the
foundation, all three enhancements, and validation land. Configuration loads at startup with no hot reload.

`retry_budget` is the retry **authorization** budget, not a record lifetime; `max_attempts` is an optional
per-record cap; `max_replay_invocations` is the internal lineage-wide safety cap. `completion_wait = 0`
disables the client wait. `replay_rpc_timeout` starts at the authorization commit and is independent of
`datanode.client.timeout` (`None` on this channel), so a zero or absent channel timeout is not an error.
Configuration rejects zero bounds, `max_record_size > max_bytes`, and an invalid `max_attempts`; it does not
clamp, and an undersized `max_transient_bytes` yields `TransientFull`.

Metrics use the `greptime_write_queue_` prefix without table/region labels. `issue_state` is `issued`,
`issue_unknown`, or `not_issued` (a zero-attempt invocation, not an RPC attempt). `replay_scope` is
`replay_issue_unknown` if any replay crossed the boundary without an observed result (a later observed
invocation does not erase it), otherwise `replay_issued` if any result was observed, otherwise
`replay_not_sent`, which is local queue state only. `settled_total` uses `applied`, `observed_failure`,
`issue_unknown`, `not_sent`, or `rebuilt`. `replay_latency_seconds` measures boundary-to-classification only.

# Consistency

**Ordering.** Enqueue FIFO is per `(Frontend queue, RegionId)` with at most one in-flight record; a failed head
blocks only its key while other keys continue. Successors join their destination key; no order is promised
across keys, Frontends, or old keys that rebuild into one destination region (R3). A retarget keeps the
record's FIFO position; fan-out successors append in admission order only, and no completion group orders
them. Lineage `(sequence, record id)` is correlation only.

**Newer live writes and DDL.** The queue orders admitted records only against each other; a live write after
admission is not fenced, so older rows can replay after it. Replay compares the captured
`TableIdent.version` with the routing target's version; it is a timing window, not an atomic fence. `ALTER`
can race between check and RPC, cache hits are not revalidated, `TRUNCATE` does not bump the version, `DROP`
appears as missing, a recreated table has a new id, and `UNDROP` restores metadata. The queue adds a window
where the client can run DDL such as `TRUNCATE`, then an earlier-admitted replay applies old rows.

# Implementation Plan

None of these prerequisites is complete; each blocks release. The foundation lands first behind `enable =
false`, each enhancement independently, and items 3–5 are release blockers.

1. **Dispatcher split.** Queue-enabled dispatcher for ordinary inserts and a separate inline one for Delete and
   `BulkInsert`; split in `finish`, never call `WriteRetry::retry`, preserve retryable `RoutedWrite`s.
2. **Plan metadata.** Carry `PlanMetadata` and the plan-time `TableIdent.version` through `prepare`,
   `send_batch`, peer merging, `split_request`, and `finish`; re-capture it for each successor. Unimplemented.
3. **Record and admission core.** Per-`RegionId` FIFO, one in-flight record, reservation and the `encoded_len()`
   ledger, lineage, and the record/lease snapshot.
4. **Worker and lifecycle.** One-shot invocation, gate, single-publish commit, backoff, lease check, six cells,
   supervisor recovery, at-most-once settlement, close, join, and drain.
5. **Bounded rebuild.** Streaming helper enforcing successor and transient limits before materialization.
6. **Configuration and metrics scaffold.**

**Enhancements.** Each is coupled to the foundation but lands separately.

- **A. Bounded completion wait.** Receivers, sender ownership, retarget keeping the waiter, fan-out settling
  `rebuilt`, timeout/cancel leaving the record.
- **B. Broader admission.** Per-entry eligibility from `PartialRegion`, whole-RPC, and single-entry collapse;
  `IssueUnknown` provenance; sibling independence; rejection without inline fallback.
- **C. Time-based budget.** Absolute `retry_deadline`, `replay_rpc_timeout`, shared `replay_invocations`,
  boundary charging, settlement taxonomy.

# Implementation Validation and Testing

These are acceptance criteria, not results; no measured bound is claimed.

- **Default, admission, and error contract.** `enable = false` preserves inline retry and client behavior, with
  no queue allocation or lock on the success path. Per-sibling admission covers `PartialRegion`, whole-RPC, and
  single-entry collapse, `IssueUnknown` provenance, and capture at `max_retries = 0`; cancellation before
  commit leaves no record. Rejection returns the selected client error with no inline attempt, sibling
  suppression, or capacity bypass; a terminal sibling is returned while retryable siblings are admitted
  without a waiter. Delete and `BulkInsert` reach no queue path, and both batchers flush `BulkInsert`.
- **FIFO, Retarget, and FanOut.** Per-`RegionId` serial admission and one in-flight record; a failed head blocks
  only its key and a saturated key stays within global bounds. A same-`RegionId`, unchanged-
  `partition_expr_version` rebuild retargets and keeps the waiter; a zero/multiple/different-region rebuild
  fans out, settles `rebuilt` only with an admitted successor, notifies the original failure, and gives
  successors no sender and fresh metadata. Measure per-key depth and rejection under hotspot load; this RFC
  defines no separate per-key backlog bound.
- **Completion wait.** Success only when every retryable failure was admitted and every admitted record reaches
  observed `applied`; any non-applied settlement, rejection, terminal sibling, or expiry returns the selected
  client error; timeout and cancel leave the record running; panic completes through the supervisor snapshot.
- **Issue state and budget.** Cover panic before the commit, between its lineage increment and publication, and
  after publication before the first poll; drop-before-poll, timeout, and cancellation; concurrent successors
  competing for the lineage cap. Assert one charge per crossing including retarget, zero for `NotIssued`, no
  recharge on `IssueUnknown` → `Issued`/`Succeeded`, an absolute inherited `retry_deadline` (no new send after
  it, an authorized RPC may finish), and a per-record `max_attempts` starting at zero for a successor while
  `MAX_REPLAY_INVOCATIONS` does not reset or multiply with fan-out.
- **Recovery, shutdown, and version.** Six cells; recovery reads snapshot and budget under one lock; the budget
  survives while successors are unsettled; classification survives `Rebuilding` and `Settled`; `Empty` versus
  `IssueUnknown` close handling; `Issued` shutdown waits for no `replay_rpc_timeout`; no double rebuild or
  settlement. The plan-time version survives handoff and a changed version dead-letters `table_changed`.
- **No-leader, memory, and latency.** Sustained no-leader keeps `NotIssued`, sends no RPC, adds no attempt,
  avoids busy-looping, produces an executable `next_ready`, and ends at `retry_deadline`. Measure replay and
  rebuild memory across shapes and fan-out, fix the formula and factor source, and validate sizes and
  `replay_concurrency`. Measure client response, admission, observed-success, and terminal-settlement
  separately; the p99 5% target applies only to admission-success under the same failure injection and
  workload. Validate lookup deadlines, the topology/route-refresh split, lock contention at 1/8/64 writers,
  success-path metadata cost, and restart losing in-memory records.

# Risks

- **Default-on release.** Enabling by default changes retry timing and resource use for all eligible writes; v1
  ships `enable = false`, and enabling early is unsupported.
- **Rejection loses the retry opportunity.** Saturation, limits, lookup failure, or an ineligible shape reject
  a sibling; it gets no queue replay and no inline fallback, so the client may see the error earlier.
- **Process-local loss.** Restart, crash, deadline settlement, or close can lose admitted rows after the client
  saw a failure; retention is best-effort and there is no restart recovery.
- **Duplicates and unknown application.** Lost responses, timeouts, panics after submission, rebuilds, fan-out,
  and client retries can duplicate rows; no idempotency key exists, an error never proves non-application, and
  the client cannot tell whether a write was admitted, replayed, or applied. The wait reports observed applied
  rows only; an `IssueUnknown` record may already have applied, and timeout does not mean the records stopped.
- **DDL and stale replay.** `ALTER`, stale cache, `TRUNCATE`, `DROP`/recreate, `UNDROP`, and newer writes can
  change the final value; residual risk is R1.
- **Fan-out budget.** Successors share the deadline and invocation cap, but a wide fan-out still creates many
  records and transient reservations; the client never waits for all successors, and partial admission loses
  rows.
- **Memory is not RSS.** Accounting does not bound the allocator, tonic, compression, runtime, or caches.
- **Bounded rebuild is blocked; shutdown gaps remain.** The current path materializes all destination rows, so
  limits apply after the peak; teardown can outlast `replay_rpc_timeout`, and supervisor panic or an
  unjoinable task has no final-settlement guarantee.
- **Ordering.** A successor can execute after newer records; one in-flight record per key bounds but does not
  eliminate inversion; cross-Frontend and merge order are undefined.

# Questions for Review

**R1. Is the disclosed DDL and stale-replay risk acceptable for v1 without an atomic fence?** ALTER, TRUNCATE,
DROP/UNDROP, and newer-write windows are documented as residual risk. Accept, or is stronger DDL coordination
required before adoption?

**R2. Is insert-only scope acceptable as the v1 boundary?** Delete and BulkInsert remain excluded, while the
earlier RFC and current PR description still mention delete and need separate synchronization.

**R3. Is the per-key-only FIFO boundary acceptable for v1?** FIFO holds within one `RegionId` in one Frontend
with at most one in-flight record, not across keys, Frontends, or when multiple old keys rebuild into one
destination region. Accept this rather than adding a merge-ordering mechanism?

**R4. Is the proposed in-memory replay bound acceptable before it is measured?** `max_transient_bytes` is a
hard gate, but the reservation factor and replay/rebuild peak are unmeasured; `enable = true` requires a
worst-shape factor, a formula, and an RSS caveat. Accept that gate, or require a measured bound first?

# Future Work

Durable retention and recovery with region-side idempotency; deduplication or idempotency keys; delete and
other excluded paths; a region-side DDL fence, newer-write detection, and stale-replay protection; unit-level
rebuild accounting; Flow consistency; fairness, adaptive pacing, a queryable dead-letter store, manual replay,
and worker restart. No schedule implied.

# Alternatives

- **Inline retry plus a post-retry buffer.** Two schedulers and budgets on one write; rejected.
- **Encoded envelope decoded per replay.** Adds an encode, a decode, and a second payload; retained
  `RoutedWrite` accounting is cheaper.
- **A successor completion group.** Would imply an all-or-nothing result the queue cannot provide.
- **Any inline fallback while the queue is enabled.** Whole-batch fallback can overtake an existing record;
  per-entry fallback creates two owners for one response. Ineligible failures are rejected instead.
- **A fresh budget or invocation cap per successor.** Multiplies total sends with fan-out width; rejected.
- **TTL plus `max_attempts` as the primary budget.** Bounds record age but not in-flight work; the absolute
  deadline is primary.
- **Reuse `datanode.client.timeout` for replay.** It is `None` here and cannot bound the wait; a queue-owned
  `replay_rpc_timeout` is required.
- **Region-side DDL fence.** Correct, but adds proto/manifest/region work before the core is validated.
