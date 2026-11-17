---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
Status: Draft for discussion
---

# Summary

This RFC proposes an in-memory, per-Frontend retry queue for ordinary inserts (including eligible Prometheus
remote write) failing per entry in the first dispatcher round (`Error::PartialRegion`);
`WriteDispatcher::finish` admits before returning the client error.

The proposed default for the completed and validated implementation is `enable = true`; `enable = false`
selects the existing inline path. When enabled the
queue owns the retry lifecycle: an admitted record is retried only by the queue; a rejected record returns the
original client error without entering or waiting on the inline loop, and no second scheduler starts. FIFO and
at-most-one-in-flight are per `RegionId` within one Frontend queue; lineage is not execution order, and
cross-key, cross-Frontend, and merge order are not promised (R3).

# Motivation

The dispatcher retries a failed per-region write inline with `write_retry.max_retries = 3`, jittered backoff,
and no per-attempt endpoint timeout on the Frontend-to-datanode channel: roughly 10.5s to under 21s plus RPC
time. A restart, migration, or partition can outlast that, and a client without a per-region view duplicates or
drops regions. Issue #8085 asks for a bounded Frontend buffer after the first round: an admitted record keeps
retrying in the queue, while a rejected one returns the original client error.

# Goals

One bounded queue per Frontend; a failed first round never returns `Ok`; reuse `write_retry` backoff,
`TableRouteCache`, `TablePartitioner`, and `RetryAfterRouteRefresh`; no second payload copy; one lookup set and
RPC deadline per invocation; supervisor-owned close, join, and drain; no response-shape or `affected_rows`
change.

# Non-Goals

No durability, restart recovery, exactly-once, or at-least-once; no error proves non-application; no first-round
timeout rescue, live-write ordering, or atomic DDL fence; no record-level "sent"/"not sent" claim; no RSS or
allocation bound; no new wire field; no Flow consistency; no automatic compensation for rejected admission.
Delete, `BulkInsert` (both pending-rows batchers), and the other excluded paths stay out of scope.

# Design

## Architecture

The queue is an operator-layer component owned by the Frontend and shared by the builder's dispatchers; it never
calls `WriteDispatcher::execute`, `finish`, `WriteRetry::retry`, or a second dispatcher round. Successful writes
take no queue lock, allocate no record, and skip admission. When enabled, `finish` selects the client error and
admits eligible retryable siblings independently before returning (Admission); it uses `max_attempts`, not
`write_retry.max_retries`, with per-entry capture installed when `max_retries > 0` or the queue is enabled.

## Record and Retry Work Model

A record retains target `region_id`, `RoutedWrite` (`{ peer, request: Arc<RegionRequest> }`), the monotonic
admission `deadline`, plan-time `TableIdent.version`, `partition_expr_version`, `consecutive_failures`, lineage
`(sequence, record id)`, and one lease snapshot `{ cell, invocation, attempts }`. `attempts` is the record's
sole cumulative replay budget; retaining `RoutedWrite` keeps its `Arc` and allocation with no second payload
copy.

The lineage sequence is unique within one Frontend queue lifetime and the record id is unique per record; a
successor keeps the sequence with a later id. Lineage is metadata for correlation, metrics, and debugging, with
no FIFO, deduplication, fencing, or budget authority.

**Scheduling key.** The key is the target `RegionId` (table id, region group, and region number;
`src/store-api/src/storage/descriptors.rs:51`) within one Frontend queue lifetime. A `RoutedWrite` is a
single-region write, and `write_region_id` rejects multi-region bodies
(`src/operator/src/write_dispatcher.rs:715`). FIFO and at-most-one-in-flight are per `(Frontend queue,
RegionId)`. A rebuild successor is enqueued under its own destination `RegionId`; successors rebuilding several
old keys into one destination region share that key and are appended in admission order, with no order across
old keys. **Implementation prerequisite:** the queue index and admission enqueue order are unimplemented; their
exact representation is not fixed here.

`TableIdent.version` (logical), `TableInfoValue.version` (metadata revision), and `partition_expr_version` are
distinct; a physical repartition need not bump the logical version, and only a partition-expression mismatch
through route refresh or rebuild catches it. Planning captures `TableInfo.ident.version` and preserves it
through `prepare`, `send_batch`, peer merging, and `finish`; admission and replay never re-capture it.
**Implementation prerequisite (unverified):** `RegionRequestHeader` cannot carry the sidecar.

Settlement charges the parent's final invocation once. A successor inherits post-settlement `attempts`,
`consecutive_failures`, the original deadline, and the lineage sequence; it starts `Empty` / `NotIssued` with
that cumulative `attempts` (never the parent's transient cell) and computes `next_ready` from the inherited
count. A route-refresh rebuild records the triggering invocation's observed `Issued` classification (already
charged) through `Rebuilding`; a topology rebuild keeps `NotIssued` with no charge. Each record gets a fresh
queue-local id.

## Admission

Admission runs in `finish` only when the queue is enabled, after client-error selection and before the error is
returned; it cannot change the selected error, response shape, or `affected_rows`. A sibling is eligible only if
it is `Body::Inserts` with one `InsertRequest` from the per-entry `PartialRegion` branch, has consistent ids,
`flow_source` false, targets `mito`/`mito2`/`metric`, is not append mode or instant TTL, has `owners: None`, the
queue is open, and it fits the admission limits (Memory Accounting). Eligibility may `await` bounded table and
Flow cache reads under a provisional 5s `admission_lookup_deadline`, distinct from the replay lookup deadline
and requiring validation; timeout or failure rejects that record only, without changing the client error,
re-entering inline retry, or suppressing other siblings. Reservation and commit then happen under the state lock
after a final open and limit check, with no `await` or user code. Admission is best-effort, not durability or
acknowledgment; a whole-RPC failure, including a single-entry tonic error, is not admitted, and rejection is not
evidence of non-application.

## Queue Invariants and Recovery

**Recovery authority.** Supervisor recovery reads one authoritative, recoverable lease snapshot `{cell,
invocation, attempts}`, committed and read as one unit; `attempts` has no independent copy. `cell` selects
recovery and shutdown, `invocation` preserves the accounting classification, and a cell alone never decides
whether the invocation was charged.

| Cell | Invocation classification | Meaning | Budget |
| --- | --- | --- | --- |
| `Empty` | `NotIssued` | Boundary not crossed | 0 |
| `IssueUnknown` | `IssueUnknown` | Boundary crossed; no result observed | charged once |
| `Issued` | `Issued` | Result observed | retained |
| `Succeeded` | `Issued` | Success observed; never replay | already charged |
| `Rebuilding` | saved trigger classification | Rebuild published; never twice | no new RPC charge |
| `Settled` | saved settlement classification | Terminal settlement completed | unchanged |

Classifications are boundary-based, not poll-based: `IssueUnknown` does not prove the future was polled, the
RPC was sent, or the datanode received the request, and it is never reclassified as `NotIssued` even if no poll
occurred. `NotIssued` means only that the boundary was not crossed. `Succeeded` is a cell, not a classification;
`Ok` may move `IssueUnknown` directly to `Succeeded` with classification `Issued`. `Rebuilding` and `Settled`
carry a saved classification but are not classifications.

A new lease starts `Empty` / `NotIssued` with inherited cumulative `attempts`; `Empty` does not mean zero
history. The worker publishes `IssueUnknown` immediately before the first poll, but the publication, not the
poll, is the boundary; only that boundary transition changes `attempts` (Retry Scheduling).

Transitions preserve the classification carried into them, except `IssueUnknown` → `Rebuilding`, which first
records observed `Issued`; `Rebuilding` → `Settled` keeps its saved classification and `Succeeded` → `Settled`
keeps `Issued`. Settlement never changes the classification.

After `Ok`, the first action publishes `Succeeded` / `Issued` with no fallible step; only an abort can interrupt
it, and recovery uses the last committed snapshot (`IssueUnknown` or `Issued`), never success. An observed RPC
error moves to `Issued` / `Issued`; a cancelled or panicking future with no result stays `IssueUnknown`.

A published `Rebuilding` decision is atomic and never executed twice; admitted successors survive a later
parent panic and unadmitted rows are lost. Settlement happens at most once: the worker settles under the lease,
supervisor recovery uses the snapshot, and drain never re-settles.

Close rejects new admission, new leases, and send authorization under the state lock and stops scheduling. A
task stopped before the gate is `Empty` / `NotIssued` and releases without joining; a task past the gate is
joined and handled by its snapshot:

| Cell / classification | Shutdown behavior |
| --- | --- |
| `Empty` / `NotIssued` | No boundary crossed: release lease and permit; the record remains for drain |
| `IssueUnknown` / `IssueUnknown` | Boundary crossed: wait for the in-flight future or `rpc_deadline` where a future exists; never reclassify as `NotIssued`; settle `unknown_remote_outcome` when no result can be observed |
| `Issued` / `Issued` | Result observed: join worker for classification, rebuild, or settlement; no further RPC wait |
| `Succeeded` / `Issued` | Never replay; settle `applied` |
| `Rebuilding` / saved | Never rebuild twice; recover the published decision and saved classification |
| `Settled` / saved | No further action |

`rpc_deadline` bounds only the unobserved-result wait; `Issued` does not wait, and the supervisor never
re-issues an `Issued` invocation. Cancellation or panic never downgrades `IssueUnknown` to `NotIssued`.

## Memory Accounting

`max_bytes`, `max_records`, and `max_record_size` bound retained estimates and count, not RSS: the charge is the
retained `RegionRequest`'s `encoded_len()` plus fixed `RoutedWrite`/`Arc` overhead, computed without a second
payload. Reserved, stored, and leased records all count against `max_records`; a reservation converts without a
second charge, and the count is released only on removal. `max_record_size` is checked before reservation; an
oversized request is rejected through the admission rejection path, sends no replay RPC, and consumes no
attempt. It cannot prevent the first-round allocation; `max_bytes` covers retained records only, and neither
bounds transient materialization or RSS.

`max_transient_bytes` bounds replay and rebuild reservations, separate from retained accounting and not RSS. The
reservation object, formula, and factor source are implementation prerequisites: charge each transient
materialization once, avoid double-charging retained records, use checked arithmetic that fails closed on
overflow, release only through an RAII guard, and treat a failed reservation as `TransientFull` (dead-letter
`transient_full`). Whether replay and rebuild share one factor is undecided; either
choice needs a policy validated against representative request sizes, concurrency, and rebuild peak; the peak is
unbounded until the bounded rebuild helper exists (Routing).

## Retry Scheduling

Cells and classifications are defined in Queue Invariants.

**Charge invariants.** `attempts` counts queue replay budget only, never the original dispatcher round, a
scheduling pass, a lookup retry, authorization, or guaranteed datanode arrival. Each invocation that crosses the
boundary charges exactly once; `IssueUnknown` → `Issued` or `Succeeded` never recharges; `NotIssued` never
charges. The boundary transition constructs the complete next snapshot (`cell = IssueUnknown`, `invocation =
IssueUnknown`, `attempts + 1`) and publishes it in one operation, with no panic, `await`, allocation, user code,
or other fallible step between commit start and completion. Recovery sees the complete pre- or post-commit
snapshot, never a partial update; a lock that assigns fields separately is insufficient unless partial state is
discarded. **Implementation prerequisite:** the representation must provide this single-publish guarantee; no
separate mutable charge field or worker/supervisor divergence is allowed.

A panic before the commit leaves `Empty` / `NotIssued` with no charge; a panic after it, even before the first
poll, leaves `IssueUnknown` / `IssueUnknown` with one charge and no resend. `attempts >= max_attempts` blocks
further replay; concurrent invocations are bounded by `replay_concurrency`.

**Lookups.** Table and route lookups happen before authorization, bounded by a provisional 5s replay
`lookup_deadline`; failure reinserts with backoff while open and consumes no attempt.

**No-leader deferral.** A leaderless region never crosses the boundary, consumes no attempt, and keeps
`NotIssued`; it stays under the TTL, must not busy-loop or bypass TTL through repeated lookups, and must produce
an executable `next_ready`. Whether it increments `consecutive_failures` and the concrete deferral policy are
implementation prerequisites; no backoff value or new configuration is fixed here.

**Send authorization gate.** The gate runs once per invocation under the `QueueState` lock, checking queue open,
deadline, lease validity, and `attempts < max_attempts`; only then is `send_authorized` set. Close serializes on
the same lock and wins or loses atomically. `send_authorized` is gate state, not proof of issuance, and adds no
attempt. No `await` may sit between a successful gate and the first poll; authorization survives TTL expiry and
is not reclassified.

**TTL and budget.** TTL is the maximum record age from admission; successors inherit it. `max_attempts = 3`
counts queue replays only, so budget and TTL are independent. With existing backoff, retryable RPC failures
normally exhaust three replays before `ttl = 5m`; outcomes that consume no attempt can still occupy the record
to the TTL. TTL expiry blocks new authorization only; an authorized invocation waits to `rpc_deadline =
send_start + datanode.client.timeout`, where `send_start` is the first poll, so scheduler delay can place it
after `ttl` (existing option, default 10s). **Implementation prerequisite:** confirm the hook corresponding to
send-start; neither authorization nor response may substitute. Record TTL and per-RPC timeout are distinct; a
timeout leaves the invocation `IssueUnknown` and does not prove non-execution, and success after the deadline
still settles `applied`.

**Failure progression.** `consecutive_failures` counts consecutive results that prevent the record from
completing and require rescheduling or a rebuild. It is not an RPC failure count: observing an RPC result does
not by itself decide the increment, and a terminal outcome does not create a later increment. It drives backoff
and `next_ready`, is inherited by successors, and is not an RPC error metric. Observed success ends the record,
so no reset is observable and no successor is created; a rebuild successor inherits the settled counter.
Backoff is exponential with jitter, capped at 30s.

| Path | Counter | Successor | Invocation charge | `next_ready` |
| --- | --- | --- | --- | --- |
| Retryable RPC error | +1 | no | +1 (`Issued`) | backoff |
| `RetryAfterRouteRefresh` | +1 | yes, route-refresh rebuild | +1 (`Issued`) | backoff |
| `TopologyRefresh` | +1 | yes, topology rebuild | 0 (`NotIssued`) | backoff |
| Lookup error | +1 | no | 0 (`NotIssued`) | backoff |
| No leader | implementation prerequisite | no | 0 (`NotIssued`) | deferral; policy pending |
| Transient reservation failure | n/a | no | 0 (`NotIssued`) | dead-letter `transient_full` |
| Admission rejection | n/a, no record | no | n/a (no record) | n/a |
| Non-retryable RPC error | n/a, terminal | no | +1 (`Issued`) | dead-letter |
| Observed success | n/a; record ends | no | +1 (`Issued`) | n/a |

`Invocation charge` is the current invocation's budget effect, not cumulative `attempts`; the latter lives in
the snapshot and is inherited, never reset by settlement.

## Routing and Layout Changes

A target region is resolved from `TableRouteCache` after all lookups. A missing table, id mismatch,
contradictory target, or changed plan-time `TableIdent.version` dead-letters `table_changed` with no RPC and no
attempt; a route lookup error or a leaderless region defers as described in Retry Scheduling (`NotIssued`,
consuming TTL but no attempt). A region absent from the physical route rebuilds as `TopologyRefresh` without RPC
or attempt; `RetryAfterRouteRefresh`, including a partition-expression mismatch, also rebuilds.

Rebuild invalidates routes and builds a `TablePartitioner`. The current `rebuild_write_requests` returns a fully
materialized `Result<Vec<RegionRequest>>`, `rebuild_insert_requests` builds the full `Vec`, and
`RowSplitter::split` materializes all destination rows with no early stop. A streaming bounded helper is an
implementation blocker: it must enforce `MAX_SUCCESSORS_PER_RECORD` (provisional 256) and the transient
reservation before materializing the peak, and never call the unbounded insert path.

## Failure Handling and Client Contract

Final settlement is `applied` when success was observed, `unknown_remote_outcome` when non-application cannot be
proven, or `rebuilt` when at least one admitted successor carries its rows, with unadmitted rows lost; `rebuilt`
is not `applied` and not client-visible. No error proves non-application: `UnacknowledgedWrite` may have applied
and carries `RetryAfterRouteRefresh`, and `NonRetryable` is also unknown; prior replays never imply `applied`.

| Terminal path | Outcome | Current invocation charge | Reason |
| --- | --- | --- | --- |
| Replay success | `applied` | `Issued`, +1 | Replayed rows |
| TTL expired with cell `Empty` | `unknown_remote_outcome` | `NotIssued`, 0 | `ttl_expired_before_attempt` |
| Stopped with cell `IssueUnknown`, no result | `unknown_remote_outcome` | `IssueUnknown`, +1 | `rpc_timeout` / `rpc_cancelled` / `worker_panicked` / `rpc_unknown` |
| Retryable after deadline | `unknown_remote_outcome` | `Issued`, +1 | `ttl_expired_after_attempt` |
| Attempts exhausted before invocation | `unknown_remote_outcome` | `NotIssued`, 0 | `attempts_exhausted` |
| Table changed or transient full | `unknown_remote_outcome` | `NotIssued`, 0 | `table_changed` / `transient_full` |
| Non-retryable response | `unknown_remote_outcome` | `Issued`, +1 | `non_retryable` |
| Panic after observed RPC error, before settlement | `unknown_remote_outcome` | `Issued`, +1 | `worker_panicked` |
| Rebuild done or interrupted after admissions | `rebuilt` | +1 iff `Issued`; else 0 | successors admitted; `rebuild_interrupted` |
| Rebuild failed before admitting a successor | `unknown_remote_outcome` | +1 iff `Issued`; else 0 | `rebuild_failed` / `rebuild_interrupted` |
| Drain with cell `Empty` | `unknown_remote_outcome` | `NotIssued`, 0 | `shutdown` |

`IssueUnknown`'s four reasons record the event, not the remote outcome; other unobserved stops map to
`rpc_unknown`. The drain row covers only an invocation that never crossed the boundary, and `NotIssued, 0`
never resets historical `attempts`; every other cell follows the Queue Invariants shutdown table, and this
table is a terminal-path summary, not the shutdown state machine.

A rejected successor is not a record: `successor_rejected_total` increments and its rows are lost. The client
keeps the original error; replay is compensation, not acknowledgment, and `affected_rows` keeps its applied
semantics. Drain removes remaining unsettled records; `Succeeded`, `Settled`, and published `Rebuilding`
records are not drained.

## Flow Semantics

The Flow mirror snapshot is captured in `Inserter::do_request` before `dispatcher.prepare`; replay never
re-captures or replaces it. Admission eligibility lookup is read-only: it never mutates or replaces the
snapshot and may only reject admission, so later cache state narrows eligibility only. No cross-Flow-version
consistency is claimed; the routing table snapshot is a distinct input.

# Configuration and Observability

```toml
[write_queue]
enable = true
max_bytes = "512MiB"
max_records = 100000
max_record_size = "512KiB"
max_transient_bytes = "128MiB"
ttl = "5m"
max_attempts = 3
replay_concurrency = 8
```

`enable = true` is the proposed default for the completed and validated implementation; `enable = false`
selects the existing inline path. Configuration is loaded at startup with no hot-reload path, so changing
`enable` or the queue limits requires a restart. Close is the only in-process lifecycle transition, not a
dynamic disable.

Configuration rejects zero bounds, `max_record_size > max_bytes`, a `max_transient_bytes` above the counter
ceiling, and a zero `datanode.client.timeout`; it does not clamp. A `max_transient_bytes` below one maximum-size
reservation is accepted with `TransientFull`; `max_attempts = 3` adequacy is validation, not a guarantee.

Metrics use the `greptime_write_queue_` prefix without table/region labels: `admitted_total`,
`shed_total{reason}`, `charged_bytes`, `charged_records`, `transient_reserved_bytes`,
`replay_invocations_total{issue_state}`, `rebuild_total{result,successors}`, `dead_letters_total{reason}`,
`settled_total{outcome,replay_scope}`, `successor_rejected_total`, `drained_total{reason,outcome}`,
`replay_latency_seconds`, and `inflight`. `issue_state` is `issued`, `issue_unknown`, or `not_issued`; a
`not_issued` sample classifies a zero-attempt invocation, not an RPC attempt. `replay_scope` uses one
deterministic, risk-preserving label: `replay_issue_unknown` if any replay crossed the boundary without an
observed result (a later observed invocation does not erase it), otherwise `replay_issued` if any result was
observed, otherwise `replay_not_sent`; `replay_not_sent` is local queue replay state only and does not mean the
original dispatcher round sent nothing or that the request never reached a datanode.

`replay_latency_seconds` measures one queue replay invocation: from the boundary commit that publishes
`IssueUnknown` to that invocation's classification (observed response, `rpc_deadline` timeout, cancellation, or
panic after the boundary). It excludes admission, lookups, backoff and scheduling wait before the boundary, and
settlement or rebuild after classification; invocations that never cross the boundary have no sample. Exact
timer hooks are an instrumentation detail to confirm. It is not client response, admission, observed-success,
or terminal-settlement latency.

# Consistency

**Ordering.** Enqueue FIFO is per `(Frontend queue, RegionId)` as defined in Record and Retry Work Model, with
at most one in-flight record; a failed head blocks only its key while other keys continue. Successors join their
destination key; no order is promised across keys, across Frontends, or across old keys that rebuild into one
destination region (R3). Lineage `(sequence, record id)` is correlation only and does not order scheduling.

**Newer live writes.** The queue orders admitted records only against each other; a live write reaching a key
after admission is not detected or fenced, so older rows can replay after it; no per-key epoch exists.

**DDL and stale replay.** Replay compares the captured `TableIdent.version` with the routing target's version;
it is a timing window, not an atomic fence. `ALTER` can race between check and RPC, cache hits are not
revalidated, `TRUNCATE` does not bump the version, `DROP` appears as a missing table, a recreated table has a
new id, and `UNDROP` restores metadata. The queue adds a window where the client sees the error, runs DDL such
as `TRUNCATE`, and an earlier-admitted replay applies old rows; inline retry would finish first.

**Duplicates.** A lost response, a timeout that applied, a panic after submission, a rebuild, or a client retry
can duplicate rows; no idempotency key, deduplication, or exactly-once protocol exists.

# Implementation Plan

None of these prerequisites is complete, and each blocks release, including a default-on release.

1. **Dispatcher handoff.** Preserve retryable `RoutedWrite`s through `merge_attempts`/`should_retry`; capture
   failures at `max_retries = 0`.
2. **Version propagation.** Carry plan-time `TableIdent.version` through `prepare`, `send_batch`, peer merging,
   `split_request`, and `finish`; the sidecar is unimplemented.
3. **Admission and record.** Implement eligibility, bounded admission lookup, cancellation, the `encoded_len()`
   ledger, per-region insertion, lineage metadata, and independent per-sibling admission; retain `RoutedWrite`.
4. **Worker and scheduling.** Implement the one-shot invocation, lookup deadlines, gate, the authoritative
   snapshot and its single-publish transition, TTL, backoff, the no-leader deferral policy, and lease check.
5. **Bounded rebuild.** Add the streaming helper enforcing successor and transient limits before
   materialization; no unbounded fallback.
6. **Completion and shutdown.** Implement the six cells, supervisor recovery from the snapshot, at-most-once
   settlement, close, join, and drain.
7. **Configuration and metrics.** Implement validation, metrics, and startup configuration.

# Implementation Validation and Testing

These are acceptance criteria, not results; no measured bound is claimed.

- **Default and release gate.** `enable = true` ships only after every prerequisite and criterion here passes;
  `enable = false` preserves inline retry and client behavior; startup configuration is validated at load time.
- **Admission and error contract.** Independent per-sibling admission; capture at `max_retries = 0`; an
  admission-lookup timeout rejects only that record; cancellation before commit leaves no record; rejection
  returns the original error with no inline attempt or sibling suppression.
- **Issue state and budget.** Gate pass, boundary crossing, drop-before-poll, timeout, cancellation, panic;
  panic before and after the boundary commit yields zero and one charge; one charge per boundary crossing,
  including before the first poll, zero for `NotIssued`; `IssueUnknown` to `Issued` or `Succeeded` never
  recharges; TTL versus budget; `max_attempts = 3` adequacy under prolonged restarts.
- **Recovery and shutdown.** Six cells; snapshot recovery preserves classification through `Rebuilding` and
  `Settled`; `Ok`-to-`Succeeded` has no intervening fallible step; `Empty` versus `IssueUnknown` close handling;
  `Issued` shutdown waits for no `rpc_deadline`; partial successor admission; no double rebuild or settlement;
  no replay of `Succeeded`; drain scope.
- **Version and rebuild.** The plan-time version survives the handoff and a changed version dead-letters
  `table_changed` with no RPC or attempt; the bounded helper enforces limits before materialization.
- **No-leader deferral.** Sustained no-leader keeps `NotIssued`, sends no RPC, adds no attempt, avoids
  high-frequency lookups, produces an executable `next_ready`, ends at TTL, and resumes scheduling when a
  leader appears without affecting other keys.
- **Memory and transient factor.** Measure replay and rebuild memory across column shapes and fan-out; fix the
  formula and factor source; validate provisional sizes and `replay_concurrency`; verify checked arithmetic and
  RAII-only release.
- **Client response latency.** Request start to original client-error return, separately for disabled inline
  retry, enabled with admission success, and enabled with admission rejection; do not pool. The provisional
  target is p99 no more than 5% worse than disabled only for admission-success, under the same failure
  injection and workload; the rejection arm has no 5% claim.
- **Admission latency.** Admission start to commit or rejection, by request size, fan-out, and outcome.
- **Time to observed success.** Admission to observed replay success, as a distribution; it is not compared
  with the disabled baseline, which has no background replay.
- **Time to terminal settlement.** Admission to `applied`, `unknown_remote_outcome`, or `rebuilt`, by outcome; a
  fast post-admission client error is not replay performance. Also validate lookup deadlines, the
  `TopologyRefresh`/`RetryAfterRouteRefresh` split, lock contention at 1, 8, and 64 writers, success-path
  metadata cost, and `ttl` restart.
- **Metrics boundaries.** `replay_latency_seconds` samples only boundary-to-classification for one replay
  invocation and is not one of the four latencies above; `replay_not_sent` does not mean the original request
  never sent.
- **Exclusion boundary.** Both pending-rows batchers flush `BulkInsert`
  (`src/servers/src/batcher/logical_table/region_write.rs:240`,
  `src/servers/src/batcher/table/batch.rs:75`); Instant TTL, delete, and Enterprise Flow children are
  unreachable or shed.

# Risks

- **Default-on release.** Enabling by default changes retry timing and resource use for all eligible writes; it
  is not equivalent to default-on inline retry. Releasing before the prerequisites and validation pass is
  unsupported; `enable = false` limits blast radius but does not substitute for validation.
- **Admission rejection loses the retry opportunity.** Saturation, admission limits, or lookup failure reject a
  sibling; it gets no queue replay and no inline fallback, so the automatic retry opportunity is lost, the
  client may receive the error earlier than inline retry would have, and rejection does not prove
  non-application.
- **Process-local loss.** Restart, crash, TTL, or close can lose admitted rows after the client saw a failure;
  retention is best-effort.
- **Duplicates and unknown application.** Lost responses, timeouts, rebuilds, fan-out, and client retries can
  duplicate rows; an error never proves non-application.
- **DDL and stale replay.** `ALTER`, stale cache, `TRUNCATE`, `DROP`/recreate, `UNDROP`, and newer writes can
  change the final value; residual risk is R1.
- **Fan-out budget.** Each successor consumes its own inherited budget; no lineage-wide aggregate exists, so
  aggregate RPCs grow within other limits.
- **No admission visibility.** The client cannot tell whether a failed write was admitted, replayed, or applied.
- **Memory is not RSS.** Accounting does not bound the allocator, tonic, compression, runtime, connections, or
  caches.
- **Bounded rebuild is blocked.** The current path materializes all destination rows, so limits apply after the
  peak.
- **Shutdown gaps.** Teardown can outlast `rpc_deadline`; supervisor panic, an unjoinable task, or an invalid
  cell has no final-settlement guarantee.
- **Ordering.** A successor can execute after newer records; one in-flight record per key bounds but does not
  eliminate inversion; cross-Frontend and merge order are undefined.

# Questions for Review

**R1. Is the disclosed DDL and stale-replay risk acceptable for v1 without an atomic fence?**

The v1 design has no atomic DDL fence and documents the ALTER, TRUNCATE, DROP/UNDROP, and newer-write windows.
Is this residual risk acceptable for v1, or is stronger DDL coordination required before adoption?

**R2. Is insert-only scope acceptable as the v1 boundary?**

This RFC covers ordinary inserts only; Delete and BulkInsert remain excluded. The earlier RFC and current PR
description still mention delete, so if this boundary is accepted they need separate synchronization. This is a
scope confirmation, not a technical unknown in this RFC.

**R3. Is the per-key-only FIFO boundary acceptable for v1?**

The queue guarantees FIFO within one `RegionId` in one Frontend with at most one in-flight record. It does not
define cross-key order, cross-Frontend order, or merge order when multiple old keys rebuild into one destination
region. Accept this boundary rather than adding a merge-ordering mechanism?

# Future Work

Durable retention and recovery with region-side idempotency; deduplication or idempotency keys; delete and other
excluded paths; a region-side DDL fence, newer-write detection, and stale-replay protection; unit-level rebuild
accounting; Flow consistency; fairness, adaptive pacing, a queryable dead-letter store, manual replay, and
worker restart. No schedule implied.

# Alternatives

- **Inline retry plus a post-retry buffer.** Keeps two schedulers and budgets on one write; rejected for the
  enabled path.
- **Encoded envelope decoded per replay.** Adds an encode, a decode, and a second payload; `RoutedWrite` and
  `encoded_len()` accounting are retained.
- **One record with a multi-target work set.** Needs multi-RPC attempts and a unit-completion protocol; Core v1
  keeps independent successors.
- **A separate crate with a durable-shaped `QueueStore`.** No v1 caller.
- **A bounded channel plus a writer task.** Adds an admitted-but-not-stored state and failure paths; direct
  commit is a short critical section.
- **Region-side DDL fence.** Correct, but adds proto/manifest/region work before the core is validated; Future
  Work.
