---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Updated: 2026-11-16
Author: @lczllx
Status: Draft for discussion
---

# Summary

This RFC proposes an in-memory, per-Frontend retry queue for ordinary inserts (including eligible Prometheus
remote write) failing per entry in the first dispatcher round (`Error::PartialRegion`);
`WriteDispatcher::finish` admits before returning the client error.

The proposed default for the completed and validated implementation is `enable = true`, matching the existing
policy of enabling retry by default while explicitly changing the retry lifecycle and admission-rejection
behavior; `enable = false` is the runtime kill switch. The default does not permit shipping an incomplete
implementation: the prerequisites and validation below block release. When enabled the queue owns
the retry lifecycle, so admission rejection does not re-enter the inline loop and cannot start a second retry
scheduler; within a key at most one record is in flight in enqueue order, lineage is not an execution guarantee,
and multi-key merge order is unresolved (R4).

# Motivation

The dispatcher retries a failed per-region write inline with `write_retry.max_retries = 3`, jittered backoff,
and no per-attempt endpoint timeout on the Frontend-to-datanode channel: roughly 10.5s to under 21s plus RPC
time. A restart, migration, or partition can outlast that, and a client without a per-region view duplicates or
drops regions. Issue #8085 asks for a bounded Frontend buffer after the first round: an admitted record keeps
retrying in the queue, while a rejected one returns the original client error without the inline wait. With
`enable = false` the existing inline behavior is unchanged.

# Goals

One bounded queue per Frontend; a failed first round never returns `Ok`; reuse `write_retry` backoff for
retryable errors, `TableRouteCache`, `TablePartitioner`, and `RetryAfterRouteRefresh`; no second payload copy;
one lookup set and
RPC deadline per invocation; supervisor-owned close, join, and drain; no response-shape or `affected_rows` change.

# Non-Goals

No durability, restart recovery, exactly-once, or at-least-once; no error proves non-application; no first-round
timeout rescue, live-write ordering, or atomic DDL fence; no record-level "sent"/"not sent" claim; no RSS or
allocation bound; no new wire field; no Flow consistency. Rejected admission gets no inline fallback or
automatic compensation. Delete, `BulkInsert` (both pending-rows batchers), and the other excluded paths stay out
of scope.

# Design

## Architecture

The queue is an operator-layer component owned by the Frontend and shared by the builder's dispatchers; it never
calls `WriteDispatcher::execute`, `finish`, `WriteRetry::retry`, or a second dispatcher round. Successful writes
take no queue lock, allocate no record, and skip admission. With `enable = false` admission never runs and the
inline path is used unchanged.

`finish` merges the round, selects the client error, and, when the queue is enabled, admits each eligible
retryable sibling independently before returning. Admission cannot change the selected error — the first
terminal failure in flattened per-region order, otherwise the first retryable failure — and a rejected sibling
is not queued and does not re-enter inline retry. The queue uses `max_attempts`, not `write_retry.max_retries`;
per-entry capture is installed when `max_retries > 0` or the queue is enabled.

## Record and Retry Work Model

A record retains target `region_id`, `RoutedWrite` (`{ peer, request: Arc<RegionRequest> }`), the monotonic
admission `deadline`, plan-time `TableIdent.version`, `partition_expr_version`, `may_have_applied`,
`consecutive_failures`, lineage `(sequence, record id)`, and one lease state snapshot `{ phase, invocation,
attempts }`. Retaining `RoutedWrite` keeps its `Arc` and allocation with no encode or second payload copy.
`may_have_applied` is a conservative admission-time assumption from the failed first round, not proof of
issuance or application and not a lifecycle phase. `attempts` exists only inside that snapshot and is the
record's authoritative cumulative replay budget; no second counter is maintained. No confirmed-issue counter is
normative; `attempts` is the only replay budget.

The lineage sequence is unique within one Frontend queue lifetime and identifies descent; the record id is
unique per record in that queue, and a successor keeps the parent's sequence with a later id. Neither value
orders scheduling: sequence/id are metadata for lineage correlation, metrics, and debugging, with no FIFO,
deduplication, fencing, or budget authority.

`TableIdent.version` (logical), `TableInfoValue.version` (metadata revision), and `partition_expr_version` are
distinct; a physical repartition need not bump the logical version, and only a partition-expression mismatch
through route refresh or rebuild catches it. Planning captures `TableInfo.ident.version` and preserves it
through `prepare`, `send_batch`, peer merging, and `finish`; admission and replay never re-capture it.
**Implementation prerequisite (unverified):** `RegionRequestHeader` cannot carry the sidecar.

Settlement charges the parent's final invocation once. A successor inherits post-settlement `attempts`,
`consecutive_failures`, the original deadline, and the lineage sequence, starts a lease at `Empty` /
`NotIssued` with that inherited cumulative `attempts` (never the parent's transient phase or classification),
and computes `next_ready` from the inherited failure count. A route-refresh rebuild records the triggering
invocation's observed `Issued` classification (already charged) and keeps it through `Rebuilding`; a topology
rebuild keeps `NotIssued` with no charge. Each record gets a fresh queue-local id.

## Admission

Admission runs in `finish` only when the queue is enabled, after client-error selection and before the error is
returned; it cannot change the selected error or response shape. A sibling is eligible only if it is
`Body::Inserts` with one `InsertRequest` from the per-entry `PartialRegion` branch, has consistent ids,
`flow_source` false, targets `mito`/`mito2`/`metric`, is not append mode or instant TTL, has `owners: None`, the
queue is open, and the request fits the admission limits (Memory Accounting). Eligibility may `await` bounded
table and Flow cache reads under a provisional 5s `admission_lookup_deadline`, distinct from the replay lookup
deadline and requiring validation; timeout or failure rejects admission for that record without altering the
client error, re-entering inline retry, or affecting other siblings, and these reads are read-only and never
replace the captured Flow snapshot. Reservation and commit then happen under the state lock after a final open
and limit check, with no `await` or user code. An eligible sibling is admitted independently; a terminal,
ineligible, or rejected sibling is never queued and gets no automatic later compensation. Admission is
best-effort, not durability or acknowledgment; a whole-RPC failure, including a single-entry tonic error, is not
admitted.

**Runtime disable versus commit.** Queue-enabled state and admission commit synchronize through the same
authoritative mechanism. Admission may `await` lookups outside the lock, but the final locked section re-checks
`enabled` before reservation and commit. If disable linearizes before commit, no record is committed and no
lease is created; if commit linearizes before disable, the record is admitted and continues under the queue
policy. No commit may occur after disable takes effect. A request already in the admission path when disable
wins does not fall back to inline retry: it commits no record, keeps the original client error, and starts no
second scheduler; requests that reach `finish` after disable use the inline path. This is runtime transition
behavior, not an admission failure mode.

## Queue Invariants and Recovery

**Recovery authority.** Supervisor recovery reads one authoritative, recoverable lease-state snapshot:
`phase` (one of the six lifecycle phases), `invocation` (the current lease's `NotIssued` / `IssueUnknown` /
`Issued` classification), and `attempts` (the record's cumulative queue replay budget). The three fields are
committed and read as one unit, and `attempts` has no independent second copy; any counter elsewhere is
derived. `phase` selects the recovery and shutdown action, `invocation` preserves the current lease's budget
classification across later phases, and `attempts` is the authoritative cumulative count. A phase alone never
decides whether the current invocation was charged.

A new lease starts at `Empty` / `NotIssued` and inherits the record's cumulative `attempts`; `Empty` does not
mean zero history. The boundary transition is the only one that changes `attempts`; every other transition
preserves the existing charge. The worker publishes `IssueUnknown` immediately before the first poll, crossing
the conservative boundary; it proves neither transmission nor remote execution. `Issued` is published when an
invocation result (response or RPC error) is observed; it proves observation, not datanode receipt or execution.

| Phase | Meaning | Invocation classification | Budget effect |
| --- | --- | --- | --- |
| `Empty` | First poll not entered | `NotIssued` | current invocation not charged |
| `IssueUnknown` | Boundary crossed; may have started | `IssueUnknown` | current invocation charged once |
| `Issued` | Result observed | `Issued` | existing charge retained |
| `Succeeded` | Success observed; never replay | `Issued` | success invocation already charged |
| `Rebuilding` | Rebuild published; never rebuild twice | classification of the triggering path | no new RPC charge |
| `Settled` | Terminal settlement completed | classification that caused settlement | historical charge untouched |

Transitions preserve classification except at the boundary and at an observed result: `Empty` → `Rebuilding`
keeps `NotIssued`; `IssueUnknown` → `Rebuilding` first records the observed `Issued` (a
`RetryAfterRouteRefresh`) and keeps it; `Empty` → `Settled` keeps `NotIssued`; `IssueUnknown` → `Settled` keeps
`IssueUnknown`; `Issued` → `Settled` keeps `Issued`; `Rebuilding` → `Settled` keeps its saved classification;
`Succeeded` → `Settled` keeps `Issued`. After `Ok`, the worker's first action is to publish `Succeeded` /
`Issued`; that publication takes no allocation or other fallible step, and an abort during it recovers from the
last committed snapshot (`IssueUnknown` or `Issued`), never as success.

A published `Rebuilding` decision is atomic and never executed twice; admitted successors survive a later parent
panic, unadmitted rows are lost, and the parent is removed once: `rebuilt` if any successor was admitted,
otherwise `unknown_remote_outcome` with `rebuild_interrupted`. Settlement happens at most once: the worker
settles while holding the lease, and the supervisor recovers from the snapshot alone.

Close rejects new admission, leases, and send authorization under the state lock and stops scheduling. A task
stopped before the gate is at `Empty` / `NotIssued` and releases without joining; a task past the gate is
joined and handled by its snapshot:

| Phase / classification | Shutdown behavior |
| --- | --- |
| `Empty` / `NotIssued` | No boundary crossed: release lease and permit; the record remains for drain |
| `IssueUnknown` / `IssueUnknown` | RPC may have started: await the in-flight future to completion or `rpc_deadline`; never reclassify as `NotIssued`; settle `unknown_remote_outcome` when the result cannot be observed |
| `Issued` / `Issued` | Result observed: join the worker's classification, rebuild, or settlement; do not wait for `rpc_deadline` |
| `Succeeded` / `Issued` | Never replay; complete `applied` settlement |
| `Rebuilding` / saved | Never execute rebuild twice; resume from the published decision and keep its saved classification |
| `Settled` / saved | No second settlement |

`rpc_deadline` bounds only the wait for a future whose result is not observed; it is not extra wait for
`Issued`. The supervisor never re-issues an RPC for an `Issued` invocation, and worker or supervisor
cancellation never downgrades `IssueUnknown` to `NotIssued`.

## Memory Accounting

`max_bytes`, `max_records`, and `max_record_size` bound retained estimates and count, not RSS: the charge is the
retained `RegionRequest`'s `encoded_len()` plus fixed `RoutedWrite`/`Arc` overhead, computed from the existing
request without creating a second full payload. Reserved, stored, and leased records all count against
`max_records`; a reservation converts without a second charge, and the count is released only on removal.
`max_record_size` is checked before a record is reserved; an oversized request is rejected through the existing
admission rejection path, enters no queue, sends no replay RPC, and consumes no attempt. It cannot prevent the
first-round allocation, which already exists at admission; `max_bytes` covers retained records only, and neither
bounds transient materialization or RSS.

`max_transient_bytes` bounds replay and rebuild reservations, separate from retained accounting and not an RSS
bound. The reservation object, formula, and factor source are implementation prerequisites: charge each
transient materialization once, avoid double-charging retained records, use checked arithmetic so overflow fails
closed, release only through an RAII guard, and treat a failed reservation as `TransientFull` with no RPC, no
attempt, and dead-letter `transient_full`. Whether replay and rebuild share one factor is undecided; either
choice needs an explicit policy validated against representative request sizes, concurrency, and rebuild peak.
The current rebuild path materializes the full `Vec` before limits apply, so the peak is unbounded until the
streaming helper exists, and the queue must not be released until that policy and its measurements exist.
Provisional values (`512KiB`, `128MiB`, `replay_concurrency = 8`, successor limit 256) remain unvalidated.

## Retry Scheduling

The three invocation classifications and their mapping to phases are defined in Queue Invariants. Authorization
never charges. An invocation crossing the `IssueUnknown` boundary consumes one replay attempt; the charge is
deliberately conservative and applies even if a panic prevents the first poll.

`attempts` counts queue replay budget only, never the original dispatcher round, a scheduling pass, a lookup
retry, a send authorization, or a guaranteed datanode arrival. The boundary transition is the charge
linearization point: it constructs and publishes the complete next snapshot — `phase = IssueUnknown`,
`invocation = IssueUnknown`, and the incremented cumulative `attempts` — with no panic, `await`, allocation,
user code, or other fallible step between the start of the commit and its completion. Recovery observes either
the complete pre-commit snapshot or the complete post-commit snapshot, never a partial update; holding a lock
while assigning several fields is not sufficient by itself unless a partially written state is discarded.
**Implementation prerequisite:** the record representation must provide this single-publish guarantee; until it
does, the linearization argument is unmet. No separate mutable field may duplicate the charge, and the worker
and supervisor must not derive or overwrite competing counters.

Panic behavior follows from the contract: a panic before the commit leaves `Empty` / `NotIssued` with no charge
for this invocation; a panic after the commit but before the first poll leaves `IssueUnknown` / `IssueUnknown`
with exactly one charge for this invocation, and it is not resent; an observed RPC error moves to `Issued` /
`Issued` with no second charge, and a later classification, rebuild, or settlement panic keeps `Issued`; an
observed `Ok` publishes `Succeeded` / `Issued` with the existing charge and is never retried. A future that
produces no result and is cancelled or panics stays `IssueUnknown`; non-execution is never inferred.
`attempts >= max_attempts` blocks further replay; concurrent replay invocations are bounded by
`replay_concurrency`.

**Lookups.** All table and route lookups happen before authorization, bounded by a provisional 5s replay
`lookup_deadline`; failure reinserts with backoff while open and consumes no attempt. Admission lookup has its
own bounded deadline.

**No-leader deferral.** A leaderless region never crosses the `IssueUnknown` boundary, consumes no attempt, and
keeps the current invocation `NotIssued`. The record stays under the TTL, must not busy-loop or bypass TTL
through repeated lookups, and must produce an executable `next_ready`. The concrete deferral policy, including
whether it increments `consecutive_failures`, is an implementation prerequisite; no backoff value or new
configuration is fixed here.

**Send authorization gate.** The gate runs once per invocation under the `QueueState` lock. The same critical
section checks the queue is open, the monotonic clock has not reached the deadline, the lease is valid, and
`attempts < max_attempts`; only then is `send_authorized` set. Close takes the same lock, so close and
authorization serialize: if close wins, every later gate fails and sends no RPC; if authorization wins, the
invocation is an authorized task that shutdown joins and handles by its snapshot. `send_authorized` is gate state
only, not proof of issuance, and adds no attempt. No `await`, lookup, or other suspending step may sit between a
successful gate and the first poll, and the gate cannot eliminate scheduler delay before that poll. An
invocation authorized before the deadline stays authorized even if the deadline passes before its first poll;
later TTL expiry neither revokes authorization nor reclassifies it.

**TTL and budget.** TTL is the maximum record age from admission on a monotonic clock; successors inherit it and
rebuild never resets it. `max_attempts = 3` counts queue replays only, so budget and TTL are independent and
either may end retries first. With the existing `write_retry` backoff, consecutive retryable RPC failures
normally exhaust three replays well before `ttl = 5m`; lookup failure, no-leader, and `NotIssued` outcomes
consume no attempt but can occupy the record to the TTL, so no fixed completion bound is claimed. TTL expiry
blocks a new authorization only; an authorized invocation waits to its `rpc_deadline = send_start +
datanode.client.timeout` (`send_start` is the first poll; existing option, default 10s, not a global change).
Record TTL and per-RPC timeout are distinct and never merged. A timeout leaves the invocation `IssueUnknown`
and does not prove non-execution; success after the record deadline still settles `applied`.

**Failure progression.** Backoff is exponential with jitter, computed from `consecutive_failures` and capped at
30s.

| Path | `consecutive_failures` | Successor | Invocation charge | `next_ready` |
| --- | --- | --- | --- | --- |
| Retryable RPC error | +1 | no | +1 (`Issued`) | backoff |
| `RetryAfterRouteRefresh` | +1 | yes, route-refresh rebuild | +1 (`Issued`) | backoff |
| `TopologyRefresh` | +1 | yes, topology rebuild | 0 (`NotIssued`) | backoff |
| Lookup error | +1 | no | 0 (`NotIssued`) | backoff |
| No leader | implementation prerequisite | no | 0 (`NotIssued`) | deferral; policy pending |
| Transient reservation failure | n/a | no | 0 (`NotIssued`) | dead-letter `transient_full` |
| Admission rejection | n/a, no record | no | n/a (no record) | n/a |
| Non-retryable RPC error | n/a, terminal | no | +1 (`Issued`) | dead-letter |
| Observed success | n/a, record removed | no | +1 (`Issued`) | n/a |

`Invocation charge` describes only the current invocation's budget effect; it is not the record's cumulative
`attempts`, which lives in the authoritative snapshot and is inherited by a successor, never reset by
settlement. An invocation stopped at the boundary is `+1 (IssueUnknown)` in the terminal table. No-leader
constraints are in Retry Scheduling. A retryable result after the deadline settles `ttl_expired_after_attempt`
instead of scheduling.

## Routing and Layout Changes

A target region is resolved from `TableRouteCache` after all lookups. A missing table, id mismatch,
contradictory target, or changed plan-time `TableIdent.version` dead-letters `table_changed` with no RPC and no
attempt; a route lookup error or a region without a leader defers as described in Retry Scheduling
(`NotAttempted`, `NotIssued`, consuming TTL but no attempt). A region absent from the physical route rebuilds as `TopologyRefresh` without RPC or attempt; a
`RetryAfterRouteRefresh`, including a partition-expression mismatch, rebuilds. The version check compares the
captured planning version with the routing target; it is a timing window, not an atomic DDL fence.

Rebuild invalidates routes and builds a `TablePartitioner`. The current `rebuild_write_requests` returns a fully
materialized `Result<Vec<RegionRequest>>`, `rebuild_insert_requests` builds the full `Vec`, and
`RowSplitter::split` materializes all destination rows with no early stop. A streaming bounded helper is an
implementation blocker: it must enforce `MAX_SUCCESSORS_PER_RECORD` (provisional 256) and the transient
reservation before materializing the peak, and never call the unbounded insert path.

## Failure Handling and Client Contract

Final settlement is `applied` when success was observed, `unknown_remote_outcome` when non-application cannot be
proven, or `rebuilt` when at least one admitted successor carries its rows, with unadmitted rows lost; `rebuilt`
is not `applied` and not client-visible. No error proves non-application: `UnacknowledgedWrite` may have applied
and carries `RetryAfterRouteRefresh`, and `NonRetryable` is also unknown. A record with prior replays is never
inferred `applied` unless success is observed.

| Terminal path | Outcome | Current invocation charge | Reason |
| --- | --- | --- | --- |
| Replay success | `applied` | `Issued`, +1 | Replayed rows |
| TTL expired with phase `Empty` | `unknown_remote_outcome` | `NotIssued`, 0 | `ttl_expired_before_attempt` |
| Stopped with phase `IssueUnknown`, no result | `unknown_remote_outcome` | `IssueUnknown`, +1 | `rpc_timeout` / `rpc_cancelled` / `worker_panicked` / `rpc_unknown` |
| Retryable after deadline | `unknown_remote_outcome` | `Issued`, +1 | `ttl_expired_after_attempt` |
| Attempts exhausted before invocation | `unknown_remote_outcome` | `NotIssued`, 0 | `attempts_exhausted` |
| Table changed or transient full | `unknown_remote_outcome` | `NotIssued`, 0 | `table_changed` / `transient_full` |
| Non-retryable response | `unknown_remote_outcome` | `Issued`, +1 | `non_retryable` |
| Panic after observed RPC error, before settlement | `unknown_remote_outcome` | `Issued`, +1 | `worker_panicked` |
| Rebuild done or interrupted after admissions | `rebuilt` | +1 iff `Issued`; else 0 | successors admitted; `rebuild_interrupted` |
| Rebuild failed before admitting a successor | `unknown_remote_outcome` | +1 iff `Issued`; else 0 | `rebuild_failed` / `rebuild_interrupted` |
| Drain before first poll (phase `Empty`) | `unknown_remote_outcome` | `NotIssued`, 0 | `shutdown` |

The current-invocation charge column records that invocation's classification and whether it was charged;
cumulative `attempts` lives in the authoritative snapshot and is never reset by settlement. The four reasons in
the `IssueUnknown` row record the event, not the remote outcome; other unobserved stops map to `rpc_unknown`.
The drain row covers only an invocation that never crossed the boundary: `NotIssued, 0` applies to that
invocation and never resets historical `attempts`. Shutdown for every other phase follows the state table in
Queue Invariants; this table is a terminal-path summary, not the shutdown state machine.

A rejected successor is not a record: `successor_rejected_total` increments and its rows are lost. A parent
stopped at the successor limit or deadline after admitting a successor is `rebuilt`; before admitting one it
takes the TTL reason matching its phase. The client keeps the original error; replay is compensation, not
acknowledgment, and `affected_rows` keeps its applied semantics. Drain removes remaining unsettled records;
`Succeeded`, `Settled`, and published `Rebuilding` records are not drained, and a `Rebuilding` parent settles by
the rebuild rows above.

## Flow Semantics

The Flow mirror snapshot is captured in `Inserter::do_request` before `dispatcher.prepare`; replay never
re-captures or replaces it. Admission eligibility lookup is read-only, never mutates or replaces that snapshot,
and may only reject admission: it cannot make an ineligible request eligible or change the snapshot used by the
original request, so later cache state narrows eligibility only. No cross-Flow-version consistency is claimed;
the routing table snapshot is a distinct input.

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

`enable = true` is the proposed default for the completed and validated implementation; it is not permission to
release before the prerequisites and validation pass. `enable = false` selects the existing inline path. A
runtime switch from true to false stops new admission under the commit race contract in Admission: already
admitted records keep their policy, TTL, budget, and scheduling; authorized invocations are not cancelled; new
eligible writes use inline retry; the switch neither re-admits records nor resets deadlines. Runtime disable is not close: close also rejects leases
and send authorization and enters the supervisor drain protocol (Queue Invariants). **Implementation
prerequisite:** live configuration reload; until it exists, this paragraph defines the required behavior.

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
observed, otherwise `replay_not_sent`; the last says nothing about the original dispatcher round.

# Consistency

**Ordering.** Scheduling within one key follows queue enqueue FIFO within one Frontend, with at most one
in-flight record; a failed head blocks its key while other keys continue. The lineage metadata defined above is
correlation only: a successor is appended at the tail and may carry an older sequence than records ahead of it,
so it can execute after newer records; strict lineage FIFO is not promised. There is no global or
cross-Frontend order; `prepare` groups peers in a `HashMap`, so cross-peer order is per-run; streams rebuilt
from multiple old keys into one destination region have no defined merge order (R4).

**Newer live writes.** The queue orders admitted records only against each other; a live write reaching the key
after admission is not detected or fenced, so older rows can replay after it, and no per-key epoch exists.

**DDL and stale replay.** Replay compares the captured `TableIdent.version` with the routing target's version;
it is a timing window, not an atomic fence. `ALTER` can race between check and RPC, cache hits are not
revalidated, `TRUNCATE` does not bump the version, `DROP` appears as a missing table, a recreated table has a
new id, and `UNDROP` restores metadata. The queue adds a window where the client sees the error, runs DDL such
as `TRUNCATE`, and an earlier-admitted replay applies old rows; inline retry would finish first. This is the
risk R1 asks the maintainer to accept.

**Duplicates.** A lost response, a timeout that applied, a panic after submission, a rebuild, or a client retry
can duplicate rows; no idempotency key, deduplication, or exactly-once protocol exists.

# Implementation Plan

None of these prerequisites is complete, and they block release, including a default-on release; until they and
the validation below pass, `enable = true` must not ship.

1. **Dispatcher handoff.** Preserve retryable `RoutedWrite`s through
   `merge_attempts`/`should_retry`; capture failures at `max_retries = 0`.
2. **Version propagation.** Carry plan-time `TableIdent.version` through `prepare`,
   `send_batch`, peer merging, `split_request`, and `finish`; the sidecar is unimplemented.
3. **Admission and record.** Implement eligibility, bounded admission lookup, cancellation,
   the `encoded_len()` ledger, per-region insertion, lineage metadata, independent
   per-sibling admission, and the runtime-disable re-check at commit while retaining
   `RoutedWrite`.
4. **Worker and scheduling.** Implement the one-shot invocation, lookup deadlines, gate,
   the authoritative phase/classification/attempts snapshot and its single-publish transition,
   TTL, backoff, the no-leader deferral policy, and lease check.
5. **Bounded rebuild.** Add the streaming helper enforcing successor and transient limits
   before materialization; no unbounded fallback.
6. **Completion and shutdown.** Implement the six phases, supervisor recovery from the
   snapshot, at-most-once settlement, close, join, and drain.
7. **Configuration and metrics.** Implement validation, metrics, and live enable reload; the
   transient-factor policy and measurements are required before release.

# Implementation Validation and Testing

These are acceptance criteria, not results; no measured bound is claimed.

- **Default and release gate.** The completed implementation defaults to `enable = true` only
  after every prerequisite and criterion here passes; `enable = false` preserves the existing
  inline retry path and client behavior. A runtime switch to `enable = false` rejects new
  admission, leaves admitted records running, and does not cancel authorized invocations.
- **Admission and error contract.** Independent per-sibling admission; capture at
  `max_retries = 0`; admission lookup timeout rejects only that record; cancellation before
  commit leaves no record; a rejected admission returns the original error with no inline
  attempt; unchanged error selection.
- **Issue state and budget.** Gate pass, boundary crossing, drop-before-poll, timeout,
  cancellation, panic; panic immediately before and after the boundary commit yields zero and
  one charge respectively; one charge per invocation crossing the boundary, including a panic
  before the first poll, and zero for `NotIssued`; `IssueUnknown` to `Issued` and to `Succeeded`
  never recharge; TTL versus budget; `max_attempts = 3` adequacy under prolonged restarts.
- **Recovery and shutdown.** Six phases; snapshot recovery preserves classification through
  `Rebuilding` and `Settled`; the `Ok`-to-`Succeeded` write has no intervening fallible step;
  `Empty` versus `IssueUnknown` close handling; `Issued` shutdown does not wait for
  `rpc_deadline`; partial successor admission; no double rebuild; no replay of `Succeeded`;
  no double settlement; drain scope.
- **Runtime disable race.** Commit before disable admits and continues; disable before commit
  produces no record; disable during the lookup `await` is re-checked at commit; requests after
  disable use inline retry; disable does not cancel authorized RPCs, does not empty admitted
  records, and remains distinct from close.
- **Version and rebuild.** The plan-time version survives the handoff and a changed version
  dead-letters `table_changed` with no RPC or attempt; the bounded helper enforces limits
  before materialization.
- **No-leader deferral.** Sustained no-leader keeps `NotIssued`, sends no RPC, adds no attempt,
  does not repeat lookup at high frequency, produces an executable `next_ready`, still ends at
  TTL, and resumes normal scheduling when a leader appears without affecting other keys.
- **Memory and transient factor.** Measure replay and rebuild memory across column shapes and
  multi-region fan-out; fix and document the formula and factor source; validate provisional
  sizes and `replay_concurrency`; verify checked arithmetic and RAII-only release.
- **Client response latency.** Measure request start to original client-error return separately
  for the disabled inline-retry baseline, enabled with admission success, and enabled with
  admission rejection; do not pool them. The provisional acceptance target is p99 no more than
  5% worse than the disabled baseline for the admission-success path under the same failure
  injection and workload; the rejection path is reported without a 5% claim.
- **Replay settlement latency.** From successful admission to `applied`, `unknown_remote_outcome`,
  or `rebuilt`, report time-to-observed-success and time-to-terminal-settlement separately; a
  fast post-admission client error is not evidence of replay performance. Also validate lookup
  deadlines, the `TopologyRefresh`/`RetryAfterRouteRefresh` split, lock contention at 1, 8, and
  64 writers, success-path metadata cost, admission cost, and `ttl` restart.
- **Exclusion boundary.** Both pending-rows batchers flush `BulkInsert`
  (`src/servers/src/batcher/logical_table/region_write.rs:240`,
  `src/servers/src/batcher/table/batch.rs:75`); Instant TTL, delete, and Enterprise Flow
  children are unreachable or shed.

# Risks

- **Default-on release.** Enabling by default changes retry and memory behavior for all eligible
  writes and is not equivalent to the existing inline retry; releasing before prerequisites and
  validation pass is unsupported. `enable = false` limits blast radius but does not substitute
  for validation.
- **Admission rejection loses the retry opportunity.** Queue saturation, admission limits, or a
  lookup failure reject a sibling; it gets no queue replay and no inline fallback, so an
  automatic retry opportunity is lost, and the client can receive the original error earlier
  than the inline path would have returned.
- **Runtime transition window.** An admission already awaiting lookups when disable wins commits
  no record and gets no inline fallback, so that request loses the automatic queue retry; the
  window is bounded by the admission lookup deadline.
- **Process-local loss.** Restart, crash, TTL, or close can lose admitted rows after the client
  saw a failure; retention is best-effort.
- **Duplicates and unknown application.** Lost responses, timeouts, rebuilds, fan-out, and
  client retries can duplicate rows; an error never proves non-application.
- **DDL and stale replay.** `ALTER`, stale cache, `TRUNCATE`, `DROP`/recreate, `UNDROP`, and
  newer writes can change the final value; residual risk is R1.
- **Fan-out budget.** Each successor consumes its own inherited budget; no lineage-wide
  aggregate exists, so aggregate RPCs grow within other limits.
- **No admission visibility.** The client cannot tell whether a failed write was admitted,
  replayed, or applied.
- **Memory is not RSS.** Accounting does not bound the allocator, tonic, compression, runtime,
  connections, or caches.
- **Bounded rebuild is blocked.** The current path materializes all destination rows, so limits
  apply after the peak and the bound is not real.
- **Shutdown gaps.** Teardown can outlast `rpc_deadline`; supervisor panic, an unjoinable task,
  or an invalid phase has no final-settlement guarantee.
- **Ordering.** A successor can execute after newer records; one in-flight record per key
  bounds but does not eliminate inversion; cross-Frontend and multi-key merge order are
  undefined.

# Questions for Review

**R1. Is the disclosed DDL and stale-replay risk acceptable for v1 without an atomic fence?**

The v1 design does not provide an atomic DDL fence. It documents the remaining ALTER, TRUNCATE, DROP/UNDROP, and newer-write windows. Is this trade-off acceptable for v1, or is stronger DDL coordination required before adoption?

**R2. Is insert-only scope acceptable for v1?**

This RFC covers ordinary inserts only. Delete and BulkInsert remain excluded. The earlier RFC and current PR description still mention delete and need separate synchronization if this scope is accepted.

**R3. How should ordering be defined when multiple old keys rebuild into one destination region?**

Options are a merge ordering rule, gating the new destination region until rebuilt records land, or explicitly accepting that no FIFO is promised across the merge.

# Future Work

Durable retention and recovery with region-side idempotency; deduplication or idempotency keys; delete and other
excluded paths; a region-side DDL fence, newer-write detection, and stale-replay protection; unit-level rebuild
accounting; Flow consistency; fairness, adaptive pacing, a queryable dead-letter store, manual replay, and
worker restart. No schedule implied.

# Alternatives

- **Inline retry plus a post-retry buffer.** Keeps two schedulers and budgets on one write;
  rejected for the enabled path.
- **Encoded envelope decoded per replay.** Adds an encode, a decode, and a second payload;
  `RoutedWrite` and `encoded_len()` accounting are retained.
- **One record with a multi-target work set.** Needs multi-RPC attempts and a unit-completion
  protocol; Core v1 keeps independent successors.
- **A separate crate with a durable-shaped `QueueStore`.** No v1 caller.
- **A bounded channel plus a writer task.** Adds an admitted-but-not-stored state and failure
  paths; direct commit is a short critical section.
- **Region-side DDL fence.** Correct, but adds proto/manifest/region work before the core is
  validated; Future Work.
