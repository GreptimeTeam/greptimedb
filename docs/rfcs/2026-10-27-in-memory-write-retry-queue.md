---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
Status: Draft
---

## Summary

This RFC proposes an optional, per-Frontend in-memory retry queue for eligible transient write failures that may outlast the existing inline retry window. v1 covers distributed Frontends and ordinary direct inserts targeting partitioned, Mito-backed physical Regions through the non-tracked direct-insert path. Prometheus remote write is eligible only when its actual execution path meets the same requirements. Standalone, tracked writes, metric logical-row writes, metric logical-batcher flushes, pending-rows/table-batcher flushes, Delete, BulkInsert, and Flight `DoPut` remain outside v1.

The retry policy for a direct-insert call is selected before its first write RPC. A call is queue-owned only when every Region attempt is eligible; otherwise the entire call retains its existing retry path where supported. The first-round report preserves per-Region results and provenance before batch aggregation or another inline retry. Admitted failures are owned by the queue; a queue-owned admission rejection is fail-closed and receives no inline fallback. First-round successes are never replayed.

Each admitted record remains bound to its original table identity, source `RegionId`, and plan-time `partition_expr_version`. Replay may refresh peer/leader routing but may resend only to that same Region. If the captured table, Region, or partition identity is no longer valid, replay stops; v1 does not repartition or redirect rows.

The queue bounds locally retained records and per-record replay work through retained-byte/record limits, a per-record invocation cap, an absolute retry deadline, and Frontend-wide replay concurrency. These controls do not bound the cumulative number of remote operations that may remain unresolved after local timeouts. First-round uncertainty and replay settlement remain separate. The queue is process-local, disabled by default, and provides best-effort retry rather than durability, restart recovery, or delivery guarantees.

## Motivation

With the default `write_retry.max_retries = 3`, the existing inline retry path permits up to three additional retry rounds with jittered exponential backoff; deployments may configure a different count. The configured maximum caps the pre-jitter base delay rather than the final scheduled wait. The Frontend-to-Datanode channel does not impose a per-request timeout, so the wall-clock duration of a first-round RPC is not bounded and may remain unresolved indefinitely. Queue ownership begins only after the first-round report is available; the queue-side replay timeout does not bound that initial RPC.

The initial `write_queue.retry_budget = 5m` provides an additional replay window beginning at admission. Backoff, route lookup, metadata refresh, and leaderless deferral all consume this budget; deployment-specific recovery and backlog distributions must be measured before enablement.

A Datanode restart, failover, or leader change may outlast the inline retry window. A write RPC can also fail after the request reaches the Datanode but before the Frontend receives a definitive per-Region result. Retrying such a request may duplicate a write, while dropping it may leave part of a multi-Region insert unapplied. A client-visible batch error therefore does not imply that no rows were written.

A routed insert is tied to the table identity, target Region, and partition expression used to map rows. If repartitioning or another metadata change moves those rows to a different Region, replaying the original Region request is no longer sufficient. v1 deliberately stops replay when the captured target is no longer valid rather than rebuilding the request against a new partitioner.

This RFC covers only the in-memory portion of a bounded Frontend write buffer. Durable retention, restart recovery, and cross-Region repartition recovery require separate designs.

## Goals and Non-goals

### Goals

- Extend retry handling for eligible transient insert failures beyond the existing inline retry window.
- Select one retry policy for the whole direct-insert call before the first RPC; never switch a queue-owned failure back to inline retry.
- Preserve per-Region outcomes, exact routed requests, and first-round failure provenance before batch-level result merging.
- Bound queue-retained state, each record's retry duration and invocation count, and local replay concurrency.
- Define client-visible behavior for mixed batches, partial admission, uncertain outcomes, and bounded completion waiting.
- Replay a retained request only while its captured table and same-Region routing identity remain valid.

### Non-goals

- Durable queue storage, process-restart recovery, or guaranteed eventual delivery.
- Exactly-once or at-least-once write semantics, or proof that an unknown RPC was not applied.
- A Datanode protocol change for idempotency or deduplication.
- Cross-Region repartition recovery, row repartitioning, FanOut, or successor records. If the original Region or partition-expression identity is no longer valid, v1 stops replay rather than redirecting the request.
- Queue use in Standalone mode or for calls using `confirmed_writes` / `execute_tracked`.
- Replacing retry behavior for Delete, BulkInsert, Flight `DoPut`, metric logical-batcher flushes, or writes flushed by pending-rows/table batchers. These paths remain outside the queue.
- Queue admission for Metric logical-row inserts that still target logical Metric-engine Regions.
- Queue admission for append-mode tables, instant-TTL tables, Flow-source tables, Regions with `partition_expr_version = None` (including default Regions in partitioned tables), or requests missing required plan-time context.
- Ordering across Frontends, against concurrent live writes or legacy-inline attempts, or proof of remote application order after an invocation becomes `issue_unknown`.
- An atomic DDL fence, a dead-letter query API, hot configuration reload, or per-table queue allowlisting.
- Recovery from process-level loss or corruption of the queue's coordination state.

## Request Scope and Retry Ownership

### Supported paths and retry-owner selection

The handoff occurs after first-round outcomes have been collected for a supported direct-insert request. Eligibility is based on the plan-time target metadata and normalized per-Region insert attempts, not solely on the external protocol or caller name. The retry policy for the entire call must be selected before its first write RPC so that a mixed-eligibility batch is not partially assigned to the queue and partially left on the legacy path.

| Request path | v1 behavior when `write_queue.enable = true` |
|---|---|
| Direct ordinary inserts dispatched through the non-tracked `execute_with_progress` / existing `WriteRetry` path, including SQL, gRPC, pipeline, COPY, and Prometheus remote write only where the actual call site uses this path | Queue ownership only when every Region attempt in the call is eligible; retryable first-round failures are handed to the queue before the existing retry loop runs |
| Direct inserts whose context selects `confirmed_writes` and therefore `execute_tracked` | Outside v1; retain the existing tracked retry path and `WriteCompletion` behavior |
| Direct log/trace inserts on the supported non-tracked path targeting non-append-mode Mito physical Regions | Eligible only if all ordinary queue rules are met |
| Automatically created log/trace primary tables and exemplar tables whose effective Region `append_mode` is true | Queue-ineligible; remain on their existing write path |
| Trace auxiliary tables such as services/operations | Evaluate the effective Region `append_mode` and all other eligibility rules; do not exclude by table name alone |
| Direct insert call containing any Region attempt known at planning time to be ineligible because of table shape, engine, append mode, instant-TTL mode, Flow-source status, missing context, or another unsupported condition | Keep the entire call on its existing retry path where supported |
| Any Region with `partition_expr_version = None`, including a default Region in a partitioned table | Queue-ineligible in v1; no synthetic partition version is introduced |
| Metric logical-row insert that still targets a logical Metric-engine Region | Not queue-eligible in v1; remains outside this queue |
| Metric-engine logical-batcher flush | Outside this queue; retain its existing behavior |
| Bulk insert, Flight `DoPut`, and pending-rows/table-batcher flush | Outside this queue; retain their existing behavior |
| Delete | Outside this queue; retain its existing behavior |
| Standalone deployment | Outside v1; this queue is not enabled or used |

Eligibility is determined by the actual producer path and captured Region options, not by the protocol or table name alone. Log/trace primary and exemplar tables with effective append mode enabled are excluded; trace auxiliary tables are evaluated using their own Region options. `ctx.confirmed_writes()` continues to select `execute_tracked`; queue ownership therefore uses a separate explicit per-call policy representing whole-call eligibility, selected before the first write RPC.

- **Queue policy:** every Region attempt in the call is a direct ordinary insert to a supported partitioned Mito physical Region, uses the supported non-tracked direct-insert path, and satisfies every queue eligibility rule below. The queue owns retryable first-round failures after classification; those attempts are not passed to `WriteRetry::retry`.
- **Existing retry policy:** any Region attempt is known before the first write RPC to be outside queue eligibility, or the call uses `execute_tracked`. The entire call remains with its existing retry owner where that path supports retry. This includes append-mode, instant-TTL, Flow-source, unsupported-engine, `partition_expr_version = None` (including default Regions), missing-context, preflight-oversized, and tracked-path calls.
- **Fail-closed queue admission:** once the call is assigned queue policy, an attempt rejected for retained-capacity exhaustion or queue closure, or whose supposedly valid queue sidecar is found malformed or inconsistent at admission, receives no inline fallback.

Admission must produce an observable result for every retryable Region attempt, distinguishing successful publication from capacity rejection, queue closure, and invalid admission context. The API may batch these results, but a unit return value or log-only rejection is insufficient: the caller must know whether every eligible attempt was admitted before it can return `Ok`. Sibling attempts may be admitted independently, but any rejected queue-owned attempt prevents a successful call result.

The queue is a first-round retry owner, not a buffer that runs only after the existing `WriteRetry` loop exhausts its retry count. Enqueuing from the post-exhaustion give-up path does not satisfy this v1 contract.

The selected policy must remain explicit when producers share internal insert-building logic. `WriteDispatcher` must not hold a dispatcher-global queue mode: insert, bulk insert, Delete, and batcher callers share dispatcher infrastructure, so policy must be supplied explicitly by the supported call path. Queue admission must explicitly accept only supported insert request bodies; Delete must never be admitted even if it shares dispatcher or queue plumbing. A batcher must not inherit queue ownership merely because it reuses an ordinary insert helper. If a producer cannot preserve this distinction through its call path, it remains outside v1.

### Frontend-wide queue ownership

When enabled, exactly one `WriteRetryQueue` instance is created for a distributed Frontend and owned by that Frontend runtime. The supported non-tracked direct-insert producer receives its queue reference from the runtime. `WriteDispatcher` remains policy-neutral: sharing a dispatcher does not give Delete, BulkInsert, batcher, or tracked-write callers queue ownership, and other dispatcher instances do not create independent queues. The single instance owns the Frontend-wide retained-byte, record-count, sequence, and replay-concurrency limits. Queue shutdown is tied to the Frontend runtime lifecycle.

Calls routed through `execute_tracked` remain outside the queue and retain their existing `WriteCompletion` / `WriteProgress` behavior. That aggregate completion contract is not used as a per-Region admission or queue-settlement report. Supporting tracked calls in a later version would require a separate per-Region failure handoff and queue-settlement contract.

### Queue-mode first-round behavior

Queue mode is a distinct per-call retry policy on the supported non-tracked direct-insert path, not merely the existing no-retry dispatcher mode. It must preserve first-round results, exact routed requests, and failure provenance before the legacy retry loop can consume, merge, or discard them.

Queue mode must produce a lossless first-round report before batch-level aggregation or any additional inline retry can consume or discard the per-Region outcomes. Partial-result collection is an explicit part of queue policy and must not depend on whether a legacy retry object is present. The report must preserve retryable siblings even when another Region has a terminal failure, and it must retain the original per-Region `RoutedWrite`, stable `attempt_order`, owner/attribution data where present, propagated `RetryHint`, and failure provenance.

The send boundary must distinguish a definitive per-entry result, a peer-batch RPC failure without per-entry results, and a single-entry failure whose result was collapsed by the send path. An error wrapper or request count alone is not sufficient to infer this provenance.

The required conceptual interface is a lossless first-round report produced once per non-tracked call. It contains one outcome for every Region attempt actually sent, in stable `attempt_order`, separately represents a pre-dispatch failure when no RPC was sent, and records the selected client error. Each attempt outcome retains:

- the stable order assigned when the dispatcher creates the Region attempt, before peer batching;
- whether the attempt was applied or failed;
- the original per-Region `RoutedWrite` when recoverable;
- owner/attribution data when present;
- the propagated `RetryHint` (`Retryable`, `RetryAfterRouteRefresh`, or `NonRetryable`); and
- explicit send-boundary provenance (`PerRegionPartial`, `PeerBatchRpcUnknown`, or `SingleEntryCollapsed`).

The report also carries a call-level `pre_dispatch_error` for prepare/no-leader failures when no first-round RPC was sent, and the selected client error. These are call-level fields, not Region outcomes. If the request or provenance cannot be recovered for an attempt, that attempt is marked unadmittable rather than guessed. The report is an internal handoff contract; exact Rust type and field names are implementation details.

`attempt_order` is assigned monotonically when the dispatcher creates per-Region attempts and is preserved through send and result collection. It is not based on RPC completion order or peer-batch order. The selected error is the pre-dispatch error when no RPC was sent; otherwise it is the terminal failure with the lowest `attempt_order`, or, if there is no terminal failure, the failure with the lowest `attempt_order`. The invariant is that no batch-level merge or additional `WriteRetry::retry` round occurs before the lossless report is formed and queue ownership has been decided.

Admission rules for the report are:

- `Applied` is never admitted or replayed.
- A failure with `retry_hint = Retryable` or `RetryAfterRouteRefresh` and `PerRegionPartial` provenance may be admitted as one Region record.
- A `PeerBatchRpcUnknown` failure with a retryable propagated `RetryHint` may be expanded into one record per original split only when the exact per-Region `RoutedWrite` values and peer-batch provenance are retained. Every such record is explicitly marked as first-round outcome unknown. This can replay a Region that the original peer RPC already applied; duplicate-capable behavior is intentional and must be disclosed.
- A `SingleEntryCollapsed` failure with a retryable propagated `RetryHint` may be admitted only if the send boundary preserves the exact original `RoutedWrite` and explicit collapsed-result provenance. If those facts are not available, the attempt is not admitted.
- A terminal failure is not admitted. Retryable siblings remain visible and may be independently admitted even when a terminal sibling determines the client error.
- A queue-owned failure whose request or provenance cannot be recovered is returned as a failed/unadmitted outcome. It receives no inline fallback; the queue must not guess which Region or application state the error represents.

The report applies only to the supported non-tracked direct-insert path. The handoff must occur after first-round collection and before `WriteRetry::retry` begins additional rounds; enqueueing only from the post-exhaustion give-up path is not v1 queue ownership.

The queue retains the original materialized per-Region request. It must not re-enter insert planning or re-evaluate impure defaults. Queue retention must not encode/decode the request or create a second full payload copy; any encoded-size calculation is an accounting estimate only.

### Eligibility and plan-time context

Queue admission is evaluated independently for each failed single-Region attempt. An attempt may be assigned queue ownership only when it:

- is one ordinary physical-table insert request;
- targets a supported Mito-backed physical Region on a distributed Frontend;
- carries a valid plan-time snapshot consistent with the target table and Region;
- has a non-empty `partition_expr_version` for the target used in the first-round request;
- has effective `append_mode = false` in the target Region's `RegionOptions`;
- is not instant-TTL mode;
- was not observed as a Flow source in the plan-time Flow cache;
- is not known before the first RPC to exceed `write_queue.max_record_size`; and
- retains the fully materialized per-Region request and all required plan-time context.

Any Region whose captured `partition_expr_version` is `None` is excluded in v1. This includes unpartitioned Regions and default Regions in otherwise partitioned tables. Mito skips the in-band partition-version check for `None`; an empty value therefore cannot serve as a replay fence. A table may later gain or change a partition expression without a corresponding `TableIdent.version` change. v1 does not synthesize a version for an empty expression. Supporting these Regions later requires an explicit state representation and a replay-time validation mechanism that detects the relevant partition-layout transition.

Impure defaults are materialized into ordinary insert rows before dispatch, and the retained request contains those exact rows. Replay resends the materialized request without re-entering insert planning or default filling.

For admission, v1 conservatively excludes attempts where an impure default affects a primary-key column or the time index. Impure defaults affecting only ordinary non-key, non-time-index columns do not independently make an attempt ineligible. This marker is captured from the planning metadata and carried with each Region request; a missing or ambiguous marker makes the attempt ineligible.

Missing, malformed, or mismatched plan context discovered before the first round makes the attempt queue-ineligible and leaves it on its existing retry path where supported. If an attempt was selected for queue ownership but its sidecar is found malformed or inconsistent at admission, it is rejected as `UnsafeContext` and does not fall back to inline retry. Unsupported shape or engine is `TableShapeIneligible`; capacity exhaustion and queue closure are separate queue-admission rejections.

The snapshot must be captured from the table metadata and plan context already available during insert planning, before request grouping and splitting can discard the association between a table and its Region attempts. It must carry, at minimum, the `TableId`, `TableIdent.version`, target engine, effective `RegionOptions.append_mode`, instant-TTL state, the observed `TableFlownodeSetCache` Flow-source status, whether an impure default affects a primary-key/time-index column, and the target Region's `partition_expr_version`. The Flow-source status is a cache observation, not an atomic metadata generation; no `TableIdent.version` fence for Flow changes is assumed. The snapshot must remain associated with each resulting per-Region request after peer batching and failure extraction. Admission must use this snapshot rather than adding awaited metadata/cache lookups to the request failure path. A later metadata read at admission cannot replace the captured snapshot: it would lose the distinction between the table state used for the first write and a later DDL change.

Flow mirroring remains part of the original request lifecycle and is not replayed; the retained request contains only the physical Region write. Flow-source writes are therefore queue-ineligible. Eligibility uses the plan-time `TableFlownodeSetCache` observation, which may be stale until event-driven invalidation and is not fenced by `TableIdent.version`. A stale-positive observation causes false exclusion; a stale-negative observation can incorrectly admit a Flow-source write and replay it without the corresponding mirror operation. An unavailable or ambiguous cache observation makes the request ineligible, but the current snapshot cannot detect every stale-negative case. Enablement requires explicit maintainer acceptance of this residual risk or a reliable freshness/generation check; deployments that cannot tolerate it must keep the queue disabled.

Append mode is captured from the effective `RegionOptions.append_mode` used by the target Region, not inferred solely from table name or a generic table-level option. Log/trace primary tables and exemplar tables with append mode enabled are excluded; trace auxiliary service/operation tables are evaluated using their own Region options. If the effective setting is unavailable or ambiguous, the attempt is not selected for queue ownership.

Queue replay also needs access to the metadata and cache-invalidation capabilities required to refresh table identity and same-Region routing, plus the existing `write_retry` backoff parameters. These capabilities and values must be available independently of whether legacy retry is enabled. Queue-mode partial-result collection and replay scheduling must not depend on `self.retry.is_some()` or on construction of the legacy `WriteRetry` object.

Admission is the ownership boundary for queue-owned attempts. Before publication, the queue does not own the attempt. After publication, the queue retains responsibility until the record retry deadline or a terminal outcome. Publication makes the record and its retained charge visible together; neither may become visible independently. The publication transaction is the linearization point: temporary capacity/byte reservations and all fallible allocations are completed before commit; under queue-state protection, the commit rechecks that the queue remains open and the request has not been cancelled, assigns a monotonically increasing queue sequence, and either publishes the complete record/charge/cell state or publishes nothing. For records targeting the same Region, FIFO order is the order of successful publication sequence assignment under queue-state protection. It is not guaranteed to match client arrival order, first-round RPC dispatch order, or SQL row-input order when concurrent requests race during admission. If cancellation is observed before a record's commit point, that record is not published and its temporary reservations are released. Multi-record admission is not one all-or-nothing transaction: records already published remain queue-owned if a later sibling is rejected or cancellation aborts a not-yet-published sibling. If cancellation races with an individual record's commit point, whichever operation is observed first by the commit arbitration determines that record's result; after publication, later cancellation ends only the client wait.

The first-round RPC has already been sent by this stage, so aborting admission does not roll it back or prove that it was not applied; it simply means no queue retry will follow. First-round report construction, queue-side classification, and admission must be bounded local operations. Admission must not wait for queue capacity, network I/O, metadata refresh, or replay-worker scheduling. All per-attempt publish/reject decisions are resolved before request completion begins its queue-settlement wait. The call-local completion group already owns the handles returned by successful publication; there is no waiter-registration step after publication. For a positive completion-wait budget, the timer starts after all admission decisions are complete and the group has collected the admission results. Existing cell state covers records that settled before the wait starts. With a zero budget, the same handles provide an immediate settlement snapshot without blocking. Request cancellation before a record's publication commit aborts that record's admission, while cancellation after publication detaches the completion group and ends only the client wait.

### First-round outcomes and admission

| First-round outcome | Queue/retry action | Retry owner |
|---|---|---|
| Success / observed `applied` | No record | None |
| `PerRegionPartial` with `RetryHint::Retryable` or `RetryHint::RetryAfterRouteRefresh`, retained Region request, and valid queue sidecar in a queue-owned call | Admit independently if capacity is available | Queue if admitted; otherwise none |
| `PeerBatchRpcUnknown` with a retryable propagated `RetryHint`, exact original Region splits, and explicit peer-batch provenance retained | Admit one unknown-outcome record per retained Region split; preserve the original peer-RPC uncertainty | Queue per admitted attempt; otherwise none |
| `SingleEntryCollapsed` with a retryable propagated `RetryHint`, exact original Region request, and explicit collapsed-result provenance retained | Admit the single Region attempt under the same capacity rules | Queue if admitted; otherwise none |
| Whole-RPC or collapsed failure whose original Region request/provenance cannot be recovered | Do not guess or infer; return the original selected error without queue admission | None |
| Retryable failure for a call classified as queue-ineligible before the first RPC | Continue through the existing retry path where supported | Existing retry owner |
| Initial prepare/no-leader failure before any RPC | No queue record; preserve the existing pre-dispatch error path | Existing caller/path behavior |
| Terminal failure with no retained retryable Region request | No record | None |
| Queue-owned attempt rejected for capacity, queue closure, or invalid sidecar discovered at admission | Reject without waiting for queue capacity; no inline fallback | None |

A whole-peer-RPC failure does not prove that any split was unapplied. If the dispatcher can retain each exact original `RoutedWrite`, v1 may enqueue those Region requests as separate records with `PeerBatchRpcUnknown` provenance; replay can duplicate a write that the first RPC already applied. If it cannot preserve the split requests and provenance, it must not manufacture queue entries. `issue_unknown` remains reserved for a queue replay invocation that passed send authorization but whose result was not observed; first-round uncertainty is stored separately as `FirstRoundProvenance`. The provenance must be attached at the send boundary before error wrapping or batch-level merge, not reconstructed from `BatchRequestSnafu` or the number of requests.

### Selected client error and mixed batches

The selected client error is determined from the lossless first-round report before queue admission. When no RPC was sent, it is the `pre_dispatch_error`. Otherwise, a terminal first-round failure takes precedence; if there is no terminal failure, the selected error is the retryable failure with the lowest stable `attempt_order`. This order is assigned before peer batching and is independent of RPC completion timing. It is a dispatcher Region-attempt order, not a promise to reproduce the original row-input order.

Queue admission success or failure does not change the selected error. Each eligible retryable attempt in a queue-owned call is admitted independently to its destination Region FIFO. If a queue-owned attempt is rejected, the request returns the selected error even when sibling attempts are admitted. There is no inline fallback for an attempt already assigned to queue ownership because that would bypass per-Region queue scheduling, queue limits, and single-owner retry semantics.

## Client-Visible Contract

For a queue-owned call, the request path creates one call-local completion group after the first-round report and before the first record publication. Before publishing any record, the group reserves handle slots for all candidate Region attempts; if this allocation fails, no record is published and the queue-owned candidates are rejected without inline fallback. For each candidate, publication first reserves its record/byte capacity and allocates the record, stateful completion cell, and move-only RAII handle under a temporary reservation. Under queue-state protection it rechecks cancellation and queue closure, assigns the FIFO sequence, commits the reservation, and publishes the record with its pending cell. The queue record and caller handle share that cell. No allocation, callback, or `await` occurs after the shared state is committed; the handle is placed in its pre-reserved group slot and returned as part of the admission result. If the call future is dropped before the handle is stored in the group, the handle's drop path detaches the observer. A worker may settle the record immediately, but the cell stores the terminal settlement and applied-row count until the caller consumes or detaches it. A fast replay therefore cannot complete and disappear before its result becomes observable. Rejected candidates receive no queue-owned completion cell.

The group contains at most one handle per admitted Region record. Across the Frontend, each live cell and its caller handle are charged to the corresponding record and remain counted until both the request payload and caller-observable completion state are released. The group adds a handle only when a record is successfully published, so the number of live handles is bounded by the Frontend-wide `max_records` ledger. `max_records` therefore bounds payload-retaining records plus settled records whose completion result is still held for a caller. The queue retains the cell only while the caller is attached; a drop guard detaches all handles when the caller returns, cancels, or times out. Pending replay records continue independently after detachment.

The completion-wait budget applies only to queue-owned calls. After every admission decision is complete, a call with no terminal first-round failure, every eligible retryable attempt admitted, no completion cell already at a final non-`applied` settlement, at least one pending completion cell, and a positive `write_queue.completion_wait` waits against one monotonic deadline: `completion_deadline = wait_start + write_queue.completion_wait`. Planning, preflight, initial route/leader preparation, first-round RPC execution and waiting, first-round report construction, candidate classification, admission, and cell creation are excluded. The deadline limits only the subsequent client wait; it does not cancel replay, an in-flight RPC, or queue work. The first-round Frontend-to-Datanode RPC has no per-request channel timeout and can therefore keep the original request pending indefinitely before queue ownership begins.

Wait termination is fail-fast and deterministic:

- If no terminal first-round failure or admission rejection exists and every admitted record settles as `applied`, return `Ok`.
- If any record reaches a final queue settlement other than `applied` (`issue_unknown`, `observed_failure`, `stale_target`, or `not_sent`), return the selected first-round error immediately; other admitted records continue in the background.
- If records remain pending and none has a final non-`applied` settlement, continue waiting until all are applied or the completion deadline expires. Deadline expiry returns the selected first-round error; it does not settle or cancel pending records.
- If a terminal first-round failure exists, any eligible attempt is rejected, or `completion_wait = 0`, do not block. Take an immediate consistent snapshot, return the selected error when one exists, and let admitted records continue independently.

Completion cells retain their state as well as notifying: the caller reads the stored state and waits only while it remains pending, so settlement before the wait begins cannot lose a notification. A terminal settlement is written to the cell before its observer charge can be released. When the caller detaches, the queue drops its cell reference; if the payload was already released, the completion-only entry is removed. A pending record whose caller detached keeps only the request and bounded invocation history needed for replay.

`Ok` for a queue-owned call means every admitted queue record was observed as `applied` in the response snapshot, every eligible retryable attempt was admitted, and no terminal first-round failure existed. A record may have `first_round_disposition = first_round_unknown` and later be observed applied by replay; this permits `Ok` but does not prove the original invocation was not also applied. Any final queue settlement other than `applied`, a rejected queue-owned attempt, a terminal first-round failure, or wait-deadline expiry returns the selected first-round error. Queue replay errors remain in bounded record history and diagnostics; they do not replace the selected client error.

`affected_rows` is a response-time snapshot. It includes first-round successes and queue replay successes observed before the response is constructed; a replay that succeeds later is not included. It is not an exact logical row total and may not account for duplicate application. Client cancellation, timeout, or response detaches the call-local group but does not delete, settle, cancel, or reassign admitted records. Orderly queue shutdown stops new authorizations, finalizes remaining records from their histories, and wakes attached completion cells; it does not drain the queue or prove that in-flight remote requests were cancelled. Calls assigned to the existing retry path and calls using `execute_tracked` retain their existing cancellation, `WriteCompletion`, and result behavior.

## Queue and Retry Semantics

### Records and invocations

A **record** represents one admitted single-Region attempt. It retains the original routed request, first-round provenance and first-round disposition, captured table and Region identity, absolute retry deadline, per-record invocation budget, per-record backoff state, a fixed-capacity invocation history, and (while the caller remains attached) one completion cell containing its final replay settlement and applied-row count. Its first-round disposition is either `observed_retryable_failure` (a definitive per-Region failure result) or `first_round_unknown` (the original peer-batch or collapsed single-entry result did not establish whether this Region was applied). This disposition is immutable and remains separate from replay invocation outcomes and record settlement. v1 creates no successor records: a record remains bound to its original `RegionId` and captured `partition_expr_version` for its entire lifetime.

An **invocation** is one replay RPC that has passed the queue's send authorization. The invocation's `in_flight` state must be recorded before the RPC is issued; the queue must not send first and record authorization afterward. Its outcome is one of:

- `in_flight`;
- `applied`;
- `observed_retryable_failure`;
- `observed_terminal_failure`; or
- `issue_unknown`.

Before send authorization, replay completes the required table, route, leader, and client lookups. Under queue state protection it then checks that the queue is open, the record is still owned by the current worker, and the record deadline and per-record invocation cap permit another send. It records the invocation as `in_flight` before releasing that protection. The RPC is issued immediately afterward, without an intervening `await` or additional lookup. Send authorization is a local ownership/accounting boundary, not proof that the Datanode received the request.

The recovery contract has two observable checkpoints:

1. **Replay ownership adopted:** the queue records the worker claim for the record. Failure before send authorization creates no invocation; the record is finalized from its existing history and is not automatically restarted.
2. **Send authorization recorded:** the queue records `in_flight` before calling the RPC. From this point onward, worker failure or loss of the response is conservatively classified as `issue_unknown`, even if the failure occurred in the narrow interval before the transport call was actually issued. This avoids treating an authorized attempt as definitely unsent.

The outcomes after `in_flight` are terminal for that invocation. A late response does not rewrite an invocation already classified as `issue_unknown`. A record may authorize a new, distinct invocation while an earlier invocation is `issue_unknown`, provided its deadline and per-record invocation budget still permit it. The earlier invocation is never resent as though it had not been sent, and the later invocation does not clear the earlier unknown. This permits duplicate-capable retry and is not an exactly-once mechanism.

### Record settlement

A record's terminal status is a pair of independent facts: **replay settlement** and **first-round disposition**. Replay settlement summarizes only queue-authorized invocations. First-round disposition preserves whether the original first-round Region result was definitive or unknown. Neither dimension proves remote application order.

| Replay settlement | Meaning |
|---|---|
| `applied` | At least one queue replay invocation was observed applied; an earlier replay `issue_unknown` may still exist |
| `issue_unknown` | At least one queue replay invocation remains unresolved, none was observed applied, and the record will authorize no further invocation |
| `observed_failure` | Queue replay invocations produced only observed failures and none remains unresolved |
| `stale_target` | Current table/Region/partition identity no longer matches the captured request; v1 does not redirect or rebuild the request |
| `not_sent` | No queue replay invocation was authorized, and no more specific terminal settlement (such as `stale_target`) applies; this says nothing about whether the first-round request applied |

The immutable first-round disposition is `observed_retryable_failure` for an admitted `PerRegionPartial` entry-level retryable failure, or `first_round_unknown` for an admitted `PeerBatchRpcUnknown` / `SingleEntryCollapsed` outcome without a definitive per-Region result. The summary precedence for replay settlement is `applied`, then `issue_unknown`, then `stale_target`, then `observed_failure`, then `not_sent`. Invocation history, first-round provenance, and terminal give-up reason are retained separately. For example, a record may end with `replay_settlement = observed_failure` and `first_round_disposition = first_round_unknown`; it must not be reported as an end-to-end known failure or as proof that the Region was never applied. Likewise, `replay_settlement = not_sent` with `first_round_disposition = first_round_unknown` means the queue authorized no replay, not that the original request was not applied. A record labelled `applied` may still have an earlier unresolved replay invocation; the queue cannot infer that the earlier remote attempt was not applied.

### Replay outcomes

| Replay event | Record behavior | Queue consequence |
|---|---|---|
| Observed `applied` | Settle the record as `applied` | Release its retained request payload when no longer needed for completion/accounting cleanup |
| Observed retryable failure | Schedule a new distinct invocation after backoff if the deadline and invocation cap allow it | Record remains at the Region FIFO head |
| Observed terminal failure | Settle from the record's full history; do not retry this record | Advance the Region queue when local cleanup is complete |
| Whole-RPC failure with definitive per-entry outcomes | Apply the corresponding per-entry result to each attempt | Evaluated per record/invocation |
| Whole-RPC/transport failure without a definitive result | Mark this invocation `issue_unknown`; a distinct invocation may be scheduled after backoff if the record deadline and per-record invocation cap permit it | The unknown invocation remains in history and may still apply remotely |
| Transient route lookup failure | Defer with backoff; do not authorize a replay RPC | No new invocation is created |
| Same `RegionId` remains valid and only route/peer information changes | Refresh routing and replay to the same Region | Record identity and FIFO position are retained |
| Original Region is absent, table identity/version changes, or `partition_expr_version` changes | Stop further replay as `stale_target`; do not rebuild or redirect rows to another Region | If an earlier invocation remains unresolved, final settlement remains `issue_unknown` |
| Region exists but has no leader during replay | Retain the record at the Region FIFO head and defer until the next bounded backoff timer | No new invocation is created; v1 does not require an immediate wake-up from leader-cache or watch notifications |
| No leader during initial prepare before the first RPC | No queue record is created | Outside v1 queue admission and outside this retry contract |
| Replay RPC timeout | Classify the invocation `issue_unknown`; a distinct invocation may be scheduled only if the deadline and per-record invocation cap allow it | The remote operation may still apply after the local timeout |
| Retry deadline reached before authorization | Stop scheduling and settle from recorded history | Existing unresolved invocation history remains effective |
| Retry deadline reached while an invocation is in flight | Authorize nothing further; allow the in-flight RPC until its own timeout | If no result is observed, the invocation becomes `issue_unknown` |
| Per-record invocation cap exhausted | Stop new authorizations and settle the record from recorded history | No new invocation is issued |
| Worker failure before authorization | Finalize the record from its existing history; do not invent an unknown invocation or automatically restart the replay | No send-side unknown is created |
| Worker failure after authorization without an observed result | Classify that invocation as `issue_unknown` and finalize the record from its history | A late remote application remains possible; the failed record is not requeued |
| Client cancellation | End the client wait only; queue work continues | No change to record ownership |

A **definitive per-entry first-round result** means the dispatcher has an explicit disposition for each Region entry, either success or an entry-level failure. A whole-RPC transport error without per-entry results is not definitive and remains unresolved first-round provenance; it does not turn a retryable failure into a successful write.

Replay classification uses the propagated `RetryHint`, not the status code alone:

- `Retryable` schedules another invocation to the same Region after backoff.
- `RetryAfterRouteRefresh` refreshes routing and metadata, then validates that the original Region is still the target before another send.
- `NonRetryable` is terminal for that record.

This distinction matters because `RejectWrite` and `RejectWriteInStaging` can share a storage-unavailable status while carrying different retry hints. A normal Mito write-buffer stall may retain the request in the Datanode without returning an immediate retryable response. If the queue's local replay timeout expires while waiting, the invocation becomes `issue_unknown`; timeout does not prove that Mito cancelled or discarded the request.

### Retry budget and per-record limits

The absolute retry deadline is `admission_time + write_queue.retry_budget`, where `admission_time` is the monotonic timestamp taken at the record publication commit point. Backoff, route lookup, metadata refresh, and leaderless deferral consume the deadline but never extend it. An invocation authorized before the deadline may run until its own `write_queue.replay_rpc_timeout`, which starts at authorization; record cleanup may therefore extend beyond the retry deadline.

Replay reuses the existing jittered exponential backoff helper and inherits `write_retry.initial_backoff`, `write_retry.max_backoff`, and `write_retry.backoff_multiplier`; `write_retry.max_retries` does not apply to queue-owned records. `write_retry.max_backoff` caps the base delay before positive jitter, so the final wait may approach twice that value. Backoff progression is per record. A Region with no leader is deferred until the next bounded backoff wake-up and does not consume an invocation.

A per-record invocation cap limits the total number of queue replay RPCs authorized for one record. v1 uses a fixed cap of eight. The cap and retry deadline are independent; whichever is reached first stops new authorizations. The invocation-history storage is allocated at its maximum capacity when the record is admitted: the retained charge reserves all eight fixed-size history entries up front. Each entry stores only bounded outcome, retry-hint, and error-class fields; it does not grow with arbitrary error strings or cloned requests. No history allocation or retained-charge growth is needed when a later invocation is authorized.

The legacy `write_retry.max_retries` applies only to calls that remain on the existing retry path. The queue obtains and validates the backoff parameters independently, including when legacy retries are disabled. `write_queue.max_records` counts each admitted record until both its retained request payload and any caller-observable completion cell have been released.

### Worker failure, cancellation, and shutdown

The queue maintains a single active replay owner for each claimed record. A worker failure before send authorization creates no replay invocation; the record is finalized from its existing history and the failed replay is not automatically restarted. If a worker fails after authorization and no definitive result was observed, that invocation is `issue_unknown`, and the record is finalized from its recorded history rather than silently treated as unsent. In either case, a worker failure may terminate the record before its retry deadline or invocation budget is exhausted; v1 does not reclaim or reschedule that failed record. Worker-slot capacity is a separate concern: the supervisor/scheduler must release the failed slot and make it available to other queued records (by replacing the worker task or dispatching through an equivalent replenished execution slot). Reusing a slot does not reclaim the failed record or authorize another invocation for it. A per-record claim/lease guard or equivalent cleanup mechanism must make this finalization happen exactly once when a worker task exits unexpectedly; process abort remains outside this task-level recovery contract.

A panic or process failure during synchronous admission is not recovered by v1. Temporary reservations use RAII or equivalent cleanup. All allocations and panic-prone work must complete before the final publication commit; the commit itself must contain no `await` or fallible allocation and must publish the record, charge, and completion cell together. A panic must never leave a record visible without its retained charge.

Client cancellation before publication aborts admission as described above. After publication, cancellation ends only the client wait. Queue shutdown stops new replay authorization and finalizes every remaining admitted record, including ready/deferred records that are not currently claimed by a worker, from its recorded history. Records with no authorized replay invocation and no more specific terminal settlement become `not_sent`; authorized in-flight invocations without observed results become `issue_unknown`; other records retain the settlement implied by their observed invocation history. Shutdown also finalizes active replay work and updates/notifies attached completion cells. It does not drain the queue, recover records, or prove that in-flight remote requests did not apply. Forced process termination may discard all remaining queue state.

## Routing and Same-Region Validation

### Metadata validation and route refresh

At planning time, each eligible attempt captures the table identity and metadata needed to decide whether the original routed request remains valid: `TableId`, `TableIdent.version`, engine, effective `RegionOptions.append_mode`, instant-TTL state, the observed plan-time Flow-cache status, the original `RegionId`, and `partition_expr_version`. The partition-expression version is available only for a non-empty partition expression. Any Region with `partition_expr_version = None` is excluded, including default Regions of otherwise partitioned tables. `TableIdent.version`, metadata-store version, and route-bookkeeping `route.version` are distinct concepts. `route.version` is a bookkeeping counter that can change for leader/route updates and Region reservation; it is not Region identity and must not be used as a replay fence. For supported partitioned tables, the original `RegionId` plus its `partition_expr_version` are the same-Region identity checks.

Replay reloads current table metadata and route information and rejects the record if the table identity, `TableIdent.version`, engine, effective append-mode state, instant-TTL state, original `RegionId`, or `partition_expr_version` no longer matches the captured request. A peer/leader change is allowed when the original Region still exists and the same-Region identity checks pass. The queue must not call the existing `reroute_failed_writes()` path if that helper rebuilds requests through the current partitioner, because that can move rows to successor Regions and violates the v1 same-Region contract. A `route.version` change alone must neither admit nor reject a record. Flow-source status is not re-read; the captured cache observation may be stale and no `TableIdent.version` fence for Flow creation/removal is assumed. The implementation must obtain a coherent current view of route, partition metadata, and Region state. If it cannot establish that the values being compared describe the same current target, it must fail closed. This validation is not an atomic DDL fence and does not prove that an earlier request was not applied before its response was lost.

A transient metadata or route lookup failure is deferred with backoff and is not treated as proof that a Region was removed. When the response indicates a stale route, replay refreshes metadata and routing before deciding whether the original Region remains valid:

1. Group refresh candidates by captured `TableId` and fully qualified table name. Invalidate each distinct `TableId` once; this invalidation must clear both the route cache and the associated partition-info cache.
2. Invalidate each distinct name-based table-metadata entry once, then resolve each distinct fully qualified name once for that wave. Use batch name resolution when available; otherwise use bounded single-flight lookups so records for the same name do not issue duplicate refreshes.
3. Verify for every candidate that the resolved name still maps to its captured `TableId`; never replay an old request against a newly created table that reused the name. A missing name or changed identity is terminal for the affected record.
4. Load current table metadata and route/partition information. Continue only if the original `RegionId` and `partition_expr_version` remain valid. Do not invoke a new partitioner to remap rows to other Regions.

A refresh wave is not an atomic snapshot across different tables. A transient failure for one name defers that table's records without preventing unrelated groups from progressing. Reusing stale peer information is insufficient: route refresh must establish that the original Region is still the target. The refresh may be shared by records for the same table, but each record independently validates its captured Region and partition-expression identity.

Metadata validation is not an atomic DDL fence:

| Operation or change | Replay behavior | Residual risk |
|---|---|---|
| `ALTER` that changes `TableIdent.version` | Reject a record when refreshed metadata shows a different table version | DDL may commit after validation and before the replay RPC; cross-Frontend cache propagation is asynchronous |
| `TRUNCATE` | `TRUNCATE` does not change `TableIdent.version`; the captured version check therefore cannot identify it | A queued write may replay after truncation and reinsert rows unless a separate data-generation marker is added |
| `DROP` followed by create under the same name | Reject the old record when name resolution produces a different `TableId` | The old request must never be sent to the newly created table |
| `UNDROP` | Validate the restored table identity and route before replay | An enterprise `UNDROP` may restore prior table metadata; the complete replay-identity behavior requires a regression test in an enterprise-enabled build |
| Repartition / partition-expression change | Settle as `stale_target` when the captured partition-expression version or original Region is no longer valid; do not rebuild or redirect | The original request may remain unapplied, or an earlier unknown invocation may still apply remotely |
| Flow-source creation or removal | Keep the plan-time `TableFlownodeSetCache` observation as part of eligibility context | The cache may remain stale until event-driven invalidation; `TableIdent.version` is not a Flow-change fence |

Without a separate data-generation marker, workloads that require `TRUNCATE` to fence earlier queued writes must remain queue-disabled. Enterprise `UNDROP` behavior must pass the regression test above before enablement in deployments using that feature. A stale-target decision prevents future sends only; it cannot retract an earlier invocation whose remote outcome remains unknown, and v1 adds no Datanode-side idempotency or stale-write fence.

### Same-Region route refresh

If refreshed metadata confirms that the same `TableId`, `TableIdent.version`, `RegionId`, and `partition_expr_version` remain valid, replay may update the peer/leader routing information for that same Region. This refresh preserves record identity, first-round provenance, invocation history, retry deadline, per-record invocation budget, FIFO position, and completion-cell identity. It does not change the captured plan-time table or Region snapshot.

If the original Region is absent, or the table/partition identity changes, replay stops further sends and classifies the replay path as `stale_target`. If an earlier invocation remains unresolved, the record retains the `issue_unknown` settlement because the old request may still have been applied. Replay does not rebuild the rows, split the request, or enqueue work under a new Region key. This is the explicit v1 boundary for repartition and cross-Region movement.

## Resource Limits and Operations

### Retained memory and record accounting

`write_queue.max_bytes`, `write_queue.max_records`, and `write_queue.max_record_size` are independent admission limits; the first one reached rejects the admission. `max_bytes` is the aggregate retained-accounting ceiling. `max_records` counts admitted records while either their request payload or their caller-observable completion cell remains retained. `max_record_size` is a per-Region protobuf wire-size preflight ceiling. These are queue accounting limits, not process-RSS guarantees.

A retained record keeps the original `RoutedWrite` and its `Arc<RegionRequest>`; queue retention does not encode/decode the request or create a second body copy. The retained charge is the sum of: (1) an estimated retained-heap size for the original per-Region request, calculated by recursively charging fixed message fields, every owned `Vec` by `capacity * size_of::<T>()`, and every owned string/byte buffer by its allocated capacity; and boxed nodes and reference-counted control blocks by their fixed and nested allocation sizes; (2) fixed record, FIFO-index, and sequence metadata; (3) captured plan context and owner/attribution metadata, with dynamic strings charged by byte length and vector-backed fields charged by allocated capacity times the declared per-element cost; (4) fixed first-round provenance and the full eight-entry invocation-history reservation; and (5) a fixed completion-cell and caller-handle allowance while the caller remains attached. A conservative fixed per-allocation allowance covers allocator bookkeeping in the estimate; shared payloads referenced by more than one record may be charged to each record rather than undercounted. The observer allowance is charged per record, including a conservative share of call-group overhead, so it does not require transferring a shared header charge when records settle at different times.

The invocation-history array is reserved at its full fixed capacity before publication. Completion-cell storage and the corresponding caller handle are allocated and charged as part of the record publication transaction. Publication makes the record, its retained charge, pending cell, and RAII handle available together; none becomes visible independently. If the charge cannot be reserved, the attempt is rejected without publication. There is no post-publication growth of retained history or completion state.

`encoded_len()` is computed after per-Region splitting and before the first RPC. It traverses the protobuf message but does not allocate a second encoded buffer. If any Region request exceeds `max_record_size`, the entire call is preflight-ineligible and stays on its existing retry path where supported. This wire-size gate is an admission/fairness policy; it is separate from the retained-heap estimate used by `max_bytes`. Both traversals add plan/preflight CPU cost to queue-enabled calls and must be measured separately from failure-path admission overhead. The retained-size estimator must cover every owned allocation reachable from the supported insert request shape and be tested against null-heavy, short-string, long-string, and nested value shapes.

When a record settles, its request payload can be released and it is removed from the Region FIFO. If the caller still holds its completion handle, a bounded completion-only entry remains until the caller consumes or detaches it; it still counts against `max_records` and `max_bytes`. If the caller detached first, the queue drops the cell reference and a terminal record can be removed once its payload/history cleanup is complete. After both payload and observer state are released, the record is removed from active queue state. v1 keeps no in-memory dead-letter archive and provides no dead-letter query API.

The queue does not create replay-side body copies or its own encode/decode buffers. The gRPC/protobuf transport may allocate serialization and transport buffers outside the queue ledger. `replay_concurrency` and `max_record_size` constrain the number and wire-size of concurrent replay requests, but they do not prove a hard upper bound on opaque transport allocations or total Frontend RSS. Validation must measure transport/allocator memory at the configured concurrency. If an implementation later adds a queue-owned materialized copy, it must define and reserve that allocation before creating it; such copying is not part of v1.

### Replay scheduling and fairness

Each Frontend queue has `write_queue.replay_concurrency` replay slots and permits at most one locally in-flight replay record per Region. Within a Region, records are ordered by successful admission-publication sequence assigned under queue-state protection; this is publication order, not client arrival or first-round dispatch order. Runnable Regions are scheduled round-robin among those currently ready. A Region waiting for backoff or a leader is not runnable until that condition changes. v1 has no per-Region retained-capacity reservation: one Region with a prolonged no-leader backlog can consume the Frontend-wide byte/record budget and cause admission rejection for otherwise healthy Regions.

Round-robin scheduling gives currently runnable Regions an opportunity to make progress; it does not provide bounded waiting time or per-record latency guarantees. A hot Region remains serialized, and its tail delay grows with its backlog. All queue limits and replay concurrency are per Frontend, not cluster-wide. The queue does not provide fairness for retained-memory admission across Regions.

### Drain-time model and workload envelope

The queue has both a per-Region serialization limit and a Frontend-wide concurrency limit. An optimistic lower bound for a Region with `K_i` queued records is:

```text
T_region >= K_i * (L_i + D_i)
```

where `L_i` is the average cumulative RPC service time per record for the declared failure mix (including repeated invocations), and `D_i` is the average cumulative delay per record from backoff or leaderless deferral. In the single-invocation, no-retry case, `L_i` is the mean replay RPC service time. Across `N` records, when at least `replay_concurrency` Regions are runnable and cumulative service times are approximately uniform, an aggregate lower bound is:

```text
T_frontend >= (N / replay_concurrency) * L
```

These optimistic bounds omit route lookup, metadata refresh, scheduler overhead, transport variance, and failures beyond those represented in the workload averages. The per-Region bound can dominate even when Frontend replay slots are idle; increasing global concurrency cannot accelerate a hot Region whose records must be replayed serially.

The deployment workload envelope must state total backlog, per-Region depth distribution (including the hottest Region), replay RPC latency distribution, no-leader fraction, and expected failure mix. Measure time-to-first-authorization, the fraction authorized before the retry deadline, total and hottest-Region drain time, and deadline expiry. Do not infer that `max_records = 100000` is a supported drain capacity or that the initial `5m` retry window can drain a fully populated queue.

### Observability

Expose Frontend-scope gauges/counters for retained bytes, retained record entries, admission rejections by bounded reason, replay-slot utilization, per-Region queue depth in logs or sampled diagnostics, applied/failed/stale/deadline outcomes, first-round unknowns, and replay invocations classified as `issue_unknown`. First-round uncertainty and replay unknown outcomes must remain separate. Region/table identities belong in structured logs rather than metric labels to avoid high cardinality.

The cumulative replay-`issue_unknown` counter is observational only. There is no process-lifetime unknown-outcome quota or automatic queue-wide cutoff when the counter reaches a value. Sustained increases require an explicit deployment-level alert and operator response; the remaining risk is that remote operations may outlive their local records and accumulate across successive records.

## Configuration and Rollout

The values below are provisional starting points for discussion, not validated production defaults or benchmark claims. Public queue settings use the `write_queue.` prefix. The per-record invocation cap is a fixed internal v1 limit of eight, not a public TOML parameter.

| Parameter | Scope | Purpose | Initial value / status |
|---|---|---|---|
| `write_queue.enable` | Per Frontend | Enable queue ownership for supported distributed insert paths | `false` by default |
| `write_queue.max_bytes` | Per Frontend | Aggregate retained accounting for payload, record metadata, history, and attached completion cells | 512 MiB, initial value |
| `write_queue.max_records` | Per Frontend | Maximum live retained record entries, including completion-only entries held for callers | 100000, provisional test point |
| `write_queue.max_record_size` | Per record | Preflight limit for one split Region request's protobuf wire size | 512 KiB, initial value; checked with `encoded_len()` before the first RPC |
| `write_queue.retry_budget` | Per record | Absolute replay window beginning at admission publication | 5m, initial value; not a deployment recovery guarantee |
| `write_queue.completion_wait` | Per client request | Maximum blocking wait for settlement after all admissions; `0` means immediate snapshot only | 30s, initial value |
| `write_queue.replay_rpc_timeout` | Per invocation | Queue-side timeout around the replay RPC future after send authorization | 10s, initial value |
| `write_queue.replay_concurrency` | Per Frontend | Maximum locally active replay RPCs | 8, initial value |
| `write_retry.initial_backoff` | Existing retry configuration | Initial replay scheduling backoff | Existing setting reused |
| `write_retry.max_backoff` | Existing retry configuration | Base-delay cap before positive jitter | Existing setting reused; final wait may approach twice this value |
| `write_retry.backoff_multiplier` | Existing retry configuration | Exponential replay backoff multiplier | Existing setting reused |

At startup, configuration validation must reject zero or invalid values for `max_bytes`, `max_records`, `max_record_size`, `retry_budget`, `replay_rpc_timeout`, and `replay_concurrency`; `completion_wait = 0` is valid. When queue mode is enabled, queue initialization must independently validate a positive initial backoff, `max_backoff >= initial_backoff`, and a finite multiplier of at least 1, even if legacy retries are disabled. It must not rely on the legacy `WriteRetry` object having been constructed. The internal per-record invocation cap must be a positive fixed constant covered by the retained-accounting reservation and tests.

`write_retry.max_retries` does not control attempts transferred to the queue. Queue-owned attempts use `write_queue.retry_budget`, the fixed per-record invocation cap, and replay backoff. Preflight-ineligible calls retain the existing retry configuration and behavior where supported. The production Frontend-to-Datanode channel has no per-request timeout for the original first-round RPC; v1 does not bound that first-round wait. `write_queue.replay_rpc_timeout` is a separate local timeout around a queue-owned replay future. Its expiry bounds local waiting but does not guarantee cancellation of a request already accepted by the Datanode; it does not apply to the first-round RPC.

The switch applies to every queue-eligible direct insert routed through the Frontend; v1 has no table/database allowlist or hot reload. Operators cannot enable the queue for one table while keeping another table on queue-disabled behavior within the same Frontend. Use a dedicated Frontend pool for workloads that need different retry or DDL behavior. Configuration is loaded at startup; disabling the queue requires a restart, and shutdown/restart may discard retained records. Neither operation is a drain protocol or proof that no remote write can still apply.

### Rollout and operational guardrails

- Roll out by distributed Frontend pool, not by table. Start with a dedicated canary Frontend pool and route only workloads whose duplicate-write and DDL behavior is compatible with their application requirements. The feature has no per-table allowlist or per-table gray switch.
- Standalone deployments remain outside v1; do not enable or instantiate the queue there.
- Keep tables that require strict `TRUNCATE`/DDL ordering, or cannot tolerate a late stale replay, on a separate Frontend pool where the queue remains disabled. A queue-enabled shared Frontend cannot isolate one such table at runtime.
- v1 has no per-table drain or DDL fence. Disabling the queue requires restart; this is not a drain protocol and must not be treated as proof that no queued write can still be applied.
- During a canary, monitor Frontend retained bytes/records, admission rejection reasons, time-to-first-authorization, deadline expiry, hottest-Region depth, stale-target terminations, and replay timeouts.
- For a deployment that cannot accept the `TRUNCATE` blind spot or loss of retry recovery after repartition, keep the queue disabled for that workload.

`write_queue.enable = true` is blocked until the detailed Validation Plan has passed and the following design gates are satisfied:

- The supported non-tracked direct-insert path selects an explicit per-call retry policy before the first RPC; all excluded producers remain on their existing paths. The first-round collector preserves every Region outcome, exact original request, propagated `RetryHint`, stable order, and send-boundary provenance before any batch-level terminal-error aggregation or additional inline retry.
- The client contract is implemented as written: preflight-ineligible calls retain the existing retry owner, queue-owned admission rejection has no inline fallback, first-round and replay uncertainty remain separately observable, and completion waiting uses one absolute deadline.
- Replay obtains the required coherent table/route/partition/Region view or fails closed, validates same-Region identity, and never rebuilds or redirects requests. Flow stale-negative risk, `TRUNCATE`, `UNDROP`, and the lack of an atomic DDL fence are explicitly accepted by maintainers and affected deployment owners.
- The per-record invocation cap, replay concurrency, retained-byte/record accounting, completion-cell bounds, Worker-slot replenishment, and terminal cleanup are finite and tested under failure injection. Opaque transport allocations are measured at the configured replay concurrency, not represented as a queue-owned hard budget. Unknown replay outcomes are observable, and the deployment has an explicit response to sustained elevated rates; v1 has no automatic process-lifetime unknown-outcome cutoff.
- The declared workload envelope, latency overhead, hottest-Region drain behavior, deadline expiry, and observed unknown-outcome rate have been measured. The canary and deployment isolation requirements below are met.

The Validation Plan defines the required tests and measurements; this gate list is not a substitute for them.

A future change to the shipped default requires a separate maintainer decision based on completed integration and client-contract tests, failure-injection coverage, same-Region routing/DDL validation, representative resource and latency measurements, canary results, and explicit acceptance of the remaining duplicate-write and DDL limitations. Queue enablement for a particular deployment does not by itself justify changing the shipped default.

## Risks and Limitations

- **Duplicate-capable retry and cumulative unknown outcomes.** A lost response, timeout, worker failure after submission, or client retry can cause duplicate writes. Neither a retryable response nor a later successful invocation proves that an earlier unknown invocation did not apply. The per-record invocation cap limits queue replay sends for one record, but v1 has no process-lifetime cumulative cap on `issue_unknown`; successive records can accumulate unknown remote operations after their local state is finalized. `replay_concurrency` limits locally active replay work, not remote operations that may continue after a local timeout.
- **Partial batch outcome.** A batch may return an error because one sibling has a terminal failure while a retryable sibling is admitted and applies later. The client can therefore observe a failed batch whose retryable subset is partially applied. Admitted records may also apply after the client wait expires; a batch error does not mean that no rows were written.
- **No inline fallback for queue-owned rejection.** A queue-owned attempt rejected by capacity, queue closure, or an invalid admission sidecar receives no Frontend retry after the first round. Preflight-ineligible calls retain their existing retry owner where supported.
- **Process-local and worker-failure loss.** Restart, crash, shutdown, deadline expiry, or a replay worker failure can terminate admitted work before the retry deadline or invocation cap is exhausted. Worker failures do not trigger automatic reclamation or rescheduling of the failed record; the worker slot is replenished only for other records. Queue contents and per-record invocation history are lost on process restart; remote operations from timed-out invocations may nevertheless still be executing or may apply later.
- **Repartition recovery is out of scope.** If the original Region disappears or the partition-expression version changes, the record settles as `stale_target` and is not rebuilt or redirected. A write may therefore remain unapplied after a repartition even when the current routing could identify new destination Regions. v1 intentionally does not provide cross-Region recovery.
- **Remote application order is not guaranteed.** Per-Region FIFO controls local scheduling and send authorization only. An earlier invocation classified `issue_unknown` may still apply after a later invocation or live write. v1 has no Datanode-side fence to prevent that late application.
- **DDL stale-replay window.** Metadata revalidation is not an atomic fence. A client may receive a write error, run `TRUNCATE`, and then have a queued replay reinsert rows that the client expected to be removed. `TRUNCATE` does not increment the captured `TableIdent.version`, so this version check cannot identify the truncate. `ALTER` can race between validation and RPC submission; cross-Frontend metadata propagation is asynchronous. `DROP` followed by create under the same name receives a new `TableId` and must be rejected by the captured-identity check. `UNDROP` restores stored table metadata; whether an old queued record can pass the complete replay checks after restoration must be established by a regression test. v1 has no atomic DDL fence. If no separate truncate/data-generation marker is implemented, workloads that require `TRUNCATE` to fence older writes must remain queue-disabled.
- **Cross-Region retained-capacity starvation.** Retained limits are Frontend-wide and v1 has no per-Region retained cap. One Region with a prolonged no-leader backlog can consume the shared byte/record budget and cause admission rejection for healthy Regions. Round-robin replay scheduling does not prevent this admission-time capacity starvation.
- **Resource accounting is not an RSS bound.** The retained ledger uses a conservative allocation-capacity estimate for queue-owned requests and metadata, but it does not account for every Frontend allocation or total process memory. Completion cells are bounded by retained record entries and charged to them. Opaque gRPC/protobuf transport allocations are outside the queue ledger and must be measured at the configured replay concurrency.
- **Hot-Region drain.** Per-Region serialization means global replay concurrency cannot shorten the tail of a single hot Region. Records near the end of a backlog may expire before their first authorization. The retained 5m retry budget is not a guarantee that a full queue or hot Region will drain within that window; drain-time and deadline-expiry behavior must be evaluated against the declared workload envelope.
- **Initial no-leader requests are not queued.** A request that finds no leader during prepare before its first RPC is sent is outside this queue's admission contract. The queue does not guarantee recovery for that case.
- **Unbounded first-round wait.** The production Frontend-to-Datanode channel has no per-request timeout. Because queue ownership begins only after a first-round result is collected, a first-round RPC that never resolves can keep the client request pending indefinitely; `completion_wait` and `replay_rpc_timeout` do not bound it.
- **Flow-cache eligibility window.** Queue eligibility uses the plan-time `TableFlownodeSetCache` observation, which is not a generation fence. A stale-negative observation can incorrectly admit a Flow-source table and replay the physical write without repeating its mirror; a stale-positive observation only causes false exclusion. v1 cannot detect every stale-negative observation; enablement requires explicit maintainer acceptance or a freshness check, and workloads that cannot tolerate this window must remain queue-disabled.

## Validation Plan

Validation must establish the externally observable contract and the resource/workload envelope; it should not prescribe concrete test harness APIs.

- **Request ownership and first-round report:** verify queue-disabled behavior is unchanged. With queue enabled, the entire supported direct-insert call is classified before the first RPC; any preflight-ineligible Region keeps the whole call on its existing retry path. Verify explicit per-call policy at shared dispatcher call sites so tracked writes, Delete, BulkInsert, batchers, and Standalone never inherit queue ownership. Verify `max_record_size` is checked on every split Region request before any RPC, and that an oversized request keeps the whole call on its existing path. Verify queue-mode partial-result collection does not depend on `retry.is_some()`, preserves exact `RoutedWrite`/owner data and `PerRegionPartial`, `PeerBatchRpcUnknown`, or `SingleEntryCollapsed` provenance, and cannot lose retryable siblings during batch error aggregation.
- **Completion-cell lifecycle and client contract:** race immediate replay completion against record publication and request response construction. Verify the cell is created and charged with publication, can be read after payload release, and cannot be removed while the caller remains attached. Exercise concurrent admission, completion before the caller begins waiting, immediate snapshot with `completion_wait = 0`, fail-fast on the first record with a final queue settlement other than `applied`, all-applied success, wait-deadline expiry, partial admission, terminal first-round siblings, cancellation, and shutdown wake-up. Verify caller detachment releases completion-only entries while pending records continue, and that no notification is lost.
- **Replay and failure lifecycle:** cover retryable and terminal propagated `RetryHint` values, fast `RejectWrite`, `RejectWriteInStaging`, Mito write-buffer stalls, replay RPC timeouts, and late remote application. Verify one invocation is recorded before send, no `await` or lookup occurs between authorization and RPC issue, and an authorized invocation with no observed result becomes `issue_unknown`. Verify the fixed eight-entry history reservation is made at admission, the invocation cap and absolute deadline stop further authorizations, and unknown outcomes across successive records increment telemetry without triggering an implicit process-wide cutoff. Test worker failure before/after authorization, slot replenishment, orderly shutdown of ready/deferred/in-flight records, and exactly-once cleanup of reservations.
- **Routing, cache invalidation, and DDL:** exercise stale route caches with peer/leader changes while preserving the same Region identity. Verify TableId invalidation clears route and associated partition-info caches, name-based invalidation refreshes table metadata, and refresh is batched/single-flight per table. Verify table identity/version, original `RegionId`, and `partition_expr_version` changes stop replay without repartitioning. Cover `DROP`/recreate, enterprise `UNDROP`, `ALTER`, and the known `TRUNCATE` blind spot. Verify the coherent current-view requirement or fail-closed behavior, and explicitly exercise the accepted Flow stale-negative risk.
- **Ordering and unknown remote work:** verify per-Region FIFO by admission-publication sequence under concurrent requests and at most one locally in-flight record per Region. Exercise an unknown earlier invocation followed by a later record or live write; verify local FIFO does not claim remote application order. Where Datanode instrumentation permits, measure how many timed-out requests remain active or apply after their Frontend records are finalized. Verify no-leader during initial prepare creates no record, while a post-admission no-leader replay remains deferred at the Region FIFO head.
- **Resource accounting and performance:** exhaust `max_bytes`, `max_records`, `max_record_size`, the invocation cap, and replay concurrency. Verify `max_bytes` includes the recursively estimated retained request allocations, plan/owner metadata, the full fixed invocation-history reservation, and completion-cell/group-slot charge; test the estimator against null-heavy, string-heavy, and nested-value request shapes. Verify `max_records` includes completion-only entries until caller detach, and that group/slot count never exceeds live record entries. Measure actual Frontend RSS and gRPC/protobuf transport/allocator memory at replay concurrency 1 and configured maximum; do not claim these are bounded by the retained ledger. Compare queue-enabled and disabled successful paths, measuring `encoded_len()` preflight overhead separately, and compare failure-path classification/admission/locking overhead under the same injected failures. Report p95/p99 latency, ledger lock wait/hold time, retained bytes/entries, per-Region depth/skew, replay service-time distribution, fraction authorized before deadline, hottest-Region drain time, and deadline expiry. Maintainers must set or explicitly waive the performance threshold after representative workload measurements.
- **Flow behavior:** verify `FlowMirrorTask::new` failure occurs before the first Region RPC, detached mirror failures are logged/counted without being awaited, and queue replay does not run the mirror a second time or claim to repair a missed mirror.

`write_queue.enable = true` requires all request-ownership, completion-cell, replay, routing, resource, and failure-injection tests above to pass, plus a measured deployment workload envelope and a canary with explicit acceptance of duplicate-write, Flow-cache, `TRUNCATE`, and unknown-remote-operation risks. A future change to the shipped default requires a separate maintainer decision based on the same evidence; enabling one deployment does not by itself justify changing the default.

## Open Design Questions

- **Coherent metadata view:** Which API can provide table identity, route, partition metadata, and Region state as one coherent current view? If no such view is available, replay must fail closed and enablement remains blocked.
- **Flow-cache freshness:** Is explicit maintainer acceptance of the stale-negative Flow-cache risk sufficient, or is a freshness/generation check required before enablement?
- **`TRUNCATE` protection:** Should v1 add a data-generation marker, or explicitly accept the residual risk and require affected workloads to remain queue-disabled?
- **`UNDROP` behavior:** Can a pre-drop record pass the complete replay identity checks after enterprise `UNDROP` restores stored metadata? A regression test is required before enabling the queue in deployments that use `UNDROP`.
- **Unknown-outcome operations:** What deployment-specific alert threshold and operator response should apply to a sustained increase in replay `issue_unknown`? v1 has no automatic process-lifetime cutoff; the response must not assume that restarting Frontend cancels remote operations.

## Alternatives and Future Work

A post-existing-retry buffer is not used because it would introduce a second scheduler and retry budget for the same write. v1 instead selects one retry policy before the first RPC and requires a lossless first-round handoff for queue-owned calls. Preflight-ineligible calls retain their existing retry path; queue-owned admission failures do not fall back to inline retry.

Cross-Region repartition recovery and durable retention are separate future designs. Neither is implemented or specified by this RFC; v1 stops replay when the original Region identity is no longer valid and loses retained records on process restart.
