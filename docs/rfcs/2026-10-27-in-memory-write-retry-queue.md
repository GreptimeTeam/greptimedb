---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
Status: Draft
---

## Summary

This RFC proposes an optional, per-Frontend in-memory retry queue for eligible transient write failures that may outlast the existing inline retry window. v1 is limited to distributed Frontends and ordinary direct inserts targeting partitioned, Mito-backed physical Regions through the non-tracked direct-insert path. Prometheus remote write is eligible only when its actual execution path satisfies the same requirements. Standalone deployments, tracked writes, metric logical-row writes that still target logical Metric-engine Regions, metric logical-batcher flushes, pending-rows/table-batcher flushes, Delete, BulkInsert, and Flight `DoPut` remain outside v1.

Retry ownership is selected for the entire direct-insert call before its first write RPC. The call is queue-owned only when every Region attempt is eligible; otherwise the whole call retains its existing retry path where supported. The queue receives retryable first-round failures before another inline retry round can begin. Queue-owned admission rejection is fail-closed and does not fall back to inline retry. First-round successes are never admitted or replayed, and terminal failures remain terminal.

Each admitted record remains bound to its original table identity, source `RegionId`, and plan-time `partition_expr_version`. Replay may refresh peer/leader routing and retry the same Region only. If the table identity, table version, Region identity, or partition-expression version changes, the record is stopped as stale-target work. v1 does not repartition rows or redirect requests to another Region; cross-Region repartition recovery is deferred to a separate design.

The queue provides bounded best-effort replay with per-Region FIFO scheduling, an absolute retry deadline, a per-record invocation limit, retained-capacity limits, and a Frontend-wide replay concurrency limit. The queue is process-local, disabled by default, and does not provide durability, restart recovery, delivery guarantees, or guaranteed eventual application.

## Motivation

The existing inline retry path permits up to three additional retry rounds with jittered exponential backoff. The configured maximum caps the pre-jitter base delay rather than the final scheduled wait. The Frontend-to-Datanode channel does not impose a per-request timeout, so a stalled write may remain unresolved beyond the ordinary retry window.

The initial `write_queue.retry_budget = 5m` provides an additional replay window beginning at admission. Backoff, route lookup, metadata refresh, and leaderless deferral all consume this budget; deployment-specific recovery and backlog distributions must be measured before enablement.

A Datanode restart, failover, or leader change may outlast the inline retry window. A write RPC can also fail after the request reaches the Datanode but before the Frontend receives a definitive per-Region result. Retrying such a request may duplicate a write, while dropping it may leave part of a multi-Region insert unapplied. A client-visible batch error therefore does not imply that no rows were written.

A routed insert is tied to the table identity, target Region, and partition expression used to map rows. If repartitioning or another metadata change moves those rows to a different Region, replaying the original Region request is no longer sufficient. v1 deliberately stops replay when the captured target is no longer valid rather than rebuilding the request against a new partitioner.

This RFC covers only the in-memory portion of a bounded Frontend write buffer. Durable retention, restart recovery, and cross-Region repartition recovery require separate designs.

## Goals and Non-goals

### Goals

- Extend retry handling for eligible transient insert failures beyond the existing inline retry window.
- Preserve the synchronous first write round while assigning exactly one retry owner to each failed Region attempt.
- Preserve per-entry outcomes and first-round failure provenance before batch-level result merging.
- Bound retained queue work, replay duration, per-record invocation count, and replay concurrency.
- Define client-visible behavior for mixed batches, partial admission, uncertain outcomes, and asynchronous replay.
- Replay a retained request only while its captured table and same-Region routing identity remain valid.
- State the exact scope of local FIFO scheduling, DDL and availability limitations, and the per-Frontend rollout boundary.

### Non-goals

- Durable queue storage, process-restart recovery, or guaranteed eventual delivery.
- Exactly-once or at-least-once write semantics, or proof that an unknown RPC was not applied.
- A Datanode protocol change for idempotency or deduplication.
- Cross-Region repartition recovery, row repartitioning, FanOut, or successor publication. If the original Region or partition-expression identity is no longer valid, the queued record is not redirected to another Region.
- Queue use in Standalone mode.
- Queue ownership for direct insert calls that use the `confirmed_writes` / `execute_tracked` path; these calls retain their existing tracked retry behavior in v1.
- Replacing retry behavior for Delete, BulkInsert, Flight `DoPut`, metric logical-batcher flushes, or writes flushed by pending-rows/table batchers. These paths remain outside the queue, including batcher paths that happen to use `execute_tracked`.
- Queue admission for Metric logical-row inserts that still target logical Metric-engine Regions.
- Queue admission for append-mode tables, instant-TTL tables, Flow-source tables, or any Region with `partition_expr_version = None` (including default Regions in partitioned tables), and for requests whose required plan-time context is missing or ambiguous.
- A per-table or per-database queue enablement allowlist, hot configuration reload, or automatic workload isolation. The v1 enable switch is per Frontend.
- Ordering across Frontends, against concurrent live writes, legacy-inline attempts, or unadmitted first-round requests, or proof of remote application order after an invocation becomes `issue_unknown`.
- An atomic DDL fence.
- A dead-letter query API.
- Recovery from process-level loss or corruption of the queue's own coordination state.

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

The supported target is an ordinary insert to a partitioned Mito physical Region on a distributed Frontend, dispatched through the non-tracked direct-insert path. Calls routed through `confirmed_writes` and `execute_tracked` remain on their existing tracked retry path. Protocol names do not determine eligibility: SQL, gRPC, pipeline, COPY, logs, traces, and Prometheus remote write are eligible only when their actual call path uses the supported non-tracked dispatcher and every target Region satisfies the admission rules. Log/trace primary tables and exemplar tables with effective append mode enabled are excluded; trace auxiliary tables are evaluated using their own effective Region options.


The retry policy is selected at the producer/dispatcher boundary before the first write RPC and is explicit for each supported non-tracked direct-insert call. It must be a queue-specific policy, not an overload of `ctx.confirmed_writes()`: that flag selects the separate tracked execution path and is not a queue-ownership decision. `confirmed_writes` selects tracked execution; queue ownership therefore requires a separate per-call policy that can represent whole-call eligibility.

- **Queue policy:** every Region attempt in the call is a direct ordinary insert to a supported partitioned Mito physical Region, uses the supported non-tracked direct-insert path, and satisfies every queue eligibility rule below. The queue owns retryable first-round failures after classification; those attempts are not passed to `WriteRetry::retry`.
- **Existing retry policy:** any Region attempt is known before the first write RPC to be outside queue eligibility, or the call uses `execute_tracked`. The entire call remains with its existing retry owner where that path supports retry. This includes append-mode, instant-TTL, Flow-source, unsupported-engine, `partition_expr_version = None` (including default Regions), missing-context, preflight-oversized, and tracked-path calls.
- **Fail-closed queue admission:** once the call is assigned queue policy, an attempt rejected for retained-capacity exhaustion or queue closure, or whose supposedly valid queue sidecar is found malformed or inconsistent at admission, receives no inline fallback.

Admission must produce an observable result for every retryable Region attempt, distinguishing successful publication from capacity rejection, queue closure, and invalid admission context. The API may batch these results, but a unit return value or log-only rejection is insufficient: the caller must know whether every eligible attempt was admitted before it can return `Ok`. Sibling attempts may be admitted independently, but any rejected queue-owned attempt prevents a successful call result.

The queue is a first-round retry owner, not a buffer that runs only after the existing `WriteRetry` loop exhausts its retry count. Enqueuing from the post-exhaustion give-up path does not satisfy this v1 contract.


The selected policy must remain explicit when producers share internal insert-building logic. `WriteDispatcher` must not hold a dispatcher-global queue mode: insert, bulk insert, Delete, and batcher callers share dispatcher infrastructure, so policy must be supplied explicitly by the supported call path. Queue admission must explicitly accept only supported insert request bodies; Delete must never be admitted even if it shares dispatcher or queue plumbing. A batcher must not inherit queue ownership merely because it reuses an ordinary insert helper. If a producer cannot preserve this distinction through its call path, it remains outside v1.

Calls routed through `execute_tracked` remain outside the queue and retain their existing `WriteCompletion` / `WriteProgress` behavior. That aggregate completion contract is not used as a per-Region admission or queue-settlement report. Supporting tracked calls in a later version would require a separate per-Region failure handoff and queue-settlement contract.

A single direct-insert call must not mix queue and legacy retry owners. If any attempt is ineligible at preflight, the whole call remains on the legacy path; if the whole call is queue-eligible, retryable per-Region failures are classified for independent queue admission. First-round attempts observed as `applied` are never queue-admitted or replayed; only attempts classified as retryable failures may enter queue admission, while terminal failures remain terminal. A queue-owned call that encounters capacity/closure rejection remains fail-closed and never falls back to inline retry.

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

`attempt_order` is assigned monotonically when the dispatcher creates per-Region attempts, before peer batching, and is preserved through send and result collection. It is independent of RPC completion timing and peer-batch order. The report contains only Region attempts for which a first-round RPC was sent. Missing `RoutedWrite` or provenance makes an outcome unadmittable. A prepare/no-leader failure before any RPC is represented by `pre_dispatch_error`, with no Region outcomes. The selected error is the pre-dispatch error when no RPC was sent; otherwise it is the terminal failure with the lowest `attempt_order`, or, if there is no terminal failure, the failure with the lowest `attempt_order`. The invariant is that no batch-level merge or additional `WriteRetry::retry` round occurs before the lossless report is formed and queue ownership has been decided.

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

Flow mirroring remains part of the original request lifecycle and is not replayed by the queue. The retained request contains only the physical Region write. Flow-source writes remain queue-ineligible because replay cannot repeat or compensate for a missing or failed mirror operation.

The Flow-source eligibility value is an observation from `TableFlownodeSetCache`, which may be stale until its event-driven invalidation takes effect. The snapshot must record that plan-time cache observation; it must not claim that `TableIdent.version` fences Flow creation or removal. An unavailable or ambiguous cache observation makes the request ineligible, but a stale-yet-present cache entry cannot be detected by this snapshot. v1 therefore has a residual eligibility risk for Flow creation/removal racing with cache invalidation. Enablement requires explicit maintainer acceptance of this limitation; deployments that cannot tolerate a stale Flow-source classification must keep the queue disabled unless a reliable freshness check is added.

Append mode is captured from the effective `RegionOptions.append_mode` used by the target Region, not inferred solely from table name or a generic table-level option. Log/trace primary tables and exemplar tables with append mode enabled are excluded; trace auxiliary service/operation tables are evaluated using their own Region options. If the effective setting is unavailable or ambiguous, the attempt is not selected for queue ownership.

Queue replay also needs access to the metadata and cache-invalidation capabilities required to refresh table identity and same-Region routing. These capabilities must be available independently of whether legacy retry is enabled.

Admission is the ownership boundary for queue-owned attempts. Before publication, the queue does not own the attempt. After publication, the queue retains responsibility until the record retry deadline or a terminal outcome. Publication makes the record and its retained charge visible together; neither may become visible independently. The publication transaction is the linearization point: it runs under queue-state protection, checks cancellation and the admission token, and either publishes the complete state or publishes nothing. If cancellation is observed before that commit point, admission aborts and releases temporary reservations. If cancellation races with the commit point, whichever operation is observed first by the commit arbitration determines the result; after publication, later cancellation ends only the client wait.

The first-round RPC has already been sent by this stage, so aborting admission does not roll it back or prove that it was not applied; it simply means no queue retry will follow. Completion-wait expiry is not request cancellation and does not participate in publication arbitration. It limits the time available for synchronous completion waiting, not whether an otherwise eligible attempt is admitted. First-round classification and admission must be bounded local operations: admission must not wait for queue capacity, network I/O, metadata refresh, or replay-worker scheduling. The publication decision is resolved before request completion returns. If the completion-wait deadline expires during that local work, admission still reaches its publish/reject decision; the caller then returns the selected first-round error without waiting for replay. This keeps request cancellation, admission ownership, and the client wait deadline as three distinct events.

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

The completion-wait budget applies to queue-owned calls and starts when request completion begins. Time spent collecting first-round outcomes, making the synchronous queue-admission decision, and waiting for queue-owned records consumes that budget. Calls preclassified to the existing retry path retain their existing retry timing and cancellation behavior; `write_queue.completion_wait` does not change them. Calls using `execute_tracked` are outside queue v1 and retain the existing `WriteCompletion` / tracked-progress behavior without queue admission or queue settlement.

Classification and admission are bounded local work and are not preempted by completion-wait expiry; the request resolves every queue admission commit/reject decision before returning. If the deadline has already expired when that decision completes, the selected first-round error is returned immediately and no queue replay wait is registered. `write_queue.completion_wait = 0` still permits normal admission but disables synchronous waiting for queue replay. The completion-wait deadline is a response-wait budget, not an execution-cancellation deadline for classification or admission.

The request waits for queue-owned settlement only when there is no terminal first-round failure, every queue-eligible retryable attempt was admitted, and positive completion-wait budget remains after classification and admission. Otherwise, it returns the selected error while admitted queue records continue independently. If a queue record settles non-applied before the response is constructed, its observed settlement contributes to the failed call result; if other records remain pending, they may continue after that error is returned. Waiter registration must be coordinated with publication/settlement so that a record settling between publication and waiter registration is observed as already settled; that race must not lose a notification. Request cancellation before publication is handled by the admission commit-arbitration rule above; cancellation after publication ends only the client wait.

`Ok` for a queue-owned call means that every queue-owned record settled `applied` before the completion-wait deadline, no queue-owned retryable attempt was rejected, and no terminal first-round failure exists. A call assigned to the existing retry path retains its existing success/error contract. Any non-applied queue record settlement, a rejected queue-owned attempt, a terminal first-round failure, or queue wait-deadline expiry returns the selected error.

`affected_rows` is a response-time snapshot: it reports rows from first-round and queue replay successes that the Frontend has observed as applied before constructing the client response. A replay that succeeds after the response is returned cannot update that response and is not included. Calls assigned to the existing retry path retain their existing accounting. The value is not an exact logical row total and may not account for duplicate application. Each admitted Region record retains the completion state associated with its original per-Region attempt; no successor waiter or descendant completion group exists in v1.

After queue publication, client timeout or cancellation ends only the queue completion wait. It does not delete, settle, cancel, or reassign an admitted record and does not cancel an in-flight replay RPC. Calls assigned to the existing retry path retain their existing caller-cancellation behavior. On orderly queue shutdown, any still-pending queue completion waiter is woken immediately with its already-selected first-round error. Shutdown does not wait for the remaining queue completion-wait duration. A waiter already completed before shutdown is unaffected.

## Queue and Retry Semantics

### Records and invocations

A **record** represents one admitted single-Region attempt. It retains the original routed request, first-round provenance, captured table and Region identity, absolute retry deadline, per-record invocation budget, per-record backoff state, and invocation history. v1 creates no successor records: a record remains bound to its original `RegionId` and captured `partition_expr_version` for its entire lifetime.

An **invocation** is one replay RPC that has passed the queue's send authorization. The invocation's `in_flight` state must be recorded before the RPC is issued; the queue must not send first and record authorization afterward. Its outcome is one of:

- `in_flight`;
- `applied`;
- `observed_retryable_failure`;
- `observed_terminal_failure`; or
- `issue_unknown`.

Before send authorization, replay completes the required table, route, leader, and client lookups. Under queue state protection it then checks that the queue is open, the record is still owned by the current worker, and the record deadline and invocation cap permit another send; it records the invocation as `in_flight` before releasing that protection. The RPC is issued immediately afterward, without an intervening `await` or additional lookup. Send authorization is a local ownership/accounting boundary, not proof that the Datanode received the request.

The recovery contract has two observable checkpoints:

1. **Replay ownership adopted:** the queue records the worker claim for the record. Failure before send authorization creates no invocation; the record is finalized from its existing history and is not automatically restarted.
2. **Send authorization recorded:** the queue records `in_flight` before calling the RPC. From this point onward, worker failure or loss of the response is conservatively classified as `issue_unknown`, even if the failure occurred in the narrow interval before the transport call was actually issued. This avoids treating an authorized attempt as definitely unsent.

The outcomes after `in_flight` are terminal for that invocation. A late response does not rewrite an invocation already classified as `issue_unknown`. A record may authorize a new, distinct invocation while an earlier invocation is `issue_unknown`, provided its deadline and per-record invocation budget still permit it. The earlier invocation is never resent as though it had not been sent, and the later invocation does not clear the earlier unknown. This permits duplicate-capable retry and is not an exactly-once mechanism.

### Record settlement

A record receives one settlement label at terminal classification. The label summarizes the record history; it is not proof of remote application order.

| Record settlement | Meaning |
|---|---|
| `applied` | At least one invocation was observed applied; an earlier `issue_unknown` may still exist |
| `issue_unknown` | An invocation remains unresolved and no invocation was observed applied |
| `observed_failure` | The record has only observed failures and no unresolved invocation |
| `stale_target` | Current table/Region/partition identity no longer matches the captured request; v1 does not redirect or rebuild the request |
| `not_sent` | No queue replay invocation was authorized; this says nothing about whether the first-round request applied |

The summary precedence is `applied`, then `issue_unknown`, then `stale_target`, then `observed_failure`, then `not_sent`. Invocation history and first-round provenance are retained separately. In particular, a record labelled `applied` may still have an earlier unresolved invocation; the queue cannot infer that the earlier remote attempt was not applied.

### Replay outcomes

| Replay event | Record behavior | Queue consequence |
|---|---|---|
| Observed `applied` | Settle the record as `applied` | Release its retained request payload when no longer needed for waiter/accounting cleanup |
| Observed retryable failure | Schedule a new distinct invocation after backoff if the deadline and invocation cap allow it | Record remains at the Region FIFO head |
| Observed terminal failure | Settle from the record's full history; do not retry this record | Advance the Region queue when local cleanup is complete |
| Whole-RPC failure with definitive per-entry outcomes | Apply the corresponding per-entry result to each attempt | Evaluated per record/invocation |
| Whole-RPC/transport failure without a definitive result | Mark this invocation `issue_unknown`; a distinct invocation may be scheduled after backoff | The unknown invocation remains in history and may still apply remotely |
| Transient route lookup failure | Defer with backoff; do not authorize a replay RPC | No new invocation is created |
| Transient replay-memory reservation unavailable | Keep the record at the Region FIFO head and defer with bounded backoff; do not begin the corresponding encode/materialization work | No invocation is created and the invocation cap is not consumed |
| Same `RegionId` remains valid and only route/peer information changes | Refresh routing and replay to the same Region | Record identity and FIFO position are retained |
| Original Region is absent, table identity/version changes, or `partition_expr_version` changes | Stop further replay as `stale_target`; do not rebuild or redirect rows to another Region | If an earlier invocation remains unresolved, final settlement remains `issue_unknown` |
| Region exists but has no leader during replay | Retain the record at the Region FIFO head and defer until the next bounded backoff timer | No new invocation is created; v1 does not require an immediate wake-up from leader-cache or watch notifications |
| No leader during initial prepare before the first RPC | No queue record is created | Outside v1 queue admission and outside this retry contract |
| Replay RPC timeout | Classify the invocation `issue_unknown`; a distinct invocation may be scheduled only if the deadline and invocation cap allow it | The remote operation may still apply after the local timeout |
| Retry deadline reached before authorization | Stop scheduling and settle from recorded history | Existing unresolved invocation history remains effective |
| Retry deadline reached while an invocation is in flight | Authorize nothing further; allow the in-flight RPC until its own timeout | If no result is observed, the invocation becomes `issue_unknown` |
| Per-record invocation cap exhausted | Stop new authorizations and settle the record from recorded history | No new invocation is issued |
| Worker failure before authorization | Finalize the record from its existing history; do not invent an unknown invocation or automatically restart the replay | No send-side unknown is created |
| Worker failure after authorization without an observed result | Classify that invocation as `issue_unknown` and finalize the record from its history | A late remote application remains possible |
| Client cancellation | End the client wait only; queue work continues | No change to record ownership |

A **definitive per-entry first-round result** means the dispatcher has an explicit disposition for each Region entry, either success or an entry-level failure. A whole-RPC transport error without per-entry results is not definitive and remains unresolved first-round provenance; it does not turn a retryable failure into a successful write.

Replay classification uses the propagated `RetryHint`, not the status code alone:

- `Retryable` schedules another invocation to the same Region after backoff.
- `RetryAfterRouteRefresh` refreshes routing and metadata, then validates that the original Region is still the target before another send.
- `NonRetryable` is terminal for that record.

This distinction matters because `RejectWrite` and `RejectWriteInStaging` can share a storage-unavailable status while carrying different retry hints. A normal Mito write-buffer stall may retain the request in the Datanode without returning an immediate retryable response. If the queue's local replay timeout expires while waiting, the invocation becomes `issue_unknown`; timeout does not prove that Mito cancelled or discarded the request.

### Retry budget and per-record limits

The absolute retry deadline is `admission_time + write_queue.retry_budget`. Backoff, route lookup, metadata refresh, and leaderless deferral consume the deadline but never extend it. An invocation authorized before the deadline may run until its own `write_queue.replay_rpc_timeout`, which starts at authorization; record cleanup may therefore extend beyond the retry deadline.

The existing jittered exponential backoff helper is reused for replay scheduling. `write_retry.max_backoff` caps the base delay before positive jitter; the final delay may approach twice that cap (approximately 20 seconds for a 10-second base cap). Backoff progression is per record. A Region with no leader during replay is deferred until the next bounded backoff wake-up and does not consume a replay invocation.

A per-record invocation cap limits the total number of replay RPCs authorized for one record. It is independent of the retry deadline: whichever is reached first stops new authorizations. v1 uses a provisional fixed cap of eight replay invocations per record; this finite bound must be validated against the configured retry window and representative failure workload before enabling the queue.

`write_queue.max_records` bounds the number of concurrently retained request records across the Frontend queue. The legacy `write_retry.max_retries` applies only to calls that remain on the existing retry path.

### Worker failure, cancellation, and shutdown

The queue maintains a single active replay owner for each claimed record. A worker failure before send authorization creates no replay invocation; the record is finalized from its existing history and the failed replay is not automatically restarted. If a worker fails after authorization and no definitive result was observed, that invocation is `issue_unknown`; the record is finalized from its recorded history and is not silently treated as unsent. In either case, a worker failure may terminate the record before its retry deadline or invocation budget is exhausted; v1 does not automatically reclaim and reschedule that record.

A panic or process failure during synchronous admission is not recovered by v1. The publication transaction must be structured so that no panic-prone work or `await` occurs between its final validation and all-or-nothing state publication; reservations use RAII or equivalent cleanup. A panic must never leave a record visible without its retained charge.

Client cancellation before publication aborts admission as described above. After publication, cancellation ends only the client wait. Queue shutdown stops new replay authorization and finalizes active replay work from its recorded history. It classifies authorized in-flight invocations without observed results as `issue_unknown`. Shutdown does not drain the queue, recover records, or prove that in-flight remote requests did not apply. Forced process termination may discard all remaining queue state.

## Routing and Same-Region Validation

### Metadata validation and route refresh

At planning time, each eligible attempt captures the table identity and metadata needed to decide whether the original routed request remains valid: `TableId`, `TableIdent.version`, engine, effective `RegionOptions.append_mode`, instant-TTL state, the observed plan-time Flow-cache status, the original `RegionId`, and `partition_expr_version`. The partition-expression version is available only for a non-empty partition expression. Any Region with `partition_expr_version = None` is excluded, including default Regions of otherwise partitioned tables. `TableIdent.version`, metadata-store version, and route-bookkeeping `route.version` are distinct concepts. `route.version` is a bookkeeping counter that can change for leader/route updates and Region reservation; it is not Region identity and must not be used as a replay fence. For supported partitioned tables, the original `RegionId` plus its `partition_expr_version` are the same-Region identity checks.

Replay reloads current table metadata and route information and rejects the record if the table identity, `TableIdent.version`, engine, effective append-mode state, instant-TTL state, original `RegionId`, or `partition_expr_version` no longer matches the captured request. A peer/leader change is allowed when the original Region still exists and the same-Region identity checks pass. A `route.version` change alone must neither admit nor reject a record. Flow-source status is not re-read; the captured cache observation may be stale and no `TableIdent.version` fence for Flow creation/removal is assumed. The implementation must obtain a coherent current view of route, partition metadata, and Region state. If it cannot establish that the values being compared describe the same current target, it must fail closed. This validation is not an atomic DDL fence and does not prove that an earlier request was not applied before its response was lost.

A transient metadata or route lookup failure is deferred with backoff and is not treated as proof that a Region was removed. When the response indicates a stale route, replay refreshes metadata and routing before deciding whether the original Region remains valid:

1. Group refresh candidates by captured `TableId` and fully qualified table name. Invalidate the route and table metadata entries once for each distinct table in the refresh wave.
2. Invalidate each distinct name-based metadata entry once, then resolve each distinct fully qualified name once for that wave. Use batch name resolution when available; otherwise use bounded single-flight lookups so multiple records for the same name do not issue duplicate refreshes.
3. Verify for every candidate that the resolved name still maps to its captured `TableId`; never replay an old request against a newly created table that reused the name. A missing name or changed identity is terminal for the affected record.
4. Load current table metadata and route. Continue only if the original `RegionId` and `partition_expr_version` remain valid. Do not invoke a new partitioner to remap rows to other Regions.

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

The `TRUNCATE` row above describes a hard v1 limitation: without a separate data-generation marker, workloads that require truncation to fence earlier queued writes must remain queue-disabled. Unpartitioned Regions and default Regions with `partition_expr_version = None` are also excluded because no in-band partition-version check is available. For eligible partitioned Regions, `partition_expr_version` is the in-band partition-expression check. `DROP`/recreate is rejected by the new `TableId`; enterprise `UNDROP` behavior must be validated before enablement in deployments using that feature.

The route `version` is a bookkeeping counter, not a Region-layout identity or partition-generation token. Replay must not reject a record solely because the current route `version` differs from a captured value. It must refresh the current route and validate the captured `TableId`, applicable `TableIdent.version`, original `RegionId`, and `partition_expr_version` from one coherent metadata view; if that view cannot be established, replay fails closed.

A route or partition-expression change can leave the original invocation's remote outcome unknown. v1 does not rebuild or redirect the request after such a change, but the old request may still have been applied before its response was lost. The queue does not add a Datanode-side idempotency or stale-write fence.

### Same-Region route refresh

If refreshed metadata confirms that the same `TableId`, `TableIdent.version`, `RegionId`, and `partition_expr_version` remain valid, replay may update the peer/leader routing information for that same Region. This refresh preserves record identity, first-round provenance, invocation history, retry deadline, per-record invocation budget, FIFO position, and waiter. It does not change the captured plan-time table or Region snapshot.

If the original Region is absent, or the table/partition identity changes, replay stops further sends and classifies the replay path as `stale_target`. If an earlier invocation remains unresolved, the record retains the `issue_unknown` settlement because the old request may still have been applied. Replay does not rebuild the rows, split the request, or enqueue work under a new Region key. This is the explicit v1 boundary for repartition and cross-Region movement.

## Resource Limits and Operations

### Retained and transient memory

`write_queue.max_bytes`, `write_queue.max_records`, and `write_queue.max_record_size` are independent admission limits; the first one reached rejects the admission. `max_bytes` is the aggregate retained-accounting ceiling, `max_records` is the number of records whose request payloads remain retained, and `max_record_size` is the per-record protobuf wire-size preflight ceiling. With the initial `512 MiB` and `100000` values, the aggregate byte ceiling would allow an average charge of about `5 KiB` if the count ceiling were fully occupied; this is only an arithmetic reference, not a per-record size guarantee. These limits bound the queue's defined accounting units, not process RSS or peak allocation.

A retained record keeps the original routed per-Region request; admission does not encode/decode the payload or allocate a second full request copy solely for queue retention. The accounting charge is defined as the sum of: (1) `encoded_len()` of the retained per-Region `RegionRequest`, used as a request-payload size proxy; (2) the fixed record and queue-index entry sizes; (3) the captured plan-context and owner/attribution metadata sizes; and (4) the fixed-size first-round provenance plus one bounded invocation-history entry for each authorized replay RPC. Invocation history stores bounded error classes and `RetryHint` values, not arbitrary unbounded error strings or cloned request payloads. The invocation cap therefore also bounds per-record history growth. The selected client error remains owned by the request-completion path and is not retained as an unbounded error object in the queue record.

The request-payload component is a wire-size proxy, not a measurement of retained Rust heap capacity. The total charge is an explicit accounting model and does not claim to represent exact allocator, runtime, index, or transport memory. It must be computed before publication and reserved atomically with the record; if the reservation cannot be obtained, admission is rejected. `max_record_size` checks only the per-Region request wire-size proxy and is separate from the full per-record `max_bytes` charge. The retained and transient ledgers are separate controls, not one shared byte ceiling; their configured maxima plus runtime overhead define the queue's peak-memory planning envelope and must be selected together against the Frontend memory budget. `max_records` counts records while their request payloads remain retained; after terminal payload release, only bounded waiter/diagnostic state may remain and it must not retain the request payload or unbounded history.

`max_record_size` is checked for every split Region request after per-Region splitting and before dispatch. The preflight uses `prost::Message::encoded_len()` to obtain the protobuf wire length without allocating a second encoded buffer; the entire call completes this check before any Region is sent. If any Region exceeds the limit, the whole call is preflight-ineligible and retains its existing retry owner where supported. This size proxy cannot prevent the original first-round request allocation and is not a bound on retained Rust heap size or process RSS. A size or capacity rejection after a call has been assigned queue ownership is never converted into inline retry.

The record charge is recomputed or reduced when request payload ownership is released, while any compact completion state retained for a waiter remains separately bounded. Invocation history cannot exceed the configured per-record invocation cap plus the single first-round provenance entry.

Transient memory uses a separate per-Frontend ledger for replay-side encoding buffers, transport wrappers, and any measured body copies. The provisional internal budget is 64 MiB (not a public TOML parameter); its value must be validated against representative request shapes, concurrency, and the Frontend memory budget before enablement.

A transient reservation remains charged while the allocations it covers remain live, or until ownership is transferred to retained-record accounting. Accounting must follow allocation lifetime rather than task/future lifetime alone: cancellation or timeout may initiate cleanup, but a reservation cannot be returned while buffers it covers are still reachable or being dropped. Reservation conversion and cleanup must be single-owner operations and must happen exactly once. These estimates are accounting models, not RSS formulas. Reservation failure does not evict retained records.

### Replay scheduling and fairness

Each Frontend queue has `write_queue.replay_concurrency` replay slots and permits at most one locally in-flight replay record per Region. Runnable Regions are scheduled round-robin among those currently ready. A Region waiting for backoff or a leader is not runnable until that condition changes. There is no per-Region retained-capacity reservation in v1: one Region with a prolonged no-leader backlog can consume the Frontend-wide retained budget and cause admission rejection for otherwise healthy Regions.

This gives currently runnable Regions an opportunity to make progress; it does not provide a bounded waiting time or per-record latency guarantee. A hot Region remains serialized, and its tail delay grows with its backlog. All queue limits, transient memory, and replay concurrency are per Frontend, not cluster-wide. The queue does not provide fairness for retained-memory admission across Regions; round-robin replay scheduling does not prevent one Region from exhausting the shared retained ledger.

### Drain-time model and workload envelope

The queue has both a per-Region serialization limit and a Frontend-wide concurrency limit. A useful optimistic lower bound for a Region with `K_i` queued records is:

```text
T_region >= K_i * (L_i + D_i)
```

where `L_i` is the average cumulative RPC service time per record for the declared failure mix (including any repeated invocations), and `D_i` is the average cumulative delay per record from backoff or leaderless deferral. In the single-attempt, no-retry case, `L_i` is simply the mean replay RPC service time. Across `N` records, when at least `replay_concurrency` Regions are runnable and cumulative service times are approximately uniform, the aggregate lower bound is:

```text
T_frontend >= (N / replay_concurrency) * L
```

These optimistic lower bounds omit route lookup, metadata refresh, scheduler overhead, transport variance, and failures beyond those represented in the workload averages. The aggregate bound uses cumulative RPC service work and excludes periods when replay slots are idle. The per-Region bound can dominate even when the Frontend has idle replay slots; increasing global concurrency cannot accelerate a hot Region whose records must be replayed serially.

For any proposed deployment envelope, substitute the measured replay RPC service-time distribution and expected backlog into these bounds. Do not infer that `max_records = 100000` is a supported drain capacity or that the initial `5m` retry window can drain a fully populated queue; a single hot Region may dominate even when global replay slots are idle.

The validation workload envelope should state total backlog, per-Region depth distribution (including the hottest Region), replay RPC latency distribution, no-leader fraction, and expected failure mix. Report time-to-first-authorization, the fraction authorized before the retry deadline, total drain time, hottest-Region drain time, deadline expiries, and transient-budget rejections. Use these results to evaluate and tune the starting configuration values before rollout.

### Observability

Operational visibility should let operators answer the following questions:

- How much work is retained, and how close is the queue to its retained and transient capacity limits?
- Are failed attempts being admitted, rejected, replayed, or given up, and what broad reasons explain rejection or give-up?
- Are replay attempts making progress, reaching terminal outcomes, or expiring at the retry deadline?
- Are replay latency or worker failures preventing the queue from making recovery progress?
- Is one Region's retained backlog causing Frontend-wide admission rejection?
- Are stale-target terminations occurring because table, Region, or partition identity changed?

Retained record/byte gauges and admission-rejection counters should be available at Frontend scope. Region/table identities remain in structured logs rather than metric labels to avoid high-cardinality metrics; rejection and backlog diagnostics should include the relevant Region/table identity in logs where available. Replay counters should distinguish applied, observed failures, unknown outcomes, stale-target termination, timeout, and deadline exhaustion with bounded reason dimensions. The queue should expose enough structured context to diagnose a record without introducing a high-cardinality metrics dimension.

## Configuration and Rollout

The values below are **provisional starting defaults for discussion**, not validated production settings. The RFC defines the bounded behavior and required controls; final numbers depend on workload measurements and maintainer approval. The transient-memory budget is an internal hard limit and is intentionally not exposed as a TOML setting.

Public settings use the `write_queue.` prefix. Values below are provisional test points, not benchmark-derived production defaults. The invocation cap and transient-memory budget are internal v1 hard limits, not TOML knobs; they must remain finite and be validated before enablement.

| Parameter | Scope | Purpose | Initial value / status |
|---|---|---|---|
| `write_queue.enable` | Per Frontend | Enable queue ownership for supported distributed insert paths | `false` by default |
| `write_queue.max_bytes` | Per Frontend | Retained request payload and record-history accounting | 512 MiB, initial value |
| `write_queue.max_records` | Per Frontend | Maximum currently retained request records | 100000, provisional test point |
| `write_queue.max_record_size` | Per record | Preflight eligibility limit for a retained request | 512 KiB, initial value; checked with `encoded_len()` before the first RPC |
| `write_queue.retry_budget` | Per record | Additional replay window from admission | 5m, initial value; not a deployment recovery guarantee |
| `write_queue.completion_wait` | Per client request | Maximum synchronous wait; `0` means no wait | 30s, initial value |
| `write_queue.replay_rpc_timeout` | Per invocation | Queue-side timeout applied to the replay RPC future after authorization; independent of the Frontend-to-Datanode channel timeout | 10s, initial value |
| `write_queue.replay_concurrency` | Per Frontend | Global replay slots | 8, initial value |
| `write_retry.max_backoff` | Existing retry configuration | Maximum replay scheduling backoff | Existing setting reused as the pre-jitter base-delay cap; final wait may approach twice this value |

With queue mode enabled, `write_retry.max_retries` does not control attempts transferred to the queue. Those attempts use `write_queue.retry_budget`, the per-record invocation cap, and replay backoff. Attempts classified before the first RPC as queue-ineligible retain their existing retry configuration and behavior where supported. The production Frontend-to-Datanode channel has no per-request timeout; `write_queue.replay_rpc_timeout` is a queue-owned timeout around the replay RPC future. Its expiry bounds local waiting but does not cancel a request already accepted by the Datanode.

`write_queue.enable` is a per-Frontend switch with no v1 table/database allowlist. It applies to every queue-eligible direct insert routed through that Frontend. Operators cannot use it to enable the queue for one table while keeping another table on queue-disabled behavior within the same Frontend. For mixed or high-risk workloads, use a dedicated Frontend pool and route only the intended workloads to it, or keep the queue disabled. Workload owners must assess DDL and duplicate-write tolerance for every table routed through an enabled Frontend; one high-risk table is enough to make that shared Frontend unsuitable unless the table is isolated to a separate queue-disabled pool. Configuration is loaded at startup; hot reload is not part of v1. Disabling the queue requires a configuration change and process restart. Shutdown does not promise to drain or recover retained records.

### Rollout and operational guardrails

- Roll out by distributed Frontend pool, not by table. Start with a dedicated canary Frontend pool and route only workloads whose duplicate-write and DDL behavior is compatible with their application requirements. The feature has no per-table allowlist or per-table gray switch.
- Standalone deployments remain outside v1; do not enable or instantiate the queue there.
- Keep tables that require strict `TRUNCATE`/DDL ordering, or cannot tolerate a late stale replay, on a separate Frontend pool where the queue remains disabled. A queue-enabled shared Frontend cannot isolate one such table at runtime.
- v1 has no per-table drain or DDL fence. Disabling the queue requires restart, and shutdown/restart may discard retained in-memory records; disabling or restarting is not a drain protocol and must not be treated as proof that no queued write can still be applied.
- During a canary, monitor Frontend retained bytes/records, admission rejection reasons, time-to-first-authorization, deadline expiry, hottest-Region depth, stale-target terminations, and replay timeouts.
- For a deployment that cannot accept the `TRUNCATE` blind spot or loss of retry recovery after repartition, keep the queue disabled for that workload.

`write_queue.enable = true` is blocked until all of the following are complete:

- The supported non-tracked direct-insert path selects an explicit per-call retry policy before the first RPC. Shared dispatcher users cannot inherit a global queue mode. A call is queue-owned only if every Region attempt is eligible; otherwise the whole call retains its existing retry owner where supported. The handoff occurs after first-round classification and before additional inline retries.
- Calls routed through `ctx.confirmed_writes()` / `execute_tracked` remain outside v1 and retain their existing tracked retry behavior. Tests verify that these calls, table/logical batcher flushes, Delete, BulkInsert, and other excluded producers cannot inherit queue ownership. No queue result is represented through the current aggregate `WriteCompletion` contract in v1.
- Plan-time metadata is captured and propagated through request grouping, per-Region splitting, and queue admission. First-round provenance (definitive per-entry, whole-RPC unresolved, or single-entry-collapsed) remains explicit through classification. The first-round collector preserves retryable siblings before terminal-error aggregation; it must preserve retryable sibling outcomes even when a terminal sibling determines the client-visible error.
- Replay refreshes route and metadata, verifies table identity/version and the original `RegionId`/`partition_expr_version`, and never redirects or rebuilds the request to another Region.
- Per-Region FIFO scheduling, send authorization, record settlement, worker failure, cancellation, timeout, and shutdown behavior pass the validation plan below.
- The pre-first-RPC `max_record_size` check using `encoded_len()` is applied to every split Region request for each enabled request path.
- The provisional per-record invocation cap and internal transient-memory budget are finite hard limits and are validated against the declared workload envelope.
- Transient reservations are validated against representative replay encoding and transport-buffer shapes. Reservation release follows the actual lifetime of covered allocations, including cancellation and timeout paths.
- The DDL residual risks, duplicate-capable retry semantics, Flow-cache freshness limitation, worker-failure record termination, and loss of cross-Region repartition recovery are documented. Maintainers and deployment owners explicitly accept the applicable residual risks before enablement.
- The client contract is documented and covered by tests, including whole-call existing retry behavior when any Region attempt is preflight-ineligible. Failure-path and drain measurements are reported against the declared workload envelope.
- Deployments requiring table/business isolation use separate Frontend pools; otherwise the queue remains disabled for the affected workloads.
- `DROP`/recreate and `UNDROP` replay identity tests are complete, and the `TRUNCATE` data-generation-marker option has been evaluated. If no marker is implemented, workloads that cannot tolerate the documented `TRUNCATE` blind spot remain queue-disabled.

A future change to the shipped default requires a separate maintainer decision based on completed integration and client-contract tests, failure-injection coverage, same-Region routing/DDL validation, representative resource and latency measurements, canary results, and explicit acceptance of the remaining duplicate-write and DDL limitations. Queue enablement for a particular deployment does not by itself justify changing the shipped default.

## Risks and Limitations

- **Duplicate-capable retry.** A lost response, timeout, worker failure after submission, or client retry can cause duplicate writes. Neither a retryable response nor a later successful invocation proves that an earlier unknown invocation did not apply.
- **Partial batch outcome.** A batch may return an error because one sibling has a terminal failure while a retryable sibling is admitted and applies later. The client can therefore observe a failed batch whose retryable subset is partially applied. Admitted records may also apply after the client wait expires; a batch error does not mean that no rows were written.
- **No inline fallback for queue-owned rejection.** A queue-owned attempt rejected by capacity, queue closure, or an invalid admission sidecar receives no Frontend retry after the first round. Preflight-ineligible calls retain their existing retry owner where supported.
- **Process-local and worker-failure loss.** Restart, crash, shutdown, deadline expiry, or a replay worker failure can terminate admitted work before the retry deadline or invocation cap is exhausted. Worker failures do not trigger automatic record reclamation or rescheduling in v1. Queue contents are lost on process restart.
- **Repartition recovery is out of scope.** If the original Region disappears or the partition-expression version changes, the record settles as `stale_target` and is not rebuilt or redirected. A write may therefore remain unapplied after a repartition even when the current routing could identify new destination Regions. v1 intentionally does not provide cross-Region recovery.
- **Remote application order is not guaranteed.** Per-Region FIFO controls local scheduling and send authorization only. An earlier invocation classified `issue_unknown` may still apply after a later invocation or live write. v1 has no Datanode-side fence to prevent that late application.
- **DDL stale-replay window.** Metadata revalidation is not an atomic fence. A client may receive a write error, run `TRUNCATE`, and then have a queued replay reinsert rows that the client expected to be removed. `TRUNCATE` does not increment the captured `TableIdent.version`, so this version check cannot identify the truncate. `ALTER` can race between validation and RPC submission; cross-Frontend metadata propagation is asynchronous. `DROP` followed by create under the same name receives a new `TableId` and must be rejected by the captured-identity check. `UNDROP` restores stored table metadata; whether an old queued record can pass the complete replay checks after restoration must be established by a regression test. v1 has no atomic DDL fence. If no separate truncate/data-generation marker is implemented, workloads that require `TRUNCATE` to fence older writes must remain queue-disabled.
- **Cross-Region retained-capacity starvation.** Retained limits are Frontend-wide and v1 has no per-Region retained cap. One Region with a prolonged no-leader backlog can consume the shared byte/record budget and cause admission rejection for healthy Regions. Round-robin replay scheduling does not prevent this admission-time capacity starvation.
- **Resource accounting is not an RSS bound.** Retained and transient ledgers constrain their defined accounting units, not every Frontend allocation or total process memory. Replay buffers must reserve before allocation; accounting release must follow the lifetime of covered allocations.
- **Hot-Region drain.** Per-Region serialization means global replay concurrency cannot shorten the tail of a single hot Region. Records near the end of a backlog may expire before their first authorization. The retained 5m retry budget is not a guarantee that a full queue or hot Region will drain within that window; drain-time and deadline-expiry behavior must be evaluated against the declared workload envelope.
- **Initial no-leader requests are not queued.** A request that finds no leader during prepare before its first RPC is sent is outside this queue's admission contract. The queue does not guarantee recovery for that case.
- **Flow-cache eligibility window.** Queue eligibility uses the plan-time `TableFlownodeSetCache` observation. Because event-driven invalidation is not a generation fence, a stale-yet-present cache entry may classify a Flow-source table incorrectly. v1 cannot detect every such stale observation; enablement requires explicit maintainer acceptance, and workloads that cannot tolerate this window must remain queue-disabled unless a freshness check is added.
- **Standalone is not covered.** The v1 contract applies only to distributed Frontends.

## Validation Plan

Validation should establish the behavior of the design rather than prescribe specific internal data structures or test harness APIs.

- **Dispatcher ownership and request reuse:** with the queue disabled, existing behavior is unchanged. With queue mode enabled, a direct ordinary physical-insert call uses queue handoff only when every Region attempt in that call satisfies plan-time eligibility; if any attempt is preflight-ineligible, the entire call retains its existing retry owner where supported. Verify explicit per-call policy at shared dispatcher call sites so batchers, bulk insert, Delete, and standalone do not inherit queue ownership. Verify the pre-first-RPC size gate for each enabled shape; do not treat an enqueue-time size check as preflight. Verify partial-result collection is explicitly requested by queue policy and does not depend on `retry.is_some()`. Verify the send boundary emits `PerRegionPartial`, `PeerBatchRpcUnknown`, or `SingleEntryCollapsed` provenance before `BatchRequestSnafu` wrapping; verify exact `RoutedWrite`/owner preservation. Verify `merge_attempts` cannot discard retryable siblings before the `FirstRoundReport` is formed. First-round `applied` attempts are never queue-admitted or replayed. Whole-RPC unknown and collapsed single-entry tests must verify duplicate-capable replay only when exact per-Region requests and explicit provenance survive; otherwise they must verify fail-closed no-admission. Queue-owned attempts rejected after selection never fall back to the existing retry path.
- **Request scope and eligibility:** verify distributed Frontends are the only supported deployment mode and Standalone does not use the queue. Verify that effective `RegionOptions.append_mode` is captured from the Region write settings; auto-created log/trace primary and exemplar tables with append mode are excluded, while trace auxiliary tables are evaluated by their own effective options. Verify every Region represented by `partition_expr_version = None` is rejected, including default Regions of otherwise partitioned tables. Verify Flow-source eligibility uses the plan-time cache observation and does not treat `TableIdent.version` as a generation fence for Flow changes; document that stale-yet-present cache state is a residual eligibility risk requiring explicit acceptance. Verify the plan snapshot includes the table/Region fields used by replay validation. Verify a Region request known to exceed `max_record_size` before the first RPC makes the entire call preflight-ineligible; verify this does not turn a queue-owned post-first-round capacity rejection into inline retry. Verify missing or ambiguous required metadata makes the call ineligible before dispatch. Verify Delete request bodies are never queue-admitted.
- **First-round results and tracked-path exclusion:** verify that first-round applied Region attempts are never re-admitted, and that retryable siblings remain visible when another Region has a terminal error. Verify the supported non-tracked direct-insert path preserves the routed request and provenance through queue admission and caller completion. Verify calls selected by `confirmed_writes` remain on `execute_tracked`, retain the existing tracked retry behavior and `WriteCompletion` aggregation, and never enter the queue. Verify table/logical batcher tracked flushes also remain excluded. Verify that the handoff occurs before any `WriteRetry::retry` additional round and never from the post-exhaustion give-up path. Verify terminal-error precedence, stable `attempt_order` selection, response-time `affected_rows` snapshots (including replay success after the response), waiter registration racing with settlement, cancellation, and shutdown waiter behavior for queue-owned calls.
- **Replay history and authorization:** verify that a later `applied` invocation does not erase an earlier `issue_unknown`, and late responses do not rewrite terminal invocation outcomes. Verify table, route, leader, and client lookups complete before send authorization; no additional lookup or `await` occurs between authorization and issuing the RPC. Confirm that authorization recorded before an unobserved worker failure is classified as `issue_unknown`, not `not_sent`. Verify the replay invocation cap and absolute retry deadline stop new authorizations. Exercise deferred route and leader retries without consuming an invocation.
- **Same-Region routing and DDL:** start with stale caches and changed peer/leader routing while keeping the same Region identity; verify cache invalidation and replay to that same Region. Change `route.version` through route bookkeeping, leader updates, and Region-number reservation and verify that this counter is never used as a Region identity fence. Change the table identity, `TableIdent.version`, original `RegionId`, or `partition_expr_version`; verify the record settles as `stale_target` and no request is rebuilt or sent to a different Region. Verify whether route, partition metadata, and Region state can be read from one coherent current view; if not, verify fail-closed behavior. Verify that repartition does not increment `TableIdent.version` but does change the partition-expression version for eligible partitioned tables. Verify `DROP`/recreate rejects the old `TableId`; exercise enterprise-gated `UNDROP` and verify whether a pre-drop record can pass the entire replay validation path; confirm that `TRUNCATE` does not change the captured `TableIdent.version` and that no unimplemented data-generation marker is claimed.
- **Ordering and unknown outcomes:** verify per-Region FIFO scheduling and at most one locally in-flight replay record per Region. Exercise a timed-out or unknown earlier invocation followed by a later queued record and document that local FIFO does not prove remote application order. Confirm that a late remote application remains possible after the Frontend has classified the invocation as `issue_unknown`; do not claim that the queue prevents late application or stale-write effects. Verify that a request failing during initial prepare because no leader is found produces no queue record; separately verify that a replay record with no leader after admission remains deferred at its Region FIFO head.
- **Resource and failure behavior:** force retained, transient, record, and invocation limits to exhaust. Verify the retained charge includes the request-payload proxy, fixed record/index metadata, plan context, first-round provenance, and bounded invocation history. Verify reservations are not leaked or double charged. Exercise cancellation, timeout, worker failure before and after send authorization, admission panic/failure around publication, and shutdown cleanup; verify worker failure finalizes rather than automatically reschedules the record, and confirm reservation release follows the actual lifetime of covered buffers, not merely future/task cancellation. Fill the retained budget with one no-leader Region and verify the client-visible rejection behavior for another healthy Region; do not infer cross-Region fairness from round-robin replay.
- **Flow mirror behavior:** verify `FlowMirrorTask::new` failure occurs before the first Region RPC and verify detached per-peer mirror failures are logged/counted without being awaited. Verify queue replay never launches the mirror a second time and that the queue does not claim to repair a missed mirror.
- **Mito timeout and error classification:** separately exercise a write-buffer stall that remains pending in Mito, a fast `RejectWrite`, and `RejectWriteInStaging`. Verify classification from the propagated `RetryHint`, not status code alone. Confirm that local replay timeout records `issue_unknown` and does not imply remote cancellation. Verify that `max_backoff` caps the pre-jitter base delay and that the final jittered wait may exceed that base cap.
- **Performance and workload envelope:** compare queue-disabled and queue-enabled behavior under the same request shapes, concurrency, and failure injection. Measure successful-path overhead from plan-context capture separately from failure-path overhead added during request completion by classification, admission, reservation, and waiter setup. Report the incremental p99 overhead of first-round queue classification, admission, reservation, and waiter setup against the queue-disabled first-round baseline under the same injected failure result. Exclude remote recovery time, replay backlog wait, and the existing retry tail. A 5% overhead target may be used as an initial discussion point, but it is not an established acceptance threshold; maintainers should set or explicitly waive a threshold after representative workload measurements. Report end-to-end failure-path p95/p99 separately, and report successful-path plan-snapshot overhead separately. Measure queue-state/ledger lock wait and hold time at 1, 8, and 64 concurrent actors, together with throughput. For the failure path, report p95/p99 request latency, admission latency, retained/transient memory, per-Region depth/skew, RPC latency distribution, time-to-first-authorization, fraction authorized before deadline, drain time, and deadline expiry. Use the lower-bound model above to interpret results and define the workload envelope required for enablement.

## Open Design Questions

- **Coherent metadata view:** Can the table identity, route, partition rule, and Region metadata required for replay validation be obtained as one coherent current view? If not, the implementation must fail closed when it cannot prove that these values describe the same target.
- **Flow-cache freshness:** Is the residual risk of a stale-yet-present plan-time `TableFlownodeSetCache` observation acceptable for v1, or is a freshness/generation check required before enabling the queue?
- **TRUNCATE protection:** Should v1 add a data-generation marker to prevent queued writes from being replayed after `TRUNCATE`, or explicitly accept this residual risk and require affected workloads to remain queue-disabled?
- **UNDROP behavior:** Does enterprise `UNDROP` restore metadata that would allow a pre-drop record to pass the complete replay-identity checks? The full path needs a regression test before enabling the queue in deployments that use `UNDROP`.
- **Resource and workload envelope:** Are the retained and transient memory budgets sufficient for the intended workload? What backlog, hottest-Region depth, deadline-expiry rate, drain-time, and first-round latency overhead should gate deployment-specific enablement?

## Alternatives and Future Work

A post-existing-retry buffer is not used because it would introduce a second scheduler and retry budget for the same write. The v1 queue instead takes explicit ownership of first-round Region failures whose requests and provenance survive lossless classification; calls classified as queue-ineligible before the first RPC remain with their existing retry owner where supported. For a call already assigned to queue ownership, a failure shape whose request/provenance cannot be recovered is returned without inline fallback; v1 must not guess or silently transfer ownership back to the legacy loop.

Cross-Region repartition recovery is intentionally excluded from v1. A future RFC may evaluate rebuilding the original materialized rows against a refreshed partitioner and admitting them to new Regions. Such a design must separately define parent/descendant client completion, partial successor admission, nested repartition, destination ordering, resource reservation before row-selection and payload materialization, and the behavior of unknown old-Region RPC outcomes. It must not assume that local timeout or route refresh proves the old request was not applied.

Durable retention, restart recovery, region-side idempotency, deduplication, and a stronger DDL fence require additional protocol and storage work and are deferred to separate designs.
