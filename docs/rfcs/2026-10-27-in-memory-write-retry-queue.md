---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
Status: Draft for discussion
---

# In-Memory Frontend Write Retry Queue

## Summary

This RFC proposes an optional, per-Frontend in-memory retry queue for eligible transient write failures that may outlast the existing inline retry window. v1 covers direct ordinary inserts targeting Mito-backed physical Regions. Prometheus remote write is eligible only when it reaches that same physical write path and satisfies the same admission rules. Metric logical-row inserts that still target logical Metric-engine Regions are not eligible in v1; the current code path does not normalize them to the supported Mito physical-Region path. Metric-engine logical-batcher flushes, pending-rows/table-batcher flushes, Delete, BulkInsert, and Flight `DoPut` remain outside the queue and retain their existing retry behavior.

Retry policy is selected explicitly per `execute` call at the producer/dispatcher boundary, using plan-time context captured before the first write round. A call is assigned queue ownership only when every Region attempt in that call is queue-eligible; if any Region attempt is ineligible, the entire call remains on its existing inline retry path. Once a call has been assigned to queue ownership, admission rejection because of capacity or queue closure is fail-closed: it receives no inline fallback. Queue mode must preserve per-entry first-round outcomes and provenance before any batch-level error merge can discard retryable siblings.

The queue performs bounded, best-effort replay with per-Region FIFO scheduling, FanOut-local destination barriers, an absolute retry deadline, retained-capacity limits, and cumulative limits on FanOut records and replay invocations. FIFO describes local scheduling and send-authorization order within one Frontend queue's `RegionId` key; it does not prove remote application order. Successors belonging to one FanOut retain ordinal order within the protected destination segment. v1 does not provide old-before-new ordering between independent source-Region lineages that later converge on one destination. This draft explicitly supersedes the earlier author response in the K8 discussion that proposed a destination gate for that cross-source merge case; the gate is not part of this v1 design and requires explicit maintainer acceptance of the narrower ordering contract.

A destination barrier normally releases after its tracked FanOut lineage is quiescent, the relevant replay outcomes have definitive observed results, and every tracked first-round attempt in that lineage that was addressed to that destination has a definitive per-entry result. If an outcome remains unknown, the barrier becomes eligible for release at `lineage_retry_deadline + write_queue.replay_rpc_timeout`. Successor publication and barrier release are linearized under the same destination queue-state lock: after the lineage deadline or barrier release, a stale planning task cannot publish a successor. Expiry does not wait for local planning tasks to finish; their publication is fenced, and their transient reservations remain charged until the associated allocations are actually released. The release is recorded as `ordering_uncertain` and does not prove that the remote write was not applied.

The queue is process-local and best-effort. It does not provide durability, restart recovery, at-least-once or exactly-once delivery, proof that an unknown write was not applied, or guaranteed eventual application. It does not order against live writes, legacy-inline attempts, unadmitted first-round requests, or other Frontends. `write_queue.enable` is a per-Frontend switch, not a per-table or per-database allowlist; enabling it exposes all queue-eligible direct inserts on that Frontend to the v1 contract and residual risks. It is disabled by default and may be enabled only after the prerequisite write-path changes, implementation validation, resource limits, workload validation, and maintainer acceptance described below are complete.

## Motivation

The existing inline retry path has a bounded retry window. The baseline assumed by this proposal is `write_retry.max_retries = 3` additional rounds, with jittered exponential backoff starting around
1.5 seconds and capped by `max_backoff = 10 seconds`. The relevant Frontend-to-Datanode channel does not currently impose a per-attempt Frontend timeout. These values and behaviors describe the existing path, not a new queue guarantee.

The initial `write_queue.retry_budget = 5m` is intended as an additional replay window after queue admission for recovery from one common Datanode restart or one common single failover event. The clock starts at `admission_time`, after the first-round failure has been classified; it does not include time elapsed before admission. It is not a measured guarantee, does not promise recovery from repeated failovers or a prolonged partition, and must be validated against the deployment's actual recovery distribution before enablement.

A Datanode restart, failover, or partition change may outlast that inline retry window. A write RPC can also fail after the request has reached the Datanode but before the Frontend receives a
definitive per-Region result. In that case, retrying may duplicate a write, while dropping the attempt may leave a partially applied batch. A client-visible batch error therefore does not imply that
no rows were written.

This problem is especially relevant to a time-series database because a routed insert is tied to the table identity and the partition expression used to map rows to Regions. When a table is
repartitioned, replaying the original Region request unchanged is not sufficient: the current table metadata and partitioner may map the same rows to different Regions. A queue must therefore preserve
the plan-time context needed to decide whether the original request is still valid, can be retargeted, or must be rebuilt.

Issue #8085 asks for a bounded Frontend buffer with persistence. This RFC covers only the in-memory queue portion. Durable retention and restart recovery require a separate proposal.

## Goals and Non-goals

### Goals

- Extend retry handling for eligible transient insert failures beyond the existing inline retry window.
- Preserve the synchronous first write round while assigning exactly one retry owner to each failed Region attempt.
- Preserve per-entry outcomes and first-round failure provenance before batch-level result merging.
- Bound retained queue work, replay duration, FanOut expansion, replay concurrency, and temporary rebuild memory.
- Define client-visible behavior for mixed batches, partial admission, uncertain outcomes, and asynchronous replay.
- Rebuild requests against current table routing when a Region or partition expression changes.
- State the exact scope of destination ordering, the DDL and availability limitations, and the per-Frontend rollout boundary.

### Non-goals

- Durable queue storage, process-restart recovery, or guaranteed eventual delivery.
- Exactly-once or at-least-once write semantics, or proof that an unknown RPC was not applied.
- A Datanode protocol change for idempotency or deduplication.
- Replacing retry behavior for Delete, BulkInsert, Flight `DoPut`, metric logical-batcher flushes, or writes flushed by pending-rows/table batchers. These paths remain outside the queue and retain their existing retry behavior.
- Queue admission for Metric logical-row inserts that still target logical Metric-engine Regions. They remain on their existing retry path until a real normalization path produces eligible Mito physical-Region requests with the required plan-time context.
- A per-table or per-database queue enablement allowlist, hot configuration reload, or automatic workload isolation. The v1 enable switch is per Frontend.
- Ordering across Frontends, against concurrent live writes, legacy-inline attempts, or unadmitted first-round requests, or between different source-Region queues that may later rebuild into the same destination Region. The queue provides local per-Region FIFO scheduling and FanOut-local successor order only.
- An atomic DDL fence or a transaction boundary across a FanOut lineage.
- A dead-letter query API.
- Recovery from process-level loss or corruption of the queue's own coordination state. Individual replay worker failures are in scope and have the explicit handling described below; they are not covered by this non-goal.

## Request Scope and Retry Ownership

### Supported paths and retry-owner selection

The proposed handoff occurs after first-round outcomes have been collected for a supported direct-insert request. Eligibility is based on the plan-time target metadata and the normalized per-Region insert attempt, not solely on the external protocol or caller name. The retry policy for the entire `execute` call must be selected before the first write round so that a mixed-eligibility batch is not partially assigned to the queue and partially left on the legacy path.

| Request path | v1 behavior when `write_queue.enable = true` |
|---|---|
| Direct ordinary inserts targeting Mito-backed physical Regions, including SQL, gRPC, pipeline, COPY, logs, traces, and Prometheus remote write when it reaches this same path | Select queue ownership only when every Region attempt in the `execute` call is eligible; retryable failures are admitted independently after first-round classification |
| Direct ordinary insert call containing any Region attempt known at planning time to be ineligible because of table shape, engine, append mode, instant-TTL mode, Flow-source status, or another unsupported context | Keep the entire call on the existing inline retry path where supported; do not silently drop its retry owner |
| Metric logical-row insert that still targets a logical Metric-engine Region | Not queue-eligible in v1; retain its existing retry path until a supported Mito physical-Region normalization path exists |
| Metric-engine logical-batcher flush | Existing inline retry; remains outside this queue |
| Bulk insert, Flight `DoPut`, and pending-rows/table-batcher flush | Existing inline retry; remains outside this queue |
| Delete | Existing inline retry; remains outside this queue |

For v1, the supported queue target is a Mito-backed physical Region write. The current metric logical-row path reaches a logical Metric-engine Region; this RFC does not claim that it is normalized to a Mito physical Region. Supporting that path later requires a concrete normalization implementation, propagation of the same plan-time context, and separate validation before it is added to the supported scope.

The retry policy is selected at the producer/dispatcher boundary and is explicit for each `execute` call:

- **Queue policy:** every Region attempt in the call is a direct ordinary insert to a supported Mito physical Region and satisfies every queue eligibility rule below. The queue owns eligible failed attempts after first-round classification.
- **Legacy inline policy:** any Region attempt in the call is known before the first round to be outside queue eligibility, provided that the existing producer path supports inline retry. The entire call remains on the legacy path. This preserves existing behavior for append-mode, instant-TTL, Flow-source, unsupported-engine, missing-context, and other ineligible direct inserts.
- **Fail-closed queue admission:** once the call is assigned queue policy, an attempt rejected for retained-capacity exhaustion or queue closure, or whose supposedly valid queue sidecar is found malformed or inconsistent at admission, receives no inline fallback.

The selected policy must remain explicit when producers share internal insert-building logic. `WriteDispatcher` must not hold a dispatcher-global queue mode: inserter, bulk inserter, Deleter, and batcher callers share dispatcher infrastructure, so policy is supplied explicitly per `execute` call. For a direct-insert call containing multiple Region attempts, queue policy is selected only if every attempt is eligible; otherwise the complete call uses its existing inline policy. This avoids mixed retry owners inside one logical batch. A batcher must not inherit queue ownership merely because it reuses an ordinary insert helper. If a producer cannot preserve this distinction through its call path, it remains outside v1.

A single `execute` call must not mix queue and legacy-inline retry owners. If any attempt is ineligible at preflight, the whole call remains on the legacy inline path; if the whole call is queue-eligible, all retryable per-Region failures are classified for independent queue admission. This whole-call fallback is deliberately conservative: one ineligible target can keep otherwise eligible siblings on the existing inline path, but it preserves batch-level result semantics and avoids a new mixed-owner completion protocol. A queue-owned call that encounters capacity/closure rejection remains fail-closed and never falls back to inline retry.

### Queue-mode first-round behavior

Queue mode is a distinct per-call retry policy, not the existing no-retry dispatcher mode. It must preserve the first-round batching and result semantics needed for queue admission:

- The first-round request preserves per-entry/partial Region results independently of whether legacy inline retry is enabled for that call. In particular, `accept_partial_result` must not depend on `retry.is_some()`.
- Each failed entry is exposed as its corresponding per-Region attempt before any batch-level `merge_attempts` operation can discard retryable siblings when a terminal sibling exists.
- First-round classification retains explicit provenance: definitive per-entry failure, whole-RPC failure without per-entry outcomes, and single-entry-collapsed failure must remain distinguishable. Admission must not infer provenance from an error wrapper such as `BatchRequestSnafu` alone.
- The failed queue-owned attempt retains the original routed per-Region request for later replay. Queue retention does not encode/decode the request or create a second full payload copy; any encoded-size calculation is an accounting estimate only.
- A queue-owned attempt is sent once in the first round and does not enter the legacy inline retry loop. A legacy-inline attempt remains with its existing retry owner.

The current dispatcher has shared call sites and result-merging behavior that do not, by themselves, satisfy these requirements. The explicit dispatcher handoff contract and its implementation prerequisites are listed in Configuration and Rollout.

### Eligibility and plan-time context

Queue admission is evaluated independently for each failed single-Region attempt. An attempt may be assigned queue ownership only when it:

- is one ordinary physical-table insert request;
- targets a supported Mito-backed physical Region;
- carries a valid plan-time snapshot consistent with the target table and Region;
- has a non-empty `partition_expr_version` for the target used in the first-round request;
- is not append mode or instant-TTL mode;
- was not a Flow source at plan time; and
- satisfies the conservative identity-column impure-default rule below.

An impure default is materialized into the rows before dispatch. The retained request therefore carries the already-materialized values. Replay and FanOut rebuild must reuse those rows and must never re-enter a planning/default-filling path that evaluates the default again. For v1, an attempt where an impure default affected a primary-key column or the time index remains queue-ineligible unless the implementation can prove that the complete rebuild path uses only the already-materialized values and cannot re-evaluate that default. An impure default affecting only a non-key, non-time-index column does not by itself make the attempt ineligible. The conservative exclusion is a restriction on the rebuild contract, not a claim that retaining the original routed request loses its materialized values.

Missing, malformed, or mismatched plan context discovered before the first round makes the attempt queue-ineligible and leaves it on the existing inline retry path where supported. If an attempt was selected for queue ownership but its sidecar is found malformed or inconsistent at admission, it is rejected as `UnsafeContext` and does not fall back to inline retry. Unsupported shape or engine is `TableShapeIneligible`; capacity exhaustion and queue closure are separate queue-admission rejections.

The snapshot must be captured from the table metadata and plan context already available during insert planning, before request grouping and splitting can discard the association between a table and its Region attempts. It must carry, at minimum, the `TableId`, `TableIdent.version`, target engine, effective append-mode state, instant-TTL state, Flow-source state at plan time, whether an impure default affected a primary-key/time-index column, and the target Region's `partition_expr_version`. The snapshot must remain associated with each resulting per-Region request after peer batching and failure extraction. Admission must use this snapshot rather than adding awaited metadata/cache lookups to the request failure path. Reading current metadata at admission is not a substitute: it would lose the distinction between the table state used for the first write and a later DDL change.

Where the original request performs Flow mirroring, that action remains part of the original request lifecycle and runs exactly once before queue handoff. It is not part of the retained physical write; replay must never reconstruct or repeat the mirror action. Flow-source writes remain queue-ineligible under the rule above and retain their existing inline retry path where supported. If the implementation cannot keep the mirror action separate from the replayable physical write, the affected request is not queue-eligible.

Append mode must be captured as the effective plan-time setting used by the Region write path; it must not be assumed to be a single field on table metadata. Flow-source status is captured from the plan-time Flow metadata/cache. It is not silently replaced with a later lookup during replay. If the implementation cannot provide an unambiguous value for a required queue field, the attempt must not be selected for queue ownership.

Queue replay also needs access to the metadata and cache-invalidation capabilities required to refresh table identity and routing. These capabilities must be available independently of whether legacy inline retry is enabled.

Admission is the ownership boundary for queue-owned attempts. Before publication, the queue does not own the attempt. After publication, the queue retains responsibility until the lineage retry deadline or a terminal outcome. Publication makes the record, retained charge, and any required FanOut barrier position visible together; none may become visible independently. The publication transaction is the linearization point: it runs under queue-state protection, checks cancellation and the admission token, and either publishes the complete state or publishes nothing. If cancellation is observed before that commit point, admission aborts and releases temporary reservations. If cancellation races with the commit point, whichever operation is observed first by the commit arbitration determines the result; after publication, later cancellation ends only the client wait.

The first-round RPC has already been sent by this stage, so aborting admission does not roll it back or prove that it was not applied; it simply means no queue retry will follow. Completion-wait expiry is not request cancellation and does not participate in publication arbitration. It limits the time available for synchronous completion waiting, not whether an otherwise eligible attempt is admitted. First-round classification and admission must be bounded local operations: admission must not wait for queue capacity, network I/O, metadata refresh, or replay-worker scheduling. The publication decision is resolved before request completion returns. If the completion-wait deadline expires during that local work, admission still reaches its publish/reject decision; the caller then returns the selected first-round error without waiting for replay. This keeps request cancellation, admission ownership, and the client wait deadline as three distinct events.

### First-round outcomes and admission

| First-round outcome | Queue/inline action | Retry owner |
|---|---|---|
| Success | No record | None |
| Retryable per-entry failure with a retained Region request and valid queue sidecar in a queue-owned call | Admit independently if capacity is available | Queue if admitted; otherwise none |
| Retryable whole-RPC failure that can be represented as retained per-Region attempts in a queue-owned call | Admit each Region attempt independently; preserve unresolved first-round provenance | Queue per admitted attempt; otherwise none |
| Retryable single-entry-collapsed failure with a recoverable original Region request and plan snapshot in a queue-owned call | Admit the single Region attempt under the same capacity rules | Queue if admitted; otherwise none |
| Retryable failure for an attempt classified as queue-ineligible before the first round | Continue through the existing inline retry path where supported | Legacy inline |
| Terminal failure with no retained request | No record | None |
| Queue-owned attempt rejected for capacity, queue closure, or invalid sidecar discovered at admission | Reject without waiting for queue capacity; no inline fallback | None |

A whole-RPC transport failure does not prove that the first-round write was not applied. Its provenance is retained separately from replay invocation history. `issue_unknown` is reserved for a replay invocation that was authorized but whose result was not observed; a first-round whole-RPC failure is recorded as unresolved first-round provenance, not as a replay invocation. The implementation must expose this provenance before any batch-level merge.

### Selected client error and mixed batches

The selected client error is determined from first-round outcomes before queue admission. A terminal first-round failure takes precedence. If there is no terminal failure, the selected error is the first retryable failure in the observed per-Region attempt order. Requests grouped by peer do not preserve a global input index across peers, so cross-peer selection is not guaranteed to match original request input order.

If multiple terminal failures exist, the first terminal failure in dispatcher attempt order is selected. Queue admission success or failure does not change the selected error. Each eligible retryable attempt in a queue-owned call is admitted independently to its destination Region FIFO. If a queue-owned attempt is rejected, the request returns the selected error even when sibling attempts are admitted. There is no inline fallback for an attempt already assigned to queue ownership because that would bypass per-Region queue scheduling, queue limits, and single-owner retry semantics.

## Client-Visible Contract

For a FanOut parent, the parent's waiter receives the selected first-round error; it does not receive a later successor outcome. Successful successors do not turn the FanOut parent's result into `Ok`, and there is no all-or-nothing completion result across a FanOut lineage. This parent-error rule does not override the defined `Ok` result for a fully applied, non-rebuilt admitted record.

The completion-wait budget applies to queue-owned calls and starts when request completion begins. Time spent collecting first-round outcomes, making the synchronous queue-admission decision, and waiting for queue-owned records consumes that budget. Calls preclassified to the legacy inline policy retain their existing retry timing and cancellation behavior; `write_queue.completion_wait` does not change them. A queue-owned call can return `Ok` only if every retryable attempt was admitted, every admitted record settles `applied` before the completion-wait deadline, and no terminal first-round failure exists. A queue-owned attempt rejected for capacity, queue closure, or invalid admission context prevents `Ok`.

Classification and admission are bounded local work and are not preempted by completion-wait expiry; the request resolves every queue admission commit/reject decision before returning. If the deadline has already expired when that decision completes, the selected first-round error is returned immediately and no queue replay wait is registered. `write_queue.completion_wait = 0` still permits normal admission but disables synchronous waiting for queue replay. The completion-wait deadline is a response-wait budget, not an execution-cancellation deadline for classification or admission.

The request waits for queue-owned settlement only when there is no terminal first-round failure, every queue-eligible retryable attempt was admitted, and positive completion-wait budget remains after classification and admission. Otherwise, it returns the selected error while admitted queue siblings continue independently. Waiter registration must be coordinated with publication/settlement so that a record settling between publication and waiter registration is observed as already settled; that race must not lose a notification. Request cancellation before publication is handled by the admission commit-arbitration rule above; cancellation after publication ends only the client wait.

`Ok` for a queue-owned call means that every queue-owned record settled `applied` before the completion-wait deadline, no queue-owned retryable attempt was rejected, and no terminal first-round failure exists. A call assigned to the legacy inline policy retains the existing inline success/error contract. It does not prove exactly-once behavior: the first-round request or an earlier replay invocation may have been applied even though its response was lost, and a later replay may also apply. Any non-applied queue record settlement, a rebuilt FanOut parent, a rejected queue-owned attempt, a legacy-inline terminal failure, or queue wait-deadline expiry returns the selected error.

`affected_rows` reports observed applied rows from first-round and queue replay successes for queue-owned calls; legacy-inline calls retain their existing accounting. It is not an exact logical row total and may not account for duplicate application. A same-Region retarget retains the original waiter. A FanOut parent notifies its waiter with the selected error; successors do not create separate client waiters.

After queue publication, client timeout or cancellation ends only the queue completion wait. It does not delete, settle, cancel, or reassign an admitted record and does not cancel an in-flight replay RPC. Calls assigned to the legacy inline policy retain the existing caller-cancellation behavior. On orderly queue shutdown, any still-pending queue completion waiter is woken immediately with its already-selected first-round error. Shutdown does not wait for the remaining queue completion-wait duration. A waiter already completed before shutdown is unaffected.

## Queue and Retry Semantics

### Records, lineages, and invocations

A **record** represents one admitted single-Region attempt. It retains the routed request, first-round provenance, absolute deadline, lineage identity, and settlement history. A **lineage** consists
of an admitted parent record and all records derived from it by FanOut. A **successor** is a descendant record in that lineage. A successor inherits the lineage deadline, invocation budget, and backoff progression, but has its own record identity and invocation history. It does not inherit the parent's replay invocation outcomes.

An **invocation** is one replay RPC that has passed the queue's send authorization. The invocation's `in_flight` state must be recorded before the RPC is issued; the queue must not send first and record authorization afterward. Its outcome is one of:

- `in_flight`;
- `applied`;
- `observed_retryable_failure`;
- `observed_terminal_failure`; or
- `issue_unknown`.

Before send authorization, replay completes the required table, route, leader, and client lookups. Under queue state protection it then checks that the queue is open, the record is still owned by the current worker, and the lineage deadline and invocation cap permit another send; it records the invocation as `in_flight` before releasing that protection. The RPC is issued immediately afterward, without an intervening `await` or additional lookup. Send authorization is a local ownership/accounting boundary, not proof that the Datanode received the request.

The recovery contract has three observable checkpoints:

1. **Replay ownership adopted:** the queue records the worker claim for the record. Failure before send authorization creates no invocation; the record is finalized from its existing history and is not automatically restarted.
2. **Send authorization recorded:** the queue records `in_flight` before calling the RPC. From this point onward, worker failure or loss of the response is conservatively classified as `issue_unknown`, even if the failure occurred in the narrow interval before the transport call was actually issued. This avoids treating an authorized attempt as definitely unsent.
3. **Rebuild classification and successor publication:** route/partition classification is local planning state, not a committed successor. Each successor becomes queue-owned only at its destination-lock commit point, where its record, retained charge, ordinal disposition, and barrier position become visible together. If FanOut stops part-way through, committed successors remain published and every uncommitted ordinal becomes `not_attempted`; parent finalization uses only the committed successor set. A planning result that cannot be safely materialized is terminally `rebuild_rejected`, while a temporary reservation shortage is deferred with backoff.

The outcomes after `in_flight` are terminal for that invocation. A late response does not rewrite an invocation already classified as `issue_unknown`. This conservative rule may keep an affected
barrier closed even if the remote operation later completes.

A record may authorize a new, distinct invocation while an earlier invocation is `issue_unknown`, provided its deadline and lineage invocation budget still permit it. The earlier invocation is never
resent as though it had not been sent, and the later invocation does not clear the earlier unknown. This permits duplicate-capable retry and is not an exactly-once mechanism.

### Record settlement

A record receives one settlement label at terminal classification. The label summarizes the record history; it is not an ordering-safety proof.

| Record settlement | Meaning |
|---|---|
| `applied` | At least one invocation was observed applied; an earlier `issue_unknown` may still exist |
| `rebuilt` | At least one successor admission committed; it does not mean the rows were applied |
| `issue_unknown` | An invocation remains unresolved and no invocation was observed applied |
| `observed_failure` | The record has only observed failures and no unresolved invocation |
| `rebuild_rejected` | A deterministic FanOut eligibility or pre-allocation-bound check rejected rebuilding before any successor committed; this is terminal for that rebuild lineage |
| `not_sent` | No queue replay invocation was authorized; this says nothing about whether the first-round request applied |

The summary precedence is `applied`, then `rebuilt`, then `issue_unknown`, then `rebuild_rejected`, then `observed_failure`, then `not_sent`. Invocation history and first-round provenance are retained separately for ordering
decisions. In particular, a record labelled `applied` may still have an earlier unresolved invocation and may therefore remain unsafe for barrier release.

### Replay outcomes

| Replay event | Record behavior | Ordering consequence |
|---|---|---|
| Observed `applied` | Settle the record when no further replay is needed | This invocation is ordering-safe; earlier unknown invocations remain unresolved |
| Observed retryable failure | Schedule a new distinct invocation after backoff if the deadline and invocation cap allow it | This invocation has a definitive observed outcome; the record is not terminal while another retry is scheduled |
| Observed terminal failure | Settle from the record's full history; do not retry this record | This invocation is ordering-safe; earlier unknown invocations remain unresolved |
| Whole-RPC failure with definitive per-entry outcomes | Apply the corresponding per-entry result to each attempt | Evaluated per invocation and destination |
| Whole-RPC/transport failure without a definitive result | Mark this invocation `issue_unknown`; a distinct invocation may be scheduled after backoff | The unknown invocation keeps affected barriers closed |
| Transient route lookup failure | Defer with backoff; do not authorize a replay RPC | No new invocation is created |
| Replay transient-memory reservation temporarily unavailable | Keep the record at the Region FIFO head and defer with bounded backoff; do not begin the corresponding encode/materialization work | No invocation is created and the lineage invocation cap is not consumed |
| No conservative pre-allocation bound for this FanOut shape | Settle the rebuild as `rebuild_rejected`; do not retry the same unsafe planning path | No destination ordinal or successor is published |
| Target Region absent or partition expression changed | Refresh metadata and routing, then retarget or FanOut if the current table identity and schema constraints still permit replay | FanOut successors are admitted under destination barriers |
| Region exists but has no leader | Retain the record at the Region FIFO head and defer until the next bounded backoff timer | No new invocation is created; v1 does not require an immediate wake-up from leader-cache or watch notifications |
| Replay RPC timeout | Classify the invocation `issue_unknown`; a distinct invocation may be scheduled only if the deadline and invocation cap allow it | The unknown invocation keeps affected barriers closed |
| Retry deadline reached before authorization | Stop scheduling and settle from recorded history | Existing unresolved invocation history remains effective |
| Retry deadline reached while an invocation is in flight | Authorize nothing further; allow the in-flight RPC until its own timeout | If no result is observed, the invocation becomes `issue_unknown` |
| Lineage invocation cap exhausted | Stop new authorizations and successor admissions; settle records from recorded history | Unresolved unknowns keep affected barriers closed |
| Lineage record cap exhausted | Stop new successor admissions; already admitted records may continue within the remaining invocation budget | Existing barrier state is unchanged |
| Worker failure before authorization | Finalize the record from its existing history; do not invent an unknown invocation or automatically restart the failed replay | No send-side unknown is created |
| Worker failure after authorization without an observed result | Classify that invocation as `issue_unknown` and finalize the record from its history | Affected barriers remain closed |
| Client cancellation | End the client wait only; queue work continues | No change |

A **definitive per-entry first-round result** means the dispatcher has an explicit disposition for each Region entry, either success or an entry-level failure. A whole-RPC transport error without per-entry results is not definitive and remains unresolved first-round provenance; it does not turn a retryable failure into a successful write.

### Retry budget and lineage limits

The absolute retry deadline is `admission_time + write_queue.retry_budget` and is inherited by every successor. Backoff, route lookup, metadata refresh, rebuild, and leaderless deferral consume the deadline but never extend it. An invocation authorized before the deadline may run until its own `write_queue.replay_rpc_timeout`, which starts at authorization. Replay settlement for an RPC authorized before the deadline may occur up to one queue-side RPC timeout after the retry deadline; the barrier's local release is independently fenced at the derived expiry and does not wait for non-RPC planning tasks. The retry budget is not a strict total-lifetime bound for record cleanup.

The existing jittered exponential backoff schedule is reused for replay scheduling, with `write_retry.max_backoff` (10 seconds in the current configuration) as the maximum individual wait. Backoff progression is inherited by FanOut successors. A Region with no leader is deferred until the next bounded backoff wake-up and does not consume a replay invocation. The absolute deadline and invocation cap remain shared by the lineage.

Two independent cumulative limits apply to each lineage:

- `max_lineage_records` limits the total number of parent and successor records ever admitted for that lineage. It gates successor admission only. Exhausting it does not prevent already-admitted records from using the remaining replay invocation budget.
- The lineage invocation cap limits the total number of replay RPCs authorized by the parent and all successors. It gates new replay authorization and successor admission. It is not reset when a successor is created.

There is no additional per-record retry-count limit in v1. The legacy `write_retry.max_retries` applies only to paths that retain inline retry. The lineage invocation cap and absolute retry deadline are the queue's hard replay bounds. The concrete invocation cap must be selected against the accepted maximum FanOut width and failure-workload envelope, then fixed and validated before enabling the queue.

### Worker failure, cancellation, and shutdown

The queue maintains a single active replay owner for each claimed record. A worker failure before send authorization creates no replay invocation; the record is finalized from its existing history and the failed replay is not automatically restarted. If a worker fails after authorization and no definitive result was observed, that invocation is `issue_unknown`; the record is finalized from its recorded history and is not silently treated as unsent.

If FanOut is interrupted, only successors whose admission committed remain published. Uncommitted destinations have no record and no retained charge. The parent is finalized from the committed successor set. Parent settlement as `rebuilt` does not itself release destination barriers.

A panic or process failure during synchronous admission is not recovered by v1. The publication transaction must be structured so that no panic-prone work or `await` occurs between its final validation and its all-or-nothing state publication; reservations use RAII or equivalent cleanup. If the process aborts, in-memory queue state is lost as described above. A panic must never leave a record visible without its retained charge or required barrier position.

Client cancellation before publication aborts admission as described above. After publication, cancellation ends only the client wait. Queue shutdown stops new replay authorization and closes active FanOut admission. It classifies authorized in-flight invocations without observed results as `issue_unknown` and finalizes active parent admission from committed successors. Shutdown does not drain the queue, recover records, or prove that in-flight remote requests did not apply. Forced process termination may discard all remaining queue state.

## Routing, FanOut, and Destination Ordering

### Metadata validation and route refresh

At planning time, each eligible attempt captures the table identity and the metadata that determines whether its original rows remain safe to replay: `TableId`, `TableIdent.version`, engine, effective
append-mode state, instant-TTL state, plan-time Flow-source status, and `partition_expr_version` for its target Region. The table identity version is distinct from metadata-store and route-bookkeeping versions; those values are not substitutes for the captured table identity version.

Replay reloads current table metadata and rejects the record if the table identity, `TableIdent.version`, engine, append-mode state, or instant-TTL state no longer matches the captured plan.
Flow-source status is captured at plan time and is not re-read; a Flow status change after planning is a known residual risk. Replay uses the captured partition-expression version when deciding whether the original request remains valid after a partition-expression change. This check does not prove that an earlier request was not applied before its response was lost.

A transient metadata or route lookup failure is deferred with backoff and is not treated as proof that a Region was removed. When the response indicates a stale route, missing Region, or
partition-expression mismatch, replay must refresh metadata and routing before rebuilding. Refresh is coalesced across the currently ready records that need the same table metadata:

1. Group refresh candidates by captured `TableId` and fully qualified table name. Invalidate the route and table metadata entries once for each distinct table in the refresh wave.
2. Invalidate each distinct name-based metadata entry once, then resolve each distinct fully qualified name once for that wave. Use batch name resolution when available; otherwise use bounded single-flight lookups so multiple records for the same name do not issue duplicate refreshes.
3. Verify for every candidate that the resolved name still maps to its captured `TableId`; never replay an old request against a newly created table that reused the name. A missing name or changed identity is terminal for the affected candidates.
4. Load the current table metadata and route, then build one refreshed partitioner per distinct table. Records in the same group may share that refreshed metadata, but each record performs its own row-to-Region mapping and admission decisions.

A refresh wave is not an atomic snapshot across different tables. A transient failure for one name defers that table's group without preventing unrelated groups from progressing. Reusing the first-round partitioner or refreshing only the peer address is insufficient: it can repeatedly rebuild against stale routing until the retry deadline expires. The metadata refresh is shared by records for the same table; row-to-Region rebuilding and successor admission remain per record.

The current validation boundary is not an atomic DDL fence:

| Operation or change | Source-level behavior relevant to v1 | Queue behavior and residual risk |
|---|---|---|
| Physical or logical `ALTER` that changes `TableIdent.version` | The ALTER path increments the table identity version | Replay rejects a visible version mismatch, but DDL can commit after metadata validation and before RPC submission; cross-Frontend cache propagation is asynchronous |
| `TRUNCATE` | The current truncate path does not increment `TableIdent.version` | The captured version check cannot identify this change. A queued write may replay after truncate and reinsert rows unless a separate truncate/data-generation marker is introduced |
| `DROP` followed by create under the same name | A dropped table is unavailable; a newly created table receives a new `TableId` | Name resolution plus the captured-`TableId` comparison must reject the old record rather than targeting the new table |
| `UNDROP` | The undrop path restores the dropped table's stored metadata; the complete identity/version values observed by the replay path must be checked in the target code baseline | The RFC does not assume that `UNDROP` creates a new incarnation. A regression test must establish whether an old queued record can pass all replay checks after restoration; until then, treat this as an unresolved residual risk |
| Repartition / partition-expression change | Replay refreshes route and partitioner and compares the captured partition-expression version | Rebuild may duplicate rows if the original request applied before its result became unknown |
| Flow-source creation or removal | Eligibility uses the plan-time Flow-source snapshot | A change after planning is not re-read by this check |

The implementation audit and regression tests must distinguish facts established by the current source from the remaining design decision. In particular, the current code has no `TableIdent.version` bump for `TRUNCATE`; before asking maintainers to accept the risk, the author should inspect whether the existing metadata path can expose a distinct data-generation marker for truncate without introducing an atomic cross-component DDL fence. The implementation cost and compatibility impact of adding such a marker are then a design decision for review. If no such marker is implemented in v1, the documentation and deployment warning must continue to state that `TRUNCATE` is not detected by the captured table version. The `DROP`/recreate `TableId` check and the full `UNDROP` restoration path must have explicit tests; source-level behavior must not be left as an open-ended maintainer question.

A route or partition-expression change can leave the original invocation's remote outcome unknown. Rebuilding against refreshed metadata may therefore duplicate rows already applied by the old invocation; v1 does not add a Datanode-side idempotency or stale-write fence.

### Retarget

If refreshed metadata shows that the same `RegionId` and `partition_expr_version` remain valid, replay may retarget the existing record by updating its destination and routing information. Retarget preserves
record identity, first-round provenance, invocation history, retry deadline, lineage limits, FIFO position, and waiter. The retarget bookkeeping itself consumes no invocation; the next RPC issued to the retargeted destination is a normal replay invocation and consumes the lineage invocation cap. Retarget does not change the captured plan-time table snapshot.

### FanOut rebuild and admission

If refreshed routing shows that the old Region is absent, the partition expression changed, or the rows now map to multiple/different destination Regions, replay rebuilds the rows using the refreshed
table partitioner. FanOut is required because the partition expression determines which Region owns each row; sending the old Region payload to a new peer would not be a valid substitute for
repartitioning the rows.

FanOut produces an ordered sequence of destination ordinals. It may retain lightweight destination/row-selection metadata, but it must not retain all fully materialized destination requests before
capacity has been reserved. Each destination payload is materialized only after transient rebuild capacity is available. The reservation must cover the output payload and the temporary
row-selection/partitioning/encoding work included in the declared memory bound.

Successors are admitted independently. A successor becomes visible only together with the destination barrier position that protects its ordering. The parent record identity and destination ordinal identify each successor admission, so retrying the same admission step cannot publish the same successor twice. FanOut is not atomic as a whole: some successors may commit while others are rejected or remain
unadmitted. A rejected ordinal leaves no successor record, barrier position, retained charge, or committed ordinal. Rejection of one ordinal does not itself prevent later ordinals from being attempted
while the admission pass remains open.

Every planned destination ordinal receives exactly one final admission disposition: `admitted`, `rejected` with a bounded reason, or `not_attempted` with a bounded reason. A reservation/materialization or per-target admission failure is `rejected`; cancellation, shutdown, or an admission-pass stop before the ordinal is examined is `not_attempted`. The ordinal must never disappear from accounting. The corresponding rows are counted in the same disposition, so `planned_rows = admitted_rows + rejected_rows + not_attempted_rows` for the rebuild plan.

When the FanOut admission pass terminates, the queue uses the committed successor set. If at least one successor committed, the parent settles `rebuilt`, even if other ordinals were rejected or never attempted. If no successor committed because the shape has no conservative pre-allocation bound, the parent settles `rebuild_rejected` and that rebuild is not retried. If no successor committed because a temporary reservation was unavailable, the parent is not finalized while deferred: it remains at the FIFO head and retries planning with bounded backoff. If its deadline is reached before planning resumes successfully, it settles from its recorded history. Each completed FanOut emits a structured lineage diagnostic containing the planned target/row counts, committed successor/row counts, and rejected/not-attempted counts grouped by reason. Low-cardinality counters aggregate target outcomes and reasons; table, Region, lineage, and record identifiers remain in structured logs, not metric labels. Parent finalization does not wait for successor application and does not release destination barriers.

### Destination ordering and barriers

Queue scheduling is per `(Frontend queue, RegionId)`, with at most one replay record locally in flight for a destination Region. Records admitted to the same Region queue are replayed in queue order. This defines local scheduling and send-authorization order, not remote application order: an earlier invocation whose result is `issue_unknown` may still apply after a later invocation. The FIFO guarantee does not extend to legacy-inline attempts, live writes, unadmitted first-round requests, or other Frontends.

A destination barrier is installed when the first successor for a FanOut is admitted to that destination. Records ordered ahead of the barrier are processed before its successors. Successors belonging to the same FanOut run in ordinal order. Normal queue records ordered after the barrier remain behind its successor segment until the barrier releases. Multiple barriers retain their installation order, and nested FanOut joins an applicable existing barrier or installs a later position; it does not bypass an earlier barrier.

The barrier's first-round evidence is limited to attempts belonging to the same FanOut lineage whose first-round request was addressed to that destination and whose per-entry disposition is retained in the lineage's queue-owned history. A first-round request addressed only to an obsolete source Region does not hold the new destination after refreshed routing confirms that the source Region is no longer a valid target; this does not prove that the old request was not applied at its source. Unrelated lineages, live writes, legacy-inline attempts, unadmitted attempts, and first-round requests whose outcomes were not retained are outside this barrier condition and remain outside the queue's ordering guarantee.

A barrier normally releases when its FanOut lineage is quiescent, every replay invocation associated with that destination has a definitive observed outcome, and every tracked first-round attempt in that lineage that was addressed to that destination has a definitive per-entry result. An invocation that remains `in_flight` or `issue_unknown`, including a timeout or post-authorization worker failure, is not ordering-safe. A whole-RPC first-round failure without a per-entry result keeps the barrier closed only when that unresolved attempt belongs to the same lineage and was addressed to that destination. A later `applied` result does not erase an earlier unknown invocation. This normal release rule preserves the strongest ordering evidence available from the queue, but an unknown remote outcome can otherwise keep a destination blocked indefinitely.

To bound that blockage, every barrier also has an expiry derived from the lineage's existing retry deadline and replay RPC timeout:

`barrier_expiry = lineage_retry_deadline + write_queue.replay_rpc_timeout`

The lineage deadline closes successor publication. Every successor admission carries a destination barrier identity/generation and is committed under that destination queue-state lock. For the first successor at a destination, the commit atomically creates the active barrier position and publishes the successor; later successors validate and append to that active position. Under the lock, commit must verify that the lineage deadline has not been reached, the queue is open, the token is neither stale nor tombstoned as released, and the successor ordinal has not already committed. Only then may the successor record, retained charge, committed ordinal, and barrier position become visible together. This is the linearization point for successor publication. The critical section must contain only bounded synchronous state checks and publication mutations: it must not hold the destination lock across RPCs, metadata lookups, partition planning, payload materialization, or any `await`.

All barrier release transitions, including normal quiescent release and expiry-based release, use that same destination queue-state lock. When expiry is reached, release marks the barrier position terminal/released before removing its scheduling hold. The released identity/generation remains as a tombstone for as long as a local planning/admission operation could still reference it; a stale operation must never recreate the released barrier. Tombstone/history memory remains charged to the retained-history ledger until it is safe to reclaim. If a successor commit obtains the lock before the lineage deadline and passes all checks, it is published before release can proceed. If the deadline has passed, the barrier is released, or the token is stale, commit fails, the ordinal is accounted as `not_attempted` with a bounded reason, and no successor is published. Thus a planning task cannot publish a successor after the lineage deadline or after barrier release.

At the lineage retry deadline, the queue stops authorizing new replay RPCs and signals cancellation to metadata refresh and FanOut planning. Cancellation is cooperative, so a local task may continue briefly; it cannot pass the publication check after the deadline. Temporary reservations remain charged while the allocations they cover are still live and are released only when those allocations are dropped or transferred to retained accounting. At `barrier_expiry`, the barrier is released under the destination lock without waiting for local planning tasks to finish. Authorized RPCs that began before the lineage deadline have at most their configured queue-side RPC timeout to produce a result; an unresponsive local task cannot extend the barrier hold or publish a successor after the fence. Timer scheduling and lock acquisition overhead remain implementation costs and must be measured; the protocol does not claim a hard real-time release instant.

The release is recorded as `ordering_uncertain`; the underlying `issue_unknown` and first-round provenance remain unchanged. This is a bounded local-liveness fallback, not proof that the remote write was not applied. A Datanode may still apply an earlier request after the Frontend has timed out or lost its response. After an `ordering_uncertain` release, later queue records may proceed, and a late remote application may therefore appear after a later write. v1 accepts this residual ordering risk to avoid an indefinitely blocked destination. No new public timeout setting is introduced: the bound reuses the lineage retry budget and replay RPC timeout already defined above.

An `ordering_uncertain` release records local ordering uncertainty, not write success or failure, and does not change the underlying invocation history. Each expiry increments a low-cardinality counter and emits one structured warning before retained barrier history is reclaimed, with the table/destination and lineage/barrier identities, expiry time and age, and available unresolved-outcome references. A non-zero count should trigger investigation; a sustained or repeated non-zero rate should be treated as a data-ordering risk, not an ordinary retry error rate. Operators should set deployment-specific alert thresholds. v1 provides no per-release query, manual replay, rollback, or repair tooling; the log, subject to normal retention, is the only per-release investigation evidence.

Expiry affects only the barrier being released. It does not mark unrelated barriers uncertain or force-release them. Removing this barrier allows the destination queue to advance; later records may still stop at another barrier, which follows its own release conditions and expiry. The resulting business risk is a possible ordering inversion or stale-write effect, not an automatic rollback or a guarantee of physical data corruption. Whether that risk is acceptable depends on the table's write patterns and merge semantics; v1 should only be enabled for workloads that can tolerate it.

The barrier orders successors within one FanOut and preserves the order in which barriers are installed on a destination queue. It does not define an old-before-new order between independent records from different source Regions that are later rebuilt into the same destination Region. Such lineages enter the destination queue independently, and their relative order follows destination-queue admission rather than original client arrival or source-Region admission. This draft supersedes the earlier author response in the K8 discussion that proposed a destination gate for this case; no cross-source-key FIFO guarantee is included in v1. This is a deliberate narrowing of the earlier proposal, not a maintainer-approved change. It must be made explicit in the next review. It also does not order live writes, legacy-inline attempts, or records in another Frontend.

## Resource Limits and Operations

### Retained and transient memory

`write_queue.max_bytes`, `write_queue.max_records`, and `write_queue.max_record_size` are independent retained-accounting limits that all apply; the first one reached rejects the admission. `max_bytes` is the aggregate byte-accounting ceiling, `max_records` is the concurrent record-count ceiling, and `max_record_size` is the per-record ceiling. With the initial `512 MiB` and `100000` values, the byte ceiling permits an average of about `5 KiB` per retained record at the count ceiling; this is not a per-record size guarantee. These settings bound retained queue accounting, not process RSS or peak allocation. A retained record keeps the original routed per-Region request; admission does not encode/decode the payload or allocate a second full request copy solely for queue retention. A retained record is charged using an encoded-size proxy for its single-Region request. The proxy is an accounting estimate, not a newly allocated encoded payload, and does not represent exact heap usage or all allocator, index, runtime, and transport overhead. The retained and transient ledgers are separate controls, not one shared byte ceiling; their configured maxima plus runtime overhead define the queue's peak-memory planning envelope and must be selected together against the Frontend memory budget. `max_records` counts currently retained request records. `max_record_size` is checked before queue reservation but cannot prevent the first-round request allocation.

Barrier metadata and invocation/first-round history that must outlive a settled request are also queue-owned memory. Their bounded accounting must be included in `write_queue.max_bytes`; they cannot be left outside both record accounting and barrier accounting. A settled record may release its payload and stop counting against `max_records`, but the compact history needed by an unreleased barrier remains charged until no barrier can reference it. This separates retained payload capacity from the lifetime of ordering evidence.

Transient memory uses a separate per-Frontend ledger shared by concurrent replay RPCs, FanOut planning, and destination materialization. The budget is an internal limit, not currently a public TOML parameter. Its value is selected from representative request and FanOut shapes and concurrent measurements against the Frontend memory budget; it is not a new public tuning knob.

Transient reservations cover three allocation phases:

- **Replay:** encoding buffers, transport wrappers, and any measured body copies.
- **FanOut planning:** row-selection/index state, partitioning state, destination descriptors, and other planning intermediates. The planning reservation must be acquired before these allocations begin.
- **Destination materialization:** each destination payload, its metadata, and overlapping encoding or materialization buffers. A separate reservation is acquired for each destination before its payload is materialized.

One per-Frontend transient budget accounts for the sum of reservations that are concurrently live across these phases, including overlapping allocations. A reservation remains charged while the allocations it covers remain live, or until ownership is transferred to retained-record accounting.

The FanOut planning reservation covers the work needed to discover destination Regions and select rows, not just the final destination payloads. If a conservative pre-allocation bound cannot be established for a request shape, that request is not FanOut-eligible in v1 and the rebuild is rejected before planning allocations begin.

After the destination plan exists, the queue materializes destinations incrementally; it must not eagerly build and retain the complete destination request collection outside the ledger. The planning reservation remains charged while its planning structures remain live. Each destination reservation remains charged until its payload is either atomically converted to retained-record accounting at successor publication or discarded. The planning reservation is released only after the planning structures are no longer live. Reservations must include overlap between planning structures, destination payloads, encoding buffers, and commit-time copies.

These estimates are accounting models, not RSS formulas. They must be conservative enough to reserve before the corresponding allocations. If an eager FanOut path materializes the complete destination collection before the relevant reservations can be enforced, the ledger does not impose a hard bound on that actual peak. Queue enablement is blocked while any enabled FanOut path can make such an unaccounted allocation. A prose caveat that the peak is unbounded is not an acceptable substitute for an enforceable pre-allocation reservation.

A planning-reservation outcome is classified before destination ordinals exist:

- If the request shape has no conservative pre-allocation bound, rebuilding is deterministically `rebuild_rejected`. The queue does not repeatedly retry the same unsafe planning path, no destination ordinal is fabricated, and one bounded `planning_rejected_total` diagnostic is emitted.
- If a valid bound exists but the transient ledger is temporarily exhausted, planning is deferred with bounded backoff. The parent remains at the relevant Region FIFO head, no replay RPC is authorized by that deferral, and the lineage invocation cap is not consumed. A deadline reached before planning can resume settles the record from its recorded history; it does not busy-loop.
- If a destination reservation or per-target admission fails after planning, that ordinal is counted as `rejected` with its bounded reason, and other ordinals may continue while the admission pass remains open.

On publication, the destination reservation is converted to retained-record accounting exactly once; on rejection, cancellation, worker failure, or shutdown, it is released exactly once after the covered allocations are no longer live. A reservation failure does not evict retained records or release existing barriers.

### Replay scheduling and fairness

Each Frontend queue has `write_queue.replay_concurrency` replay slots and permits at most one locally in-flight replay per Region. Runnable Regions are scheduled round-robin among those currently
ready. A Region waiting for backoff, a leader, or an earlier barrier is not runnable until that condition changes. There is no per-Region retained-capacity reservation in v1: one Region with a prolonged no-leader backlog can consume the Frontend-wide retained budget and cause admission rejection for otherwise healthy Regions.

This gives currently runnable Regions an opportunity to make progress; it does not provide a bounded waiting time or per-record latency guarantee. A hot Region remains serialized, and its tail delay
grows with its backlog. All queue limits, transient memory, and replay concurrency are per Frontend, not cluster-wide. The queue does not provide fairness for retained-memory admission across Regions; round-robin replay scheduling does not prevent one Region from exhausting the shared retained ledger.

### Drain-time model and workload envelope

The queue has both a per-Region serialization limit and a Frontend-wide concurrency limit. A useful optimistic lower bound for a Region with `K_i` queued records is:

```text
T_region >= K_i * (L_i + D_i)
```

where `L_i` is the average cumulative RPC service time per record for the declared failure mix (including any repeated invocations), and `D_i` is the average cumulative delay per record from backoff or leaderless deferral. In the single-attempt, no-retry case, `L_i` is simply the mean replay RPC service time. Across `N` records, when at least `replay_concurrency` Regions are runnable and cumulative service times are approximately uniform, the aggregate lower bound is:

```text
T_frontend >= (N / replay_concurrency) * L
```

These are optimistic lower bounds, not drain-time guarantees. The aggregate bound uses cumulative RPC service work and excludes periods when replay slots are idle; both bounds still omit route lookup, metadata refresh, rebuild, admission, scheduler overhead, transport variance, and failures beyond those represented in the workload averages. The per-Region bound can dominate even when the Frontend has idle replay slots; increasing global concurrency cannot accelerate a hot Region whose records must be replayed serially.

For illustration only, assuming one replay RPC per record, `100000` records, `replay_concurrency = 8`, and an assumed mean RPC service time of `24ms` imply an aggregate lower bound of `300s` before route, rebuild, scheduling, or other overhead. A single Region with `50000` records at the same assumed service time has a serial lower bound of `1200s`. These are arithmetic examples, not measured GreptimeDB latency or a claim that the proposed `5m` retry budget drains either workload. They show why `max_records = 100000` is a provisional test point rather than a supported-capacity or drain-time guarantee.

The validation workload envelope should state total backlog, per-Region depth distribution (including the hottest Region), replay RPC latency distribution, no-leader fraction, FanOut width, and expected failure mix. Report time-to-first-authorization, the fraction authorized before the retry deadline, total drain time, hottest-Region drain time, deadline expiries, and transient-budget rejections. Use these results to evaluate and tune the starting configuration values before rollout.

### Observability

Operational visibility should let operators answer the following questions:

- How much work is retained, and how close is the queue to its retained and transient capacity limits?
- Are failed attempts being admitted, rejected, replayed, or given up, and what broad reasons explain rejection or give-up?
- Are replay attempts making progress, reaching terminal outcomes, or expiring at the retry deadline?
- Are unresolved outcomes or destination barriers accumulating, how long are destinations being blocked, and how often are barriers force-released as `ordering_uncertain`?
- Are FanOut rebuilds being partially admitted or rejected because transient capacity is unavailable, and are all planned targets accounted for?
- Are replay latency or worker failures preventing the queue from making recovery progress?
- Are temporary FanOut planning reservations being deferred, or are unsupported shapes being deterministically rejected?
- Is one Region's retained backlog causing Frontend-wide admission rejection?

FanOut counters should distinguish `admitted`, `rejected`, and `not_attempted` target outcomes, with bounded reason dimensions. Add low-cardinality `planning_rejected_total` and `planning_deferred_total` counters, separated by bounded reason; a temporary reservation shortage must not be indistinguishable from a deterministic ineligible shape. Retained record/byte gauges and admission-rejection counters should be available at Frontend scope. Region/table identities remain in structured logs rather than metric labels to avoid high-cardinality metrics; rejection and backlog diagnostics should include the relevant Region/table identity in logs where available. Barrier metrics should distinguish normal from expiry-based `ordering_uncertain` release and report barrier age. Each expiry emits one structured warning with table/destination, lineage/barrier, expiry/age, and available unresolved-outcome context; high-cardinality identities belong in logs, not metric labels. Operators should alert on sustained or non-negligible expiry rates using a deployment-specific threshold. A completed rebuild should also emit one structured diagnostic with the per-lineage target/row reconciliation totals defined above; all target outcomes and row counts must be accounted for.

## Configuration and Rollout

The public queue settings use the `write_queue.` prefix. The values below are initial configuration values for validation and are not benchmark claims. Workload measurements should be used to tune them before rollout. `write_queue.max_records = 100000` is a provisional test point, not a capacity or drain-time guarantee. Internal lineage and transient limits are implementation bounds whose concrete values must be selected and validated before enablement.

| Parameter | Scope | Purpose | Initial value / status |
|---|---|---|---|
| `write_queue.enable` | Per Frontend | Enable queue ownership for supported insert paths | `false` by default |
| `write_queue.max_bytes` | Per Frontend | Retained payload, barrier, and retained history accounting | 512 MiB, initial value |
| `write_queue.max_records` | Per Frontend | Maximum currently retained request records | 100000, provisional test point |
| `write_queue.max_record_size` | Per record | Reject a request above the retained-size limit | 512 KiB, initial value |
| `write_queue.retry_budget` | Per lineage | Additional replay window from admission, shared by successors; initial target is one common Datanode restart or single failover recovery | 5m, initial value; not a measured guarantee |
| `write_queue.completion_wait` | Per client request | Maximum synchronous wait; `0` means no wait | 30s, initial value |
| `write_queue.replay_rpc_timeout` | Per invocation | Queue-side timeout applied to the replay RPC future after authorization; independent of the Frontend-to-Datanode channel timeout | 10s, initial value |
| `write_queue.replay_concurrency` | Per Frontend | Global replay slots | 8, initial value |
| `max_lineage_records` | Per lineage | Cumulative parent-plus-successor record limit | Internal; fix before enablement |
| Lineage invocation cap | Per lineage | Cumulative authorized replay RPC limit | Internal; fix and validate before enablement |
| Transient rebuild budget | Per Frontend | Peak temporary FanOut materialization accounting | Internal; measure and fix before enablement |
| `write_retry.max_backoff` | Existing retry configuration | Maximum replay scheduling backoff | Existing setting reused (10s) |

With queue mode enabled, `write_retry.max_retries` does not control attempts transferred to the queue. Those attempts use `write_queue.retry_budget`, the lineage invocation cap, and replay backoff. Attempts classified before the first round as queue-ineligible retain the existing inline retry configuration and behavior. The replay RPC timeout is enforced by a queue-side timeout around the replay RPC future; it is not inherited from the Frontend-to-Datanode channel, whose current timeout is not a substitute for this queue-owned bound.

`write_queue.enable` is a per-Frontend switch with no v1 table/database allowlist. It applies to every queue-eligible direct insert routed through that Frontend. Operators cannot use it to enable the queue for one table while keeping another table on queue-disabled behavior within the same Frontend. For mixed or high-risk workloads, use a dedicated Frontend pool and route only the intended workloads to it, or keep the queue disabled. Workload owners must assess DDL and ordering tolerance for every table routed through an enabled Frontend; one high-risk table is enough to make that shared Frontend unsuitable unless the table is isolated to a separate queue-disabled pool. Configuration is loaded at startup; hot reload is not part of v1. Disabling the queue requires a configuration change and process restart. Shutdown does not promise to drain or recover retained
records.

### Rollout and operational guardrails

- Roll out by Frontend pool, not by table. Start with a dedicated canary Frontend pool and route only workloads whose duplicate, ordering, and DDL risks have been accepted. The feature has no per-table allowlist or per-table gray switch.
- Keep tables that require strict `TRUNCATE`/DDL ordering, or cannot tolerate a late stale replay, on a separate Frontend pool where the queue remains disabled. A queue-enabled shared Frontend cannot isolate one such table at runtime.
- v1 has no per-table drain, barrier inspection, or DDL fence. Disabling the queue requires restart, and shutdown/restart may discard retained in-memory records; disabling or restarting is not a drain protocol and must not be treated as proof that no queued write can still be applied.
- During a canary, monitor Frontend retained bytes/records, admission rejection reasons, planning deferrals/rejections, time-to-first-authorization, deadline expiry, hottest-Region depth, barrier age, and `ordering_uncertain`. Any non-zero `ordering_uncertain` count requires investigation before expanding the rollout.
- For a deployment that cannot accept the `TRUNCATE` blind spot, keep the queue disabled for that workload until a distinct data-generation marker or stronger DDL coordination is implemented and validated.

`write_queue.enable = true` is blocked until all of the following are complete:

- The dispatcher accepts an explicit per-call retry policy; shared dispatcher users cannot inherit a global queue mode. A call is queue-owned only if every Region attempt is eligible; otherwise the whole call remains inline. The queue handoff preserves first-round per-entry results and original per-Region requests while bypassing inline retry only for queue-owned attempts; preclassified ineligible attempts retain their legacy inline owner.
- Plan-time metadata is captured and propagated through request grouping, per-Region splitting, and queue admission. First-round provenance (definitive per-entry, whole-RPC unresolved, or single-entry-collapsed) remains explicit through classification.
- Replay performs the required cache invalidation, table-identity check, metadata reload, and refreshed-partitioner rebuild.
- Per-Region FIFO scheduling, FanOut-local destination barriers, same-lock successor-publication/barrier-release linearization, and bounded `ordering_uncertain` barrier expiry pass the ordering validation below; no cross-source-key merge ordering or DDL fence is claimed.
- FanOut, record/invocation limits, retained-history accounting, transient memory limits, shutdown waiters, and failure semantics pass the validation plan below.
- `max_lineage_records`, the invocation cap, and transient rebuild budget have concrete implementation values selected and validated against the declared workload envelope; these internal values are not public configuration knobs.
- The transient estimator is validated against representative replay, FanOut planning, and destination-materialization shapes. Every enabled rebuild path reserves before row-selection/partitioning allocations and before each destination payload materialization. Metadata refresh and FanOut planning must stop at the lineage deadline so no successor can be published after barrier expiry. An unbounded eager FanOut path blocks queue enablement until enforceable pre-allocation reservations are implemented.
- Maintainers accept the DDL residual risks, duplicate-capable retry semantics, and the bounded barrier-expiry trade-off: unresolved remote outcomes do not block a destination forever, but a late remote application may occur after the barrier is released and may affect a later write to the same `(primary key, time index)` under the table's merge semantics. The expiry counter, structured warning diagnostic, and deployment-specific alert threshold are defined and validated.
- The client contract is reviewed, including whole-call fallback when any Region attempt is ineligible, and failure-path/drain measurements are reported against the declared workload envelope.
- The per-Frontend blast radius is accepted; deployments requiring table/business isolation use separate Frontend pools or keep the queue disabled.
- DROP/recreate and UNDROP replay identity tests are complete, and the TRUNCATE data-generation-marker option has been evaluated. If no marker is implemented, the documented TRUNCATE blind spot is explicitly accepted.

The queue depends on three write-path improvements before enablement:

- **Bounded completion wait:** the caller uses one absolute response-wait budget that is consumed by first-round result processing, synchronous admission, and any replay wait. Expiry does not cancel the admission commit decision; it prevents further synchronous replay waiting.
- **Broader retryable-failure admission:** classification and retention cover eligible per-entry failures, retryable whole-RPC failures, and retryable single-entry-collapsed failures when the original per-Region request and plan context can be retained. A whole-RPC error without a recoverable request is not made queue-eligible merely because it is retryable.
- **Absolute time budget:** each admitted lineage has one retry deadline inherited by all FanOut successors; route lookup, rebuild, backoff, and leaderless deferral consume that same budget.

The dispatcher handoff contract is also a prerequisite:

- Retry ownership is passed explicitly per call/per Region attempt; it is not stored as a shared `WriteDispatcher` mode because insert, bulk-insert, Delete, and batcher producers share dispatcher infrastructure.
- Queue-mode partial-result reporting is enabled independently of `retry.is_some()`; queue mode must not lose per-entry outcomes merely because it bypasses the legacy inline loop.
- Per-attempt classification and provenance are exposed before `merge_attempts` or any terminal-error aggregation can discard retryable siblings. Whole-RPC transport errors, definitive per-entry failures, and single-entry-collapsed failures remain distinguishable.
- The queue retains the original materialized rows. FanOut rebuild reuses them and must not re-run impure-default filling or the original planning path.

These enhancements form one retry contract but may be implemented and merged independently. Queue enablement requires all of them to be available and validated. Durability and restart recovery remain a separate proposal and are not prerequisites for this in-memory queue.

## Risks and Limitations

- **Duplicate-capable retry.** A lost response, timeout, worker failure after submission, FanOut, or client retry can cause duplicate writes. Neither a retryable response nor a later successful invocation proves that an earlier unknown invocation did not apply.
- **Partial batch outcome.** A batch may return an error because one sibling has a terminal failure while a retryable sibling is admitted and applies later. The client can therefore observe a failed batch whose retryable subset is partially applied. Admitted records may also apply after the client wait expires; a batch error does not mean that no rows were written.
- **No inline fallback for queue-owned rejection.** A queue-owned attempt rejected by capacity, queue closure, or an invalid admission sidecar receives no Frontend retry after the first round. This does not apply to attempts classified as queue-ineligible before the first round: where the existing producer supports inline retry, the entire containing call retains that legacy owner. Calls assigned to the legacy inline policy are not ordered by the queue.
- **Process-local loss.** Restart, crash, shutdown, or deadline expiry can discard admitted work. The queue is not durable and provides no restart recovery.
- **Barrier expiry and stale-write risk.** An unresolved invocation may hold a FanOut destination barrier until the lineage retry deadline plus one replay RPC timeout. The barrier is then released as `ordering_uncertain` so the destination can progress, although a remote request may still apply late and overtake a later write. If both writes target the same `(primary key, time index)`, the late-arriving row may affect the merged result according to `merge_mode`; under `last_row`, the latest inserted row takes precedence. Different timestamps remain distinct samples, so an older timestamp does not automatically overwrite a newer one. v1 does not detect, roll back, or repair stale-write effects. This is a workload-dependent ordering/consistency risk, not a claim that every expiry corrupts data. The release is fenced against late local successor publication, but that local fence cannot fence a remote Datanode operation already issued.
- **Incomplete ordering scope.** FIFO scheduling is per destination queue and FanOut successors preserve only their own ordinal order. v1 does not define ordering between different old source-Region records that merge into one new destination; it also does not order original client request start time, unadmitted first-round requests, legacy-inline attempts, live writes, or other Frontends. The earlier author-proposed cross-source destination gate has been withdrawn from this v1 draft and requires explicit maintainer acceptance of the narrower contract.
- **Per-Frontend blast radius.** `write_queue.enable` is not scoped per table or database. Every queue-eligible direct insert through an enabled Frontend is subject to the queue contract; operators cannot selectively exclude one high-risk table on that Frontend. A dedicated Frontend pool is required for workload isolation. There is no per-table queue drain/fence or safe table-specific disable operation in v1.
- **DDL stale-replay window.** Metadata revalidation is not an atomic fence. A client may receive a write error, run `TRUNCATE`, and then have a queued replay reinsert rows that the client expected to be removed. The current `TRUNCATE` path does not increment the captured `TableIdent.version`, so this version check cannot identify the truncate. `ALTER` can race between validation and RPC submission; cross-Frontend metadata propagation is asynchronous. `DROP` followed by create under the same name receives a new `TableId` and must be rejected by the captured-identity check. `UNDROP` restores stored table metadata; whether an old queued record can pass the complete replay checks after restoration must be established by a regression test. v1 has no atomic DDL fence. If no separate truncate/data-generation marker is implemented, the truncate blind spot must be accepted explicitly.
- **Cross-Region retained-capacity starvation.** Retained limits are Frontend-wide and v1 has no per-Region retained cap. One Region with a prolonged no-leader backlog can consume the shared byte/record budget and cause admission rejection for healthy Regions. Round-robin replay scheduling does not prevent this admission-time capacity starvation. The operational metrics expose Frontend-wide saturation; Region/table detail belongs in structured diagnostics rather than metric labels.
- **Resource accounting is not an RSS bound.** Retained and transient ledgers constrain their defined accounting units, not every Frontend allocation or total process memory. FanOut planning and each destination materialization must reserve before allocation; request shapes without a conservative pre-planning estimate are rejected for rebuild.
- **Hot-Region drain.** Per-Region serialization means global replay concurrency cannot shorten the tail of a single hot Region. Records near the end of a backlog may expire before their first authorization. The 5m initial retry budget is a target for one common restart/failover window, not a guarantee that a full queue or hot Region will drain within 5m.

## Validation Plan

Validation should establish the behavior of the design rather than prescribe specific internal data structures or test harness APIs.

- **Dispatcher ownership and request reuse:** with the queue disabled, existing behavior is unchanged. With queue mode enabled, a direct ordinary physical-insert call uses queue handoff only when every Region attempt in that call satisfies plan-time eligibility; if any attempt is ineligible, the entire call retains its legacy inline retry path where supported. Delete, metric logical-batcher flush, pending-rows/table-batcher flush, BulkInsert, Flight `DoPut`, and Metric logical-row inserts that still target a logical Metric-engine Region remain outside the queue. Verify explicit per-call policy at shared dispatcher call sites so batchers, bulk insert, and Delete do not inherit queue ownership. Verify a call with any preflight-ineligible Region remains wholly on the legacy inline path, while a fully eligible call is wholly queue-owned. Verify `accept_partial_result` is independent of `retry.is_some()`, terminal siblings do not discard retryable attempts before classification, and whole-RPC / per-entry / single-entry-collapsed provenance remains explicit. Cover mixed-eligibility input batches and verify the whole-call fallback prevents mixed retry owners and false `Ok` results. Verify the queue retains the original materialized per-Region request without an extra full-payload copy. Queue-owned attempts rejected after selection never fall back to inline retry.
- **Plan snapshot and eligibility:** verify the plan context is captured from table metadata already held by the insert-planning path and carried through grouping, splitting, and admission without an additional awaited metadata lookup. Mutate table version, engine, append mode, TTL, Flow status, and partition-expression metadata after planning; verify that impure defaults on primary-key/time-index columns are rejected while non-key defaults do not cause unnecessary rejection; replay must compare against the captured plan snapshot. Verify admission rejects missing or ambiguous required metadata, unsupported engines, and an absent captured partition-expression version. Verify that current Metric logical-row inserts targeting a logical Metric-engine Region are queue-ineligible; add a separate test only if a future implementation introduces the required Mito physical-Region normalization. Where Flow mirroring applies, verify it runs once on the original path and is never repeated by queue replay.
- **Admission and client contract:** verify request cancellation observed before publication leaves no record or barrier and releases all temporary reservations; cancellation after publication only ends the client wait. Verify the completion-wait deadline expiring during bounded local classification/admission does not cancel publication, that `completion_wait = 0` still performs admission but does not wait for replay, and that positive remaining budget is required for synchronous replay waiting. Verify terminal-error precedence, dispatcher-attempt-order selection, partial admission, `affected_rows`, waiter registration racing with settlement, and shutdown waiter behavior.
- **Replay history and authorization:** verify that a later `applied` invocation does not erase an earlier `issue_unknown`, late responses do not rewrite terminal invocation outcomes, and record settlement labels do not independently release barriers. Verify table, route, leader, and client lookups complete before send authorization; no additional lookup or `await` occurs between authorization and issuing the RPC. Confirm that an authorization recorded before an unobserved worker failure is classified as `issue_unknown`, not `not_sent`. Verify the existing jittered backoff schedule is capped by `write_retry.max_backoff`, is inherited by FanOut successors, and that the absolute deadline and invocation cap remain lineage-wide.
- **Route refresh and FanOut:** start with stale caches, move or repartition a table, and verify cache invalidation, table identity, refreshed partitioning, and coalesced refreshes. Verify an unknown result from an obsolete source Region does not block a different confirmed destination, while a lost response from an old-Region RPC is not treated as proof that the write was unapplied. Verify `DROP`/recreate rejects the old `TableId`; exercise `UNDROP` and verify whether the old record can pass the entire replay validation path; confirm that `TRUNCATE` does not change the captured `TableIdent.version` and evaluate whether any separate data-generation marker is available. Verify partial successor admission, exact target/row accounting, deterministic planning rejection, temporary planning deferral without busy-looping, and no orphaned barrier or record state.
- **Destination ordering:** verify per-Region FIFO scheduling, FanOut ordinal order, and nested/intervening barrier order. Exercise the successor-publication-versus-barrier-release race under the same destination queue-state lock: a commit before the lineage deadline is visible before release; a commit at/after the deadline or after release is `not_attempted`, leaves no successor/barrier charge, and cannot recreate a released barrier. Verify that expiry does not wait for an unresponsive local planning task, that the task cannot publish after the deadline, and that its transient reservation remains charged until its allocations are dropped. An unresolved destination outcome must hold its barrier through the lineage retry deadline; release as `ordering_uncertain` at expiry (subject to timer/lock scheduling overhead), with exactly one counter increment and warning log before retained barrier history is reclaimed. Verify expiry releases only that barrier, preserves `issue_unknown`, and leaves later progress subject to other barriers. Exercise late writes under the configured merge semantics for the same `(primary key, time index)` and confirm different timestamps remain distinct. Verify the explicitly limited first-round scope and that no cross-source-key FIFO guarantee is claimed when old Regions converge on one destination.
- **Resource and failure behavior:** force retained, transient, record, and invocation limits to exhaust. Verify history remains accounted while referenced by barriers, settled payload can be released safely, and reservations are not leaked or double charged. Instrument allocation points to prove the FanOut planning reservation precedes row-selection/partitioning allocation and each destination reservation precedes payload materialization. Verify temporary planning-budget exhaustion defers with backoff and no invocation charge, while a shape with no conservative pre-allocation bound is terminally `rebuild_rejected`. Fill the retained budget with one no-leader Region and verify the client-visible rejection behavior for another healthy Region; do not infer cross-Region fairness from round-robin replay. Inject worker failure before and after authorization, admission panic/failure around publication, partial FanOut interruption, shutdown during replay, and no-leader recovery through bounded timer backoff.
- **Performance and workload envelope:** compare queue-disabled and queue-enabled behavior under the same request shapes, concurrency, and failure injection. Measure successful-path overhead from plan-context capture separately from failure-path overhead added during request completion by classification, admission, reservation, and waiter setup. Proposed initial acceptance target: the incremental p99 latency overhead attributable to first-round queue classification, admission, reservation, and waiter setup should be no more than 5% against the queue-disabled first-round baseline under the same injected failure result, excluding remote recovery time, replay backlog wait, and the legacy inline retry tail. This is an author-proposed target for maintainer review, not an already accepted requirement. Report end-to-end failure-path p95/p99 separately, and report successful-path plan-snapshot overhead separately. Measure queue-state/ledger lock wait and hold time at 1, 8, and 64 concurrent actors, together with throughput. For the failure path, report p95/p99 request latency, admission latency, retained/transient memory, FanOut width, per-Region depth/skew, RPC latency distribution, time-to-first-authorization, fraction authorized before deadline, drain time, deadline expiry, and barrier blocking. Use the lower-bound model above to interpret results and agree on the acceptance envelope before enablement.

## Questions for Review

### Core design decisions

- Is the per-call retry-ownership contract acceptable: a direct-insert call enters the queue only when every Region attempt targets an eligible Mito physical Region; if any attempt is preflight-ineligible, the entire call retains its existing inline retry path; a queue-owned call rejected for capacity/closure or an invalid sidecar is fail-closed with no inline fallback; and Metric logical-row inserts remain outside v1 until a real physical-Region normalization path exists? This also requires an explicit per-call dispatcher policy, partial-result reporting independent of the legacy retry flag, and per-attempt provenance before batch-level error merging.
- Is the DDL/stale-replay risk acceptable without an atomic DDL fence, including the confirmed `TRUNCATE` blind spot when `TableIdent.version` is unchanged, the `ALTER` check-to-RPC race and asynchronous metadata propagation, `DROP`/recreate identity validation, and the `UNDROP` restoration behavior that must be established by regression test? Should v1 add a separate truncate/data-generation marker, or is explicit acceptance of the documented blind spot sufficient?
- Is this v1 ordering contract acceptable: local per-Region FIFO scheduling and FanOut-local successor order, but no old-before-new guarantee when independent source-Region lineages converge on one destination? This draft explicitly supersedes the earlier author response in the K8 discussion that proposed a destination gate; should v1 retain this narrower contract or restore a minimal destination gate? Are the same-lock successor-publication/barrier-release fence and expiry to `ordering_uncertain` acceptable, including possible late remote application after local release?
- Are the separate retained/transient memory budgets and pre-allocation FanOut reservations sufficient without an aggregate process-memory ceiling, and what workload envelope should gate enablement? In particular, is the proposed 5m target for one common Datanode restart/single failover, the provisional 100000-record test point, the hottest-Region drain model, and the author-proposed first-round failure-path classification/admission p99 overhead target of at most 5% acceptable?

## Alternatives and Future Work

A post-inline-retry buffer was rejected because it would introduce a second scheduler and retry budget for the same write. Inline fallback for a call already assigned to queue ownership was rejected because it bypasses per-Region ordering and splits retry ownership. To preserve existing behavior, a call containing any preflight-ineligible Region attempt is assigned wholly to the legacy inline policy before the first write round; this is policy selection, not post-admission fallback. v1 does not add a global or cross-source-key ordering mechanism. This draft withdraws the earlier author-proposed destination gate for records from different old Regions that converge on one destination; maintainers must explicitly accept that narrower contract or request a minimal destination gate before implementation is enabled. A permanent barrier hold was rejected because one unresolved remote outcome could stop a destination indefinitely; the bounded expiry rule above releases the local barrier with an explicit `ordering_uncertain` outcome. A successor completion group is not included because the queue cannot provide an all-or-nothing result across a FanOut lineage.

Durable retention, restart recovery, region-side idempotency, deduplication, and a stronger DDL fence require additional protocol and storage work and are deferred to separate designs.
