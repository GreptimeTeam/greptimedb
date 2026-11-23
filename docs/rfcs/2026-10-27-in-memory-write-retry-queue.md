---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
Status: Draft for discussion
---

# In-Memory Frontend Write Retry Queue

## Summary

This RFC proposes an optional, per-Frontend in-memory retry queue for eligible transient write failures that may outlast the existing inline retry window. The initial scope is eligible Mito-backed physical-table inserts, including metric logical-row inserts only after normalization to the same supported physical write path, and Prometheus remote write only when it reaches that path and satisfies the same admission rules.

The first write round remains synchronous. Eligible failed per-Region attempts are handed to the queue after first-round classification. An admitted attempt has one retry owner: the queue. The queue performs bounded, best-effort replay with per-Region FIFO scheduling, FanOut-local destination barriers, an absolute retry deadline, retained-capacity limits, and cumulative limits on FanOut records and replay invocations. A barrier normally releases after its FanOut lineage is quiescent and its destination outcomes are definitive. If an outcome remains unknown, the barrier is released at a bounded expiry derived from the lineage retry deadline and replay RPC timeout; the release is recorded as `ordering_uncertain` and does not prove that the remote write was not applied. An attempt rejected at admission does not fall back to inline retry.

The queue is process-local and best-effort. It does not provide durability, restart recovery, at-least-once or exactly-once delivery, proof that an unknown write was not applied, or guaranteed eventual application. FIFO is guaranteed only within one Frontend queue's `RegionId` key, and FanOut successors retain their order within the same FanOut. v1 does not define a relative FIFO order between records originating from different source Regions that are later rebuilt into the same destination Region. The queue does not order against live writes, unadmitted first-round requests, or other Frontends. It is disabled by default. It can be enabled through configuration only after the prerequisite write-path improvements, implementation validation, resource limits, and maintainer acceptance described below are complete.

## Motivation

The existing inline retry path has a bounded retry window. The baseline assumed by this proposal is `write_retry.max_retries = 3` additional rounds, with jittered exponential backoff starting around
1.5 seconds and capped by `max_backoff = 10 seconds`. The relevant Frontend-to-Datanode channel does not currently impose a per-attempt Frontend timeout. These values and behaviors describe the existing path, not a new queue guarantee.

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
- Preserve the synchronous first write round while transferring retry ownership explicitly after classification.
- Bound retained queue work, replay duration, FanOut expansion, replay concurrency, and temporary rebuild memory.
- Define client-visible behavior for mixed batches, partial admission, uncertain outcomes, and asynchronous replay.
- Rebuild requests against current table routing when a Region or partition expression changes.
- State the exact scope of destination ordering and the DDL and availability limitations.

### Non-goals

- Durable queue storage, process-restart recovery, or guaranteed eventual delivery.
- Exactly-once or at-least-once write semantics, or proof that an unknown RPC was not applied.
- A Datanode protocol change for idempotency or deduplication.
- Replacing retry behavior for Delete, BulkInsert, Flight `DoPut`, metric-engine logical-batcher flushes, or writes flushed by pending-rows batchers. These paths remain outside the queue and retain inline retry.
- Ordering across Frontends, against concurrent live writes or unadmitted first-round requests, or between different source-Region queues that may later rebuild into the same destination Region. The queue provides per-Region FIFO and FanOut-local successor order only.
- An atomic DDL fence or a transaction boundary across a FanOut lineage.
- A dead-letter query API.
- Recovery from failure of the queue's own coordination component.

## Request Scope and Retry Ownership

### Supported paths

The proposed handoff occurs after the first-round outcomes for a supported insert path request have been collected. Eligibility is based on the normalized per-Region insert attempts and their target
table metadata, not solely on the external protocol or caller name.

| Request path | Queue behavior when enabled |
|---|---|
| Ordinary physical-table inserts, including SQL, gRPC, pipeline, COPY, logs, traces, and Prometheus remote write through the supported insert path | Queue admission is evaluated per failed Region attempt |
| Metric logical-row inserts normalized to supported physical-table requests | Eligible only when each target is a supported physical Region write and carries the required plan-time context |
| Metric-engine logical-batcher flush | Existing inline retry; remains outside this queue |
| Bulk insert, Flight `DoPut`, and pending-rows/table-batcher flush | Existing inline retry; remains outside this queue |
| Delete | Existing inline retry; remains outside this queue |

For v1, the supported target is a Mito-backed physical Region write. A request that still targets a logical Metric-engine table, uses another engine, or cannot be unambiguously mapped to a supported
physical target is ineligible. Metric logical-row inserts are distinct from metric-engine logical-batcher flushes: the former may be eligible after normalization to a supported physical target, while the latter remains on its separate inline path.

The retry policy is selected at the request-producer boundary, not inferred from the serialized request shape alone:

- **Queue handoff:** direct ordinary physical-table inserts, including metric logical-row requests only after normalization to an eligible physical target.
- **Inline retry:** Delete, metric-engine logical-batcher flush, pending-rows/table-batcher flush, BulkInsert, and Flight `DoPut`.

The selected policy must remain explicit when producers share internal insert-building logic. A batcher must not inherit queue ownership merely because it eventually produces an ordinary insert request or reuses a shared insert helper. If a producer cannot preserve this distinction through its call path, it remains outside v1.

### Queue-mode first-round behavior

Queue mode is a distinct retry policy, not the existing no-retry dispatcher mode. It must preserve the first-round batching and result semantics needed for queue admission:

- The first-round request still supports per-entry/partial Region results.
- Failed entries are split into their corresponding per-Region attempts.
- The failed attempt retains the original routed per-Region request for later replay. Queue retention does not encode/decode the request or create a second full payload copy; any encoded-size calculation is an accounting estimate only.
- The first-round request is sent once; queue-owned attempts do not enter the legacy inline retry loop.


Once an attempt is admitted, the queue owns all subsequent retry, route refresh, retarget, and rebuild decisions. The original inline retry and route-refresh loop must not also retry it. An eligible
attempt rejected for capacity, a closed queue, or unsafe context is returned as a first-round failure and receives no inline fallback. Paths outside the queue's supported request scope continue to use
the existing inline path.

### Eligibility and plan-time context

Admission is evaluated independently for each failed single-Region attempt. A request is eligible only when it:

- is one ordinary physical-table insert request;
- targets a supported Mito-backed physical Region;
- carries a valid plan-time snapshot consistent with the target table and Region;
- has a non-empty `partition_expr_version` for the target used in the first-round request;
- is not append mode or instant-TTL mode;
- was not a Flow source at plan time; and
- does not depend on re-evaluating an impure default for a primary-key column or the time index during replay rebuild.

An impure default that affects only a non-key, non-time-index column does not by itself make the attempt ineligible. The queue must preserve values materialized by the original plan rather than
re-evaluate them during rebuild. If identity-column values cannot be shown to remain unchanged, the attempt is rejected as `TableShapeIneligible`.

Missing, malformed, or mismatched plan context is `UnsafeContext`. Unsupported table shape or engine is `TableShapeIneligible`. Capacity exhaustion and queue closure are separate admission rejections.
These conditions do not create a queue record and do not wait for capacity.

The snapshot must be captured from the table metadata and plan context already available during insert planning, before request grouping and splitting can discard the association between a table and its Region attempts. It must carry, at minimum, the `TableId`, `TableIdent.version`, target engine, effective append-mode state, instant-TTL state, Flow-source state at plan time, whether an impure default affected a primary-key/time-index column, and the target Region's `partition_expr_version`. The snapshot must remain associated with each resulting per-Region request after peer batching and failure extraction. Admission must use this snapshot rather than adding awaited metadata/cache lookups to the request failure path. Reading current metadata at admission is not a substitute: it would lose the distinction between the table state used for the first write and a later DDL change.

Where the original request performs Flow mirroring, that action remains part of the original request lifecycle and runs exactly once before queue handoff. It is not part of the retained physical write; replay must never reconstruct or repeat the mirror action. Flow-source writes remain ineligible under the rule above. If the implementation cannot keep the mirror action separate from the replayable physical write, the affected request is `UnsafeContext` and must not be admitted.

Append mode must be captured as the effective plan-time setting used by the Region write path; it must not be assumed to be a single field on table metadata. Flow-source status is captured from the
plan-time Flow metadata/cache. It is not silently replaced with a later lookup during replay. If the implementation cannot provide an unambiguous value for any required field, the attempt is rejected
as `UnsafeContext`.

Queue replay also needs access to the metadata and cache-invalidation capabilities required to refresh table identity and routing. These capabilities must be available independently of whether legacy inline retry is enabled.

Admission is the ownership boundary. Before publication, the queue does not own the attempt. After publication, the queue retains responsibility until the lineage retry deadline or a terminal outcome. Publication makes the record, retained charge, and any required FanOut barrier position visible together; none may become visible independently.

Immediately before publication, admission checks request cancellation. If cancellation has been observed, admission is aborted and all temporary reservations are released without publishing a record or barrier. The cancellation check and publication must be coordinated so a cancellation observed before the ownership-transfer point cannot leave a published record. After publication, queue ownership has transferred and later cancellation ends only the client wait. The first-round RPC has already been sent by this stage, so aborting admission does not roll it back or prove that it was not applied; it simply means no queue retry will follow.

Completion-wait expiry is not request cancellation and does not participate in the publication arbitration. It limits the time available for synchronous completion waiting, not whether an otherwise eligible attempt is admitted. First-round classification and admission must be synchronous, bounded local operations: admission must not wait for queue capacity, network I/O, metadata refresh, or replay-worker scheduling. The publication decision is resolved before request completion returns. If the completion-wait deadline expires during that local work, admission still reaches its publish/reject decision; the caller then returns the selected first-round error without waiting for replay. This keeps request cancellation, admission ownership, and the client wait deadline as three distinct events.

### First-round outcomes and admission

| First-round outcome | Queue action | Retry owner |
|---|---|---|
| Success | No record | None |
| Retryable per-entry failure with a retained Region request | Admit independently if eligible and capacity is available | Queue if admitted; otherwise none |
| Retryable whole-RPC failure that can be represented as retained per-Region attempts | Admit each eligible attempt independently; preserve that the first-round result may be unknown | Queue per admitted attempt; otherwise none |
| Retryable single-entry-collapsed failure with a recoverable original Region request and plan snapshot | Admit the single Region attempt under the same eligibility and capacity rules | Queue if admitted; otherwise none |
| Terminal failure with no retained request | No record | None |
| Unsupported shape/engine or unsafe plan context | Reject without waiting for queue capacity | None |
| Capacity exhausted or queue closed | Reject without waiting for queue capacity | None |

A whole-RPC transport failure does not prove that the first-round write was not applied. Its provenance is retained separately from replay invocation history. `issue_unknown` is reserved for a replay
invocation that was authorized but whose result was not observed; a first-round whole-RPC failure is recorded as unresolved first-round provenance, not as a replay invocation.

### Selected client error and mixed batches

The selected client error is determined from first-round outcomes before queue admission. A terminal first-round failure takes precedence. If there is no terminal failure, the selected error is the
first retryable failure in the observed per-Region attempt order. Requests grouped by peer do not preserve a global input index across peers, so cross-peer selection is not guaranteed to match original request input order.

If multiple terminal failures exist, the first terminal failure in dispatcher attempt order is selected. Admission success or failure does not change the selected error. Each eligible retryable
attempt is admitted independently to its destination Region FIFO. If a batch contains a rejected attempt, the request returns the selected error even when sibling attempts are admitted. There is no
inline fallback for rejected attempts because that would bypass per-Region scheduling, queue limits, and single-owner retry semantics.

## Client-Visible Contract

For a FanOut parent, the parent's waiter receives the selected first-round error; it does not receive a later successor outcome. Successful successors do not turn the FanOut parent's result into `Ok`, and there is no all-or-nothing completion result across a FanOut lineage. A non-FanOut record follows the ordinary completion-wait rules below; this parent-error rule must not be read as overriding the defined `Ok` result for a fully applied, non-rebuilt admitted record.

The completion-wait budget starts when request completion begins. Time spent collecting first-round outcomes and making the synchronous admission decision consumes that budget, as does any subsequent wait for replay settlement. Classification and admission are bounded local work and are not preempted by completion-wait expiry; request completion resolves the admission commit/reject decision before returning. If the deadline has already expired when that decision completes, the selected first-round error is returned immediately and no replay wait is registered. `write_queue.completion_wait = 0` still permits normal admission but disables the synchronous wait for replay. The completion-wait deadline is therefore a response-wait budget, not an execution-cancellation deadline for classification or admission.

The request waits only when there is no terminal first-round failure, every retryable attempt was admitted, and positive completion-wait budget remains after classification and admission. Otherwise, it returns the selected error immediately while admitted siblings continue independently. Waiter registration must be coordinated with publication/settlement so that a record settling between publication and waiter registration is observed as already settled; that race must not lose a notification. Request cancellation before publication is handled by the admission commit-arbitration rule above; cancellation after publication ends only the client wait.

`Ok` means that every admitted record settled `applied` before the completion-wait deadline and no retryable attempt was rejected. It does not prove exactly-once behavior: the first-round request or an earlier replay invocation may have been applied even though its response was lost, and a later replay may also apply. Any non-applied record settlement, a rebuilt parent, a rejected retryable attempt, or wait-deadline expiry returns the selected error.

`affected_rows` reports observed applied rows from first-round and replay successes. It is not an exact logical row total and may not account for duplicate application. A same-Region retarget retains the original waiter. A FanOut parent notifies its waiter with the selected error; successors do not create separate client waiters.

After publication, client timeout or cancellation ends only the wait. It does not delete, settle, cancel, or reassign an admitted record and does not cancel an in-flight replay RPC. On orderly queue shutdown, any still-pending completion waiter is woken immediately with its already-selected first-round error. Shutdown does not wait for the remaining completion-wait duration. A waiter already completed before shutdown is unaffected.

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
| `not_sent` | No queue replay invocation was authorized; this says nothing about whether the first-round request applied |

The summary precedence is `applied`, then `rebuilt`, then `issue_unknown`, then `observed_failure`, then `not_sent`. Invocation history and first-round provenance are retained separately for ordering
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

The absolute retry deadline is `admission_time + write_queue.retry_budget` and is inherited by every successor. Backoff, route lookup, metadata refresh, rebuild, and leaderless deferral consume the deadline but never extend it. An invocation authorized before the deadline may run until its own `write_queue.replay_rpc_timeout`, which starts at authorization. Settlement may therefore occur up to one RPC timeout after the retry deadline; the retry budget is not a strict total-lifetime bound.

The existing jittered exponential backoff schedule is reused for replay scheduling, with `write_retry.max_backoff` (10 seconds in the current configuration) as the maximum individual wait. Backoff progression is inherited by FanOut successors. A Region with no leader is deferred until the next bounded backoff wake-up and does not consume a replay invocation. The absolute deadline and invocation cap remain shared by the lineage.

Two independent cumulative limits apply to each lineage:

- `max_lineage_records` limits the total number of parent and successor records ever admitted for that lineage. It gates successor admission only. Exhausting it does not prevent already-admitted records from using the remaining replay invocation budget.
- The lineage invocation cap limits the total number of replay RPCs authorized by the parent and all successors. It gates new replay authorization and successor admission. It is not reset when a successor is created.

There is no additional per-record retry-count limit in v1. The legacy `write_retry.max_retries` applies only to paths that retain inline retry. The lineage invocation cap and absolute retry deadline are the queue's hard replay bounds. The concrete invocation cap must be selected against the accepted maximum FanOut width and failure-workload envelope, then fixed and validated before enabling the queue.

### Worker failure, cancellation, and shutdown

The queue maintains a single active replay owner for each claimed record. A worker failure before send authorization creates no replay invocation; the record is finalized from its existing history and the failed replay is not automatically restarted. If a worker fails after authorization and no definitive result was observed, that invocation is `issue_unknown`; the record is finalized from its recorded history and is not silently treated as unsent.

If FanOut is interrupted, only successors whose admission committed remain published. Uncommitted destinations have no record and no retained charge. The parent is finalized from the committed successor set. Parent settlement as `rebuilt` does not itself release destination barriers.

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

| Operation or change | Design behavior | Residual risk |
|---|---|---|
| Physical or logical `ALTER` that changes `TableIdent.version` | Reject replay when the captured and current versions differ | DDL can commit after metadata validation but before RPC submission; cross-Frontend cache propagation is asynchronous |
| `TRUNCATE` | The current version check may not detect it because `TableIdent.version` is not incremented by the existing path | A queued write may replay after truncate because the current version check does not provide a truncate-generation fence |
| `DROP` | A missing table is terminal | A later `UNDROP` may require additional incarnation validation; same-identity restoration behavior must be confirmed before enablement |
| Repartition / partition-expression change | Refresh route and partitioner; rebuild rows against the current layout when the old target or expression is no longer valid | The original request may already have applied before its outcome became unknown; rebuilding may duplicate rows |
| Flow-source creation or removal | Use the plan-time Flow-source snapshot for eligibility | A change after planning is not detected by this check |

A route or partition-expression change can leave the original invocation's remote outcome unknown. Rebuilding against refreshed metadata may therefore duplicate rows already applied by the old invocation; v1 does not add a Datanode-side idempotency or stale-write fence.

### Retarget

If refreshed metadata shows that the same `RegionId` and `partition_expr_version` remain valid, replay may retarget the existing record by updating its destination and routing information. Retarget preserves
record identity, first-round provenance, invocation history, retry deadline, lineage limits, FIFO position, and waiter. It does not change the captured plan-time table snapshot or consume another replay
invocation.

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

At parent finalization, the queue uses the committed successor set. If at least one successor committed, the parent settles `rebuilt`, even if other ordinals were rejected or never attempted. If none committed, the parent settles from its own invocation history. Each completed FanOut emits a structured lineage diagnostic containing the planned target/row counts, committed successor/row counts, and rejected/not-attempted counts grouped by reason. Low-cardinality counters aggregate target outcomes and reasons; table, Region, lineage, and record identifiers remain in structured logs, not metric labels. Parent finalization does not wait for successor application and does not release destination barriers.

### Destination ordering and barriers

Queue scheduling is per `(Frontend queue, RegionId)`, with at most one replay record locally in flight for a destination Region. Records admitted to the same Region queue are replayed in queue order. This defines local scheduling and send-authorization order, not remote application order: an earlier invocation whose result is `issue_unknown` may still apply after a later invocation.

A destination barrier is installed when the first successor for a FanOut is admitted to that destination. Records ordered ahead of the barrier are processed before its successors. Successors belonging to the same FanOut run in ordinal order. Normal queue records ordered after the barrier remain behind its successor segment until the barrier releases. Multiple barriers retain their installation order, and nested FanOut joins an applicable existing barrier or installs a later position; it does not bypass an earlier barrier.

A barrier normally releases when its FanOut lineage is quiescent, the replay invocations associated with that destination have definitive observed outcomes, and every first-round request addressed to that destination has a definitive per-entry result. An invocation that remains `in_flight` or `issue_unknown`, including a timeout, lost response, or post-authorization worker failure, is not ordering-safe. A whole-RPC first-round failure without a per-entry result also keeps the barrier closed when the request was addressed to that destination. A first-round request addressed to a different, obsolete source Region does not by itself hold the new destination after refreshed routing confirms that the source Region is no longer a valid target; this does not prove that the old request was not applied at its source. A later `applied` result does not erase an earlier unknown invocation. This normal release rule preserves the strongest ordering evidence available from the queue, but an unknown remote outcome can otherwise keep a destination blocked indefinitely.

To bound that blockage, every barrier also has an expiry derived from the lineage's existing retry deadline and replay RPC timeout:

`barrier_expiry = lineage_retry_deadline + write_queue.replay_rpc_timeout`

At the lineage retry deadline, the queue stops authorizing new replay RPCs and stops publishing new FanOut successors for that lineage. Any metadata refresh or FanOut planning still in progress is cancelled before it can publish more successors; uncommitted work is abandoned, temporary reservations are released, and remaining rows are counted as `not_attempted`. Replay RPCs authorized before the deadline may finish until their own timeout. Once the barrier expiry is reached and no local replay or FanOut operation for that lineage remains active, the barrier is force-released even if first-round or replay history still contains an unknown outcome. Metadata refresh and FanOut planning must therefore respect the lineage deadline and stop before the barrier expiry. The barrier release is recorded as `ordering_uncertain`; the underlying `issue_unknown` history remains unchanged.

This is a bounded liveness fallback, not proof that the remote write was not applied. A Datanode may still apply an earlier request after the Frontend has timed out or lost its response. After an `ordering_uncertain` release, later queue records may proceed, and a late remote application may therefore appear after a later write. v1 accepts this residual ordering risk to avoid an indefinitely blocked destination. No new public timeout setting is introduced: the bound reuses the lineage retry budget and replay RPC timeout already defined above.

An `ordering_uncertain` release records local ordering uncertainty, not write success or failure, and does not change the underlying `issue_unknown` history. Each expiry increments a low-cardinality counter and emits one structured warning before retained barrier history is reclaimed, with the table/destination and lineage/barrier identities, expiry time and age, and available unresolved-outcome references. Operators should alert on a sustained or non-negligible release rate using a deployment-specific threshold. v1 provides no per-release query, manual replay, rollback, or repair tooling; the log, subject to normal retention, is the only per-release investigation evidence.

Expiry affects only the barrier being released. It does not mark unrelated barriers uncertain or force-release them. Removing this barrier allows the destination queue to advance; later records may still stop at another barrier, which follows its own release conditions and expiry. The resulting business risk is a possible ordering inversion or stale-write effect, not an automatic rollback or a guarantee of physical data corruption. Whether that risk is acceptable depends on the table's write patterns and merge semantics; v1 should only be enabled for workloads that can tolerate it.

The barrier orders successors within one FanOut and preserves the order in which barriers are installed on a destination queue. It does not define an old-before-new order between independent records from different source Regions that are later rebuilt into the same destination Region. Such lineages enter the destination queue independently, and their relative order follows destination-queue admission rather than original client arrival or source-Region admission. v1 makes no cross-source-key FIFO guarantee for this merge case. It also does not order live writes or records in another Frontend.

## Resource Limits and Operations

### Retained and transient memory

`write_queue.max_bytes`, `write_queue.max_records`, and `write_queue.max_record_size` bound retained queue accounting, not process RSS or peak allocation. A retained record keeps the original routed per-Region request; admission does not encode/decode the payload or allocate a second full request copy solely for queue retention. A retained record is charged using an encoded-size proxy for its single-Region request. The proxy is an accounting estimate, not a newly allocated encoded payload, and does not represent exact heap usage or all allocator, index, runtime, and transport overhead. The retained and transient ledgers are separate controls, not one shared byte ceiling; their configured maxima plus runtime overhead define the queue's peak-memory planning envelope and must be selected together against the Frontend memory budget. `max_records` counts currently retained request records. `max_record_size` is checked before queue reservation but cannot prevent the first-round request allocation.

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

If the planning reservation fails, FanOut planning does not begin; no destination ordinal has yet been created, so the parent records a bounded planning-level rejection diagnostic rather than fabricating per-ordinal outcomes. If a destination reservation or admission fails after planning, that ordinal is counted as `rejected` with its bounded reason, and other ordinals may continue while the admission pass remains open. On publication, the destination reservation is converted to retained-record accounting exactly once; on rejection, cancellation, worker failure, or shutdown, it is released exactly once. A reservation failure does not evict retained records or release existing barriers.

### Replay scheduling and fairness

Each Frontend queue has `write_queue.replay_concurrency` replay slots and permits at most one locally in-flight replay per Region. Runnable Regions are scheduled round-robin among those currently
ready. A Region waiting for backoff, a leader, or an earlier barrier is not runnable until that condition changes.

This gives currently runnable Regions an opportunity to make progress; it does not provide a bounded waiting time or per-record latency guarantee. A hot Region remains serialized, and its tail delay
grows with its backlog. All queue limits, transient memory, and replay concurrency are per Frontend, not cluster-wide.

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

FanOut counters should distinguish `admitted`, `rejected`, and `not_attempted` target outcomes, with bounded reason dimensions. Barrier metrics should distinguish normal from expiry-based `ordering_uncertain` release and report barrier age. Each expiry emits one structured warning with table/destination, lineage/barrier, expiry/age, and available unresolved-outcome context; high-cardinality identities belong in logs, not metric labels. Operators should alert on sustained or non-negligible expiry rates using a deployment-specific threshold. A completed rebuild should also emit one structured diagnostic with the per-lineage target/row reconciliation totals defined above; all target outcomes and row counts must be accounted for.

## Configuration and Rollout

The public queue settings use the `write_queue.` prefix. The values below are initial configuration values for validation and are not benchmark claims. Workload measurements should be used to tune them before rollout. `write_queue.max_records = 100000` is a provisional test point, not a capacity or drain-time guarantee. Internal lineage and transient limits are implementation bounds whose concrete values must be selected and validated before enablement.

| Parameter | Scope | Purpose | Initial value / status |
|---|---|---|---|
| `write_queue.enable` | Per Frontend | Enable queue ownership for supported insert paths | `false` by default |
| `write_queue.max_bytes` | Per Frontend | Retained payload, barrier, and retained history accounting | 512 MiB, initial value |
| `write_queue.max_records` | Per Frontend | Maximum currently retained request records | 100000, provisional test point |
| `write_queue.max_record_size` | Per record | Reject a request above the retained-size limit | 512 KiB, initial value |
| `write_queue.retry_budget` | Per lineage | Absolute authorization window shared by successors | 5m, initial value |
| `write_queue.completion_wait` | Per client request | Maximum synchronous wait; `0` means no wait | 30s, initial value |
| `write_queue.replay_rpc_timeout` | Per invocation | Maximum wait for one replay RPC after authorization | 10s, initial value |
| `write_queue.replay_concurrency` | Per Frontend | Global replay slots | 8, initial value |
| `max_lineage_records` | Per lineage | Cumulative parent-plus-successor record limit | Internal; fix before enablement |
| Lineage invocation cap | Per lineage | Cumulative authorized replay RPC limit | Internal; fix and validate before enablement |
| Transient rebuild budget | Per Frontend | Peak temporary FanOut materialization accounting | Internal; measure and fix before enablement |
| `write_retry.max_backoff` | Existing retry configuration | Maximum replay scheduling backoff | Existing setting reused (10s) |

With queue mode enabled, `write_retry.max_retries` does not control attempts transferred to the queue. Those attempts use `write_queue.retry_budget`, the lineage invocation cap, and replay backoff.
Paths that retain inline retry continue to use the existing retry configuration. The replay RPC timeout is queue-owned and must be configured independently of the inline retry policy.

Configuration is loaded at startup; hot reload is not part of v1. Disabling the queue requires a configuration change and process restart. Shutdown does not promise to drain or recover retained
records.

`write_queue.enable = true` is blocked until all of the following are complete:

- The queue handoff preserves first-round per-entry results and original per-Region requests while bypassing inline retry for queue-owned attempts.
- Plan-time metadata is captured and propagated through request grouping, per-Region splitting, and queue admission.
- Replay performs the required cache invalidation, table-identity check, metadata reload, and refreshed-partitioner rebuild.
- Per-Region FIFO, FanOut-local destination barriers, and bounded `ordering_uncertain` barrier expiry pass the ordering validation below; no cross-source-key merge ordering or DDL fence is claimed.
- FanOut, record/invocation limits, retained-history accounting, transient memory limits, shutdown waiters, and failure semantics pass the validation plan below.
- `max_lineage_records`, the invocation cap, and transient rebuild budget have concrete implementation values selected and validated against the declared workload envelope; these internal values are not public configuration knobs.
- The transient estimator is validated against representative replay, FanOut planning, and destination-materialization shapes. Every enabled rebuild path reserves before row-selection/partitioning allocations and before each destination payload materialization. Metadata refresh and FanOut planning must stop at the lineage deadline so no successor can be published after barrier expiry. An unbounded eager FanOut path blocks queue enablement until enforceable pre-allocation reservations are implemented.
- Maintainers accept the DDL residual risks, duplicate-capable retry semantics, and the bounded barrier-expiry trade-off: unresolved remote outcomes do not block a destination forever, but a late remote application may occur after the barrier is released and may affect a later write to the same `(primary key, time index)` under the table's merge semantics. The expiry counter, structured warning diagnostic, and deployment-specific alert threshold are defined and validated.
- The client contract is reviewed, and failure-path/drain measurements are reported against the declared workload envelope.

The queue depends on three write-path improvements before enablement:

- **Bounded completion wait:** the caller uses one absolute response-wait budget that is consumed by first-round result processing, synchronous admission, and any replay wait. Expiry does not cancel the admission commit decision; it prevents further synchronous replay waiting.
- **Broader retryable-failure admission:** classification and retention cover eligible per-entry failures, retryable whole-RPC failures, and retryable single-entry-collapsed failures when the original per-Region request and plan context can be retained. A whole-RPC error without a recoverable request is not made queue-eligible merely because it is retryable.
- **Absolute time budget:** each admitted lineage has one retry deadline inherited by all FanOut successors; route lookup, rebuild, backoff, and leaderless deferral consume that same budget.

These enhancements form one retry contract but may be implemented and merged independently. Queue enablement requires all three to be available and validated. Durability and restart recovery remain a separate proposal and are not prerequisites for this in-memory queue.

## Risks and Limitations

- **Duplicate-capable retry.** A lost response, timeout, worker failure after submission, FanOut, or client retry can cause duplicate writes. Neither a retryable response nor a later successful
  invocation proves that an earlier unknown invocation did not apply.
- **Partial batch outcome.** A batch may return an error because one sibling has a terminal failure while a retryable sibling is admitted and applies later. The client can therefore observe a failed batch whose retryable subset is partially applied. Admitted records may also apply after the client wait expires; a batch error does not mean that no rows were written.
- **No inline fallback.** An otherwise eligible attempt rejected by queue capacity or closure receives no Frontend retry after the first round. A client retry may duplicate append or Flow-related
  writes.
- **Process-local loss.** Restart, crash, shutdown, or deadline expiry can discard admitted work. The queue is not durable and provides no restart recovery.
- **Barrier expiry and stale-write risk.** An unresolved invocation may hold a FanOut destination barrier until the lineage retry deadline plus one replay RPC timeout. The barrier is then released as `ordering_uncertain` so the destination can progress, although a remote request may still apply late and overtake a later queue write. If both writes target the same `(primary key, time index)`, the late-arriving row may affect the merged result according to `merge_mode`; under `last_row`, the latest inserted row takes precedence. Different timestamps remain distinct samples, so an older timestamp does not automatically overwrite a newer one. v1 does not detect, roll back, or repair stale-write effects. This is a workload-dependent ordering/consistency risk, not a claim that every expiry corrupts data. Before expiry, lineage-wide quiescence can also block one destination while a descendant is delayed in another Region.
- **Incomplete ordering scope.** FIFO is per destination queue and FanOut successors preserve only their own ordinal order. v1 does not define ordering between different old source-Region records that merge into one new destination; it also does not order original client request start time, unadmitted first-round requests, live writes, or other Frontends. An expiry-based barrier release explicitly trades strict local waiting for progress when the remote outcome remains unknown.
- **Resource accounting is not an RSS bound.** Retained and transient ledgers constrain their defined accounting units, not every Frontend allocation or total process memory. FanOut planning and each destination materialization must reserve before allocation; request shapes without a conservative pre-planning estimate are rejected for rebuild.
- **DDL stale-replay window.** Metadata revalidation is not an atomic fence. A client may receive a write error, run `TRUNCATE`, and then have a queued replay reinsert rows that the client expected to be removed. `ALTER` can race between validation and RPC submission; `TRUNCATE` may not be distinguished by the captured table version; `DROP`/`UNDROP` incarnation behavior requires confirmation; Flow-source changes after planning are not re-read.
- **Hot-Region drain.** Per-Region serialization means global replay concurrency cannot shorten the tail of a single hot Region. Records near the end of a backlog may expire before their first
  authorization.

## Validation Plan

Validation should establish the behavior of the design rather than prescribe specific internal data structures or test harness APIs.

- **Dispatcher ownership and request reuse:** with the queue disabled, existing behavior is unchanged. With queue mode enabled, direct ordinary physical inserts use queue handoff after the first round; Delete, metric logical-batcher flush, pending-rows/table-batcher flush, BulkInsert, and Flight `DoPut` retain inline retry. Verify this at producer boundaries, including callers that reuse internal insert-building helpers; queue eligibility must not be inferred from the request shape alone. Cover retryable per-entry, whole-RPC, and single-entry-collapsed failures when their original per-Region request and plan snapshot are recoverable. Verify the queue retains the original routed per-Region request without an extra full-payload copy. Rejected queue attempts never fall back to inline retry.
- **Plan snapshot and eligibility:** verify the plan context is captured from table metadata already held by the insert-planning path and carried through grouping, splitting, and admission without an additional awaited metadata lookup. Mutate table version, engine, append mode, TTL, Flow status, and partition-expression metadata after planning; verify that impure defaults on primary-key/time-index columns are rejected while non-key defaults do not cause unnecessary rejection; replay must compare against the captured plan snapshot. Verify admission rejects missing or ambiguous required metadata, unsupported engines, and an absent captured partition-expression version. Validate metric logical-row inserts separately from metric logical-batcher and pending-rows flushes. Where Flow mirroring applies, verify it runs once on the original path and is never repeated by queue replay.
- **Admission and client contract:** verify request cancellation observed before publication leaves no record or barrier and releases all temporary reservations; cancellation after publication only ends the client wait. Verify the completion-wait deadline expiring during bounded local classification/admission does not cancel publication, that `completion_wait = 0` still performs admission but does not wait for replay, and that positive remaining budget is required for synchronous replay waiting. Verify terminal-error precedence, dispatcher-attempt-order selection, partial admission, `affected_rows`, waiter registration racing with settlement, and shutdown waiter behavior.
- **Replay history and authorization:** verify that a later `applied` invocation does not erase an earlier `issue_unknown`, late responses do not rewrite terminal invocation outcomes, and record settlement labels do not independently release barriers. Verify table, route, leader, and client lookups complete before send authorization; no additional lookup or `await` occurs between authorization and issuing the RPC. Confirm that an authorization recorded before an unobserved worker failure is classified as `issue_unknown`, not `not_sent`. Verify the existing jittered backoff schedule is capped by `write_retry.max_backoff`, is inherited by FanOut successors, and that the absolute deadline and invocation cap remain lineage-wide.
- **Route refresh and FanOut:** start with stale caches, move or repartition a table, and verify cache invalidation, table identity, refreshed partitioning, and coalesced refreshes. Verify an unknown result from an obsolete source Region does not block a different confirmed destination, while a lost response from an old-Region RPC is not treated as proof that the write was unapplied. Verify partial successor admission, exact target/row accounting, planning-reservation rejection, and no orphaned barrier or record state.
- **Destination ordering:** verify per-Region FIFO, FanOut ordinal order, and nested/intervening barrier order. An unresolved destination outcome must hold its barrier through the retry deadline; then stop new lineage work, wait for authorized RPCs to finish or time out, and release as `ordering_uncertain` within one further RPC timeout, with exactly one counter increment and warning log before retained barrier history is reclaimed. Verify expiry releases only that barrier, preserves `issue_unknown`, and leaves later progress subject to other barriers. Exercise late writes under the configured merge semantics for the same `(primary key, time index)` and confirm different timestamps remain distinct; verify no cross-source-key FIFO guarantee is claimed when old Regions converge on one destination.
- **Resource and failure behavior:** force retained, transient, record, and invocation limits to exhaust. Verify history remains accounted while referenced by barriers, settled payload can be released safely, and reservations are not leaked or double charged. Instrument allocation points to prove the FanOut planning reservation precedes row-selection/partitioning allocation and each destination reservation precedes payload materialization. Compare reservation estimates with measured replay and FanOut peaks across representative shapes; verify that an unbounded eager FanOut path blocks enablement until an enforceable pre-allocation bound is implemented. Inject worker failure before and after authorization, partial FanOut interruption, shutdown during replay, and no-leader recovery through bounded timer backoff.
- **Performance and workload envelope:** compare queue-disabled and queue-enabled behavior under the same request shapes, concurrency, and failure injection. Measure successful-path overhead from plan-context capture separately from failure-path overhead added during request completion by classification, admission, reservation, and waiter setup. For the failure path, report p95/p99 request latency, admission latency, retained/transient memory, FanOut width, per-Region depth/skew, RPC latency distribution, time-to-first-authorization, fraction authorized before deadline, drain time, deadline expiry, and barrier blocking. Use the lower-bound model above to interpret results and agree on the acceptance envelope before enablement.

## Questions for Review

**Core design decisions**

- Is the v1 ownership contract acceptable: queue only ordinary Mito physical-Region inserts and eligible normalized metric logical-row inserts; leave Delete, BulkInsert, Flight `DoPut`, and metric/pending-rows batcher flushes on inline retry; and fail closed on rejected admission, with the parent retaining its selected first-round error even if FanOut successors later apply, under the documented publication, cancellation, and completion-wait rules?
- Is the DDL/stale-replay risk acceptable without an atomic DDL fence, including `TRUNCATE` followed by replay, `ALTER`, `DROP`/`UNDROP`, asynchronous metadata propagation, and the check-to-RPC race?
- Is this v1 ordering contract acceptable: per-Region FIFO and FanOut-local successor order, but no cross-source-key FIFO when old Regions converge; unresolved barriers expire to `ordering_uncertain` at the lineage deadline plus one replay RPC timeout, accepting possible late-write effects with counter, warning-log, and deployment-alert visibility?
- Are the separate retained/transient memory budgets and pre-allocation FanOut reservations sufficient without an aggregate queue-memory ceiling, and what backlog, hottest-Region depth, deadline-expiry, drain-time, and failure-path p95/p99 thresholds should gate enablement?

## Alternatives and Future Work

A post-inline-retry buffer was rejected because it would introduce a second scheduler and retry budget for the same write. Inline fallback while the queue is enabled was rejected because it bypasses per-Region ordering and splits retry ownership. v1 does not add a global or cross-source-key ordering mechanism; stronger ordering for records from different old Regions that converge on one destination requires a separate design. A permanent barrier hold was rejected because one unresolved remote outcome could stop a destination indefinitely; the bounded expiry rule above releases the local barrier with an explicit `ordering_uncertain` outcome. A successor completion group is not included because the queue cannot provide an all-or-nothing result across a FanOut lineage.

Durable retention, restart recovery, region-side idempotency, deduplication, and a stronger DDL fence require additional protocol and storage work and are deferred to separate designs.
