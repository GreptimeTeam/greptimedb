---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
Status: Draft for discussion
---

# In-Memory Frontend Write Retry Queue

## Summary

This RFC proposes an optional, per-Frontend in-memory retry queue for eligible transient write failures that may outlast the existing inline retry window. The initial scope is ordinary `Body::Inserts`
requests handled by `Inserter`, including Prometheus remote write only when it reaches the same supported insert path and satisfies the same admission rules.

The first write round remains synchronous. Eligible failed per-Region attempts are handed to the queue after first-round classification. An admitted attempt has one retry owner: the queue. The queue
performs bounded, best-effort replay with per-Region scheduling, an absolute retry deadline, retained-capacity limits, and cumulative limits on FanOut records and replay invocations. An attempt
rejected at admission does not fall back to inline retry.

The queue is process-local and best-effort. It does not provide durability, restart recovery, at-least-once or exactly-once delivery, proof that an unknown write was not applied, or guaranteed
eventual application. It is disabled by default. It can be enabled through configuration only after the prerequisite write-path improvements, implementation validation, resource limits, and maintainer
acceptance described below are complete.

## Motivation

The existing inline retry path has a bounded retry window. The baseline assumed by this proposal is `write_retry.max_retries = 3` additional rounds, with jittered exponential backoff starting around
1.5 seconds and capped by `max_backoff = 10 seconds`. The relevant Frontend-to-Datanode channel does not currently impose a per-attempt Frontend timeout. These values and behaviors must be checked
against the source revision used for implementation; they describe the existing path, not a new queue guarantee.

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
- Replacing retry behavior for Delete, BulkInsert, or metric-engine logical-batcher flushes.
- Global ordering across Frontends, Regions, or concurrent live writes.
- An atomic DDL fence or a transaction boundary across a FanOut lineage.
- A dead-letter query API.
- Recovery from failure of the queue's own coordination component.

## Request Scope and Retry Ownership

### Supported paths

The proposed handoff occurs after the first-round outcomes for a supported `Inserter` request have been collected. Eligibility is based on the normalized per-Region insert attempts and their target
table metadata, not solely on the external protocol or caller name.

| Request path | Queue behavior when enabled |
|---|---|
| Ordinary inserts through `Inserter` (`Body::Inserts`), including SQL, gRPC, pipeline, COPY, logs, traces, and Prometheus remote write that reaches this path | Queue admission is evaluated per failed Region attempt |
| Metric logical-row inserts handled by `handle_metric_row_inserts` and then routed through `do_request` | Not automatically included or excluded by entrypoint; eligible only when each normalized target is a supported physical table request and carries the required plan snapshot |
| Metric-engine logical-batcher flush | Existing inline retry; remains outside this queue |
| Bulk insert (`Body::BulkInsert`), Flight `DoPut`, and table-batcher flush | Existing inline retry; remains outside this queue |
| Delete (`Body::Deletes`) | Existing inline retry; remains outside this queue |

For v1, the supported target is a Mito-backed physical Region write. A request that still targets a logical Metric-engine table, uses another engine, or cannot be unambiguously mapped to a supported
physical target is ineligible. A metric logical-row request is not equivalent to a metric-engine logical-batcher flush: the former may pass through the ordinary insert path, while the latter remains
on its separate inline path. This distinction must be reflected in the dispatcher wiring and the admission tests.

The existing wiring shares a `WriteDispatcher` across insert and delete paths, and the logical batcher reuses the inserter's dispatcher. The proposal therefore requires an explicit split between
queue-enabled ordinary inserts and paths that retain inline retry. `Deleter`, the metric logical-batcher flush, and the bulk-inserter path must not accidentally inherit queue ownership.

### Queue-mode first-round behavior

Queue mode is a distinct retry policy, not the existing no-retry dispatcher mode. It must preserve the first-round batching and result semantics needed for queue admission:

- The first-round request still supports per-entry/partial Region results.
- Failed entries are split into their corresponding per-Region attempts.
- The failed attempt retains the `RoutedWrite` required for later replay.
- The first-round request is sent once; `finish` does not run the legacy `should_retry` / `retry` loop for queue-owned attempts.

This distinction is essential. The existing no-retry path does not retain the same per-entry request information required by the queue, while the existing inline-retry path executes its retry loop
before a queue handoff could occur. Queue mode must preserve the former path's first-round result representation without also enabling its inline retry behavior. The exact dispatcher API is an
implementation choice; the two behaviors above are the contract.

Once an attempt is admitted, the queue owns all subsequent retry, route refresh, retarget, and rebuild decisions. The original inline retry and route-refresh loop must not also retry it. An eligible
attempt rejected for capacity, a closed queue, or unsafe context is returned as a first-round failure and receives no inline fallback. Paths outside the queue's supported request scope continue to use
the existing inline path.

### Eligibility and plan-time context

Admission is evaluated independently for each failed single-Region attempt. A request is eligible only when it:

- is one ordinary insert request represented by `Body::Inserts`;
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

The snapshot must be captured while the insert plan is still available, before request grouping and splitting can discard the association between a table and its Region attempts. It must carry, at
minimum, the `TableId`, `TableIdent.version`, target engine, effective append-mode state, instant-TTL state, Flow-source state at plan time, whether an impure default affected a primary-key/time-index
column, and the target Region's `partition_expr_version`. The snapshot must remain associated with each resulting `InsertRequest` after peer batching and per-entry failure extraction. Reading current
metadata at admission is not a substitute: it would lose the distinction between the table state used for the first write and a later DDL change.

Append mode must be captured as the effective plan-time setting used by the Region write path; it must not be assumed to be a single field on `TableInfo`. Flow-source status is captured from the
plan-time Flow metadata/cache. It is not silently replaced with a later lookup during replay. If the implementation cannot provide an unambiguous value for any required field, the attempt is rejected
as `UnsafeContext`.

Queue replay also needs access to the metadata and cache-invalidation services required to refresh table identity and routing. Those collaborators must be available to the queue independently of
whether the legacy inline `WriteRetry` component is enabled. The queue cannot rely on a retry-enabled dispatcher flag to obtain them.

Admission is the ownership boundary. Before publication, the queue does not own the attempt. After publication, client cancellation or completion-wait expiry does not revoke it; the queue retains
responsibility until its retry deadline or a terminal outcome.

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
first retryable failure in the dispatcher-produced attempt order. This is the order of attempts returned by the existing dispatcher, not original request input order: requests grouped by peer do not
currently preserve a global input index across peers. The RFC does not promise a stable cross-peer input-order selection unless the implementation explicitly preserves that index.

If multiple terminal failures exist, the first terminal failure in dispatcher attempt order is selected. Admission success or failure does not change the selected error. Each eligible retryable
attempt is admitted independently to its destination Region FIFO. If a batch contains a rejected attempt, the request returns the selected error even when sibling attempts are admitted. There is no
inline fallback for rejected attempts because that would bypass per-Region scheduling, queue limits, and single-owner retry semantics.

## Client-Visible Contract

The parent request's waiter receives the selected first-round error; it does not receive a later successor outcome. Successful successors do not turn the parent's result into `Ok`. There is no
all-or-nothing completion result across a FanOut lineage.

The completion-wait budget starts when `finish` begins and includes first-round result processing and queue admission. After admission, the request waits only for the remaining budget. If the budget
expires during admission, or if `write_queue.completion_wait = 0`, the selected error is returned immediately. A settlement racing with admission must still be observable by a registered waiter.

The request waits only when there is no terminal first-round failure and every retryable attempt was admitted. Otherwise, it returns the selected error immediately while admitted siblings continue
independently.

`Ok` means that every admitted record settled `applied` before the completion-wait deadline and no retryable attempt was rejected. It does not prove exactly-once behavior: the first-round request or
an earlier replay invocation may have been applied even though its response was lost, and a later replay may also apply. Any non-applied record settlement, a rebuilt parent, a rejected retryable
attempt, or wait-deadline expiry returns the selected error.

`affected_rows` reports observed applied rows from first-round and replay successes. It is not an exact logical row total and may not account for duplicate application. A same-Region retarget retains
the original waiter. A FanOut parent notifies its waiter with the selected error; successors do not create separate client waiters.

Client timeout or cancellation ends only the wait. It does not delete, settle, cancel, or reassign an admitted record and does not cancel an in-flight replay RPC. On orderly queue shutdown, any
still-pending completion waiter is woken immediately with its already-selected first-round error. Shutdown does not wait for the remaining completion-wait duration. A waiter already completed before
shutdown is unaffected.

## Queue and Retry Semantics

### Records, lineages, and invocations

A **record** represents one admitted single-Region attempt. It retains the routed request, first-round provenance, absolute deadline, lineage identity, and settlement history. A **lineage** consists
of an admitted parent record and all records derived from it by FanOut. A **successor** is a descendant record in that lineage. A successor inherits the lineage deadline and backoff progression, but
has its own record identity and invocation history. It does not inherit the parent's replay invocation outcomes.

An **invocation** is one replay RPC that has passed the queue's send authorization. Its outcome is one of:

- `in_flight`;
- `applied`;
- `observed_retryable_failure`;
- `observed_terminal_failure`; or
- `issue_unknown`.

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
| Region exists but has no leader | Retain the record at the Region FIFO head and defer with backoff | No new invocation is created |
| Replay RPC timeout | Classify the invocation `issue_unknown`; a distinct invocation may be scheduled only if the deadline and invocation cap allow it | The unknown invocation keeps affected barriers closed |
| Retry deadline reached before authorization | Stop scheduling and settle from recorded history | Existing unresolved invocation history remains effective |
| Retry deadline reached while an invocation is in flight | Authorize nothing further; allow the in-flight RPC until its own timeout | If no result is observed, the invocation becomes `issue_unknown` |
| Lineage invocation cap exhausted | Stop new authorizations and successor admissions; settle records from recorded history | Unresolved unknowns keep affected barriers closed |
| Lineage record cap exhausted | Stop new successor admissions; already admitted records may continue within the remaining invocation budget | Existing barrier state is unchanged |
| Worker failure before authorization | Finalize the record from its existing history; do not invent an unknown invocation or automatically restart the failed replay | No send-side unknown is created |
| Worker failure after authorization without an observed result | Classify that invocation as `issue_unknown` and finalize the record from its history | Affected barriers remain closed |
| Client cancellation | End the client wait only; queue work continues | No change |

A **definitive per-entry first-round result** means the dispatcher has an explicit disposition for each relevant Region entry, either success or an entry-level failure. A whole-RPC transport error
without per-entry results is not definitive. This distinction is used only for the destination-ordering decision below; it does not turn a retryable failure into a successful write.

### Retry budget and lineage limits

The absolute retry deadline is `admission_time + write_queue.retry_budget` and is inherited by every successor. Backoff, route lookup, metadata refresh, rebuild, and leaderless deferral consume the
deadline but never extend it. An invocation authorized before the deadline may run until its own `write_queue.replay_rpc_timeout`, which starts at authorization. Settlement may therefore occur up to
one RPC timeout after the retry deadline; the retry budget is not a strict total-lifetime bound.

The existing jittered exponential backoff schedule is reused for replay scheduling, with `write_retry.max_backoff` as the maximum individual wait. Backoff progression is inherited by successors as
lineage scheduling state; a Region with no leader advances the backoff without consuming a replay invocation. A replay invocation is charged only when a replay RPC is authorized.

Two independent cumulative limits apply to each lineage:

- `max_lineage_records` limits the total number of parent and successor records ever admitted for that lineage. It gates successor admission only. Exhausting it does not prevent already-admitted
  records from using the remaining replay invocation budget.
- The lineage invocation cap limits the total number of replay RPCs authorized by the parent and all successors. It gates new replay authorization and successor admission. It is not reset when a
  successor is created.

There is no additional per-record retry-count limit in v1. The legacy `write_retry.max_retries` applies only to paths that retain inline retry. The lineage invocation cap and absolute retry deadline
are the queue's hard replay bounds. A candidate invocation-cap formula is `4 * N + 8`, where `N` is the maximum FanOut width in the accepted workload; the actual value must be fixed and validated
before enabling the queue.

### Worker failure, cancellation, and shutdown

The queue maintains a single active replay owner for each claimed record. A worker failure before send authorization creates no replay invocation; the record is finalized from its existing history and
the failed replay is not automatically restarted. If a worker fails after authorization and no definitive result was observed, that invocation is `issue_unknown`; the record is finalized from its
recorded history and is not silently treated as unsent.

If FanOut is interrupted, only successors whose admission committed remain published. Uncommitted destinations have no record and no retained charge. The parent is finalized from the committed
successor set. Parent settlement as `rebuilt` does not itself release destination barriers.

Client cancellation is distinct from queue shutdown. Client cancellation ends only the client wait. Queue shutdown stops new replay authorization and closes active FanOut admission. It classifies
authorized in-flight invocations without observed results as `issue_unknown` and finalizes active parent admission from committed successors. Shutdown does not drain the queue, recover records, or prove
that in-flight remote requests did not apply. Forced process termination may discard all remaining queue state.

## Routing, FanOut, and Destination Ordering

### Metadata validation and route refresh

At planning time, each eligible attempt captures the table identity and the metadata that determines whether its original rows remain safe to replay: `TableId`, `TableIdent.version`, engine, effective
append-mode state, instant-TTL state, plan-time Flow-source status, and `partition_expr_version` for its target Region. `TableInfoValue.version` is a meta-store CAS version, not a substitute for
`TableIdent.version`. `TableRouteValue.version` is route bookkeeping, not by itself a write-path correctness fence.

Replay reloads current table metadata and rejects the record if the table identity, `TableIdent.version`, engine, append-mode state, or instant-TTL state no longer matches the captured plan.
Flow-source status is captured at plan time and is not re-read; a Flow status change after planning is a known residual risk. Replay uses the captured partition-expression version to determine whether
the original request may still be accepted after a partition-expression change. The Datanode-side protection for this check is part of the destination-ordering argument below.

A transient metadata or route lookup failure is deferred with backoff and is not treated as proof that a Region was removed. When the response indicates a stale route, missing Region, or
partition-expression mismatch, replay must perform the equivalent of the existing reroute refresh before rebuilding:

1. Invalidate the route/table cache entry keyed by `TableId` and reload the table metadata.
2. Invalidate the name-based cache and resolve the table name again.
3. Verify that the name still resolves to the same `TableId`; never replay an old request against a newly created table that reused the name.
4. Build the current partitioner from the refreshed table metadata and use it to determine the new destination Regions.

The queue does not re-enter the original dispatcher, so it must own or receive the metadata and cache-invalidation capabilities required for this refresh. Reusing the first-round partitioner or only
refreshing the peer address is insufficient: it can repeatedly rebuild against stale routing until the retry deadline expires.

The current validation boundary is not an atomic DDL fence:

| Operation or change | Design behavior | Residual risk |
|---|---|---|
| Physical or logical `ALTER` that changes `TableIdent.version` | Reject replay when the captured and current versions differ | DDL can commit after metadata validation but before RPC submission; cross-Frontend cache propagation is asynchronous |
| `TRUNCATE` | The current version check may not detect it because `TableIdent.version` is not incremented by the existing path | A queued write may replay after truncate; a stronger generation fence is outside this RFC |
| `DROP` | A missing table is terminal | A later `UNDROP` may require additional incarnation validation; same-identity restoration behavior must be confirmed before enablement |
| Repartition / partition-expression change | Refresh route and partitioner; rebuild when the old target or expression is no longer valid | Correctness depends on the captured expression version and Datanode stale-request rejection |
| Flow-source creation or removal | Use the plan-time Flow-source snapshot for eligibility | A change after planning is not detected by this check |

The destination-ordering argument relies on the Datanode rejecting a late request to a removed Region and rejecting an obsolete request-side partition-expression version after the Region's partition
expression changes. These are the source-side protections assumed by this proposal; their behavior must be validated for the supported Mito-backed write path.

### Retarget

If refreshed metadata shows that the same `RegionId` and `partition_expr_version` remain valid, replay may retarget the existing record by updating peer and routing information. Retarget preserves
record identity, first-round provenance, invocation history, retry deadline, lineage limits, FIFO position, and waiter. It does not refresh the captured table snapshot or consume another replay
invocation.

### FanOut rebuild and admission

If refreshed routing shows that the old Region is absent, the partition expression changed, or the rows now map to multiple/different destination Regions, replay rebuilds the rows using the refreshed
table partitioner. FanOut is required because the partition expression determines which Region owns each row; sending the old Region payload to a new peer would not be a valid substitute for
repartitioning the rows.

FanOut produces an ordered sequence of destination ordinals. It may retain lightweight destination/row-selection metadata, but it must not retain all fully materialized destination requests before
capacity has been reserved. Each destination payload is materialized only after transient rebuild capacity is available. The reservation must cover the output payload and the temporary
row-selection/partitioning/encoding work included in the declared memory bound.

Successors are admitted independently. A successor becomes visible only together with the destination barrier position that protects its ordering. The `(parent_record_id, ordinal)` pair identifies one
successor admission, so retrying the same admission step cannot publish the same ordinal twice. FanOut is not atomic as a whole: some successors may commit while others are rejected or remain
unadmitted. A rejected ordinal leaves no successor record, barrier position, retained charge, or committed ordinal. Rejection of one ordinal does not itself prevent later ordinals from being attempted
while the admission pass remains open.

At parent finalization, the queue uses the committed successor set. If at least one successor committed, the parent settles `rebuilt`, even if other ordinals were rejected or never attempted. If none
committed, the parent settles from its own invocation history. Unadmitted rows are accounted for as not transferred to the queue. Parent finalization does not wait for successor application and does
not release destination barriers.

### Destination ordering and barriers

Queue scheduling is per `(Frontend queue, RegionId)`, with at most one replay record locally in flight for a destination Region. This defines local queue scheduling and authorization order, not remote
application order. An earlier invocation whose result is `issue_unknown` may still apply after a later invocation or record. No global, cross-Frontend, cross-Region, or live-write ordering is
promised.

A destination barrier is installed at the position where the first successor for a FanOut is admitted to that destination. Records admitted before that position are predecessors and are processed
before the barrier's successors. Successors for the same barrier run in FanOut ordinal order. Normal queue records admitted after the barrier remain behind its successor segment until the barrier
releases. Multiple barriers retain their installation order; a later barrier cannot allow its successors to overtake normal records admitted between the earlier and later barriers. Nested FanOut joins
an existing applicable barrier or installs a later barrier position; it does not bypass an earlier barrier. Joining an existing barrier does not create a second barrier-control charge.

A barrier releases only after the lineage is quiescent, every replay invocation addressed to that destination is ordering-safe, and every relevant first-round request to that destination has a
definitive per-entry result. An invocation is ordering-safe if it was never authorized or it has a terminal observed outcome. `issue_unknown`, timeout, lost response, post-authorization worker
failure, and an invocation unresolved at the retry deadline are not ordering-safe. A later `applied` result does not clear an earlier `issue_unknown`.

**First-round relevance is destination-scoped.** A first-round request is relevant to destination `D` when it was addressed to `D` and may still be accepted by `D`. A whole-RPC failure without
per-entry results for such a request keeps `D`'s barrier closed. A first-round request addressed to an obsolete source Region `R != D` does not by itself keep `D`'s barrier closed after route refresh
has established that a late request to `R` cannot be applied to `D`: the old Region is absent, or the old request's captured partition-expression version is rejected by the Datanode after the partition
expression changes. If the implementation cannot establish that destination-specific fencing condition, the barrier remains closed; this does not permit publishing a successor without its barrier.

This destination-scoped rule avoids making every newly discovered destination wait forever for a whole-RPC outcome from an obsolete source Region, while still preventing a barrier from overtaking an
unknown first-round write that could be accepted by that same destination. The proof depends on the Datanode's Region-removal and partition-expression checks described above; it is not a generic
guarantee for arbitrary RPCs.

The lineage-wide quiescence condition is deliberately conservative: a destination may remain blocked while a descendant is delayed in another Region. An unresolved invocation addressed to the
destination can keep its barrier closed indefinitely. v1 has no manual barrier release because releasing without ordering evidence would assert a safety property the queue cannot establish. A Frontend
restart discards its local queue and barriers but does not prove that an old remote RPC did not apply.

The ordering guarantee is limited to FanOut-local scheduling priority at destinations discovered by that FanOut. It does not cover old-key records that have not yet been rebuilt, live writes, other
Regions, other Frontends, or remote application order when an earlier invocation remains unresolved. In particular, the broader requirement that a destination wait for every queued old-key record that
could later rebuild into it is not guaranteed by this v1 barrier model; maintainers must decide whether that cross-lineage guarantee is required before enablement.

## Resource Limits and Operations

### Retained and transient memory

`write_queue.max_bytes`, `write_queue.max_records`, and `write_queue.max_record_size` bound retained queue accounting, not process RSS or peak allocation. A retained record is charged using an
encoded-size proxy for its single-Region request. The proxy does not represent exact heap usage or all allocator, index, runtime, and transport overhead. The retained and transient ledgers are
separate controls, not one shared byte ceiling; their configured maxima plus runtime overhead define the queue's peak-memory planning envelope and must be selected together against the Frontend memory
budget. `max_records` counts currently retained request records. `max_record_size` is checked before queue reservation but cannot prevent the first-round request allocation.

Barrier metadata and invocation/first-round history that must outlive a settled request are also queue-owned memory. Their bounded accounting must be included in `write_queue.max_bytes`; they cannot
be left outside both record accounting and barrier accounting. A settled record may release its payload and stop counting against `max_records`, but the compact history needed by an unreleased barrier
remains charged until no barrier can reference it. This separates retained payload capacity from the lifetime of ordering evidence.

Transient rebuild memory has a separate per-Frontend ledger shared by concurrent FanOut rebuilds. It covers destination payload construction and the temporary row-selection, partitioning, and encoding
allocations included in the accepted accounting model. The budget is an internal limit, not currently a public TOML parameter. Its value is selected from representative FanOut shapes and concurrent
rebuild measurements against the Frontend memory budget; it is not a new public tuning knob.

The estimator has two paths. Replay reuses the retained request, so its transient estimate covers the replay encoding buffer, per-call transport wrapper, and any body copy observed in the selected
send path. Rebuild estimates the maximum simultaneously live materialization stage, including row-selection/index state, partitioning state, live destination payloads, per-destination schema/header/
container overhead, and overlap between intermediate and output buffers. The shared transient budget must cover concurrent replay and rebuild reservations:

```text
E_replay = E_encode + E_transport_wrapper + E_measured_body_copy
E_rebuild_peak = max_over_live_stages(
    E_row_selection + E_partition_state + E_live_destination_payloads
    + E_destination_metadata + E_intermediate_overlap
)
E_transient = sum(all concurrently live replay and rebuild reservations)
```

The estimates are accounting models, not RSS formulas. They must be conservative enough to reserve before the corresponding allocations. If an eager FanOut path materializes the complete destination collection
before the estimate or reservation can be enforced, the ledger does not impose a hard bound on that actual peak. In that case the rebuild peak remains explicitly unbounded until incremental materialization
or another enforceable pre-allocation bound is available; the RFC must not describe `write_queue.max_transient_bytes` as a hard FanOut memory ceiling. This limitation is part of the enablement assessment, not evidence that the public configuration values above have been rejected.

The implementation must reserve transient capacity before materializing each destination payload. If the materialized output exceeds its reservation, the successor is rejected before publication. On
commit, the reservation is converted to retained-record accounting; on rejection, cancellation, worker failure, or shutdown, it is released exactly once. A failed reservation rejects only the current
successor; committed successors and existing barriers remain unchanged. Transient-budget exhaustion does not evict retained records or release barriers.

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

The validation workload envelope should state total backlog, per-Region depth distribution (including the hottest Region), replay RPC latency distribution, no-leader fraction, FanOut width, and expected failure mix. Report time-to-first-authorization, the fraction authorized before the retry deadline, total drain time, hottest-Region drain time, deadline expiries, and transient-budget rejections. The envelope is used to evaluate and tune the proposed starting values; it does not turn each public configuration value into a separate maintainer-approval item.

### Observability

Operational visibility should let operators answer the following questions:

- How much work is retained, and how close is the queue to its retained and transient capacity limits?
- Are failed attempts being admitted, rejected, replayed, or given up, and what broad reasons explain rejection or give-up?
- Are replay attempts making progress, reaching terminal outcomes, or expiring at the retry deadline?
- Are unresolved outcomes or destination barriers accumulating, and how long are destinations being blocked?
- Are FanOut rebuilds being partially admitted or rejected because transient capacity is unavailable?
- Are replay latency or worker failures preventing the queue from making recovery progress?

Metrics should use low-cardinality dimensions and must not use table, Region, record, lineage, or invocation identities as metric labels. Those identifiers belong in structured diagnostic logs. The RFC
defines the operational questions and cardinality constraints; concrete metric names and instrumentation are implementation choices.

## Configuration and Rollout

The public queue settings use the `write_queue.` prefix. Values below are the author's proposed starting values, not benchmark claims. They are not individually open maintainer-approval questions; workload measurements may justify tuning them before rollout. `write_queue.max_records = 100000` is explicitly a provisional test point, not a capacity or drain-time guarantee. Internal lineage and transient limits are implementation bounds whose concrete values must be selected and validated before enablement.

| Parameter | Scope | Purpose | Proposed value / status |
|---|---|---|---|
| `write_queue.enable` | Per Frontend | Enable queue ownership for supported insert paths | `false` by default |
| `write_queue.max_bytes` | Per Frontend | Retained payload, barrier, and retained history accounting | 512 MiB, author-proposed |
| `write_queue.max_records` | Per Frontend | Maximum currently retained request records | 100000, provisional test point |
| `write_queue.max_record_size` | Per record | Reject a request above the retained-size limit | 512 KiB, author-proposed |
| `write_queue.retry_budget` | Per lineage | Absolute authorization window shared by successors | 5m, author-proposed |
| `write_queue.completion_wait` | Per client request | Maximum synchronous wait; `0` means no wait | 30s, author-proposed |
| `write_queue.replay_rpc_timeout` | Per invocation | Maximum wait for one replay RPC after authorization | 10s, author-proposed |
| `write_queue.replay_concurrency` | Per Frontend | Global replay slots | 8, author-proposed |
| `max_lineage_records` | Per lineage | Cumulative parent-plus-successor record limit | Internal; fix before enablement |
| Lineage invocation cap | Per lineage | Cumulative authorized replay RPC limit | Candidate `4 * N + 8`; fix and validate before enablement |
| Transient rebuild budget | Per Frontend | Peak temporary FanOut materialization accounting | Internal; measure and fix before enablement |
| `write_retry.max_backoff` | Existing retry configuration | Maximum replay scheduling backoff | Existing setting reused (10s) |

With queue mode enabled, `write_retry.max_retries` does not control attempts transferred to the queue. Those attempts use `write_queue.retry_budget`, the lineage invocation cap, and replay backoff.
Paths that retain inline retry continue to use the existing retry configuration. The replay RPC timeout is queue-owned and must be configured independently of the inline retry policy.

Configuration is loaded at startup; hot reload is not part of v1. Disabling the queue requires a configuration change and process restart. Shutdown does not promise to drain or recover retained
records.

`write_queue.enable = true` is blocked until all of the following are complete:

- The queue-specific dispatcher policy preserves first-round per-entry results and retained requests while bypassing inline retry for queue-owned attempts.
- Plan-time metadata is captured and propagated through request grouping, per-Region splitting, and queue admission.
- Replay performs the required cache invalidation, table-identity check, metadata reload, and refreshed-partitioner rebuild.
- Destination-scoped barrier release is validated against the Datanode's Region-removal and non-empty partition-expression-version checks.
- FanOut, record/invocation limits, retained-history accounting, transient memory limits, shutdown waiters, and failure semantics pass the validation plan below.
- `max_lineage_records`, the invocation cap, and transient rebuild budget have concrete implementation values selected and validated against the declared workload envelope; these internal values are not public configuration knobs.
- The transient estimator is validated against representative replay and FanOut shapes. If the rebuild path cannot enforce a conservative reservation before materialization, its peak remains explicitly unbounded and must not be described as hard-bounded.
- Maintainers accept the DDL residual risks, duplicate-capable retry semantics, and fail-closed barrier behavior.
- The client contract is reviewed, and failure-path/drain measurements are reported against the declared workload envelope.

The queue depends on three write-path improvements before enablement:

- **Bounded completion wait:** the caller waits only within one absolute budget that includes first-round result processing and queue admission.
- **Broader retryable-failure admission:** classification and retention cover eligible per-entry failures, retryable whole-RPC failures, and retryable single-entry-collapsed failures when the original per-Region request and plan context can be retained. A whole-RPC error without a recoverable request is not made queue-eligible merely because it is retryable.
- **Absolute time budget:** each admitted lineage has one retry deadline inherited by all FanOut successors; route lookup, rebuild, backoff, and leaderless deferral consume that same budget.

These enhancements are designed together as one retry contract, but may be implemented and merged independently. Queue enablement requires all three to be available and validated; none is claimed to be already implemented by this RFC. Durability and restart recovery remain a separate proposal and are not prerequisites for this in-memory queue.

## Risks and Limitations

- **Duplicate-capable retry.** A lost response, timeout, worker failure after submission, FanOut, or client retry can cause duplicate writes. Neither a retryable response nor a later successful
  invocation proves that an earlier unknown invocation did not apply.
- **Partial batch outcome.** A batch may return an error after some siblings have applied, or admitted records may apply after the client wait expires. A batch error does not mean that no rows were
  written.
- **No inline fallback.** An otherwise eligible attempt rejected by queue capacity or closure receives no Frontend retry after the first round. A client retry may duplicate append or Flow-related
  writes.
- **Process-local loss.** Restart, crash, shutdown, or deadline expiry can discard admitted work. The queue is not durable and provides no restart recovery.
- **Barrier head-of-line blocking.** An unresolved invocation addressed to a destination can retain its barrier indefinitely. Lineage-wide quiescence can also block one destination while a descendant
  is delayed in another Region.
- **Incomplete ordering scope.** FanOut-local queue scheduling does not order unrebuilt old-key records, live writes, other Regions, or other Frontends, and does not guarantee remote application order
  for unknown RPCs.
- **Resource accounting is not an RSS bound.** Retained and transient ledgers constrain their defined accounting units, not every Frontend allocation or total process memory.
- **DDL stale-replay window.** Metadata revalidation is not an atomic fence. `ALTER` can race between validation and RPC submission; `TRUNCATE` may not be distinguished by the captured table version;
  `DROP`/`UNDROP` incarnation behavior requires confirmation; Flow-source changes after planning are not re-read.
- **Hot-Region drain.** Per-Region serialization means global replay concurrency cannot shorten the tail of a single hot Region. Records near the end of a backlog may expire before their first
  authorization.

## Validation Plan

Validation should establish the behavior of the design rather than prescribe specific internal data structures or test harness APIs.

- **Dispatcher and ownership:** with the queue disabled, existing behavior is unchanged. With queue mode enabled, first-round writes still produce per-entry outcomes and retained per-Region requests,
  but transferred attempts do not enter the inline retry loop. Cover eligible retryable per-entry, whole-RPC, and single-entry-collapsed failures when their original per-Region request and plan snapshot are recoverable. Delete, BulkInsert, and logical-batcher flush paths remain inline. Rejected queue attempts never fall back to inline retry.
- **Plan snapshot and eligibility:** mutate table version, engine, append mode, TTL, Flow status, and partition-expression metadata after planning; verify that impure defaults on
  primary-key/time-index columns are rejected while non-key defaults do not cause unnecessary rejection; replay must compare against the captured plan snapshot. Verify admission rejects missing or ambiguous
  required metadata, unsupported engines, and an absent captured partition-expression version. Validate metric logical-row inserts separately from metric logical-batcher flushes.
- **Selected error and client contract:** verify terminal-error precedence, dispatcher-attempt-order selection, partial admission, wait-deadline behavior, `affected_rows`, cancellation, and the
  specified shutdown behavior for pending waiters.
- **Replay history:** verify that a later `applied` invocation does not erase an earlier `issue_unknown`, late responses do not rewrite terminal invocation outcomes, and record settlement labels do
  not independently release barriers.
- **Route refresh and FanOut:** start with stale table/route caches, move or repartition a table, and verify cache invalidation, table-ID identity validation, use of the refreshed partitioner, partial
  successor admission, and rejection without orphaned barrier/record/accounting state.
- **Destination ordering:** test a whole-RPC first-round failure to an old Region followed by repartition and FanOut to new destinations; verify the old Region's unknown result does not block a new
  destination only when Region removal or a non-empty partition-expression-version mismatch fences the old request. Conversely, a whole-RPC unknown addressed to the destination itself must keep that
  destination's barrier closed. Test multiple and nested barriers, intervening normal records, and unresolved replay invocations.
- **Resource and failure behavior:** force retained, transient, record, and invocation limits to exhaust. Verify history remains accounted while referenced by barriers, settled payload can be released
  safely, and reservations are not leaked or double charged. Compare estimator outputs with measured replay and FanOut peaks across representative shapes; verify that no allocation described as bounded
  occurs before its reservation. Inject worker failure before and after authorization, partial FanOut interruption, shutdown during replay, and queue-coordination failure.
- **Performance and workload envelope:** compare queue-disabled and queue-enabled failure paths under the same declared workload. Measure admission overhead, retained/transient memory, FanOut width,
  per-Region depth/skew, RPC latency distribution, time-to-first-authorization, fraction authorized before deadline, drain time, deadline expiry, and barrier blocking. Use the lower-bound model above to
  interpret results. These measurements validate or motivate tuning of the proposed settings; this RFC does not invent an additional numeric overhead threshold or require separate maintainer approval of each
  configuration value.

## Questions for Review

- Is the v1 engine boundary correct: Mito-backed physical Region writes only, with metric logical-row requests eligible only after normalization to supported physical targets?
- Is the destination-scoped first-round relevance rule sufficiently supported by the Datanode's Region-removal and partition-expression checks?
- Is the remaining DDL boundary acceptable for v1, particularly `TRUNCATE`, `DROP`/`UNDROP`, asynchronous metadata propagation, and the check-to-RPC race?
- Is lineage-wide quiescence an acceptable availability trade-off for destination barriers, given that a descendant in another Region can delay release?
- Are separate retained and transient memory ledgers acceptable when their limits are planned together against the Frontend memory budget, or is an aggregate queue-memory ceiling required?
- Is FanOut-local ordering sufficient, or must a destination also wait for queued old-key records from other lineages that may later rebuild into it?

## Alternatives and Future Work

A post-inline-retry buffer was rejected because it would introduce a second scheduler and retry budget for the same write. Inline fallback while the queue is enabled was rejected because it bypasses
per-Region ordering and splits retry ownership. A successor completion group is not included because the queue cannot provide an all-or-nothing result across a FanOut lineage.

Durable retention, restart recovery, region-side idempotency, deduplication, and a stronger DDL fence require additional protocol and storage work and are outside this RFC.
