---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
Status: Draft
---

## Summary

This RFC proposes an optional, Frontend-local in-memory retry queue for eligible ordinary, non-tracked direct inserts to partitioned, Mito-backed physical Regions on distributed Frontends. It owns the whole direct-insert call before the first write RPC; after the complete first-round report, it retains eligible failed Region requests and replays them only to their original Regions.

Each record has an absolute retry deadline and a separate budget for potentially submitted replay attempts. A bounded completion wait can preserve the existing client behavior by returning `Ok` when every admitted record is observed applied in time. The queue is disabled by default and is best-effort and process-local: it provides no durability, exactly-once, or at-least-once guarantee, and ambiguous outcomes remain duplicate-capable. Once queue ownership is selected, admission rejection is fail-closed with no inline fallback.

## Motivation

The existing `WriteRetry` in `src/operator/src/write_dispatcher.rs` retries eligible failures inline with a bounded count and jittered exponential backoff. Failed `RoutedWrite` values are retained only while that loop is active; after it gives up, no Frontend-local owner remains to retry them during a longer recovery window.

A Datanode restart can outlast the inline retry window. Several failure forms are also difficult to recover from today. A connection refusal may occur before submission, but error conversion can discard its precise transport cause. A peer-batched RPC may fail without per-entry results, and a single-entry failure is returned as a tonic error rather than a `PartialRegion` response. A queue limited to explicit per-entry failures would miss these cases.

The bounded completion wait avoids returning the first-round error immediately after background admission. Capture begins only after the complete first-round report, so a first-round RPC that never returns remains outside the queue's recovery guarantee.

The proposed five-minute `retry_budget` is a qualification starting point for a Datanode restart, including process cold start, Region reopening, and gRPC readiness; it is not a guarantee for arbitrary outages or every migration path. Region failover is disabled by default in the current MetaSrv options, so the budget must not assume that failover will promptly move a Region to another peer. Deployments that enable failover must qualify failure detection, route publication, and migration time separately and verify the required `H_min` before enabling the queue.

## Goals and Non-goals

### Goals

- Extend recovery beyond the inline retry window without introducing a second retry owner.
- Preserve request identity, peer-batch provenance, attribution, and deterministic client-visible results.
- Replay only to the original Region after validating current table and Region compatibility.
- Bound retained state, replay lifetime, potentially submitted attempts, and local replay work.
- Restrict ownership to the supported direct-insert path within one distributed Frontend runtime.

### Non-goals

- Durable storage, process-restart recovery, or guaranteed eventual delivery.
- Exactly-once, at-least-once, idempotent, or deduplicated write semantics.
- Datanode-side cancellation or a bound on work that a Datanode continues after the Frontend stops waiting.
- Cross-Region repartition recovery, request rebuilding, repartitioning, fan-out, successor records, or destination-region barriers.
- An atomic DDL fence, a `TRUNCATE` generation fence, a `DROP`/`UNDROP` generation fence, or a Flow-generation fence.
- Ordering between queued replay and concurrent live writes, legacy-inline writes, or writes from other Frontends.
- A queue-side deadline for the initial first-round RPC or streaming handoff of partial first-round results.
- Queue support for tracked/confirmed writes, Delete, BulkInsert, Flight `DoPut`, metric logical-batcher flushes, pending-row/table batcher flushes, or other producers that do not use the supported direct-insert path.
- A per-table allowlist, hot configuration reload, per-Region retained-capacity reservation, strict admission fairness, or a global token-bucket rate ceiling.
- Automatic restart of a failed queue coordinator.

## Request Scope and Retry Ownership

### Supported path

V1 applies only to ordinary, non-tracked direct inserts that dispatch to partitioned, Mito-backed physical Regions through the distributed Frontend direct-insert path.

Eligibility depends on the actual producer path and planned Region requests, not the external protocol or table name. SQL, gRPC, pipeline, COPY, and Prometheus requests are included only when their producer uses this direct-insert path and meets every eligibility rule.

The following remain on their existing paths:

| Path or target | V1 behavior |
|---|---|
| Ordinary non-tracked direct insert to eligible partitioned Mito physical Regions | Eligible when every Region attempt in the call passes preflight |
| Confirmed-write / tracked execution | Existing tracked path; queue is not used |
| Append-mode, instant-TTL, or Flow-source target | Whole call remains on its existing path |
| Mito physical Region with no partition-expression version | Whole call remains on its existing path |
| Sparse primary-key encoding | Whole call remains on its existing path |
| Metric logical-engine Regions, metric logical-batcher flushes, pending-row/table batchers | Existing path; not queue-owned |
| Delete, BulkInsert, Flight `DoPut`, and other non-direct insert operations | Existing path; not queue-owned |
| Standalone Frontend | Unsupported; startup validation rejects `write_queue.enable = true` |

Regions without a `partition_expr_version` are excluded intentionally. V1 does not invent a synthetic partition fence for unpartitioned Regions or Regions whose partition-expression version is unavailable.

V1 requires effective `Dense` primary-key encoding in both the planned Region options and the retained request hint; an absent hint means `Dense`. Sparse encoding is excluded because its missing-column synthesis semantics differ. Replay never rewrites the retained hint.

### Whole-call retry-owner selection

When queue mode is disabled, the producer skips queue-specific capture and estimation and keeps the existing path. When enabled, preflight runs after planning, materialization, and Region-request preparation, but before the first write RPC. If any Region attempt is ineligible, the whole call stays on its existing path; a no-leader or preparation failure before RPC entry is a call-level error and creates no record.

When every Region attempt is eligible, compute the request charge and atomically select the retry owner for the whole call. The selection succeeds only while the queue is open and healthy; if it is already closed or unhealthy, reject the call before the first write RPC rather than silently falling back to inline retry. Once selected, the queue is the sole retry owner and ownership cannot later switch to inline retry. Candidate publication is still per Region, so a later close, coordinator failure, or capacity rejection may reject a candidate without fallback. A stale-leader or transport failure after RPC entry is classified from its first-round provenance. This avoids mixing retry owners within one call.

Queue-owned calls work with `write_retry.max_retries = 0` because capture and replay are independent of `WriteRetry`. Preflight-ineligible calls keep the existing behavior and receive no legacy retry rounds when that setting is zero.

### Frontend ownership and producer boundary

A distributed Frontend owns one queue, coordinator, ledger, and shutdown lifecycle shared by all of its request-serving instances. A standalone Frontend is unsupported. The queue becomes the retry owner only for eligible ordinary, non-tracked direct inserts; tracked writes, bulk operations, pending-row/table batchers, and unrelated dispatcher users retain their existing paths.

Eligibility is decided from the planned request before the first write RPC. The queue captures the planning-time Flow decision and reuses already-materialized request values; it does not re-evaluate impure defaults or submit Flow mirrors during replay. A request with an existing Flow mirror remains ineligible.

## First-Round Capture and Admission

### Required capture path

The queue requires an explicit capture path independent of the legacy `WriteRetry` loop and independent of whether `WriteProgress` is present. This path must preserve the original materialized Region requests and their provenance; it must not reconstruct or serialize request payloads solely for replay.

Before peer grouping, assign each planned Region attempt a deterministic ordinal and retain its exact mapping to the peer-batch entry, original Region and peer, retry hint, typed submission disposition, plan-time compatibility metadata, and `WriteAttribution`. After the complete first-round report, create records only for eligible failed Region attempts. Successful entries remain observed applied and are never replayed. Missing or ambiguous request/result provenance makes the affected attempt unadmittable rather than guessed.

Preserve attribution independently of retry ownership. A successful Datanode acknowledgement updates only its successful Region rows. A queued failed Region retains its matching attribution and may update it once on an observed replay acknowledgement; first-round successes must not be counted again. A local attribution/accounting error after observed Datanode success remains a client-visible local error but must never turn the confirmed storage success into a replay candidate. `affected_rows` and progress remain acknowledgement observations, not exact logical-row or deduplicated-application totals.

If a retained progress handle owns an upstream reservation or acknowledgement observer, the queue must retain that ownership safely and account for its bounded memory. Shared progress allocations and reservations are charged once across records and released only after the last queue-owned reference is released.

### Partial results and Datanode compatibility

For queue-owned multi-entry insert RPCs, the Frontend must set `accept_partial_result` independently of legacy retry state. When supported by the Datanode, the response identifies failed entries and leaves successful entries observed as applied.

The Datanode returns a tonic error for a single-entry failure, even when partial results are requested. For multi-entry requests, it can return per-entry failures when `accept_partial_result` is enabled. The queue therefore:

- expands per-entry responses through the retained entry mapping;
- attributes a collapsed single-entry error to its sole Region request;
- expands a whole-peer-RPC error into `first_round_unknown` records only when every original request, mapping, and retry attribution is preserved; and
- marks an attempt `unadmittable` when request or error provenance is missing or ambiguous, rather than guessing.

Every target Datanode must support the per-entry partial-result contract before enablement. Mixed-version operation is unsupported: upgrade all targets before enabling the queue, and disable queue-enabled Frontends before downgrading any target. V1 does not negotiate this capability per peer.

### First-round outcome classification

| First-round outcome | Queue action |
|---|---|
| Observed Datanode success | Never replay; preserve the applied result |
| Datanode success followed by a local attribution/accounting error | Preserve the observed application and local error separately; do not create a queue record or replay the write |
| Per-entry retryable failure with exact request and sidecar | Admit as `observed_retryable_failure` |
| Collapsed single-entry retryable failure with exact request and sidecar | Admit as `first_round_unknown`; the original write may have applied |
| Other retryable whole-peer-RPC failure with exact split requests and batch mapping | Admit one `first_round_unknown` record per original Region split; an exact typed `not_submitted` marker is classified as `first_round_not_submitted` as in the following row |
| Confirmed `not_submitted` with exact request and provenance | Admit as `first_round_not_submitted`, even without a normal retry hint |
| Terminal error | Preserve the error; do not admit that Region attempt |
| Retryable-looking error with missing/ambiguous provenance | `unadmittable`; preserve the error and do not replay |

The table covers outcomes after RPC entry. A whole-peer-RPC or collapsed single-entry failure does not prove non-application and remains duplicate-capable. Queue records have exactly three first-round dispositions: `observed_retryable_failure`, `first_round_not_submitted`, or `first_round_unknown`; terminal and `unadmittable` outcomes create no record. A successful Datanode response remains observed applied even when local attribution/accounting fails: preserve the accounting error for client response selection, but never admit or replay that Region.

V1 waits for the complete first-round report before handing results to the queue; it neither streams partial results nor adds a timeout to the initial RPC. The queue does not define a separate cancellation protocol for in-flight first-round RPCs: if caller cancellation or task abort prevents a complete report from being produced, no partial report is handed to the queue, although a Datanode may already have applied an RPC. When a complete report is available, cancellation is checked at each candidate-publication boundary. A hung first-round RPC can therefore delay the whole call and prevent already-failed siblings from admission; `completion_wait` starts only after the report and admission decisions are complete.

Retryable siblings may still be admitted when another sibling is terminal or unadmittable. The caller receives an error, but admitted siblings remain queue-owned and may apply later.

### `not_submitted` classification

`not_submitted` is reserved for a typed transport disposition that positively establishes that the RPC was not submitted. Attach that disposition before error normalization can discard the evidence, and carry it with the original request and peer-batch provenance.

Generic `Unavailable`/`Unknown` statuses, error text, arbitrary transport errors, or a source chain inspected after normalization are not proof of non-submission. If the transport layer cannot provide the typed proof, retain the propagated retry hint or mark the attempt unadmittable; do not infer that the remote write did not happen. Confirmed non-submission does not consume the potentially-submitted attempt budget, but remains subject to the record deadline and backoff.

### Admission and capacity behavior

Admission is a per-Region ownership transition: publication transfers a candidate to the queue, which owns it until settlement or shutdown. Cancellation observed before publication prevents that candidate from being admitted; cancellation after publication only detaches the caller. The record, retained-memory charge, and completion registration must become visible together.

Admission is non-blocking and may reject a candidate because the queue is closed or unhealthy, provenance is invalid, or shared record/byte capacity is exhausted. Publication is per Region, not atomic for the whole call, and V1 does not reserve capacity for the whole call before its first RPC. A candidate rejected after queue ownership has been selected fails closed with no inline fallback; already-published siblings remain queue-owned.

If the retained charge cannot be computed safely or exceeds the entire configured byte limit, exclude the whole call before selecting queue ownership. Whole-call capacity reservation is deferred; the accepted possibility of partial admission must be covered by workload qualification.

## Client-Visible Contract

### Selected error

Assign each Region attempt its ordinal before peer grouping. Determine the selected first-round error from the complete first-round report and admission results, before entering completion wait:

1. If any terminal, unadmittable, admission-rejected, or cancelled-before-publication candidate exists, or a local attribution/accounting error is attached to an observed Datanode success, select the lowest-ordinal associated error. For a failed candidate, preserve its original first-round error; for an observed success with failed local attribution, preserve the local accounting error. Capacity/closed rejection and cancellation do not replace an original error with a synthetic queue error. A failed candidate that was not published must prevent `Ok`, even if every already-published sibling later applies.
2. Otherwise, if at least one candidate was admitted, select the original first-round error belonging to the lowest-ordinal admitted candidate. This is the fallback response if completion wait does not observe all records applied.
3. If the report contains no errors and no failed Region candidates, return the ordinary first-round success result.

Selection is deterministic and independent of peer grouping order or RPC completion timing. A later replay error never replaces the selected first-round error; replay outcomes remain in bounded record history and queue observability.

### Bounded completion wait

After the complete report and admission decisions:

- If the report contains a terminal or unadmittable outcome, a candidate rejected or cancelled before publication, or an observed Datanode success with a local attribution/accounting error, return the error selected by rule 1; already-published siblings remain queue-owned.
- If every failed candidate is admitted and observed applied, return `Ok` without waiting.
- Otherwise, wait up to `write_queue.completion_wait` for every admitted record to become applied. Return `Ok` only when all are applied before the deadline; return the fallback first-round error selected by rule 2 as soon as any record settles non-applied or the wait expires.
- Caller detachment or wait expiry does not cancel queue-owned records. `completion_wait = 0` takes an immediate snapshot and never blocks.

Register completion notification before a record becomes runnable so a completion racing with wait registration remains observable. After publication, caller detachment releases only the observer; it does not cancel the record. Cancellation before publication prevents admission, as defined in **Admission and capacity behavior**.

`affected_rows` is a response-time snapshot. It includes first-round successes and replay successes observed before the response is constructed. `WriteProgress` follows the same acknowledgement-based accounting. Neither value is an exact logical row total: an earlier `first_round_unknown` or replay `issue_unknown` may also have applied, and a duplicate-capable replay can acknowledge rows whose original application was not observed.

## Replay Model

### Record and ownership

One queue record represents one original Region request. It retains the original materialized request, Region identity, matching write attribution, immutable first-round disposition, bounded error/retry provenance, plan-time replay-policy and request-compatibility metadata, retained-charge estimate, admission time/deadline, attempt and backoff state, bounded replay history, and caller completion state while attached.

The first-round disposition never changes. Replay outcomes update only replay history and final settlement. History and diagnostics are bounded; raw or unbounded error messages are not retained. Caller detachment stops completion observation but does not release record-owned request or attribution state.

There is one FIFO per Region, ordered by successful admission publication. Only one record per Region is eligible to replay at a time. The FIFO head retains its Region position during backoff but releases its Frontend-wide replay slot. This FIFO orders queue records only; it does not serialize replay against concurrent live writes, legacy-inline retries, or other Frontends.

### Retry deadline and attempts

Each record has an absolute monotonic deadline:

`deadline = admission_time + write_queue.retry_budget`

The deadline covers backoff, route/catalog/leader/client preparation, and replay authorization. No RPC may be authorized at or after the deadline. Preparation that outlives the remaining budget is cancelled locally and settles from the history already observed. A replay authorized before the deadline may finish under its separate `replay_rpc_timeout`; settlement and cleanup may occur after the deadline, but no later invocation may be authorized. Expiry of one record must not cancel a shared refresh still needed by other records.

`write_queue.max_attempts` counts potentially submitted replay invocations, not the original first-round request. A confirmed typed `not_submitted` invocation does not consume this budget, but does consume deadline and backoff. Any other invocation authorized for RPC submission consumes one attempt. A timeout or worker failure after authorization without an observed result is unresolved (`issue_unknown`) and remains duplicate-capable.

The authorization boundary must be linearized immediately before invoking the RPC. Once authorization commits, the invocation is potentially submitted unless a typed proof of non-submission is observed; timeout or worker failure after that boundary is not evidence that the Datanode did no work. There must be no asynchronous gap between authorization and invoking the RPC. `replay_rpc_timeout` covers the complete authorized RPC operation.

### Connection-establishment timeout ordering

A typed connection failure can be classified as `not_submitted` only if it reaches the queue before the outer `replay_rpc_timeout` expires. When the queue is enabled, startup validation must use the effective `datanode.client.connect_timeout` and require it to be non-zero and `write_queue.replay_rpc_timeout >= datanode.client.connect_timeout + 1s`. Reject an invalid configuration at startup; do not silently relax the classification or fall back to the legacy retry owner. The one-second floor is a provisional minimum propagation margin, not a proven upper bound for scheduler delay; its adequacy must be demonstrated by the required failure tests. It does not cover arbitrary queueing behind other calls on the shared channel.

The repository example sets `datanode.client.connect_timeout = 10s`; therefore the provisional `replay_rpc_timeout` in this RFC is `15s`, not `10s`. Operators who change the effective Datanode client timeout must keep this relationship valid. If the outer RPC timeout wins before a typed connection disposition is observed, the invocation remains `issue_unknown` and consumes one potentially submitted attempt; the implementation must never infer `not_submitted` merely because the peer appears unreachable.

The shared per-peer lazy channel can serialize delivery of connection failures to queued calls. Qualification must therefore also exercise multiple replay invocations targeting the same peer at the maximum configured `replay_concurrency`, alongside the single-invocation blackhole case. Record which calls receive typed connection errors and which reach the outer timeout. Any timeout without typed proof remains `issue_unknown`; if that behavior exhausts the declared retry/workload envelope, enablement is blocked until the timeout/concurrency arrangement is corrected or the supported workload is explicitly narrowed. The timeout inequality alone is not evidence that arbitrary same-channel backlog is covered.

Queue replay reuses `write_retry.initial_backoff`, `write_retry.max_backoff`, and `write_retry.backoff_multiplier`, including the existing positive-jitter policy, but not the finite legacy retry iterator or `write_retry.max_retries`. The queue uses a saturating, deadline-bounded backoff whose base delay is capped at `max_backoff` before positive jitter; as in the existing policy, the actual wait may approach twice the configured cap. Exact `not_submitted` outcomes and pre-authorization deferrals continue to be scheduled while the record deadline remains.

The first replay preserves the first-round classification. `observed_retryable_failure` and `first_round_unknown` retain the original `RetryHint`; a route-refresh hint joins a coalesced route refresh after the initial backoff. `first_round_not_submitted` resolves the current route after backoff without forcing invalidation solely because the original connection was refused. In all cases replay resolves a peer/leader for the same Region and does not use the legacy request-rebuild path.

| Replay-processing outcome | Queue action |
|---|---|
| Observed success | Settle `applied`; stop replay for the record |
| Retryable hint | Consume one potentially submitted attempt; back off while both budgets remain |
| `RetryAfterRouteRefresh` | Consume one attempt; coalesce refresh work, then retry only the original Region if still valid |
| Non-retryable hint | Stop authorizing retries; settlement follows **Replay of unknown outcomes** |
| Confirmed typed `not_submitted` | Do not consume an attempt; retain the Region FIFO head and back off |
| No-leader or transient preparation failure before authorization | Defer with backoff; no invocation or attempt consumed |
| Timeout or worker failure after authorization without a result | Record unresolved `issue_unknown`; consume one potentially submitted attempt |

### Replay of unknown outcomes

An authorized invocation that returns no result before `replay_rpc_timeout` becomes `issue_unknown` and consumes one potentially submitted attempt. Another invocation may follow while both retry deadline and attempt budget remain.

Dropping the Frontend RPC future does not cancel work already accepted by a Datanode. A later replay may overlap with that remote work. Thus `replay_concurrency` bounds only local Frontend work, not Datanode operations. Qualification and canary tests must measure lingering ingest tasks, stalled requests, and write-buffer pressure under repeated timeouts; workloads that cannot accept this overlap must keep the queue disabled.

Each record has one final replay settlement, with earlier outcomes retained in bounded history. `give_up_reason` records why replay stopped; it is independent of the settlement and must not override it. Apply the following precedence:

1. `applied` if any replay response was observed applied. This does not prove that an earlier unresolved invocation did not also apply; no further replay is authorized after observed success.
2. `issue_unknown` if no replay success was observed and at least one potentially submitted invocation remains unresolved. This takes precedence over a later `stale_target` or observed error because neither proves that the unresolved invocation did not apply.
3. `stale_target` if refreshed metadata conclusively rejects the original target and no unresolved invocation or observed replay success takes precedence. Preserve `stale_target` as the `give_up_reason` even when the final settlement is `issue_unknown`.
4. `observed_failure` if a potentially submitted replay returned an error, no replay success was observed, no invocation remains unresolved, and replay stops without a confirmed stale-target result.
5. `replay_not_sent` if no potentially submitted replay invocation occurred and no confirmed stale-target result applies. Confirmed `not_submitted` invocations and pre-authorization deferrals do not change this settlement.

`replay_not_sent` describes only the replay phase. The original first-round disposition remains unchanged and is recorded separately. In particular, when the first-round disposition is `first_round_unknown`, any non-`applied` replay settlement describes only replay and does not establish that the logical write was not applied by the original RPC.

A worker failure before send authorization creates no invocation. A worker failure after authorization without an observed result marks that invocation unresolved. A per-record failure must not stop unrelated Regions. V1 does not automatically restart a failed record or coordinator.

### Same-Region replay only

Replay always targets the original `RegionId`. It may resolve a new peer or leader for that Region, but it never repartitions rows, creates successor records, rebuilds the request, or redirects it to another Region.

Before send authorization, replay must establish:

- the original table still exists with the same stable `TableId`;
- the original Region remains in a writable route and has the captured non-empty partition-expression version;
- the selected leader belongs to the same refreshed route snapshot;
- the target remains a supported Mito physical Region with append mode and instant-TTL disabled and Dense encoding;
- the retained request remains compatible with current column identity and synthesis semantics;
- the original non-Flow decision remains the one captured at plan time (replay does not perform a second Flow lookup or mirror pass).

A confirmed mismatch sets `give_up_reason = stale_target` and stops replay. Final settlement still follows **Replay of unknown outcomes**; an earlier unresolved invocation remains `issue_unknown`. Stale, unavailable, incomplete, or conflicting observations defer with backoff and never authorize a send.

Route and catalog refreshes may be coalesced across ready Region heads to avoid one refresh per record. They are observations, not an atomic cross-cache snapshot and not a substitute for per-record validation. Every record must independently validate the refreshed table identity, Region route, request compatibility, and selected leader before send authorization. Stale, unavailable, incomplete, or conflicting metadata defers the record; it never authorizes a speculative send.

Queue replay does not rebuild a partitioner, repartition rows, or redirect to another Region. A refreshed route is used only to locate the current peer/leader for the original `RegionId`. If the existing services cannot provide a sufficiently current view, defer the send.

The table metadata version is a freshness signal, not table identity and not an automatic stale condition. A version change triggers compatibility revalidation. A rename preserves identity when the stable table ID is unchanged; drop/create under the same name produces a different identity.

### Request compatibility

The plan-time sidecar captures stable table/column identity, the type and semantic properties relevant to retained values, nullability/default synthesis rules, and replay-policy fields (engine, append mode, instant-TTL, effective Dense encoding, and the original Flow decision). It must include a complete planning-time column-name-to-`ColumnId` mapping so drop/recreate under the same name can be distinguished from a genuinely new column.

If planning metadata lacks a complete column-ID mapping, the whole call is preflight-ineligible. Replay-time missing or ambiguous metadata is unverifiable and defers without sending.

For explicitly present request columns:

- the same name must still resolve to the captured `ColumnId`;
- logical type, datatype extension, and semantic type must remain compatible;
- a retained NULL requires the current column to remain nullable;
- a nullability change is compatible when the retained values contain no NULL;
- a default change does not affect a column whose value is explicitly retained in the request;
- rename, replacement, type change, datatype-extension change, or semantic-type change is incompatible. V1 performs no coercion or request reconstruction.

For omitted columns, distinguish columns that existed when the request was planned from columns added later:

- **Existed at planning:** synthesis semantics must remain unchanged. A changed literal default, added/removed default, newly impure default, or newly non-fillable `NOT NULL` column makes the record stale. An unchanged nullable/no-default or unchanged literal-default synthesis remains compatible.
- **Added after planning:** only a name that was absent from the complete planning-time name-to-`ColumnId` map is treated as a genuinely new column. It is compatible if Mito can synthesize it without an impure default: nullable with or without a literal default, or `NOT NULL` with a literal default. An impure default or `NOT NULL` without a default is incompatible.
- **Dropped and re-added under the same name:** this is not a genuinely new post-planning column. The name existed in the captured map but now resolves to a different `ColumnId`, so the compatibility check returns `stale_target`, including when the replacement column would otherwise be nullable or have a literal default. Further replay stops; final record settlement follows **Replay of unknown outcomes**.

This is a request-specific compatibility check, not a general schema-diff framework. A table metadata version change or `ADD COLUMN` is not automatically stale when Mito can safely synthesize the omitted value.

## Resource Limits and Scheduling

### Frontend-wide limits

The queue has one ledger and coordinator per distributed Frontend, shared by all `Instance` references and clones. Its limits are:

- `write_queue.max_bytes`: estimated charge for all queue-retained request, attribution/progress, and bounded metadata state;
- `write_queue.max_records`: admitted records, including completion-only records whose payload has been released;
- `write_queue.replay_concurrency`: active local replay work, including metadata/route/leader/client preparation and RPC execution.

The queue retains the original materialized request; it does not create an encoded or replay-side payload copy. The estimator must conservatively account for queue-owned allocations using allocated capacities, including request data, compatibility metadata, attribution, shared progress reservations, bounded diagnostics, and caller-completion state. If an owned allocation cannot be measured safely, the call is preflight-ineligible. Shared allocations and reservations are charged once and released only after their last queue-owned reference is released.

`max_bytes` is an admission-accounting bound, not a process RSS cap. Payload/attribution and completion-state charges have separate release points: settlement releases the payload/attribution charge, while completion state remains charged until the caller detaches. `max_records` likewise includes attached completion-only records. Every terminal/rejection path must release each applicable charge exactly once. No dead-letter payload archive or query API is provided in v1.

When queue mode is enabled, estimation is part of the successful producer path even when the first-round write succeeds; this cost must be measured against a queue-disabled baseline. The estimator must not perform route/catalog I/O or create a payload copy solely for accounting. Temporary reservations and published records share the same ledger so concurrent admissions cannot overcommit configured limits.

### Scheduling and fairness

The scheduler runs ready Region FIFO heads with round-robin selection among runnable Regions. A head retains its Region FIFO position during backoff but releases its Frontend-wide replay slot. Preparation consumes a slot and is bounded by the remaining record deadline. V1 does not promise bounded drain time or strict fairness: a hot Region is serialized, a prolonged no-leader backlog can consume shared capacity, and slow preparation can occupy every replay slot.

The supported backlog, hot-Region depth, recovery drain objective, first-authorization latency, expiry fraction, and `H_min` must be qualified under **Performance and workload qualification**. Refresh coalescing reduces repeated metadata work but does not add per-Region replay parallelism. Further scheduling, reservation, or rate-control mechanisms require separate evidence and review.

### Performance and workload qualification

RFC review does not require running performance or RSS tests. Before canary enablement, the implementation must pass a reproducible qualification against a queue-disabled baseline using the same release build, hardware/container limits, Datanode topology, data, schema, and workload. Test normal and peak eligible-ingest rates, representative production request shapes, capacity/fan-out stress, and failure cases covering Datanode restart, fast connection refusal, an unanswered/blackholed peer reaching `datanode.client.connect_timeout`, same-peer concurrent replays through the shared lazy channel, no-leader/route refresh, Datanode rejection, write-buffer stalls, and repeated local timeouts with remote work still pending. Exercise both hot-Region and distributed-Region workloads.

The qualification must measure: (1) successful-path CPU/row, throughput, and p50/p95/p99 latency, including estimator cost; (2) estimated retained charge versus an independent retained-object capacity measurement, Frontend RSS at baseline/peak backlog/post-drain, and exact-once charge release; (3) Datanode RSS, write-buffer pressure, lingering remote work, confirmed `not_submitted` connection-attempt rates, and the fraction of same-peer calls that time out before a typed connection result; (4) first-authorization latency, pre-authorization expiry, admission rejection, backlog/drain behavior, per-Region wait, and replay-slot utilization; and (5) correctness of request mapping, acknowledgements, attribution, and documented duplicate-capable outcomes.

The deployment owner must predeclare numeric workload-envelope values from production traces or representative load tests: normal/peak requests and rows per second; request size/row distribution; active Region count, peer fan-out, maximum simultaneous replay candidates targeting one Datanode peer (bounded by `replay_concurrency` unless the implementation imposes a lower limit), and hot-Region depth; supported restart/no-leader/stall duration and failure fraction; maximum backlog and acceptable rejection; drain and first-authorization objectives; pre-authorization expiry limit; Frontend RSS headroom and Datanode pressure/lingering-work budgets; aggregate confirmed connection-attempt-rate budgets; and successful-path overhead budgets.

Derive `H_min` from the recovery objective and measured p99 replay duration. It must cover the required replay timeout(s), configured backoffs including positive jitter, route/catalog preparation, settlement, and a safety margin. State explicitly when the objective requires only one replay opportunity; do not imply that the full `max_attempts` budget is guaranteed. The envelope must be consistent with `retry_budget`, `max_attempts`, backoff, and `replay_concurrency`.

Qualification passes only when all predeclared budgets are met; the independent retained-object measurement does not exceed the estimator; concurrent admission does not overcommit `max_bytes`/`max_records`; all charges release exactly once; Queue live charges return to zero after drain; Frontend/Datanode memory and lingering work remain within budget; and failure injection preserves the specified outcome and attribution semantics. RSS need not return byte-for-byte to baseline because allocators may retain pages, but it must reach the approved stable post-drain plateau.

If any criterion fails, keep the feature disabled. Fix accounting/release bugs, reduce limits or the declared workload envelope, adjust backoff/recovery objectives, or optimize estimator overhead without weakening accounting. Do not add a new scheduler, per-Region reservation, coalescing layer, or token bucket solely to pass a benchmark; such mechanisms require measured justification and a separate design decision. Record the approved envelope, configuration, measurements, failure results, and maintainer acceptance in the implementation PR or a linked qualification report. Missing values or unreviewed failures block rollout.

### Queue failure and shutdown

One shared `Arc`-backed Queue is held by the Frontend `Instance` and accessed by request-serving clones. The owning distributed Frontend drives Queue startup and shutdown; dropping any `Instance` clone must not close it. Start the coordinator before protocol servers accept requests. If startup fails, roll back any partially started servers, the coordinator, and the heartbeat task.

During shutdown, stop accepting protocol requests, close queue admission and replay authorization, then drain already-authorized work. Queue cleanup must run even when server shutdown reports an error. A replay is either authorized before closure or rejected after it. Already-authorized RPCs may finish within `replay_rpc_timeout`; local timeout or Frontend shutdown does not cancel work already accepted by a Datanode.

The current server shutdown signal does not guarantee that every accepted handler has exited before the queue closes. A first-round report arriving after closure is therefore rejected without inline fallback even though its original RPC may have applied. V1 accepts this handoff-loss window and does not add an in-flight handoff barrier. A coordinator failure closes the queue and stops new admission/authorization; the coordinator is not automatically restarted, while an individual record failure must remain isolated.

## DDL, Flow, and Ordering Limitations

Queued retries may execute after the client has received an error, introducing delayed writes beyond the lifetime of the inline retry loop.

| Change or concurrent operation | V1 behavior and accepted risk |
|---|---|
| Concurrent live write to the same Region/key | Queue FIFO orders only queued records. A later live write may complete before an earlier queued replay; final conflict behavior depends on storage semantics. |
| `TRUNCATE` after the first-round error | No data-generation fence exists. Delayed replay may reinsert rows after truncation. |
| `DROP` then create under the same name | New table has a different `TableId`; replay is rejected. |
| `UNDROP` | V1 has no drop-generation fence. Because `UNDROP` can restore the original table identity, a pre-drop record may pass identity checks after restoration and replay into the restored table. |
| Repartition or partition-expression change | The original Region/partition identity is no longer valid; stop replay with `give_up_reason = stale_target`, do not redirect or rebuild. Final settlement follows **Replay of unknown outcomes** if an earlier invocation remains unresolved. |
| `ALTER` racing between validation and send | There is no atomic DDL fence. Request-specific metadata validation reduces but cannot eliminate this race. |
| Flow exists at planning time | Flow mirror work is submitted independently before Region dispatch; mirror completion is not part of the Region write result. A later queue replay cannot determine whether the original mirror applied and must not submit it again, so Flow-source requests are preflight-ineligible. |
| Flow is created/changed after a non-Flow decision was captured | Replay uses the captured non-Flow decision and does not re-read Flow membership; a delayed source write is therefore not mirrored under the new definition. V1 has no Flow-generation fence. |

Workloads requiring DDL fencing, same-key ordering across delayed replay and live writes, or Flow changes to cover delayed writes must use a queue-disabled Frontend pool. V1 has no per-table allowlist or hot reload; enabling the queue affects every eligible direct insert on that Frontend, so isolate the pool operationally.

## Observability

Expose Frontend-scope metrics with bounded labels and non-overlapping accounting:

- preflight exclusions by bounded reason;
- admission candidates accepted/rejected by bounded reason;
- first-round outcomes by bounded classification: applied, `observed_retryable_failure`, `first_round_not_submitted`, `first_round_unknown`, terminal failure, and unadmittable; only the three named dispositions create records;
- replay invocations by outcome: applied, retryable error, terminal error, `issue_unknown`, and confirmed `not_submitted`;
- final record settlements: `applied`, `issue_unknown`, `observed_failure`, `stale_target`, and `replay_not_sent`;
- terminal `give_up_reason` counts by bounded reason, kept separate from final settlement (for example, a `stale_target` stop can end with `issue_unknown`);
- retained estimated bytes, retained record entries, and active replay-slot utilization;
- time from admission to first authorization, pre-authorization deadline expiry, and backlog drain observations;
- replay-observed rows, explicitly documented as observations rather than exact logical rows.

Emit one bounded structured event for each non-applied settlement, including table/Region identity, first-round provenance, final settlement, give-up reason, and potentially submitted attempt count. Keep IDs, request payloads, and unbounded error text out of metric labels.

Pair Queue metrics with process-level Frontend/Datanode RSS and the approved workload envelope. Alert on sustained admission rejection, capacity saturation, hot-Region backlog, slow first-authorization latency, deadline expiry, stale-target stops, replay timeouts, `issue_unknown` growth, and confirmed connection failures per second. Because `not_submitted` does not consume `max_attempts`, deadline and backoff do not establish an aggregate connection-attempt-rate ceiling.

## Configuration and Rollout

The following are provisional starting values. They are not production defaults until measured against the supported workload envelope.

| Parameter | Scope | Initial value | Meaning |
|---|---|---:|---|
| `write_queue.enable` | Per Frontend | `false` | Opt-in for the supported direct-insert path |
| `write_queue.max_bytes` | Per Frontend | `512MiB` | Estimated retained charge |
| `write_queue.max_records` | Per Frontend | `100000` | Retained entries, including completion-only entries |
| `write_queue.retry_budget` | Per record | `5m` | Absolute lifetime from admission |
| `write_queue.max_attempts` | Per record | `3` | Potentially submitted replay attempts; excludes exact `not_submitted` |
| `write_queue.completion_wait` | Per call | `30s` | Caller wait after report and admission |
| `write_queue.replay_rpc_timeout` | Per invocation | `15s` | Local timeout for an authorized replay RPC; at least 1s greater than the effective Datanode client connect timeout |
| `write_queue.replay_concurrency` | Per Frontend | `8` | Active local replay work |
| `write_retry.initial_backoff` | Shared backoff | Existing setting | Queue base backoff |
| `write_retry.max_backoff` | Shared backoff | Existing setting | Base-delay cap before positive jitter |
| `write_retry.backoff_multiplier` | Shared backoff | Existing setting | Exponential multiplier |
| `write_retry.max_retries` | Legacy path only | `3` | Does not control queue-owned records |

Queue configuration is loaded at startup. When enabled, required limits and `datanode.client.connect_timeout` must be valid and non-zero; startup rejects `replay_rpc_timeout < datanode.client.connect_timeout + 1s`. `completion_wait = 0` is allowed. The provisional `15s` replay timeout assumes the example `10s` Datanode connect timeout; deployments must validate against their effective value. Changing the enablement switch requires a Frontend restart, which may discard pending records.

### Enablement gates

Keep the queue disabled until all of the following pass:

- Capture tests preserve original requests, deterministic peer-batch mapping, retry provenance, typed submission disposition, and write attribution independently of legacy retry settings.
- Outcome tests cover per-entry, collapsed single-entry, whole-peer-RPC, mixed-batch, missing-provenance, partial-admission, and all-Datanode compatibility cases.
- Client-contract and lifecycle tests cover deterministic error selection, completion wait, cancellation/publication, startup rollback, shutdown, late first-round reports, authorized replay drain, and coordinator/worker failure.
- Same-Region and schema-compatibility tests cover route refresh, explicit/omitted columns, `ColumnId` replacement, DDL limitations, and Flow exclusion.
- Resource and failure tests verify no ledger overcommit or double release, deadline behavior, hot/leaderless Regions, saturation, fast connection refusal, a blackholed/unanswered peer that reaches `connect_timeout`, same-peer concurrent replay on the shared lazy channel at maximum configured concurrency, Datanode stalls, local timeout, and lingering remote work. Startup-validation tests reject invalid `replay_rpc_timeout` / `connect_timeout` combinations. The blackhole test must prove that a typed connection failure arrives before the outer replay timeout for the single-invocation case; concurrent cases must preserve `issue_unknown` whenever the outer timeout wins without typed proof.
- Performance, RSS, Datanode-pressure, connection-attempt-rate, and workload-envelope qualification passes with predeclared and approved numeric budgets. An unfilled envelope or unapproved threshold blocks enablement.

### Rollout

- Upgrade every target Datanode before enabling the queue; do not run mixed-version operation.
- Start with a dedicated canary Frontend pool whose workloads accept the documented duplicate, delayed-replay, DDL, Flow, and ordering risks.
- Apply the approved workload-envelope values as canary alert and rollback thresholds. Monitor admission rejection, retained bytes/records, time to first authorization, deadline expiry, confirmed connection failures, `issue_unknown`, hot-Region backlog, replay-slot utilization, and Frontend/Datanode RSS.
- On any budget breach, stop sending traffic to the canary pool and restart it with the queue disabled before returning traffic. This may discard pending records and does not cancel work already accepted by Datanodes.
- Changing the shipped default requires a separate maintainer decision based on integration tests, failure-injection results, workload measurements, canary evidence, and explicit acceptance of residual risks.

## Implementation Plan

Implement the queue-specific capture path in the eligible ordinary direct-insert pipeline, followed by the Frontend-owned bounded ledger/coordinator, same-Region replay, request-compatibility validation, and lifecycle integration. Keep existing retry behavior unchanged for ineligible producers. The implementation PR should document concrete source touchpoints and demonstrate every **Enablement gates** criterion; the current PR remains documentation-only.

## Alternatives

- **Append a queue after existing inline retries:** rejected because it creates two retry owners, extends the total retry window unpredictably, and complicates request ownership and duplicate handling.
- **Limit admission to explicit `PartialRegion` responses:** rejected because whole-peer-RPC and single-entry failures include important restart and single-Region cases.
- **Rebuild requests against current partitioning:** rejected for v1 because cross-Region repartition recovery requires new row routing, successor ordering, and additional ownership rules. V1 replays only to the original Region.
- **Durable queue:** deferred to a separate design. It changes acknowledgement and recovery semantics and is not required for the in-memory v1 goal.
- **Inline fallback after queue admission rejection:** not included in v1 because it introduces a second retry owner after the queue has been selected. Capacity rejection is instead an explicit accepted limitation and rollout gate.

