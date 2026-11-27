---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
Status: Draft
---

## Summary

This RFC proposes an optional, Frontend-local in-memory retry queue for eligible ordinary, non-tracked direct inserts to partitioned, Mito-backed physical Regions on distributed Frontends.

The queue is selected as the sole retry owner for the whole direct-insert call before its first write RPC; it is not an additional retry stage after `WriteRetry`. After the complete first-round report is available, it retains eligible failed Region requests and replays them only to their original Regions.

Each record has an absolute retry deadline and a separate budget for potentially submitted replay attempts. A bounded completion wait preserves the existing client behavior: a transient failure can still return `Ok` when replay success is observed in time.

The queue can capture per-entry failures, collapsed single-entry errors, and attributable whole-peer-RPC failures, but only when the original requests and batch mapping are preserved. A confirmed connection failure that proves the unary request was not submitted consumes deadline and backoff, but not the potentially submitted attempt budget.

The queue is disabled by default and is best-effort and process-local. It provides neither durable retention nor exactly-once or at-least-once delivery; ambiguous RPC outcomes remain duplicate-capable. Admission rejection after queue ownership is selected is fail-closed and receives no inline fallback. This can provide fewer retry opportunities than the existing inline path under capacity pressure and is an accepted v1 limitation.

## Motivation

The existing `WriteRetry` in `src/operator/src/write_dispatcher.rs` retries eligible failures inline with a bounded count and jittered exponential backoff. Failed `RoutedWrite` values are retained only while that loop is active; after it gives up, no Frontend-local owner remains to retry them during a longer recovery window.

A Datanode restart can outlast the inline retry window. Several failure forms are also difficult to recover from today. A connection refusal may occur before submission, but error conversion can discard its precise transport cause. A peer-batched RPC may fail without per-entry results, and a single-entry failure is returned as a tonic error rather than a `PartialRegion` response. A queue limited to explicit per-entry failures would miss these cases.

The queue also needs a bounded completion wait. Returning the first-round error immediately after background admission would regress the existing contract, under which an inline retry can turn a transient failure into `Ok`.

Queue ownership begins only after the complete first-round report is available. A first-round RPC that never returns therefore remains outside the queue's recovery guarantee, and ambiguous outcomes remain duplicate-capable.

## Goals and Non-goals

### Goals

- Extend the recovery window for eligible transient insert failures beyond the current inline retry window.
- Select one retry owner for the whole direct-insert call before the first write RPC.
- Preserve original Region requests, peer-batch mapping, result provenance, and deterministic error selection.
- Capture retryable per-entry, collapsed single-entry, and attributable whole-peer-RPC failures.
- Return `Ok` when every admitted Region record is observed applied within the bounded completion wait.
- Keep each replay bound to its original Region and validate that the retained request remains compatible with the current table and Region metadata.
- Bound retained queue state, record lifetime, potentially submitted replay attempts, and local active replay work.
- Keep queue ownership and lifecycle inside one distributed Frontend runtime and opt the queue into only the supported producer path.

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

V1 applies only to ordinary, non-tracked direct inserts that reach the `Inserter::do_request` path and dispatch writes to partitioned, Mito-backed physical Regions through the Frontend `WriteDispatcher`.

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
| Standalone Frontend | Queue unsupported |

Regions without a `partition_expr_version` are excluded intentionally. `partition/src/cache.rs` represents an empty partition expression as `None`, including for unpartitioned Regions, and Mito does not validate a missing version. V1 does not invent a synthetic fence.

V1 requires effective `Dense` primary-key encoding in both the planned Region options and the retained request hint; an absent hint means `Dense`. Sparse encoding is excluded because its missing-column synthesis semantics differ. Replay never rewrites the retained hint.

### Whole-call retry-owner selection

The producer performs queue preflight after the original insert has been planned and materialized, and after the target Region requests have been prepared, but before the first write RPC.

- If every Region attempt is eligible and queue mode is enabled for this supported producer call, the whole call selects the queue as its retry owner.
- If any Region attempt is ineligible, the whole call remains on the existing path. V1 does not mix queue ownership and legacy inline ownership within one call.
- Once queue ownership is selected, admission rejection is fail-closed. The queue does not fall back to inline retry for a rejected candidate.

Preflight follows route/leader preparation. A no-leader or preparation failure before the first write RPC is a call-level error and creates no queue record. A stale-leader or transport failure after RPC entry is a first-round outcome and may be admitted when its provenance and retry classification permit it.

Queue-owned calls work with `write_retry.max_retries = 0` because capture and replay are independent of `WriteRetry`. Preflight-ineligible calls keep the existing behavior and receive no legacy retry rounds when that setting is zero.

### Explicit producer integration

The distributed Frontend `Instance` owns one queue, constructed by `FrontendBuilder`. Inject it only into the supported ordinary `inserter`; do not add it to shared `WriteDispatcher` configuration.

`FrontendBuilder::try_build` creates the ordinary `inserter` and execution-only `bulk_inserter` through a shared closure. Keep the `bulk_inserter` queue-free: inject the queue after separating the ordinary producer path. It must not reach pending-row/table-batcher flushes, Delete, BulkInsert, tracked writes, or other shared-dispatcher users. Carry the eligibility decision in the supported direct-insert execution context.

Capture Flow eligibility from the same `FlowMirrorTask` construction that creates and detaches mirror work before Region dispatch. Carry an immutable `has_mirror_requests` (or equivalent) result; do not query Flow membership again during replay. Impure defaults are materialized before dispatch, and replay reuses the retained request without re-evaluation.

## First-Round Capture and Admission

### Required capture path

The current dispatcher does not provide the capture behavior required by the queue in all configurations:

- `execute_with_progress` selects `send_batch` only when legacy retry is present or progress tracking is enabled.
- The direct `send` path does not retain a failed request when `retry` is `None`.
- The `send_batch` path with `retry == None` requests per-entry partial results, but its failure results do not retain the original per-Region requests for replay.
- The current `WriteRetry::should_retry` treats a terminal sibling as a reason not to retry the whole attempt set.

V1 therefore requires a queue-specific first-round capture path that is explicitly selected by the supported producer and works independently of both `WriteRetry` and `WriteProgress`. It must not change the retry behavior of unrelated dispatch callers.

Before peer grouping, assign each planned Region attempt a deterministic ordinal and retain a lightweight sidecar containing:

- the ordinal, target `RegionId`, and original peer-batch membership;
- the exact mapping from each peer-batch entry back to its Region attempt;
- the original retry hint/error and structured submission disposition for each observed result;
- the plan-time table, Region, partition, Flow, and request-compatibility metadata;
- the precomputed retained-charge estimate.

Keep shared ownership of the original peer-batch request through the RPC; do not build a second payload copy. On failure, use the captured entry mapping to isolate only failed entries and create `RoutedWrite` values holding their original materialized `Arc<RegionRequest>`. Successful entries do not become queue records. The implementation may use the envelope type already present in `send_batch`, but must not encode/decode or reconstruct payloads.

`WriteDispatcher::prepare` groups Region requests by peer using a `HashMap`, so iteration order is unstable. Assign Region ordinals and preserve entry mapping before grouping; error selection must not depend on map order or RPC completion timing.

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
| Observed success | Never replay |
| Per-entry retryable failure with exact request and sidecar | Admit as an independently retryable Region record |
| Collapsed single-entry retryable failure with exact request and sidecar | Admit as `first_round_unknown`; the original write may have applied |
| Whole-peer-RPC retryable failure with exact split requests and batch mapping | Admit one `first_round_unknown` record for each original Region split |
| Confirmed `not_submitted` with exact request and provenance | Admit as `first_round_not_submitted`, even without a normal retry hint |
| Terminal error | Preserve the error; do not admit that Region attempt |
| Retryable-looking error with missing/ambiguous provenance | `unadmittable`; preserve the error and do not replay |

The table covers outcomes after the first write RPC is entered. A no-leader or preparation failure before RPC entry is a call-level pre-dispatch error and creates no queue record; see **Whole-call retry-owner selection**. A whole-peer-RPC or collapsed single-entry failure does not prove that the Datanode applied nothing, so replay remains duplicate-capable.

The dispatcher waits for all peer-batch futures before producing the first-round report. V1 keeps this handoff boundary: it neither streams partial results into the queue nor adds a timeout to the initial RPC. A hung first-round RPC can therefore delay the whole call and prevent already-failed siblings from admission; `completion_wait` starts only after the report and admission decisions are complete.

Retryable siblings may still be admitted when another sibling is terminal or unadmittable. The caller receives an error, but admitted siblings remain queue-owned and may apply later.

### `not_submitted` classification

Datanode channels are lazy: `client.ready()` may succeed before a connection is established. A connection refusal can instead surface from `unary()` as `Unavailable` with the exact `tonic::ConnectError` in its raw source chain.

Attach the typed `not_submitted` disposition at that `unary()` error boundary, before `map_region_error` normalizes the status and drops the source detail. Carry it as structured metadata with the original Region request and peer-batch provenance.

Do not classify a request as `not_submitted` based on:

- a `ready()` error;
- generic `Unavailable` or `Unknown` status;
- error text or message matching;
- an arbitrary transport error;
- a source chain inspected later, after error normalization.

Only that exact connection-error type at the unary boundary qualifies. If the marker is unavailable or the client behavior changes, fall back to the propagated retry hint or mark the attempt unadmittable. Pin the implementation and test to the repository's resolved Tonic version.

### Admission and capacity behavior

Admission is the ownership boundary: publication transfers a candidate to the queue, which owns it until settlement or shutdown.

The caller's cancellation token is checked under the QueueState lock immediately before each candidate is published, with no `await` between the check and commit. This is the candidate's publication linearization point: cancellation observed at the check prevents publication; cancellation racing after the check loses to publication.

The record, retained charge, and completion-observer registration are committed together. Publication is per Region candidate, not atomic for the whole call. Already-published records remain queue-owned; cancellation only detaches the caller and prevents publication of later candidates.

Admission does not wait for capacity, network I/O, metadata lookup, or worker scheduling. It may reject a candidate because the queue is closed or unhealthy, its sidecar is invalid, or shared record/byte capacity is exhausted. Only errors from explicitly fallible reservation/allocation APIs are surfaced; v1 does not catch process-wide OOM.

**V1 does not reserve capacity for the whole call before its first RPC.** A candidate that fits individually may still be rejected after queue ownership is selected because other records consumed the shared budget. Rejection is fail-closed, with no inline fallback, and can provide fewer retry opportunities than the existing inline path under saturation.

This is an accepted v1 limitation; opt-in rollout and workload qualification must establish that the configured envelope fits. Whole-call reservation is a separate design requirement if maintainers require no capacity-pressure regression.

If a candidate's charge cannot be computed safely or exceeds the entire `max_bytes` limit, preflight excludes the whole call before queue ownership is selected.

## Client-Visible Contract

### Selected error

Assign each Region attempt its ordinal before peer grouping. Determine the selected first-round error from the complete first-round report and admission results, before entering completion wait:

1. If any terminal, unadmittable, or admission-rejected candidate exists, select the original first-round error belonging to the lowest-ordinal such candidate. Capacity/closed rejection does not replace it with a synthetic queue error; the rejection reason is recorded separately.
2. Otherwise, if at least one candidate was admitted, select the original first-round error belonging to the lowest-ordinal admitted candidate. This is the fallback response if completion wait does not observe all records applied.
3. If there are no failed candidates, return the ordinary first-round success result.

Selection is independent of peer `HashMap` order and RPC completion timing. A later replay error never replaces the selected first-round error; replay outcomes remain in bounded record history and queue observability.

### Bounded completion wait

After the complete first-round report and all admission decisions are available:

- If any first-round outcome is terminal or unadmittable, or any candidate is rejected, return the selected first-round error. Admitted siblings continue independently.
- If every failed candidate is admitted and already observed applied, return `Ok` without waiting.
- Otherwise, wait up to `write_queue.completion_wait` for all admitted records to become applied. Return `Ok` only if every record is observed applied before the deadline; otherwise return the selected first-round error.
- A final non-applied settlement or wait expiry does not cancel other queue-owned records. `completion_wait = 0` takes an immediate completion snapshot and never blocks.

Register completion notification before a record becomes runnable so a completion racing with wait registration remains observable. After publication, caller detachment releases only the observer; it does not cancel the record. Cancellation before publication prevents admission, as defined in **Admission and capacity behavior**.

`affected_rows` is a response-time snapshot. It includes first-round successes and replay successes observed before the response is constructed. It is not an exact logical row total: an earlier `first_round_unknown` or replay `issue_unknown` may also have applied.

## Replay Model

### Record and ownership

One queue record represents one original Region request. It retains:

- the original materialized `Arc<RegionRequest>`;
- the original peer and Region identity;
- immutable first-round disposition and bounded error/retry provenance;
- plan-time replay-policy and request-compatibility snapshots;
- retained-charge estimate;
- admission time and absolute retry deadline;
- potentially-submitted replay-attempt count;
- backoff state and a bounded potentially-submitted replay-attempt history, with at most `max_attempts` entries;
- bounded diagnostics for confirmed replay `not_submitted` outcomes (a saturating per-record count and the last bounded error class, not an unbounded per-invocation list);
- caller completion state while the caller remains attached.

The first-round disposition never changes. Replay outcomes update only replay history and settlement. Both the potentially submitted history and `not_submitted` diagnostics are bounded; raw or unbounded error messages are not retained.

There is one FIFO per Region. Records are ordered by successful admission-publication sequence, not client arrival, original dispatch-start time, or SQL row order. Only one record per Region is eligible to replay at a time. The FIFO head retains its Region reservation across backoff but does not occupy a Frontend-wide replay slot while sleeping.

A FIFO covers admitted queue records only. It does not serialize replay against concurrent live writes, legacy-inline retries, or other Frontends.

### Retry deadline and attempts

Each record has an absolute monotonic deadline:

`deadline = admission_time + write_queue.retry_budget`

The deadline includes backoff, route/catalog preparation, leader resolution, and replay RPC execution. A record that reaches its deadline before send authorization is settled without another RPC.

`write_queue.max_attempts` counts potentially submitted replay invocations, not the original first-round request. Confirmed `not_submitted` invocations increment the bounded diagnostics in **Record and ownership**, but do not enter the attempt history or consume this budget; they still consume deadline and backoff. Every other authorized invocation consumes one attempt. If it ends without an observed result, it is recorded as unresolved (`issue_unknown`) and remains duplicate-capable.

Queue replay reuses `write_retry.initial_backoff`, `write_retry.max_backoff`, and `write_retry.backoff_multiplier`, including positive jitter. `write_retry.max_retries` does not apply to queue-owned records. The maximum backoff setting caps the base delay before jitter, so the actual wait may approach twice the configured cap.

The table covers both pre-authorization outcomes and replay invocations. Classify RPC errors using the final propagated `RetryHint`; the queue's deadline and attempt budget do not make a non-retryable error retryable:

| Replay-processing outcome | Queue action |
|---|---|
| Observed success | Settle `applied` and release the Region FIFO head |
| Retryable hint without route refresh | Consume one potentially submitted attempt; back off and retry while both budgets remain |
| `RetryAfterRouteRefresh` | Consume one potentially submitted attempt; join a coalesced refresh cycle, refresh routing for the original Region, then retry only that same Region if still valid and budgets remain |
| Non-retryable hint | Stop authorizing retries; final settlement follows **Replay of unknown outcomes**, where an earlier unresolved invocation keeps the result `issue_unknown` |
| Exact typed replay `not_submitted` | Record bounded diagnostics; consume deadline/backoff but not `max_attempts` or the potentially-submitted attempt history |
| No-leader or transient route/catalog preparation failure before authorization | Defer with backoff; no invocation and no attempt consumed |
| Timeout or worker failure after authorization without an observed result | Mark the invocation unresolved (`issue_unknown`); consume one potentially submitted attempt and permit another invocation only while both budgets remain |

Use the final hint from the existing error-classification path, never status text. Pre-authorization deferrals and confirmed `not_submitted` outcomes release the active replay slot during backoff but retain the Region FIFO head. Route refresh never changes the target Region; see **Same-Region replay only**.

### Replay of unknown outcomes

An authorized invocation that returns no result before `replay_rpc_timeout` becomes `issue_unknown` and consumes one potentially submitted attempt. Another invocation may follow while both retry deadline and attempt budget remain.

Dropping the Frontend RPC future does not cancel work already accepted by a Datanode. A later replay may overlap with that remote work. Thus `replay_concurrency` bounds only local Frontend work, not Datanode operations. Qualification and canary tests must measure lingering ingest tasks, stalled requests, and write-buffer pressure under repeated timeouts; workloads that cannot accept this overlap must keep the queue disabled.

Each record has one final replay settlement, with earlier outcomes retained in bounded history. `give_up_reason` records why replay stopped; it is independent of the settlement and must not override it. Apply the following precedence:

1. `applied` if any replay response was observed applied. This does not prove that an earlier unresolved invocation did not also apply; no further replay is authorized after observed success.
2. `issue_unknown` if no replay success was observed and at least one potentially submitted invocation remains unresolved. This takes precedence over a later `stale_target` or observed error because neither proves that the unresolved invocation did not apply.
3. `stale_target` if refreshed metadata conclusively rejects the original target and no unresolved invocation or observed replay success takes precedence. Preserve `stale_target` as the `give_up_reason` even when the final settlement is `issue_unknown`.
4. `observed_failure` if a potentially submitted replay returned an error, no replay success was observed, no invocation remains unresolved, and replay stops without a confirmed stale-target result.
5. `replay_not_sent` if no potentially submitted replay invocation occurred and no confirmed stale-target result applies. Confirmed `not_submitted` invocations and pre-authorization deferrals do not change this settlement.

`replay_not_sent` describes only the replay phase. The original first-round disposition remains unchanged and is recorded separately.

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

Route and catalog observations are refreshed through the existing Frontend partition/catalog/cache services. They are not an atomic cross-cache snapshot. For one replay refresh cycle, work for runnable records should be coalesced by table identity and reuse the current `WriteRetry::reroute_failed_writes` refresh pattern where applicable:

1. Batch-invalidate the affected Region routes.
2. Resolve each distinct table's current name from its stable table identity.
3. Invalidate the corresponding name-keyed table cache entry.
4. Reload each distinct table through the existing `catalog_manager.table(name)` path, preserving its version-checked cache behavior.
5. Revalidate each record against its own captured table/Region/request sidecar and refreshed route snapshot.

Unlike the inline retry path, queue replay does not reconstruct a partitioner or reroute the request. The refreshed route is used only to find the current peer/leader for the original `RegionId`. If existing services cannot provide a sufficiently current route/catalog view, defer the send.

`TableIdent.version` is a freshness signal, not table identity and not an automatic stale condition. A version change triggers compatibility revalidation. A rename preserves identity when `TableId` remains unchanged; drop/create under the same name produces a different identity.

### Request compatibility

The plan-time sidecar captures the minimum information needed to validate the meaning of the retained request:

- complete table column-name-to-`ColumnId` mapping;
- for each planning column: name, `ColumnId`, logical type including datatype extension, semantic type, nullability, and default constraint;
- for present request columns: whether the retained values contain NULL;
- replay-policy fields: engine, append mode, instant-TTL, effective Dense encoding, and the original Flow-mirror decision.

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

This is a request-specific compatibility check, not a general schema-diff framework. A `TableIdent.version` change or `ADD COLUMN` is not automatically stale when Mito can safely synthesize the omitted value.

## Resource Limits and Scheduling

### Frontend-wide limits

The queue is one instance per distributed Frontend. It owns the following Frontend-wide limits:

- `write_queue.max_bytes`: estimated retained charge for request payload and bounded queue metadata;
- `write_queue.max_records`: admitted records, including records whose payload has been released but whose caller completion state remains attached;
- `write_queue.replay_concurrency`: active local replay work, including metadata/route preparation and RPC execution.

The queue retains the original materialized request by `Arc`; it does not create an encoded copy or replay-side body copy. A typed, checked-arithmetic estimator computes the retained charge once per Region request before the first RPC. It covers request rows/columns and dynamic string/bytes/JSON payloads supported by the request type, plus bounded queue metadata and completion state. If the estimate cannot be computed safely, preflight keeps the whole call on its existing path.

The estimator runs on the successful producer path, so first-round successes also pay its traversal cost. It must not acquire the QueueState lock, perform extra route/catalog lookups, or encode/copy the payload solely for queue accounting. Run it once over the materialized request and report its cost separately from failure-path work.

Before rollout, compare CPU per row, throughput, and latency with the queue-disabled baseline. Maintainers must explicitly accept the measured overhead; this RFC sets no percentage threshold. The ledger is an estimate, not an RSS bound: allocator overhead, gRPC/protobuf buffers, unrelated Frontend allocations, and remote Datanode work are excluded. Measure RSS separately.

Temporary reservations and published entries use the same ledger so concurrent admissions cannot overcommit the configured queue limits. Every rejection and terminal path releases its reservation/charge exactly once. No dead-letter payload archive or dead-letter query API is provided in v1.

### Scheduling and fairness

The scheduler runs ready Region FIFO heads with round-robin selection among Regions that are currently runnable. A record in backoff holds its Region FIFO head but no Frontend-wide active replay slot. Slow route/catalog/leader preparation consumes an active replay slot.

V1 makes no drain-time promise for arbitrary backlogs. `retry_budget` is an authorization deadline, not a service objective. Before enablement, maintainers must define the supported total backlog, maximum hot-Region depth, drain-time objective, p99 admission-to-first-authorization latency, and acceptable pre-authorization expiry fraction.

Qualification must show that records within this envelope retain an approved `H_min` at first authorization. Set `H_min` from measured replay RPC duration, settlement overhead, and a safety margin; this RFC does not assign it a numeric value. If one-record-per-RPC scheduling and per-Region serialization cannot meet the envelope, keep the feature disabled. Coalescing or per-Region reservations require a separate decision.

V1 provides neither bounded drain time nor strict fairness. A hot Region is serialized; a prolonged no-leader backlog can consume shared capacity; and slow metadata preparation can occupy every replay slot. There is no per-Region reservation, per-batch burst limit, or global rate ceiling for confirmed connection failures. Treat these as workload-envelope and canary criteria; add scheduling or rate-control mechanisms only if measurements justify them.

### Queue failure and shutdown

The Frontend `Instance` owns queue shutdown. The QueueState lock linearizes shutdown against send authorization: shutdown closes the queue under that lock, so an authorization either commits before closure or is rejected after it.

Route, catalog, and leader preparation happen before authorization. Shutdown during this phase creates no replay invocation. Apply the precedence in **Replay of unknown outcomes**: without a prior potentially submitted invocation or confirmed stale target, settle as `replay_not_sent`; otherwise preserve the recorded history. Pre-authorization cancellation never creates `issue_unknown`.

An invocation authorized before shutdown is treated as potentially submitted. Wait for its result up to `replay_rpc_timeout`; without a result, mark it `issue_unknown`. Do not `await` between committing authorization and first-polling the RPC future. Closing the local queue does not cancel work already accepted by a Datanode.

If the coordinator fails, close and mark the queue unhealthy through the same boundary. Stop admission and new authorizations, join active tasks within their existing timeouts, settle records from their histories, and notify attached callers. V1 does not restart the coordinator automatically; a record-task failure must remain isolated to that record.

## DDL, Flow, and Ordering Limitations

Queued retries may execute after the client has received an error, introducing delayed writes beyond the lifetime of the inline retry loop.

| Change or concurrent operation | V1 behavior and accepted risk |
|---|---|
| Concurrent live write to the same Region/key | Queue FIFO orders only queued records. A later live write may complete before an earlier queued replay; final conflict behavior depends on storage semantics. |
| `TRUNCATE` after the first-round error | No data-generation fence exists. Delayed replay may reinsert rows after truncation. |
| `DROP` then create under the same name | New table has a different `TableId`; replay is rejected. |
| `UNDROP` | The current implementation restores the tombstoned `TableInfo` and `TableRoute` without changing the captured identity fields; `DROP` does not bump `TableIdent.version`. A pre-drop record can therefore pass identity checks after `UNDROP` and replay into the restored table. V1 has no drop-generation fence. |
| Repartition or partition-expression change | The original Region/partition identity is no longer valid; stop replay with `give_up_reason = stale_target`, do not redirect or rebuild. Final settlement follows **Replay of unknown outcomes** if an earlier invocation remains unresolved. |
| `ALTER` racing between validation and send | There is no atomic DDL fence. Request-specific metadata validation reduces but cannot eliminate this race. |
| Flow exists at planning time | Flow-source request is preflight-ineligible because the original mirror is detached before Region dispatch and replay cannot repeat it safely. |
| Flow is created/changed after a non-Flow decision was captured | Replay uses the captured decision and does not re-read Flow membership; the delayed source write is not mirrored under the new definition. |

Workloads requiring DDL fencing, same-key ordering across delayed replay and live writes, or Flow changes to cover delayed writes must use a queue-disabled Frontend pool. V1 has no per-table allowlist or hot reload; enabling the queue affects every eligible direct insert on that Frontend, so isolate the pool operationally.

## Observability

Expose Frontend-scope metrics with bounded labels and non-overlapping accounting:

- preflight exclusions by bounded reason;
- admission candidates accepted/rejected by bounded reason;
- first-round outcomes: applied, retryable failure, terminal failure, `first_round_not_submitted`, `first_round_unknown`, and unadmittable;
- replay invocations by outcome: applied, retryable error, terminal error, `issue_unknown`, and confirmed `not_submitted`;
- final record settlements: `applied`, `issue_unknown`, `observed_failure`, `stale_target`, and `replay_not_sent`;
- terminal `give_up_reason` counts by bounded reason, kept separate from final settlement (for example, a `stale_target` stop can end with `issue_unknown`);
- retained bytes, retained record entries, and active replay-slot utilization;
- replay-observed rows, explicitly documented as observations rather than exact logical rows.

Emit one bounded structured event for each non-applied settlement, including table/Region identity, first-round provenance, final settlement, give-up reason, and potentially submitted attempt count. Keep IDs, request payloads, and unbounded error text out of metric labels.

Alert on sustained admission rejection, capacity saturation, hot-Region backlog, slow first-replay latency, deadline expiry, stale-target stops, replay timeouts, `issue_unknown` growth, and confirmed connection failures per second. Because `not_submitted` does not consume `max_attempts`, deadline and backoff bound elapsed time but not the aggregate connection-attempt rate.

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
| `write_queue.replay_rpc_timeout` | Per invocation | `10s` | Local timeout for an authorized replay RPC |
| `write_queue.replay_concurrency` | Per Frontend | `8` | Active local replay work |
| `write_retry.initial_backoff` | Shared backoff | Existing setting | Queue base backoff |
| `write_retry.max_backoff` | Shared backoff | Existing setting | Base-delay cap before positive jitter |
| `write_retry.backoff_multiplier` | Shared backoff | Existing setting | Exponential multiplier |
| `write_retry.max_retries` | Legacy path only | `3` | Does not control queue-owned records |

Queue configuration is loaded at startup. When enabled, required limits must be valid and non-zero; `completion_wait = 0` is allowed. Changing the enablement switch requires a Frontend restart, which may discard pending records.

### Enablement gates

Queue mode must remain disabled until all of the following are satisfied:

- The queue-specific first-round capture path preserves original Region requests, deterministic ordinals, per-entry mapping, retry provenance, and structured submission disposition independently of `WriteRetry` and `WriteProgress`.
- Tests cover per-entry failures, collapsed single-entry failures, whole-peer-RPC failures, mixed batches, missing provenance, `write_retry.max_retries = 0`, and partial admission.
- Every target Datanode supports the per-entry partial-result contract; mixed-version operation is excluded.
- The typed `not_submitted` marker is attached only at the verified unary error boundary, before error conversion, and survives the request/result path without being inferred from generic status or text.
- Completion wait, cancellation, publication ownership, response selection, and shutdown behavior pass integration and failure-injection tests.
- Same-Region validation and request-compatibility tests cover explicit columns, omitted-column Case A/Case B, `ColumnId` replacement, route refresh, and DDL limitations.
- Resource tests cover null-heavy and string/bytes-heavy requests, concurrent admission, maximum peer-batch fan-out, hot Regions, leaderless backlog, capacity exhaustion, and release-once behavior.
- Failure tests cover repeated connection refusal, fast Datanode rejection, Mito write-buffer stalls, local replay timeout, lingering remote work, worker failure, coordinator failure, and shutdown.
- Performance tests compare queue-enabled and queue-disabled successful paths under identical load, including estimator CPU per row, throughput, latency percentiles, retained estimate versus RSS, and failure-path behavior. The results must be reviewed by maintainers against an explicitly accepted successful-path overhead budget; no threshold is assumed by this RFC.
- The declared workload envelope specifies maximum total backlog, maximum hot-Region depth, drain-time objective, p99 admission-to-first-authorization latency, acceptable pre-authorization expiry fraction, and an approved `H_min`. Measurements demonstrate that records within that envelope reach first potentially submitted replay authorization with at least `H_min` remaining and that Datanode backlog/RSS remain acceptable under repeated timeouts.

### Rollout

- Upgrade every target Datanode before enabling the queue. Keep it disabled during mixed-version operation.
- Enable it first on a dedicated canary Frontend pool whose workloads accept the documented delayed-replay, duplicate, DDL, Flow, and ordering risks.
- Monitor admission rejection, queue depth/bytes, time to first replay, deadline expiry, confirmed connection failure rate, `issue_unknown`, hot-Region backlog, replay-slot utilization, and Frontend/Datanode RSS.
- Define alert thresholds and rollback criteria before canary. If the queue exceeds the qualified envelope, stop sending traffic to the pool and restart it with the queue disabled before returning traffic. This operator-driven rollback can discard pending records and does not cancel remote work already accepted by Datanodes.
- A change to the shipped default requires a separate maintainer decision after integration tests, failure-injection results, workload measurements, canary evidence, and explicit acceptance of remaining risks.

## Implementation Mapping to the Current Code

This RFC requires code changes; the current PR is documentation-only.

| Existing code | Current behavior | Required queue change |
|---|---|---|
| `Inserter::do_request` (`src/operator/src/insert.rs`) | Materializes impure defaults, creates/detaches Flow mirror work, prepares ordinary or tracked writes | Capture plan-time eligibility and request-compatibility metadata here; opt in only the supported non-tracked direct path; keep tracked and unsupported producers unchanged |
| `WriteDispatcher::prepare` (`src/operator/src/write_dispatcher.rs`) | Splits Region requests, resolves peers, groups entries by peer | Preserve deterministic Region ordinals and exact per-entry mapping before peer grouping |
| `WriteDispatcher::execute_with_progress` | Uses `send_batch` only when legacy retry or progress is present; otherwise calls `send` | Add an explicit queue-owned capture path independent of both legacy retry and progress; do not globally enable queue behavior on shared dispatchers |
| `WriteDispatcher::send_batch` | Requests partial results; with `retry == None`, failure attempts do not retain replayable requests | Retain each original Region request and provenance for queue-owned calls even when `write_retry.max_retries = 0` |
| `RoutedWrite` | Already retains `Arc<RegionRequest>`, attribution, and peer | Reuse this ownership model; do not encode/decode or rebuild payloads |
| `WriteRetry::retry` / `reroute_failed_writes` | Performs inline retries and batches route refresh for failures requiring refresh | Keep this existing path unchanged for non-queue calls; queue has its own bounded replay owner and batches queue refresh work without using inline retry's request rebuilding |
| `RegionRequester::handle_inner` (`src/client/src/region.rs`) | `ready()` errors are normalized; unary errors are mapped to `RegionServer` errors | Attach typed `not_submitted` only to the verified `unary()` ConnectError branch before `map_region_error` |
| Datanode `handle_write_request` (`src/datanode/src/region_server.rs`) | Partial result response for supported multi-entry requests; single-entry errors remain tonic errors | Keep upgrade-order gate; test both response forms and preserve original request mapping |
| `FrontendBuilder::try_build` (`src/frontend/src/instance/builder.rs`) | Creates ordinary `inserter` and execution-only `bulk_inserter` through a shared construction closure | Construct one queue per distributed Frontend `Instance`; inject it only into the ordinary supported direct-insert `inserter`; keep `bulk_inserter` queue-free and do not put queue injection in the shared closure |
| Partition/catalog/cache services | Existing route and table metadata caches | Use these services for same-Region refresh and name-keyed/version-checked metadata revalidation; do not introduce a separate catalog or routing subsystem |

Land the capture path and its tests before enabling the queue. Keep the queue disabled until every **Enablement gates** criterion passes.

## Alternatives

- **Append a queue after existing inline retries:** rejected because it creates two retry owners, extends the total retry window unpredictably, and complicates request ownership and duplicate handling.
- **Limit admission to explicit `PartialRegion` responses:** rejected because whole-peer-RPC and single-entry failures include important restart and single-Region cases.
- **Rebuild requests against current partitioning:** rejected for v1 because cross-Region repartition recovery requires new row routing, successor ordering, and additional ownership rules. V1 replays only to the original Region.
- **Durable queue:** deferred to a separate design. It changes acknowledgement and recovery semantics and is not required for the in-memory v1 goal.
- **Inline fallback after queue admission rejection:** not included in v1 because it introduces a second retry owner after the queue has been selected. Capacity rejection is instead an explicit accepted limitation and rollout gate.

