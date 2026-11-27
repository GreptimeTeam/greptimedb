---
Feature Name: In-Memory Frontend Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
Status: Draft
---

## Summary

This RFC proposes an optional, per-Frontend in-memory retry queue for eligible transient failures from ordinary, non-tracked direct inserts to partitioned, Mito-backed physical Regions on distributed Frontends. Prometheus remote write is included only when its actual producer path uses this same non-tracked direct-insert path and satisfies every eligibility rule. For eligible calls, the queue provides an alternative retry owner with a longer bounded recovery budget; it is not a second retry stage appended after the existing inline retry loop. Calls that fail preflight eligibility remain on their existing path.

The retry owner is selected for the entire direct-insert call before its first write RPC. A queue-owned call captures the complete first-round per-Region report before aggregation or another inline retry. Successful Regions are never replayed. Eligible failed Region requests are admitted independently to their original Region FIFOs. Once queue ownership has been selected, admission rejection is fail-closed and never falls back to inline retry.

Each record retains the original materialized request and remains bound to its captured table identity, Region identity, and partition-expression version. Replay may refresh routing to a new peer or leader, but it may send only to the original Region; it never repartitions, rebuilds, or redirects the request.

The queue bounds retained state, per-record replay time and potentially submitted replay attempts, and Frontend-wide active replay work. A bounded completion wait may turn a transient failure into an observed success, but expiry does not cancel queue work. The queue is disabled by default and is best-effort only: it is in-memory, may lose work on process restart, and permits duplicate-capable outcomes when RPC submission or remote application is uncertain.

### Request lifecycle at a glance

1. **Preflight:** classify the whole call before its first write RPC. If any Region attempt is ineligible, keep the whole call on its existing retry path.
2. **Capture:** for a queue-owned call, retain the exact Region requests, peer-batch mapping, request sidecars, and complete first-round result report.
3. **Classify:** never replay successes. Classify failures using the propagated retry hint, except for the exact typed `not_submitted` disposition defined in the first-round capture rules.
4. **Admit:** publish each eligible failed Region attempt independently and atomically with its retained charge and caller-observer state. A rejected candidate gets no inline fallback.
5. **Replay:** process each Region FIFO head against the same Region identity, with bounded deadline, attempt count, and active work.
6. **Respond:** after all admission decisions, return the selected first-round error or wait up to `write_queue.completion_wait` for observed application. Queue work continues after the caller detaches.

## Motivation

The existing inline retry path is bounded by `write_retry.max_retries` and jittered backoff, but it does not retain failed Region requests after the retry loop gives up. With the default three retry rounds, the configured base backoffs are 1.5s, 3s, and 6s (10.5s total before RPC time); positive jitter can bring the scheduled backoff total close to 21s. First-round RPC duration is also not bounded by the queue: queue ownership cannot begin until all first-round peer-batch results have been collected. The proposed queue therefore owns the retry decision before the first write RPC and starts its own bounded replay policy only after that report is complete.

`write_queue.retry_budget` is an absolute per-record deadline beginning at admission, not a promise to issue RPCs for the full interval. It includes backoff, route and metadata preparation, leaderless periods, connection failures, and replay RPC execution. Ordinary failures follow the final propagated retry hint; the narrowly scoped typed `not_submitted` outcome is the only exception. It consumes deadline and backoff but not the potentially-submitted attempt budget. Every other authorized invocation is duplicate-capable and consumes an attempt, including timeouts and lost responses.

The proposed five-minute budget is a starting point, not a production guarantee. Its primary recovery case is a Datanode process restart, including cold start, Region reopening, and gRPC readiness; it is not a general guarantee for every failover or migration path. Each deployment must qualify its actual failure-detection, initialization, migration, scheduling, and replay timings. The budget must leave enough time for at least one potentially submitted replay attempt after the Region becomes runnable; deployments whose recovery window does not fit must increase the budget or keep the queue disabled.

A Datanode restart, failover, or leader change may outlast the inline retry window. A write RPC can also fail after the request reaches the Datanode but before the Frontend receives a definitive per-Region result. Retrying such a request may duplicate a write, while dropping it may leave part of a multi-Region insert unapplied. A client-visible batch error therefore does not imply that no rows were written.

A routed insert is tied to the table identity, target Region, and partition expression used to map rows. If repartitioning or another metadata change moves those rows to a different Region, replaying the original Region request is no longer sufficient. v1 stops replay when the captured target is no longer valid rather than rebuilding the request against a new partitioner.

This RFC specifies only bounded, process-local retry and completion behavior. It does not provide durable retention, restart recovery, exactly-once delivery, or cross-Region repartition recovery; those require separate designs.

## Goals and Non-goals

### Goals

- Extend retry handling for eligible transient insert failures beyond the existing inline retry window.
- Select one retry policy for the whole direct-insert call before the first RPC; never switch a queue-owned failure back to inline retry.
- Preserve per-Region outcomes, exact routed requests, and first-round failure provenance before batch-level result merging.
- Bound queue-retained state, each record's retry duration and potentially submitted replay-attempt count, and local replay concurrency; confirmed `not_submitted` invocations are separately bounded in time by the record deadline and backoff.
- Define client-visible behavior for mixed batches, partial admission, uncertain outcomes, and bounded completion waiting.
- Replay a retained request only while its captured table and same-Region routing identity remain valid.

### Non-goals

- Durable queue storage, process-restart recovery, or guaranteed eventual delivery.
- Exactly-once or at-least-once write semantics, or proof that an unknown RPC was not applied.
- A Datanode protocol change for idempotency or deduplication.
- Cross-Region repartition recovery, row repartitioning, FanOut, or successor records. If the original Region or partition-expression identity is no longer valid, v1 stops replay rather than redirecting the request.
- Queue use in Standalone mode or for calls using confirmed-write mode / the tracked execution path.
- Replacing retry behavior for Delete, BulkInsert, Flight `DoPut`, metric logical-batcher flushes, or writes flushed by pending-rows/table batchers. These paths remain outside the queue.
- Queue admission for Metric logical-row inserts that still target logical Metric-engine Regions.
- Queue admission for append-mode tables, instant-TTL tables, Flow-source tables, Regions with an absent partition-expression version (including default Regions in partitioned tables), or requests missing required plan-time context.
- Ordering across Frontends, against concurrent live writes or legacy-inline attempts, or proof of remote application order after an invocation becomes `issue_unknown`.
- A deadline or cancellation guarantee for the initial first-round Region RPC. Queue ownership begins only after the complete first-round report is available; a peer-batch RPC that never returns can keep the caller pending and prevent queue handoff.
- A data-generation fence for `TRUNCATE`, a drop-generation fence for `DROP`/`UNDROP`, or an atomic DDL fence; a dead-letter query API, hot configuration reload, or per-table queue allowlisting.
- Recovery from process-level loss or corruption of the queue's coordination state.

## Request Scope and Retry Ownership

### Supported paths and retry-owner selection

The retry policy is selected for the entire direct-insert call before its first write RPC. Eligibility is based on the actual producer path, plan-time target metadata, and normalized per-Region insert attempts—not solely on the external protocol or table name. A mixed-eligibility call stays entirely on its existing retry path. Once queue ownership is selected, the first-round report is captured before any legacy retry round or result aggregation; admission rejection is fail-closed and never falls back to inline retry.

| Request path | v1 behavior when enabled |
|---|---|
| Ordinary, non-tracked direct inserts to eligible Mito physical Regions, including supported SQL, gRPC, pipeline, COPY, and Prometheus remote-write paths that use this producer path | Queue-owned only when every Region attempt in the call is eligible |
| Confirmed-write / tracked execution | Outside v1; existing tracked completion and retry behavior remains |
| Log/trace inserts to non-append-mode Mito physical Regions | Eligible only when all other rules are met |
| Automatically created append-mode log/trace primary or exemplar tables | Ineligible; existing behavior remains |
| Trace auxiliary tables | Evaluated by effective Region options, not table name alone |
| Delete, BulkInsert, Flight `DoPut`, metric logical-batcher flushes, and pending-row/table batchers | Outside v1 |

Queue mode requires a request-capture path that preserves original Region requests and per-entry results independently of the legacy retry count. `write_retry.max_retries = 0` is therefore compatible with queue-owned calls; preflight-ineligible calls retain the existing non-queue behavior, which in that configuration performs no legacy retry rounds. The queue is not a second stage appended after inline retries are exhausted.

The caller must receive an observable admission result for each eligible failed Region attempt, distinguishing publication from capacity rejection, queue closure, invalid context, and explicitly fallible allocation/reservation failure. A unit return value or log-only rejection is insufficient: the caller may return `Ok` only when every eligible candidate was admitted. Sibling attempts may be admitted independently, but any rejected queue-owned attempt prevents a successful call result.

Queue ownership is supplied explicitly by the supported producer. Shared insert-building or dispatch components must not infer queue mode globally, and unsupported producers must not inherit queue behavior merely because they share internal helpers. Admission revalidates the request shape before publication.

### Frontend-wide queue ownership

The distributed Frontend runtime creates and owns exactly one queue. That instance owns the Frontend-wide retained-byte, record-count, FIFO-sequence, and replay-concurrency limits. The queue reference and replay dependencies are passed explicitly through the supported ordinary, non-tracked direct-insert path; they are not installed as a global mode on shared dispatch infrastructure used by unrelated producers. Route and catalog services are supplied by the Frontend runtime. Queue shutdown follows that runtime's lifecycle.

Calls using tracked execution remain outside v1. Their aggregate completion contract is not a per-Region admission or settlement report and requires a separate handoff design.

### Eligibility and plan-time context

Queue eligibility is selected for the whole direct-insert call before its first write RPC. A call is queue-eligible only when every Region attempt satisfies the v1 scope; otherwise the whole call remains on its existing retry path. Admission later decides independently whether each eligible failed Region attempt can be published.

An attempt is eligible only when it:

- is one ordinary insert to a supported Mito-backed physical Region on a distributed Frontend;
- carries complete plan-time table, Region, partition, and replay-policy context;
- has a non-empty partition-expression version;
- targets a table whose planning metadata has `append_mode = false` and is not instant-TTL;
- carries an unambiguous non-Flow decision captured during original insert planning;
- uses effective `Dense` primary-key encoding in both the target Region options and retained request hint (an absent hint means `Dense`);
- does not omit a `NOT NULL` column without a default;
- has a safely computed retained-charge estimate no larger than `write_queue.max_bytes`; and
- retains the complete, materialized Region request and the sidecar needed to validate it before replay.

Regions whose partition-expression version is absent are excluded, including unpartitioned Regions and default Regions in otherwise partitioned tables. An absent version represents an empty partition expression, and the Datanode does not validate a partition-expression version when the request omits it. v1 does not invent a version for this case because it could not fence a later partition-layout change.

Routing uses the input rows before impure defaults are materialized. Values introduced by default materialization do not trigger a second partitioning pass. Replay therefore retains the original Region and never recomputes routing from the materialized values. If that Region or its captured partition identity is no longer valid, replay stops according to the target-validation rules; it does not select a successor Region.

Impure defaults are materialized before the first Region RPC. The queue retains the exact materialized request and never evaluates an impure default again. An impure default on a primary-key or time-index column is not itself an exclusion: for example, `DEFAULT current_timestamp()` remains eligible when the original value is present in the retained request. A required materialized value that is absent makes the candidate unsafe and must be rejected before publication. After publication, request completeness is an invariant; a defensive violation stops replay before send authorization and is treated as an internal consistency failure.

Sparse primary-key encoding is excluded because missing-column synthesis is available only for Dense requests. v1 requires both the plan-time target option and retained request hint to resolve to Dense; it does not rely on the Datanode to reject a hint that disagrees with the target Region encoding. The retained request and its original encoding hint are never rewritten.

The same Flow-mirror decision used by the original insert must be carried with each Region attempt. An initialized empty Flow set is an explicit non-Flow result; a failed, unavailable, or ambiguous lookup makes the whole call queue-ineligible before dispatch. Flow-source writes are excluded because the original insert submits its Flow mirror work before source-Region dispatch, but that work is detached from the source Region result and is not part of queue completion. A delayed source replay does not wait for or repeat the original mirror pass; allowing it could change source/mirror ordering or duplicate a mirror side effect. If a Flow is created or changed after a non-Flow decision was captured, replay does not re-read membership and the delayed source write is not mirrored according to the new definition. v1 has no Flow-generation fence.

Each Region attempt carries three pieces of context:

- **Replay-policy and Region-identity snapshot:** stable table identity, plan-time `TableIdent.version`, target engine, table-managed append mode, instant-TTL state, the captured Flow-mirror decision, Region identity, and partition-expression version. The table metadata version is a freshness signal that triggers revalidation; it is not an identity field or a rejection condition by itself. These values remain associated with the exact request through dispatch and result extraction.
- **Request-compatibility snapshot:** the minimal information needed to validate this request's meaning, covering both present and omitted columns. For each planning column, retain its name, `ColumnId`, logical type including datatype extension, semantic type, nullability, and default constraint. For present columns, also retain whether the request values contain NULL. The planning metadata must provide a complete name-to-ID mapping; if it is missing or incomplete, the call is queue-ineligible rather than guessing IDs from names or positions. Replay applies the detailed compatibility rules in **Metadata compatibility and route refresh**.
- **Retained-charge estimate:** compute once for each materialized Region request before the first RPC, cache it with that attempt, and reuse it during admission. It is accounting data, not part of the metadata snapshot.

Before the first write RPC, preflight selects either queue ownership or the existing retry path. The following table distinguishes preflight exclusions from later admission rejections:

| Classification | Meaning and behavior |
|---|---|
| Unsupported request shape | The body, table shape, engine, or producer path is outside v1. The whole call remains on its existing path. |
| Invalid plan-time context | Required metadata is missing, malformed, inconsistent, or cannot be associated with the exact Region request. The whole call remains on its existing path. If a supposedly valid sidecar fails validation during admission, that candidate is rejected without inline fallback. |
| Retained-charge estimate unavailable or larger than `write_queue.max_bytes` | Preflight exclusion before the first RPC; the whole call remains on its existing retry path. |
| Shared capacity exhausted, queue closure, invalid admission sidecar, or explicitly fallible allocation/reservation failure | Admission-stage rejection after queue ownership is selected. No capacity wait or inline fallback occurs. |

Queue ownership is enabled explicitly by supported producer context, not inferred from shared dispatch internals. The Frontend runtime supplies route and catalog dependencies and owns queue shutdown.

Admission is the ownership boundary. Before publication, the queue does not own the candidate; after publication, it remains responsible until settlement or deadline. Each candidate is admitted independently, so already-published siblings remain queue-owned if a later sibling is rejected. The record, retained charge, and caller-observer state become visible together in one non-awaiting ledger critical section. The final cancellation check and publication decision are linearized there: cancellation or queue closure observed first rejects publication; otherwise publication wins and later cancellation only detaches the caller. Temporary reservations are released exactly once on every pre-publication failure. Admission never waits for capacity, network I/O, metadata refresh, or replay-worker scheduling.

A Region request whose retained-charge estimate cannot be computed safely or exceeds the entire `max_bytes` budget is known to be unadmittable before dispatch, so it keeps the whole call on the existing path. A request that fits individually may still be rejected later because the shared ledger is full; after queue ownership is selected that rejection is fail-closed. Only errors returned by explicitly fallible allocation or reservation APIs can be surfaced as admission errors. v1 does not promise to catch or convert arbitrary allocator exhaustion or process-wide OOM.

### First-round capture and ownership boundary

Queue mode needs a lossless first-round report before any legacy retry or result aggregation can consume per-Region outcomes. The report contains each original Region request, peer-batch membership and entry mapping, deterministic Region-attempt ordinal, propagated retry hint, original error, typed submission disposition, and plan-time sidecar. This is a narrow capture capability, not a second retry framework; it must work whether `write_retry.max_retries` is positive or zero.

One peer-batched RPC may contain multiple Region entries. The report must expand the result back to those original attempts and distinguish explicit per-entry results, a collapsed single-entry failure, a whole-peer-RPC failure without per-entry results, and an outcome whose request or failure provenance cannot be recovered. Successful Regions are never replayed. A collapsed or whole-peer-RPC failure may be expanded only when the exact original Region requests, split-to-entry mapping, batch provenance, and attributable retryable hint are retained. Such records are `first_round_unknown`: the original RPC may already have applied, so replay is duplicate-capable. Missing or ambiguous provenance makes the affected attempt `unadmittable`; the queue must not manufacture a request or infer attribution from entry count alone.

Ordinary failures are queue-admissible only when the final propagated `RetryHint` is retryable. The sole exception is the typed `not_submitted` disposition. With the lazy client channel used by this path, `client.ready()` may succeed before a connection is established; the refused-connection error is surfaced by `unary()`, whose raw `Status` source chain can contain the exact `tonic::ConnectError`. Attach the typed disposition only in that `unary()` error branch, before error normalization drops the source. Do not attach it to a `ready()` failure. Carry the disposition as structured metadata into the first-round report alongside the retained request and peer-batch mapping. Generic `Unknown` or `Unavailable` statuses, other transport errors, and connection-related message text do not establish non-submission. Downstream queue logic must not scan arbitrary source chains or infer the disposition from status or text. If the marker or matching provenance is lost, use the ordinary retry-hint rule or classify the attempt as `unadmittable`.

Queue-owned multi-entry Region insert RPCs must request the per-entry partial-result contract independently of the legacy retry setting. All target Datanodes must support that contract before queue mode is enabled; older Datanodes that collapse a whole peer batch into one error cannot provide the required Region-level report, so mixed-version operation is excluded. A single-entry failure may still be collapsed; it is admissible only if its original request and attributable retry hint are retained. The queue-specific capture path must retain failed requests even when the legacy retry path would not, without encoding, decoding, or rebuilding them.

Preparation and leader resolution complete before peer-batch RPCs begin. A preparation/no-leader error is a call-level pre-dispatch error and creates no queue record; this differs from a stale-leader failure returned after a peer RPC has been entered. The non-tracked insert path collects first-round peer-batch futures in a background dispatch task. Its caller-cancellation token does not interrupt that `join_all`: all peer-batch results are awaited before the complete report is available. A peer-batch RPC that never returns prevents handoff for the whole call, even if other batches have already failed. The initial RPC has no queue-side deadline. Caller cancellation is checked after the complete report and again at each candidate publication boundary; candidates not published when cancellation is observed are not admitted, while already-published records remain queue-owned. If the background dispatch task is aborted or fails before producing the complete report, no partial handoff occurs.

Assign each Region attempt an ordinal before peer grouping and preserve it through result expansion. Peer grouping order is not stable, so the ordinal—not RPC completion timing—determines selected-error precedence. It does not promise to reproduce SQL row-input order.

### First-round outcomes and admission

| First-round outcome | Queue/retry action | Retry owner |
|---|---|---|
| Success / observed `applied` | No record; never replay | None |
| Per-entry failure with retryable propagated hint, retained Region request, and valid sidecar | Admit independently if capacity is available | Queue if admitted; otherwise none |
| Per-entry, collapsed single-entry, or whole-peer-RPC failure with a non-retryable final hint | Preserve the original error and ordinal(s); do not create a queue record | Terminal for client-error selection |
| Whole-peer-RPC failure with attributable retryable hint and exact original Region splits | Admit one `first_round_unknown` record per retained Region split; replay may duplicate a write already applied by the original RPC | Queue per admitted attempt; otherwise none |
| Collapsed single-entry failure with attributable retryable hint and exact original request | Admit one `first_round_unknown` record if capacity is available | Queue if admitted; otherwise none |
| Exact typed `not_submitted` disposition with retained request and peer-batch provenance | Admit each original split as `first_round_not_submitted`, even without a retry hint | Queue; consumes deadline/backoff but no replay-attempt budget |
| Connection-related failure without the exact typed disposition | Apply the ordinary propagated-hint rule; without an attributable retryable hint, preserve as `unadmittable` | Queue only with an ordinary retryable hint; otherwise none |
| Failure with missing/ambiguous request, batch provenance, or retry attribution | Preserve the original error and ordinal; do not create a record | Terminal for client-error selection |
| Retryable failure for a call excluded by preflight | Keep the existing retry behavior where supported | Existing retry owner |
| Prepare/no-leader failure before any RPC | Return the call-level error; create no queue record | Existing pre-dispatch behavior |
| Admission rejected for capacity, closure, invalid sidecar, or explicitly fallible allocation/reservation failure | Reject without waiting; no inline fallback | None |

Each expanded Region attempt is an independent record with its own retained charge, `write_queue.max_attempts`, and absolute `write_queue.retry_budget` deadline. All records share the Frontend-wide byte, record-count, and replay-concurrency limits. A large failed peer batch can therefore create a burst of records; its potentially submitted replay-attempt count can scale with the number of admitted splits multiplied by `max_attempts`. v1 has no separate per-batch burst limiter or per-batch attempt budget. First-round `first_round_unknown` is distinct from replay invocation `issue_unknown`, which is used only after a replay invocation is authorized but its result is not observed.

### Selected client error and mixed batches

The selected client error is determined from the complete first-round report before admission. If preparation fails before any RPC, return the call-level pre-dispatch error. Otherwise, select the lowest-ordinal terminal or `unadmittable` outcome first. An `unadmittable` attempt has no recoverable exact request/batch provenance or no attributable retryable hint and no exact typed `not_submitted` disposition; this classification does not claim that the remote write was unapplied. A retryable or confirmed-not-submitted sibling does not make it retryable. If there is no terminal or `unadmittable` outcome, select the lowest-ordinal queue-admissible failure as the client-visible first-round error.

Admission success or failure does not change the selected error. Each candidate is admitted independently; a rejected queue-owned candidate causes the request to return the selected first-round error even when sibling candidates were published. Already-published siblings remain queue-owned. There is no inline fallback after queue ownership has been selected.

## Client-Visible Contract

For a queue-owned call, each admitted candidate (ordinary retryable or exact first-round-not-submitted) has one bounded caller-observable completion result. The record, retained charge, and observer state are published together; the observer is established before the record becomes runnable. A record may settle immediately after publication, but its result remains observable until the caller detaches. Caller-observer state is included in retained accounting. `max_records` counts entries while either the request payload or caller-observable completion state remains retained; a payload-released entry remains counted until the caller detaches.

The completion-wait budget applies only after all first-round outcomes and admission decisions have been processed. When a positive `write_queue.completion_wait` is applicable, it uses one monotonic deadline beginning at `wait_start`; planning, preflight, initial route/leader preparation, first-round RPC execution and waiting, report construction, classification, and admission are excluded. The wait deadline limits only the caller's wait. It does not cancel replay, an in-flight RPC, or queue-owned work.

The caller evaluates the registered completion results before waiting; `completion_wait = 0` returns from that immediate snapshot without blocking. Completion that occurs before waiting begins must remain observable, and the wait protocol must not lose a completion racing with wait registration.

- A terminal first-round failure or any queue-owned attempt that was not published returns the selected first-round error without waiting; already admitted siblings continue independently.
- If every queue-admissible candidate (ordinary retryable or exact first-round-not-submitted) was admitted, no terminal first-round failure exists, and every admitted record is observed as `applied`, return `Ok` immediately.
- If a record reaches a **final** queue settlement other than `applied` (`issue_unknown`, `observed_failure`, `stale_target`, or `not_sent`), return the selected first-round error immediately; other admitted records continue independently.
- An invocation-level `issue_unknown` is not by itself a final record settlement. If the record still has remaining retry budget and attempt budget, it may authorize another distinct invocation and the caller may continue waiting. Repeated RPC timeouts can therefore keep the record pending until another invocation succeeds, the record reaches a final settlement, or `completion_wait` expires.
- If records remain pending and no final non-`applied` settlement exists, a positive completion-wait budget continues until all are applied or the deadline expires. With a zero budget, pending records cause the selected first-round error to be returned immediately. Deadline expiry returns the selected first-round error; it does not settle or cancel pending records.

A `first_round_unknown` record may be observed applied by replay and permit `Ok`, but this does not prove that the original first-round RPC was not also applied. Queue replay errors remain in bounded record history and diagnostics; they do not replace the selected first-round client error.

`affected_rows` is a response-time snapshot. It includes first-round successes and queue replay successes observed before the response is constructed; later replay successes are not included. It is not an exact logical row total and may not account for duplicate application. Admission publication is the ownership boundary: if cancellation wins before a candidate is published, that candidate is not queued, even when its first-round RPC was issued; the remote operation may still apply. If publication wins first, later cancellation, timeout, or response only detaches the caller-observer state and does not revoke queue ownership. Orderly queue shutdown closes admission and new authorization under the ledger mutex, waits for already-authorized RPCs until their existing `replay_rpc_timeout`, finalizes records from their histories, and wakes attached callers. Calls assigned to the existing retry path and calls using the tracked execution path retain their existing cancellation and result behavior.

## Replay State Machine and Retry Semantics

### Records and invocations

A **record** represents one admitted single-Region attempt. It retains the original routed request, first-round provenance and disposition, replay-policy and Region-identity snapshots, request-compatibility metadata, an absolute retry deadline, a potentially-submitted attempt budget, per-record backoff state, bounded attempt history, and caller-observable completion state while the caller remains attached. Its immutable first-round disposition is `observed_retryable_failure`, `first_round_not_submitted`, or `first_round_unknown`. v1 creates no successor records: a record remains bound to its original Region and partition-expression version for its lifetime.

A **send-authorized invocation** begins when the queue authorizes and first polls the Region RPC future. Authorization creates a provisional in-flight entry; it does not prove that the request was submitted. A worker failure after authorization without an observed result is conservatively treated as `issue_unknown`; it must not be reclassified as not sent merely because the worker failed. The exact typed `not_submitted` disposition removes that provisional entry, consumes no `write_queue.max_attempts`, releases the active replay slot immediately, advances backoff, and retains the Region FIFO head reservation. Every other observed result commits one potentially-submitted attempt; an invocation with no observed result becomes `issue_unknown` and also consumes one attempt.

The **Region FIFO head reservation** keeps a record at the front of its Region queue across backoff without occupying a Frontend-wide replay slot. `write_queue.max_attempts` counts committed potentially-submitted attempts only. Confirmed connection failures are counted separately for diagnostics and are bounded by the absolute record deadline and backoff, not by `max_attempts`.

All catalog metadata, route, same-Region validation, leader resolution, request construction, and acquisition of the peer's `RegionRequester` complete before authorization. The send-authorized future is the subsequent `RegionRequester::handle` operation itself; its raw client/channel acquisition, readiness check, and unary RPC are inside the authorized invocation and inside `write_queue.replay_rpc_timeout`. With the lazy channel, readiness can succeed while the connector failure is returned by `unary()`, so the `not_submitted` marker is attached only to that unary error before normalization. Immediately before authorization, under the queue ledger mutex, the task checks `closed`, record ownership, deadline, and remaining attempt budget; `closed` takes precedence over deadline expiry. It publishes the provisional in-flight entry, releases the mutex, and immediately first-polls the RPC future with no intervening `await`, metadata refresh, route lookup, request reconstruction, or other asynchronous work.

A result without a retryable propagated hint is terminal under the queue policy. A send-authorized invocation without a result is `issue_unknown`; a distinct later invocation may still be scheduled when deadline and attempt budget permit, while the earlier unknown remains in history and may still apply remotely. A failed worker does not automatically restart its record.

### Record settlement

A record exposes two independent facts: its immutable **first-round disposition** and its final **replay settlement**. The first-round disposition records whether the original outcome was a definitive retryable per-entry failure, a confirmed non-submission, or an unknown peer-batch/collapsed result. Replay settlement summarizes the record's complete replay history; neither field proves remote application order.

| Replay settlement | Meaning |
|---|---|
| `applied` | At least one replay invocation was observed applied; an earlier `issue_unknown` may still exist |
| `issue_unknown` | An invocation remains unresolved, none was observed applied, and no further invocation will be authorized |
| `observed_failure` | A potentially-submitted replay attempt returned an error and none remains unresolved; this is a client-observed failure, not proof of non-application |
| `stale_target` | Current metadata confirms that the captured table, Region, partition, replay policy, or request meaning is incompatible |
| `not_sent` | No potentially-submitted replay attempt was committed and no higher-precedence target-invalidity result applies |

The settlement precedence is `applied`, `issue_unknown`, `stale_target`, `observed_failure`, then `not_sent`. The first-round disposition remains separately attached. A confirmed `not_submitted` invocation does not count as a potentially-submitted attempt and therefore does not by itself rule out `not_sent`. Any other send-authorized outcome commits an attempt. An observed replay error may still be duplicate-capable; a record with `observed_failure` and `first_round_unknown` must not be reported as a known end-to-end failure. Likewise, a record labelled `applied` may still have an earlier unresolved invocation.

### Replay outcomes

| Event | Behavior |
|---|---|
| Replay observed applied | Settle as `applied`; release payload when completion/accounting cleanup no longer needs it |
| Replay returns a retryable hint | Schedule a distinct invocation after backoff if deadline and attempt budget permit; retain the Region FIFO head |
| Replay returns a terminal hint | Stop this record and settle from its complete history |
| Exact typed `not_submitted` disposition | Remove the provisional entry, consume no attempt, release the active slot, retain the FIFO head, and defer with backoff |
| Send-authorized invocation has no result | Commit one attempt as `issue_unknown`; a later distinct invocation is allowed only if budget remains |
| Route, metadata, leader, or client preparation cannot complete | Do not authorize an RPC; defer with backoff while the deadline permits |
| Current metadata confirms an incompatible target | Stop replay as `stale_target`; do not rebuild, repartition, or redirect the request |
| Deadline expires or attempt budget is exhausted | Stop new authorizations and settle from the complete invocation history |
| Worker failure | Finalize this record from history, release its Region claim and active slot, and do not automatically restart it |
| Orderly shutdown | Close admission and authorization; join authorized calls until their result or existing replay timeout, then settle unresolved calls as `issue_unknown` |

A **definitive per-entry first-round result** gives an explicit success or failure for each Region entry. A whole-RPC transport error without per-entry results is not definitive; its expanded records retain `first_round_unknown` provenance.

For the first replay, `observed_retryable_failure` and `first_round_unknown` use the propagated first-round retry hint. `first_round_not_submitted` has no retry hint, so it waits for the initial backoff and then resolves the current route for the same Region; it does not force route invalidation solely because a connection failure occurred. Later retryable results use normal route/leader resolution, while an explicit route-refresh hint forces relevant route refresh before same-Region validation. A missing or ambiguous hint never authorizes another invocation.

A fast Datanode rejection consumes one potentially-submitted attempt and follows its retry hint. A Mito write-buffer stall may not return promptly; if the local replay timeout expires, the attempt becomes `issue_unknown` and remains duplicate-capable because Datanode work may continue after the Frontend stops waiting.

### Retry budget and per-record limits

The absolute retry deadline is `admission_time + write_queue.retry_budget`, measured monotonically from record publication. Backoff, metadata lookup, route refresh, same-Region validation, leader resolution, client preparation, and leaderless deferral all consume this deadline. The scheduler wakes at `min(backoff_ready_at, record.deadline)`. If the deadline expires before authorization, no RPC is issued; the record is finalized from its existing history and its Region claim and active slot are released exactly once. A preparation future that started before expiry cannot authorize a call after the deadline; expiry while a route, metadata, leader, or client-preparation lookup is pending finalizes the record from its existing history and releases the Region FIFO head and any active replay slot exactly once.

Transient lookup failures and no-leader results defer with backoff while retaining the Region FIFO head but release the active replay slot. After preparation, queue state, record ownership, deadline, and remaining attempt budget are checked again immediately before authorization. An invocation authorized before the deadline may run until its separate `write_queue.replay_rpc_timeout`, which starts at authorization; cleanup may therefore finish after the record deadline.

Queue replay reuses the existing jittered exponential backoff parameters `write_retry.initial_backoff`, `write_retry.max_backoff`, and `write_retry.backoff_multiplier`; `write_retry.max_retries` does not apply to queue-owned records. The max-backoff setting caps the base delay before positive jitter, so the final wait may approach twice the configured cap. Backoff advances for pre-authorization deferrals, retryable RPC failures, and confirmed connection failures; it is independent of the potentially-submitted attempt count.

`write_queue.max_attempts` caps potentially-submitted replay attempts after the original first-round request. v1 proposes 3. The exact typed `not_submitted` disposition is the only authorized outcome that consumes no attempt; timeouts and worker failures without a result consume one as `issue_unknown`. The cap and deadline are independent; whichever is reached first stops new authorizations. Attempt history is bounded by this cap. Confirmed connection failures do not enter that history and do not have a per-record diagnostic counter or unbounded event log.

`write_queue.max_records` counts each admitted record until both its retained request payload and caller-observable completion state have been released. Queue-owned calls use their own capture path and limits regardless of whether legacy retry state is constructed.

### Worker failure, cancellation, and shutdown

Each Region FIFO head has one active replay owner. A per-record worker failure finalizes that record from its existing history and removes it from the FIFO; it does not automatically restart the record or hold the Region until its original deadline. A failure before authorization creates no invocation. A failure after authorization without a result commits the invocation as `issue_unknown`. The Region claim, replay slot, and retained resources are released exactly once; caller-observable completion state remains until detachment.

The scheduler/coordinator and per-record work have separate failure scopes. A record-task failure must not stop unrelated Regions. The Frontend runtime retains an independent close/drain handle and observes coordinator termination. If the coordinator exits unexpectedly, the queue becomes unhealthy and closed under the ledger lock; new admission and authorization stop, active tasks are joined within their existing replay timeouts, unresolved calls become `issue_unknown`, remaining records are settled from history, and attached callers are notified. v1 does not automatically restart a failed coordinator.

Cancellation before publication prevents that candidate from entering the queue and releases temporary reservations. After publication, caller cancellation only ends the caller's wait. Orderly shutdown closes admission and authorization atomically, cancels pre-authorization preparation, and waits for authorized calls until they return or reach their existing `write_queue.replay_rpc_timeout`. Unresolved calls then become `issue_unknown`; an authorized call that returns the exact typed `not_submitted` disposition is not charged as a potentially-submitted attempt and, if no such attempt exists in its history, settles as `not_sent` without another invocation after close. No new invocation is scheduled after close. Dropping a local future does not guarantee cancellation of work already accepted by a Datanode. Completion-only entries remain counted until their callers detach.

## Same-Region Replay Validation

### Metadata compatibility and route refresh

At planning time, each eligible attempt captures four logical snapshots:

- **Table identity:** stable `TableId`; table name and metadata version are not identity.
- **Target Region identity:** original `RegionId` and non-empty partition-expression version.
- **Replay policy:** target engine, table-managed append mode, instant-TTL state, effective primary-key encoding, and original Flow-mirror decision.
- **Request compatibility:** the minimum column metadata needed to preserve the meaning of this exact retained request, including present and omitted columns.

A rename preserves identity when `TableId` is unchanged; drop/create under the same name produces a different identity. Metadata and route versions are freshness signals, not rejection criteria by themselves. Missing planning metadata makes the call queue-ineligible.

Before authorization, replay validates two separate observations: one internally consistent route snapshot for the original Region, partition expression, peer set, leader state, and write policy; and one refreshed catalog observation for table identity, replay policy, and request compatibility. These are independently refreshed views, not an atomic cross-cache snapshot; each must satisfy its own freshness and consistency requirements. A confirmed mismatch is `stale_target`; stale, unavailable, incomplete, or conflicting observations defer with backoff. Replay never repartitions, rebuilds, or redirects the request. To avoid one catalog invalidation and lookup per queued record, refresh work for runnable records should be coalesced by refresh cycle: deduplicate table identities, refresh the required route view once for the batch, resolve the current fully qualified table names, invalidate the corresponding name-keyed table-cache entries, and reload each distinct table through that name-keyed, version-checked lookup path. Each record is still checked against its own captured identity and request-compatibility snapshot; an ID-only lookup must not bypass the name-keyed cache consistency path.

For explicitly present request columns, require the original name to resolve to the captured `ColumnId`, exact logical type and datatype extension, and the same semantic type when carried by the request. Retained NULL values require the current column to remain nullable; a nullability change is compatible when the retained values contain no NULL. A rename, replacement, type change (including widening), datatype-extension change, or semantic-type change is incompatible. v1 performs no coercion or request reconstruction. A default change does not affect a column whose value is explicit in the retained request.

Omitted columns require separate treatment according to whether they existed at planning:

- **Case A — existed at planning:** If the same name still resolves to the captured `ColumnId`, its synthesis semantics must remain unchanged. This includes nullability and the literal default value. Changing `D0` to `D1`, adding or removing a default, introducing an impure default, or making a `NOT NULL` column unsynthesizable is `stale_target`. The original RPC may already have stored a row using `D0` before an `ALTER`; replaying the still-omitted column after the metadata changes could write `D1` at a later sequence and change the visible value. An existing omitted column with an unchanged literal default or unchanged nullable/no-default behavior remains compatible. If the old name no longer resolves to the captured ID, treat the current same-name column as a post-planning column instead; an absent column needs no synthesis.
- **Case B — added after planning:** There is no plan-time value to preserve. The current column is compatible when the Datanode can synthesize it without an impure default: nullable with or without a literal default, or `NOT NULL` with a literal default. A new impure-default column or a `NOT NULL` column without a default is `stale_target`. This also applies to nullable columns introduced by schemaless ingestion after the original request was planned.

The precheck is request-specific, not a full schema snapshot or generic schema-diff mechanism. It requires a complete planning-time column-name-to-ID mapping and never infers `ColumnId` from schema position. Missing or incomplete planning metadata makes the call queue-ineligible; missing or ambiguous replay-time metadata defers as unverifiable. A metadata version change alone does not stale the record.

The replay-policy snapshot is also revalidated. The table must remain a supported Mito target with append mode disabled, instant-TTL disabled, and Dense encoding. Flow membership is not re-read: the captured decision remains authoritative, and replay does not run a second mirror pass.

The selected leader must belong to the peer set in the same route snapshot. An absent or `Downgrading` leader is temporarily unverifiable and is deferred. `IgnoreAllWrites` is a confirmed target-policy mismatch and settles as `stale_target`; v1 does not redirect to a merge destination. `Staging` alone does not invalidate the target; classify an actual Datanode rejection from its returned result. Failed refreshes or unresolved conflicts defer without sending while the deadline permits.

| Operation or change | Replay behavior | Residual risk |
|---|---|---|
| Client issues `TRUNCATE` after the first-round error | No data-generation marker or queue invalidation is introduced; the admitted record remains queue-owned | Delayed replay may reinsert rows after truncation. This post-error ordering is introduced by work continuing after the response, unlike the completed inline retry loop |
| `DROP` followed by create under the same name | Reject because the new table has a different `TableId` | The old request is never sent to the replacement table |
| `UNDROP` | Restored identity passes the captured identity checks | No drop-generation fence; pre-drop records may replay into the restored table |
| Repartition or partition-expression change | Stop when the captured target is no longer valid; do not redirect | The original request may remain unapplied, or an earlier unknown attempt may still apply |
| Route enters `IgnoreAllWrites` | Stop as `stale_target`; do not redirect to a merge destination | Original Region is no longer an allowed write target |
| Leader is absent or `Downgrading` | Defer until a usable leader is observed or the deadline expires | A prolonged transition may exhaust the deadline |
| Leader is `Staging` | Do not reject solely on this label; classify the actual Datanode result | Local staging policy may reject the write |

## Resource Limits and Operations

### Retained memory and record accounting

`write_queue.max_bytes` and `write_queue.max_records` are the v1 admission limits. `max_bytes` bounds the queue's estimated retained charge, not process RSS. `max_records` counts admitted records while either their request payload or caller-observable completion state remains retained. Before the first RPC, a candidate whose estimated retained charge exceeds `max_bytes` makes the whole call queue-ineligible because it could never fit in an empty queue. A candidate that fits individually may still be rejected after queue ownership is selected if the shared ledger lacks capacity; this rejection is fail-closed.

The record retains the original materialized per-Region request by shared ownership (`Arc<RegionRequest>`); the queue does not encode/decode it or create a second full payload copy. A typed estimate is computed once for each split Region request before the first RPC and reused at admission. The estimate covers retained request data, schema/column/value storage, dynamic string/bytes/JSON payloads supported by the request type, and bounded queue-owned metadata, provenance, send-authorized invocation history, and caller-observer state. It uses checked arithmetic but is not a generic allocator-tree walk and does not model every allocator header, runtime allocation, or transport buffer. If the estimate cannot be computed safely in preflight, the call remains on its existing retry path where supported.

Temporary capacity reservations count against the same Frontend-wide ledger as published records so concurrent admissions cannot overcommit `max_bytes` or `max_records`. The retained charge includes bounded history for send-authorized invocations and caller-observer state. Record publication and its charge/observer state are atomic; on allocation or pre-publication rejection, all temporary reservations are released exactly once. `max_records` includes completion-only entries until the caller detaches. v1 keeps no in-memory dead-letter archive and provides no dead-letter query API.

The retained estimate is an admission estimate, not an exact heap measurement or hard upper bound on Rust memory. Opaque gRPC/protobuf transport allocations, allocator overhead, and unrelated Frontend allocations remain outside the ledger. The queue does not create replay-side body copies or its own encode/decode buffers; any future queue-owned copy must be separately accounted for before allocation. Deployment validation must compare the estimate with measured retained memory and actual Frontend RSS at configured replay concurrency.

### Replay scheduling and fairness

Each Frontend queue has `write_queue.replay_concurrency` active replay-work slots. A slot covers pre-send metadata/route/leader/client preparation and, if authorized, the Region RPC future. A transient lookup failure, no-leader result, confirmed `not_submitted` result, or backoff wait defers the record and releases the active replay-work slot. The record retains a separate **Region FIFO head reservation** across deferral; at most one record per Region holds that reservation, and later records for that Region cannot overtake it. The head reservation is not a Frontend-wide replay-concurrency slot. Within a Region, records are ordered by successful admission-publication sequence, not client arrival, first-round dispatch order, or SQL row-input order. Runnable Regions are scheduled round-robin among those currently ready. A record waiting in backoff holds no Frontend-wide replay-work slot; when its backoff expires it must reacquire a slot before preparation resumes.

Round-robin scheduling does not provide bounded waiting time or per-record latency guarantees. A hot Region remains serialized, and its tail delay grows with backlog. v1 has no per-Region retained-capacity reservation, so one Region with a prolonged no-leader backlog can consume the Frontend-wide byte/record budget and cause admission rejection for otherwise healthy Regions. Slow metadata or route lookups also consume replay slots; if they occupy all configured slots, otherwise healthy Regions may temporarily be unable to start replay. v1 does not reserve a separate pool for RPC-ready work. All queue limits and replay concurrency are per Frontend, not cluster-wide.

### Observability

Expose Frontend-scope metrics with non-overlapping accounting semantics:

- **Preflight:** calls kept on their existing retry path, by a bounded reason such as unsupported path/shape/engine, append mode, instant TTL, Flow status, missing context, unavailable estimate, or estimate larger than `max_bytes`. These are not admission rejections.
- **Admission:** candidates admitted or rejected, with bounded reasons such as capacity, closure, invalid context, unadmittable first-round result, or explicitly fallible allocation/reservation failure.
- **Replay invocations:** authorized invocations by outcome (`applied`, `observed_retryable_failure`, `observed_terminal_failure`, `issue_unknown`, or `confirmed_connect_failure`). Pre-authorization lookup and leaderless deferrals are not invocations.
- **Replay-observed rows:** `write_queue_replayed_rows` counts `affected_rows` reported by replay responses that settle records as `applied`. It is an observation counter, not an exact logical-row count: an earlier `first_round_unknown` or replay `issue_unknown` invocation may also have applied.
- **Record settlement:** each record counted once at final settlement (`applied`, `issue_unknown`, `observed_failure`, `stale_target`, or `not_sent`), separately from invocation outcomes.
- **First-round outcomes:** `observed_retryable_failure`, `observed_terminal_failure`, `first_round_not_submitted`, and `first_round_unknown`, with separate `unadmittable` reason counts for missing hints, typed dispositions, or matching request/batch provenance.
- **Resource gauges:** retained bytes, retained record entries, and active replay-slot utilization. Confirmed connection failures are counted separately by first-round versus replay phase; this is an invocation diagnostic, not an attempt count.

Deadline expiry and attempt exhaustion are final give-up reasons, not invocation outcomes. Emit one bounded structured event for each non-`applied` terminal record with table/Region identity, first-round provenance, replay settlement, give-up reason, and potentially-submitted attempt count. Do not include request payloads or unbounded error text. Region/table identities belong in structured logs, not metric labels.

The cumulative `issue_unknown` counter is observational only; it does not measure remote operations still running. v1 has no process-lifetime invocation quota or automatic queue-wide cutoff. Sustained increases require a deployment-level alert and operator response; remote operations may outlive local records and accumulate across successive records.

## Configuration and Rollout

The following values are provisional discussion starting points, not validated production defaults. Queue settings use the `write_queue.` prefix and have an attempt budget independent of the legacy retry count.

| Parameter | Scope | Purpose | Initial value / status |
|---|---|---|---|
| `write_queue.enable` | Per Frontend | Enable queue ownership for supported distributed insert paths | `false` by default |
| `write_queue.max_bytes` | Per Frontend | Estimated retained charge for payload, metadata, history, and attached completion state | 512 MiB; requires workload validation |
| `write_queue.max_records` | Per Frontend | Maximum retained records, including completion-only entries | 100000; requires workload validation |
| `write_queue.retry_budget` | Per record | Absolute replay window from admission through scheduling and replay | 5m; increase or disable when the workload envelope does not fit |
| `write_queue.max_attempts` | Per record | Maximum potentially-submitted replay attempts after the first-round request | 3; confirmed `not_submitted` outcomes do not consume this budget |
| `write_queue.completion_wait` | Per client request | Maximum caller wait after admission; `0` means immediate snapshot | 30s |
| `write_queue.replay_rpc_timeout` | Per invocation | Local timeout for an authorized replay RPC, including client readiness | 10s |
| `write_queue.replay_concurrency` | Per Frontend | Active replay-work slots, including preparation and RPC execution | 8 |
| `write_retry.max_retries` | Legacy path only | Retry rounds for calls not owned by the queue | 3 by default; `0` is compatible with queue-owned calls |
| `write_retry.initial_backoff` | Shared backoff settings | Initial replay scheduling delay | Existing setting reused |
| `write_retry.max_backoff` | Shared backoff settings | Base-delay cap before positive jitter | Existing setting reused; final wait may approach twice the cap |
| `write_retry.backoff_multiplier` | Shared backoff settings | Exponential backoff multiplier | Existing setting reused |

`H_min` is a workload-qualification value, not a runtime setting. It is the minimum deadline reserve at the first potentially-submitted replay attempt for one full RPC timeout, measured result/settlement overhead, and a safety margin: `H_min = replay_rpc_timeout + T_result_settlement,qualified + safety_margin`. A second attempt requires additional reserve for the maximum scheduled backoff (including positive jitter), another RPC timeout, and its result/settlement overhead. The per-record clock starts at publication: pre-send preparation, route/metadata/leader lookup, and backoff after publication consume `retry_budget`. First-round collection and preflight happen before publication and do not consume an individual record's retry budget; they still count toward end-to-end request latency and must be measured separately.

When `write_queue.enable = true`, startup validation rejects zero or invalid values for `max_bytes`, `max_records`, `retry_budget`, `max_attempts`, `replay_rpc_timeout`, and `replay_concurrency`; `completion_wait = 0` remains valid. Backoff settings must have a positive initial delay, `max_backoff >= initial_backoff`, and a finite multiplier of at least 1. Arithmetic must use checked or saturating operations and must never schedule beyond the record deadline. Queue-only runtime range checks are dormant when the queue is disabled; configuration syntax and type parsing still apply. `write_retry.max_retries` controls only calls left on the legacy path and does not set the queue's attempt budget. If it is zero, preflight-ineligible calls receive no legacy retry rounds, as they do when the queue is disabled.

Configuration is loaded at startup. The switch applies to every queue-eligible direct insert on that Frontend; v1 has no table/database allowlist or hot reload. Workloads needing different DDL or retry behavior require separate Frontend pools. Disabling the queue requires a restart, and restart may discard retained records.

### Rollout and operational guardrails

- Roll out by distributed Frontend pool, starting with a dedicated canary. Workloads requiring strict `TRUNCATE`/DDL or same-key ordering, Flow changes that must cover delayed writes, or intolerance for late replay must use a queue-disabled pool.
- During canary, monitor retained bytes/records, preflight exclusions, admission rejections, time to first potentially-submitted replay attempt, confirmed connection-failure rate by phase, replay-slot utilization, healthy-Region progress during peer outages, deadline expiry, hottest-Region depth, stale-target settlements, timeouts, and unknown-outcome counts.
- Set alert thresholds for sustained `issue_unknown` growth and confirmed connection attempts per second during a shared-peer outage. Qualify the attempt-rate threshold using the deployed backoff parameters; lowering backoff invalidates the qualification. If a threshold is exceeded, stop expansion, remove the pool from write traffic, restart it with the queue disabled, and only then restore traffic. This operator-driven rollback may discard pending in-memory records; it does not cancel remote work already accepted by a Datanode.

Queue enablement is blocked until all of the following are true:

- The first-round report preserves original requests, per-entry results, deterministic ordinals, typed submission disposition, and peer-batch provenance, including when `write_retry.max_retries = 0`.
- Every target Datanode supports per-entry partial results; mixed-version operation is excluded.
- The exact connection-failure exception is attached before error conversion, survives client-to-Frontend conversions, and is granted only for the verified connection-error type at the intended RPC boundary. Generic statuses, error text, and client-readiness failures must fail closed.
- Replay obtains a consistent route snapshot and a refreshed catalog observation that verifies table identity, request compatibility, replay policy, and the same Region target.
- The original Flow-mirror decision is preserved; missing or ambiguous Flow context makes the call queue-ineligible, and replay performs no second Flow lookup.
- Integration, failure-injection, compatibility, and resource tests pass. Maintainer-approved performance thresholds are recorded before benchmarking; measurements cover successful-path CPU per row, throughput, latency percentiles, the same injected failure workload, and Datanode backlog/RSS under repeated replay timeouts.
- The measured workload fits the declared backlog/skew envelope, each eligible record reaches its first potentially-submitted replay attempt with the required deadline reserve, and operators accept the documented duplicate-write, DDL, Flow-change, and unresolved-remote-operation risks.

Upgrade all target Datanodes before enabling the queue and keep it disabled during mixed-version operation. Disable queue-enabled Frontends before downgrading any target Datanode. Workloads that require Flow changes after planning to cover delayed writes must remain queue-disabled until a Flow-generation fence or equivalent coordination exists.

A future change to the shipped default requires a separate maintainer decision after integration and client-contract tests, failure-injection coverage, same-Region/DDL validation, representative resource and latency measurements, canary results, and explicit acceptance of the remaining risks. Enabling the queue for one deployment does not by itself justify changing the shipped default.

## Drawbacks

- **Additional retained memory:** `write_queue.max_bytes` (512 MiB as a provisional starting value) caps the queue's estimated retained charge, not total Frontend memory. Transport buffers, allocator overhead, and unrelated Frontend allocations remain outside this limit, so actual RSS can exceed the configured ledger budget.
- **Successful-path overhead:** when queue mode is enabled, every eligible split Region request is traversed once by the typed retained-charge estimator before the first RPC, including requests whose first-round write succeeds and is never queued. The success path must reuse plan-time metadata and the existing materialized request; it must not perform queue admission, add route/catalog lookups, encode/decode the request, or contend on the queue ledger for successful Regions. The estimator is the intentional extra CPU work and must be measured.
- **A bounded client wait can add latency:** after the first-round report and all admission decisions are complete, the caller may wait up to `write_queue.completion_wait` (30s initially) for replay settlement. This can turn a short transient failure into an observed success, but also delays an error response and does not cancel queue-owned work when the wait expires.
- **One Frontend-wide operational policy:** the queue has shared Frontend-wide capacity and concurrency, no per-table allowlist, no per-Region retained-capacity reservation, and no hot reload. Workloads with different DDL-ordering or retry requirements may need separate Frontend pools.
- **Fail-closed admission trades retry continuity for a single owner:** once a call is queue-owned, a later capacity or queue-closure rejection receives no inline fallback. This avoids running two retry schedulers for the same write, but can forgo a retry opportunity that the legacy path might otherwise have provided.

## Risks and Limitations

- **Duplicate-capable retry and cumulative unknown outcomes.** Lost responses, post-connection failures, timeouts, worker failures after authorization, and client retries can cause duplicate writes. A later success does not prove an earlier unknown invocation was not applied. `max_attempts` counts every potentially-submitted replay attempt; only the exact typed `not_submitted` outcome is excluded. `replay_concurrency`, `max_bytes`, and `max_records` bound local active work and retained state, but do not bound remote operations that continue after local timeouts or the lifetime total of confirmed connection failures. One peer-batch expansion into `N` Region records can commit up to `N × max_attempts` replay attempts. v1 has no aggregate replay-rate ceiling or process-lifetime invocation quota; unresolved remote work and repeated connection failures may accumulate across successive records.
- **Partial batch outcome and unknown-batch expansion.** A batch may return an error because one sibling has a terminal or unadmittable outcome while retryable or confirmed `not_submitted` siblings are admitted and may apply later. An unknown peer-batch failure may expand into many independent `first_round_unknown` records at once; the shared byte/record ledger and replay-concurrency cap bound local retention and active work, but the burst can increase queue occupancy and delay other ready Regions. v1 has no separate per-batch burst limiter. The client can observe a failed batch whose retryable subset is partially applied; admitted records may also apply after the client wait expires. A batch error does not mean that no rows were written.
- **Connection-phase classification is deliberately narrow.** The non-submission exception depends on a typed marker attached before error conversion and preserved with request/batch provenance. It is not inferred from a generic status, transport category, or error text. If the client library changes or an error conversion loses the marker, the queue must fail closed and use the ordinary retry-hint rule. All other authorized outcomes remain potentially submitted and duplicate-capable.
- **No inline fallback for queue-owned rejection.** A queue-owned attempt rejected by capacity, queue closure, or an invalid admission sidecar receives no Frontend retry after the first round. Preflight-ineligible calls retain their existing retry owner where supported.
- **Worker-failure loss.** Shutdown, deadline expiry, or a replay worker failure can terminate admitted work before the retry deadline or `write_queue.max_attempts` is exhausted. A worker failure finalizes and removes its record from the Region FIFO, releases the claim and slot, and does not automatically reschedule that record; the next same-Region record may proceed. Remote operations from timed-out invocations may nevertheless still be executing or may apply later.
- **Repartition recovery is out of scope.** If the original Region disappears or the partition-expression version changes, the record settles as `stale_target` and is not rebuilt or redirected. A write may therefore remain unapplied after a repartition even when the current routing could identify new destination Regions. v1 intentionally does not provide cross-Region recovery.
- **Delayed replay can be overtaken by live writes.** Per-Region FIFO orders only admitted queue records; it does not serialize replay with concurrent live writes or legacy-inline attempts. An earlier write may return a definitive retryable failure and be admitted, while a later live write to the same Region succeeds before the earlier record is replayed. If both affect the same key, the older request may be submitted to storage after the newer one, so the final conflict result depends on the engine's write/version semantics; this does not require an `issue_unknown` first-round outcome. Separately, an earlier invocation classified `issue_unknown` may still apply after a later operation. v1 has no Datanode-side sequencing fence. Workloads that require per-Region or same-key ordering across delayed replay and live writes must not enable this queue; a queue-disabled pool avoids this background-replay behavior but does not itself provide a general write-ordering guarantee.
- **DDL stale-replay window.** Replay checks the captured request-compatibility information against refreshed metadata but has no atomic DDL fence. An `ALTER` may still race between validation and RPC submission, and metadata propagation across Frontends is asynchronous. A default-synthesis change to a planning-existing omitted column is rejected as `stale_target`, even if that particular first-round RPC had not applied; the queue cannot safely distinguish it from a first-round invocation that stored the old default. A genuinely new post-planning fillable column remains compatible. `TRUNCATE` has no data-generation marker, so delayed replay may reinsert rows after truncation. `DROP` followed by create under the same name is rejected by the changed table identity. In contrast, `UNDROP` restores the original table identity and route without a drop-generation fence; pre-drop records therefore pass the captured identity checks and may be replayed into the restored table. Workloads that require `TRUNCATE` or `DROP`/`UNDROP` to fence older writes must remain queue-disabled.
- **Flow changes after planning.** Flow-source writes that already have a Flow at planning time are preflight-ineligible. However, a request captured with a non-Flow decision may be admitted before a Flow is created or its definition changes. Replay does not re-evaluate Flow membership, so the delayed source write is not mirrored according to the newer definition. v1 has no Flow-generation fence or coordination with Flow DDL. Workloads that require Flow changes to cover delayed writes must remain queue-disabled until such coordination exists.
- **Retained-capacity concentration.** Retained limits are Frontend-wide and v1 has no per-Region or per-record reservation. One large record may consume most of `max_bytes`, and one Region with a prolonged no-leader backlog can consume the shared byte/record budget and cause admission rejection for healthy Regions. A record whose individual estimate exceeds the entire `max_bytes` budget is rejected before the first RPC; otherwise, admission fairness across Regions is not guaranteed. Completion-only entries remain counted against `max_records` until the caller detaches, even after their request payload has been released. A long configured `completion_wait` or delayed caller detachment can therefore exhaust record slots and reject new admissions while the queue retains little or no replay payload. Round-robin scheduling does not prevent admission-time capacity starvation.
- **Resource accounting is not an RSS bound.** The retained ledger uses a typed estimate for queue-owned request data and bounded metadata, but it is not an exact heap measurement and does not account for every Frontend allocation or total process memory. Completion cells are bounded by retained record entries and charged to them. Opaque gRPC/protobuf transport allocations are outside the queue ledger and must be measured at the configured replay concurrency.
- **Replay-slot starvation during slow preparation.** `replay_concurrency` includes metadata, route, leader, and client preparation as well as RPC execution. Slow or stuck lookups can occupy every slot before an RPC is sent, temporarily preventing otherwise healthy Regions from starting replay. A confirmed `not_submitted` connection failure is different: it must release its active replay-work slot as soon as the typed result is observed, while retaining only the Region FIFO head reservation during backoff. Because this slot is reusable and is not a rate limit, many short connection failures across distinct Region heads can still produce a high aggregate connection-attempt rate during an outage. v1 does not reserve separate lookup and RPC slot pools or impose a process-wide connection-attempt rate ceiling; this behavior must be included in deployment validation and canary alerting.
- **Remote ingest work can outlive a replay timeout.** The local `replay_rpc_timeout` bounds the Frontend's wait and releases its replay-work slot when the future is dropped; it is not a Datanode-side cancellation or concurrency limit. Datanode-side ingest work may continue independently after the Frontend stops waiting, and Mito may retain a stalled write request internally. If the client stops waiting, that server-side task may continue after the Frontend has recorded `issue_unknown` and started a later replay invocation. Repeated timeouts for one Region can therefore leave multiple remote tasks or stalled requests alive even though the Frontend has at most one locally active queue record for that Region. Existing Datanode write-buffer pressure/rejection remains the server-side protection; v1 does not add a remote-operation fence or deduplication. This behavior must be measured under sustained stalls and repeated timeout/replay cycles before enablement.
- **Peer-batch unknown burst.** One failed peer-batch RPC with an attributable retryable hint can expand into `N` independent `first_round_unknown` Region records. Each record has its own `max_attempts` budget, so that one expansion cohort can commit at most `N × write_queue.max_attempts` potentially submitted replay attempts; confirmed `not_submitted` invocations consume deadline/backoff but not this budget. There is no shared batch-level attempt budget or per-batch burst limiter. The shared retained-byte/record limits and `replay_concurrency` cap simultaneous local retention and active Frontend work, but they do not bound server-side tasks that continue after a replay timeout, isolate the batch from other records, or prevent a burst from increasing queue depth and delaying other ready Regions. The supported workload envelope must include the largest peer-batch fan-out expected in production.
- **Drain-time uncertainty.** Per-Region serialization means global replay concurrency cannot shorten the tail of a single hot Region. Records near the end of a backlog may expire before the first send-authorized replay invocation. The proposed 5m retry budget and configured retention limits do not establish drain capacity; the supported backlog must be defined by the workload-envelope benchmark.
- **Unbounded first-round wait and call-level head-of-line blocking.** A prepare/no-leader error before any write RPC is a call-level pre-dispatch error and creates no queue record. A first-round RPC that never returns can keep the client request pending indefinitely; queue ownership begins only after a complete first-round report, so this queue does not resolve that wait. Because the current dispatch collection waits for all first-round Region/peer RPCs before constructing the report, one hung Region can also delay admission of other Regions whose failures have already returned. v1 does not stream partial reports into the queue; this preserves one call-level retry owner and all-or-nothing preflight, but leaves the entire call subject to its slowest first-round RPC. Pre-publication cancellation and admission handoff follow the ownership rules in the first-round capture section.

## Validation Plan

### First-round capture and client contract

- Verify whole-call preflight selection, queue ownership before the first RPC, no inline fallback after ownership, and unchanged behavior for preflight-ineligible calls.
- Cover per-entry success/failure, explicit terminal failures, collapsed single-entry errors, whole-peer-batch failures, mixed outcomes, missing provenance, deterministic Region ordinals, and partial admission. Verify that a non-retryable terminal result is never admitted, while retryable siblings may continue independently. A peer-batch failure may expand only when every original request and its mapping are retained.
- Verify that the queue-owned multi-entry insert path requests per-entry partial results even when `write_retry.max_retries = 0`, and that queue enablement is blocked unless every target Datanode supports the contract. Verify the lazy-channel behavior: `ready()` may return `Ok`, while a refused connection surfaces from `unary()` with the exact `ConnectError` in the raw status source chain. Verify that the typed `not_submitted` disposition is attached only in that `unary()` error branch before normalization, survives all error conversions with matching request/batch provenance, and is not assigned to a `ready()` failure, generic statuses, unrelated error sources, or message text.
- Verify first-round `not_submitted` admission without a normal retry hint; confirm it consumes deadline/backoff but not replay attempts. Run the same case during replay and verify immediate active-slot release with the Region FIFO head retained.
- Verify request capture and per-entry results when `write_retry.max_retries = 0`; confirm public error status and retry-hint behavior remain compatible for calls that stay on the legacy path.
- Exercise caller cancellation while first-round `join_all` is still collecting results, cancellation after the complete report but before publication, cancellation after publication, background dispatch abort before report completion, completion racing with wait registration, partial admission, `completion_wait = 0`, and completion-wait expiry. Verify that no partial first-round report is handed off.

### Replay lifecycle and failure handling

- Cover retryable and terminal results, explicit route-refresh hints, confirmed connection failures, transient route/metadata failures, leaderless deferral, replay timeout, attempt exhaustion, and orderly shutdown. Hold route, metadata, and leader lookups open until the record deadline expires; verify that no RPC is authorized and the Region FIFO head and active slot are released exactly once.
- Compare a fast Datanode rejection with a Mito write-buffer stall. Confirm the stall becomes `issue_unknown` after the local timeout and that a later invocation can overlap with still-running Datanode work.
- Exercise repeated short confirmed connection failures across many Regions. Measure connection attempts per second, active-slot release/reuse, healthy-Region progress, and Frontend CPU during a shared-peer outage. Verify that a later successful connection can still use the remaining attempt budget.
- Inject per-record worker failure before and after send authorization, and coordinator failure separately. A pre-authorization failure creates no invocation; an authorized failure without a result settles as `issue_unknown`. During shutdown, cover an authorized RPC that returns `applied`, one that returns typed `not_submitted` (no attempt charged and no retry after close), and one that reaches `replay_rpc_timeout` without a result. Verify that unrelated Regions continue after a record failure; coordinator failure closes admission, joins active tasks within their existing timeouts, settles records from history, notifies attached callers, releases resources exactly once, and does not auto-restart.
- Verify that local timeout or shutdown does not claim to cancel remote work already accepted by a Datanode. Measure lingering ingest tasks, stalled requests, write-buffer pressure, and RSS through repeated timeouts and until the system returns to baseline.

### Metadata compatibility and replay safety

- Verify that planning with a missing or incomplete name-to-`ColumnId` mapping keeps the whole call on its existing path, and that missing or incomplete replay-time mapping defers without sending. Verify that replay uses the original shared request and Region identity, one internally consistent route snapshot, and a separately refreshed catalog observation. Verify that refreshes for multiple ready records are coalesced by table identity and preserve the name-keyed, version-checked table-cache path. Compatible targets proceed; confirmed mismatches stop as `stale_target`; stale, incomplete, unavailable, or conflicting observations defer without sending.
- Cover explicit columns: rename/replacement, a same-name/same-type replacement with a different `ColumnId` (which the queue must reject even if the Datanode would accept by name), logical type, datatype extension, semantic type, nullability with retained NULLs, and default changes when values are explicit. Also verify that nullability changes remain compatible when retained values contain no NULL and that a renamed column does not make the old request name valid.
- Cover omitted-column Case A and Case B separately. For Case A, unchanged synthesis remains compatible, while changing a literal default (`D0` to `D1`), adding/removing a default, introducing an impure default, or making the column non-fillable stops replay. For Case B, allow nullable columns with or without literal defaults and `NOT NULL` columns with literal defaults; reject impure defaults and `NOT NULL` columns without defaults. A dropped-and-readded same-name column is evaluated as Case B when it has a new identity.
- Verify that nullable columns introduced by schemaless ingestion after planning remain compatible, while existing omitted columns whose defaults change do not. Include the ambiguous case where the first request may already have applied using `D0` before the schema changes to `D1`.
- Verify stable `TableId` across rename and unrelated option changes; reject drop/create under the same name; document that `UNDROP` can restore the same identity and that v1 has no drop-generation fence. Verify that `TRUNCATE` has no data-generation fence and may be followed by a delayed replay.
- Verify Dense-only admission, absent request hints defaulting to Dense, and exclusion of Sparse targets/requests. Verify that impure defaults are materialized once before routing and the original values are replayed without re-evaluation or repartitioning; reject a missing required materialized impure-default value before publication and treat any post-publication absence only as a defensive consistency failure.
- Verify Flow-source exclusion and the original ordering: the detached Flow mirror work is submitted before source Region dispatch, is not awaited or resubmitted by replay, and the plan-time decision is reused without a second Flow lookup. Verify the missed-mirror risk when Flow is created or changed after a non-Flow decision was captured. Verify that repartitioning never redirects a retained request to a successor Region.

### Resource, performance, and rollout qualification

- Verify retained-charge accounting and checked arithmetic across null-heavy rows, short strings, large string/bytes values, and supported nested payloads; test concurrent reservations, bounded history, completion-only record accounting, and exactly-once release on every terminal path. Compare estimated retained memory with measured memory and RSS.
- Exercise maximum peer-batch fan-out and the resulting `N × max_attempts` cohort bound, sustained record turnover, hot-Region backlog, leaderless periods, shared capacity exhaustion, slow metadata preparation, and fairness across runnable Regions. Confirm that a Region request whose estimate exceeds `max_bytes` remains on the existing path before the first RPC, while a later shared-capacity rejection after queue ownership receives no inline fallback.
- Benchmark queue-enabled and queue-disabled success paths under identical load: CPU per row, throughput, p50/p99/p999 latency, and failure-path behavior. Record maintainer-approved acceptance thresholds before benchmarking rather than defining a universal percentage in this RFC.
- Qualify the declared workload envelope and require each eligible record to reach its first potentially-submitted replay attempt with at least `H_min` remaining. Run a canary within that envelope and verify rollback behavior, including the possibility that restart discards pending in-memory records.

## Alternatives and Future Work

A post-existing-retry buffer is not used because it would add a second scheduler and retry budget to the same write. v1 selects one retry owner before the first RPC; queue admission begins only from a lossless first-round report. Durable retention and cross-Region repartition recovery require separate RFCs.
