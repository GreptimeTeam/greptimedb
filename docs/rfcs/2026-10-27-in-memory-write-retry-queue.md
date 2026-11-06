---
Feature Name: Frontend Write Retry Queue (in-memory v1)
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
---

# Summary

Add a bounded retry queue in the frontend for failed writes that can still be
replayed after in-process retries are exhausted. It hooks `WriteDispatcher`'s
give-up points, encodes each already-split per-region request once into a
record (payload bytes plus the row count and table version), and parks it
behind a three-operation `QueueStore` (`append`, `lease`, `complete`). v1 keeps
records in an in-memory ordered map that dies with the frontend; the payload
layout lets the durable phase add its framing.

Memory is bounded exactly for record bytes. The request path computes the size
without allocating (`RegionRequest::encoded_len()`), reserves it in one ledger
whose check and charge happen under a single lock, and only then encodes into
the reserved buffer; a burst cannot allocate ahead of the budget. Replay holds
a permit per in-flight record, which bounds the decoded copy, the transport
buffer and the rebuild split.

The queue is compensation, not an acknowledgment: the client still gets the
original error and `affected_rows` keeps its applied meaning. It is off by
default, because a client that retries can duplicate the replayed write. Replay
is per-region FIFO and at-least-once; TTL, attempts, failed DDL checks and
process exit are explicit drops.

A queued record is checked at replay against existing frontend metadata: the
table exists, its `TableIdent.version` is unchanged, and the record can still be
placed on the current route. `TRUNCATE` and `UNDROP` move none of those signals,
so v1 cannot detect them; the checks, a short TTL and DDL fencing bound the
window, and closing it needs the region-side fence (future work). v1 adds no
proto field, no manifest field and no region path change.

# Motivation

`WriteDispatcher` splits a write per region and retries retryable failures with
jittered backoff: three additional attempts, starting at 1.5s and capped at 10s.
That is right for a brief outage, but a datanode restart, a region migration or
a partition can take longer, and only the client can retry today. A client has
no per-region view: it either replays the whole statement, duplicating the
regions that succeeded, drops the failed ones, or has exhausted its own budget.
Issue #8085 asks for a bounded frontend-side buffer that replays asynchronously.

The queue cannot make the client see success: a queued write has not been
routed, is invisible to reads until replay, and in v1 lives only in one
frontend's memory, so v1 keeps the error response and gets the core right first:
admission, ordering, replay and exact record-byte accounting.

# Goals

- A failed write that can still be replayed is enqueued best effort before the
  frontend returns the error, even beside a terminal sibling, and replayed later.
- The response does not change: `affected_rows` keeps its applied meaning and a
  queued write is never reported as success.
- Per-region FIFO replay, one in-flight record per region.
- Exact bounds on bytes, record count, record size, age and attempts; the queue
  sheds rather than waits, no record is allocated before its size is reserved,
  every release path releases what it charged, and neither writer nor replay
  blocks a query worker.
- Non-retryable records and records past the age or attempt bound leave as
  observable dead letters; admission rejects are counted sheds.
- No behaviour change when disabled, no new error classification, no new
  persisted or wire format; the encoded payload is the stable format and
  `QueueStore` is the replaceable seam.

# Non-goals

- Surviving a frontend restart, a pod reschedule or node loss: records live in
  process memory and die with the process; durable recovery is the later phase.
- Exactly-once delivery; a record without a client idempotency key can duplicate
  rows. v1 has no idempotency key, so a table with `append_mode = true` is shed
  at admission: deduplication cannot hide a duplicate there.
- Returning success for a queued write, or changing `affected_rows` from applied
  to accepted (see The `affected_rows` contract).
- Bulk inserts, including every write the pending-rows batcher flushes as a
  `BulkInsert` request: their row count lives inside the encoded body rather
  than in a protobuf field, so they need their own accounting first.
- Fencing `TRUNCATE` and `DROP` followed by `UNDROP`; v1 documents the window.
- Standalone mode, which rejects `enable = true` during option validation rather
  than silently ignoring the section.
- Writes outside the retry and give-up path (such as the enterprise Flow child
  path) and tables that feed a flow. `Inserter::do_request` spawns
  `FlowMirrorTask` before `dispatch`, so the mirror pass runs against the live
  request; a replayed record arrives after that pass and is never mirrored
  again, so the Flow sink misses exactly the replayed rows (a flow created
  after enqueue has nothing to replay into either). No region rejects that late
  write, so admission and replay both check the table's flow set and shed it;
  the set is already a frontend cache hit on the insert path.
- Global ordering, ordering against newer live writes, a query API for dead
  letters, and any backend other than the in-memory store.

# Details

## Where the queue sits

The queue hooks the dispatcher's give-up points: the merge branch of
`WriteDispatcher::finish` when `should_retry` is false, and the retry loop when
it exhausts or a terminal sibling ends a later round. By then the write is split
per region and each error carries a `RetryHint`. A terminal error is what the
client is told, but it does not suppress a retryable sibling: a failed write is
queued even when another in the statement cannot.

Client cancellation is expected, not an error path. A request is queued only if
it reaches a give-up point uncancelled: a client that times out or disconnects
drops the request future, and a dropped future is not enqueued. The handoff
re-checks cancellation before each reservation, so a disconnect during the retry
loop leaves nothing behind; a future dropped while holding a reservation gives
it back through the same `LedgerGuard`. The default retry window is 10.5–21s:
three backoffs of 1.5s, 3s and 6s, each extended by up to its own length of
jitter, plus RPC time. Only a caller that outlives it is ever queued.

Admission has one decision point, so "was this write queued?" has a single
answer: the ledger's check-and-charge is the linearization point for every
bound, and a record is queued if and only if it holds a ledger charge. The
durable phase moves that point to the durable commit; the invariant does not
change.

## Admission and memory accounting

During a cluster-wide outage the failure path is the request path: every write
that exhausts its retries arrives here once per region. The handoff does no new
I/O on a cache hit and allocates nothing before admission; failed regions are
grouped per table once per batch, so catalog and route reads happen once.

1. The request path reduces the header to `dbname` and `tracing_context` and
   computes the payload length with `RegionRequest::encoded_len()`, a prost
   field walk that allocates nothing. The ledger reserves that payload length;
   the record's two `u64` fields are counted in the per-record bookkeeping.
2. It reserves that size in `Mutex<Ledger { pending_bytes, pending_records }>`.
   Both checks and both charges happen in one critical section with no `await`,
   so a burst cannot slip records past a bound the way separate atomic counters
   can: a rollback-based atomic would overshoot by up to concurrent admissions ×
   `max_record_size`, not a few kilobytes; no `await` inside bounds contenders
   by worker threads, not in-flight requests. An oversized record or a crossing
   reservation is shed and counted at once, before anything is allocated.
3. The reservation then allows one buffer of `payload_len` bytes for the
   encoded payload. `encoded_len()` is exact for that encoding, so the buffer
   is never reallocated; it becomes the stored payload, with no second copy.
4. The record moves into the writer's channel, which is unbounded and has no
   budget of its own: every message in it is already charged.
5. The charge is released exactly once when the record completes, is discarded
   or expires. Count and bytes move together under one lock; the state machine
   removes the record under the state lock before releasing, so completion and
   TTL sweeping cannot release it twice. The charge is held by a `LedgerGuard`
   and released on its `Drop`; the guard is disarmed only after the write into
   the writer's channel succeeds, when the record takes ownership of the charge.

Admission also sheds a record for a table with `append_mode = true`: v1 has no
idempotency key and such a table keeps a duplicate as a second row, so a replay
would change query results. The option is a cache hit in the same metadata the
DDL checks read.

Per-record bookkeeping is a constant pinned by a test and bounded by
`max_records`, not by `max_bytes`; it includes each record's two `u64` fields,
its `Vec` header and allocator metadata:

```text
pending record bytes + bookkeeping        <= max_bytes + max_records * overhead
in-flight replay decode and send buffers  <= replay_concurrency * 64 * max_record_size
```

Replay decodes, and decoding allocates. A lease hands out the stored record
without copying its payload. The worker holds one of `replay_concurrency`
permits from before decoding until the send finishes; per record the transient
footprint is at most `REPLAY_TRANSIENT_FACTOR × max_record_size`, a constant of
64, not 3. The worst shape is a single column whose every row holds one null:
four wire bytes per row (two for the outer `rows` field, two for the empty
`Value` message), against a 24-byte `Row` slot plus one `Value` in the inner
heap. `Row` is itself the `Vec<Value>` header; `Value` is
`Option<value::ValueData>` at 40 bytes, because `ValueData`'s largest variant
is a 32-byte `JsonValue` payload plus its discriminant. One value still
allocates a fresh `Vec`, whose first allocation is `RawVec`'s minimum of four
elements, so the inner heap is 160 bytes; the outer `Vec<Row>` can double once
more, so a row costs at most 2 × 24 + 160 = 208 bytes — a factor of 52 over its
four wire bytes. Records with several nulls per row amortize the 160-byte floor
and approach 20; short strings, `ListValue`, `StructValue` and `JsonValue`
expand along the same lines but do not beat the single-null shape. The factor
also covers the tonic transport re-encode and the `rebuild_write_requests`
split; 64 leaves about 20% over the 53× worst case (52 decoded plus one
transport copy) for allocator size classes and the decoded request's fixed
fields. A test decodes generated row shapes — null-heavy, short-string-heavy
and nested — and recursively sums the decoded containers' capacities to assert
it. It is not a knob, because a knob would let a deployment weaken the bound
the test pins, and a permit wait delays a record instead of dead-lettering it.

A record that does not fit is shed; the client gets the same error as without
the queue. The queue never waits for room and never evicts: rejecting equals
having no queue, while eviction would tie contents to the storm's order.

## Failure-path budget

The success path is untouched: the hook is only at the give-up points, so a
write that succeeds never calls `encoded_len`, takes the ledger or touches the
queue or the store. A test asserts zero queue calls and record-buffer
allocations.

The failure path is still the request path, so the added cost per failed batch
is explicit:

| Step | Scope | Cost |
| --- | --- | --- |
| Grouping by table | once per failed batch | a `HashMap` and `Vec` sized by the number of failed regions |
| Catalog and route read | once per table per failed batch, shared by all of that table's failed regions | cache hit on the common path; a miss is a metadata read after the write has already failed |
| `RegionRequest::encoded_len()` | per failed region | a prost field walk, no allocation |
| Ledger check and charge | per failed region | one short `Mutex` critical section, integer arithmetic only |
| Allocate and encode the record | per failed region | one allocation of the reserved length and one encode of the payload |
| Channel send | per failed region | one unbounded channel send; the entry is already charged |
| Writer drain | per batch | one state lock and a memory append |

There is no store call, RPC, blocking work or task spawn on the request path,
so a failed request cannot be parked behind the queue; a cache miss falls back
to the metadata store exactly as the live path does. The writer's drain and
completion are memory operations, so the only added serialization is the state
and ledger locks, neither able to stall a worker.

The handoff must cost less than the failed attempt: one encode, one channel
send, no round trip, no store. The code PR measures, with every region returning
`StorageUnavailable`: handoff p50/p99, added CPU per failed region, and
end-to-end failure-path p99. Acceptance: no allocation before admission, zero
queue calls and record-buffer allocations on the success path, failure-path p99
regression within 5%, and ledger lock cost at 1, 8 and 64 threads.

## Storage

The queue is a new base-layer crate, `common-retry-queue`, defining a
`QueueStore` trait as an ordered map from a queue key to opaque records, so the
crate does not know the write protocol. v1 needs three operations:

- `append(records)`: the writer drain; the store appends them in order.
- `lease(key) -> Option<Record>`: the oldest unconfirmed record of a key; the
  state machine marks it in flight.
- `complete(key)`: drop the leased head.

The state machine keeps the per-key logical state in memory (in-flight flag,
attempt counter, retry deadline, TTL); the store keeps each key's records in
append order. `lease` suffices because a key has at most one in-flight record
and completion is always a prefix. Shutdown is task lifecycle, so v1 has no
`stop`; the durable phase adds `list_keys`, `latest`, `sync`, `create` and
`delete` behind the same seam.

`MemoryQueueStore` keeps encoded bytes, not decoded requests: `max_bytes` stays
exact, the crate stays protocol-independent, and a durable adapter persists the
same bytes; the cost is one encode and one decode.

The key is `RegionId::as_u64()`. A record is a struct, not a self-describing
envelope:

```text
rows u64 | table_version u64 | payload (the `RegionRequest` protobuf)
```

`payload` is the protobuf encoding of the `RegionRequest`; `rows` serves the
dead-letter metric without decoding it. `table_version` must be captured while
the insert plan is built — `Inserter::do_request` already holds the `TableInfo`
for every table in the batch — and carried with the request next to the
payload. It must not be read when the record is enqueued or handed off:
`reroute_failed_writes` refreshes the table cache during retries, so an
enqueue-time read can already be post-ALTER and would pass against itself,
letting a payload built for the old schema through while the region fills
defaults. v1 stores no route version and no wall-clock enqueue time: a leader
change bumps the route version, so replayability is decided by whether the
source region is still in the current route. Age is monotonic.

There are no ack records: completion is always a prefix. v1 adds no framing —
the key is the map key, the length is in the struct, no reader sees a record it
did not write; the durable phase adds a version and length.

Per key the queue keeps a metadata deque. One mutex serializes writer
insertion, leasing and completion, and only the writer and replayer take it, so
it never delays a request. A lease is the head record that is not in flight and
has not expired; `complete` drops the leased head from the store and the deque
and releases its ledger charge; the dead-letter path calls it and reports a dead
letter. TTL is checked at lease time and by a periodic sweep of each key's head;
age is monotonic, so a clock step cannot expire it early.

## Replay

The replay worker lives in `operator` next to `WriteDispatcher` for its routing
and partition machinery; the queue crate owns admission accounting, storage and
leasing. It keeps one in-flight record per key, leases the head of an idle key,
applies the table checks, takes a replay permit, decodes, resolves the route and
leader, applies the route check, and sends through the live path's client.

- Success: complete the record and lease the next one.
- `Retryable`: increment attempts, back off with jitter from about one second,
  release the permit and leave the record at the head.
- `RetryAfterRouteRefresh`: increment attempts, invalidate the route caches and
  resolve again. If the source region is still in the route, only its peer
  moved, so the record goes to the current peer. If the source region is gone
  (split, merge or repartition), rebuild with `rebuild_write_requests` and
  compare the rebuilt target regions, not the route version. A rebuild
  targeting several regions is sent as a unit, so per-key order holds. A record
  whose rows cannot be placed is a dead letter. After a split, a new target
  region's own queue stays paused until every record rebuilt from the old key
  that targets it has landed; a rebuilt record that is dead-lettered also
  releases the gate, so a permanent failure cannot wedge the new region.
- `NonRetryable`: dead-letter it; a DDL-invalidated record is discarded at the
  check, before it is sent.

At most `replay_concurrency` records are in flight, which bounds concurrency,
not rate: a backlog drains as fast as its targets acknowledge it, and replayed
rows carry old timestamps. A datanode whose write buffer is over
`global_write_buffer_reject_size` answers `StorageUnavailable`, so replay backs
off instead of driving the engine harder.
A poisonous record surfaces as a retryable error and its region's queue waits
behind it until `max_attempts`; v1 does not quarantine.

## The `affected_rows` contract

A successful write today means three things at once: routed to the region's
peer, owned by the region's WAL, and observed by a following read; a queued
write has none of them. Returning success would redefine `affected_rows` from
rows applied to rows accepted, and the count is load-bearing: an insert returns
applied rows, a delete returns the affected rows the region reported, and the
bulk-batch path checks it against the batch row count, a check that would then
pass for rows that may never land. Three properties are at stake — an exact
count, immediate visibility, and applying the write after the client stopped
waiting; any two can hold, all three cannot, because a delete's matched-row
count is known only once it is applied.

- **A. Compensation, v1.** The client receives the original error; the queue
  makes the write land later. An error already does not mean "not applied", so
  A widens an existing window rather than opening one.
- **B. Acknowledged write.** A new outcome meaning accepted and pending. Its
  prerequisites are load-bearing: a client idempotency key with region-side
  deduplication, a queue that survives pod loss with fencing, and the pending
  state in all four protocols.
- **C. No queue.** The client retries or the write is lost.

v1 takes A; B is a later phase recorded here rather than a second document; C is
the baseline A has to beat.

## Consistency

Queued records for one region replay in enqueue order, one at a time — the
ordering the normal path gives, because the dispatcher sends regions
concurrently and only per-region order matters. The guarantee is per-region
FIFO and at-least-once for an admitted record not dead-lettered or lost with
the process; not exactly-once, because `RetryAfterRouteRefresh` does not imply
the failed operation had no side effects and a lost response can apply a record
twice. A queued record can still land after a newer live write and win
deduplication on a table with a primary key and timestamp; the retry path has a
shorter version of this window and the queue extends it to the TTL.

There is no persistent intermediate state: every state maps to "empty" on
restart, so there is no torn state to repair. The durable phase must keep that
property: a half-written record has to be detectable from its framing and
checksum.

## DDL fencing

A queued write can outlive the data it was written against. Today `WriteRetry`
retries for roughly 10s and a DDL from another session can race that retry; the
loop finishes before the client gets the error, so the same client cannot race
it. The queue extends the concurrent window to `ttl` and adds an ordering
today's path does not have: the write fails, the client gets the error, the
client truncates, and a replay puts the old rows back. That is a new
client-visible race the queue introduces, not a longer one: nothing in the
client's view ties the failed write to a later replay.

v1 checks what existing frontend metadata exposes, before every send:

1. The table still exists. A missing table is a terminal dead letter, never a
   name-based rebuild.
2. The table's `TableIdent.version` equals the value captured while the insert
   plan was built and carried with the request. It is not read at handoff:
   retries refresh the table cache, so that read could already be post-ALTER and
   pass against itself. ALTER and logical-table reconciliation bump the version,
   so such a change after planning is discarded rather than retried.
3. The record can still be placed on the current route: if the source region is
   still in the route, only its peer moved; if it is gone (split, merge or
   repartition), rebuild with `rebuild_write_requests` and compare the rebuilt
   target regions, not the route version. An unplaceable record is a dead
   letter.

The version check is a frontend cache hit and cannot be left to the region: the
region fills defaults for columns added after enqueue instead of rejecting the
request, so without it an ALTER lands old rows with defaults.

These catch ALTER, DROP while the table is absent, repartition and logical-table
reconciliation. They do not catch `TRUNCATE` or `DROP` followed by `UNDROP`:
`TRUNCATE` keeps the ids and clears the rows, so an older record puts them back
— zombie data, never a duplicate — and moves neither the table info nor the
cache; `UNDROP` restores the same info, so the version does not move. `DROP`
then `CREATE` is safe because ids are never reused. Check-to-send and a stale
cache remain.

| DDL | Today's in-process retry | Queue extension | New ordering after the error | v1 detection | Residual risk |
| --- | --- | --- | --- | --- | --- |
| Client `TRUNCATE`s after receiving the error | not possible: the retry loop ends before the client sees the error | the write fails, the client truncates, replay puts the old rows back | **yes — a new race the queue introduces** | not detected | zombie rows until `ttl`; bounded compensation only |
| ALTER | a concurrent session can race it | same race, window to `ttl` | no | `TableIdent.version` mismatch: discard | a lagging frontend cache can pass a stale version |
| DROP while absent | a concurrent session can race it | same race, window to `ttl` | no | table missing: dead letter | check-to-send window only |
| DROP + UNDROP | a concurrent session can race it | same race, window to `ttl` | **yes**: fail, `UNDROP`, replay puts rows back | not detected | zombie rows until TTL |
| TRUNCATE | a concurrent session can race it | same race, window to `ttl` | **yes**: fail, `TRUNCATE`, replay puts rows back | not detected | zombie rows until TTL |
| repartition | a concurrent session can race it | same race, window to `ttl` | no | route rebuild or dead letter | check-to-send window |

v1's guarantee is bounded compensation, not DDL-safe replay: off by default,
compensation only, records die with the frontend, and `ttl` caps how long a
zombie replay can happen. The region-side fence needs a table DDL epoch, the
replay epoch in the request header, a truncate epoch in the region manifest,
and new or `UNDROP`-restored regions initialized at the current epoch.

## Failure handling

The defaults are fail-closed: invalid options (zero bounds,
`max_record_size > max_bytes`, `max_record_size > u32::MAX`, overflow) fail
validation at startup; a full queue, an oversized record or a metadata lookup
failure sheds and counts; a missing table, a version mismatch or an unplaceable
route is a dead letter. A replay waits for a `replay_concurrency` permit before
decoding, so a busy budget delays a lease instead of failing a record.

| Situation | Enqueue | Client | Queue |
| --- | --- | --- | --- |
| retryable failures remain, handoff succeeds | handed off | original error | writer queues it; pending and replayable |
| queue full, or a record over the size bound | rejected and counted; the size is known before allocation | original error | saturation stays off the request path |
| table has `append_mode = true` | rejected and counted at admission | original error | v1 has no idempotency key, so the record is not queued |
| terminal failure mixed with retryable ones | retryable siblings queued | terminal error | retryable siblings replay; the terminal one does not |
| client cancels (timeout or disconnect) | not enqueued; the handoff re-checks cancellation before each reservation | original error | a dropped future leaves no record |
| replay `Retryable` / `RetryAfterRouteRefresh` | - | - | attempts++ and backoff / caches refreshed and retried |
| replay `NonRetryable`, TTL expired, attempts exhausted | - | - | dead letter |
| ALTER or destructive schema change after enqueue | - | - | version mismatch: discarded as a DDL dead letter |
| `TRUNCATE` or `UNDROP` after enqueue | - | - | **not detected by v1 checks; a replay may put old rows back** |
| replay finds a stale route or a leader moved | - | - | route refreshed and retried |

## Configuration

Names and defaults are a proposal.

```toml
[write_queue]
enable = false          # off: a retrying client can duplicate a replayed write
max_bytes = "512MiB"    # exact ceiling for pending record bytes; not a disk quota
max_records = 100000    # bounds per-record bookkeeping, which max_bytes does not count
max_record_size = "512KiB" # rejected before allocation; must be <= max_bytes
ttl = "5m"              # derived: datanode cold start and serving again, plus margin
max_attempts = 10       # not persisted; ttl bounds age, attempts bound failed replays
replay_concurrency = 8  # bounds in-flight decode and transport buffers and workers
```

`max_bytes` is the pending-record budget, not a disk quota. The queue's total
memory is at most `max_bytes + max_records * overhead +
replay_concurrency * 64 * max_record_size`; with the defaults that is 512MiB
plus 256MiB of replay transient, about 768MiB before bookkeeping. Operators
size the container limit for that peak, not for `max_bytes`.

`max_record_size` must not exceed `max_bytes`; validation rejects the pair and
zero bounds rather than clamping, and caps it at `u32::MAX`. An oversized record
is rejected before its buffer is allocated. `512KiB` is deliberate: the decoded
form of a record is up to 64× its wire size (see Admission), so a larger record
multiplies the replay transient directly. The cost is that a write whose
per-region request exceeds 512KiB is shed rather than compensated: the queue
covers the tail of normal-sized inserts, not bulk loads.

The TTL default is derived, not tuned. Region failover is off by default, so a
default deployment has no migration to wait for: five minutes covers a datanode
restart — the K8s cold start plus the time to serve again — not a failover
migration. When failover is enabled, detection is the phi-accrual detector
(default threshold 8 and a 10s acceptable heartbeat pause), not a datanode
lease, and the 120s `DEFAULT_REGION_MIGRATION_TIMEOUT` bounds the migration
after it. The code PR records the observed restart-to-serving time and revises
the default if it does not fit.

Standalone rejects `enable = true`. The queue and `write_retry.max_retries` are
independent; when the queue is enabled the frontend installs `WriteRetry`
regardless, and `max_retries = 0` means "enqueue the first retryable failure".

## Lifecycle and wiring

`Frontend` registers the writer and replay tasks on the shared runtimes and
passes an optional handle through the adapter. The queue crate stays in the base
layer; `operator` depends on it, not on `log-store`. Startup does no I/O and
recovers no state, and invalid options fail validation; shutdown is process
exit, with no drain because the client has the error or stopped waiting.

Metrics: handed-off records and bytes (with sheds by reason), pending records
and bytes, in-flight replays and oldest age, replay attempts, dead letters, and
handoff/replay latency. Region and table ids are log fields, not labels.

# Alternatives

| Alternative | Why not |
| --- | --- |
| Durable queue in v1 | A `LogStore`-backed store with startup recovery, `sync_write`, disk bounds and a low-water mark is a second crash-safe log to own and test, and it obscures the part v1 needs to get right: exact admission, ordering and replay. The payload bytes are kept; the durable phase adds framing and the store operations. |
| A log-shaped `QueueStore` from day one | The durable-shaped interface (append, read from a watermark, compact, list keys, latest, create, delete, sync) gives the adapter a head start, but v1's queue only calls append, lease and complete. The extra methods have no v1 caller and shape the core around a phase that is not being built. |
| Store decoded requests instead of encoded payloads | Removes the replay decode, but makes the exact byte bound approximate (Rust container capacities) and couples the queue crate to the write protocol. The encoded payload is what the durable store needs anyway. |
| Region-side DDL fence in v1 | Closes the TRUNCATE/UNDROP window, but adds a proto field, a manifest field and a region path before the queue's core is validated. v1 bounds the window with checks and a short TTL; the fence is a later phase's prerequisite. |

# Drawbacks

- `TRUNCATE` and `UNDROP` are not detected, so a replay can resurrect rows the
  operator deleted; the DDL checks also race (a DDL can land between the check
  and the send, and a stale frontend cache can pass the version check), and a
  client that got the error can `TRUNCATE` while a replay puts the rows back.
  This is v1's main correctness debt, bounded by the documented window, the
  off-by-default switch and the future region-side fence.
- The client is told the write failed while it may still be replayed, so a
  retrying client may duplicate rows; a stale replay can override a newer live
  write, and the window extends to the TTL. A table with `append_mode = true`
  gets no compensation at all, because admission sheds it: deduplication is off
  there, so the client's own retry and a replay would both stay as extra rows
  and inflate any count, sum or average over them.
- The failure path becomes a load path, and the peak is the bound plus the
  replay transient (`max_bytes + replay_concurrency × 64 × max_record_size`,
  about 768MiB with the defaults), so a container sized for `max_bytes` alone
  can be OOM-killed. Replay load also lands as the datanodes recover: old
  timestamps add flush and compaction work, and write-buffer backpressure only
  covers rejected writes, so size `max_background_flushes`,
  `max_background_compactions` and the compaction memory limit for the backlog.

# Future work

1. The durable implementation: a `DurableQueueStore` with `list_keys`, `latest`,
   `sync`, `create` and `delete`, a `LogStore` adapter, startup recovery, disk
   bounds and a low-water mark, plus framing compatibility fixtures.
2. The region-side DDL fence, closing the window v1 documents.
3. The client idempotency key with region-side deduplication, a replay-side
   newer-write check, and the acknowledged contract (position B) across all
   protocols; later, an object-store-backed queue, persisted attempts and dead
   letters, an inspect/replay API, per-row filtering, adaptive per-datanode
   replay rate limiting driven by datanode backpressure, per-table fair
   queueing, and standalone support.

# Open questions and validation items

## Open design questions

- **Fairness.** Is a global byte bound enough for v1, or does one region's backlog
  filling `max_bytes` and rejecting other regions' enqueues warrant per-table
  queueing now?
- **Dead-letter visibility.** Are metrics and a log line enough for v1, or does v1
  need a queryable record or table?

## Validation to record in the code PR

- **TTL default.** The derivation is in Configuration. The code PR records the
  observed datanode restart-to-serving time and the e2e migration duration when
  failover is on, and revises the default if they do not fit.
- **Transient factor.** A test decodes an admitted record and recursively sums
  the decoded containers' capacities for null-heavy, short-string-heavy and
  nested row shapes; if a shape exceeds 64, the constant and the documented peak
  move together, not a new knob.
- **Route changes.** Tests cover a leader move, a split or merge, and an
  unplaceable row.
- **Accounting and overhead.** Tests pin the per-record bookkeeping size, the
  success path's zero queue calls and zero record-buffer allocations, and the
  ledger lock cost at 1, 8 and 64 threads. These are merge criteria.
