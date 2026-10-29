---
Feature Name: Frontend Write Retry Queue (in-memory v1)
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
---

# Summary

Add a bounded retry queue in the frontend for writes whose remaining failures
are all retryable after the in-process retries are exhausted. It hooks
`WriteDispatcher`'s give-up points, encodes each already-split per-region request
once into a versioned envelope, and parks it behind a small `QueueStore` trait.
v1 keeps those records in process memory: the trait's only implementation is an
in-memory ordered map, and the queue dies with the frontend. The envelope layout
and the trait shape are the ones the durable implementation will use, so making
the queue durable later is a second implementation behind the same interface,
not a redesign.

Memory is bounded exactly, and that is v1's main constraint. The request path
computes the record size without allocating (`RegionRequest::encoded_len()`),
reserves it in one ledger whose check and charge happen under a single lock, and
only then encodes into the reserved buffer; a burst cannot create buffers ahead
of the budget. The ledger is released when a record completes, is discarded or
is dropped. A second reservation covers the decoded copy, the transport buffer
and the rebuild split of each in-flight replay, so replay cannot grow memory
without bound either.

The queue is compensation, not an acknowledgment: the client still gets the
original error and `affected_rows` keeps its applied meaning. It is off by
default, because a client that retries can duplicate the replayed write. Replay
is per-region FIFO and at-least-once; exactly-once and pod loss handling are out
of scope.

A queued record is checked at replay against existing frontend metadata: the
table exists, its `TableIdent.version` is unchanged, and the route still places
the record where it was split. `TRUNCATE` and `DROP` followed by `UNDROP` do not
change those signals, so v1 cannot detect them; the window is documented as a
known limitation and is closed by the region-side DDL fence of the durable
phase, which needs a dedicated table epoch, a replay epoch in the request header
and a truncate epoch in the region manifest. v1 adds none of that.

# Motivation

`WriteDispatcher` splits a write per region and, when retries are enabled,
retries retryable failures with jittered backoff: three additional attempts,
starting at 1.5 s and capped at 10 s. That is right for a brief outage, but a
datanode restart, a region migration or a partition can take longer, and only the
client can retry today. A client has no per-region view of what failed: it either
replays the whole statement, duplicating the regions that succeeded, or drops the
failed regions, or it has already exhausted its own budget. Issue #8085 asks for a
frontend-side buffer that replays asynchronously and is bounded.

The queue cannot make the client see success: a queued write has not been routed,
is invisible to reads until replay, and in v1 lives only in one frontend's
memory, so v1 keeps the error response. The durable half of the issue — surviving
a frontend restart — is deliberately a later phase. A durable queue has to own a
second crash-safe log: segmentation, framing, checksums, torn-tail recovery,
garbage collection, startup recovery, disk bounds and a low-water mark. v1 gets
the queue's core right first: admission, ordering, replay and exact memory
accounting, with the envelope and the store trait already in the shape the
durable implementation needs.

# Goals

- A write whose remaining failures are all retryable is enqueued best effort
  before the frontend returns the original error, and replayed later.
- The response does not change: `affected_rows` keeps its applied meaning and a
  queued write is never reported as success.
- Per-region FIFO replay, one in-flight record per region; a region's replay
  backlog occupies at most one replay slot.
- Exact bounds on bytes and record count, plus bounds on record size, record age
  and attempts; the queue sheds rather than waits, and neither the writer nor
  replay performs blocking work on a worker that serves queries.
- The queue's memory attributable to it is bounded by configuration, not by the
  size of a failure storm: a record's size is reserved before it is allocated,
  and every release path returns exactly what was charged.
- Non-retryable records and records past a bound leave the queue as observable
  dead letters; a record an ALTER invalidates is discarded, never retried.
- No behaviour change when disabled; no new error classification; no new
  persisted or wire format.
- The envelope and the `QueueStore` trait are the durable implementation's
  interface, so the in-memory store can be replaced without touching the write
  path, the state machine or replay.

# Non-goals

- Surviving a frontend restart, a pod reschedule or node loss. Records live in
  process memory and are lost with the process; durability is the later phase.
- Exactly-once delivery; a record without a client idempotency key can duplicate
  rows. The key is recommended before production enablement but is not v1.
- Returning success for a queued write, or changing `affected_rows` from applied
  to accepted (see The `affected_rows` contract).
- Fencing `TRUNCATE` and `DROP` followed by `UNDROP`. The existing metadata
  signals v1 checks do not move for either, and the region-side fence is part of
  the durable phase. v1 documents the window instead of closing it.
- Standalone mode, which sets `write_retry.max_retries = 0` because local region
  calls do not use remote write retries, so there is no give-up point to hook;
  and writes outside the retry and give-up path, such as the enterprise Flow
  child path, which prepares the request and then dispatches it itself.
- Tables that feed a flow. Replay sends through the dispatcher's per-region send
  path, below the insert path that mirrors a write to a flow and marks its time
  window dirty, so a replayed write is never mirrored and never marks a window
  dirty; the flow's sink keeps missing those rows, permanently once the window
  has been emitted.
- Global ordering, ordering against newer live writes, a query API for dead
  letters, and any backend other than the in-memory store.

# Details

## Where the queue sits

The queue hooks the two give-up points of the write path: the tail of the retry
loop and the retry-enabled merge branch of `WriteDispatcher::finish`, where a
zero budget lands. By then the write is split per region, each error carries a
`RetryHint`, and route refresh is available.

A terminal error takes precedence: with terminal and retryable failures in one
statement the siblings are not queued, because a client retry of the statement
would duplicate them. Only `Inserts`, `Deletes` and `BulkInsert` are queued, and
a cancelled request is not enqueued.

## Admission and memory accounting

During a cluster-wide outage the failure path is the request path: every write
that exhausts its retries arrives here, once per region. The handoff therefore
does no I/O and allocates nothing before the record is admitted:

1. The request path first reduces the request header to `dbname` and
   `tracing_context`. On that request it computes the payload length with
   `RegionRequest::encoded_len()`, which prost computes by walking the message
   without allocating; a bulk insert carries its Arrow IPC payload in one `bytes`
   field, so the same call covers it and no special case is needed. The record
   size is that payload plus the fixed 45-byte envelope header.
2. It reserves that record size in the queue's ledger. The ledger is one
   `Mutex<Ledger { pending_bytes, pending_records, transient_bytes }>` and the
   byte check, the record-count check and both charges happen in the same
   critical section, with no `await` inside, so a burst cannot slip records past
   a bound the way separate atomic counters can. A record longer than
   `max_record_size` is rejected, and a reservation that would cross either bound
   fails; both return at once, shed and count, and nothing has been allocated
   yet.
3. Only after the reservation succeeds does it allocate one buffer of
   `header_len + payload_len` bytes, write the header and encode the request into
   the payload area. `encoded_len()` is exact for the final encoding, so the
   buffer is not reallocated and its length is the reserved length. The buffer
   then becomes the stored record: there is no second copy.
4. The record is moved into the writer's channel. The channel has no budget of
   its own: every message in it is already charged, so a stalled writer cannot
   make memory grow past the ledger.
5. The ledger is released when the record completes, is discarded as a dead
   letter, expires, or the frontend shuts down; record count and byte count move
   together under the same lock.

The per-record bookkeeping is a constant pinned by a test that sums the struct
and queue-node sizes with `size_of`: the entry id, the queue node, the monotonic
enqueue instant, the attempt counter and the channel entry. It is not charged to
`max_bytes`; it is bounded by `max_records`, so the pending bound is `max_bytes`
of record bytes plus `max_records × overhead`, not an estimate of the payloads.

Replay needs a decoded request, and decoding allocates. A lease hands out the
stored record without copying its payload; before decoding, the worker reserves
`REPLAY_TRANSIENT_FACTOR × record_len` in the ledger's transient field, and
releases it when the send finishes. The factor is a documented constant of 3,
covering the decoded request (container growth is at most doubling), the
transport buffer the client encodes into, and the split that
`rebuild_write_requests` may perform; a test decodes the largest admitted record
and sums the decoded containers' capacities to assert they stay within it. The
transient field has its own cap,
`replay_concurrency × REPLAY_TRANSIENT_FACTOR × max_record_size`, so replay can
never consume the pending budget and a backlog cannot make the queue shed
records it would otherwise keep.

The two ledgers give a hard total for memory the queue holds:

```text
pending record bytes + bookkeeping        <= max_bytes + max_records * overhead
in-flight replay decode and send buffers  <= replay_concurrency * 3 * max_record_size
```

A record that does not fit is not queued; the client gets the same error it would
get without the queue. The queue never waits for room and never evicts a pending
record to make room: rejecting is the same result as having no queue, while
eviction would make the queue's contents depend on the order of a failure storm.

The writer drains the channel, takes the queue state lock once per drain, appends
the envelopes to their keys' deques and assigns entry ids. The request path never
takes the state lock, so under a storm requests contend only on the ledger lock,
whose critical section holds no allocation or `await`. The state lock and the
ledger lock nest only in the completion direction (state, then ledger), and no
path takes them in the other order.

## Failure-path load

The failure path is still the request path, so the handoff cost per failed
request matters: one length computation, one short lock, one encode of the bytes
the record must hold anyway, and one channel send. There is no store call, no
blocking work and no task spawn; a failed request cannot be parked behind the
queue.

The writer is the only task that adds records to the queues, and its drain is a
memory append, so the old single-writer store ceiling does not apply. Completion
is the replayer's, and it is also a memory operation. The remaining queue-wide
serialization is the state lock, taken by the writer per drain and by the
replayer per lease and completion, plus the ledger lock per record; neither can
stall a worker. Under a storm both are hot, which is the price of exact
accounting; it is paid on a path that has already failed and returns immediately
after the handoff.

Replay runs on the shared runtimes as an ordinary task. Its decode and its
client's encode are CPU and allocation work; `replay_concurrency` and the
transient cap bound both, and the worker yields at each send. A payload at
`max_record_size` can occupy a worker for the duration of one decode, which is
the same order as the live write path already spends encoding a request.

## Storage

The queue is a new base-layer crate, `common-retry-queue`, defining a
`QueueStore` trait that treats a store as an ordered map from a queue key to
opaque records, so the crate does not know the write protocol. Its shape matches
what the durable implementation will need from `LogStore`: append a batch, read
from a watermark, compact a prefix, list keys, read the latest entry id, create
and drop a key, sync, stop. The v1 implementation, `MemoryQueueStore`, keeps the
encoded envelope bytes, not decoded requests, so the envelope and every trait
method are exercised now and the durable adapter is a drop-in.

The key is `RegionId::as_u64()`. A record is a versioned envelope:

```text
version u8 | key u64 | rows u64 | ddl_epoch u64 | route_version u64
          | enqueued_at_millis u64 | payload_len u32 | payload
```

Integers are fixed-width little-endian. `payload` is the protobuf encoding of
the `RegionRequest`; `rows` serves the dead-letter metric without decoding it.
`ddl_epoch` holds the table's `TableIdent.version` observed at enqueue; the
durable phase replaces it with a dedicated DDL epoch. `route_version` is the
route version observed at enqueue; a replay rebuilds the split when it changes.
The `enqueued_at_millis` field is kept for the durable store, but v1 measures age
with a monotonic instant taken at enqueue and never reads the wall-clock field
for expiry. The payload is encoded from the header-reduced request; the durable
phase adds a replay epoch to the request header, a field v1 does not have.

There are no ack records: a region has at most one in-flight record, so
completion is always a prefix. `MemoryQueueStore` holds a deque and a watermark
per key; completion advances the watermark and pops the prefix, which is
`compact`. An unknown envelope version is rejected when the record is read: the
in-memory store never sees a foreign record, but the durable reader fails startup
on one rather than dropping records the queue still holds, and keeping the check
in the shared envelope code keeps that behavior when the implementation changes.

## Queue state machine

Per key the queue keeps a deque of pending records, the last entry id and the ack
watermark. One mutex serializes writer insertion, id allocation, leasing and
completion; only the writer and the replayer take it, so it never delays a
request.

A lease is the head record of a key that is not in flight and has not expired.
`complete` advances the watermark, pops the record and releases its bytes;
`discard` does the same and reports a dead letter. TTL is checked at lease time
and by a periodic sweep of each key's head, which also unblocks a key whose head
expired. Age comes from the monotonic enqueue instant, so a clock step while the
process runs cannot expire a record early or keep it alive longer. `max_attempts`
stops a permanently failing record quickly; attempts are in memory and reset
when the process restarts, which is moot because the records do too.

At startup the queue is empty. Shutdown stops leasing, settles in-flight sends,
drops the pending records and releases the ledger; the records are gone with the
process by design, and the client was already told the write failed.

## Replay

The replay worker lives in `operator` next to `WriteDispatcher`, because it needs
the same routing and partition machinery; the queue crate only stores and leases.
It keeps one in-flight record per key, leases the head of an idle key, resolves
the region's leader, applies the DDL checks, reserves its transient budget,
decodes the payload, and sends through the same client the live path uses.

- Success: complete the record and lease the next one.
- `Retryable`: increment attempts, back off with jitter from about one second,
  release the transient reservation and leave the record at the head.
- `RetryAfterRouteRefresh`: increment attempts, invalidate the route caches and
  resolve again. If the route version changed, rebuild with
  `rebuild_write_requests`, comparing the rebuilt target region because a leader
  migration bumps the version too; a rebuild targeting several regions is sent as
  a unit, so per-key order holds.
- `NonRetryable`: dead-letter it; a DDL-invalidated record is discarded at the
  check, before it is sent.

At most `replay_concurrency` records are in flight. That bounds concurrency, not
rate: a backlog drains as fast as its targets acknowledge it, and replayed rows
carry old timestamps, so they add flush and compaction work over existing windows
while live queries run. When the datanode's write buffer passes its reject
threshold it answers `StorageUnavailable`, which the classifier treats as
retryable, so replay backs off instead of driving the engine harder; pacing
replay by rate is future work. A record can be poisonous to its target rather
than to itself: a malformed payload, or a request that trips a panicking path,
surfaces as an unavailable or internal error that the classifier reads as
retryable, and the region's queue waits behind it until `max_attempts`. The
proposed mitigation is to quarantine a record after two consecutive
peer-unreachable attempts; a per-datanode circuit breaker is the broader fix
(Future work).

## The `affected_rows` contract

A successful write today means three things at once: the request was routed to
the region's peer, the region's WAL owns it, and a following read observes it. A
queued write has none of them — it has not been routed, and in v1 it lives only
in one frontend's memory until replay. Returning success would redefine
`affected_rows` from rows applied to rows accepted, and the count is
load-bearing: an insert returns applied rows, a delete returns the affected rows
the region reported, and the bulk-batch path checks it against the batch row
count, a check that would then pass for rows that may never land.

Three properties are at stake: an exact count, immediate visibility, and applying
the write after the client stopped waiting. Any two can hold; all three cannot: a
delete's matched-row count is known only once it is applied, so an accepted delete
has no exact number to return.

- **A. Compensation, v1.** The client receives the original error; the queue
  makes the write land later in the common case. An error does not mean "not
  applied" today either, so A widens an existing window rather than opening one.
- **B. Acknowledged write.** A new outcome meaning accepted and pending. Its
  prerequisites are load-bearing: a client idempotency key with region-side
  deduplication, a queue that survives pod loss with fencing, and the pending
  state in all four protocols. An accepted record is never dropped by TTL or
  attempts.
- **C. No queue.** The client retries or the write is lost.

v1 takes A; B is a later phase recorded here rather than a second document; C is
the baseline A has to beat.

## Consistency

Queued records for one region replay in enqueue order, one at a time — the same
ordering the normal path gives, because the dispatcher sends regions concurrently
and only per-region order matters. The guarantees are per-region FIFO and
at-least-once, not exactly-once: `RetryHint::RetryAfterRouteRefresh` documents
that the hint does not imply the failed operation had no side effects. A queued
record can also land after a newer live write, and on a table with a primary key
and timestamp it is likely assigned a newer sequence, so a stale value can win
deduplication; the retry path has a shorter version of this window, and the queue
extends it to the TTL.

A crash loses the queue. That is the same outcome as today for these writes: the
client already holds the error. It is not an at-least-once violation, because
nothing was promised past the handoff.

## DDL fencing

A queued write can outlive the data it was written against. `TRUNCATE` keeps the
table id and region ids and clears the rows, so a record enqueued before it puts
the cleared rows back — zombie data, not a duplicate. `DROP` followed by `UNDROP`
restores the same table id and regions, so the table-existence check alone misses
it; `DROP` followed by `CREATE` is safe because ids are never reused.

v1 checks what existing frontend metadata exposes, before every send:

1. The table still exists. A missing table is a terminal dead letter, never a
   name-based rebuild.
2. The table's `TableIdent.version` equals the value captured at enqueue. ALTER
   and logical-table reconciliation bump it and invalidate the table cache, so
   such a change after enqueue is discarded rather than retried.
3. The route still places the record where it was split. A leader change only
   needs the current peer; a layout change needs a rebuild, told apart by the
   rebuilt target region, not the route version, which moves for both. A record
   that cannot be placed is a dead letter.

These checks catch ALTER, DROP (while the table is absent), repartition and
logical-table metadata reconciliation. They do not catch `TRUNCATE` or `DROP`
followed by `UNDROP`: `TRUNCATE` never updates the table info and never
invalidates the table cache, and `UNDROP` restores the same table info (same id,
same version), so `TableIdent.version` does not move in either case; `UNDROP`
does invalidate the cache, but that changes nothing the check looks at. They
also do not close the window between the check and the send: an ALTER, DROP or
repartition can land in between, and a frontend whose cache has not yet seen an
ALTER can pass check 2 on a stale version. v1 ships with that window, under four
constraints: the feature is off by default, it is compensation only (the client
already holds the error), records die with the frontend process, and `ttl`
bounds how long a zombie replay can happen.

Closing the window needs the region-side fence: a dedicated table DDL epoch
persisted with the table metadata and bumped before the destructive step, the
epoch in the replay request header, a truncate epoch persisted with the data
clear in the region manifest, and new or `UNDROP`-restored regions initialized at
the table's current epoch. That fence is the durable phase's merge gate; v1 adds
no proto field, no manifest field and no region path change, which is why its
compatibility surface is empty.

A frontend-side discard hook on DDL would narrow the local window but cannot
close it: a DDL through one frontend cannot purge another frontend's memory, and
a frontend restarted after the DDL has no records left. It is recorded as future
work, behind the region-side fence.

## Failure handling

| Situation | Enqueue | Client | Queue |
| --- | --- | --- | --- |
| all remaining failures retryable, handoff succeeds | handed off | original error | writer queues it; pending and replayable |
| queue full, or a record over the size bound | rejected and counted; the size is known before allocation | original error | saturation stays off the request path |
| frontend exits, crashes, or the pod is lost | already handed off | original error | records lost with the process |
| terminal failure mixed with retryable ones | siblings not queued | terminal error | unchanged |
| client retries while its record is queued | - | its own retry | both can apply; duplicates possible |
| replay `Retryable` / `RetryAfterRouteRefresh` | - | - | attempts++ and backoff / caches refreshed and retried |
| replay `NonRetryable`, TTL expired, attempts exhausted | - | - | dead letter |
| ALTER or destructive schema change after enqueue | - | - | version mismatch: discarded as a DDL dead letter |
| table dropped after enqueue, not restored | - | - | table missing: dead letter |
| `TRUNCATE` or `UNDROP` after enqueue | - | - | **not detected by v1 checks; a replay may put old rows back** |
| partition layout changed | - | - | rebuilt; a row that cannot be placed is a dead letter |
| replay finds a stale route or a leader moved | - | - | route refreshed and retried |

## Compatibility

v1 persists nothing and changes no wire format, so a build without the queue and
a build with it can share data and protocol; disabling the queue leaves no state
behind. The envelope carries an explicit version and unknown
versions are rejected when read, which is a forward-looking rule for the durable
implementation: since no released frontend writes records, there is no
downgrade path to test yet.

A replayed duplicate is invisible on a table that deduplicates: it lands on the
same primary key and timestamp as the original, and the higher sequence wins
against that original. `append_mode = true` turns deduplication off, so on those
tables a duplicate stays a second row and any count, sum or average over those
rows is inflated; metrics tables deduplicate by default, log-style append-mode
tables do not.

## Configuration

Names and defaults are a proposal.

```toml
[write_queue]
enable = false          # the client response does not change
max_bytes = "512MB"     # pending record bytes; the admission bound
max_records = 100000
max_record_size = "8MB"
ttl = "30m"
max_attempts = 10       # not persisted; ttl is the bound
replay_concurrency = 8
```

`max_bytes` is the pending-record budget, not a disk quota and not a soft target.
The queue's total memory is at most `max_bytes + max_records * overhead +
replay_concurrency * 3 * max_record_size`; the configuration docs state that sum,
and `max_record_size` must not exceed `max_bytes`. A record larger than
`max_record_size` is rejected before its buffer is allocated, so one huge bulk
insert cannot claim the budget.

Standalone rejects `enable = true`. The queue and `write_retry.max_retries` are
independent. `with_retry` installs `WriteRetry` only when `max_retries > 0`; when
the queue is enabled the frontend installs it regardless, and `max_retries = 0`
then means "enqueue the first retryable failure".

## Lifecycle and wiring

`Frontend` gains the queue when enabled: it registers the writer and replay
tasks on the shared runtimes and passes an optional handle through the adapter.
The queue crate stays in the base layer and depends only on `common-*`;
`operator` depends on the queue crate, not on `log-store`, because v1 has no
store to adapt. Startup cannot fail because of the queue: there is nothing to
open and no state to recover. Shutdown stops leasing, settles in-flight sends and
drops the records.

The metrics to export are handed-off records and bytes with sheds by reason;
pending records and bytes, transient replay bytes and oldest pending age; replay
attempts by outcome; dead letters by reason; and handoff and replay latency.
Region and table ids are not labels; they appear in the dead-letter log line with
the row count.

# Alternatives

| Alternative | Why not |
| --- | --- |
| Durable queue in v1 | The original shape: a `LogStore`-backed store under the frontend, with startup recovery, `sync_write`, disk bounds and a low-water mark. It is a second crash-safe log to own and test, and it obscures the part v1 needs to get right: exact admission, ordering and replay. The envelope and `QueueStore` keep it as the next implementation. |
| In-process retries only (raising `max_retries`) | The first, zero-cost option, but it cannot outlive the client's timeout or a frontend restart. |
| Enqueue at the gRPC entry | Lacks the per-region split and `RetryHint`, and would enqueue never-rejected requests. |
| Append inline on the request path | Blocks the calling task on a store that backpressures when its disk falls behind. |
| `spawn_blocking` per enqueue | Moves the blocking call off the worker but not the load, and the request still waits. |
| Wait for the append before returning | Restores "durable when the response returns", but every failed request pays the deadline, which only delays the shed. |
| Several writers sharded by region | Raises storm absorption, but the writer's drain is a memory append and the ledger is now the hot path; a single writer is not the ceiling. |
| Evict the oldest record instead of shedding the new one | Bounds memory the same way, but makes the queue's contents depend on storm order and drops compensation that may be closer to completion. v1 rejects the new record; revisit with fairness. |
| Datanode-side queue, or a sidecar | The datanode is often the down component, and a sidecar adds a deployment component and a hop while still needing the same semantics. |
| Acknowledge a queued write as accepted | Relaxes the counter so an accepted write can be dropped by TTL or a lost pod. The distinct pending outcome is the recorded later phase. |
| Frontend-side DDL discard hook | A DDL through one frontend cannot purge another frontend's memory and does not close the check-to-send window; it is an optimization, not the fence. |

# Drawbacks

- Records die with the frontend process or pod. A restart loses the queued
  compensation; the client was already told the write failed, but the feature's
  value in a long outage is bounded by the process's lifetime.
- `TRUNCATE` and `UNDROP` are not detected, so a replay can resurrect rows the
  operator deleted. The window is documented, the feature is off by default, and
  the durable phase's region-side fence is the fix. This is the main correctness
  debt of v1.
- The DDL checks race with DDL even for the cases they cover: an ALTER, DROP or
  repartition can land between the check and the send, and a lagging frontend
  cache can pass the version check on stale metadata.
- The client is told the write failed while it may still be replayed, so a
  retrying client may duplicate rows; a stale replay can override a newer live
  write on the same primary key and timestamp, and the window extends to the TTL.
- Any invalidating ALTER discards queued compensation for the table, so a table
  with frequent schema changes keeps losing records.
- The failure path becomes a load path: under an outage every failed write is
  copied and encoded until `max_bytes`, competing with the rest of the frontend.
  The ledger makes the bound exact but not free; the ledger lock is per record
  and the encode is one copy of the record.
- Replay decodes on the shared runtimes. The transient cap bounds it, but a
  `max_record_size` payload costs a worker one decode and the client one encode,
  next to live queries.
- The bounds are global, so one hot region can fill `max_bytes` and make other
  regions' enqueues fail; attempts are not persisted, and dead letters are metric
  and log only.
- Replay load arrives at the datanodes just as they recover: an old backlog
  drains at the acknowledgement rate, and its rows carry old timestamps, so it
  adds flush and compaction work to windows that already exist, next to live
  queries. The write buffer's reject threshold turns the extreme case into
  retryable backpressure, but scheduled compaction still competes for CPU and IO.
  Operators should budget for that and treat `max_background_flushes`,
  `max_background_compactions` and the compaction memory limit as part of the
  sizing.

# Future work

1. The durable implementation behind `QueueStore`: a `LogStore` adapter in the
   frontend, startup recovery, `sync_write`, disk bounds and a low-water mark,
   plus the compatibility fixtures for the envelope. This is the phase that
   makes issue #8085's "survives a restart" real.
2. The region-side DDL fence: a dedicated table DDL epoch, the replay epoch in
   the request header, a truncate epoch in the region manifest, new and
   `UNDROP`-restored regions starting at the current epoch, and the tests that
   truncate/undrop/repartition a table with a pending record. It is a merge gate
   for that phase, and it closes the window v1 documents.
3. The client idempotency key with region-side deduplication, and a replay-side
   newer-write check to close the stale window.
4. The acknowledged contract (position B): a pending outcome across protocols and
   a queue that survives pod loss with fencing.
5. An object-store-backed queue, persisted attempts and dead letters, an
   inspect/replay API, an export of dead letters to object storage for offline
   analysis and replay after the queue has dropped them, an optional local purge
   hook on DDL, and per-row filtering so a time-range truncate only drops the
   rows it removed.
6. Per-datanode replay rate limiting and circuit breaking, per-table fair
   queueing, preserving the original receive time for ordering against later
   writes, and standalone support if it ever enables remote retries.

# Unresolved questions

- **Shedding versus eviction.** v1 rejects a record when the bounds are reached.
  Should it instead discard the oldest pending record, as a TTL expiry would? The
  argument for shedding is that it does not reorder compensation; the argument
  for eviction is that the oldest record is the one the client has waited on
  longest.
- **TTL and the DDL window.** With the lightweight checks, the TTL is also the
  bound on how long a `TRUNCATE`/`UNDROP` replay can happen. Is 30 minutes short
  enough for v1, or should the default be shorter until the region-side fence
  lands?
- **Poison quarantine and replay pacing.** Is quarantining a record after two
  consecutive peer-unreachable attempts enough, or is a per-datanode circuit
  breaker needed in v1 to keep one record from failing the same node repeatedly?
- **Replay transient factor.** The 3× reservation is derived from container
  doubling, the transport encode and the rebuild split; the test that pins it
  needs a worst-case decoded record. Should the factor, or the derived transient
  cap, be a configuration knob rather than a constant?
- **Fairness.** Is a global byte bound enough for v1, or does one region's backlog
  filling `max_bytes` and rejecting other regions' enqueues warrant per-table
  queueing now?
- **Dead-letter visibility.** Are metrics and a log line enough for v1, or does v1
  need a queryable record or table?
