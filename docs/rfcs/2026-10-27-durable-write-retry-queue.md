---
Feature Name: Durable Write Retry Queue
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/8085
Date: 2026-10-27
Author: @lczllx
---

# Summary

Add a durable, bounded retry queue in the frontend for writes whose remaining
failures are all retryable after the in-process retries are exhausted. It hooks
`WriteDispatcher`'s give-up points and persists already-split per-region requests
through a small `QueueStore` trait over `LogStore`, backed by a frontend-owned
`RaftEngineLogStore` on local disk. A worker replays each region in order; enqueue
is a handoff, not an append on the request path, so an outage cannot turn failed
writes into blocking store calls on a runtime that serves queries.

The queue is compensation, not an acknowledgment: the client still gets the
original error and `affected_rows` keeps its applied meaning. It is off by
default, because a client that retries can duplicate the replayed write.

A queued record is fenced by a table DDL epoch that `TRUNCATE`, `DROP`, `UNDROP`
and schema changes bump, with a region-side check at apply time. **The queue does
not merge before this fence is implemented and tested**, or a replay after
`TRUNCATE` silently puts back rows the operator deleted. Replay is per-region FIFO
and at-least-once; exactly-once, pod loss and power loss are out of scope.

# Motivation

`WriteDispatcher` splits a write per region and, when retries are enabled,
retries retryable failures with jittered backoff: three additional attempts,
starting at 1.5 s and capped at 10 s. That is right for a brief outage, but a
datanode restart, a region migration or a partition can take longer, and only the
client can retry today. A client has no per-region view of what failed: it either
replays the whole statement, duplicating the regions that succeeded, or drops the
failed regions, or it has already exhausted its own budget. Issue #8085 asks for a
frontend-side buffer that survives a restart, replays asynchronously, and is
bounded.

The queue cannot make the client see success: a queued write has not been routed,
is held on one frontend's disk, and is invisible to reads until replay, so v1
keeps the error response. `LogStore` already provides per-region namespaces,
monotonic entry ids, `read` from an entry id, prefix compaction and namespace
listing; a second append-only format would mean owning segmentation, checksums,
torn-tail recovery and garbage collection again.

# Goals

- A write whose remaining failures are all retryable is enqueued best effort
  before the frontend returns the original error, and replayed later.
- The response does not change: `affected_rows` keeps its applied meaning and a
  queued write is never reported as success.
- Per-region FIFO replay, one in-flight record per region, surviving a frontend
  restart; one region's backlog neither blocks nor reorders another's.
- Hard bounds on bytes, record size, record count, record age and attempts; the
  queue sheds rather than waits, and neither the writer nor replay blocks a
  worker that serves queries.
- Non-retryable records and records past a bound leave the queue as observable
  dead letters; a record the table's DDL epoch invalidates is discarded, never
  retried.
- No behaviour change when disabled; `LogStore` and `RetryHint` are reused, with
  no new segmented-log format or second error classification.

# Non-goals

- Exactly-once delivery; a record without a client idempotency key can duplicate
  rows. The key is recommended before production enablement but is not v1.
- Returning success for a queued write, or changing `affected_rows` from applied
  to accepted (see The `affected_rows` contract).
- Surviving the loss of the frontend pod and its disk, moving the queue between
  nodes, and power-loss durability: this is local state for one process.
- Standalone mode, which sets `write_retry.max_retries = 0` because local region
  calls do not use remote write retries, so there is no give-up point to hook;
  and writes outside the retry and give-up path, such as the Flow child path.
- Tables that feed a flow. Replay runs inside `WriteDispatcher`, below the insert
  path that mirrors a write to a flow and marks its time window dirty, so a
  replayed write is never mirrored and never marks a window dirty; the flow's
  sink keeps missing those rows, permanently once the window has been emitted.
- A durable record before the client response, and global ordering, ordering
  against newer live writes, a query API for dead letters, and any backend other
  than the local Raft Engine store.

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

## Failure-path load

During a cluster-wide outage the failure path is the request path: every write
that exhausts its retries arrives here, once per region. Awaiting a store append
would hold the request task until the store catches up, even if the blocking
write itself ran on the blocking pool, and one append per record would multiply
the store's work by the number of failed requests. The enqueue is therefore a
handoff, and only the queue's writer touches the store:

- The request path encodes the record once into its payload buffer, charges the
  buffer's length against the byte budget and moves the buffer into a bounded
  channel; the writer appends payloads without re-encoding them. A record that
  arrives when the queue is full is encoded and then dropped; `max_record_size`
  caps that waste. Encoding once is cheaper than measuring the record and
  encoding it later, and the charge is exactly the payload size.
- Memory here is bounded per request, not by `max_bytes`: a buffer exists before
  the budget is consulted, so a storm holds one buffer per concurrent failed
  request. A bulk-insert request carries its Arrow payload in one field, so the
  size check runs before the encode and an oversized one is rejected without
  allocating; a row-based request can only be measured by encoding it, so an
  oversized one is encoded and then dropped, and the protocol's request limit is
  its ceiling.
- One writer drains the channel and calls `append_batch` once per drain: one call
  carries entries for many regions and returns one id per region, and ids follow
  drain order, so a failure storm costs one store write per drain and per-region
  FIFO holds.
- The writer, startup recovery and replay are ordinary tasks on the shared
  runtimes, not a component-local runtime: tasks go through `spawn_global`, and
  the append's blocking part belongs on the blocking pool (`spawn_blocking`),
  which is the store fix listed as a question. A private runtime would add a
  second thread pool and still not isolate the store: `RaftEngineLogStore` starts
  its gc and sync tasks on the global runtime that query and ingest share, and
  its `read` blocks a global worker per call, so up to `replay_concurrency` reads
  can occupy the whole pool on a slow disk.
- The handoff channel and the pending set share `max_bytes` and `max_records`;
  reaching either fails the handoff at once, returns the original error and
  counts the shed. The queue never waits or evicts: rejecting is the same result
  as having no queue, and eviction would make the contents depend on the order of
  a failure storm.

The writer is also the ceiling: it appends serially, so while it sits in one
append every key stops, and the replayer shares the state mutex, so a compaction
delays the next append the same way. The handoff keeps that off the request path
but does not remove the queue-wide head-of-line: above the ceiling it sheds the
writes the queue exists to compensate, exactly when they are most numerous.
Sharded writers lift the ceiling and are a later step (Alternatives).

## Storage

The queue is a new base-layer crate, `common-retry-queue`, defining a `QueueStore`
trait that treats a durable store as an ordered map from a queue key to opaque
records, so the crate does not know the write protocol. It mirrors what the queue
needs from `LogStore`: append a batch, read from a watermark, compact a prefix,
list namespaces, create and drop a stream. The adapter lives in the frontend, the
only layer that depends on both. The key is `RegionId::as_u64()`, already the log
unit's namespace; Raft Engine takes an exclusive lock on `<dir>/LOCK`, so a second
process fails to open the directory instead of sharing it.

A record is a versioned envelope:

```text
version u8 | key u64 | rows u64 | ddl_epoch u64 | route_version u64
          | enqueued_at_millis u64 | payload_len u32 | payload
```

`payload` is the protobuf encoding of the `RegionRequest`; `rows` serves the
dead-letter metric without decoding it. Before encoding, every header field is
cleared except `dbname` and `tracing_context`; the queue sets the replay epoch
when it replays the record, and 0 means "not a queue replay".

There are no ack records: a region has at most one in-flight record, so completion
is always a prefix. The queue advances an in-memory ack watermark and periodically
compacts through it; a crash between applying and compacting replays the record,
which is the at-least-once. Attempts are not persisted, and `sync_write = false`
by default preserves the queue across a process restart but loses the last sync
interval on power loss; nothing is acknowledged, so v1 never calls `sync`.

`max_bytes` bounds payload bytes, not disk use: Raft Engine's segments count
against the disk until purge catches up, and rejecting records frees nothing. v1
has no low-water mark; the proposed shape is a `min_free_disk_bytes` check in the
writer that discards the oldest pending records through the TTL path.

## Queue state machine

Per key the queue keeps a deque of pending records, the last entry id and the ack
watermark. One mutex serializes append, id allocation and compaction, because the
store needs contiguous ids per region; only the writer and the replayer take it,
so a lock held during an append never delays a request. The byte and record
counters the handoff reads are separate atomics, so a burst can admit a few
records past a bound, bounded by the burst.

A lease is the head record of a key that is not in flight and has not expired.
`complete` advances the watermark and frees the slot; `discard` does the same and
reports a dead letter. TTL is checked at lease time and by a periodic sweep.
`enqueued_at_millis` has to be wall-clock time, because a monotonic clock means
nothing across a restart and the record is persisted; it is read once, not
consulted while the process runs. At startup the queue derives each record's
age once from the stored timestamp, and from then on advances that age with a
monotonic clock; a record enqueued by the running process starts its age at
zero. A clock step during the process therefore cannot expire a record early or
keep it alive longer. Across a restart there is no monotonic reference left, so
a step between processes moves every record's age by that step: `ttl` is a
tolerance bound, not an exact deadline, and assumes NTP-disciplined clocks.
Backoff and RPC deadlines are durations and already use monotonic timers.
`max_attempts` stops a permanently failing record quickly, and since attempts
reset on restart, `ttl` is the bound that always applies.

If enabled and the store cannot be opened or recovered, startup fails, because
starting without the previous process's records would silently discard
compensation. Shutdown stops leasing, settles in-flight sends and stops the
store; pending records stay for the next process.

## Replay

The replay worker lives in `operator` next to `WriteDispatcher`, because it needs
the same routing and partition machinery; the queue crate only stores and leases.
It keeps one in-flight record per key, leases the head of an idle key, resolves
the region's leader, applies the DDL fence checks, and sends through the same
client the live path uses.

- Success: complete the record and lease the next one.
- `Retryable`: increment attempts, back off with jitter from about one second.
- `RetryAfterRouteRefresh`: increment attempts, invalidate the route caches and
  resolve again. If the route version changed, rebuild with
  `rebuild_write_requests`, comparing the rebuilt target region because a leader
  migration bumps the version too; a rebuild targeting several regions is sent as
  a unit, so per-key order holds.
- `NonRetryable`: dead-letter it; a DDL-invalidated record is discarded at the
  fence check, before it is sent.

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
queued write has none of them. Returning success would redefine `affected_rows`
from rows applied to rows accepted, and the count is load-bearing: an insert
returns applied rows, a delete returns the affected rows the region reported, and
the bulk-batch path checks it against the batch row count, a check that would then
pass for rows that may never land.

Three properties are at stake: an exact count, immediate visibility, and applying
the write after the client stopped waiting. Any two can hold; all three cannot: a
delete's matched-row count is known only once it is applied, so an accepted delete
has no exact number to return.

- **A. Compensation, v1.** The client receives the original error; the queue
  makes the write land later in the common case. An error does not mean "not
  applied" today either, so A widens an existing window rather than opening one.
- **B. Acknowledged write.** A new outcome meaning accepted and pending. Its
  prerequisites are load-bearing: a client idempotency key with region-side
  deduplication, an object-storage queue with fencing, and the pending state in
  all four protocols. An accepted record is never dropped by TTL or attempts.
- **C. No queue.** The client retries or the write is lost.

v1 takes A; B is a later phase recorded here rather than a second document; C is
the baseline A has to beat.

## Consistency

Queued records for one region replay in enqueue order, one at a time — the same
ordering the normal path gives. The guarantees are per-region FIFO and
at-least-once, not exactly-once: `RetryHint::RetryAfterRouteRefresh` documents
that the hint does not imply the failed operation had no side effects. A queued
record can also land after a newer live write, and on a table with a primary key
and timestamp it is likely assigned a newer sequence, so a stale value can win
deduplication; the retry path has a shorter version of this window, and the queue
extends it to the TTL.

## DDL fencing

A queued write can outlive the data it was written against. `TRUNCATE` keeps the
table id and region ids and clears the rows, so a record enqueued before it puts
the cleared rows back — zombie data, not a duplicate. `DROP` followed by `UNDROP`
restores the same table id and regions, so the table-existence check alone misses
it; `DROP` followed by `CREATE` is safe because ids are never reused. A
frontend-side discard hook cannot work: a DDL through one frontend cannot purge
another frontend's directory.

The fence is a table DDL epoch, persisted with the table metadata and bumped
before the destructive step by `TRUNCATE`, `DROP`, `UNDROP` and destructive schema
changes; bumping first means a crash discards compensation instead of
resurrecting data. A repartition does not bump it — it rebuilds the record — and
for `DROP` the bumped epoch goes into the tombstone `UNDROP` restores.

Replay checks the table exists, that the record's epoch is the current one, and
that the route still places the record where it was split (a leader change only
needs the current peer; a layout change needs a rebuild, told apart by the
rebuilt target region, not the route version, which moves for both). A missing
table is a terminal dead letter, never a name-based rebuild.

Those checks race with the DDL, so the record also carries its epoch in the
request header and the region enforces the fence at apply time:

- The request is marked as a queue replay; table epochs start at 1, so 0 in the
  header means "not a queue replay".
- A truncate carries the new epoch, and the region persists its truncate epoch in
  the same manifest action that clears the data and rejects older queue replays,
  ordered with the truncate, in the apply path.
- A new region, and a region restored by `UNDROP`, start at the table's current
  epoch, so a rebuilt record cannot bypass the fence.
- Only queue replays are checked: rejecting live writes would turn a lagging
  frontend cache into client errors.

**This is a merge gate.** The deciding test is not a unit test of the comparison:
truncate a table with a pending record, let replay run, and assert the rows do not
come back, the same for `UNDROP` and for a repartition that moves the region. Any
bump discards queued compensation for the table, visible only as a late write the
client was already told failed. DML `DELETE` is not fenced, the same at-least-once
exposure as a replay overwriting a newer write.

## Failure handling

| Situation | Enqueue | Client | Queue |
| --- | --- | --- | --- |
| all remaining failures retryable, handoff succeeds | handed off | original error | writer appends; pending and replayable |
| queue full, or a record over the size bound | rejected, counted; a bulk insert is rejected before it is encoded | original error | saturation stays off the request path |
| append fails or has an unknown outcome, or the frontend exits first | already handed off | original error | dropped or ambiguous; a client retry may duplicate |
| terminal failure mixed with retryable ones | siblings not queued | terminal error | unchanged |
| client retries while its record is queued | - | its own retry | both can apply; duplicates possible |
| replay `Retryable` / `RetryAfterRouteRefresh` | - | - | attempts++ and backoff / caches refreshed and retried |
| replay `NonRetryable`, TTL expired, attempts exhausted | - | - | dead letter |
| truncate, drop, undrop or schema change after enqueue | - | - | epoch mismatch or missing table: discarded as a DDL dead letter |
| partition layout changed, or replay reaches a newer truncate epoch | - | - | rebuilt, or rejected by the region |
| frontend crashes / the pod and its disk are lost | - | - | replayed from the last compaction; a lost disk is unrecoverable |
| queue directory cannot be opened at startup | queue not running | startup fails | directory untouched; the frontend stays down |
| queue filesystem nearly full | handoff keeps shedding | original error | v1 frees nothing; purge is the only reclaim |

## Compatibility

A build without the queue ignores the `[write_queue]` section, because the
options struct does not reject unknown fields: a downgrade stops the queue
silently, and the records already on disk stay unread until an upgraded frontend
returns with the same `dir`. A queue directory is owned by one frontend process
and the envelope carries an explicit version, so a reader never has to guess what
it is reading; since no released frontend writes queue records yet, that is
forward-looking.

The compatibility surface today is the DDL state the fence persists: `ddl_epoch`
and `route_version` in the envelope, a replay epoch in the request header, an
epoch in the truncate request, and a truncate epoch in the region manifest
action. The manifest field and the table epoch are serde-defaulted, so older data
reads as epoch 0 and epoch 1 respectively; both need a compatibility case. The
fence only holds once every datanode honors the epoch, so enable the queue after
the fence has rolled out, not during it.

A replayed duplicate is invisible on a table that deduplicates: it lands on the
same primary key and timestamp as the original, and the higher sequence wins.
`append_mode = true` turns deduplication off, so on those tables a duplicate
stays a second row and any count, sum or average over those rows is inflated;
metrics tables deduplicate by default, log-style append-mode tables do not.

## Configuration

Names and defaults are a proposal.

```toml
[write_queue]
enable = false          # the client response does not change
dir = ""                # required; one per frontend; give it its own volume
max_bytes = "1GB"       # bounds pending payload bytes and the handoff channel
max_records = 100000
max_record_size = "16MB"
ttl = "30m"
max_attempts = 10       # not persisted; ttl is the bound
replay_concurrency = 16
sync_write = false      # true adds an fsync per append batch
```

`dir` must survive a process restart and has no sensible default because the
frontend has no data home today. The queue and `write_retry.max_retries` are
independent, except that a zero budget does not install `WriteRetry` at all, so
the frontend installs it whenever the queue is enabled: `max_retries = 0` then
means "enqueue the first retryable failure". Standalone rejects `enable = true`.
On a frontend whose `dir` cannot survive a restart the queue would die with the
pod, so raising `write_retry.max_retries` is the whole answer. Give `dir` its own
volume: under a cluster-wide outage the queue writes continuously, and a
directory that shares a device with query spill or other local I/O turns that
into query latency.

## Lifecycle and wiring

`Frontend` gains the queue when enabled: it registers the writer and replay
tasks on the shared runtimes, builds the dedicated store under `write_queue.dir`,
and passes an optional handle through the adapter. The frontend gains a `log-store` dependency for the first
time; `operator` depends on the queue crate, not on `log-store`, so the adapter
stays in the frontend.

If the store cannot be opened or recovered, startup fails. That has an
availability cost, because of the exclusive lock: a previous pod that is still
running, or two frontends on one directory, keeps the new frontend down in
`CrashLoopBackOff`. An OOM kill or `SIGKILL` is not one of these — the kernel
drops the lock when the process dies. Starting with the queue disabled instead is
recorded as a question. On shutdown the worker stops leasing, settles in-flight
sends and stops the store; pending records remain for the next start.

The metrics to export are handed-off records and bytes with sheds by reason;
writer append failures and batch size; pending records, bytes and oldest age;
replay attempts by outcome; dead letters by reason; free bytes on the queue
filesystem and the store's on-disk size, which is not `max_bytes`; and handoff and
replay latency. Region and table ids are not labels; they appear in the
dead-letter log line with the row count.

# Alternatives

| Alternative | Why not |
| --- | --- |
| New append-only segment format | Exact control over framing and compaction, but a second crash-safe log with its own segmentation, checksums, torn-tail recovery and GC. |
| Object-store WAL | Survives pod loss, but `ObjectStoreLogStore` is experimental, every enqueue pays a round trip, and a shared prefix needs fencing between frontends. |
| In-process retries only (raising `max_retries`) | The first, zero-cost option, but it cannot outlive the client's timeout or a frontend restart. |
| Enqueue at the gRPC entry | Lacks the per-region split and `RetryHint`, and would enqueue never-rejected requests. |
| Append inline on the request path | Blocks the calling task on a store that backpressures when its disk falls behind. |
| `spawn_blocking` per enqueue | Moves the blocking call off the worker but not the load, and the request still waits. A semaphore leaves one permit for per-region FIFO: a single writer without coalescing. |
| Wait for the append before returning | Restores "durable when the response returns", but every failed request pays the deadline, which only delays the shed. Can be layered on the handoff later. |
| Several writers sharded by region | Raises storm absorption, but the budget, recovery and shutdown must coordinate across shards; a tuning step if one writer proves to be the ceiling. |
| Datanode-side queue, or a sidecar | The datanode is often the down component, and a sidecar adds a deployment component and a hop while still needing the same semantics. |
| Acknowledge a queued write as accepted | Relaxes the counter so an accepted write can be dropped by TTL or a lost pod. The distinct pending outcome is the recorded later phase. |

# Drawbacks

- The client is told the write failed while it may still land later, so a
  retrying client may duplicate rows; at-least-once covers a crash between apply
  and compaction, an ambiguous append or an RPC.
- A stale replay can override a newer live write on the same primary key and
  timestamp; the window extends to the TTL.
- Any invalidating DDL discards queued compensation for the table, so a table
  with frequent DDL keeps losing records; a row that cannot be placed is lost.
- The queue is local disk: an ephemeral queue dies with a pod reschedule, node
  loss is not covered, and `sync_write = false` does not cover power loss.
- The failure path becomes a load path: under an outage every failed write is
  copied and written to disk until a bound, competing with the rest of the
  frontend, and each concurrent failed request briefly holds its own encoded
  buffer before the byte budget sees it. Isolation keeps that off the request
  path; it is not free.
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

1. The client idempotency key with region-side deduplication, and a replay-side
   newer-write check to close the stale window.
2. The acknowledged contract (position B): a pending outcome across protocols and
   a queue that survives pod loss with fencing.
3. An object-store-backed queue, persisted attempts and dead letters, an
   inspect/replay API, an export of dead letters to object storage for offline
   analysis and replay after the queue has dropped them, an optional local purge
   hook on DDL, and per-row filtering for a time-range truncate.
4. Per-datanode replay rate limiting and circuit breaking, per-table fair
   queueing, preserving the original receive time for ordering against later
   writes, and standalone support if it ever enables remote retries.

# Unresolved questions

- **Store open failure.** An unopenable directory fails startup today, so an
  optional feature can keep the frontend down, but compensation is never silently
  dropped. Start with the queue disabled and retry the open instead, accepting a
  window in which the operator believes the queue is on?
- **Store runtime ownership.** The store starts its sync and GC tasks on the
  global runtime, its append blocks the calling task unless the write moves to
  `spawn_blocking`, and its `read` blocks a global worker per call, so replay can
  contend with queries on a slow disk. Must the fix — `spawn_blocking` around the
  append and the read — land before the feature can be enabled by default?
- **Disk low-water mark.** Should v1 discard the oldest pending records below a
  filesystem threshold? Rejecting records frees nothing, and leaving it out means
  a full host disk fails the frontend's other writes first.
- **Poison quarantine and replay pacing.** Is quarantining a record after two
  consecutive peer-unreachable attempts enough, or is a per-datanode circuit
  breaker needed in v1 to keep one record from failing the same node repeatedly?
