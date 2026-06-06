---
Feature Name: Pipelined WAL Write in Mito Region Worker
Tracking Issue: TBD
Date: 2026-06-06
Author: "lyang24 <lanqingy93@gmail.com>"
---

# Summary

This RFC proposes pipelining the write path of the mito region worker so that the
WAL append of batch `N` overlaps with the preparation (and optionally the WAL
encoding) of batch `N+1`, instead of the current fully serial
`prepare → encode → append → memtable` loop. Measurements on a saturated single
region show the WAL stage consumes up to 92% of worker time, of which roughly
one third is parallelizable pure-CPU protobuf encoding. The estimated throughput
ceiling improvement is +9% to +31% for the minimal variant and substantially
higher when encoding is moved off the serial path or when the WAL backend has
high latency (e.g. Kafka).

# Motivation

## The write path is serial per worker

Each region is owned by exactly one region worker, and
`RegionWorkerLoop::handle_write_requests` processes a batch fully serially:

1. `prepare_region_write_ctx` — group requests by region, assign sequences;
2. `WalWriter::add_entry` — protobuf-encode every region's `WalEntry`;
3. `wal_writer.write_to_wal().await` — one `append_batch` to the log store;
4. write memtables, advance `committed_sequence`, ack callers.

While step 3 is in flight the worker cannot start step 1 or 2 for the next
batch; requests accumulate in the channel. Group commit (batching) amortizes
the *number* of WAL operations, but nothing overlaps the *time* a WAL operation
takes.

## Measurements

All numbers below were collected on a standalone release build
(RaftEngine WAL on a local SATA SSD, default config unless noted), and re-validated
against `main` at `7baa4f685`. The methodology is reproducible: Prometheus
remote-write load plus a direct ingest loader, with deltas of
`greptime_mito_write_stage_elapsed`, `greptime_logstore_op_elapsed` and
`greptime_mito_write_rows_total` over fixed windows.

**1. WAL stage share grows with load and dominates at saturation.**

| Load shape | Avg batch size (rows) | WAL share of write path |
|---|---|---|
| Light remote write (~8k rows/s) | ~2,000 | 57% |
| Heavy remote write (~70k rows/s) | ~26,000 | 78% |
| Saturated single region (~800k rows/s) | ~156,000 | **92%** |

The mechanism is clear from per-row costs: memtable writes amortize with batch
size (0.65 µs/row at 2k-row batches → 0.10 µs/row at 156k-row batches) while
WAL cost stays flat at ~1.0–1.1 µs/row because it is bound by payload bytes
(encode + copy + append). The better group commit works, the more the WAL stage
dominates — batching cannot amortize it further.

**2. Single-region throughput ceiling is the serial WAL stage.**

A concurrency ramp against one region (one table, influx line protocol):

| Writers | Server rows/s | WAL ms/batch | Memtable ms/batch | Worker busy |
|---|---|---|---|---|
| 16 | 774,821 | 33.1 | 10.1 | 99.6% |
| 32 | 787,442 | 85.1 | 12.0 | 99.7% |
| 64 | 796,815 | 179.7 | 15.9 | 99.6% |

Throughput plateaus at ~800k rows/s with the worker pegged at ~100% busy —
the ceiling is the worker's serial loop, not the storage device or memtables.

**3. A large share of the WAL stage is parallelizable CPU work.**

Subtracting `logstore_op_elapsed{optype="append_batch"}` from the
`write_wal` stage over the same window. The encode share is workload-dependent:

| Workload | Encode (pure CPU, serial) | raft-engine `append_batch` |
|---|---|---|
| Remote write, hundreds of small regions (14.7k-row batches) | 3.9 ms/batch (~31%) | 8.6 ms/batch (~69%) |
| Single hot region (33.7k-row batches) | 21.5 ms/batch (~63%) | 12.5 ms/batch (~37%) |
| Single hot region (156k-row batches) | 99.6 ms/batch (~58%) | 71.5 ms/batch (~42%) |

Encode cost is flat per row (~0.26 µs/row multi-region, ~0.64 µs/row for the
wider single-table schema) because prost serialization walks the payload twice
(`encoded_len` + encode). In the single-hot-region shape — the shape that hits
the per-worker ceiling first — encoding is the *majority* of the WAL stage.

A per-batch fsync (only if `sync_write=true`) adds ~4.2–4.5 ms/batch
(device flush latency) and costs ~8% throughput at the 16-writer shape.

**4. Remote WAL makes this worse.** With a Kafka WAL the append is a
network round trip (typically milliseconds). Every millisecond of append
latency is currently a millisecond the worker cannot prepare, encode, or apply
anything. The pipeline benefit scales with WAL latency.

# Details

## Proposed design

Split the write loop into pipeline stages with explicit hand-off:

```
Stage S (serial, cheap):  drain channel → group by region → assign sequence /
                          entry ids from worker-local counters
Stage E (parallel):       protobuf-encode WalEntry per region
Stage A (serial, ordered): logstore append_batch  ── the only stage that must
                          remain strictly ordered per region
Stage M (parallel):       write memtables, advance committed_sequence in batch
                          order, ack senders
```

While batch `N` is in stage A (awaiting the log store), the worker runs stage
S/E for batch `N+1`. Stage M for batch `N` runs concurrently with stage A for
batch `N+1` (this matches RocksDB's pipelined write design).

### Invariants that must be preserved

1. **WAL-before-memtable durability**: a batch's memtable apply still only
   happens after its own `append_batch` succeeds. Pipelining reorders work
   *across* batches, never within one.
2. **Per-region WAL ordering**: stage A is serial; entry ids remain dense and
   ordered because they are assigned in stage S in admission order.
3. **Sequence allocation without version_control reads**: today
   `RegionWriteCtx::new` reads `committed_sequence` from `VersionControl`,
   which is only advanced after the memtable write. The worker must instead
   maintain per-region running counters (`next_sequence`, `next_entry_id`) so
   stage S of batch `N+1` does not depend on stage M of batch `N`. Counters
   resynchronize from `VersionControl` on failure or region reopen.
4. **Failure propagation**: if `append_batch` for batch `N` fails, all
   in-flight successors (`N+1` prepared/encoded, not yet appended) for the
   affected regions must also fail, and the worker-local counters reset from
   `VersionControl`. This mirrors the current behavior where a WAL error fails
   the whole `RegionWriteCtx`.
5. **Visibility order**: `committed_sequence` advances in batch order even if
   stage M tasks complete out of order (a small reorder buffer or per-region
   chain suffices).
6. **Barriers for non-write requests**: DDL, alter, flush, and region state
   changes drain the pipeline before executing, preserving today's "worker as
   serialization point" semantics. The stall/reject backpressure checks stay at
   admission (stage S) and must account for bytes held by in-flight batches.

### Staging the work

- **Phase 1 (minimal pipeline)**: overlap stage A of batch `N` with stage S +
  M of neighbors; encode stays inside the serial path. Measured ceiling:
  +9% (very large batches) to +31% (mid-size batches) on a local SATA SSD.
  Note on hardware: the async-mode append stage is dominated by memcpy and
  raft-engine internals rather than device latency, so these shares should
  transfer to faster storage; the sync-mode fsync figure (~4.2 ms) is
  SATA-class and would shrink on NVMe-class devices, further increasing the
  encode share of the WAL stage.
- **Phase 2 (encode offload)**: move stage E to parallel tasks (encoding only
  needs sequences, which stage S supplies). Removes 31–63% of the WAL stage
  from the serial path depending on workload shape; at the measured
  single-hot-region saturation shape the combined ceiling is
  `(171.1+15.7)/max(99.6, 71.5) ≈ +88%`. Higher still on Kafka WAL. Phase 2
  only lands if Phase 1's measured win and added complexity justify it.

## Expected benefits

- Raises the per-region (per-worker) ingestion ceiling without changing the
  storage format, WAL format, or client protocol.
- Benefit grows with exactly the deployments that hurt today: large batches
  (group commit working well) and remote/Kafka WAL (high append latency).
- No effect on the read path; no new persistent state; recovery semantics
  unchanged (WAL content and ordering are identical to today).

# Drawbacks

- **Worker loop complexity**: the simple `loop { recv; handle }` becomes a
  small state machine with in-flight batches; error paths multiply
  (append failure with successors in flight, shutdown with a non-empty
  pipeline, region close/drop mid-pipeline).
- **Memory**: up to one extra batch (requests + encoded WAL bytes) held per
  worker. With 156k-row batches this was ~tens of MB engine-wide in our runs;
  needs accounting in the stall/reject thresholds.
- **Latency vs throughput**: individual request latency does not improve; a
  request acks after its own batch's stages complete. Under light load the
  pipeline is empty and behavior is identical to today.
- **Sequence counter duplication**: worker-local counters introduce a second
  source of truth for `next_sequence`/`next_entry_id`; bugs here corrupt
  ordering. Mitigated by debug assertions against `VersionControl` and reset
  on every failure.
- **Testing burden**: needs fault-injection tests (WAL failure mid-pipeline,
  DDL barrier, shutdown) in addition to throughput benchmarks.

# Alternatives

- **Do nothing / scale by region count.** Multi-region tables already spread
  load across workers. This works but does not help single-hot-region
  workloads (a common shape: one big metrics/log table per tenant), and burns
  CPU on more workers rather than using each worker fully.
- **Parallel encode only (no pipeline).** Encode the per-region `WalEntry`s
  concurrently inside the current serial loop. Simpler, captures ~31% of the
  WAL stage at most, leaves the append latency fully serial. Could be a
  stepping stone; rejected as the end state because Kafka WAL gains nothing.
- **io_uring for the WAL.** Rejected: the WAL is a single sequential append
  stream already group-committed — per-batch syscall overhead is ~µs against
  an 8.6 ms append stage dominated by memcpy and raft-engine internals;
  fsync latency is device-bound and can only be overlapped (which the
  pipeline already achieves at the application level, without touching
  raft-engine or the async runtime).
- **Swap or modify the WAL backend** (e.g. raft-engine internal pipelining,
  alternative log libraries). Highest cost and risk; the measured serial-stage
  problem lives in mito's worker loop, not in raft-engine's internals, so fix
  it where it is.

# Unresolved questions

- Exact interaction with the bulk ingest path (`write_bulk` /
  `BulkPart`), which shares the WAL writer but has its own memtable apply.
- Whether Phase 2's encode offload should reuse the existing blocking thread
  pool (`spawn_blocking_global`) or a dedicated pool, given flush/compaction
  also compete for it.
- Whether the pipeline depth should be fixed at 2 (one in-flight append) or
  configurable; measurements so far suggest depth 2 captures nearly all of the
  win on local disk, but Kafka WAL may benefit from more.
