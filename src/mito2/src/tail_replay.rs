// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Background replay of the pending WAL tails of a region into SSTs.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common_base::cancellation::CancellableFuture;
use common_telemetry::{error, info, warn};
use futures::TryStreamExt;
use snafu::ensure;
use store_api::logstore::{LogStore, WalReadUpperBound};
use store_api::storage::RegionId;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::Sender;

use crate::config::MitoConfig;
use crate::error::{Error, FlushCancelledSnafu, Result, TailEntryMissingSnafu};
use crate::flush::MemtableSstWriter;
use crate::manifest::action::{
    PendingTail, RegionEdit, RegionMetaAction, RegionMetaActionList, UptoKind, WalRecoveryEdit,
    cap_wal_release,
};
use crate::memtable::time_partition::TimePartitions;
use crate::metrics::TAIL_REPLAY_FAILURE_TOTAL;
use crate::region::opener::replay_entry;
use crate::region::version::{VersionBuilder, VersionControl, VersionControlRef};
use crate::region::{MitoRegion, MitoRegionRef, TailPublication};
use crate::request::{BackgroundNotify, WorkerRequest, WorkerRequestWithTime};
use crate::schedule::CancellableTaskState;
use crate::sst::file::{FileMeta, UncommittedSsts};
use crate::wal::{EntryId, Wal, WalEntryStream};
use crate::worker::WorkerListener;

/// Memtable usage at which replayed rows are written to SSTs.
const CHUNK_MEMTABLE_USAGE: usize = 64 * 1024 * 1024;
/// Replays that read and write at once on a node.
const MAX_CONCURRENT_TAIL_REPLAYS: usize = 16;
/// The replay chunks of a node may use `1 / TAIL_REPLAY_MEMORY_FACTOR` of the global write
/// buffer size.
const TAIL_REPLAY_MEMORY_FACTOR: usize = 8;
/// Delay before the first retry of a failed round or an unfinished commit.
const MIN_BACKOFF: Duration = Duration::from_secs(1);
/// Longest delay between retries.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// How often a replay checks whether the region received a Leader lease.
const LEASE_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// Returns whether the pending tails of `region` are to be replayed: it has some and
/// [can replay them](MitoRegion::can_replay_tails).
pub(crate) fn should_replay_tails(region: &MitoRegion) -> bool {
    region.can_replay_tails() && region.version().has_pending_tails()
}

/// Replays the pending tails of a region into SSTs and publishes them in one manifest edit.
///
/// Each round reads the tails in order and writes each into its own SSTs: an SST can give all its
/// rows its largest sequence, so in an SST shared with a later tail, an earlier tail's rows would
/// take the later tail's sequence and outrank rows that a leader between the two tails flushed. A
/// tail's chunks only bound memory: writes accepted during the replay already take sequences
/// above every tail's. A chunk is written once it reaches 64 MiB, or once it grows above its floor
/// and the node's [TailReplayMemory] pool cannot cover the growth. The edit adds the SSTs, clears
/// the tails and advances the flushed entry id and sequence. Another node's listed tail is only a
/// bound, so until the region receives a Leader lease after a catchup made it writable, the edit
/// keeps its unread part, which a second edit clears once a lease arrives. A failed round is
/// retried after a back-off while [should_replay_tails] holds, unless the storage holds a newer
/// manifest: then the task fences the region for a reload and ends. The task always reports its
/// end to the worker.
pub(crate) struct TailReplayTask<S> {
    pub(crate) region: MitoRegionRef,
    /// Id of this node, whose tails are read from `wal`.
    pub(crate) node_id: Option<u64>,
    pub(crate) wal: Wal<S>,
    /// Writes the chunks.
    pub(crate) sst_writer: MemtableSstWriter,
    pub(crate) listener: WorkerListener,
    pub(crate) request_sender: Sender<WorkerRequestWithTime>,
    /// Limits of the node's replays. Its write semaphore is the one `sst_writer` holds.
    pub(crate) limits: TailReplayLimits,
    pub(crate) state: CancellableTaskState,
}

impl<S: LogStore> TailReplayTask<S> {
    pub(crate) async fn run(self) {
        let region_id = self.region.region_id;
        let _ended = TailReplayEndGuard {
            sender: self.request_sender.clone(),
            region_id,
        };
        let mut backoff = MIN_BACKOFF;
        while !self.state.is_cancelled() && should_replay_tails(&self.region) {
            match self.replay_round().await {
                Ok(()) => return,
                Err(_) if self.state.is_cancelled() => break,
                Err(e) => {
                    error!(e; "Failed to replay the pending WAL tails of region {region_id}");
                    TAIL_REPLAY_FAILURE_TOTAL.inc();
                    self.listener.on_tail_replay_failed(region_id, &e);
                    // The tails may be gone from a newer manifest, e.g. after another writer
                    // committed them and their source released its WAL. The worker reloads the
                    // fenced region once this task ends.
                    match self.fence_if_manifest_updated().await {
                        Ok(true) => return,
                        Ok(false) => {}
                        Err(_) if self.state.is_cancelled() => break,
                        Err(e) => {
                            warn!(e; "Failed to check region {region_id} for a newer manifest");
                        }
                    }
                }
            }
            if !self.sleep(backoff).await {
                break;
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
        info!(
            "Region {region_id} stopped replaying its pending WAL tails before they were published"
        );
    }

    /// Fences the region for a reload if the storage holds a newer manifest. Returns whether
    /// it fenced the region. Cancellation ends it until it holds the manifest write lock.
    async fn fence_if_manifest_updated(&self) -> Result<bool> {
        let manifest_ctx = &self.region.manifest_ctx;
        let Some(installed) = self
            .unless_cancelled(manifest_ctx.installed_version_if_updated())
            .await?
        else {
            return Ok(false);
        };
        let region_id = self.region.region_id;
        info!(
            "Region {region_id} found a manifest newer than its version {installed} after a failed replay round, fencing it"
        );
        self.listener.on_tail_fence_begin(region_id);
        let mut manager = self
            .unless_cancelled(async { Ok(manifest_ctx.manifest_manager.write().await) })
            .await?;
        Ok(manifest_ctx
            .fence_for_reload_if_unchanged(&mut manager, region_id, installed)
            .await)
    }

    /// Replays the current tails and publishes the result. An error asks for another round.
    async fn replay_round(&self) -> Result<()> {
        let region_id = self.region.region_id;
        let replay_semaphore = &self.limits.replay_semaphore;
        let permit = match replay_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                info!("Region {region_id} waits for a tail replay permit");
                self.unless_cancelled(async { Ok(replay_semaphore.acquire().await.unwrap()) })
                    .await?
            }
        };
        // Keeps no version past this point: it would hold the region's memtables, and their
        // write buffer memory, after a flush released them.
        let (tails, flushed_entry_id) = {
            let version = self.region.version();
            (version.pending_tails.clone(), version.flushed_entry_id)
        };
        let uncommitted = UncommittedSsts::new(
            region_id,
            self.region.access_layer.clone(),
            Some(self.sst_writer.cache_manager.clone()),
        );
        let replayed = self.replay_tails(&tails, &uncommitted).await;
        drop(permit);
        let (files, last_entry_ids) = match replayed {
            Ok(replayed) => replayed,
            Err(e) => {
                uncommitted.cleanup().await;
                return Err(e);
            }
        };
        let lease_received = self.region.manifest_ctx.leader_lease_received();
        let remaining = tails
            .iter()
            .zip(&last_entry_ids)
            .filter_map(|(tail, last_entry_id)| {
                self.remaining_tail(tail, *last_entry_id, lease_received)
            })
            .collect::<Vec<_>>();
        if !files.is_empty() || remaining != tails {
            let last_entry_id = last_entry_ids.into_iter().flatten().max().unwrap_or(0);
            let flushed_sequence = files
                .iter()
                .filter_map(|file| file.sequence)
                .map(NonZeroU64::get)
                .max();
            let edit = RegionEdit {
                flushed_sequence,
                files_to_add: files,
                files_to_remove: Vec::new(),
                timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
                compaction_time_window: None,
                flushed_entry_id: (last_entry_id > flushed_entry_id).then_some(last_entry_id),
                // Later writes of the region take sequences above the replayed rows.
                committed_sequence: flushed_sequence,
                wal_recovery: WalRecoveryEdit {
                    pending_tails: Some(remaining.clone()),
                    handoff_floor: None,
                },
            };
            if !self.publish(&tails, edit, &uncommitted).await? {
                return Ok(());
            }
        }
        if remaining.is_empty() || !self.wait_for_leader_lease().await {
            return Ok(());
        }
        let edit = RegionEdit {
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            wal_recovery: WalRecoveryEdit {
                pending_tails: Some(Vec::new()),
                handoff_floor: None,
            },
            ..Default::default()
        };
        self.publish(&remaining, edit, &uncommitted).await?;
        Ok(())
    }

    /// Returns what stays recorded of `tail` after a replay read it up to `last_entry_id`.
    /// Only another node's listed tail stays, from its last entry read, and only until the
    /// region receives a Leader lease.
    fn remaining_tail(
        &self,
        tail: &PendingTail,
        last_entry_id: Option<EntryId>,
        lease_received: bool,
    ) -> Option<PendingTail> {
        if lease_received
            || tail.upto_kind == UptoKind::Reported
            || Some(tail.node_id) == self.node_id
        {
            return None;
        }
        let after = last_entry_id.unwrap_or(tail.after);
        (after < tail.upto).then(|| PendingTail {
            after,
            ..tail.clone()
        })
    }

    /// Replays `tails` into chunks of SSTs. Returns the SSTs and the last entry id read of
    /// each tail.
    async fn replay_tails(
        &self,
        tails: &[PendingTail],
        uncommitted: &UncommittedSsts,
    ) -> Result<(Vec<FileMeta>, Vec<Option<EntryId>>)> {
        let region_id = self.region.region_id;
        let provider = &self.region.provider;
        let mut files = Vec::new();
        let mut last_entry_ids = Vec::with_capacity(tails.len());
        for tail in tails {
            let mut entries = self.unless_cancelled(self.read_tail_entries(tail)).await?;
            let mut chunk = self.new_chunk_version();
            let mut reservation = self.limits.memory.chunk_reservation();
            let mut last_entry_id = None;
            while let Some((entry_id, entry)) = self.unless_cancelled(entries.try_next()).await? {
                if entry_id > tail.upto {
                    break;
                }
                replay_entry(provider, region_id, &chunk, entry, entry_id).await?;
                last_entry_id = Some(entry_id);
                let usage = chunk.current().version.memtables.mutable_usage();
                if usage >= CHUNK_MEMTABLE_USAGE || !reservation.grow_to(usage) {
                    files.extend(self.write_chunk_ssts(&chunk, uncommitted).await?);
                    chunk = self.new_chunk_version();
                    reservation.clear();
                }
                if entry_id == tail.upto {
                    break;
                }
            }
            ensure!(
                last_entry_id == Some(tail.upto) || tail.upto_kind == UptoKind::Listed,
                TailEntryMissingSnafu {
                    region_id,
                    node_id: tail.node_id,
                    entry_id: tail.upto,
                }
            );
            files.extend(self.write_chunk_ssts(&chunk, uncommitted).await?);
            last_entry_ids.push(last_entry_id);
        }
        Ok((files, last_entry_ids))
    }

    /// Streams the region's entries of `tail`, starting after `tail.after`. This node's reported
    /// tail comes from its own WAL, whose stream may go past `tail.upto`. Every other tail is read
    /// up to its bound through [Wal::read_node], since a listed bound may name no entry.
    async fn read_tail_entries(&self, tail: &PendingTail) -> Result<WalEntryStream<'static>> {
        let (provider, region_id) = (&self.region.provider, self.region.region_id);
        let upper = match tail.upto_kind {
            UptoKind::Reported if Some(tail.node_id) == self.node_id => {
                let mut reader = self.wal.wal_entry_reader(provider, region_id);
                return reader.read(provider, tail.after + 1);
            }
            UptoKind::Reported => WalReadUpperBound::LastEntry(tail.upto),
            UptoKind::Listed => WalReadUpperBound::Listed(tail.upto),
        };
        self.wal
            .read_node(region_id, tail.node_id, tail.after, upper)
            .await
    }

    /// Waits until the region receives a Leader lease. Returns false if the region stops being
    /// a writable, altering or editing leader, or the task is cancelled, first.
    async fn wait_for_leader_lease(&self) -> bool {
        let region_id = self.region.region_id;
        info!("Region {region_id} waits for a Leader lease to clear its unread listed tails");
        while self.region.can_replay_tails() {
            self.listener.on_tail_lease_check(region_id);
            if self.region.manifest_ctx.leader_lease_received() {
                return true;
            }
            if !self.sleep(LEASE_CHECK_INTERVAL).await {
                info!("Region {region_id} cancelled its tail replay while waiting for a lease");
                return false;
            }
        }
        info!("Region {region_id} can no longer replay, keeping its unread listed tails");
        false
    }

    /// Returns a scratch version with an empty memtable for one chunk of replayed rows.
    fn new_chunk_version(&self) -> VersionControlRef {
        let version = self.region.version();
        let mutable = TimePartitions::new(
            version.metadata.clone(),
            version.memtables.mutable.memtable_builder().clone(),
            0,
            Some(version.memtables.mutable.part_duration()),
        );
        Arc::new(VersionControl::new(
            VersionBuilder::new(version.metadata.clone(), mutable)
                .options(version.options.clone())
                .build(),
        ))
    }

    /// Writes the rows of `chunk` to SSTs.
    async fn write_chunk_ssts(
        &self,
        chunk: &VersionControl,
        uncommitted: &UncommittedSsts,
    ) -> Result<Vec<FileMeta>> {
        chunk.freeze_mutable()?;
        let version = chunk.current().version;
        if version.memtables.immutables().is_empty() {
            return Ok(Vec::new());
        }
        self.sst_writer
            .write_ssts(&version, &self.state, uncommitted, "TailReplay")
            .await
    }

    /// Commits `edit` for the replayed `tails`. It commits the same content again while the
    /// result is unknown or the region is busy. Returns whether it committed; if not, the task
    /// ends. An error asks for another round.
    async fn publish(
        &self,
        tails: &[PendingTail],
        edit: RegionEdit,
        uncommitted: &UncommittedSsts,
    ) -> Result<bool> {
        let region_id = self.region.region_id;
        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        let mut backoff = MIN_BACKOFF;
        // Whether an attempt may have persisted the edit, which only the same content confirms.
        let mut uncertain = false;
        loop {
            self.listener.on_tail_commit_begin(region_id).await;
            if self.state.is_cancelled() {
                info!("Region {region_id} cancelled its tail replay before publishing");
                uncommitted.cleanup().await;
                return Ok(false);
            }
            match self
                .region
                .manifest_ctx
                .update_manifest_for_tails(tails, action_list.clone())
                .await
            {
                Ok(TailPublication::Committed(manifest_version)) => {
                    uncommitted.disarm_cleanup();
                    info!(
                        "Published the replayed WAL tails of region {region_id} in manifest version {manifest_version}"
                    );
                    self.release_wal().await;
                    let region = self.region.clone();
                    let tails = tails.to_vec();
                    let notify = BackgroundNotify::TailReplayCommitted {
                        region,
                        edit,
                        tails,
                    };
                    notify_worker(&self.request_sender, region_id, notify).await;
                    return Ok(true);
                }
                Ok(TailPublication::Busy) => {}
                Ok(TailPublication::Stale) => {
                    info!("Region {region_id} changed its role or tails, dropping the replay");
                    uncommitted.cleanup().await;
                    return Ok(false);
                }
                // The commit fenced the region, which the worker reloads once this task ends.
                Err(e @ Error::ManifestVersionConflict { .. }) => {
                    uncommitted.disarm_cleanup();
                    warn!(e; "Publishing the replayed WAL tails of region {region_id} conflicted");
                    return Ok(false);
                }
                Err(e) if e.may_have_persisted_manifest_update() => {
                    uncommitted.disarm_cleanup();
                    uncertain = true;
                    error!(e; "Publishing the replayed WAL tails of region {region_id} may have failed, retrying");
                }
                Err(Error::RegionStopped { .. }) => {
                    info!("Region {region_id} stopped, dropping the replay");
                    uncommitted.cleanup().await;
                    return Ok(false);
                }
                Err(e) if uncertain => {
                    error!(e; "Failed to publish the replayed WAL tails of region {region_id} again, retrying");
                }
                Err(e) => {
                    uncommitted.cleanup().await;
                    return Err(e);
                }
            }
            if !self.sleep(backoff).await {
                info!(
                    "Region {region_id} cancelled its tail replay while retrying the publication"
                );
                uncommitted.cleanup().await;
                return Ok(false);
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// Releases the WAL up to where the region now replays it on open, keeping this node's
    /// remaining tails. A failure is only logged.
    async fn release_wal(&self) {
        let region = &self.region;
        let manifest = region.manifest_ctx.manifest().await;
        let entry_id = cap_wal_release(
            &manifest.pending_tails,
            self.node_id,
            manifest.wal_replay_start(self.node_id),
        );
        if let Err(e) = self
            .wal
            .obsolete(region.region_id, entry_id, &region.provider)
            .await
        {
            error!(e; "Failed to release the WAL of region {} after publishing its tails", region.region_id);
        }
    }

    /// Awaits `future` unless the task is cancelled first.
    async fn unless_cancelled<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        CancellableFuture::new(future, self.state.cancel_handle())
            .await
            .unwrap_or_else(|_| FlushCancelledSnafu.fail())
    }

    /// Sleeps for `delay`. Returns false if the task is cancelled first.
    async fn sleep(&self, delay: Duration) -> bool {
        CancellableFuture::new(tokio::time::sleep(delay), self.state.cancel_handle())
            .await
            .is_ok()
    }
}

/// Sends `notify` about the region `region_id` to its worker.
pub(crate) async fn notify_worker(
    sender: &Sender<WorkerRequestWithTime>,
    region_id: RegionId,
    notify: BackgroundNotify,
) {
    let request = WorkerRequest::Background { region_id, notify };
    if sender
        .send(WorkerRequestWithTime::new(request))
        .await
        .is_err()
    {
        warn!("The worker of region {region_id} has stopped");
    }
}

/// Reports the end of a tail replay to the worker, however the task ends.
struct TailReplayEndGuard {
    sender: Sender<WorkerRequestWithTime>,
    region_id: RegionId,
}

impl Drop for TailReplayEndGuard {
    fn drop(&mut self) {
        let sender = self.sender.clone();
        let region_id = self.region_id;
        common_runtime::spawn_global(async move {
            notify_worker(&sender, region_id, BackgroundNotify::TailReplayEnded).await;
        });
    }
}

/// Bounds the tail replays of a node. They take none of the flush permits, while their chunk
/// memtables still count in the global write buffer.
#[derive(Clone)]
pub(crate) struct TailReplayLimits {
    /// Replays that read and write at once.
    replay_semaphore: Arc<Semaphore>,
    /// SST writes of the replays, as many as the flushes may run, so a node may write up to
    /// `2 * max_background_flushes` level 0 SSTs at once. Writer buffers are not part of the
    /// replay memory budget.
    pub(crate) write_semaphore: Arc<Semaphore>,
    /// Memtable memory of the replay chunks.
    memory: Arc<TailReplayMemory>,
}

impl TailReplayLimits {
    pub(crate) fn new(config: &MitoConfig) -> Self {
        Self {
            replay_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_TAIL_REPLAYS)),
            write_semaphore: Arc::new(Semaphore::new(config.max_background_flushes)),
            memory: Arc::new(TailReplayMemory::new(
                config.global_write_buffer_size.as_bytes() as usize / TAIL_REPLAY_MEMORY_FACTOR,
            )),
        }
    }

    /// Returns the bytes the replays reserve from their memory pool.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> usize {
        self.memory.reserved.load(Ordering::Relaxed)
    }
}

/// Memtable memory of the replay chunks on a node, out of a `budget`. The chunks share a pool of
/// half the budget above their floors, and each of the [MAX_CONCURRENT_TAIL_REPLAYS] replays may
/// always use a floor of an equal share of the other half.
struct TailReplayMemory {
    floor: usize,
    pool: usize,
    reserved: AtomicUsize,
}

impl TailReplayMemory {
    fn new(budget: usize) -> Self {
        Self {
            floor: budget / (2 * MAX_CONCURRENT_TAIL_REPLAYS),
            pool: budget / 2,
            reserved: AtomicUsize::new(0),
        }
    }

    /// Returns an empty reservation for one chunk.
    fn chunk_reservation(&self) -> ChunkReservation<'_> {
        ChunkReservation {
            memory: self,
            bytes: 0,
        }
    }

    /// Reserves `bytes` of the pool. Returns false if the pool has not enough left.
    fn try_reserve(&self, bytes: usize) -> bool {
        self.reserved
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |reserved| {
                (reserved + bytes <= self.pool).then_some(reserved + bytes)
            })
            .is_ok()
    }

    fn release(&self, bytes: usize) {
        self.reserved.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// The pool memory a chunk uses above its floor. It is released on drop.
struct ChunkReservation<'a> {
    memory: &'a TailReplayMemory,
    bytes: usize,
}

impl ChunkReservation<'_> {
    /// Grows the reservation to cover a chunk using `usage` bytes. Returns false if the pool
    /// has not enough left, in which case the reservation is unchanged.
    fn grow_to(&mut self, usage: usize) -> bool {
        let needed = usage.saturating_sub(self.memory.floor);
        if needed <= self.bytes {
            return true;
        }
        if !self.memory.try_reserve(needed - self.bytes) {
            return false;
        }
        self.bytes = needed;
        true
    }

    /// Releases the reservation for a new chunk.
    fn clear(&mut self) {
        self.memory.release(self.bytes);
        self.bytes = 0;
    }
}

impl Drop for ChunkReservation<'_> {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_reservation_keeps_floor_when_pool_is_exhausted() {
        // Floor 1 KiB, pool 16 KiB.
        let memory = TailReplayMemory::new(32 * 1024);
        let mut first = memory.chunk_reservation();
        let mut second = memory.chunk_reservation();
        assert!(first.grow_to(17 * 1024));
        assert!(!first.grow_to(17 * 1024 + 1));
        // The pool is used up, but another chunk still grows to its floor.
        assert!(second.grow_to(1024));
        assert!(!second.grow_to(1025));

        first.clear();
        assert!(second.grow_to(17 * 1024));
        drop(second);
        assert_eq!(0, memory.reserved.load(Ordering::Relaxed));
    }
}
