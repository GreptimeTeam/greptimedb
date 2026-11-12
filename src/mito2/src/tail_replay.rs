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
use std::time::Duration;

use common_base::cancellation::CancellableFuture;
use common_telemetry::{error, info, warn};
use futures::TryStreamExt;
use snafu::ensure;
use store_api::logstore::{LogStore, WalReadUpperBound};
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;

use crate::error::{Error, FlushCancelledSnafu, Result, TailEntryMissingSnafu};
use crate::flush::RegionFlushTask;
use crate::manifest::action::{
    PendingTail, RegionEdit, RegionMetaAction, RegionMetaActionList, UptoKind, WalRecoveryEdit,
    cap_wal_release,
};
use crate::memtable::time_partition::TimePartitions;
use crate::region::opener::replay_entry;
use crate::region::version::{VersionBuilder, VersionControl, VersionControlRef};
use crate::region::{MitoRegion, MitoRegionRef, TailPublication};
use crate::request::{BackgroundNotify, WorkerRequest, WorkerRequestWithTime};
use crate::schedule::CancellableTaskState;
use crate::sst::file::{FileMeta, UncommittedSsts};
use crate::wal::{EntryId, Wal, WalEntryStream};

/// Memtable usage at which replayed rows are written to SSTs.
const CHUNK_MEMTABLE_USAGE: usize = 64 * 1024 * 1024;
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
/// Each round reads the tails in order and writes the rows of each into its own chunks of
/// SSTs, since an SST can give all its rows its largest sequence. The edit adds the SSTs, clears
/// the tails and advances the flushed entry id and sequence. Another node's listed tail is
/// only a bound, so until the region receives a Leader lease after a catchup made it
/// writable, the edit keeps its unread part, which a second edit clears once a lease
/// arrives. A failed round is retried after a back-off for as long as [should_replay_tails]
/// holds. The task always reports its end to the worker.
pub(crate) struct TailReplayTask<S> {
    pub(crate) region: MitoRegionRef,
    /// Id of this node, whose tails are read from `wal`.
    pub(crate) node_id: Option<u64>,
    pub(crate) wal: Wal<S>,
    /// Writes the chunks. It is never scheduled as a flush.
    pub(crate) flush_task: RegionFlushTask,
    pub(crate) state: CancellableTaskState,
}

impl<S: LogStore> TailReplayTask<S> {
    pub(crate) async fn run(self) {
        let region_id = self.region.region_id;
        let _ended = EndNotifier {
            sender: self.flush_task.request_sender.clone(),
            region_id,
        };
        let mut backoff = MIN_BACKOFF;
        while !self.state.is_cancelled() && should_replay_tails(&self.region) {
            match self.replay_round().await {
                Ok(()) => return,
                Err(_) if self.state.is_cancelled() => return,
                Err(e) => {
                    error!(e; "Failed to replay the pending WAL tails of region {region_id}");
                    self.flush_task
                        .listener
                        .on_tail_replay_failed(region_id, &e);
                }
            }
            if !self.sleep(backoff).await {
                return;
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// Replays the current tails and publishes the result. An error asks for another round.
    async fn replay_round(&self) -> Result<()> {
        // Keeps no version past this point: it would hold the region's memtables, and their
        // write buffer memory, after a flush released them.
        let (tails, flushed_entry_id) = {
            let version = self.region.version();
            (version.pending_tails.clone(), version.flushed_entry_id)
        };
        let uncommitted = UncommittedSsts::new(
            self.region.region_id,
            self.region.access_layer.clone(),
            Some(self.flush_task.cache_manager.clone()),
        );
        let (files, last_entry_ids) = match self.replay_tails(&tails, &uncommitted).await {
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
            let mut last_entry_id = None;
            while let Some((entry_id, entry)) = self.unless_cancelled(entries.try_next()).await? {
                if entry_id > tail.upto {
                    break;
                }
                replay_entry(provider, region_id, &chunk, entry, entry_id).await?;
                last_entry_id = Some(entry_id);
                if chunk.current().version.memtables.mutable_usage() >= CHUNK_MEMTABLE_USAGE {
                    files.extend(self.write_chunk_ssts(&chunk, uncommitted).await?);
                    chunk = self.new_chunk_version();
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
            if self.region.manifest_ctx.leader_lease_received() {
                return true;
            }
            if !self.sleep(LEASE_CHECK_INTERVAL).await {
                return false;
            }
        }
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
        self.flush_task
            .write_ssts(&version, &self.state, uncommitted)
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
            self.flush_task
                .listener
                .on_tail_commit_begin(region_id)
                .await;
            if self.state.is_cancelled() {
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
                    let notify = BackgroundNotify::TailReplayCommitted { region, edit };
                    notify_worker(&self.flush_task.request_sender, region_id, notify).await;
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
struct EndNotifier {
    sender: Sender<WorkerRequestWithTime>,
    region_id: RegionId,
}

impl Drop for EndNotifier {
    fn drop(&mut self) {
        let sender = self.sender.clone();
        let region_id = self.region_id;
        common_runtime::spawn_global(async move {
            notify_worker(&sender, region_id, BackgroundNotify::TailReplayEnded).await;
        });
    }
}
