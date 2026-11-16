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

//! Starting, applying and ending background replays of pending WAL tails.

use std::sync::Arc;

use common_telemetry::{error, info, warn};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;

use crate::config::IndexBuildMode;
use crate::error::{RegionBusySnafu, RegionStateSnafu};
use crate::manifest::action::{
    PendingTail, RegionEdit, RegionMetaAction, RegionMetaActionList, WalRecoveryEdit,
};
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState, TailPublication};
use crate::request::{BackgroundNotify, BuildIndexRequest, OptionOutputTx, WorkerRequestWithTime};
use crate::schedule::CancellableTaskState;
use crate::sst::index::IndexBuildType;
use crate::tail_replay::{
    TailReplayTask, notify_worker, release_wal_after_tails_edit, should_replay_tails,
};
use crate::worker::RegionWorkerLoop;
use crate::worker::handle_reload::notify_if_fenced;

impl<S: LogStore> RegionWorkerLoop<S> {
    /// Starts replaying the pending tails of the region, unless a replay of it still runs.
    pub(crate) fn maybe_replay_tails(&mut self, region_id: RegionId) {
        let Some(region) = self.regions.get_region(region_id) else {
            return;
        };
        if self.tail_replays.contains_key(&region_id) || !should_replay_tails(&region) {
            return;
        }
        info!("Replaying the pending WAL tails of region {region_id}");
        let state = CancellableTaskState::new();
        let task = TailReplayTask {
            sst_writer: self.tail_replay_limits.new_sst_writer(
                &region,
                self.config.clone(),
                self.cache_manager.clone(),
            ),
            region,
            node_id: self.file_ref_manager.node_id(),
            wal: self.wal.clone(),
            listener: self.listener.clone(),
            request_sender: self.sender.clone(),
            limits: self.tail_replay_limits.clone(),
            state: state.clone(),
        };
        self.tail_replays.insert(region_id, state);
        common_runtime::spawn_global(task.run());
    }

    /// Cancels the tail replay of the region. The replay still reports its end.
    pub(crate) fn cancel_tail_replay(&self, region_id: RegionId) {
        if let Some(state) = self.tail_replays.get(&region_id) {
            let _ = state.request_cancel();
        }
    }

    /// Discards the pending tails of the region that node `node_id` wrote, without replaying
    /// them, and sends the number of discarded tails to `sender`. A region without such tails
    /// sends 0 and changes nothing.
    ///
    /// The edit removes the tails, keeps the others in order and raises the flushed entry id
    /// to the largest `upto` of the removed tails, for a listed tail too. Entries of this node
    /// at or below it are then never replayed, which relies on the obligation of whoever
    /// records a tail (see
    /// [RegionManifest::wal_replay_start](crate::manifest::action::RegionManifest::wal_replay_start)).
    ///
    /// The edit is committed outside the worker against the tails the version records now, so
    /// a replay or a sync that changed them first makes it fail as busy, and a running replay
    /// of the region is left to find the tails changed. It also fails as busy while the
    /// region's last manifest update has an unknown result, which a replay of the tails could
    /// have saved (see
    /// [RegionManifestManager::has_unconfirmed_update](crate::manifest::manager::RegionManifestManager::has_unconfirmed_update));
    /// the next manifest update that succeeds, such as a flush, clears it.
    pub(crate) fn handle_discard_pending_tails(
        &mut self,
        region_id: RegionId,
        node_id: u64,
        sender: OptionOutputTx,
    ) {
        let region = match self.regions.writable_non_staging_region(region_id) {
            Ok(region) => region,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };
        let (tails, flushed_entry_id) = {
            let version = region.version();
            (version.pending_tails.clone(), version.flushed_entry_id)
        };
        let (discarded, remaining): (Vec<_>, Vec<_>) = tails
            .iter()
            .cloned()
            .partition(|tail| tail.node_id == node_id);
        let Some(upto) = discarded.iter().map(|tail| tail.upto).max() else {
            info!("Region {region_id} has no pending tails from node {node_id}");
            sender.send(Ok(0));
            return;
        };
        warn!(
            "Discarding the pending WAL tails of region {region_id} from node {node_id}: {discarded:?}, flushed entry id: {flushed_entry_id}"
        );
        let edit = RegionEdit {
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            flushed_entry_id: (upto > flushed_entry_id).then_some(upto),
            wal_recovery: WalRecoveryEdit {
                pending_tails: Some(remaining),
                handoff_floor: None,
            },
            ..Default::default()
        };
        let wal = self.wal.clone();
        let local_node_id = self.file_ref_manager.node_id();
        let request_sender = self.sender.clone();
        common_runtime::spawn_global(async move {
            let action_list =
                RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
            match region
                .manifest_ctx
                .update_manifest_for_tails(&tails, action_list, |manager| {
                    !manager.has_unconfirmed_update()
                })
                .await
            {
                Ok(TailPublication::Committed(manifest_version)) => {
                    info!(
                        "Discarded the pending WAL tails of region {region_id} from node {node_id} in manifest version {manifest_version}"
                    );
                    // The worker applies the edit before a replay that found the tails changed
                    // can restart from the old ones.
                    let notify = BackgroundNotify::PendingTailsDiscarded {
                        region: region.clone(),
                        edit,
                        tails,
                        discarded: discarded.len(),
                        sender,
                    };
                    notify_worker(&request_sender, region_id, notify).await;
                    release_wal_after_tails_edit(&region, &wal, local_node_id).await;
                }
                Ok(TailPublication::Unconfirmed) => {
                    info!(
                        "An earlier manifest update of region {region_id} is unconfirmed, not discarding the tails from node {node_id}"
                    );
                    sender.send(RegionBusySnafu { region_id }.fail());
                }
                Ok(TailPublication::Busy) => {
                    info!(
                        "Region {region_id} is altering or editing, not discarding the tails from node {node_id}"
                    );
                    sender.send(RegionBusySnafu { region_id }.fail());
                }
                Ok(TailPublication::Stale) => {
                    let state = region.state();
                    if state == RegionRoleState::Leader(RegionLeaderState::Writable) {
                        info!(
                            "Region {region_id} changed its pending tails, not discarding the tails from node {node_id}"
                        );
                        sender.send(RegionBusySnafu { region_id }.fail());
                    } else {
                        sender.send(
                            RegionStateSnafu {
                                region_id,
                                state,
                                expect: RegionRoleState::Leader(RegionLeaderState::Writable),
                            }
                            .fail(),
                        );
                    }
                }
                Err(e) => {
                    error!(e; "Failed to discard the pending WAL tails of region {region_id} from node {node_id}");
                    notify_if_fenced(&request_sender, &region).await;
                    sender.send(Err(e));
                }
            }
        });
    }

    /// Applies a published edit that discards pending tails, see [Self::apply_tails_edit], and
    /// sends the number of `discarded` tails, even if the region changed since.
    pub(crate) async fn handle_pending_tails_discarded(
        &mut self,
        region_id: RegionId,
        region: MitoRegionRef,
        edit: RegionEdit,
        tails: Vec<PendingTail>,
        discarded: usize,
        sender: OptionOutputTx,
    ) {
        self.apply_tails_edit(region_id, &region, edit, &tails)
            .await;
        sender.send(Ok(discarded));
        self.maybe_replay_tails(region_id);
    }

    /// Applies `edit`, which a tail replay or a discard published for the pending tails
    /// `tails`, unless the instance `region` was replaced, or its version no longer records
    /// `tails` because a sync rebuilt it from a later manifest.
    pub(crate) async fn apply_tails_edit(
        &mut self,
        region_id: RegionId,
        region: &MitoRegionRef,
        edit: RegionEdit,
        tails: &[PendingTail],
    ) {
        let registered = self.regions.get_region(region_id);
        if !registered.is_some_and(|registered| Arc::ptr_eq(&registered, region)) {
            info!("Region {region_id} changed after its tails were published");
            return;
        }
        if region.version().pending_tails != tails {
            info!("Region {region_id} no longer records the published tails, skipping their edit");
            return;
        }
        // A truncate uses the last entry id, which must not be below the flushed entry id.
        if let Some(flushed_entry_id) = edit.flushed_entry_id
            && flushed_entry_id > region.version_control.current().last_entry_id
        {
            region.version_control.set_entry_id(flushed_entry_id);
        }
        let file_metas = edit.files_to_add.clone();
        region
            .version_control
            .apply_edit(Some(edit), &[], region.file_purger.clone());
        if self.config.index.build_mode == IndexBuildMode::Async && !file_metas.is_empty() {
            self.handle_rebuild_index(
                BuildIndexRequest {
                    region_id,
                    build_type: IndexBuildType::Flush,
                    file_metas,
                    result_senders: Vec::new(),
                },
                OptionOutputTx::new(None),
            )
            .await;
        }
        self.schedule_compaction(region).await;
    }

    /// Cancels the tail replay of the region unless `region`, the instance a role change made
    /// unable to replay, was replaced since.
    pub(crate) fn handle_region_cannot_replay(&self, region_id: RegionId, region: MitoRegionRef) {
        let registered = self.regions.get_region(region_id);
        if registered.is_some_and(|registered| Arc::ptr_eq(&registered, &region)) {
            self.cancel_tail_replay(region_id);
        }
    }

    /// Forgets the ended tail replay of the region and starts another if it still has tails.
    pub(crate) fn handle_tail_replay_ended(&mut self, region_id: RegionId) {
        self.tail_replays.remove(&region_id);
        self.listener.on_tail_replay_ended(region_id);
        self.maybe_replay_tails(region_id);
    }
}

/// Asks the worker to replay the pending tails of a region that became writable, if it has
/// any, without waiting.
pub(crate) fn notify_writable(sender: &Sender<WorkerRequestWithTime>, region: &MitoRegionRef) {
    if !region.version().has_pending_tails() {
        return;
    }
    let sender = sender.clone();
    let region_id = region.region_id;
    common_runtime::spawn_global(async move {
        notify_worker(&sender, region_id, BackgroundNotify::RegionBecameWritable).await;
    });
}

/// Asks the worker to cancel the tail replay of a region that a role change outside the worker
/// made unable to replay, if it has pending tails, without waiting. A replay the cancel reaches
/// after the region became writable again ends and the worker starts another.
pub(crate) fn notify_cannot_replay(sender: &Sender<WorkerRequestWithTime>, region: &MitoRegionRef) {
    if !region.version().has_pending_tails() {
        return;
    }
    let sender = sender.clone();
    let region_id = region.region_id;
    let notify = BackgroundNotify::RegionCannotReplay {
        region: region.clone(),
    };
    common_runtime::spawn_global(async move {
        notify_worker(&sender, region_id, notify).await;
    });
}
