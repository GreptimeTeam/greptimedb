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

use common_telemetry::info;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;

use crate::access_layer::WriteType;
use crate::config::IndexBuildMode;
use crate::manifest::action::{PendingTail, RegionEdit};
use crate::region::{MitoRegion, MitoRegionRef};
use crate::request::{BackgroundNotify, BuildIndexRequest, OptionOutputTx, WorkerRequestWithTime};
use crate::schedule::CancellableTaskState;
use crate::sst::index::IndexBuildType;
use crate::tail_replay::{TailReplayTask, notify_worker, should_replay_tails};
use crate::worker::RegionWorkerLoop;

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
            sst_writer: self.new_sst_writer(
                &region,
                WriteType::TailReplay,
                None,
                self.config.clone(),
                self.tail_replay_limits.write_semaphore.clone(),
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

    /// Applies a published tail replay unless its instance was replaced, or its version no
    /// longer records `tails`, the tails it replayed, because a sync rebuilt it from a later
    /// manifest.
    pub(crate) async fn handle_tail_replay_committed(
        &mut self,
        region_id: RegionId,
        region: MitoRegionRef,
        edit: RegionEdit,
        tails: Vec<PendingTail>,
    ) {
        let registered = self.regions.get_region(region_id);
        if !registered.is_some_and(|registered| Arc::ptr_eq(&registered, &region)) {
            info!("Region {region_id} changed after its tails were published");
            return;
        }
        if region.version().pending_tails != tails {
            info!("Region {region_id} no longer records the replayed tails, skipping their edit");
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
        self.schedule_compaction(&region).await;
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
pub(crate) fn notify_writable(sender: &Sender<WorkerRequestWithTime>, region: &MitoRegion) {
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
pub(crate) fn notify_unwritable(sender: &Sender<WorkerRequestWithTime>, region: &MitoRegion) {
    if !region.version().has_pending_tails() {
        return;
    }
    let sender = sender.clone();
    let region_id = region.region_id;
    common_runtime::spawn_global(async move {
        notify_worker(&sender, region_id, BackgroundNotify::RegionBecameUnwritable).await;
    });
}
