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

//! Reloading a region that a manifest version conflict fenced.

use std::sync::Arc;

use common_telemetry::{error, info, warn};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;

use crate::error::Result;
use crate::region::{MitoRegion, MitoRegionRef};
use crate::request::{BackgroundNotify, WorkerRequest, WorkerRequestWithTime};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    /// Starts reloading the region from its manifest and this node's WAL if a manifest version
    /// conflict fenced it and no catchup or reload of it runs.
    pub(crate) fn maybe_reload_region(&mut self, region_id: RegionId) {
        let Some(region) = self.regions.get_region(region_id) else {
            return;
        };
        // A flush of the old instance would report to the replacement, so the reload waits until
        // the flush reports here, which checks again.
        if !region.manifest_ctx.is_reload_pending()
            || self.catchup_regions.is_region_exists(region_id)
            || self.flush_scheduler.is_flush_requested(region_id)
        {
            return;
        }
        // The worker writes a region inline and a Follower takes no writes, so every
        // acknowledged write has an entry id at or below the one read below.
        if !region.is_follower() {
            warn!(
                "Region {region_id} waits for a reload but is {:?}, not reloading it",
                region.state()
            );
            return;
        }
        let opener = match self.region_reopener(&region) {
            Ok(opener) => opener.skip_wal_replay(false),
            Err(e) => {
                error!(e; "Failed to build the reload of region {region_id}");
                return;
            }
        };
        let last_entry_id = region.version_control.current().last_entry_id;
        info!("Reloading region {region_id}, last entry id: {last_entry_id}");
        self.catchup_regions.insert_region(region_id);
        let durability_barrier = self.wal.durability_barrier(region_id, &region.provider);
        let wal = self.wal.clone();
        let config = self.config.clone();
        let sender = self.sender.clone();
        common_runtime::spawn_global(async move {
            let result = async {
                // The replay reads only what the log store has made durable.
                durability_barrier.wait(last_entry_id).await?;
                opener.open(&config, &wal).await
            }
            .await;
            let notify = BackgroundNotify::RegionReloaded {
                old: region,
                result,
            };
            let _ = sender
                .send(WorkerRequestWithTime::new(WorkerRequest::Background {
                    region_id,
                    notify,
                }))
                .await;
        });
    }

    /// Replaces the fenced instance with the reloaded one, unless the region was closed or
    /// reopened during the reload. A failed reload leaves the fenced instance in place.
    pub(crate) async fn handle_region_reloaded(
        &mut self,
        region_id: RegionId,
        old: MitoRegionRef,
        result: Result<MitoRegionRef>,
    ) {
        self.listener.on_region_reload_swap(region_id).await;
        self.catchup_regions.remove_region(region_id);
        let region = match result {
            Ok(region) => region,
            Err(e) => {
                error!(e; "Failed to reload region {region_id}");
                return;
            }
        };
        let registered = self.regions.get_region(region_id);
        if !registered.is_some_and(|registered| Arc::ptr_eq(&registered, &old)) {
            info!("Region {region_id} changed during its reload, dropping the reloaded instance");
            return;
        }
        // Preserve allocated sequences without publishing the old memtables.
        region
            .version_control
            .update_last_sequence(old.version_control.current().last_sequence);
        // Leases apply through the shared region map, so only those from heartbeats sent after
        // the replacement may promote the new instance.
        region.manifest_ctx.ignore_earlier_promotions();
        // Requests waiting for the old instance are rejected as a close rejects them.
        self.fail_region_stalled_requests_as_not_found(&region_id);
        self.reject_region_edit_queue_as_not_found(region_id);
        self.register_region(region);
        if let Some(state) = &self.series_index_task_state {
            state.wake();
        }
        info!("Region {region_id} is reloaded");
    }
}

/// Asks the worker to reload the region if a commit outside the worker fenced it. The caller
/// must not hold the region's manifest lock, which the worker may wait for.
pub(crate) async fn notify_if_fenced(sender: &Sender<WorkerRequestWithTime>, region: &MitoRegion) {
    if region.manifest_ctx.is_reload_pending() {
        let _ = sender
            .send(WorkerRequestWithTime::new(WorkerRequest::Background {
                region_id: region.region_id,
                notify: BackgroundNotify::RegionFenced,
            }))
            .await;
    }
}
