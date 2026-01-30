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

//! Handling catchup request.

use std::sync::Arc;

use common_telemetry::tracing::warn;
use common_telemetry::{debug, error, info};
use store_api::logstore::LogStore;
use store_api::region_engine::{RegionRole, SettableRegionRoleState};
use store_api::region_request::RegionCatchupRequest;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::region::MitoRegion;
use crate::region::catchup::RegionCatchupTask;
use crate::region::opener::RegionOpener;
use crate::request::OptionOutputTx;
use crate::wal::entry_distributor::WalEntryReceiver;
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_catchup_request(
        &mut self,
        region_id: RegionId,
        request: RegionCatchupRequest,
        entry_receiver: Option<WalEntryReceiver>,
        sender: OptionOutputTx,
    ) {
        let Some(region) = self.regions.get_region(region_id) else {
            sender.send(Err(error::RegionNotFoundSnafu { region_id }.build()));
            return;
        };

        if region.is_writable() {
            debug!("Region {region_id} is writable, skip catchup");
            sender.send(Ok(0));
            return;
        }

        if self.catchup_regions.is_region_exists(region_id) {
            warn!("Region {region_id} under catching up");
            sender.send(Err(error::RegionBusySnafu { region_id }.build()));
            return;
        }

        // If the memtable is not empty or the manifest has been updated, we need to reopen the region.
        let region = match self.reopen_region_if_needed(region).await {
            Ok(region) => region,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };

        self.catchup_regions.insert_region(region_id);
        let catchup_regions = self.catchup_regions.clone();
        let wal = self.wal.clone();
        let allow_stale_entries = self.config.allow_stale_entries;
        common_runtime::spawn_global(async move {
            let mut task = RegionCatchupTask::new(region.clone(), wal, allow_stale_entries)
                .with_entry_receiver(entry_receiver)
                .with_expected_last_entry_id(request.entry_id)
                .with_location_id(request.location_id)
                .with_replay_checkpoint_entry_id(request.checkpoint.map(|c| c.entry_id));

            match task.run().await {
                Ok(_) => {
                    if request.set_writable {
                        region.set_role(RegionRole::Leader);
                        // Finalize leadership: persist backfilled metadata.
                        if let Err(err) = region
                            .set_role_state_gracefully(SettableRegionRoleState::Leader)
                            .await
                        {
                            error!(err; "Failed to set region {region_id} to leader");
                        }
                    }
                    catchup_regions.remove_region(region_id);
                    sender.send(Ok(0));
                }
                Err(err) => {
                    error!(err; "Failed to catchup region {region_id}");
                    catchup_regions.remove_region(region_id);
                    sender.send(Err(err));
                }
            }
        });
    }

    /// Reopens a region.
    pub(crate) async fn reopen_region(
        &mut self,
        region: &Arc<MitoRegion>,
    ) -> Result<Arc<MitoRegion>> {
        let region_id = region.region_id;
        let manifest_version = region.manifest_ctx.manifest_version().await;
        let flushed_entry_id = region.version_control.current().last_entry_id;
        info!(
            "Reopening the region: {region_id}, manifest version: {manifest_version}, flushed entry id: {flushed_entry_id}"
        );
        let reopened_region = RegionOpener::new(
            region_id,
            region.table_dir(),
            region.access_layer.path_type(),
            self.memtable_builder_provider.clone(),
            self.object_store_manager.clone(),
            self.purge_scheduler.clone(),
            self.puffin_manager_factory.clone(),
            self.intermediate_manager.clone(),
            self.time_provider.clone(),
            self.file_ref_manager.clone(),
            self.partition_expr_fetcher.clone(),
        )
        .cache(Some(self.cache_manager.clone()))
        .options(region.version().options.clone())?
        .skip_wal_replay(true)
        .open(&self.config, &self.wal)
        .await?;
        debug_assert!(!reopened_region.is_writable());
        self.regions.insert_region(reopened_region.clone());

        Ok(reopened_region)
    }

    async fn reopen_region_if_needed(
        &mut self,
        region: Arc<MitoRegion>,
    ) -> Result<Arc<MitoRegion>> {
        // Note: Currently, We protect the split brain by ensuring the memtable table is empty.
        // It's expensive to execute catch-up requests without `set_writable=true` multiple times.
        let version = region.version();
        let is_empty_memtable = version.memtables.is_empty();

        // Utilizes the short circuit evaluation.
        let region = if !is_empty_memtable || region.manifest_ctx.has_update().await? {
            if !is_empty_memtable {
                warn!(
                    "Region {} memtables is not empty, which should not happen, manifest version: {}, last entry id: {}",
                    region.region_id,
                    region.manifest_ctx.manifest_version().await,
                    region.version_control.current().last_entry_id
                );
            }
            self.reopen_region(&region).await?
        } else {
            region
        };

        Ok(region)
    }
}
