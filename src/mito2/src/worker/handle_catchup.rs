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
use common_telemetry::{debug, info};
use snafu::ensure;
use store_api::logstore::LogStore;
use store_api::region_engine::{RegionRole, SettableRegionRoleState};
use store_api::region_request::{AffectedRows, RegionCatchupRequest};
use store_api::storage::RegionId;
use tokio::time::Instant;

use crate::error::{self, Result};
use crate::region::MitoRegion;
use crate::region::opener::{RegionOpener, replay_memtable};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_catchup_request(
        &mut self,
        region_id: RegionId,
        request: RegionCatchupRequest,
    ) -> Result<AffectedRows> {
        let Some(region) = self.regions.get_region(region_id) else {
            return error::RegionNotFoundSnafu { region_id }.fail();
        };

        if region.is_writable() {
            debug!("Region {region_id} is writable, skip catchup");
            return Ok(0);
        }
        // Note: Currently, We protect the split brain by ensuring the mutable table is empty.
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

        if region.provider.is_remote_wal() {
            let flushed_entry_id = region.version_control.current().last_entry_id;
            let replay_from_entry_id = request
                .checkpoint
                .map(|c| c.entry_id)
                .unwrap_or_default()
                .max(flushed_entry_id);
            info!(
                "Trying to replay memtable for region: {region_id}, provider: {:?}, replay from entry id: {replay_from_entry_id}, flushed entry id: {flushed_entry_id}",
                region.provider
            );
            let timer = Instant::now();
            let wal_entry_reader =
                self.wal
                    .wal_entry_reader(&region.provider, region_id, request.location_id);
            let on_region_opened = self.wal.on_region_opened();
            let last_entry_id = replay_memtable(
                &region.provider,
                wal_entry_reader,
                region_id,
                replay_from_entry_id,
                &region.version_control,
                self.config.allow_stale_entries,
                on_region_opened,
            )
            .await?;
            info!(
                "Elapsed: {:?}, region: {region_id}, provider: {:?} catchup finished. replay from entry id: {replay_from_entry_id}, flushed entry id: {flushed_entry_id}, last entry id: {last_entry_id}, expected: {:?}.",
                timer.elapsed(),
                region.provider,
                request.entry_id
            );
            if let Some(expected_last_entry_id) = request.entry_id {
                ensure!(
                    // The replayed last entry id may be greater than the `expected_last_entry_id`.
                    last_entry_id >= expected_last_entry_id,
                    error::UnexpectedSnafu {
                        reason: format!(
                            "failed to set region {} to writable, it was expected to replayed to {}, but actually replayed to {}",
                            region_id, expected_last_entry_id, last_entry_id,
                        ),
                    }
                )
            }
        } else {
            let version = region.version_control.current();
            let mut flushed_entry_id = version.last_entry_id;

            let latest_entry_id = self
                .wal
                .store()
                .latest_entry_id(&region.provider)
                .unwrap_or_default();
            warn!(
                "Skips to replay memtable for region: {}, flushed entry id: {}, latest entry id: {}",
                region.region_id, flushed_entry_id, latest_entry_id
            );

            if latest_entry_id > flushed_entry_id {
                warn!(
                    "Found latest entry id is greater than flushed entry id, using latest entry id as flushed entry id, region: {}, latest entry id: {}, flushed entry id: {}",
                    region_id, latest_entry_id, flushed_entry_id
                );
                flushed_entry_id = latest_entry_id;
                region.version_control.set_entry_id(flushed_entry_id);
            }
            let on_region_opened = self.wal.on_region_opened();
            on_region_opened(region_id, flushed_entry_id, &region.provider).await?;
        }

        if request.set_writable {
            region.set_role(RegionRole::Leader);
            // Finalize leadership: persist backfilled metadata.
            region
                .set_role_state_gracefully(SettableRegionRoleState::Leader)
                .await?;
        }

        Ok(0)
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
        let reopened_region = Arc::new(
            RegionOpener::new(
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
            .await?,
        );
        debug_assert!(!reopened_region.is_writable());
        self.regions.insert_region(reopened_region.clone());

        Ok(reopened_region)
    }
}
