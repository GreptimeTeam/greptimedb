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

use common_telemetry::info;
use snafu::ensure;
use store_api::logstore::LogStore;
use store_api::region_request::{AffectedRows, RegionCatchupRequest};
use store_api::storage::RegionId;
use tokio::time::Instant;

use crate::error::{self, Result};
use crate::region::opener::{replay_memtable, RegionOpener};
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
            return Ok(0);
        }
        // Note: Currently, We protect the split brain by ensuring the mutable table is empty.
        // It's expensive to execute catch-up requests without `set_writable=true` multiple times.
        let is_mutable_empty = region.version().memtables.mutable.is_empty();

        // Utilizes the short circuit evaluation.
        let region = if !is_mutable_empty || region.manifest_manager.has_update().await? {
            info!("Reopening the region: {region_id}, empty mutable: {is_mutable_empty}");
            let reopened_region = Arc::new(
                RegionOpener::new(
                    region_id,
                    region.region_dir(),
                    self.memtable_builder.clone(),
                    self.object_store_manager.clone(),
                    self.scheduler.clone(),
                )
                .cache(Some(self.cache_manager.clone()))
                .options(region.version().options.clone())
                .skip_wal_replay(true)
                .open(&self.config, &self.wal)
                .await?,
            );
            debug_assert!(!reopened_region.is_writable());
            self.regions.insert_region(reopened_region.clone());

            reopened_region
        } else {
            region
        };

        let flushed_entry_id = region.version_control.current().last_entry_id;
        info!("Trying to replay memtable for region: {region_id}, flushed entry id: {flushed_entry_id}");
        let timer = Instant::now();
        let last_entry_id = replay_memtable(
            &self.wal,
            &region.wal_options,
            region_id,
            flushed_entry_id,
            &region.version_control,
        )
        .await?;
        info!(
            "Elapsed: {:?}, region: {region_id} catchup finished. last entry id: {last_entry_id}, expected: {:?}.",
            timer.elapsed(),
            request.entry_id
        );
        if let Some(expected_last_entry_id) = request.entry_id {
            ensure!(
                expected_last_entry_id == last_entry_id,
                error::UnexpectedReplaySnafu {
                    region_id,
                    expected_last_entry_id,
                    replayed_last_entry_id: last_entry_id,
                }
            )
        }

        if request.set_writable {
            region.set_writable(true);
        }

        Ok(0)
    }
}
