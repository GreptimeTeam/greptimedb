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

//! Handling open request.

use std::sync::Arc;

use common_telemetry::info;
use object_store::util::join_path;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::LogStore;
use store_api::region_request::RegionOpenRequest;
use store_api::storage::RegionId;

use crate::error::{
    ObjectStoreNotFoundSnafu, OpenDalSnafu, OpenRegionSnafu, RegionNotFoundSnafu, Result,
};
use crate::region::opener::RegionOpener;
use crate::request::OptionOutputTx;
use crate::wal::entry_distributor::WalEntryReceiver;
use crate::worker::handle_drop::remove_region_dir_once;
use crate::worker::{RegionWorkerLoop, DROPPING_MARKER_FILE};

impl<S: LogStore> RegionWorkerLoop<S> {
    async fn check_and_cleanup_region(
        &self,
        region_id: RegionId,
        request: &RegionOpenRequest,
    ) -> Result<()> {
        let object_store = if let Some(storage_name) = request.options.get("storage") {
            self.object_store_manager
                .find(storage_name)
                .context(ObjectStoreNotFoundSnafu {
                    object_store: storage_name.to_string(),
                })?
        } else {
            self.object_store_manager.default_object_store()
        };
        // Check if this region is pending drop. And clean the entire dir if so.
        if !self.dropping_regions.is_region_exists(region_id)
            && object_store
                .is_exist(&join_path(&request.region_dir, DROPPING_MARKER_FILE))
                .await
                .context(OpenDalSnafu)?
        {
            let result = remove_region_dir_once(&request.region_dir, object_store).await;
            info!(
                "Region {} is dropped, worker: {}, result: {:?}",
                region_id, self.id, result
            );
            return RegionNotFoundSnafu { region_id }.fail();
        }

        Ok(())
    }

    pub(crate) async fn handle_open_request(
        &mut self,
        region_id: RegionId,
        request: RegionOpenRequest,
        wal_entry_receiver: Option<WalEntryReceiver>,
        sender: OptionOutputTx,
    ) {
        if self.regions.is_region_exists(region_id) {
            sender.send(Ok(0));
            return;
        }
        let Some(sender) = self
            .opening_regions
            .wait_for_opening_region(region_id, sender)
        else {
            return;
        };
        if let Err(err) = self.check_and_cleanup_region(region_id, &request).await {
            sender.send(Err(err));
            return;
        }
        info!("Try to open region {}, worker: {}", region_id, self.id);

        // Open region from specific region dir.
        let opener = match RegionOpener::new(
            region_id,
            &request.region_dir,
            self.memtable_builder_provider.clone(),
            self.object_store_manager.clone(),
            self.purge_scheduler.clone(),
            self.puffin_manager_factory.clone(),
            self.intermediate_manager.clone(),
        )
        .skip_wal_replay(request.skip_wal_replay)
        .cache(Some(self.cache_manager.clone()))
        .wal_entry_reader(wal_entry_receiver.map(|receiver| Box::new(receiver) as _))
        .parse_options(request.options)
        {
            Ok(opener) => opener,
            Err(err) => {
                sender.send(Err(err));
                return;
            }
        };

        let regions = self.regions.clone();
        let wal = self.wal.clone();
        let config = self.config.clone();
        let opening_regions = self.opening_regions.clone();
        let region_count = self.region_count.clone();
        let worker_id = self.id;
        opening_regions.insert_sender(region_id, sender);
        common_runtime::spawn_global(async move {
            match opener.open(&config, &wal).await {
                Ok(region) => {
                    info!("Region {} is opened, worker: {}", region_id, worker_id);
                    region_count.inc();

                    // Insert the Region into the RegionMap.
                    regions.insert_region(Arc::new(region));

                    let senders = opening_regions.remove_sender(region_id);
                    for sender in senders {
                        sender.send(Ok(0));
                    }
                }
                Err(err) => {
                    let senders = opening_regions.remove_sender(region_id);
                    let err = Arc::new(err);
                    for sender in senders {
                        sender.send(Err(err.clone()).context(OpenRegionSnafu));
                    }
                }
            }
        });
    }
}
