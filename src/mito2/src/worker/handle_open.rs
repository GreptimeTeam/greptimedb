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
use std::time::Instant;

use common_telemetry::info;
use object_store::util::join_path;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::LogStore;
use store_api::region_request::RegionOpenRequest;
use store_api::storage::RegionId;
use table::requests::STORAGE_KEY;

use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::write_cache::WriteCacheRef;
use crate::error::{
    ObjectStoreNotFoundSnafu, OpenDalSnafu, OpenRegionSnafu, RegionNotFoundSnafu, Result,
};
use crate::metrics::WRITE_CACHE_INFLIGHT_DOWNLOAD;
use crate::region::opener::RegionOpener;
use crate::request::OptionOutputTx;
use crate::sst::location;
use crate::wal::entry_distributor::WalEntryReceiver;
use crate::worker::handle_drop::remove_region_dir_once;
use crate::worker::{MitoRegionRef, RegionWorkerLoop, DROPPING_MARKER_FILE};

impl<S: LogStore> RegionWorkerLoop<S> {
    async fn check_and_cleanup_region(
        &self,
        region_id: RegionId,
        request: &RegionOpenRequest,
    ) -> Result<()> {
        let object_store = if let Some(storage_name) = request.options.get(STORAGE_KEY) {
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
                .exists(&join_path(&request.region_dir, DROPPING_MARKER_FILE))
                .await
                .context(OpenDalSnafu)?
        {
            let result = remove_region_dir_once(&request.region_dir, object_store, true).await;
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
            self.time_provider.clone(),
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
        let write_cache = self.cache_manager.write_cache().cloned();
        opening_regions.insert_sender(region_id, sender);
        common_runtime::spawn_global(async move {
            match opener.open(&config, &wal).await {
                Ok(region) => {
                    info!("Region {} is opened, worker: {}", region_id, worker_id);
                    region_count.inc();

                    // Insert the Region into the RegionMap.
                    let region = Arc::new(region);
                    regions.insert_region(region.clone());

                    let senders = opening_regions.remove_sender(region_id);
                    for sender in senders {
                        sender.send(Ok(0));
                    }

                    if let Some(write_cache) = write_cache {
                        prefetch_latest_ssts(region, write_cache).await;
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

/// Download latest SSTs from the remote storage for the region.
async fn prefetch_latest_ssts(region: MitoRegionRef, write_cache: WriteCacheRef) {
    let version = region.version();
    // Sort ssts by time range in descending order.
    let mut ssts: Vec<_> = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .collect();
    ssts.sort_unstable_by(|left, right| right.time_range().1.cmp(&left.time_range().1));

    let layer = region.access_layer.clone();
    let region_id = region.region_id;
    // Prefetch the latest SSTs.
    let mut has_err = false;
    let mut fetched = 0;
    let mut downloaded_bytes = 0;
    let start = Instant::now();
    for sst in ssts {
        if has_err || write_cache.available_space() <= write_cache.capacity() / 2 {
            break;
        }

        WRITE_CACHE_INFLIGHT_DOWNLOAD.add(1);

        let file_meta = sst.meta_ref();
        let index_key = IndexKey::new(region_id, file_meta.file_id, FileType::Parquet);
        let remote_path = location::sst_file_path(layer.region_dir(), file_meta.file_id);
        let file_size = file_meta.file_size;
        if let Err(err) = write_cache
            .download(index_key, &remote_path, layer.object_store(), file_size)
            .await
        {
            common_telemetry::error!(
                err; "Failed to download parquet file, region_id: {}, index_key: {:?}, remote_path: {}", region_id, index_key, remote_path
            );
            has_err = true;
        } else {
            fetched += 1;
            downloaded_bytes += file_size;
        }

        let is_index_exist = file_meta.exists_index();
        if !has_err && is_index_exist {
            let index_file_size = file_meta.index_file_size();
            let index_file_index_key =
                IndexKey::new(region_id, file_meta.file_id, FileType::Puffin);
            let index_remote_path =
                location::index_file_path(layer.region_dir(), file_meta.file_id);
            // also download puffin file
            if let Err(err) = write_cache
                .download(
                    index_file_index_key,
                    &index_remote_path,
                    layer.object_store(),
                    index_file_size,
                )
                .await
            {
                common_telemetry::error!(
                    err; "Failed to download puffin file, region_id: {}, index_file_index_key: {:?}, index_remote_path: {}", region_id, index_file_index_key, index_remote_path
                );
                has_err = true;
            } else {
                fetched += 1;
                downloaded_bytes += index_file_size;
            }
        }

        WRITE_CACHE_INFLIGHT_DOWNLOAD.sub(1);
    }

    common_telemetry::info!(
        "region {} prefetched {} files with total {} bytes, elapsed: {:?}",
        region_id,
        fetched,
        downloaded_bytes,
        start.elapsed(),
    );
}
