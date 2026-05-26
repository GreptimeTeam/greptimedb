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
use common_wal::options::WalOptions;
use object_store::util::join_path;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::LogStore;
use store_api::region_request::{OpenRegionCapability, RegionOpenRequest};
use store_api::storage::RegionId;
use table::requests::STORAGE_KEY;

use crate::error::{
    InvalidRequestSnafu, ObjectStoreNotFoundSnafu, OpenDalSnafu, OpenRegionSnafu,
    RegionNotFoundSnafu, Result,
};
use crate::region::opener::{RegionOpener, sanitize_open_request_options};
use crate::region::options::parse_wal_options;
use crate::request::OptionOutputTx;
use crate::sst::location::region_dir_from_table_dir;
use crate::wal::entry_distributor::WalEntryReceiver;
use crate::worker::handle_drop::remove_region_dir_once;
use crate::worker::{DROPPING_MARKER_FILE, RegionWorkerLoop};

fn has_object_storage_capability(object_store: &object_store::ObjectStore) -> bool {
    object_store.info().scheme() != object_store::services::FS_SCHEME
}

fn has_remote_wal_capability(request: &RegionOpenRequest) -> Result<bool> {
    Ok(matches!(
        parse_wal_options(&request.options).context(crate::error::SerdeJsonSnafu)?,
        WalOptions::Kafka(_)
    ))
}

impl<S: LogStore> RegionWorkerLoop<S> {
    fn object_store_from_request(
        &self,
        request: &RegionOpenRequest,
    ) -> Result<&object_store::ObjectStore> {
        if let Some(storage_name) = request.options.get(STORAGE_KEY) {
            self.object_store_manager
                .find(storage_name)
                .with_context(|| ObjectStoreNotFoundSnafu {
                    object_store: storage_name.clone(),
                })
        } else {
            Ok(self.object_store_manager.default_object_store())
        }
    }

    fn validate_open_region_capabilities(
        &self,
        region_id: RegionId,
        request: &RegionOpenRequest,
        object_store: &object_store::ObjectStore,
    ) -> Result<()> {
        let required_capabilities = request.required_capabilities;
        if required_capabilities.contains(OpenRegionCapability::OBJECT_STORAGE)
            && !has_object_storage_capability(object_store)
        {
            return InvalidRequestSnafu {
                region_id,
                reason: format!(
                    "opening region requires ObjectStorage capability, but storage scheme is {}",
                    object_store.info().scheme()
                ),
            }
            .fail();
        }

        if required_capabilities.contains(OpenRegionCapability::REMOTE_WAL)
            && !has_remote_wal_capability(request)?
        {
            return InvalidRequestSnafu {
                region_id,
                reason: "opening region requires RemoteWal capability, but wal options are not remote wal"
                    .to_string(),
            }
            .fail();
        }

        Ok(())
    }

    async fn check_and_cleanup_region(
        &self,
        region_id: RegionId,
        request: &RegionOpenRequest,
    ) -> Result<()> {
        let object_store = self.object_store_from_request(request)?;
        self.validate_open_region_capabilities(region_id, request, object_store)?;
        // Check if this region is pending drop. And clean the entire dir if so.
        let region_dir =
            region_dir_from_table_dir(&request.table_dir, region_id, request.path_type);
        if !self.dropping_regions.is_region_exists(region_id)
            && object_store
                .exists(&join_path(&region_dir, DROPPING_MARKER_FILE))
                .await
                .context(OpenDalSnafu)?
        {
            let result = remove_region_dir_once(&region_dir, object_store, true).await;
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
        mut request: RegionOpenRequest,
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
        sanitize_open_request_options(&mut request.options);

        // Open region from specific region dir.
        let opener = match RegionOpener::new(
            region_id,
            &request.table_dir,
            request.path_type,
            self.memtable_builder_provider.clone(),
            self.object_store_manager.clone(),
            self.purge_scheduler.clone(),
            self.puffin_manager_factory.clone(),
            self.intermediate_manager.clone(),
            self.time_provider.clone(),
            self.file_ref_manager.clone(),
            self.partition_expr_fetcher.clone(),
        )
        .skip_wal_replay(request.skip_wal_replay)
        .cache(Some(self.cache_manager.clone()))
        .wal_entry_reader(wal_entry_receiver.map(|receiver| Box::new(receiver) as _))
        .replay_checkpoint(request.checkpoint.map(|checkpoint| checkpoint.entry_id))
        .parse_options(request.options)
        {
            Ok(opener) => opener,
            Err(err) => {
                sender.send(Err(err));
                return;
            }
        };

        let now = Instant::now();
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
                    info!(
                        "Region {} is opened, worker: {}, elapsed: {:?}",
                        region_id,
                        worker_id,
                        now.elapsed()
                    );
                    region_count.inc();

                    // Insert the Region into the RegionMap.
                    regions.insert_region(region);

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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_wal::options::{KafkaWalOptions, WAL_OPTIONS_KEY, WalOptions};
    use object_store::ObjectStore;
    use object_store::services::{Fs, Memory};
    use store_api::region_request::{OpenRegionCapability, PathType, RegionOpenRequest};

    use super::{has_object_storage_capability, has_remote_wal_capability};

    fn new_open_request(options: HashMap<String, String>) -> RegionOpenRequest {
        RegionOpenRequest {
            engine: "mito2".to_string(),
            table_dir: "test".to_string(),
            path_type: PathType::Bare,
            options,
            skip_wal_replay: false,
            checkpoint: None,
            required_capabilities: OpenRegionCapability::empty(),
        }
    }

    #[test]
    fn test_has_object_storage_capability() {
        let dir = common_test_util::temp_dir::create_temp_dir("handle-open");
        let fs = ObjectStore::new(Fs::default().root(dir.path().to_str().unwrap()))
            .unwrap()
            .finish();
        assert!(!has_object_storage_capability(&fs));

        let memory = ObjectStore::new(Memory::default()).unwrap().finish();
        assert!(has_object_storage_capability(&memory));
    }

    #[test]
    fn test_has_remote_wal_capability() {
        assert!(!has_remote_wal_capability(&new_open_request(HashMap::new())).unwrap());

        let wal_options = serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
            topic: "test-topic".to_string(),
        }))
        .unwrap();
        let request = new_open_request(HashMap::from([(WAL_OPTIONS_KEY.to_string(), wal_options)]));
        assert!(has_remote_wal_capability(&request).unwrap());
    }
}
