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

//! Handling close request.

use common_telemetry::info;
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::region_request::RegionFlushRequest;
use store_api::storage::RegionId;

use crate::flush::FlushReason;
use crate::request::OptionOutputTx;
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_close_request(
        &mut self,
        region_id: RegionId,
        sender: OptionOutputTx,
    ) {
        let Some(region) = self.regions.get_region(region_id) else {
            sender.send(Ok(0));
            return;
        };

        info!("Try to close region {}, worker: {}", region_id, self.id);

        // If the region is using Noop WAL and has data in memtable,
        // we should flush it before closing to ensure durability.
        if region.provider == Provider::Noop
            && !region
                .version_control
                .current()
                .version
                .memtables
                .is_empty()
        {
            info!("Region {} has pending data, waiting for flush", region_id);
            self.handle_flush_request(
                region_id,
                RegionFlushRequest {
                    row_group_size: None,
                },
                Some(FlushReason::Closing),
                sender,
            );
            return;
        }

        // WAL configured or memtable is empty, flush is not necessary.
        self.remove_region(region_id).await;
        info!("Region {} closed, worker: {}", region_id, self.id);
        sender.send(Ok(0))
    }

    /// Remove a region and stop all related tasks.
    pub(crate) async fn remove_region(&mut self, region_id: RegionId) {
        let Some(region) = self.regions.remove_region(region_id) else {
            return;
        };
        region.stop().await;
        // Clean flush status.
        self.flush_scheduler.on_region_closed(region_id);
        // Clean compaction status.
        self.compaction_scheduler.on_region_closed(region_id);
        // clean index build status.
        self.index_build_scheduler.on_region_closed(region_id).await;
        self.region_count.dec();
    }
}
