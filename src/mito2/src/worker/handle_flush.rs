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

//! Handling flush related requests.

use common_query::Output;
use common_telemetry::error;
use common_time::util::current_time_millis;
use store_api::region_request::RegionFlushRequest;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::flush::{FlushReason, RegionFlushTask};
use crate::region::MitoRegionRef;
use crate::request::{FlushFailed, FlushFinished};
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    /// Handles manual flush request.
    pub(crate) async fn handle_flush_request(
        &mut self,
        _region_id: RegionId,
        _request: RegionFlushRequest,
    ) -> Result<Output> {
        // TODO(yingwen): schedule flush.
        unimplemented!()
    }

    /// On region flush job finished.
    pub(crate) async fn handle_flush_finished(
        &mut self,
        _region_id: RegionId,
        _request: FlushFinished,
    ) {
        // TODO(yingwen):
        // 1. check region existence
        // 2. write manifest
        // 3. update region metadata.
        // 4. handle all pending requests.
        // 5. schedule next flush.
        unimplemented!()
    }

    /// On region flush job failed.
    pub(crate) async fn handle_flush_failed(
        &mut self,
        _region_id: RegionId,
        _request: FlushFailed,
    ) {
        // TODO(yingwen): fail all pending requests.
        unimplemented!()
    }

    /// Checks whether the engine reaches flush threshold. If so, finds regions in this
    /// worker to flush.
    pub(crate) fn maybe_flush_worker(&mut self) {
        if !self.write_buffer_manager.should_flush_engine() {
            // No need to flush worker.
            return;
        }

        // If the engine needs flush, each worker will find some regions to flush. We might
        // flush more memory than expect but it should be acceptable.
        if let Err(e) = self.flush_regions_on_engine_full() {
            error!(e; "Failed to flush worker");
        }
    }

    /// Flush a region if it meets flush requirements.
    pub(crate) fn flush_region_if_full(&mut self, _region: &MitoRegionRef) {
        // Now we does nothing.
    }

    /// Find some regions to flush to reduce write buffer usage.
    fn flush_regions_on_engine_full(&mut self) -> Result<()> {
        let regions = self.regions.list_regions();
        let now = current_time_millis();
        let min_last_flush_time = now - self.config.auto_flush_interval.as_millis() as i64;
        let mut max_mutable_size = 0;
        // Region with max mutable memtable size.
        let mut max_mem_region = None;

        for region in &regions {
            if self.flush_scheduler.is_flush_requested(region.region_id) {
                // Already flushing.
                continue;
            }

            let version = region.version();
            let region_mutable_size = version.memtables.mutable_bytes_usage();
            // Tracks region with max mutable memtable size.
            if region_mutable_size > max_mutable_size {
                max_mem_region = Some(region);
                max_mutable_size = region_mutable_size;
            }

            if region.last_flush_millis() < min_last_flush_time {
                // If flush time of this region is earlier than `min_last_flush_time`, we can flush this region.
                let task = self.new_flush_task(region.region_id, FlushReason::EngineFull);
                self.flush_scheduler.schedule_flush(region, task)?;
            }
        }

        // Flush memtable with max mutable memtable.
        if let Some(region) = max_mem_region {
            if !self.flush_scheduler.is_flush_requested(region.region_id) {
                let task = self.new_flush_task(region.region_id, FlushReason::EngineFull);
                self.flush_scheduler.schedule_flush(region, task)?;
            }
        }

        Ok(())
    }

    fn new_flush_task(&self, region_id: RegionId, reason: FlushReason) -> RegionFlushTask {
        RegionFlushTask {
            region_id,
            reason,
            sender: None,
            request_sender: self.sender.clone(),
            object_store: self.object_store.clone(),
            memtable_builder: self.memtable_builder.clone(),
        }
    }
}
