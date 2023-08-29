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

use store_api::region_request::RegionFlushRequest;
use store_api::storage::RegionId;

use crate::flush::{FlushReason, RegionFlushTask};
use crate::region::MitoRegionRef;
use crate::request::{FlushFailed, FlushFinished};
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    /// Handles manual flush request.
    pub(crate) async fn handle_flush(
        &mut self,
        _region_id: RegionId,
        _request: RegionFlushRequest,
    ) {
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
    pub(crate) fn maybe_flush_worker(&self) {
        if !self.write_buffer_manager.should_flush_engine() {
            // No need to flush worker.
            return;
        }
        // If the engine needs flush, each worker will find some regions to flush. We might
        // flush more memory than expect but it should be acceptable.
        self.find_regions_to_flush();
    }

    /// Find some regions to flush to reduce write buffer usage.
    pub(crate) fn find_regions_to_flush(&self) {
        unimplemented!()
    }

    /// Flush a region if it meets flush requirements.
    pub(crate) fn flush_region_if_full(&mut self, region: &MitoRegionRef) {
        let version_data = region.version_control.current();
        if self
            .write_buffer_manager
            .should_flush_region(version_data.version.mutable_stats())
        {
            // We need to flush this region.
            let task = RegionFlushTask {
                region_id: region.region_id,
                reason: FlushReason::MemtableFull,
                sender: None,
            };
            self.flush_scheduler.schedule_flush(region, task);
        }
    }
}
