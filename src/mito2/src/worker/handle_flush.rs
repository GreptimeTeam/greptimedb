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
use common_telemetry::{error, info};
use common_time::util::current_time_millis;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::oneshot;

use crate::error::{RegionNotFoundSnafu, Result};
use crate::flush::{FlushReason, RegionFlushTask};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::region::MitoRegionRef;
use crate::request::{FlushFailed, FlushFinished};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    /// On region flush job finished.
    pub(crate) async fn handle_flush_finished(
        &mut self,
        region_id: RegionId,
        mut request: FlushFinished,
    ) {
        let Some(region) = self.regions.get_region(region_id) else {
            // We may dropped or closed the region.
            request.on_failure(RegionNotFoundSnafu { region_id }.build());
            return;
        };

        // Write region edit to manifest.
        let edit = RegionEdit {
            files_to_add: std::mem::take(&mut request.file_metas),
            files_to_remove: Vec::new(),
            compaction_time_window: None,
            flushed_entry_id: Some(request.flushed_entry_id),
        };
        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        if let Err(e) = region.manifest_manager.update(action_list).await {
            error!(e; "Failed to write manifest, region: {}", region_id);
            request.on_failure(e);
            return;
        }

        // Apply edit to region's version.
        region
            .version_control
            .apply_edit(edit, region.file_purger.clone());
        region.update_flush_millis();

        // Delete wal.
        info!(
            "Region {} flush finished, tries to bump wal to {}",
            region_id, request.flushed_entry_id
        );
        if let Err(e) = self.wal.obsolete(region_id, request.flushed_entry_id).await {
            error!(e; "Failed to write wal, region: {}", region_id);
            request.on_failure(e);
            return;
        }

        // Notifies waiters.
        request.on_success();

        // Handle pending DDL requests for the region.
        if let Some(ddl_requests) = self.flush_scheduler.on_flush_success(region_id) {
            self.handle_ddl_requests(ddl_requests).await;
        }

        // Handle stalled requests.
        let stalled = std::mem::take(&mut self.stalled_requests);
        // We already stalled these requests, don't stall them again.
        self.handle_write_requests(stalled.requests, false).await;

        self.listener.on_flush_success(region_id);
    }
}

impl<S> RegionWorkerLoop<S> {
    /// Handles manual flush request.
    pub(crate) async fn handle_flush_request(
        &mut self,
        region_id: RegionId,
        sender: Option<oneshot::Sender<Result<Output>>>,
    ) {
        let Some(region) = self.regions.get_region(region_id) else {
            if let Some(sender) = sender {
                let _ = sender.send(RegionNotFoundSnafu { region_id }.fail());
            }
            return;
        };

        let mut task = self.new_flush_task(&region, FlushReason::Manual);
        if let Some(sender) = sender {
            task.senders.push(sender);
        }
        if let Err(e) = self.flush_scheduler.schedule_flush(&region, task) {
            error!(e; "Failed to schedule flush task for region {}", region.region_id);
        }
    }

    /// On region flush job failed.
    pub(crate) async fn handle_flush_failed(&mut self, region_id: RegionId, request: FlushFailed) {
        self.flush_scheduler.on_flush_failed(region_id, request.err);
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
            let region_mutable_size = version.memtables.mutable_usage();
            // Tracks region with max mutable memtable size.
            if region_mutable_size > max_mutable_size {
                max_mem_region = Some(region);
                max_mutable_size = region_mutable_size;
            }

            if region.last_flush_millis() < min_last_flush_time {
                // If flush time of this region is earlier than `min_last_flush_time`, we can flush this region.
                let task = self.new_flush_task(region, FlushReason::EngineFull);
                self.flush_scheduler.schedule_flush(region, task)?;
            }
        }

        // Flush memtable with max mutable memtable.
        // TODO(yingwen): Maybe flush more tables to reduce write buffer size.
        if let Some(region) = max_mem_region {
            if !self.flush_scheduler.is_flush_requested(region.region_id) {
                let task = self.new_flush_task(region, FlushReason::EngineFull);
                self.flush_scheduler.schedule_flush(region, task)?;
            }
        }

        Ok(())
    }

    fn new_flush_task(&self, region: &MitoRegionRef, reason: FlushReason) -> RegionFlushTask {
        // TODO(yingwen): metrics for flush requested.
        RegionFlushTask {
            region_id: region.region_id,
            reason,
            senders: Vec::new(),
            request_sender: self.sender.clone(),
            access_layer: region.access_layer.clone(),
            memtable_builder: self.memtable_builder.clone(),
            file_purger: region.file_purger.clone(),
        }
    }
}
