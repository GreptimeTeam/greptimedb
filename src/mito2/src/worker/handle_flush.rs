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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_telemetry::{error, info};
use store_api::logstore::LogStore;
use store_api::region_request::RegionFlushRequest;
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::error::{RegionNotFoundSnafu, Result};
use crate::flush::{FlushReason, RegionFlushTask};
use crate::region::MitoRegionRef;
use crate::request::{FlushFailed, FlushFinished, OnFailure, OptionOutputTx};
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
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

    /// Finds some regions to flush to reduce write buffer usage.
    fn flush_regions_on_engine_full(&mut self) -> Result<()> {
        let regions = self.regions.list_regions();
        let now = self.time_provider.current_time_millis();
        let min_last_flush_time = now - self.config.auto_flush_interval.as_millis() as i64;
        let mut max_mutable_size = 0;
        // Region with max mutable memtable size.
        let mut max_mem_region = None;

        for region in &regions {
            if self.flush_scheduler.is_flush_requested(region.region_id) || !region.is_writable() {
                // Already flushing or not writable.
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
                let task =
                    self.new_flush_task(region, FlushReason::EngineFull, None, self.config.clone());
                self.flush_scheduler.schedule_flush(
                    region.region_id,
                    &region.version_control,
                    task,
                )?;
            }
        }

        // Flush memtable with max mutable memtable.
        // TODO(yingwen): Maybe flush more tables to reduce write buffer size.
        if let Some(region) = max_mem_region {
            if !self.flush_scheduler.is_flush_requested(region.region_id) {
                let task =
                    self.new_flush_task(region, FlushReason::EngineFull, None, self.config.clone());
                self.flush_scheduler.schedule_flush(
                    region.region_id,
                    &region.version_control,
                    task,
                )?;
            }
        }

        Ok(())
    }

    /// Creates a flush task with specific `reason` for the `region`.
    pub(crate) fn new_flush_task(
        &self,
        region: &MitoRegionRef,
        reason: FlushReason,
        row_group_size: Option<usize>,
        engine_config: Arc<MitoConfig>,
    ) -> RegionFlushTask {
        RegionFlushTask {
            region_id: region.region_id,
            reason,
            senders: Vec::new(),
            request_sender: self.sender.clone(),
            access_layer: region.access_layer.clone(),
            listener: self.listener.clone(),
            engine_config,
            row_group_size,
            cache_manager: self.cache_manager.clone(),
            manifest_ctx: region.manifest_ctx.clone(),
            index_options: region.version().options.index_options.clone(),
        }
    }
}

impl<S: LogStore> RegionWorkerLoop<S> {
    /// Handles manual flush request.
    pub(crate) async fn handle_flush_request(
        &mut self,
        region_id: RegionId,
        request: RegionFlushRequest,
        mut sender: OptionOutputTx,
    ) {
        let Some(region) = self.regions.flushable_region_or(region_id, &mut sender) else {
            return;
        };
        // `update_topic_latest_entry_id` updates `topic_latest_entry_id` when memtables are empty.
        // But the flush is skipped if memtables are empty. Thus should update the `topic_latest_entry_id`
        // when handling flush request instead of in `schedule_flush` or `flush_finished`.
        self.update_topic_latest_entry_id(&region);
        info!(
            "Region {} flush request, high watermark: {}",
            region_id,
            region.topic_latest_entry_id.load(Ordering::Relaxed)
        );

        let reason = if region.is_downgrading() {
            FlushReason::Downgrading
        } else {
            FlushReason::Manual
        };

        let mut task =
            self.new_flush_task(&region, reason, request.row_group_size, self.config.clone());
        task.push_sender(sender);
        if let Err(e) =
            self.flush_scheduler
                .schedule_flush(region.region_id, &region.version_control, task)
        {
            error!(e; "Failed to schedule flush task for region {}", region.region_id);
        }
    }

    /// Flushes regions periodically.
    pub(crate) fn flush_periodically(&mut self) -> Result<()> {
        let regions = self.regions.list_regions();
        let now = self.time_provider.current_time_millis();
        let min_last_flush_time = now - self.config.auto_flush_interval.as_millis() as i64;

        for region in &regions {
            if self.flush_scheduler.is_flush_requested(region.region_id) || !region.is_writable() {
                // Already flushing or not writable.
                continue;
            }
            self.update_topic_latest_entry_id(region);

            if region.last_flush_millis() < min_last_flush_time {
                // If flush time of this region is earlier than `min_last_flush_time`, we can flush this region.
                let task = self.new_flush_task(
                    region,
                    FlushReason::Periodically,
                    None,
                    self.config.clone(),
                );
                self.flush_scheduler.schedule_flush(
                    region.region_id,
                    &region.version_control,
                    task,
                )?;
            }
        }

        Ok(())
    }

    /// On region flush job finished.
    pub(crate) async fn handle_flush_finished(
        &mut self,
        region_id: RegionId,
        mut request: FlushFinished,
    ) {
        // Notifies other workers. Even the remaining steps of this method fail we still
        // wake up other workers as we have released some memory by flush.
        self.notify_group();

        let region = match self.regions.get_region(region_id) {
            Some(region) => region,
            None => {
                request.on_failure(RegionNotFoundSnafu { region_id }.build());
                return;
            }
        };

        region.version_control.apply_edit(
            request.edit.clone(),
            &request.memtables_to_remove,
            region.file_purger.clone(),
        );

        region.update_flush_millis();

        // Delete wal.
        info!(
            "Region {} flush finished, tries to bump wal to {}",
            region_id, request.flushed_entry_id
        );
        if let Err(e) = self
            .wal
            .obsolete(region_id, request.flushed_entry_id, &region.provider)
            .await
        {
            error!(e; "Failed to write wal, region: {}", region_id);
            request.on_failure(e);
            return;
        }

        // Notifies waiters and observes the flush timer.
        request.on_success();

        // Handle pending requests for the region.
        if let Some((mut ddl_requests, mut write_requests)) =
            self.flush_scheduler.on_flush_success(region_id)
        {
            // Perform DDLs first because they require empty memtables.
            self.handle_ddl_requests(&mut ddl_requests).await;
            // Handle pending write requests, we don't stall these requests.
            self.handle_write_requests(&mut write_requests, false).await;
        }

        // Handle stalled requests.
        self.handle_stalled_requests().await;

        // Schedules compaction.
        self.schedule_compaction(&region).await;

        self.listener.on_flush_success(region_id);
    }

    /// Updates the latest entry id since flush of the region.
    /// **This is only used for remote WAL pruning.**
    pub(crate) fn update_topic_latest_entry_id(&mut self, region: &MitoRegionRef) {
        if region.provider.is_remote_wal() && region.version().memtables.is_empty() {
            let high_watermark = self
                .wal
                .store()
                .high_watermark(&region.provider)
                .unwrap_or(0);
            if high_watermark != 0 {
                region
                    .topic_latest_entry_id
                    .store(high_watermark, Ordering::Relaxed);
            }
            info!(
                "Region {} high watermark updated to {}",
                region.region_id, high_watermark
            );
        }
    }
}
