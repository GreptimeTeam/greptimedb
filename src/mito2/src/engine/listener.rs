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

//! Engine event listener for tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use common_telemetry::info;
use store_api::storage::{FileId, RegionId};
use tokio::sync::Notify;

use crate::sst::file::RegionFileId;

/// Mito engine background event listener.
#[async_trait]
pub trait EventListener: Send + Sync {
    /// Notifies the listener that a region is flushed successfully.
    fn on_flush_success(&self, region_id: RegionId) {
        let _ = region_id;
    }

    /// Notifies the listener that the engine is stalled.
    fn on_write_stall(&self) {}

    /// Notifies the listener that the region starts to do flush.
    async fn on_flush_begin(&self, region_id: RegionId) {
        let _ = region_id;
    }

    /// Notifies the listener that the later drop task starts running.
    /// Returns the gc interval if we want to override the default one.
    fn on_later_drop_begin(&self, region_id: RegionId) -> Option<Duration> {
        let _ = region_id;
        None
    }

    /// Notifies the listener that the later drop task of the region is finished.
    fn on_later_drop_end(&self, region_id: RegionId, removed: bool) {
        let _ = region_id;
        let _ = removed;
    }

    /// Notifies the listener that ssts has been merged and the region
    /// is going to update its manifest.
    async fn on_merge_ssts_finished(&self, region_id: RegionId) {
        let _ = region_id;
    }

    /// Notifies the listener that the worker receives requests from the request channel.
    fn on_recv_requests(&self, request_num: usize) {
        let _ = request_num;
    }

    /// Notifies the listener that the file cache is filled when, for example, editing region.
    fn on_file_cache_filled(&self, _file_id: FileId) {}

    /// Notifies the listener that the compaction is scheduled.
    fn on_compaction_scheduled(&self, _region_id: RegionId) {}

    /// Notifies the listener that region starts to send a region change result to worker.
    async fn on_notify_region_change_result_begin(&self, _region_id: RegionId) {}

    /// Notifies the listener that region starts to send a enter staging result to worker.
    async fn on_enter_staging_result_begin(&self, _region_id: RegionId) {}

    /// Notifies the listener that the index build task is executed successfully.
    async fn on_index_build_finish(&self, _region_file_id: RegionFileId) {}

    /// Notifies the listener that the index build task is started.
    async fn on_index_build_begin(&self, _region_file_id: RegionFileId) {}

    /// Notifies the listener that the index build task is aborted.
    async fn on_index_build_abort(&self, _region_file_id: RegionFileId) {}
}

pub type EventListenerRef = Arc<dyn EventListener>;

/// Listener to watch flush events.
#[derive(Default)]
pub struct FlushListener {
    notify: Notify,
    success_count: AtomicUsize,
}

impl FlushListener {
    /// Wait until one flush job is done.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }

    /// Returns the success count.
    pub fn success_count(&self) -> usize {
        self.success_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl EventListener for FlushListener {
    fn on_flush_success(&self, region_id: RegionId) {
        info!("Region {} flush successfully", region_id);

        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_one();
    }
}

/// Listener to watch stall events.
#[derive(Default)]
pub struct StallListener {
    notify: Notify,
}

impl StallListener {
    /// Wait for a stall event.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

#[async_trait]
impl EventListener for StallListener {
    fn on_flush_success(&self, _region_id: RegionId) {}

    fn on_write_stall(&self) {
        info!("Engine is stalled");

        self.notify.notify_one();
    }
}

/// Listener to watch begin flush events.
///
/// Creates a background thread to execute flush region, and the main thread calls `wait_truncate()`
/// to block and wait for `on_flush_region()`.
/// When the background thread calls `on_flush_begin()`, the main thread is notified to truncate
/// region, and background thread thread blocks and waits for `notify_flush()` to continue flushing.
#[derive(Default)]
pub struct FlushTruncateListener {
    /// Notify flush operation.
    notify_flush: Notify,
    /// Notify truncate operation.
    notify_truncate: Notify,
}

impl FlushTruncateListener {
    /// Notify flush region to proceed.
    pub fn notify_flush(&self) {
        self.notify_flush.notify_one();
    }

    /// Wait for a truncate event.
    pub async fn wait_truncate(&self) {
        self.notify_truncate.notified().await;
    }
}

#[async_trait]
impl EventListener for FlushTruncateListener {
    /// Calling this function will block the thread!
    /// Notify the listener to perform a truncate region and block the flush region job.
    async fn on_flush_begin(&self, region_id: RegionId) {
        info!(
            "Region {} begin do flush, notify region to truncate",
            region_id
        );
        self.notify_truncate.notify_one();
        self.notify_flush.notified().await;
    }
}

/// Listener on dropping.
pub struct DropListener {
    gc_duration: Duration,
    notify: Notify,
}

impl DropListener {
    /// Creates a new listener with specific `gc_duration`.
    pub fn new(gc_duration: Duration) -> Self {
        DropListener {
            gc_duration,
            notify: Notify::new(),
        }
    }

    /// Waits until later drop task is done.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

#[async_trait]
impl EventListener for DropListener {
    fn on_later_drop_begin(&self, _region_id: RegionId) -> Option<Duration> {
        Some(self.gc_duration)
    }

    fn on_later_drop_end(&self, _region_id: RegionId, removed: bool) {
        // Asserts result.
        assert!(removed);
        self.notify.notify_one();
    }
}

/// Listener on handling compaction requests.
#[derive(Default)]
pub struct CompactionListener {
    handle_finished_notify: Notify,
    blocker: Notify,
}

impl CompactionListener {
    /// Waits for handling compaction finished request.
    pub async fn wait_handle_finished(&self) {
        self.handle_finished_notify.notified().await;
    }

    /// Wakes up the listener.
    pub fn wake(&self) {
        self.blocker.notify_one();
    }
}

#[async_trait]
impl EventListener for CompactionListener {
    async fn on_merge_ssts_finished(&self, region_id: RegionId) {
        info!("Handle compaction finished request, region {region_id}");

        self.handle_finished_notify.notify_one();

        // Blocks current task.
        self.blocker.notified().await;
    }
}

/// Listener to block on flush and alter.
#[derive(Default)]
pub struct AlterFlushListener {
    flush_begin_notify: Notify,
    block_flush_notify: Notify,
    request_begin_notify: Notify,
}

impl AlterFlushListener {
    /// Waits on flush begin.
    pub async fn wait_flush_begin(&self) {
        self.flush_begin_notify.notified().await;
    }

    /// Waits on request begin.
    pub async fn wait_request_begin(&self) {
        self.request_begin_notify.notified().await;
    }

    /// Continue the flush job.
    pub fn wake_flush(&self) {
        self.block_flush_notify.notify_one();
    }
}

#[async_trait]
impl EventListener for AlterFlushListener {
    async fn on_flush_begin(&self, region_id: RegionId) {
        info!("Wait on notify to start flush for region {}", region_id);

        self.flush_begin_notify.notify_one();
        self.block_flush_notify.notified().await;

        info!("region {} begin flush", region_id);
    }

    fn on_recv_requests(&self, request_num: usize) {
        info!("receive {} request", request_num);

        self.request_begin_notify.notify_one();
    }
}

#[derive(Default)]
pub struct NotifyRegionChangeResultListener {
    notify: Notify,
}

impl NotifyRegionChangeResultListener {
    /// Continue to sending region change result.
    pub fn wake_notify(&self) {
        self.notify.notify_one();
    }
}

#[async_trait]
impl EventListener for NotifyRegionChangeResultListener {
    async fn on_notify_region_change_result_begin(&self, region_id: RegionId) {
        info!(
            "Wait on notify to start notify region change result for region {}",
            region_id
        );
        self.notify.notified().await;
        info!(
            "Continue to sending region change result for region {}",
            region_id
        );
    }
}

#[derive(Default)]
pub struct NotifyEnterStagingResultListener {
    notify: Notify,
}

impl NotifyEnterStagingResultListener {
    /// Continue to sending enter staging result.
    pub fn wake_notify(&self) {
        self.notify.notify_one();
    }
}

#[async_trait]
impl EventListener for NotifyEnterStagingResultListener {
    async fn on_enter_staging_result_begin(&self, region_id: RegionId) {
        info!(
            "Wait on notify to start notify enter staging result for region {}",
            region_id
        );
        self.notify.notified().await;
        info!(
            "Continue to sending enter staging result for region {}",
            region_id
        );
    }
}

#[derive(Default)]
pub struct IndexBuildListener {
    begin_count: AtomicUsize,
    begin_notify: Notify,
    finish_count: AtomicUsize,
    finish_notify: Notify,
    abort_count: AtomicUsize,
    abort_notify: Notify,
    // stop means finished or aborted
    stop_notify: Notify,
}

impl IndexBuildListener {
    /// Wait until index build is done for `times` times.
    pub async fn wait_finish(&self, times: usize) {
        while self.finish_count.load(Ordering::Relaxed) < times {
            self.finish_notify.notified().await;
        }
    }

    /// Wait until index build is stopped for `times` times.
    pub async fn wait_stop(&self, times: usize) {
        while self.finish_count.load(Ordering::Relaxed) + self.abort_count.load(Ordering::Relaxed)
            < times
        {
            self.stop_notify.notified().await;
        }
    }

    /// Wait until index build is begun for `times` times.
    pub async fn wait_begin(&self, times: usize) {
        while self.begin_count.load(Ordering::Relaxed) < times {
            self.begin_notify.notified().await;
        }
    }

    /// Clears the success count.
    pub fn clear_finish_count(&self) {
        self.finish_count.store(0, Ordering::Relaxed);
    }

    /// Returns the success count.
    pub fn finish_count(&self) -> usize {
        self.finish_count.load(Ordering::Relaxed)
    }

    /// Returns the start count.
    pub fn begin_count(&self) -> usize {
        self.begin_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl EventListener for IndexBuildListener {
    async fn on_index_build_finish(&self, region_file_id: RegionFileId) {
        info!("Region {} index build successfully", region_file_id);
        self.finish_count.fetch_add(1, Ordering::Relaxed);
        self.finish_notify.notify_one();
        self.stop_notify.notify_one();
    }

    async fn on_index_build_begin(&self, region_file_id: RegionFileId) {
        info!("Region {} index build begin", region_file_id);
        self.begin_count.fetch_add(1, Ordering::Relaxed);
        self.begin_notify.notify_one();
    }

    async fn on_index_build_abort(&self, region_file_id: RegionFileId) {
        info!("Region {} index build aborted", region_file_id);
        self.abort_count.fetch_add(1, Ordering::Relaxed);
        self.abort_notify.notify_one();
        self.stop_notify.notify_one();
    }
}
