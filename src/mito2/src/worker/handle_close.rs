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

use common_telemetry::{info, warn};
use store_api::logstore::LogStore;
use store_api::region_request::{RegionCloseRequest, RegionFlushReason, RegionFlushRequest};
use store_api::storage::RegionId;

use crate::error::FlushableRegionStateSnafu;
use crate::manifest::action::cap_wal_release;
use crate::metrics::WAL_RELEASE_ADMISSION_FAILURE_TOTAL;
use crate::region::MitoRegionRef;
use crate::request::{DdlRequest, OptionOutputTx, SenderDdlRequest};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_close_request(
        &mut self,
        region_id: RegionId,
        request: RegionCloseRequest,
        sender: OptionOutputTx,
    ) {
        let Some(region) = self.regions.get_region(region_id) else {
            sender.send(Ok(0));
            return;
        };

        info!("Try to close region {}, worker: {}", region_id, self.id);

        if request.flush_on_close
            && let Err(e) = region.version().ensure_no_pending_tails()
        {
            sender.send(Err(e));
            return;
        }

        if region.in_staging_session() {
            // A staging region cannot flush, so it leaves staging before the close-time flush
            // below. The manifest lock lets a successful end of staging in progress finish first,
            // after which the region is already out of staging.
            let _manager = region.manifest_ctx.manifest_manager.write().await;
            region.abort_staging();
            // Edits queued behind a staged edit belong to the staging that has just ended.
            self.reject_region_edits_as_not_found(region_id);
        }

        // The refusal and the flush decision use one snapshot of the state, so a concurrent role
        // change cannot skip both.
        let needs_flush = close_needs_flush(&region, &request);
        let state = region.state();
        if request.flush_on_close && needs_flush && !state.is_flushable() {
            info!(
                "Region {} in state {:?} cannot flush before closing, keeping it open",
                region_id, state
            );
            sender.send(FlushableRegionStateSnafu { region_id, state }.fail());
            return;
        }
        if needs_flush && state.is_flushable() {
            // A region that awaits its Leader lease holds rows that exist nowhere else only if
            // it was written to, which happens only after it was routed to; a close, such as the
            // one of a shutdown, must not drop them. It gives up the role as a downgrade does, so
            // its flush commits under the downgrade's checks.
            if region.is_writable() && region.manifest_ctx.awaits_leader_lease() {
                region.manifest_ctx.begin_downgrade(region_id);
            }
            info!("Region {} has pending data, waiting for flush", region_id);
            if self.flush_scheduler.is_flush_requested(region_id) {
                self.flush_scheduler
                    .add_ddl_request_to_pending(SenderDdlRequest {
                        region_id,
                        sender,
                        request: DdlRequest::Close(request),
                    });
                self.attach_pending_closes_to_flush(region_id);
                return;
            }
            self.handle_flush_request(
                region_id,
                RegionFlushRequest {
                    reason: Some(RegionFlushReason::Closing),
                    ..Default::default()
                },
                sender,
            );
            if self.flush_scheduler.is_flush_requested(region_id) {
                self.attach_pending_closes_to_flush(region_id);
            }
            return;
        }

        let pending_closes = self.take_pending_closes(region_id);
        self.close_region(region_id).await;
        for ddl in pending_closes {
            ddl.sender.send(Ok(0));
        }
        info!("Region {} closed, worker: {}", region_id, self.id);
        sender.send(Ok(0))
    }

    /// Closes a region and queues a release of its persisted WAL history.
    /// Pending tails cap the release, as they do after a flush.
    /// The stopped manifest makes the watermark stable, and the worker awaits
    /// admission before it can process a reopen. Upload failures are retried by
    /// the store; failure to admit a release retains extra WAL history.
    pub(crate) async fn close_region(&mut self, region_id: RegionId) {
        let Some(region) = self.regions.get_region(region_id) else {
            return;
        };
        self.remove_region(region_id).await;
        let version = region.version();
        let release_entry_id = cap_wal_release(
            &version.pending_tails,
            self.file_ref_manager.node_id(),
            version.flushed_entry_id,
        );
        if let Err(error) = self
            .wal
            .release_entries(region_id, release_entry_id, &region.provider)
            .await
        {
            WAL_RELEASE_ADMISSION_FAILURE_TOTAL.inc();
            warn!(error; "Failed to queue WAL release for closed region {}", region_id);
        }
    }

    /// Remove a region and stop all related tasks.
    pub(crate) async fn remove_region(&mut self, region_id: RegionId) {
        let Some(region) = self.regions.get_region(region_id) else {
            return;
        };
        // Before the removal, so no catchup promotes an unregistered instance.
        region.stop().await;
        self.regions.remove_region(region_id);
        self.granted_leases.remove(region_id);
        self.deferred_lease_work.remove(&region_id);
        region.deactivate_region_manifest_size_metric();
        self.cancel_tail_replay(region_id);
        self.fail_region_stalled_requests_as_not_found(&region_id);
        self.reject_region_requests_as_not_found(region_id);
        // Clean flush status.
        self.flush_scheduler.on_region_closed(region_id);
        // Clean compaction status.
        self.compaction_scheduler.on_region_closed(region_id);
        // clean index build status.
        self.index_build_scheduler.on_region_closed(region_id).await;
        self.region_count.dec();

        // Notify the region hook that the region has been closed. The region is
        // fully stopped and unregistered, but its files/manifest are preserved.
        // Runs inline; the hook contract requires it to be fast.
        if let Some(hook) = region.manifest_ctx.hook() {
            let metadata = region.metadata();
            hook.on_region_closed(region_id, &metadata).await;
        }
    }

    /// Gives closes accepted during an edit the same terminal flush result.
    fn attach_pending_closes_to_flush(&mut self, region_id: RegionId) {
        for ddl in self.take_pending_closes(region_id) {
            self.flush_scheduler.add_ddl_request_to_pending(ddl);
        }
    }

    fn take_pending_closes(&mut self, region_id: RegionId) -> Vec<SenderDdlRequest> {
        self.pending_ddls.take_closes(region_id)
    }
}

/// Whether closing requires flushing data, independent of temporary region state.
pub(crate) fn close_needs_flush(region: &MitoRegionRef, request: &RegionCloseRequest) -> bool {
    let version = region.version();
    !version.memtables.is_empty()
        && (request.flush_on_close || region.skip_wal() || version.memtables.has_skip_wal_writes())
}
