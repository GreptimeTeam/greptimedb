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

//! Handling write requests.

use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use api::v1::OpType;
use common_telemetry::{debug, error};
use common_wal::options::WalOptions;
use snafu::ensure;
use store_api::codec::PrimaryKeyEncoding;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;

use crate::error::{InvalidRequestSnafu, RegionStateSnafu, RejectWriteSnafu, Result};
use crate::metrics;
use crate::metrics::{WRITE_REJECT_TOTAL, WRITE_ROWS_TOTAL, WRITE_STAGE_ELAPSED};
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::region_write_ctx::RegionWriteCtx;
use crate::request::{SenderBulkRequest, SenderWriteRequest, WriteRequest};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    /// Takes and handles all write requests.
    pub(crate) async fn handle_write_requests(
        &mut self,
        write_requests: &mut Vec<SenderWriteRequest>,
        bulk_requests: &mut Vec<SenderBulkRequest>,
        allow_stall: bool,
    ) {
        if write_requests.is_empty() && bulk_requests.is_empty() {
            return;
        }

        // Flush this worker if the engine needs to flush.
        self.maybe_flush_worker();

        if self.should_reject_write() {
            // The memory pressure is still too high, reject write requests.
            reject_write_requests(write_requests, bulk_requests);
            // Also reject all stalled requests.
            self.reject_stalled_requests();
            return;
        }

        if self.write_buffer_manager.should_stall() && allow_stall {
            self.stalled_count.add(write_requests.len() as i64);
            self.stalled_requests.append(write_requests, bulk_requests);
            self.listener.on_write_stall();
            return;
        }

        // Prepare write context.
        let mut region_ctxs = {
            let _timer = WRITE_STAGE_ELAPSED
                .with_label_values(&["prepare_ctx"])
                .start_timer();
            self.prepare_region_write_ctx(write_requests, bulk_requests)
        };

        // Write to WAL.
        {
            let _timer = WRITE_STAGE_ELAPSED
                .with_label_values(&["write_wal"])
                .start_timer();
            let mut wal_writer = self.wal.writer();
            for region_ctx in region_ctxs.values_mut() {
                if let WalOptions::Noop = &region_ctx.version().options.wal_options {
                    // Skip wal write for noop region.
                    continue;
                }
                if let Err(e) = region_ctx.add_wal_entry(&mut wal_writer).map_err(Arc::new) {
                    region_ctx.set_error(e);
                }
            }
            match wal_writer.write_to_wal().await.map_err(Arc::new) {
                Ok(response) => {
                    for (region_id, region_ctx) in region_ctxs.iter_mut() {
                        if let WalOptions::Noop = &region_ctx.version().options.wal_options {
                            continue;
                        }

                        // Safety: the log store implementation ensures that either the `write_to_wal` fails and no
                        // response is returned or the last entry ids for each region do exist.
                        let last_entry_id = response.last_entry_ids.get(region_id).unwrap();
                        region_ctx.set_next_entry_id(last_entry_id + 1);
                    }
                }
                Err(e) => {
                    // Failed to write wal.
                    for mut region_ctx in region_ctxs.into_values() {
                        region_ctx.set_error(e.clone());
                    }
                    return;
                }
            }
        }

        let (mut put_rows, mut delete_rows) = (0, 0);
        // Write to memtables.
        {
            let _timer = WRITE_STAGE_ELAPSED
                .with_label_values(&["write_memtable"])
                .start_timer();
            if region_ctxs.len() == 1 {
                // fast path for single region.
                let mut region_ctx = region_ctxs.into_values().next().unwrap();
                region_ctx.write_memtable().await;
                region_ctx.write_bulk().await;
                put_rows += region_ctx.put_num;
                delete_rows += region_ctx.delete_num;
            } else {
                let region_write_task = region_ctxs
                    .into_values()
                    .map(|mut region_ctx| {
                        // use tokio runtime to schedule tasks.
                        common_runtime::spawn_global(async move {
                            region_ctx.write_memtable().await;
                            region_ctx.write_bulk().await;
                            (region_ctx.put_num, region_ctx.delete_num)
                        })
                    })
                    .collect::<Vec<_>>();

                for result in futures::future::join_all(region_write_task).await {
                    match result {
                        Ok((put, delete)) => {
                            put_rows += put;
                            delete_rows += delete;
                        }
                        Err(e) => {
                            error!(e; "unexpected error when joining region write tasks");
                        }
                    }
                }
            }
        }
        WRITE_ROWS_TOTAL
            .with_label_values(&["put"])
            .inc_by(put_rows as u64);
        WRITE_ROWS_TOTAL
            .with_label_values(&["delete"])
            .inc_by(delete_rows as u64);
    }

    /// Handles all stalled write requests.
    pub(crate) async fn handle_stalled_requests(&mut self) {
        // Handle stalled requests.
        let stalled = std::mem::take(&mut self.stalled_requests);
        self.stalled_count.sub(stalled.stalled_count() as i64);
        // We already stalled these requests, don't stall them again.
        for (_, (_, mut requests, mut bulk)) in stalled.requests {
            self.handle_write_requests(&mut requests, &mut bulk, false)
                .await;
        }
    }

    /// Rejects all stalled requests.
    pub(crate) fn reject_stalled_requests(&mut self) {
        let stalled = std::mem::take(&mut self.stalled_requests);
        self.stalled_count.sub(stalled.stalled_count() as i64);
        for (_, (_, mut requests, mut bulk)) in stalled.requests {
            reject_write_requests(&mut requests, &mut bulk);
        }
    }

    /// Rejects a specific region's stalled requests.
    pub(crate) fn reject_region_stalled_requests(&mut self, region_id: &RegionId) {
        debug!("Rejects stalled requests for region {}", region_id);
        let (mut requests, mut bulk) = self.stalled_requests.remove(region_id);
        self.stalled_count.sub(requests.len() as i64);
        reject_write_requests(&mut requests, &mut bulk);
    }

    /// Handles a specific region's stalled requests.
    pub(crate) async fn handle_region_stalled_requests(&mut self, region_id: &RegionId) {
        debug!("Handles stalled requests for region {}", region_id);
        let (mut requests, mut bulk) = self.stalled_requests.remove(region_id);
        self.stalled_count.sub(requests.len() as i64);
        self.handle_write_requests(&mut requests, &mut bulk, true)
            .await;
    }
}

impl<S> RegionWorkerLoop<S> {
    /// Validates and groups requests by region.
    fn prepare_region_write_ctx(
        &mut self,
        write_requests: &mut Vec<SenderWriteRequest>,
        bulk_requests: &mut Vec<SenderBulkRequest>,
    ) -> HashMap<RegionId, RegionWriteCtx> {
        // Initialize region write context map.
        let mut region_ctxs = HashMap::new();
        self.process_write_requests(&mut region_ctxs, write_requests);
        self.process_bulk_requests(&mut region_ctxs, bulk_requests);
        region_ctxs
    }

    fn process_write_requests(
        &mut self,
        region_ctxs: &mut HashMap<RegionId, RegionWriteCtx>,
        write_requests: &mut Vec<SenderWriteRequest>,
    ) {
        for mut sender_req in write_requests.drain(..) {
            let region_id = sender_req.request.region_id;

            // If region is waiting for alteration, add requests to pending writes.
            if self.flush_scheduler.has_pending_ddls(region_id) {
                // TODO(yingwen): consider adding some metrics for this.
                // Safety: The region has pending ddls.
                self.flush_scheduler
                    .add_write_request_to_pending(sender_req);
                continue;
            }

            // Checks whether the region exists and is it stalling.
            if let hash_map::Entry::Vacant(e) = region_ctxs.entry(region_id) {
                let Some(region) = self
                    .regions
                    .get_region_or(region_id, &mut sender_req.sender)
                else {
                    // No such region.
                    continue;
                };
                match region.state() {
                    RegionRoleState::Leader(RegionLeaderState::Writable) => {
                        let region_ctx = RegionWriteCtx::new(
                            region.region_id,
                            &region.version_control,
                            region.provider.clone(),
                        );

                        e.insert(region_ctx);
                    }
                    RegionRoleState::Leader(RegionLeaderState::Altering) => {
                        debug!(
                            "Region {} is altering, add request to pending writes",
                            region.region_id
                        );
                        self.stalled_count.add(1);
                        self.stalled_requests.push(sender_req);
                        continue;
                    }
                    state => {
                        // The region is not writable.
                        sender_req.sender.send(
                            RegionStateSnafu {
                                region_id,
                                state,
                                expect: RegionRoleState::Leader(RegionLeaderState::Writable),
                            }
                            .fail(),
                        );
                        continue;
                    }
                }
            }

            // Safety: Now we ensure the region exists.
            let region_ctx = region_ctxs.get_mut(&region_id).unwrap();

            if let Err(e) = check_op_type(
                region_ctx.version().options.append_mode,
                &sender_req.request,
            ) {
                // Do not allow non-put op under append mode.
                sender_req.sender.send(Err(e));

                continue;
            }

            // Double check the request schema
            let need_fill_missing_columns =
                if let Some(ref region_metadata) = sender_req.request.region_metadata {
                    region_ctx.version().metadata.schema_version != region_metadata.schema_version
                } else {
                    true
                };
            // Only fill missing columns if primary key is dense encoded.
            if need_fill_missing_columns
                && sender_req.request.primary_key_encoding() == PrimaryKeyEncoding::Dense
            {
                if let Err(e) = sender_req
                    .request
                    .maybe_fill_missing_columns(&region_ctx.version().metadata)
                {
                    sender_req.sender.send(Err(e));

                    continue;
                }
            }

            // Collect requests by region.
            region_ctx.push_mutation(
                sender_req.request.op_type as i32,
                Some(sender_req.request.rows),
                sender_req.request.hint,
                sender_req.sender,
            );
        }
    }

    /// Processes bulk insert requests.
    fn process_bulk_requests(
        &mut self,
        region_ctxs: &mut HashMap<RegionId, RegionWriteCtx>,
        requests: &mut Vec<SenderBulkRequest>,
    ) {
        let _timer = metrics::REGION_WORKER_HANDLE_WRITE_ELAPSED
            .with_label_values(&["prepare_bulk_request"])
            .start_timer();
        for mut bulk_req in requests.drain(..) {
            let region_id = bulk_req.region_id;
            // If region is waiting for alteration, add requests to pending writes.
            if self.flush_scheduler.has_pending_ddls(region_id) {
                // Safety: The region has pending ddls.
                self.flush_scheduler.add_bulk_request_to_pending(bulk_req);
                continue;
            }

            // Checks whether the region exists and is it stalling.
            if let hash_map::Entry::Vacant(e) = region_ctxs.entry(region_id) {
                let Some(region) = self.regions.get_region_or(region_id, &mut bulk_req.sender)
                else {
                    continue;
                };
                match region.state() {
                    RegionRoleState::Leader(RegionLeaderState::Writable) => {
                        let region_ctx = RegionWriteCtx::new(
                            region.region_id,
                            &region.version_control,
                            region.provider.clone(),
                        );

                        e.insert(region_ctx);
                    }
                    RegionRoleState::Leader(RegionLeaderState::Altering) => {
                        debug!(
                            "Region {} is altering, add request to pending writes",
                            region.region_id
                        );
                        self.stalled_count.add(1);
                        self.stalled_requests.push_bulk(bulk_req);
                        continue;
                    }
                    state => {
                        // The region is not writable.
                        bulk_req.sender.send(
                            RegionStateSnafu {
                                region_id,
                                state,
                                expect: RegionRoleState::Leader(RegionLeaderState::Writable),
                            }
                            .fail(),
                        );
                        continue;
                    }
                }
            }

            // Safety: Now we ensure the region exists.
            let region_ctx = region_ctxs.get_mut(&region_id).unwrap();

            // Double-check the request schema
            let need_fill_missing_columns = region_ctx.version().metadata.schema_version
                != bulk_req.region_metadata.schema_version;

            // Only fill missing columns if primary key is dense encoded.
            if need_fill_missing_columns {
                // todo(hl): support filling default columns
                bulk_req.sender.send(
                    InvalidRequestSnafu {
                        region_id,
                        reason: "Schema mismatch",
                    }
                    .fail(),
                );
                return;
            }

            // Collect requests by region.
            region_ctx.push_bulk(bulk_req.sender, bulk_req.request);
        }
    }

    /// Returns true if the engine needs to reject some write requests.
    pub(crate) fn should_reject_write(&self) -> bool {
        // If memory usage reaches high threshold (we should also consider stalled requests) returns true.
        self.write_buffer_manager.memory_usage() + self.stalled_requests.estimated_size
            >= self.config.global_write_buffer_reject_size.as_bytes() as usize
    }
}

/// Send rejected error to all `write_requests`.
fn reject_write_requests(
    write_requests: &mut Vec<SenderWriteRequest>,
    bulk_requests: &mut Vec<SenderBulkRequest>,
) {
    WRITE_REJECT_TOTAL.inc_by(write_requests.len() as u64);

    for req in write_requests.drain(..) {
        req.sender.send(
            RejectWriteSnafu {
                region_id: req.request.region_id,
            }
            .fail(),
        );
    }
    for req in bulk_requests.drain(..) {
        let region_id = req.region_id;
        req.sender.send(RejectWriteSnafu { region_id }.fail());
    }
}

/// Rejects delete request under append mode.
fn check_op_type(append_mode: bool, request: &WriteRequest) -> Result<()> {
    if append_mode {
        ensure!(
            request.op_type == OpType::Put,
            InvalidRequestSnafu {
                region_id: request.region_id,
                reason: "DELETE is not allowed under append mode",
            }
        );
    }

    Ok(())
}
