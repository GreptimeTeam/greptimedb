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

use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadata;
use store_api::storage::RegionId;

use crate::error::{RejectWriteSnafu, Result};
use crate::metrics::{
    WRITE_REJECT_TOTAL, WRITE_ROWS_TOTAL, WRITE_STAGE_ELAPSED, WRITE_STALL_TOTAL,
};
use crate::region_write_ctx::RegionWriteCtx;
use crate::request::{SenderWriteRequest, WriteRequest};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    /// Takes and handles all write requests.
    pub(crate) async fn handle_write_requests(
        &mut self,
        mut write_requests: Vec<SenderWriteRequest>,
        allow_stall: bool,
    ) {
        if write_requests.is_empty() {
            return;
        }

        // Flush this worker if the engine needs to flush.
        self.maybe_flush_worker();

        if self.should_reject_write() {
            // The memory pressure is still too high, reject write requests.
            reject_write_requests(write_requests);
            // Also reject all stalled requests.
            let stalled = std::mem::take(&mut self.stalled_requests);
            reject_write_requests(stalled.requests);
            return;
        }

        if self.write_buffer_manager.should_stall() && allow_stall {
            WRITE_STALL_TOTAL.inc_by(write_requests.len() as u64);

            self.stalled_requests.append(&mut write_requests);
            self.listener.on_write_stall();
            return;
        }

        let mut region_ctxs = self.prepare_region_write_ctx(write_requests);

        // Write to WAL.
        {
            let _timer = WRITE_STAGE_ELAPSED
                .with_label_values(&["write_wal"])
                .start_timer();
            let mut wal_writer = self.wal.writer();
            for region_ctx in region_ctxs.values_mut() {
                if let Err(e) = region_ctx.add_wal_entry(&mut wal_writer).map_err(Arc::new) {
                    region_ctx.set_error(e);
                }
            }
            match wal_writer.write_to_wal().await.map_err(Arc::new) {
                Ok(response) => {
                    for (region_id, region_ctx) in region_ctxs.iter_mut() {
                        // Safety: the log store implementation ensures that either the `write_to_wal` fails and no
                        // response is returned or the last entry ids for each region do exist.
                        let last_entry_id =
                            response.last_entry_ids.get(&region_id.as_u64()).unwrap();
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
            for mut region_ctx in region_ctxs.into_values() {
                region_ctx.write_memtable();
                put_rows += region_ctx.put_num;
                delete_rows += region_ctx.delete_num;
            }
        }
        WRITE_ROWS_TOTAL
            .with_label_values(&["put"])
            .inc_by(put_rows as u64);
        WRITE_ROWS_TOTAL
            .with_label_values(&["delete"])
            .inc_by(delete_rows as u64);
    }
}

impl<S> RegionWorkerLoop<S> {
    /// Validates and groups requests by region.
    fn prepare_region_write_ctx(
        &mut self,
        write_requests: Vec<SenderWriteRequest>,
    ) -> HashMap<RegionId, RegionWriteCtx> {
        // Initialize region write context map.
        let mut region_ctxs = HashMap::new();
        for mut sender_req in write_requests {
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
                    .writable_region_or(region_id, &mut sender_req.sender)
                else {
                    // No such region or the region is read only.
                    continue;
                };

                let region_ctx = RegionWriteCtx::new(
                    region.region_id,
                    &region.version_control,
                    region.wal_options.clone(),
                );

                e.insert(region_ctx);
            }

            // Safety: Now we ensure the region exists.
            let region_ctx = region_ctxs.get_mut(&region_id).unwrap();

            // Checks whether request schema is compatible with region schema.
            if let Err(e) =
                maybe_fill_missing_columns(&mut sender_req.request, &region_ctx.version().metadata)
            {
                sender_req.sender.send(Err(e));

                continue;
            }

            // Collect requests by region.
            region_ctx.push_mutation(
                sender_req.request.op_type as i32,
                Some(sender_req.request.rows),
                sender_req.sender,
            );
        }

        region_ctxs
    }

    /// Returns true if the engine needs to reject some write requests.
    fn should_reject_write(&self) -> bool {
        // If memory usage reaches high threshold (we should also consider stalled requests) returns true.
        self.write_buffer_manager.memory_usage() + self.stalled_requests.estimated_size
            >= self.config.global_write_buffer_reject_size.as_bytes() as usize
    }
}

/// Send rejected error to all `write_requests`.
fn reject_write_requests(write_requests: Vec<SenderWriteRequest>) {
    WRITE_REJECT_TOTAL.inc_by(write_requests.len() as u64);

    for req in write_requests {
        req.sender.send(
            RejectWriteSnafu {
                region_id: req.request.region_id,
            }
            .fail(),
        );
    }
}

/// Checks the schema and fill missing columns.
fn maybe_fill_missing_columns(request: &mut WriteRequest, metadata: &RegionMetadata) -> Result<()> {
    if let Err(e) = request.check_schema(metadata) {
        if e.is_fill_default() {
            // TODO(yingwen): Add metrics for this case.
            // We need to fill default value. The write request may be a request
            // sent before changing the schema.
            request.fill_missing_columns(metadata)?;
        } else {
            return Err(e);
        }
    }

    Ok(())
}
