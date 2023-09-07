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

use common_query::Output;
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadata;
use store_api::storage::RegionId;
use tokio::sync::oneshot::Sender;

use crate::error::{RegionNotFoundSnafu, RejectWriteSnafu, Result};
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
            // TODO(yingwen): stalled metrics.
            self.stalled_requests.append(&mut write_requests);
            self.listener.on_write_stall();
            return;
        }

        let mut region_ctxs = self.prepare_region_write_ctx(write_requests);

        // Write to WAL.
        let mut wal_writer = self.wal.writer();
        for region_ctx in region_ctxs.values_mut() {
            if let Err(e) = region_ctx.add_wal_entry(&mut wal_writer).map_err(Arc::new) {
                region_ctx.set_error(e);
            }
        }
        if let Err(e) = wal_writer.write_to_wal().await.map_err(Arc::new) {
            // Failed to write wal.
            for mut region_ctx in region_ctxs.into_values() {
                region_ctx.set_error(e.clone());
            }
            return;
        }

        // Write to memtables.
        for mut region_ctx in region_ctxs.into_values() {
            region_ctx.write_memtable();
        }
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

            // Checks whether the region exists and is it stalling.
            if let hash_map::Entry::Vacant(e) = region_ctxs.entry(region_id) {
                let Some(region) = self.regions.get_region(region_id) else {
                    // No such region.
                    send_result(sender_req.sender, RegionNotFoundSnafu { region_id }.fail());

                    continue;
                };

                let region_ctx = RegionWriteCtx::new(region.region_id, &region.version_control);

                e.insert(region_ctx);
            }

            // Safety: Now we ensure the region exists.
            let region_ctx = region_ctxs.get_mut(&region_id).unwrap();

            // Checks whether request schema is compatible with region schema.
            if let Err(e) =
                maybe_fill_missing_columns(&mut sender_req.request, &region_ctx.version().metadata)
            {
                send_result(sender_req.sender, Err(e));

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
    for req in write_requests {
        if let Some(sender) = req.sender {
            let _ = sender.send(
                RejectWriteSnafu {
                    region_id: req.request.region_id,
                }
                .fail(),
            );
        }
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

/// Send result to the request.
fn send_result(sender: Option<Sender<Result<Output>>>, res: Result<Output>) {
    if let Some(sender) = sender {
        // Ignore send result.
        let _ = sender.send(res);
    }
}
