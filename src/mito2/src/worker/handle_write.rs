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
use std::mem;
use std::sync::Arc;

use greptime_proto::v1::mito::{Mutation, WalEntry};
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::storage::{RegionId, SequenceNumber};
use tokio::sync::oneshot::Sender;

use crate::error::{Error, RegionNotFoundSnafu, Result, WriteGroupSnafu};
use crate::proto_util::to_proto_op_type;
use crate::region::version::{VersionControlData, VersionRef};
use crate::region::MitoRegionRef;
use crate::request::SenderWriteRequest;
use crate::wal::{EntryId, WalWriter};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    /// Takes and handles all write requests.
    pub(crate) async fn handle_write_requests(&mut self, write_requests: Vec<SenderWriteRequest>) {
        if write_requests.is_empty() {
            return;
        }

        let mut region_ctxs = self.prepare_region_write_ctx(write_requests);

        // Write WAL.
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

        todo!()
    }
}

impl<S> RegionWorkerLoop<S> {
    /// Validates and groups requests by region.
    fn prepare_region_write_ctx(
        &self,
        write_requests: Vec<SenderWriteRequest>,
    ) -> HashMap<RegionId, RegionWriteCtx> {
        let mut region_ctxs = HashMap::new();
        for sender_req in write_requests {
            let region_id = sender_req.request.region_id;
            // Checks whether the region exists.
            if let hash_map::Entry::Vacant(e) = region_ctxs.entry(region_id) {
                let Some(region) = self.regions.get_region(region_id) else {
                    // No such region.
                    send_result(sender_req.sender, RegionNotFoundSnafu { region_id }.fail());

                    continue;
                };

                // Initialize the context.
                e.insert(RegionWriteCtx::new(region));
            }

            // Safety: Now we ensure the region exists.
            let region_ctx = region_ctxs.get_mut(&region_id).unwrap();

            // Checks whether request schema is compatible with region schema.
            if let Err(e) = sender_req
                .request
                .check_schema(&region_ctx.version.metadata)
            {
                send_result(sender_req.sender, Err(e));

                continue;
            }

            // Collect requests by region.
            region_ctx.push_sender_request(sender_req);
        }

        region_ctxs
    }
}

/// Send result to the request.
fn send_result(sender: Option<Sender<Result<()>>>, res: Result<()>) {
    if let Some(sender) = sender {
        // Ignore send result.
        let _ = sender.send(res);
    }
}

/// Context to keep region metadata and buffer write requests.
struct RegionWriteCtx {
    /// Region to write.
    region: MitoRegionRef,
    /// Version of the region while creating the context.
    version: VersionRef,
    /// Next sequence number to write.
    ///
    /// The context assigns a unique sequence number for each row.
    next_sequence: SequenceNumber,
    /// Next entry id of WAL to write.
    next_entry_id: EntryId,
    /// Valid WAL entry to write.
    wal_entry: WalEntry,
    /// Error during writing this region.
    err: Option<Arc<Error>>,
    /// Result senders.
    ///
    /// All senders will receive the same result.
    senders: Vec<Sender<Result<()>>>,
}

impl RegionWriteCtx {
    /// Returns an empty context.
    fn new(region: MitoRegionRef) -> RegionWriteCtx {
        let VersionControlData {
            version,
            committed_sequence,
            last_entry_id,
        } = region.version_control.current();
        RegionWriteCtx {
            region,
            version,
            next_sequence: committed_sequence + 1,
            next_entry_id: last_entry_id + 1,
            wal_entry: WalEntry::default(),
            err: None,
            senders: Vec::new(),
        }
    }

    /// Push [SenderWriteRequest] to the context.
    fn push_sender_request(&mut self, sender_req: SenderWriteRequest) {
        let num_rows = sender_req.request.rows.rows.len() as u64;

        self.wal_entry.mutations.push(Mutation {
            op_type: to_proto_op_type(sender_req.request.op_type) as i32,
            sequence: self.next_sequence,
            rows: Some(sender_req.request.rows),
        });
        if let Some(sender) = sender_req.sender {
            self.senders.push(sender);
        }

        // Increase sequence number.
        self.next_sequence += num_rows;
    }

    /// Encode and add WAL entry to the writer.
    fn add_wal_entry<S: LogStore>(&self, wal_writer: &mut WalWriter<S>) -> Result<()> {
        wal_writer.add_entry(self.region.region_id, self.next_entry_id, &self.wal_entry)
    }

    /// Sets error and marks the write operation is failed.
    ///
    /// The context will send the error to waiters on drop.
    fn set_error(&mut self, err: Arc<Error>) {
        self.err = Some(err);
    }

    /// Sends result to waiters.
    fn notify_result(&mut self) {
        let senders = mem::take(&mut self.senders);
        for sender in senders {
            if let Some(err) = &self.err {
                // Try to send the error to waiters.
                let _ = sender.send(Err(err.clone()).context(WriteGroupSnafu));
            } else {
                // Send success result.
                let _ = sender.send(Ok(()));
            }
        }
    }
}

impl Drop for RegionWriteCtx {
    fn drop(&mut self) {
        self.notify_result();
    }
}
