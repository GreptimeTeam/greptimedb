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

use std::collections::{HashMap, HashSet, hash_map};
use std::sync::Arc;

use api::v1::OpType;
use common_telemetry::{debug, error};
use snafu::ensure;
use store_api::codec::PrimaryKeyEncoding;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;

use crate::error::{
    InvalidRequestSnafu, PartitionExprVersionMismatchSnafu, RegionNotFoundSnafu, RegionStateSnafu,
    RejectWriteSnafu, Result,
};
use crate::metrics;
use crate::metrics::{
    WRITE_REJECT_TOTAL, WRITE_ROWS_TOTAL, WRITE_STAGE_ELAPSED, WRITE_STALL_TOTAL,
};
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::region_write_ctx::RegionWriteCtx;
use crate::request::{SenderBulkRequest, SenderWriteRequest, WriteRequest};
use crate::wal::Wal;
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

        let write_region_ids = write_region_ids(write_requests, bulk_requests);

        // Check region pressure before writes to match the global write buffer behavior.
        self.maybe_flush_worker();
        let pressure = self.maybe_flush_write_regions(write_region_ids);

        if self.should_reject_write() {
            // The memory pressure is still too high, reject write requests.
            reject_write_requests(write_requests, bulk_requests);
            // Also reject all stalled requests.
            self.reject_stalled_requests();
            return;
        }

        if !pressure.rejected_region_ids.is_empty() {
            reject_region_write_requests(
                &pressure.rejected_region_ids,
                write_requests,
                bulk_requests,
            );
            for region_id in &pressure.rejected_region_ids {
                self.reject_region_stalled_requests(region_id);
            }
            if write_requests.is_empty() && bulk_requests.is_empty() {
                return;
            }
        }

        if self.write_buffer_manager.should_stall() && allow_stall {
            let stalled_count = (write_requests.len() + bulk_requests.len()) as i64;
            self.stalling_count.add(stalled_count);
            WRITE_STALL_TOTAL.inc_by(stalled_count as u64);
            self.stalled_requests.append(write_requests, bulk_requests);
            self.listener.on_write_stall();
            return;
        }

        if allow_stall {
            self.stall_region_write_requests(
                &pressure.stalled_region_ids,
                write_requests,
                bulk_requests,
            );
            if write_requests.is_empty() && bulk_requests.is_empty() {
                return;
            }
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
            if !write_wal(&self.wal, &mut region_ctxs).await {
                // Failed to write to the WAL, all waiters are notified with the error.
                return;
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
                // Publish only after all rows (including bulk parts) are
                // physically installed, so a scan opening on the committed
                // sequence can never bind H to invisible rows.
                region_ctx.publish_sequence_and_entry_id();
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
                            // The spawned task owns the moved ctx, so publish
                            // inside the task after the memtable writes.
                            region_ctx.publish_sequence_and_entry_id();
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

    /// Handles stalled write requests whose regions no longer need to stall.
    pub(crate) async fn handle_stalled_requests(&mut self) {
        let region_ids = self
            .stalled_requests
            .requests
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        let pressure = self.maybe_flush_write_regions(region_ids);
        for region_id in &pressure.rejected_region_ids {
            self.reject_region_stalled_requests(region_id);
        }
        let ready_region_ids = self
            .stalled_requests
            .requests
            .keys()
            .filter(|region_id| !pressure.stalled_region_ids.contains(region_id))
            .copied()
            .collect::<Vec<_>>();

        // These requests have already been stalled. Retry ready regions without stalling the
        // same requests again. Regions that still exceed their limit remain in the queue until
        // their own flush releases the pressure.
        for region_id in ready_region_ids {
            self.handle_region_stalled_requests(&region_id, false).await;
        }
    }

    /// Rejects all stalled requests.
    pub(crate) fn reject_stalled_requests(&mut self) {
        let stalled = std::mem::take(&mut self.stalled_requests);
        self.stalling_count.sub(stalled.stalled_count() as i64);
        for (_, (_, mut requests, mut bulk)) in stalled.requests {
            reject_write_requests(&mut requests, &mut bulk);
        }
    }

    /// Rejects a specific region's stalled requests.
    pub(crate) fn reject_region_stalled_requests(&mut self, region_id: &RegionId) {
        debug!("Rejects stalled requests for region {}", region_id);
        let (mut requests, mut bulk) = self.stalled_requests.remove(region_id);
        self.stalling_count
            .sub((requests.len() + bulk.len()) as i64);
        reject_write_requests(&mut requests, &mut bulk);
    }

    /// Fails a specific region's stalled requests if the region no longer exists.
    pub(crate) fn fail_region_stalled_requests_as_not_found(&mut self, region_id: &RegionId) {
        debug!(
            "Fails stalled requests for region {} as region not found",
            region_id
        );
        let (requests, bulk) = self.stalled_requests.remove(region_id);
        self.stalling_count
            .sub((requests.len() + bulk.len()) as i64);

        for req in requests {
            req.sender.send(
                RegionNotFoundSnafu {
                    region_id: req.request.region_id,
                }
                .fail(),
            );
        }
        for req in bulk {
            req.sender.send(
                RegionNotFoundSnafu {
                    region_id: req.region_id,
                }
                .fail(),
            );
        }
    }

    /// Handles a specific region's stalled requests.
    ///
    /// `allow_stall` should be false for backpressure retry paths to avoid stalling the same
    /// requests again. It should remain true for non-backpressure retries, such as requests stalled
    /// by alter, staging, and region editing. Global reject backpressure still applies before the
    /// stall check.
    pub(crate) async fn handle_region_stalled_requests(
        &mut self,
        region_id: &RegionId,
        allow_stall: bool,
    ) {
        debug!("Handles stalled requests for region {}", region_id);
        let (mut requests, mut bulk) = self.stalled_requests.remove(region_id);
        self.stalling_count
            .sub((requests.len() + bulk.len()) as i64);
        self.handle_write_requests(&mut requests, &mut bulk, allow_stall)
            .await;
    }

    /// Processes same-batch writes for a region before handling its edit-completion notification.
    ///
    /// The worker dispatch loop handles background notifications before the current batch's write
    /// buffer. Without this step, writes that arrived during edit N could be classified only after
    /// edit N+1 is started, placing them behind that next edit.
    pub(crate) async fn handle_buffered_region_write_requests(
        &mut self,
        region_id: &RegionId,
        write_requests: &mut Vec<SenderWriteRequest>,
        bulk_requests: &mut Vec<SenderBulkRequest>,
    ) {
        let mut current_region_write_requests = write_requests
            .extract_if(.., |r| r.request.region_id == *region_id)
            .collect::<Vec<_>>();

        let mut current_region_bulk_requests = bulk_requests
            .extract_if(.., |r| r.region_id == *region_id)
            .collect::<Vec<_>>();

        self.handle_write_requests(
            &mut current_region_write_requests,
            &mut current_region_bulk_requests,
            true,
        )
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
                #[cfg(test)]
                debug!(
                    "Handling write request for region {}, state: {:?}",
                    region_id,
                    region.state()
                );
                match region.state() {
                    RegionRoleState::Leader(RegionLeaderState::Writable)
                    | RegionRoleState::Leader(RegionLeaderState::Staging) => {
                        if region.reject_all_writes_in_staging() {
                            sender_req
                                .sender
                                .send(RejectWriteSnafu { region_id }.fail());
                            continue;
                        }

                        let region_ctx = RegionWriteCtx::new(
                            region.region_id,
                            &region.version_control,
                            region.provider.clone(),
                            Some(region.region_stats.written_bytes.clone()),
                        );

                        e.insert(region_ctx);
                    }
                    RegionRoleState::Leader(RegionLeaderState::Altering)
                    | RegionRoleState::Leader(RegionLeaderState::Editing) => {
                        // Editing is transient: queue the write so edit completion can drain it
                        // before starting the next queued edit.
                        debug!(
                            "Region {} is {:?}, add request to pending writes",
                            region.region_id,
                            region.state()
                        );
                        self.stalling_count.add(1);
                        WRITE_STALL_TOTAL.inc();
                        self.stalled_requests.push(sender_req);
                        continue;
                    }
                    RegionRoleState::Leader(RegionLeaderState::EnteringStaging) => {
                        debug!(
                            "Region {} is entering staging, add request to pending writes",
                            region.region_id
                        );
                        self.stalling_count.add(1);
                        WRITE_STALL_TOTAL.inc();
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
            let Some(region) = self
                .regions
                .get_region_or(region_id, &mut sender_req.sender)
            else {
                continue;
            };
            if region.reject_all_writes_in_staging() {
                sender_req
                    .sender
                    .send(RejectWriteSnafu { region_id }.fail());
                continue;
            }
            let expected_version = region.expected_partition_expr_version();
            if let Err(e) = check_partition_expr_version(
                region_id,
                expected_version,
                sender_req.request.partition_expr_version,
            ) {
                sender_req.sender.send(Err(e));
                continue;
            }

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
                && let Err(e) = sender_req
                    .request
                    .maybe_fill_missing_columns(&region_ctx.version().metadata)
            {
                sender_req.sender.send(Err(e));

                continue;
            }

            // Collect requests by region.
            region_ctx.push_mutation(
                sender_req.request.op_type as i32,
                Some(sender_req.request.rows),
                sender_req.request.hint,
                sender_req.sender,
                None,
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
                    RegionRoleState::Leader(RegionLeaderState::Writable)
                    | RegionRoleState::Leader(RegionLeaderState::Staging) => {
                        if region.reject_all_writes_in_staging() {
                            bulk_req.sender.send(RejectWriteSnafu { region_id }.fail());
                            continue;
                        }
                        let region_ctx = RegionWriteCtx::new(
                            region.region_id,
                            &region.version_control,
                            region.provider.clone(),
                            Some(region.region_stats.written_bytes.clone()),
                        );

                        e.insert(region_ctx);
                    }
                    RegionRoleState::Leader(RegionLeaderState::Altering)
                    | RegionRoleState::Leader(RegionLeaderState::Editing) => {
                        // Editing is transient: queue the bulk write so edit completion can drain
                        // it before starting the next queued edit.
                        debug!(
                            "Region {} is {:?}, add request to pending writes",
                            region.region_id,
                            region.state()
                        );
                        self.stalling_count.add(1);
                        WRITE_STALL_TOTAL.inc();
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
            let Some(region) = self.regions.get_region_or(region_id, &mut bulk_req.sender) else {
                continue;
            };
            if region.reject_all_writes_in_staging() {
                bulk_req.sender.send(RejectWriteSnafu { region_id }.fail());
                continue;
            }
            let expected_version = region.expected_partition_expr_version();
            if let Err(e) = check_partition_expr_version(
                region_id,
                expected_version,
                bulk_req.partition_expr_version,
            ) {
                bulk_req.sender.send(Err(e));
                continue;
            }

            // Double-check the request schema
            let need_fill_missing_columns =
                !bulk_req.region_metadata.is_some_and(|aligned_schema| {
                    aligned_schema.schema_version == region_ctx.version().metadata.schema_version
                });

            // Fill missing columns if needed
            if need_fill_missing_columns
                && let Err(e) = bulk_req
                    .request
                    .fill_missing_columns(&region_ctx.version().metadata)
            {
                bulk_req.sender.send(Err(e));
                continue;
            }

            // Collect requests by region.
            if !region_ctx.push_bulk(bulk_req.sender, bulk_req.request, None) {
                return;
            }
        }
    }

    /// Returns true if the engine needs to reject some write requests.
    pub(crate) fn should_reject_write(&self) -> bool {
        // If memory usage reaches high threshold (we should also consider stalled requests) returns true.
        self.write_buffer_manager.memory_usage() + self.stalled_requests.estimated_size
            >= self.config.global_write_buffer_reject_size.as_bytes() as usize
    }

    fn stall_region_write_requests(
        &mut self,
        stalled_region_ids: &HashSet<RegionId>,
        write_requests: &mut Vec<SenderWriteRequest>,
        bulk_requests: &mut Vec<SenderBulkRequest>,
    ) {
        let mut stalled_count = 0;
        let mut stalled_write_requests = write_requests
            .extract_if(.., |req| {
                stalled_region_ids.contains(&req.request.region_id)
            })
            .collect::<Vec<_>>();
        let mut stalled_bulk_requests = bulk_requests
            .extract_if(.., |req| stalled_region_ids.contains(&req.region_id))
            .collect::<Vec<_>>();

        stalled_count += stalled_write_requests.len() + stalled_bulk_requests.len();
        self.stalled_requests
            .append(&mut stalled_write_requests, &mut stalled_bulk_requests);

        if stalled_count > 0 {
            let stalled_count = stalled_count as i64;
            self.stalling_count.add(stalled_count);
            WRITE_STALL_TOTAL.inc_by(stalled_count as u64);
            self.listener.on_write_stall();
        }
    }
}

/// Writes WAL entries of all region contexts to the WAL in one batch and updates
/// the next entry id of each region on success.
///
/// Returns `false` if the batch fails to be written to the WAL. In this case all
/// contexts are consumed and their waiters are notified with the error, so the
/// caller should skip the memtable phase.
async fn write_wal<S: LogStore>(
    wal: &Wal<S>,
    region_ctxs: &mut HashMap<RegionId, RegionWriteCtx>,
) -> bool {
    let mut wal_writer = wal.writer();
    for region_ctx in region_ctxs.values_mut() {
        if region_ctx.skip_wal() {
            continue;
        }
        if let Err(e) = region_ctx.add_wal_entry(&mut wal_writer).map_err(Arc::new) {
            region_ctx.set_error(e);
        }
    }
    match wal_writer.write_to_wal().await.map_err(Arc::new) {
        Ok(response) => {
            for (region_id, region_ctx) in region_ctxs.iter_mut() {
                if region_ctx.skip_wal() {
                    continue;
                }
                // The entry of a failed region (e.g. failed to build its WAL entry) is
                // not in the batch so the response has no last entry id for it. Its
                // waiters are already notified with the error.
                if region_ctx.is_failed() {
                    continue;
                }

                // Safety: the log store implementation ensures that either the `write_to_wal` fails and no
                // response is returned or the last entry ids for each region in the batch do exist.
                let last_entry_id = response.last_entry_ids.get(region_id).unwrap();
                region_ctx.set_next_entry_id(last_entry_id + 1);
            }
            true
        }
        Err(e) => {
            // Failed to write wal.
            for (_, mut region_ctx) in region_ctxs.drain() {
                region_ctx.set_error(e.clone());
            }
            false
        }
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

fn reject_region_write_requests(
    rejected_region_ids: &HashSet<RegionId>,
    write_requests: &mut Vec<SenderWriteRequest>,
    bulk_requests: &mut Vec<SenderBulkRequest>,
) {
    let mut rejected_write_requests = write_requests
        .extract_if(.., |req| {
            rejected_region_ids.contains(&req.request.region_id)
        })
        .collect::<Vec<_>>();
    let mut rejected_bulk_requests = bulk_requests
        .extract_if(.., |req| rejected_region_ids.contains(&req.region_id))
        .collect::<Vec<_>>();
    reject_write_requests(&mut rejected_write_requests, &mut rejected_bulk_requests);
}

fn write_region_ids(
    write_requests: &[SenderWriteRequest],
    bulk_requests: &[SenderBulkRequest],
) -> HashSet<RegionId> {
    write_requests
        .iter()
        .map(|req| req.request.region_id)
        .chain(bulk_requests.iter().map(|req| req.region_id))
        .collect()
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

fn check_partition_expr_version(
    region_id: RegionId,
    expected_version: u64,
    request_version: Option<u64>,
) -> Result<()> {
    let request_version = match request_version {
        None => return Ok(()),
        Some(value) => value,
    };
    if request_version != expected_version {
        return PartitionExprVersionMismatchSnafu {
            region_id,
            request_version,
            expected_version,
        }
        .fail();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use api::v1::helper::{tag_column_schema, time_index_column_schema};
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row, Rows};
    use common_recordbatch::DfRecordBatch;
    use datatypes::arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use futures::stream;
    use log_store::error::{
        Error as LogStoreError, IllegalStateSnafu, InvalidProviderSnafu, Result as LogStoreResult,
    };
    use store_api::logstore::entry::{Entry, NaiveEntry};
    use store_api::logstore::provider::Provider;
    use store_api::logstore::{AppendBatchResponse, EntryId, SendableEntryStream, WalIndex};
    use store_api::region_request::AffectedRows;
    use tokio::sync::oneshot;

    use super::*;
    use crate::memtable::bulk::part::BulkPart;
    use crate::request::OptionOutputTx;
    use crate::test_util::ts_ms_value;
    use crate::test_util::version_util::VersionControlBuilder;

    /// Creates a bulk part with `num_rows` rows. The schema carries the
    /// builder metadata's columns (`tag_0` primary key + `ts` time index) so
    /// the bulk-install conversion succeeds; `timestamp_index` points at the
    /// `ts` column.
    fn new_bulk_part(num_rows: i64) -> BulkPart {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_0", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let tag = Arc::new(StringArray::from_iter_values(
            (0..num_rows).map(|value| value.to_string()),
        )) as ArrayRef;
        let ts = Arc::new(TimestampMillisecondArray::from(
            (0..num_rows).collect::<Vec<_>>(),
        )) as ArrayRef;
        let batch = DfRecordBatch::try_new(schema, vec![tag, ts]).unwrap();

        BulkPart {
            batch,
            max_timestamp: num_rows - 1,
            min_timestamp: 0,
            sequence: 0,
            timestamp_index: 1,
            raw_data: None,
        }
    }

    /// A log store that fails to build entries for `failing_region` and fails the
    /// whole batch when `fail_append` is true.
    #[derive(Debug, Default)]
    struct MockLogStore {
        failing_region: Option<RegionId>,
        fail_append: bool,
    }

    #[async_trait::async_trait]
    impl LogStore for MockLogStore {
        type Error = LogStoreError;

        async fn stop(&self) -> LogStoreResult<()> {
            Ok(())
        }

        async fn append_batch(&self, entries: Vec<Entry>) -> LogStoreResult<AppendBatchResponse> {
            if self.fail_append {
                return IllegalStateSnafu {}.fail();
            }
            let mut last_entry_ids = HashMap::new();
            for entry in &entries {
                let last_entry_id = last_entry_ids.entry(entry.region_id()).or_insert(0);
                *last_entry_id = entry.entry_id().max(*last_entry_id);
            }
            Ok(AppendBatchResponse { last_entry_ids })
        }

        async fn read(
            &self,
            _provider: &Provider,
            _entry_id: EntryId,
            _index: Option<WalIndex>,
        ) -> LogStoreResult<SendableEntryStream<'static, Entry, Self::Error>> {
            Ok(Box::pin(stream::empty()))
        }

        async fn create_namespace(&self, _ns: &Provider) -> LogStoreResult<()> {
            Ok(())
        }

        async fn delete_namespace(&self, _ns: &Provider) -> LogStoreResult<()> {
            Ok(())
        }

        async fn list_namespaces(&self) -> LogStoreResult<Vec<Provider>> {
            Ok(vec![])
        }

        async fn obsolete(
            &self,
            _provider: &Provider,
            _region_id: RegionId,
            _entry_id: EntryId,
        ) -> LogStoreResult<()> {
            Ok(())
        }

        async fn obsolete_all(
            &self,
            _provider: &Provider,
            _region_id: RegionId,
        ) -> LogStoreResult<()> {
            Ok(())
        }

        fn entry(
            &self,
            data: Vec<u8>,
            entry_id: EntryId,
            region_id: RegionId,
            provider: &Provider,
        ) -> LogStoreResult<Entry> {
            if self.failing_region == Some(region_id) {
                return InvalidProviderSnafu {
                    expected: "raft_engine",
                    actual: "mock",
                }
                .fail();
            }
            Ok(Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id,
                entry_id,
                data,
            }))
        }

        fn latest_entry_id(&self, _provider: &Provider) -> LogStoreResult<EntryId> {
            Ok(0)
        }
    }

    /// Creates a write context for `region_id` with one pending mutation of one row.
    fn new_region_ctx(
        region_id: RegionId,
    ) -> (RegionWriteCtx, oneshot::Receiver<Result<AffectedRows>>) {
        let version_control = Arc::new(VersionControlBuilder::new().build());
        let mut ctx = RegionWriteCtx::new(
            region_id,
            &version_control,
            Provider::raft_engine_provider(region_id.as_u64()),
            None,
        );
        let (tx, rx) = oneshot::channel();
        ctx.push_mutation(
            OpType::Put as i32,
            Some(Rows {
                schema: vec![
                    time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
                    tag_column_schema("tag_0", ColumnDataType::String),
                ],
                rows: vec![Row {
                    values: vec![
                        ts_ms_value(0),
                        api::v1::Value {
                            value_data: Some(ValueData::StringValue("a".to_string())),
                        },
                    ],
                }],
            }),
            None,
            OptionOutputTx::from(tx),
            None,
        );
        (ctx, rx)
    }

    #[tokio::test]
    async fn test_write_wal_skips_region_failed_to_build_entry() {
        let failing_region = RegionId::new(1, 1);
        let ok_region = RegionId::new(1, 2);
        let wal = Wal::new(Arc::new(MockLogStore {
            failing_region: Some(failing_region),
            ..Default::default()
        }));

        let mut region_ctxs = HashMap::new();
        let (ctx, failing_rx) = new_region_ctx(failing_region);
        let failing_committed_sequence = ctx.version_control().committed_sequence();
        region_ctxs.insert(failing_region, ctx);
        let (ctx, ok_rx) = new_region_ctx(ok_region);
        let ok_committed_sequence = ctx.version_control().committed_sequence();
        region_ctxs.insert(ok_region, ctx);
        let entry_id = region_ctxs[&ok_region].next_entry_id();

        // The failed region must not fail the batch or panic the worker.
        assert!(write_wal(&wal, &mut region_ctxs).await);

        assert!(region_ctxs[&failing_region].is_failed());
        assert!(!region_ctxs[&ok_region].is_failed());
        assert_eq!(entry_id + 1, region_ctxs[&ok_region].next_entry_id());

        // Run the memtable and publication phase the worker runs after `write_wal`.
        // The failed region installed no rows, so its committed sequence must not
        // advance; the successful sibling region advances normally.
        for region_ctx in region_ctxs.values_mut() {
            region_ctx.write_memtable().await;
            region_ctx.write_bulk().await;
            region_ctx.publish_sequence_and_entry_id();
        }

        assert_eq!(
            failing_committed_sequence,
            region_ctxs[&failing_region]
                .version_control()
                .committed_sequence()
        );
        assert_eq!(
            ok_committed_sequence + 1,
            region_ctxs[&ok_region]
                .version_control()
                .committed_sequence()
        );

        // Waiters of the failed region get the error while others get the result.
        drop(region_ctxs);
        assert!(failing_rx.await.unwrap().is_err());
        assert_eq!(1, ok_rx.await.unwrap().unwrap());
    }

    // The committed sequence must not be published before the bulk part's rows
    // are physically installed in the memtable: publication happens strictly
    // after `write_bulk` returns. Armed with the bulk-install test barrier,
    // this deterministically pauses the worker's memtable phase between the
    // ordinary-memtable handling and the bulk installation and asserts the
    // region's committed sequence is still its initial value (0) — read via
    // the `version_control()` accessor, never through a scanner API. After the
    // barrier is released, the committed sequence must advance to cover the
    // bulk rows.
    #[tokio::test]
    async fn test_bulk_write_sequence_not_committed_before_install_worker_level() {
        let region_id = RegionId::new(1, 1);
        let version_control = Arc::new(VersionControlBuilder::new().build());
        assert_eq!(
            0,
            version_control.committed_sequence(),
            "the builder must start at sequence 0"
        );

        let mut region_ctxs = HashMap::new();
        let mut ctx = RegionWriteCtx::new(
            region_id,
            &version_control,
            Provider::raft_engine_provider(region_id.as_u64()),
            None,
        );
        let (tx, rx) = oneshot::channel();
        // Push a 3-row bulk part without an explicit sequence, exactly like the
        // worker's `process_bulk_requests`.
        assert!(ctx.push_bulk(OptionOutputTx::from(tx), new_bulk_part(3), None));
        region_ctxs.insert(region_id, ctx);

        // Run the WAL phase the worker runs before the memtable phase.
        let wal = Wal::new(Arc::new(MockLogStore::default()));
        assert!(write_wal(&wal, &mut region_ctxs).await);
        assert!(!region_ctxs[&region_id].is_failed());

        // Arm the bulk-install barrier: `write_bulk` pauses right before the
        // parts are physically installed into the memtable.
        let mut barrier = crate::region_write_ctx::test_hooks::arm_bulk_install_barrier(region_id);

        // Run the worker's memtable and publication phases in the background.
        let write_handle = tokio::spawn(async move {
            let mut region_ctx = region_ctxs.remove(&region_id).unwrap();
            region_ctx.write_memtable().await;
            region_ctx.write_bulk().await;
            region_ctx.publish_sequence_and_entry_id();
        });

        // Wait until the write paused at the barrier: deterministic, no sleeps.
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            barrier.wait_until_reached(),
        )
        .await
        .expect("bulk write never reached the install barrier");

        // The committed sequence must not have advanced yet: publication
        // happens strictly after the bulk part is installed.
        assert_eq!(
            0,
            version_control.committed_sequence(),
            "committed sequence leaked before the bulk part was installed"
        );

        // Release the barrier: the bulk part installs and the committed
        // sequence advances to cover all 3 bulk rows.
        barrier.release();
        write_handle.await.expect("bulk write should complete");
        assert_eq!(
            3,
            version_control.committed_sequence(),
            "committed sequence must cover the installed bulk rows"
        );

        // The bulk waiter is notified with the number of installed rows.
        assert_eq!(3, rx.await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn test_write_wal_all_regions_failed_to_build_entries() {
        let failing_region = RegionId::new(1, 1);
        let wal = Wal::new(Arc::new(MockLogStore {
            failing_region: Some(failing_region),
            ..Default::default()
        }));

        let mut region_ctxs = HashMap::new();
        let (ctx, rx) = new_region_ctx(failing_region);
        region_ctxs.insert(failing_region, ctx);

        // Writing an empty batch to the WAL succeeds, the failed region must not panic
        // the worker.
        assert!(write_wal(&wal, &mut region_ctxs).await);

        assert!(region_ctxs[&failing_region].is_failed());
        drop(region_ctxs);
        assert!(rx.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_write_wal_append_batch_failure() {
        let region_id = RegionId::new(1, 1);
        let wal = Wal::new(Arc::new(MockLogStore {
            fail_append: true,
            ..Default::default()
        }));

        let mut region_ctxs = HashMap::new();
        let (ctx, rx) = new_region_ctx(region_id);
        region_ctxs.insert(region_id, ctx);

        assert!(!write_wal(&wal, &mut region_ctxs).await);

        // All contexts are consumed and waiters are notified with the error.
        assert!(region_ctxs.is_empty());
        assert!(rx.await.unwrap().is_err());
    }
}
