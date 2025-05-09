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

use std::mem;
use std::sync::Arc;

use api::v1::{Mutation, OpType, Rows, WalEntry, WriteHint};
use futures::stream::{FuturesUnordered, StreamExt};
use snafu::ResultExt;
use store_api::logstore::provider::Provider;
use store_api::logstore::LogStore;
use store_api::storage::{RegionId, SequenceNumber};

use crate::error::{Error, Result, WriteGroupSnafu};
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::KeyValues;
use crate::metrics;
use crate::region::version::{VersionControlData, VersionControlRef, VersionRef};
use crate::request::OptionOutputTx;
use crate::wal::{EntryId, WalWriter};

/// Notifier to notify write result on drop.
struct WriteNotify {
    /// Error to send to the waiter.
    err: Option<Arc<Error>>,
    /// Sender to send write result to the waiter for this mutation.
    sender: OptionOutputTx,
    /// Number of rows to be written.
    num_rows: usize,
}

impl WriteNotify {
    /// Creates a new notify from the `sender`.
    fn new(sender: OptionOutputTx, num_rows: usize) -> WriteNotify {
        WriteNotify {
            err: None,
            sender,
            num_rows,
        }
    }

    /// Send result to the waiter.
    fn notify_result(&mut self) {
        if let Some(err) = &self.err {
            // Try to send the error to waiters.
            self.sender
                .send_mut(Err(err.clone()).context(WriteGroupSnafu));
        } else {
            // Send success result.
            self.sender.send_mut(Ok(self.num_rows));
        }
    }
}

impl Drop for WriteNotify {
    fn drop(&mut self) {
        self.notify_result();
    }
}

/// Context to keep region metadata and buffer write requests.
pub(crate) struct RegionWriteCtx {
    /// Id of region to write.
    region_id: RegionId,
    /// Version of the region while creating the context.
    version: VersionRef,
    /// VersionControl of the region.
    version_control: VersionControlRef,
    /// Next sequence number to write.
    ///
    /// The context assigns a unique sequence number for each row.
    next_sequence: SequenceNumber,
    /// Next entry id of WAL to write.
    next_entry_id: EntryId,
    /// Valid WAL entry to write.
    ///
    /// We keep [WalEntry] instead of mutations to avoid taking mutations
    /// out of the context to construct the wal entry when we write to the wal.
    wal_entry: WalEntry,
    /// Wal options of the region being written to.
    provider: Provider,
    /// Notifiers to send write results to waiters.
    ///
    /// The i-th notify is for i-th mutation.
    notifiers: Vec<WriteNotify>,
    /// Notifiers for bulk requests.
    bulk_notifiers: Vec<WriteNotify>,
    /// Pending bulk write requests
    pub(crate) bulk_parts: Vec<BulkPart>,
    /// The write operation is failed and we should not write to the mutable memtable.
    failed: bool,

    // Metrics:
    /// Rows to put.
    pub(crate) put_num: usize,
    /// Rows to delete.
    pub(crate) delete_num: usize,
}

impl RegionWriteCtx {
    /// Returns an empty context.
    pub(crate) fn new(
        region_id: RegionId,
        version_control: &VersionControlRef,
        provider: Provider,
    ) -> RegionWriteCtx {
        let VersionControlData {
            version,
            committed_sequence,
            last_entry_id,
            ..
        } = version_control.current();

        RegionWriteCtx {
            region_id,
            version,
            version_control: version_control.clone(),
            next_sequence: committed_sequence + 1,
            next_entry_id: last_entry_id + 1,
            wal_entry: WalEntry::default(),
            provider,
            notifiers: Vec::new(),
            bulk_notifiers: vec![],
            failed: false,
            put_num: 0,
            delete_num: 0,
            bulk_parts: vec![],
        }
    }

    /// Push mutation to the context.
    pub(crate) fn push_mutation(
        &mut self,
        op_type: i32,
        rows: Option<Rows>,
        write_hint: Option<WriteHint>,
        tx: OptionOutputTx,
    ) {
        let num_rows = rows.as_ref().map(|rows| rows.rows.len()).unwrap_or(0);
        self.wal_entry.mutations.push(Mutation {
            op_type,
            sequence: self.next_sequence,
            rows,
            write_hint,
        });

        let notify = WriteNotify::new(tx, num_rows);
        // Notifiers are 1:1 map to mutations.
        self.notifiers.push(notify);

        // Increase sequence number.
        self.next_sequence += num_rows as u64;

        // Update metrics.
        match OpType::try_from(op_type) {
            Ok(OpType::Delete) => self.delete_num += num_rows,
            Ok(OpType::Put) => self.put_num += num_rows,
            Err(_) => (),
        }
    }

    /// Encode and add WAL entry to the writer.
    pub(crate) fn add_wal_entry<S: LogStore>(
        &mut self,
        wal_writer: &mut WalWriter<S>,
    ) -> Result<()> {
        wal_writer.add_entry(
            self.region_id,
            self.next_entry_id,
            &self.wal_entry,
            &self.provider,
        )?;
        self.next_entry_id += 1;
        Ok(())
    }

    pub(crate) fn version(&self) -> &VersionRef {
        &self.version
    }

    /// Sets error and marks all write operations are failed.
    pub(crate) fn set_error(&mut self, err: Arc<Error>) {
        // Set error for all notifiers
        for notify in &mut self.notifiers {
            notify.err = Some(err.clone());
        }

        // Fail the whole write operation.
        self.failed = true;
    }

    /// Updates next entry id.
    pub(crate) fn set_next_entry_id(&mut self, next_entry_id: EntryId) {
        self.next_entry_id = next_entry_id
    }

    /// Consumes mutations and writes them into mutable memtable.
    pub(crate) async fn write_memtable(&mut self) {
        debug_assert_eq!(self.notifiers.len(), self.wal_entry.mutations.len());

        if self.failed {
            return;
        }

        let mutable = self.version.memtables.mutable.clone();
        let mutations = mem::take(&mut self.wal_entry.mutations)
            .into_iter()
            .enumerate()
            .filter_map(|(i, mutation)| {
                let kvs = KeyValues::new(&self.version.metadata, mutation)?;
                Some((i, kvs))
            })
            .collect::<Vec<_>>();

        if mutations.len() == 1 {
            if let Err(err) = mutable.write(&mutations[0].1) {
                self.notifiers[mutations[0].0].err = Some(Arc::new(err));
            }
        } else {
            let mut tasks = FuturesUnordered::new();
            for (i, kvs) in mutations {
                let mutable = mutable.clone();
                // use tokio runtime to schedule tasks.
                tasks.push(common_runtime::spawn_blocking_global(move || {
                    (i, mutable.write(&kvs))
                }));
            }

            while let Some(result) = tasks.next().await {
                // first unwrap the result from `spawn` above
                let (i, result) = result.unwrap();
                if let Err(err) = result {
                    self.notifiers[i].err = Some(Arc::new(err));
                }
            }
        }

        // Updates region sequence and entry id. Since we stores last sequence and entry id in region, we need
        // to decrease `next_sequence` and `next_entry_id` by 1.
        self.version_control
            .set_sequence_and_entry_id(self.next_sequence - 1, self.next_entry_id - 1);
    }

    pub(crate) fn push_bulk(&mut self, sender: OptionOutputTx, mut bulk: BulkPart) {
        self.bulk_notifiers
            .push(WriteNotify::new(sender, bulk.num_rows));
        bulk.sequence = self.next_sequence;
        self.next_sequence += bulk.num_rows as u64;
        self.bulk_parts.push(bulk);
    }

    pub(crate) async fn write_bulk(&mut self) {
        if self.failed || self.bulk_parts.is_empty() {
            return;
        }
        let _timer = metrics::REGION_WORKER_HANDLE_WRITE_ELAPSED
            .with_label_values(&["write_bulk"])
            .start_timer();

        if self.bulk_parts.len() == 1 {
            let part = self.bulk_parts.swap_remove(0);
            let num_rows = part.num_rows;
            if let Err(e) = self.version.memtables.mutable.write_bulk(part) {
                self.bulk_notifiers[0].err = Some(Arc::new(e));
            } else {
                self.put_num += num_rows;
            }
            return;
        }

        let mut tasks = FuturesUnordered::new();
        for (i, part) in self.bulk_parts.drain(..).enumerate() {
            let mutable = self.version.memtables.mutable.clone();
            tasks.push(common_runtime::spawn_blocking_global(move || {
                let num_rows = part.num_rows;
                (i, mutable.write_bulk(part), num_rows)
            }));
        }
        while let Some(result) = tasks.next().await {
            // first unwrap the result from `spawn` above
            let (i, result, num_rows) = result.unwrap();
            if let Err(err) = result {
                self.bulk_notifiers[i].err = Some(Arc::new(err));
            } else {
                self.put_num += num_rows;
            }
        }

        self.version_control
            .set_sequence_and_entry_id(self.next_sequence - 1, self.next_entry_id - 1);
    }
}
