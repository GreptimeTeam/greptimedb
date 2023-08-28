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

use std::sync::Arc;

use api::v1::{Mutation, Rows, WalEntry};
use common_query::Output;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::storage::{RegionId, SequenceNumber};
use tokio::sync::oneshot::Sender;

use crate::error::{Error, Result, WriteGroupSnafu};
use crate::memtable::KeyValues;
use crate::region::version::{VersionControlData, VersionControlRef, VersionRef};
use crate::wal::{EntryId, WalWriter};

/// Context to keep region metadata and buffer write requests.
pub(crate) struct RegionWriteCtx {
    /// Id of region to write.
    region_id: RegionId,
    /// Version of the region while creating the context.
    version: VersionRef,
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
    /// Notifiers to send write results to waiters.
    ///
    /// The i-th notify is for i-th mutation.
    notifiers: Vec<WriteNotify>,
}

impl RegionWriteCtx {
    /// Returns an empty context.
    pub(crate) fn new(region_id: RegionId, version_control: &VersionControlRef) -> RegionWriteCtx {
        let VersionControlData {
            version,
            committed_sequence,
            last_entry_id,
        } = version_control.current();

        RegionWriteCtx {
            region_id,
            version,
            next_sequence: committed_sequence + 1,
            next_entry_id: last_entry_id + 1,
            wal_entry: WalEntry::default(),
            notifiers: Vec::new(),
        }
    }

    /// Push [SenderWriteRequest] to the context.
    pub(crate) fn push_mutation(
        &mut self,
        op_type: i32,
        rows: Option<Rows>,
        tx: Option<Sender<Result<Output>>>,
    ) {
        let num_rows = rows.as_ref().map(|rows| rows.rows.len()).unwrap_or(0);
        self.wal_entry.mutations.push(Mutation {
            op_type,
            sequence: self.next_sequence,
            rows,
        });

        let notify = WriteNotify::new(tx, num_rows);
        // Notifiers are 1:1 map to mutations.
        self.notifiers.push(notify);

        // Increase sequence number.
        self.next_sequence += num_rows as u64;
    }

    /// Encode and add WAL entry to the writer.
    pub(crate) fn add_wal_entry<S: LogStore>(&self, wal_writer: &mut WalWriter<S>) -> Result<()> {
        wal_writer.add_entry(self.region_id, self.next_entry_id, &self.wal_entry)
    }

    pub(crate) fn version(&self) -> VersionRef {
        self.version.clone()
    }

    /// Sets error and marks all write operations are failed.
    pub(crate) fn set_error(&mut self, err: Arc<Error>) {
        // Set error for all notifiers
        for notify in &mut self.notifiers {
            notify.err = Some(err.clone());
        }
    }

    /// Consumes mutations and writes them into mutable memtable.
    pub(crate) fn write_memtable(&mut self) {
        debug_assert_eq!(self.notifiers.len(), self.wal_entry.mutations.len());

        let mutable = self.version.memtables.mutable();
        // Takes mutations from the wal entry.
        let mutations = std::mem::take(&mut self.wal_entry.mutations);
        for (mutation, notify) in mutations.into_iter().zip(&mut self.notifiers) {
            // Write mutation to the memtable.
            let Some(kvs) = KeyValues::new(&self.version.metadata, mutation) else {
                continue;
            };
            if let Err(e) = mutable.write(&kvs) {
                notify.err = Some(Arc::new(e));
            }
        }
    }
}

/// Notifier to notify write result on drop.
pub(crate) struct WriteNotify {
    /// Error to send to the waiter.
    err: Option<Arc<Error>>,
    /// Sender to send write result to the waiter for this mutation.
    sender: Option<Sender<Result<Output>>>,
    /// Number of rows to be written.
    num_rows: usize,
}

impl WriteNotify {
    /// Creates a new notify from the `sender`.
    pub(crate) fn new(sender: Option<Sender<Result<Output>>>, num_rows: usize) -> WriteNotify {
        WriteNotify {
            err: None,
            sender,
            num_rows,
        }
    }

    /// Send result to the waiter.
    fn notify_result(&mut self) {
        let Some(sender) = self.sender.take() else {
            return;
        };
        if let Some(err) = &self.err {
            // Try to send the error to waiters.
            let _ = sender.send(Err(err.clone()).context(WriteGroupSnafu));
        } else {
            // Send success result.
            let _ = sender.send(Ok(Output::AffectedRows(self.num_rows)));
        }
    }
}

impl Drop for WriteNotify {
    fn drop(&mut self) {
        self.notify_result();
    }
}
