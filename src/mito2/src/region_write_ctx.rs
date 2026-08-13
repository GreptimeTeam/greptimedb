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
use std::sync::atomic::{AtomicU64, Ordering};

use api::v1::{BulkWalEntry, Mutation, OpType, Rows, WalEntry, WriteHint};
use futures::stream::{FuturesUnordered, StreamExt};
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::storage::{RegionId, SequenceNumber};

use crate::error::{Error, Result, WriteGroupSnafu};
use crate::memtable::KeyValues;
use crate::memtable::bulk::part::BulkPart;
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
    /// The total bytes written to the region.
    pub(crate) written_bytes: Option<Arc<AtomicU64>>,
}

impl RegionWriteCtx {
    /// Returns an empty context.
    pub(crate) fn new(
        region_id: RegionId,
        version_control: &VersionControlRef,
        provider: Provider,
        written_bytes: Option<Arc<AtomicU64>>,
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
            written_bytes,
        }
    }

    /// Push mutation to the context.
    /// This method adopts the sequence number in parameters if present.
    pub(crate) fn push_mutation(
        &mut self,
        op_type: i32,
        rows: Option<Rows>,
        write_hint: Option<WriteHint>,
        tx: OptionOutputTx,
        sequence: Option<SequenceNumber>,
    ) {
        if let Some(sequence) = sequence {
            self.next_sequence = sequence;
        }
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

    /// Returns the version control of the region.
    #[cfg(test)]
    pub(crate) fn version_control(&self) -> &VersionControlRef {
        &self.version_control
    }

    /// Returns whether writes in this context should skip WAL.
    pub(crate) fn skip_wal(&self) -> bool {
        self.provider == Provider::Noop || self.version.options.skip_wal
    }

    /// Sets error and marks all write operations are failed.
    pub(crate) fn set_error(&mut self, err: Arc<Error>) {
        // Set error for all notifiers.
        for notify in &mut self.notifiers {
            notify.err = Some(err.clone());
        }
        for notify in &mut self.bulk_notifiers {
            notify.err = Some(err.clone());
        }

        // Fail the whole write operation.
        self.failed = true;
    }

    /// Returns whether the write operation is already marked as failed.
    pub(crate) fn is_failed(&self) -> bool {
        self.failed
    }

    /// Updates next entry id.
    pub(crate) fn set_next_entry_id(&mut self, next_entry_id: EntryId) {
        self.next_entry_id = next_entry_id
    }

    /// Returns the next entry id to write.
    #[cfg(test)]
    pub(crate) fn next_entry_id(&self) -> EntryId {
        self.next_entry_id
    }

    /// Consumes mutations and writes them into mutable memtable.
    pub(crate) async fn write_memtable(&mut self) {
        debug_assert_eq!(self.notifiers.len(), self.wal_entry.mutations.len());

        if self.failed {
            return;
        }

        let mutable_memtable = self.version.memtables.mutable.clone();
        let prev_memory_usage = if self.written_bytes.is_some() {
            Some(mutable_memtable.memory_usage())
        } else {
            None
        };

        let mutations = mem::take(&mut self.wal_entry.mutations)
            .into_iter()
            .enumerate()
            .filter_map(|(i, mutation)| {
                let kvs = KeyValues::new(&self.version.metadata, mutation)?;
                Some((i, kvs))
            })
            .collect::<Vec<_>>();

        if mutations.len() == 1 {
            if let Err(err) = mutable_memtable.write(&mutations[0].1) {
                self.notifiers[mutations[0].0].err = Some(Arc::new(err));
            }
        } else {
            let mut tasks = FuturesUnordered::new();
            for (i, kvs) in mutations {
                let mutable = mutable_memtable.clone();
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

        if let Some(written_bytes) = &self.written_bytes {
            let new_memory_usage = mutable_memtable.memory_usage();
            let bytes = new_memory_usage.saturating_sub(prev_memory_usage.unwrap_or_default());
            written_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn push_bulk(
        &mut self,
        sender: OptionOutputTx,
        mut bulk: BulkPart,
        sequence: Option<SequenceNumber>,
    ) -> bool {
        if let Some(sequence) = sequence {
            self.next_sequence = sequence;
        }
        bulk.sequence = self.next_sequence;
        let entry = match BulkWalEntry::try_from(&bulk) {
            Ok(entry) => entry,
            Err(e) => {
                sender.send(Err(e));
                return false;
            }
        };

        self.bulk_notifiers
            .push(WriteNotify::new(sender, bulk.num_rows()));

        // Add bulk wal entry
        self.wal_entry.bulk_entries.push(entry);
        self.next_sequence += bulk.num_rows() as u64;
        self.bulk_parts.push(bulk);
        true
    }

    pub(crate) async fn write_bulk(&mut self) {
        if self.failed || self.bulk_parts.is_empty() {
            return;
        }
        #[cfg(test)]
        test_hooks::pause_before_bulk_install(self.region_id).await;
        let _timer = metrics::REGION_WORKER_HANDLE_WRITE_ELAPSED
            .with_label_values(&["write_bulk"])
            .start_timer();

        let mutable_memtable = &self.version.memtables.mutable;
        let prev_memory_usage = if self.written_bytes.is_some() {
            Some(mutable_memtable.memory_usage())
        } else {
            None
        };

        if self.bulk_parts.len() == 1 {
            let part = self.bulk_parts.swap_remove(0);
            let num_rows = part.num_rows();
            if let Err(e) = self.version.memtables.mutable.write_bulk(part) {
                self.bulk_notifiers[0].err = Some(Arc::new(e));
            } else {
                self.put_num += num_rows;
            }
            return;
        }

        let mut tasks = FuturesUnordered::new();
        for (i, part) in self.bulk_parts.drain(..).enumerate() {
            let mutable = mutable_memtable.clone();
            tasks.push(common_runtime::spawn_blocking_global(move || {
                let num_rows = part.num_rows();
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

        if let Some(written_bytes) = &self.written_bytes {
            let new_memory_usage = mutable_memtable.memory_usage();
            let bytes = new_memory_usage.saturating_sub(prev_memory_usage.unwrap_or_default());
            written_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    /// Publishes the sequences and entry id assigned by this context to the
    /// region's committed watermark.
    ///
    /// Must be called after both [`write_memtable`](Self::write_memtable) and
    /// [`write_bulk`](Self::write_bulk) have completed, so the committed
    /// sequence never covers rows that are not yet physically installed in the
    /// memtable (bulk part sequences are assigned in [`push_bulk`](Self::push_bulk)
    /// before installation). Since we store the last sequence and entry id in
    /// the region, we decrease `next_sequence` and `next_entry_id` by 1.
    ///
    /// If the write operation failed (e.g. the WAL entry could not be built),
    /// no rows were installed and nothing must be published: advancing the
    /// committed watermark here would make it cover rows that were never
    /// written.
    pub(crate) fn publish_sequence_and_entry_id(&self) {
        if self.failed {
            return;
        }
        self.version_control
            .set_sequence_and_entry_id(self.next_sequence - 1, self.next_entry_id - 1);
    }
}

/// Test-only hooks to make write ordering races deterministic.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use store_api::storage::RegionId;
    use tokio::sync::watch;

    /// Channels of an armed bulk-install barrier.
    ///
    /// `reached` signals that a bulk write paused between ordinary-memtable
    /// handling and bulk installation; `release` unblocks it. Dropping the
    /// senders (by disarming the barrier) also unblocks paused writes because
    /// their `wait_for` fails on a closed channel.
    struct ActiveBarrier {
        id: u64,
        /// Only bulk writes for this region pause at the barrier; writes for
        /// other regions pass through untouched, so concurrently running tests
        /// can never satisfy (or be blocked by) each other's barrier.
        target_region_id: RegionId,
        reached: watch::Sender<bool>,
        release: watch::Sender<bool>,
    }

    /// The currently armed barrier, if any. Wrapped in a `Mutex` so a test can
    /// arm a fresh barrier after a previous one was released (or dropped), and
    /// so a barrier can never leak past the test that owns it.
    static ACTIVE_BARRIER: Mutex<Option<ActiveBarrier>> = Mutex::new(None);
    static NEXT_BARRIER_ID: AtomicU64 = AtomicU64::new(1);

    fn lock_active_barrier() -> std::sync::MutexGuard<'static, Option<ActiveBarrier>> {
        // Never let a poisoned mutex (e.g. a panic in another test while
        // holding the lock) hang or break unrelated tests.
        ACTIVE_BARRIER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// RAII guard for the bulk-install barrier.
    ///
    /// The guard owns the armed barrier. Releasing it — or dropping the guard,
    /// e.g. when a test panics — unblocks any write paused at the barrier and
    /// disarms it, so a paused write can never hang and later tests can arm a
    /// fresh barrier.
    pub(crate) struct BulkInstallBarrier {
        id: u64,
        reached_rx: watch::Receiver<bool>,
        release_tx: watch::Sender<bool>,
        released: bool,
    }

    impl BulkInstallBarrier {
        /// Waits until a bulk write paused at the barrier.
        pub(crate) async fn wait_until_reached(&mut self) {
            if !*self.reached_rx.borrow() {
                let _ = self.reached_rx.wait_for(|reached| *reached).await;
            }
        }

        /// Unblocks the paused write and disarms the barrier.
        pub(crate) fn release(&mut self) {
            if self.released {
                return;
            }
            self.released = true;
            // Flip the release value so writes paused on this barrier proceed.
            let _ = self.release_tx.send(true);
            disarm_barrier(self.id);
        }
    }

    impl Drop for BulkInstallBarrier {
        fn drop(&mut self) {
            self.release();
        }
    }

    /// Arms the bulk-install barrier for `target_region_id` and returns a
    /// guard that owns it.
    ///
    /// Any previously armed barrier is replaced: its senders are dropped, which
    /// unblocks writes that were paused on it instead of letting them hang.
    pub(crate) fn arm_bulk_install_barrier(target_region_id: RegionId) -> BulkInstallBarrier {
        let (reached_tx, reached_rx) = watch::channel(false);
        let (release_tx, _release_rx) = watch::channel(false);
        let id = NEXT_BARRIER_ID.fetch_add(1, Ordering::Relaxed);
        let mut active = lock_active_barrier();
        *active = Some(ActiveBarrier {
            id,
            target_region_id,
            reached: reached_tx,
            release: release_tx.clone(),
        });
        BulkInstallBarrier {
            id,
            reached_rx,
            release_tx,
            released: false,
        }
    }

    /// Removes the barrier with `id` from the statics if it is still active.
    fn disarm_barrier(id: u64) {
        let mut active = lock_active_barrier();
        if active.as_ref().is_some_and(|barrier| barrier.id == id) {
            *active = None;
        }
    }

    /// Pauses a bulk write for `region_id` before installing its parts until
    /// the test releases the barrier (or it is disarmed). Bulk writes for
    /// other regions return immediately.
    pub(crate) async fn pause_before_bulk_install(region_id: RegionId) {
        let (reached_tx, release_rx) = {
            let active = lock_active_barrier();
            match active.as_ref() {
                Some(barrier) if barrier.target_region_id == region_id => {
                    (barrier.reached.clone(), barrier.release.subscribe())
                }
                _ => return,
            }
        };
        // Signal that a bulk write reached the pause point.
        let _ = reached_tx.send(true);
        let mut release_rx = release_rx;
        if !*release_rx.borrow() {
            // The sender is dropped when the barrier is disarmed, which makes
            // `wait_for` return an error instead of hanging forever.
            let _ = release_rx.wait_for(|released| *released).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_recordbatch::DfRecordBatch;
    use datatypes::arrow::array::{ArrayRef, TimestampMillisecondArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use store_api::logstore::provider::Provider;
    use tokio::sync::oneshot;

    use super::*;
    use crate::error::UnexpectedSnafu;
    use crate::memtable::bulk::part::BulkPart;
    use crate::test_util::version_util::VersionControlBuilder;

    #[test]
    fn test_set_error_marks_bulk_notifiers_failed() {
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let mut ctx =
            RegionWriteCtx::new(region_id, &version_control, Provider::noop_provider(), None);
        let (tx, rx) = oneshot::channel();

        assert!(ctx.push_bulk(OptionOutputTx::from(tx), new_bulk_part(), None));
        ctx.set_error(Arc::new(
            UnexpectedSnafu {
                reason: "wal failed".to_string(),
            }
            .build(),
        ));
        drop(ctx);

        let result = rx.blocking_recv().unwrap();
        assert!(result.is_err(), "bulk notifier should report WAL error");
    }

    fn new_bulk_part() -> BulkPart {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        )]));
        let arrays = vec![Arc::new(TimestampMillisecondArray::from(vec![1, 2])) as ArrayRef];
        let batch = DfRecordBatch::try_new(schema, arrays).unwrap();

        BulkPart {
            batch,
            max_timestamp: 2,
            min_timestamp: 1,
            sequence: 0,
            timestamp_index: 0,
            raw_data: None,
        }
    }
}
