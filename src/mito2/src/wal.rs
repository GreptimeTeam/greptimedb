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

//! Write ahead log of the engine.

use std::mem;
use std::sync::Arc;

use common_error::ext::BoxedError;
use futures::stream::BoxStream;
use futures::{stream, StreamExt, TryStreamExt};
use greptime_proto::v1::mito::WalEntry;
use prost::Message;
use snafu::ResultExt;
use store_api::logstore::entry::Entry;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;

use crate::error::{DecodeWalSnafu, EncodeWalSnafu, ReadWalSnafu, Result, WriteWalSnafu};

/// WAL entry id.
pub type EntryId = store_api::logstore::entry::Id;
/// A stream that yields tuple of WAL entry id and corresponding entry.
pub type WalEntryStream<'a> = BoxStream<'a, Result<(EntryId, WalEntry)>>;

/// Write ahead log.
///
/// All regions in the engine shares the same WAL instance.
#[derive(Debug)]
pub struct Wal<S> {
    /// The underlying log store.
    store: Arc<S>,
}

impl<S> Wal<S> {
    /// Creates a new [Wal] from the log store.
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

impl<S: LogStore> Wal<S> {
    /// Returns a writer to write to the WAL.
    pub fn writer(&self) -> WalWriter<S> {
        WalWriter {
            store: self.store.clone(),
            entries: Vec::new(),
            entry_encode_buf: Vec::new(),
        }
    }

    /// Scan entries of specific region starting from `start_id` (inclusive).
    pub async fn scan_region(
        &self,
        region_id: RegionId,
        start_id: EntryId,
    ) -> Result<WalEntryStream> {
        let namespace = self.store.namespace(region_id.into());
        let stream = self
            .store
            .read(&namespace, start_id)
            .await
            .map_err(BoxedError::new)
            .context(ReadWalSnafu { region_id })?;

        let stream = stream
            .map(move |entries_ret| {
                // Maps results from the WAL stream to our results.
                entries_ret
                    .map_err(BoxedError::new)
                    .context(ReadWalSnafu { region_id })
            })
            .and_then(move |entries| async move {
                let iter = entries
                    .into_iter()
                    .map(move |entry| decode_entry(region_id, entry));

                Ok(stream::iter(iter))
            })
            .try_flatten();

        Ok(Box::pin(stream))
    }
}

/// Decode Wal entry from log store.
fn decode_entry<E: Entry>(region_id: RegionId, entry: E) -> Result<(EntryId, WalEntry)> {
    let entry_id = entry.id();
    let data = entry.data();

    let wal_entry = WalEntry::decode(data).context(DecodeWalSnafu { region_id })?;

    Ok((entry_id, wal_entry))
}

/// WAL batch writer.
pub struct WalWriter<S: LogStore> {
    /// Log store of the WAL.
    store: Arc<S>,
    /// Entries to write.
    entries: Vec<S::Entry>,
    /// Buffer to encode WAL entry.
    entry_encode_buf: Vec<u8>,
}

impl<S: LogStore> WalWriter<S> {
    /// Add an wal entry for specific region to the writer's buffer.
    pub fn add_entry(
        &mut self,
        region_id: RegionId,
        entry_id: EntryId,
        wal_entry: &WalEntry,
    ) -> Result<()> {
        let namespace = self.store.namespace(region_id.into());
        // Encode wal entry to log store entry.
        self.entry_encode_buf.clear();
        wal_entry
            .encode(&mut self.entry_encode_buf)
            .context(EncodeWalSnafu { region_id })?;
        let entry = self
            .store
            .entry(&self.entry_encode_buf, entry_id, namespace);

        self.entries.push(entry);

        Ok(())
    }

    /// Write all buffered entries to the WAL.
    pub async fn write_to_wal(&mut self) -> Result<()> {
        // TODO(yingwen): metrics.

        let entries = mem::take(&mut self.entries);
        self.store
            .append_batch(entries)
            .await
            .map_err(BoxedError::new)
            .context(WriteWalSnafu)
    }
}

// TODO(yingwen): Tests.
