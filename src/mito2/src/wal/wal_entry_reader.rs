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

use api::v1::WalEntry;
use futures::StreamExt;
use prost::Message;
use snafu::ResultExt;
use store_api::logstore::entry::RawEntry;
use store_api::logstore::namespace::LogStoreNamespace;
use store_api::storage::RegionId;

use crate::error::{DecodeWalSnafu, Result};
use crate::wal::raw_entry_reader::RawEntryReader;
use crate::wal::{EntryId, WalEntryStream};

pub(crate) fn decode_raw_entry(raw_entry: RawEntry) -> Result<(EntryId, WalEntry)> {
    let entry_id = raw_entry.entry_id;
    let wal_entry = WalEntry::decode(raw_entry.data.as_slice()).context(DecodeWalSnafu {
        region_id: raw_entry.region_id,
    })?;

    Ok((entry_id, wal_entry))
}

/// [WalEntryReader] provides the ability to read and decode entries from the underlying store.
pub(crate) trait WalEntryReader: Send + Sync {
    fn read(self, ns: &'_ LogStoreNamespace, start_id: EntryId) -> Result<WalEntryStream<'static>>;
}

/// A Reader reads the [RawEntry] from [RawEntryReader] and decodes [RawEntry] into [WalEntry].
pub struct LogStoreEntryReader<R> {
    reader: R,
}

impl<R> LogStoreEntryReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R: RawEntryReader> WalEntryReader for LogStoreEntryReader<R> {
    fn read(self, ns: &'_ LogStoreNamespace, start_id: EntryId) -> Result<WalEntryStream<'static>> {
        let LogStoreEntryReader { reader } = self;
        let mut stream = reader.read(ns, start_id)?;
        let stream = stream.map(|entry| decode_raw_entry(entry?));

        Ok(Box::pin(stream))
    }
}
