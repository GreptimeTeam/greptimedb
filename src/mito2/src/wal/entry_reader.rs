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
use async_stream::stream;
use futures::StreamExt;
use object_store::Buffer;
use prost::Message;
use snafu::{ensure, ResultExt};
use store_api::logstore::entry::Entry;
use store_api::logstore::provider::Provider;

use crate::error::{CorruptedEntrySnafu, DecodeWalSnafu, Result};
use crate::wal::raw_entry_reader::RawEntryReader;
use crate::wal::{EntryId, WalEntryStream};

pub(crate) fn decode_raw_entry(raw_entry: Entry) -> Result<(EntryId, WalEntry)> {
    let entry_id = raw_entry.entry_id();
    let region_id = raw_entry.region_id();
    ensure!(raw_entry.is_complete(), CorruptedEntrySnafu { region_id });
    let buffer = into_buffer(raw_entry);
    let wal_entry = WalEntry::decode(buffer).context(DecodeWalSnafu { region_id })?;
    Ok((entry_id, wal_entry))
}

fn into_buffer(raw_entry: Entry) -> Buffer {
    match raw_entry {
        Entry::Naive(entry) => Buffer::from(entry.data),
        Entry::MultiplePart(entry) => {
            Buffer::from_iter(entry.parts.into_iter().map(bytes::Bytes::from))
        }
    }
}

/// [WalEntryReader] provides the ability to read and decode entries from the underlying store.
///
/// Notes: It will consume the inner stream and only allow invoking the `read` at once.
pub(crate) trait WalEntryReader: Send + Sync {
    fn read(&mut self, ns: &'_ Provider, start_id: EntryId) -> Result<WalEntryStream<'static>>;
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
    fn read(&mut self, ns: &'_ Provider, start_id: EntryId) -> Result<WalEntryStream<'static>> {
        let LogStoreEntryReader { reader } = self;
        let mut stream = reader.read(ns, start_id)?;

        let stream = stream! {
            let mut buffered_entry = None;
            while let Some(next_entry) = stream.next().await {
                match buffered_entry.take() {
                    Some(entry) => {
                        yield decode_raw_entry(entry);
                        buffered_entry = Some(next_entry?);
                    },
                    None => {
                        buffered_entry = Some(next_entry?);
                    }
                };
            }
            if let Some(entry) = buffered_entry {
                // Ignores tail corrupted data.
                if entry.is_complete() {
                    yield decode_raw_entry(entry);
                }
            }
        };

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use api::v1::{Mutation, OpType, WalEntry};
    use futures::TryStreamExt;
    use prost::Message;
    use store_api::logstore::entry::{Entry, MultiplePartEntry, MultiplePartHeader};
    use store_api::logstore::provider::Provider;
    use store_api::storage::RegionId;

    use crate::error;
    use crate::test_util::wal_util::MockRawEntryStream;
    use crate::wal::entry_reader::{LogStoreEntryReader, WalEntryReader};

    #[tokio::test]
    async fn test_tail_corrupted_stream() {
        common_telemetry::init_default_ut_logging();
        let provider = Provider::kafka_provider("my_topic".to_string());
        let wal_entry = WalEntry {
            mutations: vec![Mutation {
                op_type: OpType::Put as i32,
                sequence: 1u64,
                rows: None,
                write_hint: 0,
            }],
        };
        let encoded_entry = wal_entry.encode_to_vec();
        let parts = encoded_entry
            .chunks(encoded_entry.len() / 2)
            .map(Into::into)
            .collect::<Vec<_>>();
        let raw_entry_stream = MockRawEntryStream {
            entries: vec![
                Entry::MultiplePart(MultiplePartEntry {
                    provider: provider.clone(),
                    region_id: RegionId::new(1, 1),
                    entry_id: 2,
                    headers: vec![MultiplePartHeader::First, MultiplePartHeader::Last],
                    parts,
                }),
                // The tail corrupted data.
                Entry::MultiplePart(MultiplePartEntry {
                    provider: provider.clone(),
                    region_id: RegionId::new(1, 1),
                    entry_id: 1,
                    headers: vec![MultiplePartHeader::Last],
                    parts: vec![vec![1; 100]],
                }),
            ],
        };

        let mut reader = LogStoreEntryReader::new(raw_entry_stream);
        let entries = reader
            .read(&provider, 0)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|(_, entry)| entry)
            .collect::<Vec<_>>();

        assert_eq!(entries, vec![wal_entry]);
    }

    #[tokio::test]
    async fn test_corrupted_stream() {
        let provider = Provider::kafka_provider("my_topic".to_string());
        let raw_entry_stream = MockRawEntryStream {
            entries: vec![
                Entry::MultiplePart(MultiplePartEntry {
                    provider: provider.clone(),
                    region_id: RegionId::new(1, 1),
                    entry_id: 1,
                    headers: vec![MultiplePartHeader::Last],
                    parts: vec![vec![1; 100]],
                }),
                Entry::MultiplePart(MultiplePartEntry {
                    provider: provider.clone(),
                    region_id: RegionId::new(1, 1),
                    entry_id: 2,
                    headers: vec![MultiplePartHeader::First],
                    parts: vec![vec![1; 100]],
                }),
            ],
        };

        let mut reader = LogStoreEntryReader::new(raw_entry_stream);
        let err = reader
            .read(&provider, 0)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::CorruptedEntry { .. });
    }
}
