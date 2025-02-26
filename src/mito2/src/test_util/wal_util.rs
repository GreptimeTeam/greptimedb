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
use futures::stream;
use prost::Message;
use store_api::logstore::entry::{Entry, MultiplePartEntry, MultiplePartHeader};
use store_api::logstore::provider::Provider;
use store_api::logstore::EntryId;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::wal::raw_entry_reader::{EntryStream, RawEntryReader};

pub(crate) struct MockRawEntryStream {
    pub(crate) entries: Vec<Entry>,
}

impl RawEntryReader for MockRawEntryStream {
    fn read(&self, _ns: &Provider, _start_id: EntryId) -> Result<EntryStream<'static>> {
        Ok(Box::pin(stream::iter(
            self.entries.clone().into_iter().map(Ok),
        )))
    }
}

/// Puts an incomplete [`Entry`] at the end of `input`.
pub(crate) fn generate_tail_corrupted_stream(
    provider: Provider,
    region_id: RegionId,
    input: &WalEntry,
    num_parts: usize,
) -> Vec<Entry> {
    let encoded_entry = input.encode_to_vec();
    let parts = encoded_entry
        .chunks(encoded_entry.len() / num_parts)
        .map(Into::into)
        .collect::<Vec<_>>();

    vec![
        Entry::MultiplePart(MultiplePartEntry {
            provider: provider.clone(),
            region_id,
            entry_id: 0,
            headers: vec![MultiplePartHeader::First, MultiplePartHeader::Last],
            parts,
        }),
        // The tail corrupted data.
        Entry::MultiplePart(MultiplePartEntry {
            provider: provider.clone(),
            region_id,
            entry_id: 0,
            headers: vec![MultiplePartHeader::First],
            parts: vec![vec![1; 100]],
        }),
    ]
}
