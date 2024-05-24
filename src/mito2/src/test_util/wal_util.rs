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

use async_stream::stream;
use futures::stream;
use store_api::logstore::entry::Entry;
use store_api::logstore::provider::Provider;
use store_api::logstore::EntryId;

use crate::error::Result;
use crate::wal::raw_entry_reader::{EntryStream, RawEntryReader};

pub(crate) struct MockRawEntryStream {
    pub(crate) entries: Vec<Entry>,
}

impl RawEntryReader for MockRawEntryStream {
    fn read(&self, ns: &Provider, start_id: EntryId) -> Result<EntryStream<'static>> {
        let entries = self.entries.clone().into_iter().map(Ok);

        Ok(Box::pin(stream::iter(entries)))
    }
}
