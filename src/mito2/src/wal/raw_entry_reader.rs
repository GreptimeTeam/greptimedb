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

use futures::stream::BoxStream;
use store_api::logstore::entry::RawEntry;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::wal::EntryId;

/// A stream that yields [RawEntry].
pub type RawEntryStream<'a> = BoxStream<'a, Result<RawEntry>>;

// The namespace of kafka log store
pub struct KafkaNamespace<'a> {
    topic: &'a str,
}

// The namespace of raft engine log store
pub struct RaftEngineNamespace {
    region_id: RegionId,
}

/// The namespace of [RawEntryReader].
pub(crate) enum LogStoreNamespace<'a> {
    RaftEngine(RaftEngineNamespace),
    Kafka(KafkaNamespace<'a>),
}

/// [RawEntryReader] provides the ability to read [RawEntry] from the underlying [LogStore].
pub(crate) trait RawEntryReader: Send + Sync {
    fn read(&self, ctx: LogStoreNamespace, start_id: EntryId) -> Result<RawEntryStream<'static>>;
}
