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

use std::collections::HashMap;

use common_config::wal::{KafkaConfig, WalOptions};
use store_api::logstore::entry::Id as EntryId;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::Id as NamespaceId;
use store_api::logstore::{AppendBatchResponse, AppendResponse, LogStore};

use crate::error::{Error, Result};
use crate::kafka::{EntryImpl, NamespaceImpl};

#[derive(Debug)]
pub struct KafkaLogStore;

impl KafkaLogStore {
    pub async fn try_new(config: KafkaConfig) -> Result<Self> {
        todo!()
    }
}

#[async_trait::async_trait]
impl LogStore for KafkaLogStore {
    type Error = Error;
    type Entry = EntryImpl;
    type Namespace = NamespaceImpl;

    /// Create an entry of the associate Entry type.
    fn entry<D: AsRef<[u8]>>(
        &self,
        data: D,
        entry_id: EntryId,
        ns: Self::Namespace,
    ) -> Self::Entry {
        EntryImpl::new(data.as_ref().to_vec(), entry_id, ns)
    }

    /// Append an `Entry` to WAL with given namespace and return append response containing
    /// the entry id.
    async fn append(&self, entry: Self::Entry) -> Result<AppendResponse> {
        todo!()
    }

    /// For a batch of log entries belonging to multiple regions, each assigned to a specific topic,
    /// we need to determine the minimum log offset returned for each region in this batch.
    /// During replay, we use this offset to fetch log entries for a region from its assigned topic.
    /// After fetching, we filter the entries to obtain log entries relevant to that specific region.
    async fn append_batch(&self, entries: Vec<Self::Entry>) -> Result<AppendBatchResponse> {
        todo!()
    }

    /// Create a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`. The generated entries will be filtered by the namespace.
    async fn read(
        &self,
        ns: &Self::Namespace,
        entry_id: EntryId,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>> {
        todo!()
    }

    /// Create a namespace of the associate Namespace type
    fn namespace(&self, ns_id: NamespaceId, wal_options: &WalOptions) -> Self::Namespace {
        todo!()
    }

    /// Create a new `Namespace`.
    async fn create_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    /// Delete an existing `Namespace` with given ref.
    async fn delete_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    /// List all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>> {
        Ok(vec![])
    }

    /// Mark all entry ids `<=id` of given `namespace` as obsolete so that logstore can safely delete
    /// the log files if all entries inside are obsolete. This method may not delete log
    /// files immediately.
    async fn obsolete(&self, _ns: Self::Namespace, _entry_id: EntryId) -> Result<()> {
        Ok(())
    }

    /// Stop components of logstore.
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}
