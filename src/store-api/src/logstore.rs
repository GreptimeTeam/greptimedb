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

//! LogStore APIs.

use std::collections::HashMap;

use common_config::wal::WalOptions;
use common_error::ext::ErrorExt;

use crate::logstore::entry::Entry;
pub use crate::logstore::entry::Id as EntryId;
use crate::logstore::entry_stream::SendableEntryStream;
pub use crate::logstore::namespace::Id as NamespaceId;
use crate::logstore::namespace::Namespace;

pub mod entry;
pub mod entry_stream;
pub mod namespace;

/// `LogStore` serves as a Write-Ahead-Log for storage engine.
#[async_trait::async_trait]
pub trait LogStore: Send + Sync + 'static + std::fmt::Debug {
    type Error: ErrorExt + Send + Sync + 'static;
    type Namespace: Namespace;
    type Entry: Entry;

    /// Stops components of the logstore.
    async fn stop(&self) -> Result<(), Self::Error>;

    /// Appends an entry to the log store and returns a response containing the id of the append entry.
    async fn append(&self, entry: Self::Entry) -> Result<AppendResponse, Self::Error>;

    /// Appends a batch of entries and returns a response containing a map where the key is a region id
    /// while the value is the id of the last successfully written entry of the region.
    async fn append_batch(
        &self,
        entries: Vec<Self::Entry>,
    ) -> Result<AppendBatchResponse, Self::Error>;

    /// Creates a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`.
    async fn read(
        &self,
        ns: &Self::Namespace,
        id: EntryId,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>, Self::Error>;

    /// Creates a new `Namespace` from the given ref.
    async fn create_namespace(&self, ns: &Self::Namespace) -> Result<(), Self::Error>;

    /// Deletes an existing `Namespace` specified by the given ref.
    async fn delete_namespace(&self, ns: &Self::Namespace) -> Result<(), Self::Error>;

    /// Lists all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error>;

    /// Creates an entry of the associated Entry type
    fn entry<D: AsRef<[u8]>>(&self, data: D, entry_id: EntryId, ns: Self::Namespace)
        -> Self::Entry;

    /// Creates a namespace of the associated Namespace type
    // TODO(sunng87): confusion with `create_namespace`
    fn namespace(&self, ns_id: NamespaceId, wal_options: &WalOptions) -> Self::Namespace;

    /// Marks all entries with ids `<=entry_id` of the given `namespace` as obsolete,
    /// so that the log store can safely delete those entries. This method does not guarantee
    /// that the obsolete entries are deleted immediately.
    async fn obsolete(&self, ns: Self::Namespace, entry_id: EntryId) -> Result<(), Self::Error>;
}

/// The response of an `append` operation.
#[derive(Debug, Default)]
pub struct AppendResponse {
    /// The id of the entry appended to the log store.
    pub last_entry_id: EntryId,
}

/// The response of an `append_batch` operation.
#[derive(Debug, Default)]
pub struct AppendBatchResponse {
    /// Key: region id (as u64). Value: the id of the last successfully written entry of the region.
    pub last_entry_ids: HashMap<u64, EntryId>,
}
