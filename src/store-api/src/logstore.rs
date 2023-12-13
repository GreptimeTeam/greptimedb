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

use common_error::ext::ErrorExt;

use crate::logstore::entry::{Entry, Id as EntryId, Offset as EntryOffset};
use crate::logstore::entry_stream::SendableEntryStream;
use crate::logstore::namespace::{Id as NamespaceId, Namespace};

pub mod entry;
pub mod entry_stream;
pub mod namespace;

/// `LogStore` serves as a Write-Ahead-Log for storage engine.
#[async_trait::async_trait]
pub trait LogStore: Send + Sync + 'static + std::fmt::Debug {
    type Error: ErrorExt + Send + Sync + 'static;
    type Namespace: Namespace;
    type Entry: Entry;

    /// Stop components of logstore.
    async fn stop(&self) -> Result<(), Self::Error>;

    /// Append an `Entry` to WAL with given namespace and return append response containing
    /// the entry id.
    async fn append(&self, entry: Self::Entry) -> Result<AppendResponse, Self::Error>;

    /// Append a batch of entries and return an append batch response containing the start entry ids of
    /// log entries written to each region.
    async fn append_batch(
        &self,
        entries: Vec<Self::Entry>,
    ) -> Result<AppendBatchResponse, Self::Error>;

    /// Create a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`.
    async fn read(
        &self,
        ns: &Self::Namespace,
        id: EntryId,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>, Self::Error>;

    /// Create a new `Namespace`.
    async fn create_namespace(&self, ns: &Self::Namespace) -> Result<(), Self::Error>;

    /// Delete an existing `Namespace` with given ref.
    async fn delete_namespace(&self, ns: &Self::Namespace) -> Result<(), Self::Error>;

    /// List all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error>;

    /// Create an entry of the associate Entry type
    fn entry<D: AsRef<[u8]>>(&self, data: D, entry_id: EntryId, ns: Self::Namespace)
        -> Self::Entry;

    /// Create a namespace of the associate Namespace type
    // TODO(sunng87): confusion with `create_namespace`
    fn namespace(
        &self,
        ns_id: NamespaceId,
        wal_options: &HashMap<String, String>,
    ) -> Result<Self::Namespace, Self::Error>;

    /// Mark all entry ids `<=id` of given `namespace` as obsolete so that logstore can safely delete
    /// the log files if all entries inside are obsolete. This method may not delete log
    /// files immediately.
    async fn obsolete(&self, ns: Self::Namespace, entry_id: EntryId) -> Result<(), Self::Error>;
}

/// The response of an `append` operation.
#[derive(Debug)]
pub struct AppendResponse {
    /// The entry id of the appended log entry.
    pub entry_id: EntryId,
    /// The start entry offset of the appended log entry.
    /// Depends on the `LogStore` implementation, the entry offset may be missing.
    pub offset: Option<EntryOffset>,
}

/// The response of an `append_batch` operation.
#[derive(Debug, Default)]
pub struct AppendBatchResponse {
    /// Key: region id (as u64). Value: the known minimum start offset of appended log entries belonging to the region.
    /// Depends on the `LogStore` implementation, the entry offsets may be missing.
    pub offsets: HashMap<u64, EntryOffset>,
}
