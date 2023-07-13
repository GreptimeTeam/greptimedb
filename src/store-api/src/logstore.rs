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

use common_error::ext::ErrorExt;

use crate::logstore::entry::{Entry, Id};
use crate::logstore::entry_stream::SendableEntryStream;
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

    /// Stop components of logstore.
    async fn stop(&self) -> Result<(), Self::Error>;

    /// Append an `Entry` to WAL with given namespace and return append response containing
    /// the entry id.
    async fn append(&self, mut e: Self::Entry) -> Result<AppendResponse, Self::Error>;

    /// Append a batch of entries atomically and return the offset of first entry.
    async fn append_batch(
        &self,
        ns: &Self::Namespace,
        e: Vec<Self::Entry>,
    ) -> Result<Vec<Id>, Self::Error>;

    /// Create a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`.
    async fn read(
        &self,
        ns: &Self::Namespace,
        id: Id,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>, Self::Error>;

    /// Create a new `Namespace`.
    async fn create_namespace(&self, ns: &Self::Namespace) -> Result<(), Self::Error>;

    /// Delete an existing `Namespace` with given ref.
    async fn delete_namespace(&self, ns: &Self::Namespace) -> Result<(), Self::Error>;

    /// List all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error>;

    /// Create an entry of the associate Entry type
    fn entry<D: AsRef<[u8]>>(&self, data: D, id: Id, ns: Self::Namespace) -> Self::Entry;

    /// Create a namespace of the associate Namespace type
    // TODO(sunng87): confusion with `create_namespace`
    fn namespace(&self, id: namespace::Id) -> Self::Namespace;

    /// Mark all entry ids `<=id` of given `namespace` as obsolete so that logstore can safely delete
    /// the log files if all entries inside are obsolete. This method may not delete log
    /// files immediately.
    async fn obsolete(&self, namespace: Self::Namespace, id: Id) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct AppendResponse {
    pub entry_id: Id,
}
