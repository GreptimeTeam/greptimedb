// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! LogStore APIs.

use common_error::prelude::ErrorExt;

use crate::logstore::entry::{Entry, Id, Offset};
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
    type AppendResponse: AppendResponse;

    async fn start(&self) -> Result<(), Self::Error>;

    async fn stop(&self) -> Result<(), Self::Error>;

    /// Append an `Entry` to WAL with given namespace
    async fn append(&self, mut e: Self::Entry) -> Result<Self::AppendResponse, Self::Error>;

    /// Append a batch of entries atomically and return the offset of first entry.
    async fn append_batch(
        &self,
        ns: &Self::Namespace,
        e: Vec<Self::Entry>,
    ) -> Result<Id, Self::Error>;

    /// Create a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`.
    async fn read(
        &self,
        ns: &Self::Namespace,
        id: Id,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>, Self::Error>;

    /// Create a new `Namespace`.
    async fn create_namespace(&mut self, ns: &Self::Namespace) -> Result<(), Self::Error>;

    /// Delete an existing `Namespace` with given ref.
    async fn delete_namespace(&mut self, ns: &Self::Namespace) -> Result<(), Self::Error>;

    /// List all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error>;

    /// Create an entry of the associate Entry type
    fn entry<D: AsRef<[u8]>>(&self, data: D, id: Id, ns: Self::Namespace) -> Self::Entry;

    /// Create a namespace of the associate Namespace type
    // TODO(sunng87): confusion with `create_namespace`
    fn namespace(&self, id: namespace::Id) -> Self::Namespace;

    /// Mark the entry id of given namespace as stable so that logstore can safely delete
    /// the log files if all entries inside are stable. This method would not delete log
    /// files immediately.
    async fn mark_stable(&self, namespace: Self::Namespace, id: Id) -> Result<(), Self::Error>;
}

pub trait AppendResponse: Send + Sync {
    fn entry_id(&self) -> Id;

    fn offset(&self) -> Offset;
}
