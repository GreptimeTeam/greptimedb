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

    /// Append an `Entry` to WAL with given namespace
    async fn append(
        &self,
        ns: &Self::Namespace,
        mut e: Self::Entry,
    ) -> Result<Self::AppendResponse, Self::Error>;

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
    fn entry<D: AsRef<[u8]>>(&self, data: D) -> Self::Entry;

    /// Create a namespace of the associate Namespace type
    // TODO(sunng87): confusion with `create_namespace`
    fn namespace(&self, name: &str) -> Self::Namespace;
}

pub trait AppendResponse: Send + Sync {
    fn entry_id(&self) -> Id;

    fn offset(&self) -> Offset;
}
