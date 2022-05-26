//! LogStore APIs.

use common_error::prelude::ErrorExt;
use entry::Offset;

use crate::logstore::entry::Entry;
use crate::logstore::entry_stream::SendableEntryStream;
use crate::logstore::namespace::Namespace;

pub mod entry;
pub mod entry_stream;
pub mod namespace;

/// `LogStore` serves as a Write-Ahead-Log for storage engine.
#[async_trait::async_trait]
pub trait LogStore {
    type Error: ErrorExt + Send + Sync;
    type Namespace: Namespace;
    type Entry: Entry;

    /// Append an `Entry` to WAL with given namespace
    async fn append(&mut self, ns: Self::Namespace, e: Self::Entry) -> Result<Offset, Self::Error>;

    // Append a batch of entries atomically and return the offset of first entry.
    async fn append_batch(
        &mut self,
        ns: Self::Namespace,
        e: Vec<Self::Entry>,
    ) -> Result<Offset, Self::Error>;

    // Create a new `EntryStream` to asynchronously generates `Entry`.
    async fn read(
        &self,
        ns: Self::Namespace,
        offset: Offset,
    ) -> Result<SendableEntryStream<Self::Entry>, Self::Error>;

    // Create a new `Namespace`.
    async fn create_namespace(&mut self, ns: Self::Namespace) -> Result<(), Self::Error>;

    // Delete an existing `Namespace` with given ref.
    async fn delete_namespace(&mut self, ns: Self::Namespace) -> Result<(), Self::Error>;

    // List all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error>;
}
