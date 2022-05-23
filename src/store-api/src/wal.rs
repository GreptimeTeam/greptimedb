//! WAL APIs.

use crate::error::Result;
use crate::wal::entry::Entry;
use crate::wal::entry_stream::EntryStreamRef;

mod entry;
mod entry_stream;
mod namespace;

pub use entry::Offset;

use crate::wal::namespace::Namespace;

/// WAL serves as a Write-Ahead-Log for storage engine.
#[async_trait::async_trait]
pub trait Wal {
    type Namespace: Namespace;

    /// Append an `Entry` to WAL with given namespace
    async fn append(&mut self, ns: Self::Namespace, e: Entry) -> Result<Offset>;

    // Create a new `EntryStream` to asynchronously generates `Entry`.
    async fn reader(&self, ns: Self::Namespace, offset: Offset) -> Result<EntryStreamRef>;

    // Create a new `Namespace`.
    async fn create_namespace(&mut self, ns: Self::Namespace) -> Result<()>;

    // Delete an existing `Namespace` with given ref.
    async fn delete_namespace(&mut self, ns: Self::Namespace) -> Result<()>;

    // List all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>>;
}
