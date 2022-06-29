//! metadata service
mod action;
mod metadata;
mod storage;

pub use action::MetaAction;
use async_trait::async_trait;
use common_error::ext::ErrorExt;
pub use metadata::{
    File, FileMeta, FileMetaRef, Metadata, MetadataId, MetadataIdRef, MetadataRef, VersionEditMeta,
    VersionEditMetaRef,
};
pub use storage::{ManifestLogStorage, MetaLogIterator};

#[async_trait]
pub trait Manifest: Send + Sync + Clone + 'static {
    type Error: ErrorExt + Send + Sync;
    type LogStore: ManifestLogStorage<Error = Self::Error>;

    async fn update(&self, action: MetaAction) -> Result<(), Self::Error>;

    async fn load(&self, id: MetadataIdRef) -> Result<Option<MetadataRef>, Self::Error>;

    async fn checkpoint(&self, id: MetadataIdRef) -> Result<(), Self::Error>;
}
