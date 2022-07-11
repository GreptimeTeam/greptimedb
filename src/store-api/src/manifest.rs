//! metadata service
mod storage;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use object_store::ObjectStore;
use serde::{de::DeserializeOwned, Serialize};
pub use storage::*;

pub trait Metadata: Clone {}

pub trait MetadataId: Clone + Copy {}

/// The action to apply on metadata
pub trait MetaAction: Serialize + DeserializeOwned {
    type MetadataId: MetadataId;

    /// Returns the metadata id of the action
    fn metadata_id(&self) -> Self::MetadataId;
}

/// Manifest service
#[async_trait]
pub trait Manifest: Send + Sync + Clone + 'static {
    type Error: ErrorExt + Send + Sync;
    type MetaAction: MetaAction;
    type MetadataId: MetadataId;
    type Metadata: Metadata;

    fn new(id: Self::MetadataId, manifest_dir: &str, object_store: ObjectStore) -> Self;

    /// Update metadata by the action
    async fn update(&self, action: Self::MetaAction) -> Result<(), Self::Error>;

    /// Retrieve the latest metadata
    async fn load(&self) -> Result<Option<Self::Metadata>, Self::Error>;

    async fn checkpoint(&self) -> Result<(), Self::Error>;

    fn metadata_id(&self) -> Self::MetadataId;
}
