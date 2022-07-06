//! metadata service
mod storage;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use serde::{de::DeserializeOwned, Serialize};
pub use storage::*;

pub trait Metadata: Clone {}
pub trait MetadataId: Clone {}
pub trait MetaAction: Serialize + DeserializeOwned {
    type MetadataId: MetadataId;
    fn metadata_id(&self) -> &Self::MetadataId;
}

#[async_trait]
pub trait Manifest: Send + Sync + Clone + 'static {
    type Error: ErrorExt + Send + Sync;
    type MetaAction: MetaAction;
    type MetadataId: MetadataId;
    type Metadata: Metadata;

    async fn update(&self, action: Self::MetaAction) -> Result<(), Self::Error>;

    async fn load(&self, id: Self::MetadataId) -> Result<Option<Self::Metadata>, Self::Error>;

    async fn checkpoint(&self, id: Self::MetadataId) -> Result<(), Self::Error>;
}
