//! metadata service
pub mod action;
mod storage;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use serde::{de::DeserializeOwned, Serialize};

use crate::manifest::action::ProtocolAction;
use crate::manifest::action::ProtocolVersion;
pub use crate::manifest::storage::*;

pub type ManifestVersion = u64;
pub const MIN_VERSION: u64 = 0;
pub const MAX_VERSION: u64 = u64::MAX;

pub trait MetaAction: Serialize + DeserializeOwned + Send + Sync + Clone + std::fmt::Debug {
    type Error: ErrorExt + Send + Sync;

    /// Set previous valid manifest version.
    fn set_prev_version(&mut self, version: ManifestVersion);

    /// Encode this action into a byte vector
    fn encode(&self) -> Result<Vec<u8>, Self::Error>;

    /// Decode self from byte slice with reader protocol version,
    /// return error when reader version is not supported.
    fn decode(
        bs: &[u8],
        reader_version: ProtocolVersion,
    ) -> Result<(Self, Option<ProtocolAction>), Self::Error>;
}

#[async_trait]
pub trait MetaActionIterator {
    type MetaAction: MetaAction;
    type Error: ErrorExt + Send + Sync;

    async fn next_action(
        &mut self,
    ) -> Result<Option<(ManifestVersion, Self::MetaAction)>, Self::Error>;
}

/// Manifest service
#[async_trait]
pub trait Manifest: Send + Sync + Clone + 'static {
    type Error: ErrorExt + Send + Sync;
    type MetaAction: MetaAction;
    type MetaActionIterator: MetaActionIterator<Error = Self::Error, MetaAction = Self::MetaAction>;

    /// Update metadata by the action
    async fn update(&self, action: Self::MetaAction) -> Result<ManifestVersion, Self::Error>;

    /// Scan actions which version in range [start, end)
    async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<Self::MetaActionIterator, Self::Error>;

    async fn checkpoint(&self) -> Result<ManifestVersion, Self::Error>;

    fn last_version(&self) -> ManifestVersion;
}
