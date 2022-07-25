use async_trait::async_trait;
use common_error::ext::ErrorExt;

use crate::manifest::ManifestVersion;

#[async_trait]
pub trait LogIterator: Send + Sync {
    type Error: ErrorExt + Send + Sync;

    async fn next_log(&mut self) -> Result<Option<(ManifestVersion, Vec<u8>)>, Self::Error>;
}

#[async_trait]
pub trait ManifestLogStorage {
    type Error: ErrorExt + Send + Sync;
    type Iter: LogIterator<Error = Self::Error>;

    /// Scan the logs in [start, end)
    async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<Self::Iter, Self::Error>;

    /// Save  a log
    async fn save(&self, version: ManifestVersion, bytes: &[u8]) -> Result<(), Self::Error>;

    /// Delete logs in [start, end)
    async fn delete(&self, start: ManifestVersion, end: ManifestVersion)
        -> Result<(), Self::Error>;

    /// Save a checkpoint
    async fn save_checkpoint(
        &self,
        version: ManifestVersion,
        bytes: &[u8],
    ) -> Result<(), Self::Error>;

    /// Load the latest checkpoint
    async fn load_checkpoint(&self) -> Result<Option<(ManifestVersion, Vec<u8>)>, Self::Error>;
}
