use async_trait::async_trait;
use common_error::ext::ErrorExt;

pub type Version = u64;
pub const MIN_VERSION: u64 = 0;
pub const MAX_VERSION: u64 = u64::MAX;

#[async_trait]
pub trait MetaLogIterator: Send + Sync {
    type Error: ErrorExt + Send + Sync;

    async fn next_log(&mut self) -> Result<Option<(Version, Vec<u8>)>, Self::Error>;
}

#[async_trait]
pub trait ManifestLogStorage {
    type Error: ErrorExt + Send + Sync;
    type Iter: MetaLogIterator<Error = Self::Error>;

    async fn scan(&self, start: Version, end: Version) -> Result<Self::Iter, Self::Error>;

    async fn save(&self, version: Version, bytes: &[u8]) -> Result<(), Self::Error>;

    async fn delete(&self, start: Version, end: Version) -> Result<(), Self::Error>;

    async fn save_checkpoint(&self, version: Version, bytes: &[u8]) -> Result<(), Self::Error>;

    async fn load_checkpoint(&self) -> Result<Option<(Version, Vec<u8>)>, Self::Error>;
}
