use async_trait::async_trait;
use common_error::ext::ErrorExt;

pub type ManifestVersion = i64;

#[async_trait]
pub trait MetaLogIterator {
    type Error: ErrorExt + Send + Sync;

    async fn next_log(&mut self) -> Result<Option<(ManifestVersion, String)>, Self::Error>;
}

#[async_trait]
pub trait ManifestLogStorage {
    type Error: ErrorExt + Send + Sync;
    type Iter: MetaLogIterator<Error = Self::Error>;

    async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<Self::Iter, Self::Error>;

    async fn save(&self, logs: &[String]) -> Result<ManifestVersion, Self::Error>;

    async fn delete(&self, start: ManifestVersion, end: ManifestVersion)
        -> Result<(), Self::Error>;

    async fn save_checkpoint(&self, ck: &str) -> Result<(), Self::Error>;

    async fn load_checkpoint(&self) -> Result<Option<String>, Self::Error>;
}
