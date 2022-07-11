//! Engine config
#[derive(Debug, Clone)]
pub struct FileStoreConfig {
    /// Storage path
    pub store_dir: String,
}

#[derive(Debug, Clone)]
pub enum ObjectStoreConfig {
    File(FileStoreConfig),
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub store_config: ObjectStoreConfig,
}

impl EngineConfig {
    pub fn with_store_dir(store_dir: &str) -> Self {
        Self {
            store_config: ObjectStoreConfig::File(FileStoreConfig {
                store_dir: store_dir.to_string(),
            }),
        }
    }
}
