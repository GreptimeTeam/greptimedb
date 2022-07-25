//! Engine config
#[derive(Debug, Clone)]
pub struct FileStoreConfig {
    /// Storage path
    pub store_dir: String,
}

impl Default for FileStoreConfig {
    fn default() -> Self {
        Self {
            store_dir: "/tmp/greptimedb/".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ObjectStoreConfig {
    File(FileStoreConfig),
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        ObjectStoreConfig::File(FileStoreConfig::default())
    }
}

#[derive(Debug, Clone, Default)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_engine_config() {
        let engine_config = EngineConfig::default();

        let store_dir = match &engine_config.store_config {
            ObjectStoreConfig::File(file) => &file.store_dir,
        };

        assert_eq!("/tmp/greptimedb/", store_dir);
    }
}
