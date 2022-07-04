use std::sync::Arc;

use common_error::ext::BoxedError;
use snafu::ResultExt;
use store_api::logstore::{entry::Entry, namespace::Namespace, AppendResponse, LogStore};

use crate::error::{Result, WriteWalSnafu};

#[derive(Clone)]
pub struct Wal<Writer> {
    region: String,
    writer: Arc<Writer>,
}

impl<Writer> Wal<Writer> {
    pub fn new(region: impl Into<String>, writer: Arc<Writer>) -> Self {
        Self {
            region: region.into(),
            writer,
        }
    }
}

impl<Writer: LogStore> Wal<Writer> {
    pub async fn write_wal(&self, bytes: &[u8]) -> Result<(u64, usize)> {
        // TODO(jiachun): region id
        let ns = Writer::Namespace::new(&self.region, 0);
        let entry = Writer::Entry::new(bytes);

        let res = self
            .writer
            .append(ns, entry)
            .await
            .map_err(BoxedError::new)
            .context(WriteWalSnafu {
                region: &self.region,
            })?;

        Ok((res.entry_id(), res.offset()))
    }
}
#[cfg(test)]
mod tests {
    use log_store::fs::*;
    use tempdir::TempDir;

    use super::*;

    #[tokio::test]
    pub async fn test_write_wal() {
        let dir = TempDir::new("greptimedb_wal_test").unwrap();
        let dir = dir.path().to_str().unwrap().to_string();
        let config = config::LogConfig {
            append_buffer_size: 128,
            max_log_file_size: 128,
            log_file_dir: dir,
        };
        let log_store = log::LocalFileLogStore::open(&config).await.unwrap();
        let wal_writer = Wal::new("test_region", Arc::new(log_store));

        let res = wal_writer.write_wal(b"test1").await.unwrap();

        assert_eq!(0, res.0);
        assert_eq!(0, res.1);

        let res = wal_writer.write_wal(b"test2").await.unwrap();

        assert_eq!(1, res.0);
        assert_eq!(29, res.1);
    }
}
