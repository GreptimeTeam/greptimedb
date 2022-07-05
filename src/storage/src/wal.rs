use std::sync::Arc;

use common_error::prelude::BoxedError;
use snafu::ResultExt;
use store_api::logstore::{entry::Entry, namespace::Namespace, AppendResponse, LogStore};

use crate::error::{Result, WriteWalSnafu};

#[allow(dead_code)]
#[derive(Clone)]
pub struct Wal<T> {
    region: String,
    writer: Arc<T>,
}

#[allow(dead_code)]
impl<T> Wal<T> {
    pub fn new(region: impl Into<String>, writer: Arc<T>) -> Self {
        Self {
            region: region.into(),
            writer,
        }
    }
}

impl<T: LogStore> Wal<T> {
    #[allow(dead_code)]
    pub async fn write_wal(&self, bytes: &[u8]) -> Result<(u64, usize)> {
        // TODO(jiachun): region id
        let ns = T::Namespace::new(&self.region, 0);
        let e = T::Entry::new(bytes);

        let res = self
            .writer
            .append(ns, e)
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
    use log_store::test_util;

    use super::*;

    #[tokio::test]
    pub async fn test_write_wal() {
        let log_store = test_util::log_store_util::create_log_file_store("wal_test").await;
        let wal_writer = Wal::new("test_region", log_store);

        let res = wal_writer.write_wal(b"test1").await.unwrap();

        assert_eq!(0, res.0);
        assert_eq!(0, res.1);

        let res = wal_writer.write_wal(b"test2").await.unwrap();

        assert_eq!(1, res.0);
        assert_eq!(29, res.1);
    }
}
