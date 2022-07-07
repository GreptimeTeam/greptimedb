use std::sync::Arc;

use common_error::prelude::BoxedError;
use snafu::ResultExt;
use store_api::logstore::{entry::Entry, namespace::Namespace, AppendResponse, LogStore};

use crate::error::{Result, WriteWalSnafu};

#[derive(Clone)]
pub struct Wal<S> {
    region: String,
    store: Arc<S>,
}

impl<S> Wal<S> {
    pub fn new(region: impl Into<String>, store: Arc<S>) -> Self {
        Self {
            region: region.into(),
            store,
        }
    }
}

impl<S: LogStore> Wal<S> {
    pub async fn write_wal(&self, bytes: &[u8]) -> Result<(u64, usize)> {
        // TODO(jiachun): region id
        let ns = S::Namespace::new(&self.region, 0);
        let e = S::Entry::new(bytes);

        let res = self
            .store
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
        let (log_store, _tmp) =
            test_util::log_store_util::create_tmp_local_file_log_store("wal_test").await;
        let wal = Wal::new("test_region", Arc::new(log_store));

        let res = wal.write_wal(b"test1").await.unwrap();

        assert_eq!(0, res.0);
        assert_eq!(0, res.1);

        let res = wal.write_wal(b"test2").await.unwrap();

        assert_eq!(1, res.0);
        assert_eq!(29, res.1);
    }
}
