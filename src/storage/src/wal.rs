use std::sync::Arc;

use common_error::ext::BoxedError;
use snafu::ResultExt;
use store_api::logstore::{entry::Entry, namespace::Namespace, AppendResponse, LogStore};

use crate::error::{Result, WriteWalSnafu};

#[derive(Clone)]
pub struct Wal<Writer: LogStore> {
    region: String,
    writer: Arc<Writer>,
}

impl<Writer: LogStore> Wal<Writer> {
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
        let ns = Namespace::new(&self.region, 0);
        let entry = Entry::new(bytes);

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
