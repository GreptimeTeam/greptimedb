#[cfg(test)]
mod tests;
mod writer;

use std::sync::Arc;

use async_trait::async_trait;
use snafu::ensure;
use store_api::storage::{ReadContext, Region, RegionMeta, WriteContext, WriteResponse};
use tokio::sync::Mutex;

use crate::error::{self, Error, Result};
use crate::memtable::{DefaultMemtableBuilder, MemtableBuilder, MemtableSchema, MemtableSet};
use crate::metadata::{RegionMetaImpl, RegionMetadata};
use crate::region::writer::RegionWriter;
use crate::snapshot::SnapshotImpl;
use crate::version::{VersionControl, VersionControlRef};
use crate::wal::Wal;
use crate::write_batch::WriteBatch;

/// [Region] implementation.
pub struct RegionImpl<S> {
    inner: Arc<RegionInner<S>>,
}

impl<S> Clone for RegionImpl<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<S> Region for RegionImpl<S>
where
    S: Send + Sync + 'static,
{
    type Error = Error;
    type Meta = RegionMetaImpl;
    type WriteRequest = WriteBatch;
    type Snapshot = SnapshotImpl;

    fn name(&self) -> &str {
        &self.inner.name
    }

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        self.inner.in_memory_metadata()
    }

    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        self.inner.write(ctx, request).await
    }

    fn snapshot(&self, _ctx: &ReadContext) -> Result<SnapshotImpl> {
        Ok(self.inner.create_snapshot())
    }
}

impl<S> RegionImpl<S> {
    pub fn new(name: String, metadata: RegionMetadata, _wal: Wal<S>) -> RegionImpl<S> {
        let memtable_builder = Arc::new(DefaultMemtableBuilder {});
        let memtable_schema = MemtableSchema::new(metadata.columns_row_key.clone());
        let mem = memtable_builder.build(memtable_schema);
        let memtables = MemtableSet::new(mem);

        let version = VersionControl::new(metadata, memtables);
        let inner = Arc::new(RegionInner {
            name,
            version: Arc::new(version),
            writer: Mutex::new(RegionWriter::new(memtable_builder)),
            _wal,
        });

        RegionImpl { inner }
    }

    #[cfg(test)]
    #[inline]
    fn committed_sequence(&self) -> store_api::storage::SequenceNumber {
        self.inner.version.committed_sequence()
    }
}

struct RegionInner<S> {
    name: String,
    version: VersionControlRef,
    writer: Mutex<RegionWriter>,
    _wal: Wal<S>,
}

impl<S> RegionInner<S> {
    fn in_memory_metadata(&self) -> RegionMetaImpl {
        let metadata = self.version.metadata();

        RegionMetaImpl::new(metadata)
    }

    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        let metadata = self.in_memory_metadata();
        let schema = metadata.schema();
        // Only compare column schemas.
        ensure!(
            schema.column_schemas() == request.schema().column_schemas(),
            error::InvalidInputSchemaSnafu { region: &self.name }
        );

        // TODO(jiachun) write data to wal

        // Now altering schema is not allowed, so it is safe to validate schema outside of the lock.
        let mut writer = self.writer.lock().await;
        writer.write(ctx, &self.version, request).await
    }

    fn create_snapshot(&self) -> SnapshotImpl {
        let version = self.version.current();
        let sequence = self.version.committed_sequence();

        SnapshotImpl::new(version, sequence)
    }
}
