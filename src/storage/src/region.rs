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
pub struct RegionImpl<T> {
    inner: Arc<RegionInner<T>>,
}

impl<T> Clone for RegionImpl<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<T> Region for RegionImpl<T>
where
    T: Send + Sync + 'static,
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

impl<T> RegionImpl<T> {
    pub fn new(name: String, metadata: RegionMetadata, wal_writer: Wal<T>) -> RegionImpl<T> {
        let memtable_builder = Arc::new(DefaultMemtableBuilder {});
        let memtable_schema = MemtableSchema::new(metadata.columns_row_key.clone());
        let mem = memtable_builder.build(memtable_schema);
        let memtables = MemtableSet::new(mem);

        let version = VersionControl::new(metadata, memtables);
        let inner = Arc::new(RegionInner {
            name,
            version: Arc::new(version),
            writer: Mutex::new(RegionWriter::new(memtable_builder)),
            wal_writer,
        });

        RegionImpl { inner }
    }

    #[cfg(test)]
    #[inline]
    fn committed_sequence(&self) -> store_api::storage::SequenceNumber {
        self.inner.version.committed_sequence()
    }
}

#[allow(dead_code)]
struct RegionInner<T> {
    name: String,
    version: VersionControlRef,
    writer: Mutex<RegionWriter>,
    wal_writer: Wal<T>,
}

impl<T> RegionInner<T> {
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
