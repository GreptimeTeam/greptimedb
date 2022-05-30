use std::sync::Arc;

use async_trait::async_trait;
use store_api::storage::{ReadContext, Region, WriteContext, WriteResponse};
use tokio::sync::Mutex;

use crate::column_family::ColumnFamilyHandle;
use crate::error::{Error, Result};
use crate::memtable::{DefaultMemTableBuilder, MemTableBuilder, MemTableSet};
use crate::metadata::{RegionMetaImpl, RegionMetadata};
use crate::region_writer::RegionWriter;
use crate::snapshot::SnapshotImpl;
use crate::version::{VersionControl, VersionControlRef};
use crate::write_batch::WriteBatch;

/// [Region] implementation.
#[derive(Clone)]
pub struct RegionImpl {
    inner: Arc<RegionInner>,
}

#[async_trait]
impl Region for RegionImpl {
    type Error = Error;
    type Meta = RegionMetaImpl;
    type WriteRequest = WriteBatch;
    type ColumnFamily = ColumnFamilyHandle;
    type Snapshot = SnapshotImpl;

    fn name(&self) -> &str {
        &self.inner.name
    }

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        self.inner.in_memory_metadata()
    }

    // TODO(yingwen): Move list_cf() to meta trait?
    fn list_cf(&self) -> Result<Vec<ColumnFamilyHandle>> {
        unimplemented!()
    }

    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        self.inner.write(ctx, request).await
    }

    fn snapshot(&self, _ctx: &ReadContext) -> Result<SnapshotImpl> {
        unimplemented!()
    }
}

impl RegionImpl {
    pub fn new(name: String, metadata: RegionMetadata) -> RegionImpl {
        let memtable_builder = Arc::new(DefaultMemTableBuilder {});
        let mem = memtable_builder.build();
        let memtables = MemTableSet::new(mem);

        let version = VersionControl::new(metadata, memtables);
        let inner = Arc::new(RegionInner {
            name,
            version: Arc::new(version),
            writer: Mutex::new(RegionWriter::new(memtable_builder)),
        });

        RegionImpl { inner }
    }
}

struct RegionInner {
    name: String,
    version: VersionControlRef,
    writer: Mutex<RegionWriter>,
}

impl RegionInner {
    fn in_memory_metadata(&self) -> RegionMetaImpl {
        let metadata = self.version.metadata();

        RegionMetaImpl::new(metadata)
    }

    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        // TODO(yingwen): Validate schema.

        let mut writer = self.writer.lock().await;
        writer.write(ctx, &self.version, request).await
    }
}
