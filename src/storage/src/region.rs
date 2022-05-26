use std::sync::Arc;

use async_trait::async_trait;
use store_api::storage::{ReadContext, Region, WriteContext, WriteResponse};

use crate::column_family::ColumnFamilyHandle;
use crate::error::{Error, Result};
use crate::metadata::{RegionMetaImpl, RegionMetadata, RegionMetadataRef};
use crate::snapshot::SnapshotImpl;
use crate::version_control::{VersionControl, VersionControlRef};
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

    // TODO(yingwen): Add a name trait ?

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        self.inner.in_memory_metadata()
    }

    // TODO(yingwen): Move list_cf() to meta trait?
    fn list_cf(&self) -> Result<Vec<ColumnFamilyHandle>> {
        unimplemented!()
    }

    async fn write(&self, _ctx: &WriteContext, _request: WriteBatch) -> Result<WriteResponse> {
        unimplemented!()
    }

    fn snapshot(&self, _ctx: &ReadContext) -> Result<SnapshotImpl> {
        unimplemented!()
    }
}

impl RegionImpl {
    pub fn new(name: String, metadata: RegionMetadata) -> RegionImpl {
        let version = VersionControl::new(metadata);
        let inner = Arc::new(RegionInner {
            name,
            version: Arc::new(version),
        });

        RegionImpl { inner }
    }
}

struct RegionInner {
    name: String,
    version: VersionControlRef,
}

impl RegionInner {
    fn in_memory_metadata(&self) -> RegionMetaImpl {
        let metadata = self.version.metadata();

        RegionMetaImpl::new(metadata)
    }
}
