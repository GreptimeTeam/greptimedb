use std::sync::Arc;

use async_trait::async_trait;
use store_api::storage::{
    ReadContext, Region, RegionMetadata, RegionMetadataRef, WriteContext, WriteResponse,
};

use crate::column_family::ColumnFamilyHandle;
use crate::error::{Error, Result};
use crate::snapshot::SnapshotImpl;
use crate::write_batch::WriteBatch;

/// [Region] implementation.
// TODO(yingwen): Wrap a arc inner.
#[derive(Clone)]
pub struct RegionImpl {
    inner: Arc<RegionInner>,
}

#[async_trait]
impl Region for RegionImpl {
    type Error = Error;
    type WriteRequest = WriteBatch;
    type ColumnFamily = ColumnFamilyHandle;
    type Snapshot = SnapshotImpl;

    fn in_memory_metadata(&self) -> RegionMetadataRef {
        self.inner.metadata.clone()
    }

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
    pub fn new(metadata: RegionMetadata) -> RegionImpl {
        let metadata = Arc::new(metadata);
        let inner = Arc::new(RegionInner { metadata });

        RegionImpl { inner }
    }
}

struct RegionInner {
    metadata: RegionMetadataRef,
}
