use std::sync::Arc;

use async_trait::async_trait;
use store_api::storage::{ReadContext, Region, SchemaRef, WriteContext, WriteResponse};

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

    fn schema(&self) -> SchemaRef {
        self.inner.schema.clone()
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

struct RegionInner {
    schema: SchemaRef,
}
