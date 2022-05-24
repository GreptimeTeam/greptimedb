use async_trait::async_trait;
use store_api::storage::{
    GetRequest, GetResponse, ReadContext, ScanRequest, ScanResponse, SchemaRef, Snapshot,
};

use crate::column_family::ColumnFamilyHandle;
use crate::error::{Error, Result};

/// [Snapshot] implementation.
pub struct SnapshotImpl {}

#[async_trait]
impl Snapshot for SnapshotImpl {
    type Error = Error;
    type ColumnFamily = ColumnFamilyHandle;

    fn schema(&self) -> &SchemaRef {
        unimplemented!()
    }

    async fn scan(&self, _ctx: &ReadContext, _request: ScanRequest) -> Result<ScanResponse> {
        unimplemented!()
    }

    async fn get(&self, _ctx: &ReadContext, _request: GetRequest) -> Result<GetResponse> {
        unimplemented!()
    }

    fn list_cf(&self) -> Result<Vec<ColumnFamilyHandle>> {
        unimplemented!()
    }
}
