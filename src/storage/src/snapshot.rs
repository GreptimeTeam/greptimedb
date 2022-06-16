use async_trait::async_trait;
use store_api::storage::{
    GetRequest, GetResponse, ReadContext, ScanRequest, ScanResponse, SchemaRef, SequenceNumber,
    Snapshot,
};

use crate::error::{Error, Result};
use crate::version::VersionRef;

/// [Snapshot] implementation.
pub struct SnapshotImpl {
    version: VersionRef,
    /// Max sequence number (inclusive) visible to user.
    sequence: SequenceNumber,
}

#[async_trait]
impl Snapshot for SnapshotImpl {
    type Error = Error;

    fn schema(&self) -> &SchemaRef {
        self.version.schema()
    }

    async fn scan(&self, _ctx: &ReadContext, _request: ScanRequest) -> Result<ScanResponse> {
        unimplemented!()
    }

    async fn get(&self, _ctx: &ReadContext, _request: GetRequest) -> Result<GetResponse> {
        unimplemented!()
    }
}

impl SnapshotImpl {
    pub fn new(version: VersionRef, sequence: SequenceNumber) -> SnapshotImpl {
        SnapshotImpl { version, sequence }
    }
}
