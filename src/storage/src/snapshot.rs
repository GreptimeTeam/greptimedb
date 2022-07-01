use std::cmp;

use async_trait::async_trait;
use store_api::storage::{
    GetRequest, GetResponse, ReadContext, ScanRequest, ScanResponse, SchemaRef, SequenceNumber,
    Snapshot,
};

use crate::chunk::ChunkReaderImpl;
use crate::error::{Error, Result};
use crate::version::VersionRef;

/// [Snapshot] implementation.
pub struct SnapshotImpl {
    version: VersionRef,
    /// Max sequence number (inclusive) visible to user.
    visible_sequence: SequenceNumber,
}

#[async_trait]
impl Snapshot for SnapshotImpl {
    type Error = Error;
    type Reader = ChunkReaderImpl;

    fn schema(&self) -> &SchemaRef {
        self.version.schema()
    }

    async fn scan(
        &self,
        _ctx: &ReadContext,
        request: ScanRequest,
    ) -> Result<ScanResponse<ChunkReaderImpl>> {
        let _visible_sequence = self.sequence_to_read(request.sequence);
        let _mem = self.version.memtables();

        // TODO(yingwen): [flush] Scan and merge results from mutable memtables.
        unimplemented!()
    }

    async fn get(&self, _ctx: &ReadContext, _request: GetRequest) -> Result<GetResponse> {
        unimplemented!()
    }
}

impl SnapshotImpl {
    pub fn new(version: VersionRef, visible_sequence: SequenceNumber) -> SnapshotImpl {
        SnapshotImpl {
            version,
            visible_sequence,
        }
    }

    #[inline]
    fn sequence_to_read(&self, request_sequence: Option<SequenceNumber>) -> SequenceNumber {
        request_sequence
            .map(|s| cmp::min(s, self.visible_sequence))
            .unwrap_or(self.visible_sequence)
    }
}
