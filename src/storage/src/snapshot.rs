use std::cmp;

use async_trait::async_trait;
use store_api::storage::{
    GetRequest, GetResponse, ReadContext, ScanRequest, ScanResponse, SchemaRef, SequenceNumber,
    Snapshot,
};

use crate::chunk::ChunkReaderImpl;
use crate::error::{Error, Result};
use crate::memtable::IterContext;
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
    type Reader = ChunkReaderImpl;

    fn schema(&self) -> &SchemaRef {
        self.version.schema()
    }

    async fn scan(
        &self,
        ctx: &ReadContext,
        request: ScanRequest,
    ) -> Result<ScanResponse<ChunkReaderImpl>> {
        let sequence = self.sequence_to_read(request.sequence);

        let mem = self.version.mutable_memtable();
        let iter_ctx = IterContext {
            batch_size: ctx.batch_size,
            sequence,
        };
        let iter = mem.iter(iter_ctx)?;

        let reader = ChunkReaderImpl::new(self.version.schema().clone(), iter);

        Ok(ScanResponse { reader })
    }

    async fn get(&self, _ctx: &ReadContext, _request: GetRequest) -> Result<GetResponse> {
        unimplemented!()
    }
}

impl SnapshotImpl {
    pub fn new(version: VersionRef, sequence: SequenceNumber) -> SnapshotImpl {
        SnapshotImpl { version, sequence }
    }

    #[inline]
    fn sequence_to_read(&self, request_sequence: Option<SequenceNumber>) -> SequenceNumber {
        request_sequence
            .map(|s| cmp::min(s, self.sequence))
            .unwrap_or(self.sequence)
    }
}
