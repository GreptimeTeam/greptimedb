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
        ctx: &ReadContext,
        request: ScanRequest,
    ) -> Result<ScanResponse<ChunkReaderImpl>> {
        let visible_sequence = self.sequence_to_read(request.sequence);
        let memtable_version = self.version.memtables();

        let mutables = memtable_version.mutable_memtables();
        let immutables = memtable_version.immutable_memtables();
        let mut batch_iters = Vec::with_capacity(memtable_version.num_memtables());

        let iter_ctx = IterContext {
            batch_size: ctx.batch_size,
            visible_sequence,
            ..Default::default()
        };

        for (_range, mem) in mutables.iter() {
            let iter = mem.iter(iter_ctx.clone())?;

            batch_iters.push(iter);
        }

        for mem_set in immutables {
            for (_range, mem) in mem_set.iter() {
                let iter = mem.iter(iter_ctx.clone())?;

                batch_iters.push(iter);
            }
        }

        // Now we just simply chain all iterators together, ignore duplications/ordering.
        let iter = Box::new(batch_iters.into_iter().flatten());

        let reader = ChunkReaderImpl::new(self.version.schema().clone(), iter);

        Ok(ScanResponse { reader })
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
