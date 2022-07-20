use std::cmp;

use async_trait::async_trait;
use store_api::storage::{
    GetRequest, GetResponse, ReadContext, ScanRequest, ScanResponse, SchemaRef, SequenceNumber,
    Snapshot,
};

use crate::chunk::{ChunkReaderBuilder, ChunkReaderImpl};
use crate::error::{Error, Result};
use crate::memtable::IterContext;
use crate::sst::AccessLayerRef;
use crate::version::VersionRef;

/// [Snapshot] implementation.
pub struct SnapshotImpl {
    version: VersionRef,
    /// Max sequence number (inclusive) visible to user.
    visible_sequence: SequenceNumber,
    sst_layer: AccessLayerRef,
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

        let mut builder =
            ChunkReaderBuilder::new(self.version.schema().clone(), self.sst_layer.clone())
                .num_memtables(memtable_version.num_memtables())
                .iter_ctx(IterContext {
                    batch_size: ctx.batch_size,
                    visible_sequence,
                    ..Default::default()
                })
                .pick_memtables(mutables)?;

        for mem_set in immutables {
            builder = builder.pick_memtables(mem_set)?;
        }

        let reader = builder.pick_ssts(&**self.version.ssts())?.build().await?;

        Ok(ScanResponse { reader })
    }

    async fn get(&self, _ctx: &ReadContext, _request: GetRequest) -> Result<GetResponse> {
        unimplemented!()
    }
}

impl SnapshotImpl {
    pub fn new(
        version: VersionRef,
        visible_sequence: SequenceNumber,
        sst_layer: AccessLayerRef,
    ) -> SnapshotImpl {
        SnapshotImpl {
            version,
            visible_sequence,
            sst_layer,
        }
    }

    #[inline]
    fn sequence_to_read(&self, request_sequence: Option<SequenceNumber>) -> SequenceNumber {
        request_sequence
            .map(|s| cmp::min(s, self.visible_sequence))
            .unwrap_or(self.visible_sequence)
    }
}
