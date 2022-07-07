#[cfg(test)]
mod tests;
mod writer;

use std::sync::Arc;

use async_trait::async_trait;
use snafu::ensure;
use store_api::storage::{ReadContext, Region, RegionMeta, WriteContext, WriteResponse};

use crate::background::JobPoolImpl;
use crate::error::{self, Error, Result};
use crate::flush::{FlushSchedulerImpl, FlushSchedulerRef, FlushStrategyRef, SizeBasedStrategy};
use crate::memtable::{DefaultMemtableBuilder, MemtableVersion};
use crate::metadata::{RegionMetaImpl, RegionMetadata};
pub use crate::region::writer::{RegionWriter, RegionWriterRef, WriterContext};
use crate::snapshot::SnapshotImpl;
use crate::sst::AccessLayerRef;
use crate::version::{VersionControl, VersionControlRef};
use crate::wal::Wal;
use crate::write_batch::WriteBatch;

/// [Region] implementation.
pub struct RegionImpl<S> {
    inner: Arc<RegionInner<S>>,
}

impl<S> Clone for RegionImpl<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<S> Region for RegionImpl<S>
where
    S: Send + Sync + 'static,
{
    type Error = Error;
    type Meta = RegionMetaImpl;
    type WriteRequest = WriteBatch;
    type Snapshot = SnapshotImpl;

    fn name(&self) -> &str {
        &self.inner.shared.name
    }

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        self.inner.in_memory_metadata()
    }

    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        self.inner.write(ctx, request).await
    }

    fn snapshot(&self, _ctx: &ReadContext) -> Result<SnapshotImpl> {
        Ok(self.inner.create_snapshot())
    }
}

impl<S> RegionImpl<S> {
    pub fn new(
        name: String,
        metadata: RegionMetadata,
        wal: Wal<S>,
        sst_layer: AccessLayerRef,
    ) -> RegionImpl<S> {
        let memtable_builder = Arc::new(DefaultMemtableBuilder {});
        let memtable_version = MemtableVersion::new();
        // TODO(yingwen): Pass flush scheduler to `RegionImpl::new`.
        let job_pool = Arc::new(JobPoolImpl {});
        let flush_scheduler = Arc::new(FlushSchedulerImpl::new(job_pool));

        let version_control = VersionControl::new(metadata, memtable_version);
        let inner = Arc::new(RegionInner {
            shared: Arc::new(SharedData {
                name,
                version_control: Arc::new(version_control),
            }),
            writer: Arc::new(RegionWriter::new(memtable_builder)),
            _wal: wal,
            flush_strategy: Arc::new(SizeBasedStrategy),
            flush_scheduler,
            sst_layer,
        });

        RegionImpl { inner }
    }

    #[cfg(test)]
    #[inline]
    fn committed_sequence(&self) -> store_api::storage::SequenceNumber {
        self.inner.version_control().committed_sequence()
    }
}

/// Shared data of region.
pub struct SharedData {
    pub name: String,
    // TODO(yingwen): Maybe no need to use Arc for version control.
    pub version_control: VersionControlRef,
}

pub type SharedDataRef = Arc<SharedData>;

struct RegionInner<S> {
    shared: SharedDataRef,
    writer: RegionWriterRef,
    _wal: Wal<S>,
    flush_strategy: FlushStrategyRef,
    flush_scheduler: FlushSchedulerRef,
    sst_layer: AccessLayerRef,
}

impl<S> RegionInner<S> {
    #[inline]
    fn version_control(&self) -> &VersionControl {
        &*self.shared.version_control
    }

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        let metadata = self.version_control().metadata();

        RegionMetaImpl::new(metadata)
    }

    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        let metadata = self.in_memory_metadata();
        let schema = metadata.schema();
        // Only compare column schemas.
        ensure!(
            schema.column_schemas() == request.schema().column_schemas(),
            error::InvalidInputSchemaSnafu {
                region: &self.shared.name
            }
        );

        // TODO(jiachun) [flush] write data to wal

        // TODO(yingwen): [flush] Add wal to WriteContext
        let writer_ctx = WriterContext {
            shared: &self.shared,
            flush_strategy: &self.flush_strategy,
            flush_scheduler: &self.flush_scheduler,
            sst_layer: &self.sst_layer,
            writer: &self.writer,
        };
        // Now altering schema is not allowed, so it is safe to validate schema outside of the lock.
        self.writer.write(ctx, request, writer_ctx).await
    }

    fn create_snapshot(&self) -> SnapshotImpl {
        let version = self.version_control().current();
        let sequence = self.version_control().committed_sequence();

        SnapshotImpl::new(version, sequence)
    }
}
