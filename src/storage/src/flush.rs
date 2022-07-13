use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::logging;
use common_time::RangeMillis;
use store_api::manifest::Manifest;
use store_api::manifest::ManifestVersion;
use store_api::storage::SequenceNumber;

use crate::background::{Context, Job, JobHandle, JobPoolRef};
use crate::error::Result;
use crate::manifest::action::*;
use crate::memtable::MemtableRef;
use crate::region::RegionWriterRef;
use crate::region::SharedDataRef;
use crate::sst::{AccessLayerRef, FileMeta};
use crate::version::VersionEdit;

pub trait FlushStrategy: Send + Sync {
    fn should_flush(&self, bytes_allocated: usize) -> bool;
}

pub type FlushStrategyRef = Arc<dyn FlushStrategy>;

pub struct SizeBasedStrategy;

impl FlushStrategy for SizeBasedStrategy {
    fn should_flush(&self, _bytes_allocated: usize) -> bool {
        unimplemented!()
    }
}

pub struct MemtableWithMeta {
    pub memtable: MemtableRef,
    pub bucket: RangeMillis,
}

#[async_trait]
pub trait FlushScheduler: Send + Sync {
    async fn schedule_flush(&self, flush_job: FlushJob) -> Result<JobHandle>;
}

pub struct FlushSchedulerImpl {
    job_pool: JobPoolRef,
}

impl FlushSchedulerImpl {
    pub fn new(job_pool: JobPoolRef) -> FlushSchedulerImpl {
        FlushSchedulerImpl { job_pool }
    }
}

#[async_trait]
impl FlushScheduler for FlushSchedulerImpl {
    async fn schedule_flush(&self, flush_job: FlushJob) -> Result<JobHandle> {
        // TODO(yingwen): [flush] Implements flush schedule strategy, controls max background flushes.
        self.job_pool.submit(Box::new(flush_job)).await
    }
}

pub type FlushSchedulerRef = Arc<dyn FlushScheduler>;

pub struct FlushJob {
    /// Memtables to be flushed.
    pub memtables: Vec<MemtableWithMeta>,
    /// Last sequence of data to be flushed.
    pub flush_sequence: SequenceNumber,
    /// Shared data of region to be flushed.
    pub shared: SharedDataRef,
    /// Sst access layer of the region.
    pub sst_layer: AccessLayerRef,
    /// Region writer, used to persist log entry that points to the latest manifest file.
    pub writer: RegionWriterRef,
}

impl FlushJob {
    async fn write_memtables_to_layer(&self, ctx: &Context) -> Result<Vec<FileMeta>> {
        if ctx.is_cancelled() {
            // TODO(yingwen): [flush] Returns an cancelled error.
            unimplemented!();
        }

        // TODO(yingwen): [flush] Flush memtables to sst layer.
        unimplemented!()
    }

    async fn write_to_manifest(&self, file_metas: &[FileMeta]) -> Result<ManifestVersion> {
        let edit = RegionEdit {
            region_id: self.shared.id,
            region_version: self.shared.version_control.metadata().version,
            files_to_add: file_metas.to_vec(),
            files_to_remove: Vec::default(),
        };
        logging::debug!("Write region edit: {:?} to manifest.", edit);
        self.shared
            .manifest
            .update(RegionMetaAction::Edit(edit))
            .await
    }
}

#[async_trait]
impl Job for FlushJob {
    // TODO(yingwen): [flush] Support in-job parallelism (Flush memtables concurrently)
    async fn run(&mut self, ctx: &Context) -> Result<()> {
        let file_metas = self.write_memtables_to_layer(ctx).await?;

        let manifest_version = self.write_to_manifest(&file_metas).await?;

        let edit = VersionEdit {
            files_to_add: file_metas,
            flushed_sequence: Some(self.flush_sequence),
            manifest_version,
        };

        self.writer.apply_version_edit(edit, &self.shared).await?;

        Ok(())
    }
}
