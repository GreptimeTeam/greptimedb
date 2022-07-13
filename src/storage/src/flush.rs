use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::logging;
use common_time::RangeMillis;
use store_api::storage::SequenceNumber;

use crate::background::{Context, Job, JobHandle, JobPoolRef};
use crate::error::Result;
use crate::memtable::MemtableRef;
use crate::region::RegionWriterRef;
use crate::region::SharedDataRef;
use crate::sst::{AccessLayerRef, FileMeta};
use crate::version::VersionEdit;

/// Default write buffer size (32M).
const DEFAULT_WRITE_BUFFER_SIZE: usize = 32 * 1024 * 1024;

pub trait FlushStrategy: Send + Sync {
    fn should_flush(
        &self,
        shared: &SharedDataRef,
        bytes_mutable: usize,
        bytes_total: usize,
    ) -> bool;
}

pub type FlushStrategyRef = Arc<dyn FlushStrategy>;

#[derive(Debug)]
pub struct SizeBasedStrategy {
    /// Write buffer size of memtable.
    max_write_buffer_size: usize,
    /// Mutable memtable memory size limitation
    mutable_limitation: usize,
}

#[inline]
fn get_mutable_limitation(max_write_buffer_size: usize) -> usize {
    // Inspired by RocksDB
    // https://github.com/facebook/rocksdb/blob/main/include/rocksdb/write_buffer_manager.h#L86
    max_write_buffer_size * 7 / 8
}

impl Default for SizeBasedStrategy {
    fn default() -> Self {
        let max_write_buffer_size = DEFAULT_WRITE_BUFFER_SIZE;
        Self {
            max_write_buffer_size,
            mutable_limitation: get_mutable_limitation(max_write_buffer_size),
        }
    }
}

impl FlushStrategy for SizeBasedStrategy {
    fn should_flush(
        &self,
        shared: &SharedDataRef,
        bytes_mutable: usize,
        bytes_total: usize,
    ) -> bool {
        // Insipired by RocksDB flush strategy
        // https://github.com/facebook/rocksdb/blob/main/include/rocksdb/write_buffer_manager.h#L94

        if bytes_mutable > self.mutable_limitation {
            logging::info!("Region should flush, region: {}, bytes_mutable: {}, mutable_limitation: {}, bytes_total: {}, max_write_buffer_size: {} .",
                            shared.name,
                            bytes_mutable,
                            self.mutable_limitation,
                            bytes_total,
                            self.max_write_buffer_size);

            return true;
        }

        let buffer_size = self.max_write_buffer_size;

        let should_flush = bytes_total >= buffer_size && bytes_mutable >= buffer_size / 2;

        if should_flush {
            logging::info!("Region should flush, region: {}, bytes_mutable: {}, mutable_limitation: {}, bytes_total: {}, max_write_buffer_size: {} .",
                            shared.name,
                            bytes_mutable,
                            self.mutable_limitation,
                            bytes_total,
                            buffer_size);
        }

        should_flush
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

// TODO(yingwen): Use the Version number type in manifest.
pub type ManifestVersion = u64;

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

    async fn write_to_manifest(&self, _file_metas: &[FileMeta]) -> Result<ManifestVersion> {
        // TODO(yingwen): [flush] Write all metadata to manifest.
        unimplemented!()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_mutable_limitation() {
        assert_eq!(7, get_mutable_limitation(8));
        assert_eq!(8, get_mutable_limitation(10));
        assert_eq!(56, get_mutable_limitation(64));
    }
}
