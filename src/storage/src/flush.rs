use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::logging;
use common_time::RangeMillis;
use store_api::logstore::LogStore;
use store_api::manifest::{Manifest, ManifestVersion, MetaAction};
use store_api::storage::SequenceNumber;
use uuid::Uuid;

use crate::background::{Context, Job, JobHandle, JobPoolRef};
use crate::error::{CancelledSnafu, Result};
use crate::manifest::action::*;
use crate::manifest::region::RegionManifest;
use crate::memtable::{IterContext, MemtableId, MemtableRef};
use crate::region::RegionWriterRef;
use crate::region::SharedDataRef;
use crate::sst::{AccessLayerRef, FileMeta, WriteOptions};
use crate::version::VersionEdit;
use crate::wal::Wal;

/// Default write buffer size (32M).
const DEFAULT_WRITE_BUFFER_SIZE: usize = 32 * 1024 * 1024;

pub trait FlushStrategy: Send + Sync + std::fmt::Debug {
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
            logging::info!(
                "Region should flush, region: {}, bytes_mutable: {}, mutable_limitation: {}, \
                 bytes_total: {}, max_write_buffer_size: {} .",
                shared.name(),
                bytes_mutable,
                self.mutable_limitation,
                bytes_total,
                self.max_write_buffer_size
            );

            return true;
        }

        let buffer_size = self.max_write_buffer_size;

        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        let should_flush = bytes_total >= buffer_size && bytes_mutable >= buffer_size / 2;

        if should_flush {
            logging::info!(
                "Region should flush, region: {}, bytes_mutable: {}, mutable_limitation: {}, \
                 bytes_total: {}, max_write_buffer_size: {} .",
                shared.name(),
                bytes_mutable,
                self.mutable_limitation,
                bytes_total,
                buffer_size
            );
        }

        should_flush
    }
}

#[derive(Debug)]
pub struct MemtableWithMeta {
    pub memtable: MemtableRef,
    pub bucket: RangeMillis,
}

#[async_trait]
pub trait FlushScheduler: Send + Sync + std::fmt::Debug {
    async fn schedule_flush(&self, flush_job: Box<dyn Job>) -> Result<JobHandle>;
}

#[derive(Debug)]
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
    async fn schedule_flush(&self, flush_job: Box<dyn Job>) -> Result<JobHandle> {
        // TODO(yingwen): [flush] Implements flush schedule strategy, controls max background flushes.
        self.job_pool.submit(flush_job).await
    }
}

pub type FlushSchedulerRef = Arc<dyn FlushScheduler>;

pub struct FlushJob<S: LogStore> {
    /// Max memtable id in these memtables,
    /// used to remove immutable memtables in current version.
    pub max_memtable_id: MemtableId,
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
    /// Region write-ahead logging, used to write data/meta to the log file.
    pub wal: Wal<S>,
    /// Region manifest service, used to persist metadata.
    pub manifest: RegionManifest,
}

impl<S: LogStore> FlushJob<S> {
    async fn write_memtables_to_layer(&self, ctx: &Context) -> Result<Vec<FileMeta>> {
        if ctx.is_cancelled() {
            return CancelledSnafu {}.fail();
        }

        let mut futures = Vec::with_capacity(self.memtables.len());
        let iter_ctx = IterContext {
            for_flush: true,
            ..Default::default()
        };
        for m in &self.memtables {
            let file_name = Self::generate_sst_file_name();
            // TODO(hl): Check if random file name already exists in meta.
            let iter = m.memtable.iter(&iter_ctx)?;
            futures.push(async move {
                self.sst_layer
                    .write_sst(&file_name, iter, &WriteOptions::default())
                    .await?;

                Ok(FileMeta {
                    file_name,
                    level: 0,
                })
            });
        }

        let metas = futures_util::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect();

        logging::info!("Successfully flush memtables to files: {:?}", metas);
        Ok(metas)
    }

    async fn write_to_manifest(&self, file_metas: &[FileMeta]) -> Result<ManifestVersion> {
        let edit = RegionEdit {
            region_version: self.shared.version_control.metadata().version(),
            flushed_sequence: self.flush_sequence,
            files_to_add: file_metas.to_vec(),
            files_to_remove: Vec::default(),
        };
        let prev_version = self.shared.version_control.current_manifest_version();

        logging::debug!(
            "Write region edit: {:?} to manifest, prev_version: {}.",
            edit,
            prev_version,
        );

        let mut action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit));
        action_list.set_prev_version(prev_version);

        self.manifest.update(action_list).await
    }

    /// Generates random SST file name in format: `^[a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}.parquet$`
    fn generate_sst_file_name() -> String {
        format!("{}.parquet", Uuid::new_v4().hyphenated())
    }
}

#[async_trait]
impl<S: LogStore> Job for FlushJob<S> {
    // TODO(yingwen): [flush] Support in-job parallelism (Flush memtables concurrently)
    async fn run(&mut self, ctx: &Context) -> Result<()> {
        let file_metas = self.write_memtables_to_layer(ctx).await?;

        let manifest_version = self.write_to_manifest(&file_metas).await?;

        let edit = VersionEdit {
            files_to_add: file_metas,
            flushed_sequence: Some(self.flush_sequence),
            manifest_version,
            max_memtable_id: Some(self.max_memtable_id),
        };

        self.writer
            .apply_version_edit(&self.wal, edit, &self.shared)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use log_store::fs::noop::NoopLogStore;
    use regex::Regex;

    use super::*;

    #[test]
    fn test_get_mutable_limitation() {
        assert_eq!(7, get_mutable_limitation(8));
        assert_eq!(8, get_mutable_limitation(10));
        assert_eq!(56, get_mutable_limitation(64));
    }

    #[test]
    pub fn test_uuid_generate() {
        let file_name = FlushJob::<NoopLogStore>::generate_sst_file_name();
        let regex = Regex::new(r"^[a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}.parquet$").unwrap();
        assert!(
            regex.is_match(&file_name),
            "illegal sst file name: {}",
            file_name
        );
    }
}
