// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod scheduler;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use common_telemetry::logging;
pub use scheduler::{FlushHandle, FlushRequest, FlushScheduler, FlushSchedulerRef};
use store_api::logstore::LogStore;
use store_api::storage::consts::WRITE_ROW_GROUP_SIZE;
use store_api::storage::SequenceNumber;

use crate::config::EngineConfig;
use crate::error::Result;
use crate::manifest::action::*;
use crate::manifest::region::RegionManifest;
use crate::memtable::{IterContext, MemtableId, MemtableRef};
use crate::region::{RegionWriterRef, SharedDataRef};
use crate::sst::{AccessLayerRef, FileId, FileMeta, Source, SstInfo, WriteOptions};
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

impl SizeBasedStrategy {
    pub fn new(max_write_buffer_size: usize) -> Self {
        Self {
            max_write_buffer_size,
            mutable_limitation: get_mutable_limitation(max_write_buffer_size),
        }
    }
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

// #[async_trait]
// pub trait FlushScheduler: Send + Sync + std::fmt::Debug {
//     async fn schedule_flush(&self, flush_job: Box<dyn Job>) -> Result<JobHandle>;
// }

// #[derive(Debug)]
// pub struct FlushSchedulerImpl {
//     job_pool: JobPoolRef,
// }

// impl FlushSchedulerImpl {
//     pub fn new(job_pool: JobPoolRef) -> FlushSchedulerImpl {
//         FlushSchedulerImpl { job_pool }
//     }
// }

// #[async_trait]
// impl FlushScheduler for FlushSchedulerImpl {
//     async fn schedule_flush(&self, flush_job: Box<dyn Job>) -> Result<JobHandle> {
//         // TODO(yingwen): [flush] Implements flush schedule strategy, controls max background flushes.
//         self.job_pool.submit(flush_job).await
//     }
// }

// pub type FlushSchedulerRef = Arc<dyn FlushScheduler>;

pub type FlushCallback = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub struct FlushJob<S: LogStore> {
    /// Max memtable id in these memtables,
    /// used to remove immutable memtables in current version.
    pub max_memtable_id: MemtableId,
    /// Memtables to be flushed.
    pub memtables: Vec<MemtableRef>,
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
    /// Callbacks that get invoked on flush success.
    pub on_success: Option<FlushCallback>,
    /// Storage engine config
    pub engine_config: Arc<EngineConfig>,
}

impl<S: LogStore> FlushJob<S> {
    /// Execute the flush job.
    async fn run(&mut self) -> Result<()> {
        let file_metas = self.write_memtables_to_layer().await?;
        self.write_manifest_and_apply(&file_metas).await?;

        if let Some(cb) = self.on_success.take() {
            cb.await;
        }
        Ok(())
    }

    async fn write_memtables_to_layer(&mut self) -> Result<Vec<FileMeta>> {
        let region_id = self.shared.id();
        let mut futures = Vec::with_capacity(self.memtables.len());
        let iter_ctx = IterContext {
            for_flush: true,
            // TODO(ruihang): dynamic row group size based on content (#412)
            batch_size: WRITE_ROW_GROUP_SIZE,
            ..Default::default()
        };

        for m in &self.memtables {
            // skip empty memtable
            if m.num_rows() == 0 {
                continue;
            }

            let file_id = FileId::random();
            // TODO(hl): Check if random file name already exists in meta.
            let iter = m.iter(&iter_ctx)?;
            let sst_layer = self.sst_layer.clone();
            let write_options = WriteOptions {
                sst_write_buffer_size: self.engine_config.sst_write_buffer_size,
            };
            futures.push(async move {
                Ok(sst_layer
                    .write_sst(file_id, Source::Iter(iter), &write_options)
                    .await?
                    .map(
                        |SstInfo {
                             time_range,
                             file_size,
                             ..
                         }| FileMeta {
                            region_id,
                            file_id,
                            time_range,
                            level: 0,
                            file_size,
                        },
                    ))
            });
        }

        let metas = futures_util::future::try_join_all(futures)
            .await?
            .into_iter()
            .flatten()
            .collect();

        logging::info!("Successfully flush memtables to files: {:?}", metas);
        Ok(metas)
    }

    async fn write_manifest_and_apply(&mut self, file_metas: &[FileMeta]) -> Result<()> {
        let edit = RegionEdit {
            region_version: self.shared.version_control.metadata().version(),
            flushed_sequence: Some(self.flush_sequence),
            files_to_add: file_metas.to_vec(),
            files_to_remove: Vec::default(),
        };

        self.writer
            .write_edit_and_apply(
                &self.wal,
                &self.shared,
                &self.manifest,
                edit,
                Some(self.max_memtable_id),
            )
            .await?;
        self.wal.obsolete(self.flush_sequence).await
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
