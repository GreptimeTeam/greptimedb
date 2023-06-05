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

mod picker;
mod scheduler;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common_telemetry::{logging, timer};
use metrics::counter;
pub use picker::{FlushPicker, PickerConfig};
pub use scheduler::{
    FlushHandle, FlushRegionRequest, FlushRequest, FlushScheduler, FlushSchedulerRef,
};
use store_api::logstore::LogStore;
use store_api::storage::consts::WRITE_ROW_GROUP_SIZE;
use store_api::storage::{RegionId, SequenceNumber};

use crate::config::EngineConfig;
use crate::error::Result;
use crate::manifest::action::*;
use crate::manifest::region::RegionManifest;
use crate::memtable::{IterContext, MemtableId, MemtableRef};
use crate::metrics::{FLUSH_BYTES_TOTAL, FLUSH_ELAPSED};
use crate::region::{RegionWriterRef, SharedDataRef};
use crate::sst::{AccessLayerRef, FileId, FileMeta, Source, SstInfo, WriteOptions};
use crate::wal::Wal;

/// Current flush-related status of a region.
#[derive(Debug, Clone, Copy)]
pub struct RegionStatus {
    /// Id of the region this status belongs to.
    pub region_id: RegionId,
    /// Size of the mutable memtable.
    pub bytes_mutable: usize,
    /// Write buffer size of the region.
    pub write_buffer_size: usize,
}

/// Type of flush request to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushType {
    /// Flush current region.
    Region,
    /// Engine level flush. Find regions to flush globally.
    Engine,
}

/// Strategy to control whether to flush a region before writing to the region.
pub trait FlushStrategy: Send + Sync + std::fmt::Debug {
    /// Returns whether to trigger a flush operation.
    fn should_flush(&self, status: RegionStatus) -> Option<FlushType>;

    /// Reserves `mem` bytes.
    fn reserve_mem(&self, mem: usize);

    /// Tells the strategy we are freeing `mem` bytes.
    ///
    /// We are in the process of freeing `mem` bytes, so it is not considered
    /// when checking the soft limit.
    fn schedule_free_mem(&self, mem: usize);

    /// We have freed `mem` bytes.
    fn free_mem(&self, mem: usize);
}

pub type FlushStrategyRef = Arc<dyn FlushStrategy>;

/// Flush strategy based on memory usage.
#[derive(Debug)]
pub struct SizeBasedStrategy {
    /// Write buffer size for all memtables.
    global_write_buffer_size: Option<usize>,
    /// Mutable memtable memory size limitation, only valid when `global_write_buffer_size`
    /// is `Some`.
    mutable_limitation: usize,
    /// Memory in used (e.g. used by mutable and immutable memtables).
    memory_used: AtomicUsize,
    /// Memory that hasn't been scheduled to free (e.g. used by mutable memtables).
    memory_active: AtomicUsize,
}

impl SizeBasedStrategy {
    /// Returns a new [SizeBasedStrategy] with specific `global_write_buffer_size`.
    pub fn new(global_write_buffer_size: Option<usize>) -> Self {
        Self {
            global_write_buffer_size,
            mutable_limitation: get_mutable_limitation(global_write_buffer_size),
            memory_used: AtomicUsize::new(0),
            memory_active: AtomicUsize::new(0),
        }
    }

    /// Returns whether to trigger an engine level flush.
    ///
    /// Inspired by RocksDB's WriteBufferManager.
    /// https://github.com/facebook/rocksdb/blob/main/include/rocksdb/write_buffer_manager.h#L94
    fn should_flush_engine(&self) -> bool {
        // We only check global limit when it is Some.
        let Some(global_write_buffer_size) = self.global_write_buffer_size else {
            return false;
        };

        let mutable_memtable_memory_usage = self.memory_active.load(Ordering::Relaxed);
        if mutable_memtable_memory_usage > self.mutable_limitation {
            logging::info!(
                "Engine should flush (over mutable limit), mutable_usage: {}, mutable_limitation: {}.",
                mutable_memtable_memory_usage,
                self.mutable_limitation,
            );
            return true;
        }

        let memory_usage = self.memory_used.load(Ordering::Relaxed);
        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        if memory_usage >= global_write_buffer_size
            && mutable_memtable_memory_usage >= global_write_buffer_size / 2
        {
            logging::info!(
                "Engine should flush (over total limit), memory_usage: {}, global_write_buffer_size: {}, \
                 mutable_usage: {}.",
                memory_usage,
                global_write_buffer_size,
                mutable_memtable_memory_usage,
            );
            return true;
        }

        false
    }

    /// Returns true if the global memory limitation is enabled.
    #[inline]
    fn is_global_limit_enabled(&self) -> bool {
        self.global_write_buffer_size.is_some()
    }
}

#[inline]
fn get_mutable_limitation(global_write_buffer_size: Option<usize>) -> usize {
    // Inspired by RocksDB.
    // https://github.com/facebook/rocksdb/blob/main/include/rocksdb/write_buffer_manager.h#L86
    global_write_buffer_size
        .map(|size| size * 7 / 8)
        .unwrap_or(0)
}

impl Default for SizeBasedStrategy {
    fn default() -> Self {
        Self {
            global_write_buffer_size: None,
            mutable_limitation: 0,
            memory_used: AtomicUsize::new(0),
            memory_active: AtomicUsize::new(0),
        }
    }
}

impl FlushStrategy for SizeBasedStrategy {
    fn should_flush(&self, status: RegionStatus) -> Option<FlushType> {
        if status.bytes_mutable >= status.write_buffer_size {
            // If the mutable memtable is full, we should freeze it and flush it.
            logging::debug!(
                "Region should flush as mutable memtable is full, region: {}, bytes_mutable: {}, \
                write_buffer_size: {}.",
                status.region_id,
                status.bytes_mutable,
                status.write_buffer_size,
            );

            return Some(FlushType::Region);
        }

        if self.should_flush_engine() {
            return Some(FlushType::Engine);
        }

        None
    }

    fn reserve_mem(&self, mem: usize) {
        if self.is_global_limit_enabled() {
            self.memory_used.fetch_add(mem, Ordering::Relaxed);
            self.memory_active.fetch_add(mem, Ordering::Relaxed);
        }
    }

    fn schedule_free_mem(&self, mem: usize) {
        if self.is_global_limit_enabled() {
            self.memory_active.fetch_sub(mem, Ordering::Relaxed);
        }
    }

    fn free_mem(&self, mem: usize) {
        if self.is_global_limit_enabled() {
            self.memory_used.fetch_sub(mem, Ordering::Relaxed);
        }
    }
}

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
    /// Storage engine config
    pub engine_config: Arc<EngineConfig>,
}

impl<S: LogStore> FlushJob<S> {
    /// Execute the flush job.
    async fn run(&mut self) -> Result<()> {
        let _timer = timer!(FLUSH_ELAPSED);

        let file_metas = self.write_memtables_to_layer().await?;
        if file_metas.is_empty() {
            // skip writing manifest and wal if no files are flushed.
            return Ok(());
        }
        self.write_manifest_and_apply(&file_metas).await?;

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

        let metas: Vec<_> = futures_util::future::try_join_all(futures)
            .await?
            .into_iter()
            .flatten()
            .collect();

        let flush_bytes = metas.iter().map(|f| f.file_size).sum();
        counter!(FLUSH_BYTES_TOTAL, flush_bytes);

        let file_ids = metas.iter().map(|f| f.file_id).collect::<Vec<_>>();
        logging::info!("Successfully flush memtables, region:{region_id}, files: {file_ids:?}");
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
    use crate::memtable::AllocTracker;

    #[test]
    fn test_get_mutable_limitation() {
        assert_eq!(7, get_mutable_limitation(Some(8)));
        assert_eq!(8, get_mutable_limitation(Some(10)));
        assert_eq!(56, get_mutable_limitation(Some(64)));
        assert_eq!(0, get_mutable_limitation(None));
    }

    #[test]
    fn test_strategy_global_disabled() {
        let strategy = SizeBasedStrategy::new(None);
        strategy.reserve_mem(1000);
        assert_eq!(0, strategy.memory_used.load(Ordering::Relaxed));
        assert_eq!(0, strategy.memory_active.load(Ordering::Relaxed));
        strategy.schedule_free_mem(1000);
        assert_eq!(0, strategy.memory_used.load(Ordering::Relaxed));
        assert_eq!(0, strategy.memory_active.load(Ordering::Relaxed));
        strategy.free_mem(1000);
        assert_eq!(0, strategy.memory_used.load(Ordering::Relaxed));
        assert_eq!(0, strategy.memory_active.load(Ordering::Relaxed));

        let status = RegionStatus {
            region_id: 1,
            bytes_mutable: 400,
            write_buffer_size: 300,
        };
        assert_eq!(Some(FlushType::Region), strategy.should_flush(status));
        let status = RegionStatus {
            region_id: 1,
            bytes_mutable: 100,
            write_buffer_size: 300,
        };
        assert_eq!(None, strategy.should_flush(status));
    }

    #[test]
    fn test_strategy_over_mutable_limit() {
        let strategy = SizeBasedStrategy::new(Some(1000));
        strategy.reserve_mem(500);
        let status = RegionStatus {
            region_id: 1,
            bytes_mutable: 300,
            write_buffer_size: 500,
        };
        assert_eq!(None, strategy.should_flush(status));
        strategy.reserve_mem(400);

        // Flush region.
        let status = RegionStatus {
            region_id: 1,
            bytes_mutable: 400,
            write_buffer_size: 300,
        };
        assert_eq!(Some(FlushType::Region), strategy.should_flush(status));

        // More than mutable limitation, Flush global.
        let status = RegionStatus {
            region_id: 1,
            bytes_mutable: 100,
            write_buffer_size: 300,
        };
        assert_eq!(Some(FlushType::Engine), strategy.should_flush(status));

        strategy.schedule_free_mem(500);
        assert_eq!(None, strategy.should_flush(status));
        assert_eq!(900, strategy.memory_used.load(Ordering::Relaxed));
        assert_eq!(400, strategy.memory_active.load(Ordering::Relaxed));

        strategy.free_mem(500);
        assert_eq!(400, strategy.memory_used.load(Ordering::Relaxed));
        assert_eq!(400, strategy.memory_active.load(Ordering::Relaxed));
    }

    #[test]
    fn test_strategy_over_global() {
        common_telemetry::init_default_ut_logging();

        let strategy = SizeBasedStrategy::new(Some(1000));
        strategy.reserve_mem(1100);
        strategy.schedule_free_mem(200);
        // More than global limit.
        let status = RegionStatus {
            region_id: 1,
            bytes_mutable: 100,
            write_buffer_size: 300,
        };
        assert_eq!(Some(FlushType::Engine), strategy.should_flush(status));

        // More than global limit, but mutable not enough (< 500).
        strategy.schedule_free_mem(450);
        let status = RegionStatus {
            region_id: 1,
            bytes_mutable: 100,
            write_buffer_size: 300,
        };
        assert_eq!(None, strategy.should_flush(status));
        strategy.schedule_free_mem(100);
        assert_eq!(None, strategy.should_flush(status));

        // Now mutable is enough.
        strategy.reserve_mem(150);
        // We can flush again.
        assert_eq!(Some(FlushType::Engine), strategy.should_flush(status));
        strategy.reserve_mem(100);
        assert_eq!(Some(FlushType::Engine), strategy.should_flush(status));
    }

    #[test]
    fn test_alloc_tracker_without_strategy() {
        let tracker = AllocTracker::new(None);
        assert_eq!(0, tracker.bytes_allocated());
        tracker.on_allocate(100);
        assert_eq!(100, tracker.bytes_allocated());
        tracker.on_allocate(200);
        assert_eq!(300, tracker.bytes_allocated());

        tracker.done_allocating();
        assert_eq!(300, tracker.bytes_allocated());
    }

    #[test]
    fn test_alloc_tracker_with_strategy() {
        let strategy = Arc::new(SizeBasedStrategy::new(Some(1000)));
        {
            let tracker = AllocTracker::new(Some(strategy.clone() as FlushStrategyRef));

            tracker.on_allocate(100);
            assert_eq!(100, tracker.bytes_allocated());
            assert_eq!(100, strategy.memory_used.load(Ordering::Relaxed));
            assert_eq!(100, strategy.memory_active.load(Ordering::Relaxed));

            for _ in 0..2 {
                // Done allocating won't free the same memory multiple times.
                tracker.done_allocating();
                assert_eq!(100, strategy.memory_used.load(Ordering::Relaxed));
                assert_eq!(0, strategy.memory_active.load(Ordering::Relaxed));
            }
        }

        assert_eq!(0, strategy.memory_used.load(Ordering::Relaxed));
        assert_eq!(0, strategy.memory_active.load(Ordering::Relaxed));
    }

    #[test]
    fn test_alloc_tracker_without_done_allocating() {
        let strategy = Arc::new(SizeBasedStrategy::new(Some(1000)));
        {
            let tracker = AllocTracker::new(Some(strategy.clone() as FlushStrategyRef));

            tracker.on_allocate(100);
            assert_eq!(100, tracker.bytes_allocated());
            assert_eq!(100, strategy.memory_used.load(Ordering::Relaxed));
            assert_eq!(100, strategy.memory_active.load(Ordering::Relaxed));
        }

        assert_eq!(0, strategy.memory_used.load(Ordering::Relaxed));
        assert_eq!(0, strategy.memory_active.load(Ordering::Relaxed));
    }
}
