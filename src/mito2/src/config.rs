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

//! Configurations.

use std::cmp;
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_telemetry::warn;
use lazy_static::lazy_static;
use object_store::util::join_dir;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, NoneAsEmptyString};
use sysinfo::System;

use crate::error::Result;
use crate::sst::DEFAULT_WRITE_BUFFER_SIZE;

/// Default max running background job.
const DEFAULT_MAX_BG_JOB: usize = 4;

const MULTIPART_UPLOAD_MINIMUM_SIZE: ReadableSize = ReadableSize::mb(5);
/// Default channel size for parallel scan task.
const DEFAULT_SCAN_CHANNEL_SIZE: usize = 32;

lazy_static! {
    /// Number of cpu cores.
    pub static ref CPU_CORES: usize = {
        let mut sys_info = System::new();
        sys_info.refresh_cpu();
        // At least 1 cpu core.
        sys_info.cpus().len().max(1)
    };

    /// Total memory size of the OS.
    pub static ref SYS_TOTAL_MEMORY: ReadableSize = {
        let mut sys_info = System::new();
        sys_info.refresh_memory();
        ReadableSize(sys_info.total_memory())
    };
}

/// Configuration for [MitoEngine](crate::engine::MitoEngine).
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
pub struct MitoConfig {
    // Worker configs:
    /// Number of region workers (default: 1/2 of cpu cores).
    /// Sets to 0 to use the default value.
    pub num_workers: usize,
    /// Request channel size of each worker (default 128).
    pub worker_channel_size: usize,
    /// Max batch size for a worker to handle requests (default 64).
    pub worker_request_batch_size: usize,

    // Manifest configs:
    /// Number of meta action updated to trigger a new checkpoint
    /// for the manifest (default 10).
    pub manifest_checkpoint_distance: u64,
    /// Whether to compress manifest and checkpoint file by gzip (default false).
    pub compress_manifest: bool,

    // Background job configs:
    /// Max number of running background jobs (default 4).
    pub max_background_jobs: usize,

    // Flush configs:
    /// Interval to auto flush a region if it has not flushed yet (default 30 min).
    #[serde(with = "humantime_serde")]
    pub auto_flush_interval: Duration,
    /// Global write buffer size threshold to trigger flush (default 1G).
    pub global_write_buffer_size: ReadableSize,
    /// Global write buffer size threshold to reject write requests (default 2G).
    pub global_write_buffer_reject_size: ReadableSize,

    // Cache configs:
    /// Cache size for SST metadata (default 128MB). Setting it to 0 to disable the cache.
    pub sst_meta_cache_size: ReadableSize,
    /// Cache size for vectors and arrow arrays (default 512MB). Setting it to 0 to disable the cache.
    pub vector_cache_size: ReadableSize,
    /// Cache size for pages of SST row groups (default 512MB). Setting it to 0 to disable the cache.
    pub page_cache_size: ReadableSize,
    /// Whether to enable the experimental write cache.
    pub enable_experimental_write_cache: bool,
    /// File system path for write cache, defaults to `{data_home}/write_cache`.
    pub experimental_write_cache_path: String,
    /// Capacity for write cache.
    pub experimental_write_cache_size: ReadableSize,

    // Other configs:
    /// Buffer size for SST writing.
    pub sst_write_buffer_size: ReadableSize,
    /// Parallelism to scan a region (default: 1/4 of cpu cores).
    /// - 0: using the default value (1/4 of cpu cores).
    /// - 1: scan in current thread.
    /// - n: scan in parallelism n.
    pub scan_parallelism: usize,
    /// Capacity of the channel to send data from parallel scan tasks to the main task (default 32).
    pub parallel_scan_channel_size: usize,
    /// Whether to allow stale entries read during replay.
    pub allow_stale_entries: bool,

    /// Inverted index configs.
    pub inverted_index: InvertedIndexConfig,
}

impl Default for MitoConfig {
    fn default() -> Self {
        // Use 1/2 of cpu cores' number as workers' number.
        let num_workers = divide_num_cpus(2);
        // OS total memory size.
        let sys_memory = *SYS_TOTAL_MEMORY;
        // Use 1/64 of OS memory as global write buffer size, it shouldn't be greater than 1G in default mode.
        // For example, if the OS memory size is 16GB, the global write buffer size is 256MB.
        let global_write_buffer_size = cmp::min(sys_memory / 64, ReadableSize::gb(1));
        // Use 2x of global write buffer size as global write buffer reject size.
        let global_write_buffer_reject_size = global_write_buffer_size * 2;
        // Use 1/256 of OS memory size as SST meta cache size, it shouldn't be greater than 128MB in default mode.
        let sst_meta_cache_size = cmp::min(sys_memory / 256, ReadableSize::mb(128));
        // Use 1/128 of OS memory size as mem cache size, it shouldn't be greater than 512MB in default mode.
        let mem_cache_size = cmp::min(sys_memory / 128, ReadableSize::mb(512));
        // Use 1/128 of OS memory size as disk cache size, it shouldn't be greater than 512MB in default mode.
        let disk_cache_size = cmp::min(sys_memory / 128, ReadableSize::mb(512));
        // Use 1/4 of cpu cores' number as parallel number.
        let scan_parallelism = divide_num_cpus(4);

        MitoConfig {
            num_workers,
            worker_channel_size: 128,
            worker_request_batch_size: 64,
            manifest_checkpoint_distance: 10,
            compress_manifest: false,
            max_background_jobs: DEFAULT_MAX_BG_JOB,
            auto_flush_interval: Duration::from_secs(30 * 60),
            global_write_buffer_size,
            global_write_buffer_reject_size,
            sst_meta_cache_size,
            vector_cache_size: mem_cache_size,
            page_cache_size: mem_cache_size,
            enable_experimental_write_cache: false,
            experimental_write_cache_path: String::new(),
            experimental_write_cache_size: disk_cache_size,
            sst_write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            scan_parallelism,
            parallel_scan_channel_size: DEFAULT_SCAN_CHANNEL_SIZE,
            allow_stale_entries: false,
            inverted_index: InvertedIndexConfig::default(),
        }
    }
}

impl MitoConfig {
    /// Sanitize incorrect configurations.
    ///
    /// Returns an error if there is a configuration that unable to sanitize.
    pub(crate) fn sanitize(&mut self, data_home: &str) -> Result<()> {
        // Use default value if `num_workers` is 0.
        if self.num_workers == 0 {
            self.num_workers = divide_num_cpus(2);
        }

        // Sanitize channel size.
        if self.worker_channel_size == 0 {
            warn!("Sanitize channel size 0 to 1");
            self.worker_channel_size = 1;
        }

        if self.max_background_jobs == 0 {
            warn!("Sanitize max background jobs 0 to {}", DEFAULT_MAX_BG_JOB);
            self.max_background_jobs = DEFAULT_MAX_BG_JOB;
        }

        if self.global_write_buffer_reject_size <= self.global_write_buffer_size {
            self.global_write_buffer_reject_size = self.global_write_buffer_size * 2;
            warn!(
                "Sanitize global write buffer reject size to {}",
                self.global_write_buffer_reject_size
            );
        }

        if self.sst_write_buffer_size < MULTIPART_UPLOAD_MINIMUM_SIZE {
            self.sst_write_buffer_size = MULTIPART_UPLOAD_MINIMUM_SIZE;
            warn!(
                "Sanitize sst write buffer size to {}",
                self.sst_write_buffer_size
            );
        }

        // Use default value if `scan_parallelism` is 0.
        if self.scan_parallelism == 0 {
            self.scan_parallelism = divide_num_cpus(4);
        }

        if self.parallel_scan_channel_size < 1 {
            self.parallel_scan_channel_size = DEFAULT_SCAN_CHANNEL_SIZE;
            warn!(
                "Sanitize scan channel size to {}",
                self.parallel_scan_channel_size
            );
        }

        // Sets write cache path if it is empty.
        if self.experimental_write_cache_path.is_empty() {
            self.experimental_write_cache_path = join_dir(data_home, "write_cache");
        }

        self.inverted_index.sanitize(data_home)?;

        Ok(())
    }

    /// Enable experimental write cache.
    #[cfg(test)]
    pub fn enable_write_cache(mut self, path: String, size: ReadableSize) -> Self {
        self.enable_experimental_write_cache = true;
        self.experimental_write_cache_path = path;
        self.experimental_write_cache_size = size;
        self
    }
}

/// Operational mode for certain actions.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// The action is performed automatically based on internal criteria.
    #[default]
    Auto,
    /// The action is explicitly disabled.
    Disable,
}

impl Mode {
    /// Whether the action is disabled.
    pub fn disabled(&self) -> bool {
        matches!(self, Mode::Disable)
    }

    /// Whether the action is automatic.
    pub fn auto(&self) -> bool {
        matches!(self, Mode::Auto)
    }
}

/// Configuration options for the inverted index.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
pub struct InvertedIndexConfig {
    /// Whether to create the index on flush: automatically or never.
    pub create_on_flush: Mode,
    /// Whether to create the index on compaction: automatically or never.
    pub create_on_compaction: Mode,
    /// Whether to apply the index on query: automatically or never.
    pub apply_on_query: Mode,
    /// Memory threshold for performing an external sort during index creation.
    /// `None` means all sorting will happen in memory.
    #[serde_as(as = "NoneAsEmptyString")]
    pub mem_threshold_on_create: Option<ReadableSize>,
    /// File system path to store intermediate files for external sort, defaults to `{data_home}/index_intermediate`.
    pub intermediate_path: String,
}

impl Default for InvertedIndexConfig {
    fn default() -> Self {
        Self {
            create_on_flush: Mode::Auto,
            create_on_compaction: Mode::Auto,
            apply_on_query: Mode::Auto,
            mem_threshold_on_create: Some(ReadableSize::mb(64)),
            intermediate_path: String::new(),
        }
    }
}

impl InvertedIndexConfig {
    pub fn sanitize(&mut self, data_home: &str) -> Result<()> {
        if self.intermediate_path.is_empty() {
            self.intermediate_path = join_dir(data_home, "index_intermediate");
        }

        Ok(())
    }
}

/// Divide cpu num by a non-zero `divisor` and returns at least 1.
fn divide_num_cpus(divisor: usize) -> usize {
    debug_assert!(divisor > 0);
    let cores = *CPU_CORES;
    debug_assert!(cores > 0);

    (cores + divisor - 1) / divisor
}
