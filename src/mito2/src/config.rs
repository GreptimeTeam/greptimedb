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

use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_telemetry::warn;
use serde::{Deserialize, Serialize};

/// Default max running background job.
const DEFAULT_MAX_BG_JOB: usize = 4;

const MULTIPART_UPLOAD_MINIMUM_SIZE: ReadableSize = ReadableSize::mb(5);
/// Default channel size for parallel scan task.
const DEFAULT_SCAN_CHANNEL_SIZE: usize = 32;

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
}

impl Default for MitoConfig {
    fn default() -> Self {
        MitoConfig {
            num_workers: divide_num_cpus(2),
            worker_channel_size: 128,
            worker_request_batch_size: 64,
            manifest_checkpoint_distance: 10,
            compress_manifest: false,
            max_background_jobs: DEFAULT_MAX_BG_JOB,
            auto_flush_interval: Duration::from_secs(30 * 60),
            global_write_buffer_size: ReadableSize::gb(1),
            global_write_buffer_reject_size: ReadableSize::gb(2),
            sst_meta_cache_size: ReadableSize::mb(128),
            vector_cache_size: ReadableSize::mb(512),
            page_cache_size: ReadableSize::mb(512),
            sst_write_buffer_size: ReadableSize::mb(8),
            scan_parallelism: divide_num_cpus(4),
            parallel_scan_channel_size: DEFAULT_SCAN_CHANNEL_SIZE,
        }
    }
}

impl MitoConfig {
    /// Sanitize incorrect configurations.
    pub(crate) fn sanitize(&mut self) {
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
    }
}

/// Divide cpu num by a non-zero `divisor` and returns at least 1.
fn divide_num_cpus(divisor: usize) -> usize {
    debug_assert!(divisor > 0);
    let cores = num_cpus::get();
    debug_assert!(cores > 0);

    (cores + divisor - 1) / divisor
}
