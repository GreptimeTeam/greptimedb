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
use common_datasource::compression::CompressionType;
use common_telemetry::warn;
use serde::{Deserialize, Serialize};

/// Default region worker num.
const DEFAULT_NUM_WORKERS: usize = 1;
/// Default max running background job.
const DEFAULT_MAX_BG_JOB: usize = 4;

/// Configuration for [MitoEngine](crate::engine::MitoEngine).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MitoConfig {
    // Worker configs:
    /// Number of region workers (default 1).
    pub num_workers: usize,
    /// Request channel size of each worker (default 128).
    pub worker_channel_size: usize,
    /// Max batch size for a worker to handle requests (default 64).
    pub worker_request_batch_size: usize,

    // Manifest configs:
    /// Number of meta action updated to trigger a new checkpoint
    /// for the manifest (default 10).
    pub manifest_checkpoint_distance: u64,
    /// Manifest compression type (default uncompressed).
    pub manifest_compress_type: CompressionType,

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
    /// Total size for cache (default 512MB). Setting it to 0 to disable cache.
    pub cache_size: ReadableSize,
}

impl Default for MitoConfig {
    fn default() -> Self {
        MitoConfig {
            num_workers: DEFAULT_NUM_WORKERS,
            worker_channel_size: 128,
            worker_request_batch_size: 64,
            manifest_checkpoint_distance: 10,
            manifest_compress_type: CompressionType::Uncompressed,
            max_background_jobs: DEFAULT_MAX_BG_JOB,
            auto_flush_interval: Duration::from_secs(30 * 60),
            global_write_buffer_size: ReadableSize::gb(1),
            global_write_buffer_reject_size: ReadableSize::gb(2),
            cache_size: ReadableSize::mb(512),
        }
    }
}

impl MitoConfig {
    /// Sanitize incorrect configurations.
    pub(crate) fn sanitize(&mut self) {
        // Sanitize worker num.
        let num_workers_before = self.num_workers;
        if self.num_workers == 0 {
            self.num_workers = DEFAULT_NUM_WORKERS;
        }
        self.num_workers = self.num_workers.next_power_of_two();
        if num_workers_before != self.num_workers {
            warn!(
                "Sanitize worker num {} to {}",
                num_workers_before, self.num_workers
            );
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
    }
}
