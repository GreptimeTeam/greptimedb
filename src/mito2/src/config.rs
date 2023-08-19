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

use common_base::readable_size::ReadableSize;
use common_datasource::compression::CompressionType;
use common_telemetry::warn;

/// Default region worker num.
const DEFAULT_NUM_WORKERS: usize = 1;
/// Default region write buffer size.
pub(crate) const DEFAULT_WRITE_BUFFER_SIZE: ReadableSize = ReadableSize::mb(32);

/// Configuration for [MitoEngine](crate::engine::MitoEngine).
#[derive(Debug)]
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
}

impl Default for MitoConfig {
    fn default() -> Self {
        MitoConfig {
            num_workers: DEFAULT_NUM_WORKERS,
            worker_channel_size: 128,
            worker_request_batch_size: 64,
            manifest_checkpoint_distance: 10,
            manifest_compress_type: CompressionType::Uncompressed,
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
    }
}
