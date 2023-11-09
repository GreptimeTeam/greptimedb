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

//! storage engine config

use std::time::Duration;

use common_base::readable_size::ReadableSize;

/// Default max flush tasks.
pub const DEFAULT_MAX_FLUSH_TASKS: usize = 8;
/// Default region write buffer size.
pub const DEFAULT_REGION_WRITE_BUFFER_SIZE: ReadableSize = ReadableSize::mb(32);
/// Default interval to trigger auto flush in millis.
pub const DEFAULT_AUTO_FLUSH_INTERVAL: u32 = 60 * 60 * 1000;
/// Default interval to schedule the picker to flush automatically in millis.
pub const DEFAULT_PICKER_SCHEDULE_INTERVAL: u32 = 5 * 60 * 1000;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub compress_manifest: bool,
    pub manifest_checkpoint_margin: Option<u16>,
    pub manifest_gc_duration: Option<Duration>,
    pub max_files_in_l0: usize,
    pub max_purge_tasks: usize,
    /// Max inflight flush tasks.
    pub max_flush_tasks: usize,
    /// Default write buffer size for a region.
    pub region_write_buffer_size: ReadableSize,
    /// Interval to schedule the auto flush picker.
    pub picker_schedule_interval: Duration,
    /// Interval to auto flush a region if it has not flushed yet.
    pub auto_flush_interval: Duration,
    /// Limit for global write buffer size. Disabled by default.
    pub global_write_buffer_size: Option<ReadableSize>,
    /// Global retention period for all regions.
    ///
    /// The precedence order is: region ttl > global ttl.
    pub global_ttl: Option<Duration>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            compress_manifest: false,
            manifest_checkpoint_margin: Some(10),
            manifest_gc_duration: Some(Duration::from_secs(30)),
            max_files_in_l0: 8,
            max_purge_tasks: 32,
            max_flush_tasks: DEFAULT_MAX_FLUSH_TASKS,
            region_write_buffer_size: DEFAULT_REGION_WRITE_BUFFER_SIZE,
            picker_schedule_interval: Duration::from_millis(
                DEFAULT_PICKER_SCHEDULE_INTERVAL.into(),
            ),
            auto_flush_interval: Duration::from_millis(DEFAULT_AUTO_FLUSH_INTERVAL.into()),
            global_write_buffer_size: None,
            global_ttl: None,
        }
    }
}
