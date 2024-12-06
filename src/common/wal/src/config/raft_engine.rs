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

use std::time::Duration;

use common_base::readable_size::ReadableSize;
use serde::{Deserialize, Serialize};

/// Configurations for raft-engine wal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RaftEngineConfig {
    /// Wal directory
    pub dir: Option<String>,
    /// Wal file size in bytes
    pub file_size: ReadableSize,
    /// Wal purge threshold in bytes
    pub purge_threshold: ReadableSize,
    /// Purge interval in seconds
    #[serde(with = "humantime_serde")]
    pub purge_interval: Duration,
    /// Read batch size
    pub read_batch_size: usize,
    /// Whether to sync log file after every write
    pub sync_write: bool,
    /// Whether to reuse logically truncated log files.
    pub enable_log_recycle: bool,
    /// Whether to pre-create log files on start up
    pub prefill_log_files: bool,
    /// Duration for fsyncing log files.
    #[serde(with = "humantime_serde")]
    pub sync_period: Option<Duration>,
    /// Parallelism during log recovery.
    pub recovery_parallelism: usize,
}

impl Default for RaftEngineConfig {
    fn default() -> Self {
        Self {
            dir: None,
            file_size: ReadableSize::mb(256),
            purge_threshold: ReadableSize::gb(1),
            purge_interval: Duration::from_secs(60),
            read_batch_size: 128,
            sync_write: false,
            enable_log_recycle: true,
            prefill_log_files: false,
            sync_period: None,
            recovery_parallelism: num_cpus::get(),
        }
    }
}
