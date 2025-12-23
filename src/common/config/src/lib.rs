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

pub mod config;
pub mod error;
pub mod file_watcher;

use std::time::Duration;

use common_base::readable_size::ReadableSize;
pub use config::*;
use serde::{Deserialize, Serialize};

pub fn metadata_store_dir(store_dir: &str) -> String {
    format!("{store_dir}/metadata")
}

/// The default data home directory.
pub const DEFAULT_DATA_HOME: &str = "./greptimedb_data";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KvBackendConfig {
    /// The size of the metadata store backend log file.
    pub file_size: ReadableSize,
    /// The threshold of the metadata store size to trigger a purge.
    pub purge_threshold: ReadableSize,
    /// The interval of the metadata store to trigger a purge.
    #[serde(with = "humantime_serde")]
    pub purge_interval: Duration,
    /// Optional path to a metadata snapshot file for restoration.
    ///
    /// This path is interpreted as relative to the data home directory.
    /// If not provided, no metadata will be restored from a snapshot.
    ///
    /// **This option is only used when the standalone is started for the first time.**
    pub init_metadata_path: Option<String>,
    /// Whether to ignore the error when restoring metadata from a snapshot.
    ///
    /// **This option is only used when the standalone is started for the first time.**
    pub ignore_metadata_snapshot_restore_error: bool,
}

impl Default for KvBackendConfig {
    fn default() -> Self {
        Self {
            // The log file size 64MB
            file_size: ReadableSize::mb(64),
            // The log purge threshold 256MB
            purge_threshold: ReadableSize::mb(256),
            // The log purge interval 1m
            purge_interval: Duration::from_secs(60),
            init_metadata_path: None,
            ignore_metadata_snapshot_restore_error: false,
        }
    }
}
