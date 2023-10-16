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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct WalConfig {
    // wal directory
    pub dir: Option<String>,
    // wal file size in bytes
    pub file_size: ReadableSize,
    // wal purge threshold in bytes
    pub purge_threshold: ReadableSize,
    // purge interval in seconds
    #[serde(with = "humantime_serde")]
    pub purge_interval: Duration,
    // read batch size
    pub read_batch_size: usize,
    // whether to sync log file after every write
    pub sync_write: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: None,
            file_size: ReadableSize::mb(256), // log file size 256MB
            purge_threshold: ReadableSize::gb(4), // purge threshold 4GB
            purge_interval: Duration::from_secs(600),
            read_batch_size: 128,
            sync_write: false,
        }
    }
}

pub fn metadata_store_dir(store_dir: &str) -> String {
    format!("{store_dir}/metadata")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KvStoreConfig {
    // Kv file size in bytes
    pub file_size: ReadableSize,
    // Kv purge threshold in bytes
    pub purge_threshold: ReadableSize,
}

impl Default for KvStoreConfig {
    fn default() -> Self {
        Self {
            // log file size 256MB
            file_size: ReadableSize::mb(256),
            // purge threshold 4GB
            purge_threshold: ReadableSize::gb(4),
        }
    }
}
