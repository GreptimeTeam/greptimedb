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

use serde::{Deserialize, Serialize};

pub mod client;
pub mod error;

// Options for meta client in datanode instance.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct MetaClientOptions {
    pub metasrv_addrs: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub heartbeat_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub ddl_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    pub tcp_nodelay: bool,
    pub metadata_cache_max_capacity: u64,
    #[serde(with = "humantime_serde")]
    pub metadata_cache_ttl: Duration,
    #[serde(with = "humantime_serde")]
    pub metadata_cache_tti: Duration,
}

impl Default for MetaClientOptions {
    fn default() -> Self {
        Self {
            metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
            timeout: Duration::from_millis(3_000u64),
            heartbeat_timeout: Duration::from_millis(500u64),
            ddl_timeout: Duration::from_millis(10_000u64),
            connect_timeout: Duration::from_millis(1_000u64),
            tcp_nodelay: true,
            metadata_cache_max_capacity: 100_000u64,
            metadata_cache_ttl: Duration::from_secs(600u64),
            metadata_cache_tti: Duration::from_secs(300u64),
        }
    }
}

#[cfg(test)]
mod mocks;
