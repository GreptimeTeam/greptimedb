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
pub struct MetaClientOptions {
    pub metasrv_addrs: Vec<String>,
    #[serde(default = "default_timeout")]
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(default = "default_heartbeat_timeout")]
    #[serde(with = "humantime_serde")]
    pub heartbeat_timeout: Duration,
    #[serde(default = "default_ddl_timeout")]
    #[serde(with = "humantime_serde")]
    pub ddl_timeout: Duration,
    #[serde(default = "default_connect_timeout")]
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    pub tcp_nodelay: bool,
}

fn default_heartbeat_timeout() -> Duration {
    Duration::from_millis(500u64)
}

fn default_ddl_timeout() -> Duration {
    Duration::from_millis(10_000u64)
}

fn default_connect_timeout() -> Duration {
    Duration::from_millis(1_000u64)
}

fn default_timeout() -> Duration {
    Duration::from_millis(3_000u64)
}

impl Default for MetaClientOptions {
    fn default() -> Self {
        Self {
            metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
            timeout: default_timeout(),
            heartbeat_timeout: default_heartbeat_timeout(),
            ddl_timeout: default_ddl_timeout(),
            connect_timeout: default_connect_timeout(),
            tcp_nodelay: true,
        }
    }
}

#[cfg(test)]
mod mocks;
