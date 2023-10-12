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
    pub timeout_millis: Duration,
    #[serde(default = "default_heartbeat_timeout_millis")]
    pub heartbeat_timeout_millis: Duration,
    #[serde(default = "default_ddl_timeout_millis")]
    pub ddl_timeout_millis: Duration,
    pub connect_timeout_millis: Duration,
    pub tcp_nodelay: bool,
}

fn default_heartbeat_timeout_millis() -> Duration {
    Duration::from_millis(500u64)
}

fn default_ddl_timeout_millis() -> Duration {
    Duration::from_millis(10_000u64)
}

impl Default for MetaClientOptions {
    fn default() -> Self {
        Self {
            metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
            timeout_millis: Duration::from_millis(3_000u64),
            heartbeat_timeout_millis: default_heartbeat_timeout_millis(),
            ddl_timeout_millis: default_ddl_timeout_millis(),
            connect_timeout_millis: Duration::from_millis(1_000u64),
            tcp_nodelay: true,
        }
    }
}

#[cfg(test)]
mod mocks;
