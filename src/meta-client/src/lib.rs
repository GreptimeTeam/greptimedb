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

use serde::{Deserialize, Serialize};

pub mod client;
pub mod error;

// Options for meta client in datanode instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetaClientOptions {
    pub metasrv_addrs: Vec<String>,
    pub timeout_millis: u64,
    #[serde(default = "default_ddl_timeout_millis")]
    pub ddl_timeout_millis: u64,
    pub connect_timeout_millis: u64,
    pub tcp_nodelay: bool,
}

fn default_ddl_timeout_millis() -> u64 {
    10_000u64
}

impl Default for MetaClientOptions {
    fn default() -> Self {
        Self {
            metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
            timeout_millis: 3_000u64,
            ddl_timeout_millis: default_ddl_timeout_millis(),
            connect_timeout_millis: 5_000u64,
            tcp_nodelay: true,
        }
    }
}

#[cfg(test)]
mod mocks;
