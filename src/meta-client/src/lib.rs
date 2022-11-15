// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

pub mod client;
pub mod error;
#[cfg(test)]
mod mocks;
pub mod rpc;

// Options for meta client in datanode instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetaClientOpts {
    pub metasrv_addr: String,
    pub timeout_millis: u64,
    pub connect_timeout_millis: u64,
    pub tcp_nodelay: bool,
}

impl Default for MetaClientOpts {
    fn default() -> Self {
        Self {
            metasrv_addr: "127.0.0.1:3002".to_string(),
            timeout_millis: 3_000u64,
            connect_timeout_millis: 5_000u64,
            tcp_nodelay: true,
        }
    }
}
