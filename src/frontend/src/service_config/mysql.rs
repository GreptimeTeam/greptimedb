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
use servers::tls::TlsOption;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MysqlOptions {
    pub enable: bool,
    pub addr: String,
    pub runtime_size: usize,
    #[serde(default = "Default::default")]
    pub tls: TlsOption,
    pub reject_no_database: Option<bool>,
}

impl Default for MysqlOptions {
    fn default() -> Self {
        Self {
            enable: true,
            addr: "127.0.0.1:4002".to_string(),
            runtime_size: 2,
            tls: TlsOption::default(),
            reject_no_database: None,
        }
    }
}
