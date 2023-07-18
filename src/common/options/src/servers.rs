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

/// Default http body limit (64M).
pub const DEFAULT_BODY_LIMIT: ReadableSize = ReadableSize::mb(64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpOptions {
    pub addr: String,

    #[serde(with = "humantime_serde")]
    pub timeout: Duration,

    #[serde(skip)]
    pub disable_dashboard: bool,

    pub body_limit: ReadableSize,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4000".to_string(),
            timeout: Duration::from_secs(30),
            disable_dashboard: false,
            body_limit: DEFAULT_BODY_LIMIT,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Standalone,
    Distributed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HeartbeatOptions {
    pub interval_millis: u64,
    pub retry_interval_millis: u64,
}

impl Default for HeartbeatOptions {
    fn default() -> Self {
        Self {
            interval_millis: 5000,
            retry_interval_millis: 5000,
        }
    }
}
