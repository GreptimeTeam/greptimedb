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

use common_meta::distributed_time_constants;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct HeartbeatOptions {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(with = "humantime_serde")]
    pub retry_interval: Duration,
}

impl HeartbeatOptions {
    pub fn datanode_default() -> Self {
        Default::default()
    }

    pub fn frontend_default() -> Self {
        Self {
            // Frontend can send heartbeat with a longer interval.
            interval: Duration::from_millis(
                distributed_time_constants::FRONTEND_HEARTBEAT_INTERVAL_MILLIS,
            ),
            retry_interval: Duration::from_millis(
                distributed_time_constants::HEARTBEAT_INTERVAL_MILLIS,
            ),
        }
    }
}

impl Default for HeartbeatOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(distributed_time_constants::HEARTBEAT_INTERVAL_MILLIS),
            retry_interval: Duration::from_millis(
                distributed_time_constants::HEARTBEAT_INTERVAL_MILLIS,
            ),
        }
    }
}
