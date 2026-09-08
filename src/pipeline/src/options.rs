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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PipelineOptions {
    /// Time to live of the frontend-local pipeline cache. A pipeline created or
    /// deleted on another frontend takes effect on this one after at most this
    /// duration. Default: "10s".
    #[serde(with = "humantime_serde")]
    pub cache_ttl: Duration,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            cache_ttl: Duration::from_secs(10),
        }
    }
}
