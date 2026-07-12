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

use common_telemetry::warn;
use serde::{Deserialize, Serialize};

/// The default flush interval of the metadata region.
pub(crate) const DEFAULT_FLUSH_METADATA_REGION_INTERVAL: Duration = Duration::from_secs(30);

/// Configuration for the metric engine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EngineConfig {
    /// Whether to use sparse primary key encoding.
    #[serde(default = "EngineConfig::default_sparse_primary_key_encoding")]
    pub sparse_primary_key_encoding: bool,
    /// The flush interval of the metadata region.
    #[serde(
        with = "humantime_serde",
        default = "EngineConfig::default_flush_metadata_region_interval"
    )]
    pub flush_metadata_region_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            flush_metadata_region_interval: DEFAULT_FLUSH_METADATA_REGION_INTERVAL,
            sparse_primary_key_encoding: Self::default_sparse_primary_key_encoding(),
        }
    }
}

impl EngineConfig {
    fn default_flush_metadata_region_interval() -> Duration {
        DEFAULT_FLUSH_METADATA_REGION_INTERVAL
    }

    fn default_sparse_primary_key_encoding() -> bool {
        true
    }

    /// Sanitizes the configuration.
    pub fn sanitize(&mut self) {
        if self.flush_metadata_region_interval.is_zero() {
            warn!(
                "Flush metadata region interval is zero, override with default value: {:?}. Disable metadata region flush is forbidden.",
                DEFAULT_FLUSH_METADATA_REGION_INTERVAL
            );
            self.flush_metadata_region_interval = DEFAULT_FLUSH_METADATA_REGION_INTERVAL;
        }
    }
}
