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
pub struct PromStoreOptions {
    pub enable: bool,
    pub with_metric_engine: bool,
    #[serde(default, with = "humantime_serde")]
    pub pending_rows_flush_interval: Duration,
    #[serde(default = "default_max_batch_rows")]
    pub max_batch_rows: usize,
    #[serde(default = "default_max_concurrent_flushes")]
    pub max_concurrent_flushes: usize,
    #[serde(default = "default_worker_channel_capacity")]
    pub worker_channel_capacity: usize,
}

fn default_max_batch_rows() -> usize {
    100_000
}

fn default_max_concurrent_flushes() -> usize {
    256
}

fn default_worker_channel_capacity() -> usize {
    65526
}

impl Default for PromStoreOptions {
    fn default() -> Self {
        Self {
            enable: true,
            with_metric_engine: true,
            pending_rows_flush_interval: Duration::from_secs(2),
            max_batch_rows: default_max_batch_rows(),
            max_concurrent_flushes: default_max_concurrent_flushes(),
            worker_channel_capacity: default_worker_channel_capacity(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::PromStoreOptions;
    use crate::service_config::prom_store::{
        default_max_batch_rows, default_max_concurrent_flushes, default_worker_channel_capacity,
    };

    #[test]
    fn test_prom_store_options() {
        let default = PromStoreOptions::default();
        assert!(default.enable);
        assert!(default.with_metric_engine);
        assert_eq!(default.pending_rows_flush_interval, Duration::from_secs(2));
        assert_eq!(default.max_batch_rows, default_max_batch_rows());
        assert_eq!(
            default.max_concurrent_flushes,
            default_max_concurrent_flushes()
        );
        assert_eq!(
            default.worker_channel_capacity,
            default_worker_channel_capacity()
        );
    }
}
