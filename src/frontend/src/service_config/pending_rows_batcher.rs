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

use std::num::NonZeroUsize;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use servers::http::BatchingProtocol;

/// Experimental table write batching options shared by HTTP ingestion protocols.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PendingRowsBatcherOptions {
    /// HTTP write protocols sharing this batcher; empty disables all entrances.
    pub protocols: Vec<BatchingProtocol>,
    /// Time from the first pending submission to a timed flush. Zero disables batching.
    #[serde(with = "humantime_serde")]
    pub pending_rows_flush_interval: Duration,
    /// Row threshold checked after appending a complete submission.
    pub max_batch_rows: usize,
    /// Maximum concurrent flushes shared by the frontend batcher.
    pub max_concurrent_flushes: usize,
    /// Maximum number of queued submissions per table worker.
    pub worker_channel_capacity: usize,
    /// Maximum number of original requests awaiting completion.
    pub max_inflight_requests: usize,
    /// Maximum number of table Flow notifications waiting in the shared queue.
    pub flow_notification_queue_capacity: NonZeroUsize,
}

impl PendingRowsBatcherOptions {
    /// Returns whether at least one protocol opts in and batching controls are nonzero.
    pub fn pending_rows_batching_enabled(&self) -> bool {
        !self.protocols.is_empty()
            && !self.pending_rows_flush_interval.is_zero()
            && self.max_batch_rows > 0
            && self.max_concurrent_flushes > 0
            && self.worker_channel_capacity > 0
            && self.max_inflight_requests > 0
    }
}

impl Default for PendingRowsBatcherOptions {
    fn default() -> Self {
        Self {
            protocols: Vec::new(),
            pending_rows_flush_interval: Duration::ZERO,
            max_batch_rows: 100_000,
            max_concurrent_flushes: 256,
            worker_channel_capacity: 65526,
            max_inflight_requests: 3000,
            flow_notification_queue_capacity: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::service_config::pending_rows_batcher::*;

    #[test]
    fn test_protocols() {
        let options: PendingRowsBatcherOptions = toml::from_str(
            "protocols = ['influxdb', 'opentsdb', 'otlp', 'logs', 'loki', 'splunk', 'elasticsearch', 'http_sql', 'prom']",
        ).unwrap();
        assert_eq!(options.protocols.len(), 9);
        assert!(options.protocols.contains(&BatchingProtocol::HttpSql));
        assert!(PendingRowsBatcherOptions::default().protocols.is_empty());
        for invalid in ["sql", "jaeger", "unknown"] {
            assert!(
                toml::from_str::<PendingRowsBatcherOptions>(&format!("protocols = ['{invalid}']"))
                    .is_err()
            );
        }
    }

    #[test]
    fn test_notification_capacity() {
        let default = PendingRowsBatcherOptions::default();
        assert_eq!(default.flow_notification_queue_capacity.get(), 1024);
        let configured: PendingRowsBatcherOptions =
            toml::from_str("flow_notification_queue_capacity = 8").unwrap();
        assert_eq!(configured.flow_notification_queue_capacity.get(), 8);
        assert!(
            toml::from_str::<PendingRowsBatcherOptions>("flow_notification_queue_capacity = 0")
                .is_err()
        );
    }

    #[test]
    fn test_defaults_and_roundtrip() {
        let options: PendingRowsBatcherOptions = toml::from_str("").unwrap();
        assert_eq!(options, PendingRowsBatcherOptions::default());
        assert!(!options.pending_rows_batching_enabled());
        assert_eq!(options.max_batch_rows, 100_000);
        assert_eq!(options.max_concurrent_flushes, 256);
        assert_eq!(options.worker_channel_capacity, 65526);
        assert_eq!(options.max_inflight_requests, 3000);
        let serialized = toml::to_string(&options).unwrap();
        assert_eq!(
            options,
            toml::from_str::<PendingRowsBatcherOptions>(&serialized).unwrap()
        );
    }

    #[test]
    fn test_partial_options_and_zero_controls() {
        let options: PendingRowsBatcherOptions =
            toml::from_str("pending_rows_flush_interval = '5ms'").unwrap();
        assert_eq!(
            options.pending_rows_flush_interval,
            Duration::from_millis(5)
        );
        assert!(!options.pending_rows_batching_enabled());
        let enabled: PendingRowsBatcherOptions =
            toml::from_str("protocols = ['http_sql']\npending_rows_flush_interval = '5ms'")
                .unwrap();
        assert!(enabled.pending_rows_batching_enabled());
        for field in [
            "max_batch_rows",
            "max_concurrent_flushes",
            "worker_channel_capacity",
            "max_inflight_requests",
        ] {
            let options: PendingRowsBatcherOptions = toml::from_str(&format!(
                "protocols = ['influxdb']\npending_rows_flush_interval = '5ms'\n{field} = 0"
            ))
            .unwrap();
            assert!(!options.pending_rows_batching_enabled(), "{field}");
        }
    }
}
