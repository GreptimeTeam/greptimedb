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

use common_batcher::flush_policy::timing::TimingFlushPolicy;
use serde::{Deserialize, Serialize};
use servers::batcher::BatchingProtocol;
use tokio::sync::Semaphore;

use crate::frontend::FrontendOptions;

/// Independent ordinary-table and logical-table batching configuration.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PendingRowsBatcherOptions {
    /// Existing ordinary-table controls retain their original TOML paths.
    #[serde(flatten)]
    pub table: BatcherOptions,
    /// Independent logical-table controls; absence retains the legacy Prom fallback.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_logical_options"
    )]
    pub logical_table: Option<BatcherOptions>,
}

/// Write batching controls shared by ingestion protocols.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct BatcherOptions {
    /// Write protocols sharing this batcher; empty disables all entrances.
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

impl BatcherOptions {
    /// Returns whether a protocol opts in and its controls pass construction validation.
    pub fn pending_rows_batching_enabled(&self) -> bool {
        !self.protocols.is_empty()
            && TimingFlushPolicy::validate(self.pending_rows_flush_interval)
            && self.max_batch_rows > 0
            && [
                self.max_concurrent_flushes,
                self.worker_channel_capacity,
                self.max_inflight_requests,
                self.flow_notification_queue_capacity.get(),
            ]
            .into_iter()
            .all(|capacity| (1..=Semaphore::MAX_PERMITS).contains(&capacity))
    }
}

impl Default for BatcherOptions {
    fn default() -> Self {
        Self {
            protocols: Vec::new(),
            pending_rows_flush_interval: Duration::ZERO,
            max_batch_rows: 100_000,
            max_concurrent_flushes: 256,
            worker_channel_capacity: 65_536,
            max_inflight_requests: 3000,
            flow_notification_queue_capacity: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

/// Rejects protocols that cannot write metric-engine logical tables.
fn deserialize_logical_options<'de, D>(deserializer: D) -> Result<Option<BatcherOptions>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let options = Option::<BatcherOptions>::deserialize(deserializer)?;
    if options.as_ref().is_some_and(|options| {
        options
            .protocols
            .iter()
            .any(|protocol| !matches!(protocol, BatchingProtocol::Prom | BatchingProtocol::Otlp))
    }) {
        return Err(serde::de::Error::custom(
            "pending_rows_batcher.logical_table only supports prom and otlp",
        ));
    }
    Ok(options)
}

impl FrontendOptions {
    /// Explicit configuration wins even when it disables batching.
    pub(crate) fn table_batcher_options(&self) -> &BatcherOptions {
        &self.pending_rows_batcher.table
    }

    /// Resolve legacy Prom controls once without merging fields across config blocks.
    pub(crate) fn logical_batcher_options(&self) -> BatcherOptions {
        if let Some(options) = &self.pending_rows_batcher.logical_table {
            return options.clone();
        }
        let shared = &self.pending_rows_batcher.table;
        if shared.protocols.contains(&BatchingProtocol::Prom)
            && shared.pending_rows_batching_enabled()
        {
            let mut options = shared.clone();
            options.protocols = vec![BatchingProtocol::Prom];
            return options;
        }
        let prom = &self.prom_store;
        BatcherOptions {
            protocols: vec![BatchingProtocol::Prom],
            pending_rows_flush_interval: prom.pending_rows_flush_interval,
            max_batch_rows: prom.max_batch_rows,
            max_concurrent_flushes: prom.max_concurrent_flushes,
            worker_channel_capacity: prom.worker_channel_capacity,
            max_inflight_requests: prom.max_inflight_requests,
            flow_notification_queue_capacity: prom.flow_notification_queue_capacity,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::service_config::pending_rows_batcher::*;

    #[test]
    fn test_batcher_config_precedence() {
        use crate::frontend::FrontendOptions;
        let legacy = "[prom_store]\nenable = true\nwith_metric_engine = true\npending_rows_flush_interval = '2s'\nmax_batch_rows = 9\n";
        let shared = "[pending_rows_batcher]\nprotocols = ['prom', 'otlp']\npending_rows_flush_interval = '3s'\n";
        let options: FrontendOptions = toml::from_str(legacy).unwrap();
        assert_eq!(options.logical_batcher_options().max_batch_rows, 9);
        assert_eq!(
            options.logical_batcher_options().protocols,
            vec![BatchingProtocol::Prom]
        );
        let options: FrontendOptions = toml::from_str(&format!("{legacy}{shared}")).unwrap();
        assert_eq!(
            options
                .logical_batcher_options()
                .pending_rows_flush_interval,
            Duration::from_secs(3)
        );
        assert_eq!(options.logical_batcher_options().max_batch_rows, 100_000);
        assert_eq!(
            options.logical_batcher_options().protocols,
            vec![BatchingProtocol::Prom]
        );
        for explicit in [
            "",
            "protocols = ['otlp']\npending_rows_flush_interval = '5s'",
        ] {
            let options: FrontendOptions = toml::from_str(&format!(
                "{legacy}{shared}[pending_rows_batcher.logical_table]\n{explicit}"
            ))
            .unwrap();
            assert_eq!(
                &options.logical_batcher_options(),
                options.pending_rows_batcher.logical_table.as_ref().unwrap()
            );
            assert_eq!(
                options.table_batcher_options(),
                &toml::from_str::<FrontendOptions>(&format!("{legacy}{shared}"))
                    .unwrap()
                    .pending_rows_batcher
                    .table
            );
            let restored: FrontendOptions =
                toml::from_str(&toml::to_string(&options).unwrap()).unwrap();
            assert_eq!(
                options.pending_rows_batcher.logical_table,
                restored.pending_rows_batcher.logical_table
            );
            assert_eq!(options.pending_rows_batcher, restored.pending_rows_batcher);
        }
    }

    #[test]
    fn test_batcher_config_independent_defaults() {
        use crate::frontend::FrontendOptions;

        let parent = "[pending_rows_batcher]\nprotocols = ['prom']\npending_rows_flush_interval = '3s'\nmax_batch_rows = 7\n";
        let options: FrontendOptions = toml::from_str(parent).unwrap();
        let serialized = toml::to_string(&options).unwrap();
        assert!(!serialized.contains("[pending_rows_batcher.logical_table]"));
        assert!(!serialized.contains("[pending_rows_batcher.table]"));
        let restored: FrontendOptions = toml::from_str(&serialized).unwrap();
        assert_eq!(restored.logical_batcher_options().max_batch_rows, 7);

        let options: FrontendOptions =
            toml::from_str(&format!("{parent}[pending_rows_batcher.logical_table]")).unwrap();
        assert_eq!(options.table_batcher_options().max_batch_rows, 7);
        assert_eq!(options.logical_batcher_options(), BatcherOptions::default());
        assert!(
            !options
                .logical_batcher_options()
                .pending_rows_batching_enabled()
        );
    }

    #[test]
    fn test_logical_batcher_config_protocols() {
        use crate::frontend::FrontendOptions;
        for protocols in ["[]", "['prom']", "['otlp']", "['prom', 'otlp']"] {
            let options: FrontendOptions = toml::from_str(&format!(
                "[pending_rows_batcher.logical_table]\nprotocols = {protocols}"
            ))
            .unwrap();
            assert!(options.pending_rows_batcher.logical_table.is_some());
        }
        for protocol in [
            "influxdb",
            "logs",
            "loki",
            "http_sql",
            "opentsdb",
            "elasticsearch",
            "splunk",
            "mysql",
            "postgres",
        ] {
            assert!(
                toml::from_str::<FrontendOptions>(&format!(
                    "[pending_rows_batcher.logical_table]\nprotocols = ['{protocol}']"
                ))
                .is_err()
            );
        }
    }

    #[test]
    fn test_protocols() {
        let options: BatcherOptions = toml::from_str(
            "protocols = ['influxdb', 'opentsdb', 'otlp', 'logs', 'loki', 'splunk', 'elasticsearch', 'http_sql', 'prom', 'mysql', 'postgres']",
        ).unwrap();
        assert_eq!(options.protocols.len(), 11);
        assert!(options.protocols.contains(&BatchingProtocol::Mysql));
        assert!(options.protocols.contains(&BatchingProtocol::Postgres));
        assert!(options.protocols.contains(&BatchingProtocol::HttpSql));
        assert!(BatcherOptions::default().protocols.is_empty());
        for invalid in ["sql", "jaeger", "unknown"] {
            assert!(
                toml::from_str::<BatcherOptions>(&format!("protocols = ['{invalid}']")).is_err()
            );
        }
    }

    #[test]
    fn test_notification_capacity() {
        let default = BatcherOptions::default();
        assert_eq!(default.flow_notification_queue_capacity.get(), 1024);
        let configured: BatcherOptions =
            toml::from_str("flow_notification_queue_capacity = 8").unwrap();
        assert_eq!(configured.flow_notification_queue_capacity.get(), 8);
        assert!(toml::from_str::<BatcherOptions>("flow_notification_queue_capacity = 0").is_err());
    }

    #[test]
    fn test_defaults_and_roundtrip() {
        let options: BatcherOptions = toml::from_str("").unwrap();
        assert_eq!(options, BatcherOptions::default());
        assert!(!options.pending_rows_batching_enabled());
        assert_eq!(options.max_batch_rows, 100_000);
        assert_eq!(options.max_concurrent_flushes, 256);
        assert_eq!(options.worker_channel_capacity, 65_536);
        assert_eq!(options.max_inflight_requests, 3000);
        let serialized = toml::to_string(&options).unwrap();
        assert_eq!(
            options,
            toml::from_str::<BatcherOptions>(&serialized).unwrap()
        );
    }

    #[test]
    fn test_worker_capacity_override() {
        let options: FrontendOptions = toml::from_str(
            r#"
[pending_rows_batcher]
worker_channel_capacity = 12345
[pending_rows_batcher.logical_table]
worker_channel_capacity = 12345
[prom_store]
enable = true
with_metric_engine = true
worker_channel_capacity = 12345
"#,
        )
        .unwrap();
        assert_eq!(
            options.pending_rows_batcher.table.worker_channel_capacity,
            12_345
        );
        assert_eq!(
            options
                .pending_rows_batcher
                .logical_table
                .unwrap()
                .worker_channel_capacity,
            12_345
        );
        assert_eq!(options.prom_store.worker_channel_capacity, 12_345);
    }

    #[test]
    fn test_partial_options_and_zero_controls() {
        let options: BatcherOptions =
            toml::from_str("pending_rows_flush_interval = '5ms'").unwrap();
        assert_eq!(
            options.pending_rows_flush_interval,
            Duration::from_millis(5)
        );
        assert!(!options.pending_rows_batching_enabled());
        let enabled: BatcherOptions =
            toml::from_str("protocols = ['http_sql']\npending_rows_flush_interval = '5ms'")
                .unwrap();
        assert!(enabled.pending_rows_batching_enabled());
        for field in [
            "max_batch_rows",
            "max_concurrent_flushes",
            "worker_channel_capacity",
            "max_inflight_requests",
        ] {
            let options: BatcherOptions = toml::from_str(&format!(
                "protocols = ['influxdb']\npending_rows_flush_interval = '5ms'\n{field} = 0"
            ))
            .unwrap();
            assert!(!options.pending_rows_batching_enabled(), "{field}");
        }
    }
}
