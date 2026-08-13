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

//! Run flow as batching mode which is time-window-aware normal query triggered when new data arrives

use std::time::Duration;

use common_grpc::channel_manager::ClientTlsOption;
use serde::{Deserialize, Serialize};
use session::ReadPreference;

mod checkpoint;
pub(crate) mod engine;
mod eval_schedule;
pub(crate) mod frontend_client;
mod state;
mod table_creator;
mod task;
mod time_window;
pub(crate) mod utils;

/// Incremental read mode for a batching flow, selected only through the
/// reserved internal flow option
/// [`common_meta::ddl::create_flow::INTERNAL_INCREMENTAL_MODE_KEY`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IncrementalMode {
    /// Read only memtables newer than the checkpoint, skipping SSTs entirely.
    #[default]
    MemtableOnly,
    /// Exact row-level sequence delta `(C, H]` across memtables and all SSTs.
    SequenceRange,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BatchingModeOptions {
    /// The default batching engine query timeout is 10 minutes
    #[serde(with = "humantime_serde")]
    pub query_timeout: Duration,
    /// will output a warn log for any query that runs for more that this threshold
    #[serde(with = "humantime_serde")]
    pub slow_query_threshold: Duration,
    /// The minimum duration between two queries execution by batching mode task
    #[serde(with = "humantime_serde")]
    pub experimental_min_refresh_duration: Duration,
    /// The gRPC connection timeout
    #[serde(with = "humantime_serde")]
    pub grpc_conn_timeout: Duration,
    /// The gRPC max retry number
    pub experimental_grpc_max_retries: u32,
    /// Flow wait for available frontend timeout,
    /// if failed to find available frontend after frontend_scan_timeout elapsed, return error
    /// which prevent flownode from starting
    #[serde(with = "humantime_serde")]
    pub experimental_frontend_scan_timeout: Duration,
    /// Maximum number of filters allowed in a single query
    pub experimental_max_filter_num_per_query: usize,
    /// Time window merge distance
    pub experimental_time_window_merge_threshold: usize,
    /// Whether to enable experimental flow incremental source reads.
    ///
    /// When disabled, batching flows always execute full-snapshot queries.
    pub experimental_enable_incremental_read: bool,
    /// Internal incremental read mode for batching flows, injected only
    /// through the reserved internal flow option
    /// [`common_meta::ddl::create_flow::INTERNAL_INCREMENTAL_MODE_KEY`].
    #[serde(skip)]
    pub incremental_mode: IncrementalMode,
    /// Read preference of the Frontend client.
    pub read_preference: ReadPreference,
    /// TLS option for client connections to frontends.
    pub frontend_tls: Option<ClientTlsOption>,
}

impl Default for BatchingModeOptions {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(10 * 60),
            slow_query_threshold: Duration::from_secs(60),
            experimental_min_refresh_duration: Duration::new(5, 0),
            grpc_conn_timeout: Duration::from_secs(5),
            experimental_grpc_max_retries: 3,
            experimental_frontend_scan_timeout: Duration::from_secs(30),
            experimental_max_filter_num_per_query: 20,
            experimental_time_window_merge_threshold: 3,
            experimental_enable_incremental_read: false,
            incremental_mode: IncrementalMode::default(),
            read_preference: Default::default(),
            frontend_tls: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchingModeOptions, IncrementalMode};

    #[test]
    fn test_incremental_mode_not_exposed_by_options_serialization() {
        // `#[serde(skip)]`: the runtime-only field never appears in
        // serialized options, and injected input is ignored.
        let serialized = serde_json::to_string(&BatchingModeOptions::default()).unwrap();
        assert!(!serialized.contains("incremental_mode"));

        let mut json = serde_json::to_value(BatchingModeOptions::default()).unwrap();
        json["incremental_mode"] = serde_json::json!("sequence_range");
        let opts: BatchingModeOptions = serde_json::from_value(json).unwrap();
        assert_eq!(opts.incremental_mode, IncrementalMode::default());
    }
}
