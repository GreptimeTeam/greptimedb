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

/// Reserved internal epoch column name stamped on every emitted sink state row
/// when checkpoint persistence is active.
///
/// The enterprise state schema/view adds this column to the sink table; OSS
/// plan generation strips it from the schema-matching view and fills it with
/// the current epoch literal, and OSS restart/recovery reads it to validate
/// checkpoint trust. Ordinary flows (whose sinks never contain this column)
/// are byte-for-byte unaffected.
pub const INTERNAL_FLOW_EPOCH_COL_NAME: &str = "__greptime_internal_flow_epoch";

/// Sentinel window timestamp (in milliseconds: 9999-12-31T23:59:59.999Z) used
/// to mark the singleton checkpoint row in the sink table's window/time-index
/// column.
///
/// This value is a private convention between the flow runtime and the
/// internal producer of the sink state schema (the enterprise state schema/
/// view). It is NOT safe by construction: the flow's own query may bin source
/// timestamps with an arbitrary `date_bin(stride, origin)` lattice, so the
/// internal producer MUST validate at CREATE time that the sentinel cannot
/// collide with any real window for the exact flow `date_bin` (stride and
/// origin). The OSS runtime never auto-creates the sentinel row; it only
/// reads/writes it when the sink schema already contains the reserved epoch
/// column, and ordinary flows (without that column) are byte-for-byte
/// unaffected.
pub const CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS: i64 = 253_402_300_799_999;

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
