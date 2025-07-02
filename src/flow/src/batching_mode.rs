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

use serde::{Deserialize, Serialize};

pub(crate) mod engine;
pub(crate) mod frontend_client;
mod state;
mod task;
mod time_window;
mod utils;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BatchingModeOptions {
    /// The default batching engine query timeout is 10 minutes
    pub query_timeout: Duration,
    /// will output a warn log for any query that runs for more that 1 minutes, and also every 1 minutes when that query is still running
    pub slow_query_threshold: Duration,
    /// The minimum duration between two queries execution by batching mode task
    pub min_refresh_duration: Duration,
    /// Grpc connection timeout
    pub grpc_conn_timeout: Duration,
    /// Grpc max retry number
    pub grpc_max_retries: u32,
    /// Flow wait for available frontend timeout,
    /// if failed to find available frontend after FRONTEND_SCAN_TIMEOUT elapsed, return error
    /// which should prevent flownode from starting
    pub frontend_scan_timeout: Duration,
    /// Frontend activity timeout
    /// if frontend is down(not sending heartbeat) for more than FRONTEND_ACTIVITY_TIMEOUT, it will be removed from the list that flownode use to connect
    pub frontend_activity_timeout: Duration,
}

impl Default for BatchingModeOptions {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(10 * 60),
            slow_query_threshold: Duration::from_secs(60),
            min_refresh_duration: Duration::new(5, 0),
            grpc_conn_timeout: Duration::from_secs(5),
            grpc_max_retries: 3,
            frontend_scan_timeout: Duration::from_secs(30),
            frontend_activity_timeout: Duration::from_secs(60),
        }
    }
}
