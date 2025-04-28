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

pub(crate) mod engine;
pub(crate) mod frontend_client;
mod state;
mod task;
mod time_window;
mod utils;

/// TODO(discord9): make those constants configurable
/// The default batching engine query timeout is 10 minutes
pub const DEFAULT_BATCHING_ENGINE_QUERY_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// will output a warn log for any query that runs for more that 1 minutes, and also every 1 minutes when that query is still running
pub const SLOW_QUERY_THRESHOLD: Duration = Duration::from_secs(60);

/// The minimum duration between two queries execution by batching mode task
pub const MIN_REFRESH_DURATION: Duration = Duration::new(5, 0);

/// Grpc connection timeout
const GRPC_CONN_TIMEOUT: Duration = Duration::from_secs(5);

/// Grpc max retry number
const GRPC_MAX_RETRIES: u32 = 3;

/// Flow wait for available frontend timeout,
/// if failed to find available frontend after FRONTEND_SCAN_TIMEOUT elapsed, return error
/// which should prevent flownode from starting
pub const FRONTEND_SCAN_TIMEOUT: Duration = Duration::from_secs(30);

/// Frontend activity timeout
/// if frontend is down(not sending heartbeat) for more than FRONTEND_ACTIVITY_TIMEOUT, it will be removed from the list that flownode use to connect
pub const FRONTEND_ACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);
